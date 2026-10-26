use std::fmt::{Debug, Formatter};
use std::io::{SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use dav_server::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsStream, OpenOptions,
        ReadDirMeta,
    },
};
use futures_util::future::{ready, FutureExt};
use tracing::{debug, error, info, trace, warn};
use crate::{
    cache::Cache,
    drive::{QuarkDrive, QuarkFile, UploadSource},
    oplog::OpLog,
    upload_progress::UploadTracker,
    util::is_ignored_system_file,
};
use bytes::BufMut;

use md5::Context as Md5Context;
use sha1::Sha1;
use tokio::io::AsyncWriteExt;

use sha1::Digest;

use crate::drive::model::{Callback, UpAuthAndCommitRequest, UpPartMethodRequest};

/// 并行上传共用的内存预算：多文件占用合计，超限则新写入改走临时文件。
#[derive(Debug)]
pub struct UploadMemoryPool {
    limit: AtomicU64,
    used: AtomicU64,
}

impl UploadMemoryPool {
    /// `limit == 0` 表示禁止内存上传（始终落盘）。
    fn new(limit: u64) -> Self {
        Self {
            limit: AtomicU64::new(limit),
            used: AtomicU64::new(0),
        }
    }

    fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// 尝试再占 `bytes`；成功返回 true。CAS 保证并行上传合计不超限。
    fn try_reserve(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        let limit = self.limit();
        if limit == 0 {
            return false;
        }
        loop {
            let used = self.used.load(Ordering::Acquire);
            let Some(new_used) = used.checked_add(bytes) else {
                return false;
            };
            if new_used > limit {
                return false;
            }
            if self
                .used
                .compare_exchange(used, new_used, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        loop {
            let used = self.used.load(Ordering::Acquire);
            let new_used = used.saturating_sub(bytes);
            if self
                .used
                .compare_exchange(used, new_used, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn set_limit(&self, limit: u64) {
        self.limit.store(limit, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct QuarkDriveFileSystem {
    pub(crate) drive: QuarkDrive,
    pub(crate) dir_cache: Cache,
    /// 已拉回的分块，按 (fid, 块号) 共享。挂文件系统层，避免随 DavFile 句柄丢弃。
    pub(crate) chunk_cache: crate::prefetch::ChunkCache,
    /// 上传内存池：并行上传共享配额。
    upload_memory: Arc<UploadMemoryPool>,
    uploading: Arc<DashMap<String, Vec<QuarkFile>>>,
    pub(crate) root: PathBuf,
    no_trash: bool,
    read_only: bool,
    upload_buffer_size: usize,
    skip_upload_same_size: bool,
    prefer_http_download: bool,
    upload_wait_timeout: u64,
    oplog: Arc<OpLog>,
    /// Web 控制台上传进度（与 CloudSync uploading stub 独立）。
    upload_tracker: UploadTracker,
    /// 正在向网盘上传的路径。重复 PUT 挂到同一个槽上，等原任务结束再回复，避免 CloudSync 提前看到成功。
    upload_inflight: Arc<DashMap<String, Arc<InflightSlot>>>,
    /// 客户端 PUT 带来的文件 mtime（unix 毫秒）。PROPFIND 用它覆盖夸克的上传时间，
    /// 否则 CloudSync 每次对账都认为远端时间不对，把同一个文件再传一遍。
    client_mtimes: Arc<DashMap<String, u64>>,
}

impl QuarkDriveFileSystem {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        drive: QuarkDrive,
        root: String,
        cache_size: u64,
        cache_bytes: u64,
        cache_ttl: u64,
    ) -> Result<Self> {
        let dir_cache = Cache::new(cache_size, cache_bytes, cache_ttl, drive.clone());
        debug!("dir cache initialized");
        let root = if root.starts_with('/') {
            PathBuf::from(root)
        } else {
            Path::new("/").join(root)
        };
        Ok(Self {
            drive,
            dir_cache,
            chunk_cache: crate::prefetch::new_chunk_cache(crate::prefetch::DEFAULT_CACHE_BYTES),
            // 默认 64MiB：小文件走内存，并行合计超限再落盘。
            upload_memory: Arc::new(UploadMemoryPool::new(64 * 1024 * 1024)),
            uploading: Arc::new(DashMap::new()),
            root,
            no_trash: false,
            read_only: false,
            upload_buffer_size: 16 * 1024 * 1024,
            skip_upload_same_size: false,
            prefer_http_download: false,
            upload_wait_timeout: 0,
            oplog: Arc::new(OpLog::new("./logs", 7)),
            upload_tracker: UploadTracker::new(),
            upload_inflight: Arc::new(DashMap::new()),
            client_mtimes: Arc::new(DashMap::new()),
        })
    }

    /// 记下 PUT 上的客户端 mtime，供列表和上传预创建使用。
    pub fn note_client_mtime(&self, path: &str, unix_ms: u64) {
        if unix_ms == 0 || path.is_empty() {
            return;
        }
        self.client_mtimes.insert(path.to_string(), unix_ms);
    }

    fn client_mtime_ms(&self, path: &str) -> Option<u64> {
        self.client_mtimes.get(path).map(|v| *v)
    }

    /// 列表/PROPFIND 报客户端原始时间，而不是夸克记下的「上传完成时刻」。
    fn apply_client_mtime(&self, file: &mut QuarkFile) {
        let parent = file.parent_path.as_deref().unwrap_or("");
        let key = join_dav_path(parent, &file.file_name);
        if let Some(ms) = self.client_mtimes.get(&key) {
            file.updated_at = *ms;
        }
    }

    /// 尝试登记「此路径正在上传」。已有则返回 None，调用方应等待原任务，而不是再开一条 OSS。
    fn try_begin_upload(&self, path: &str) -> Option<UploadInflightGuard> {
        use dashmap::mapref::entry::Entry;
        match self.upload_inflight.entry(path.to_string()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                let slot = InflightSlot::new();
                v.insert(slot.clone());
                Some(UploadInflightGuard {
                    map: self.upload_inflight.clone(),
                    path: path.to_string(),
                    slot,
                    settled: false,
                })
            }
        }
    }

    fn upload_in_flight(&self, path: &str) -> bool {
        self.upload_inflight.contains_key(path)
    }

    fn inflight_slot(&self, path: &str) -> Option<Arc<InflightSlot>> {
        self.upload_inflight.get(path).map(|s| Arc::clone(s.value()))
    }

    /// 供 Web 页读取上传进度。
    pub fn upload_tracker(&self) -> &UploadTracker {
        &self.upload_tracker
    }

    /// 控制台取消上传：标 cancelled、abort 后台任务，并摘掉 PROPFIND 占位。
    pub fn cancel_upload(&self, path: &str) -> bool {
        if !self.upload_tracker.request_cancel(path) {
            return false;
        }
        let pb = Path::new(path);
        if let (Some(name), Some(parent)) = (
            pb.file_name().and_then(|s| s.to_str()),
            pb.parent().map(|p| {
                let s = p.to_string_lossy();
                if s.is_empty() {
                    "".to_string()
                } else {
                    s.into_owned()
                }
            }),
        ) {
            self.remove_uploading_file(&parent, name);
        }
        true
    }

    /// 供 Web 页读取操作日志。
    pub fn oplog(&self) -> &Arc<OpLog> {
        &self.oplog
    }

    /// 分块预取缓存上限（字节）。0 = 关闭缓存。用 setter 避免改动 `new` 公开签名。
    pub fn set_chunk_cache_bytes(&mut self, bytes: u64) -> &mut Self {
        self.chunk_cache = crate::prefetch::new_chunk_cache(bytes);
        self
    }

    /// 上传内存上限（字节，全进程并行合计）。0 = 始终写临时文件。
    pub fn set_upload_memory_limit(&mut self, bytes: u64) -> &mut Self {
        self.upload_memory.set_limit(bytes);
        self
    }

    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    pub fn set_no_trash(&mut self, no_trash: bool) -> &mut Self {
        self.no_trash = no_trash;
        self
    }

    pub fn set_upload_buffer_size(&mut self, upload_buffer_size: usize) -> &mut Self {
        self.upload_buffer_size = upload_buffer_size;
        self
    }

    pub fn set_skip_upload_same_size(&mut self, skip_upload_same_size: bool) -> &mut Self {
        self.skip_upload_same_size = skip_upload_same_size;
        self
    }

    pub fn set_prefer_http_download(&mut self, prefer_http_download: bool) -> &mut Self {
        self.prefer_http_download = prefer_http_download;
        self
    }

    pub fn set_upload_wait_timeout(&mut self, upload_wait_timeout: u64) -> &mut Self {
        self.upload_wait_timeout = upload_wait_timeout;
        self
    }

    pub fn set_oplog(&mut self, oplog: Arc<OpLog>) -> &mut Self {
        self.oplog = oplog;
        self
    }

    fn drive_mode(&self) -> &'static str {
        if self.drive.is_open_mode() {
            "open"
        } else {
            "cookie"
        }
    }

    fn parent_cache_key(parent: &Path) -> String {
        let s = parent.to_string_lossy();
        if s.is_empty() {
            "/".to_string()
        } else {
            s.into_owned()
        }
    }

    fn list_uploading_files(&self, parent_file_path: &str) -> Vec<QuarkFile> {
        self.uploading
            .get(parent_file_path)
            .map(|val_ref| val_ref.value().clone())
            .unwrap_or_default()
    }

    fn remove_uploading_file(&self, parent_file_path: &str, file_name: &str) {
        if let Some(mut files) = self.uploading.get_mut(parent_file_path) {
            if let Some(index) = files.iter().position(|x| x.file_name == file_name) {
                files.swap_remove(index);
            }
        }
    }

    /// CloudSync 等客户端 PUT 后立刻 PROPFIND：上传过程中必须能查到条目与正确 size。
    fn update_uploading_file(&self, parent_file_path: &str, file_name: &str, size: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if let Some(mut files) = self.uploading.get_mut(parent_file_path) {
            if let Some(f) = files.iter_mut().find(|x| x.file_name == file_name) {
                f.size = size;
                f.updated_at = now;
            }
        }
    }

    /// 上传完成后等到父目录列表能看见该文件。
    /// 返回是否已在远端列表中见到；无论是否见到都先保留 uploading 占位一段时间，
    /// 因为 CloudSync 常在 PUT 一返回就 PROPFIND，夸克列表/编码偶发滞后时占位能兜住「未找到远程文件」。
    async fn wait_until_remote_visible(
        &self,
        parent_dir: &Path,
        file_name: &str,
        size: u64,
        parent_key: &str,
    ) -> bool {
        self.update_uploading_file(parent_key, file_name, size);
        self.dir_cache.invalidate(parent_dir).await;

        // 90s：小文件也偶发列表延迟；超时仍保留占位。
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
        let mut delay = std::time::Duration::from_millis(300);
        let key = Self::parent_cache_key(parent_dir);
        let mut visible = false;

        loop {
            if let Some(files) = self.dir_cache.get_or_insert(&key).await {
                if files.iter().any(|f| file_names_equal(&f.file_name, file_name)) {
                    debug!(
                        parent = %parent_dir.display(),
                        file_name = %file_name,
                        "upload visible in remote listing"
                    );
                    visible = true;
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    parent = %parent_dir.display(),
                    file_name = %file_name,
                    "remote listing still missing file after upload; keep uploading stub for PROPFIND"
                );
                break;
            }
            self.dir_cache.invalidate(parent_dir).await;
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(2));
        }

        // 延迟摘占位：给 CloudSync 紧接 PUT 的 PROPFIND 留窗口。
        let fs = self.clone();
        let parent_key = parent_key.to_string();
        let file_name = file_name.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            fs.remove_uploading_file(&parent_key, &file_name);
            debug!(
                parent = %parent_key,
                file_name = %file_name,
                "deferred remove uploading stub"
            );
        });
        visible
    }

    /// 新文件已经出现在列表里之后再删旧 fid。
    /// 秒传若复用了旧 fid，列表里不会有另一份，这时不能删，否则唯一副本没了。
    async fn delete_replaced_file(&self, old_fid: &str, parent_dir: &Path, file_name: &str) {
        if old_fid.is_empty() {
            return;
        }
        let key = Self::parent_cache_key(parent_dir);
        // 30 秒：大文件 finish 后列表偶尔滞后；超时就留着旧文件，避免误删。
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            self.dir_cache.invalidate(parent_dir).await;
            let found_new = self.dir_cache.get_or_insert(&key).await.map(|files| {
                files.iter().any(|f| {
                    file_names_equal(&f.file_name, file_name)
                        && !f.fid.is_empty()
                        && f.fid != old_fid
                })
            }).unwrap_or(false);
            if found_new {
                match self.drive.remove_file(old_fid, !self.no_trash).await {
                    Ok(()) => {
                        self.dir_cache.invalidate(parent_dir).await;
                    }
                    Err(err) => {
                        // 新文件已在列表里。删旧失败不能把这次成功报成失败。
                        warn!(old_fid, error = %err, "new upload ok but deleting previous file failed");
                    }
                }
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(old_fid, file_name, "replacement not visible; keep previous file");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// 上传字节已经结束。这里才等列表，调用方必须先放开上传槽和快车道。
    async fn complete_background_upload(
        &self,
        tracker: &UploadTracker,
        progress_path: &str,
        parent_dir: &Path,
        file_name: &str,
        size: u64,
        parent_key: &str,
        log_path: &str,
        log_mode: &str,
        old_fid: &str,
    ) {
        self.delete_replaced_file(old_fid, parent_dir, file_name).await;
        let visible = self
            .wait_until_remote_visible(parent_dir, file_name, size, parent_key)
            .await;
        let result = if visible { "ok" } else { "ok:list_pending" };
        tracker.finish(progress_path, "ok", result);
        self.oplog.log_upload(log_path, size, log_mode, result).await;
    }

    /// 失败：摘占位、作废目录缓存，让下一次 PUT 看到网盘真实状态而不是半截列表。
    async fn fail_background_upload(
        &self,
        tracker: &UploadTracker,
        progress_path: &str,
        parent_dir: &Path,
        file_name: &str,
        parent_key: &str,
        log_path: &str,
        size: u64,
        log_mode: &str,
        detail: &str,
    ) {
        let detail = flatten_error(detail);
        tracker.finish(progress_path, "error", &detail);
        self.remove_uploading_file(parent_key, file_name);
        self.dir_cache.invalidate(parent_dir).await;
        let result = if detail.is_empty() {
            "error".to_string()
        } else if detail.starts_with("error") {
            detail
        } else {
            format!("error:{detail}")
        };
        self.oplog.log_upload(log_path, size, log_mode, &result).await;
    }

    async fn find_in_cache(&self, path: &Path) -> Result<Option<QuarkFile>, FsError> {
        if let Some(parent) = path.parent() {
            let parent_str = Self::parent_cache_key(parent);
            let file_name = path
                .file_name()
                .ok_or(FsError::NotFound)?
                .to_string_lossy()
                .into_owned();
            let file = self.dir_cache.get_or_insert(&parent_str).await.and_then(|files| {
                for file in &files {
                    if file_names_equal(&file.file_name, &file_name) {
                        return Some(file.clone());
                    }
                }
                None
            });
            Ok(file.map(|mut f| {
                self.apply_client_mtime(&mut f);
                f
            }))
        } else {
            let root = QuarkFile::new_root();
            Ok(Some(root))
        }
    }

    async fn get_file(&self, path: PathBuf) -> Result<Option<QuarkFile>, FsError> {
        if let Some(file) = self.find_in_cache(&path).await? {
            trace!(path = %path.display(), file_id = %file.fid, "file found in cache");
            return Ok(Some(file));
        }
        // 上传中 / 刚传完尚未进列表：用 uploading 占位，供 CloudSync PROPFIND。
        if let Some(parent) = path.parent() {
            let parent_key = Self::parent_cache_key(parent);
            let want = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if let Some(mut f) = self
                .list_uploading_files(&parent_key)
                .into_iter()
                .find(|f| file_names_equal(&f.file_name, want))
            {
                self.apply_client_mtime(&mut f);
                return Ok(Some(f));
            }
        }
        Ok(None)
    }


    pub(crate) async fn get_file_md5_for_path(&self, path: &Path) -> Option<String> {
        let file = self.get_file(path.to_path_buf()).await.ok()??;
        if file.fid.is_empty() {
            return None;
        }
        // Try cached md5 first (populated by get_download_urls during file serving)
        if let Some(md5) = self.drive.get_cached_md5(&file.fid) {
            return Some(md5);
        }
        // Fall back to API call
        self.drive.get_file_md5(&file.fid).await.ok()?
    }

    fn normalize_dav_path(&self, dav_path: &DavPath) -> PathBuf {
        let path = dav_path.as_pathbuf();
        if self.root.parent().is_none() || path.starts_with(&self.root) {
            return path;
        }
        let rel_path = dav_path.as_rel_ospath();
        if rel_path == Path::new("") {
            return self.root.clone();
        }
        self.root.join(rel_path)
    }
}

impl DavFileSystem for QuarkDriveFileSystem {
    fn open<'a>(
        &'a self,
        dav_path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        let path = self.normalize_dav_path(dav_path);
        let mode = if options.write { "write" } else { "read" };
        debug!(path = %path.display(), mode = %mode, "fs: open");
        async move {
            if options.append {
                // Can't support open in write-append mode
                error!(path = %path.display(), "unsupported write-append mode");
                return Err(FsError::NotImplemented);
            }
            let parent_path = path.parent().ok_or(FsError::NotFound)?;
            let parent_file = self
                .get_file(parent_path.to_path_buf())
                .await?
                .ok_or(FsError::NotFound)?;
            let sha1 = options.checksum.and_then(|c| {
                if let Some((algo, hash)) = c.split_once(':') {
                    if algo.eq_ignore_ascii_case("sha1") {
                        Some(hash.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            });

            #[cfg(feature = "local_upload_hash")]
            if options.write && path.is_file() && sha1.is_none() {
                if let Ok((_, sha1_val)) = calc_md5_sha1(&path) {
                    sha1 = Some(sha1_val);
                }
            }
            let mut dav_file = if let Some(file) = self.get_file(path.clone()).await? {
                if options.write && options.create_new {
                    return Err(FsError::Exists);
                }
                if options.write && self.read_only {
                    return Err(FsError::Forbidden);
                }
                QuarkDavFile::new(
                    self.clone(),
                    file,
                    parent_file.fid,
                    parent_path.to_path_buf(),
                    // Always start at 0: consume_buf() accumulates the actual bytes written
                    0u64,
                    sha1,
                )
            } else if options.write && (options.create || options.create_new) {
                if self.read_only {
                    return Err(FsError::Forbidden);
                }

                let size = options.size;
                let name = dav_path
                    .file_name()
                    .ok_or(FsError::GeneralFailure)?
                    .to_string();

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let mut file = QuarkFile {
                    fid: "".to_string(),
                    file_name: name,
                    pdir_fid: parent_file.fid.clone(),
                    size: size.unwrap_or(0),
                    format_type: "application/octet-stream".to_string(),
                    status: 1,
                    dir: false,
                    file: true,
                    content_hash: sha1.clone(),
                    created_at: now as u64,
                    updated_at: now as u64,
                    download_url: None,
                    parent_path: Some(parent_path.to_string_lossy().into_owned()),
                    thumbnail_url: None,
                };
                // 占位期间 PROPFIND 也要报客户端 mtime，避免 PUT 还没返回群晖就觉得时间不对。
                self.apply_client_mtime(&mut file);

                let parent_key = Self::parent_cache_key(parent_path);
                let mut uploading = self.uploading.entry(parent_key).or_default();
                uploading.push(file.clone());
                QuarkDavFile::new(
                    self.clone(),
                    file,
                    parent_file.fid,
                    parent_path.to_path_buf(),
                   // size.unwrap_or(0),
                    // The client will not provide the size of large files,
                    // So the size is calculated uniformly by the post program
                    0u64,
                    sha1,
                )
            } else {
                return Err(FsError::NotFound);
            };
            dav_file.http_download = self.prefer_http_download;
            Ok(Box::new(dav_file) as Box<dyn DavFile>)
        }
            .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        let path = self.normalize_dav_path(path);
        debug!(path = %path.display(), "fs: read_dir");
        async move {
            let key = Self::parent_cache_key(&path);
            let mut files = self
                .dir_cache
                .get_or_insert(&key)
                .await
                .ok_or(FsError::NotFound)?;

            // 合并正在上传的占位，避免 CloudSync 列目录时看不到刚 PUT 的文件。
            for up in self.list_uploading_files(&key) {
                if !files
                    .iter()
                    .any(|f| file_names_equal(&f.file_name, &up.file_name))
                {
                    files.push(up);
                }
            }

            let mut v: Vec<Result<Box<dyn DavDirEntry>, FsError>> =
                Vec::with_capacity(files.len());
            for mut file in files {
                self.apply_client_mtime(&mut file);
                v.push(Ok(Box::new(file)));
            }

            let stream = futures_util::stream::iter(v);
            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
            .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        let mut path = self.normalize_dav_path(path);
        if path.as_path().to_str() == Some("0") {
            // root path
            debug!("fs: metadata for root");
            path = PathBuf::from("/");
        }
        debug!(path = %path.display(), "fs: metadata");
        async move {
            // if root return
            if path == self.root {
                debug!("fs: metadata for root");
                let root_file = QuarkFile::new_root();
                return Ok(Box::new(root_file) as Box<dyn DavMetaData>);
            }

            let file = self
                .get_file(path.clone())
                .await
                .unwrap_or(None)
                .ok_or(FsError::NotFound)?;
            Ok(Box::new(file) as Box<dyn DavMetaData>)
        }
            .boxed()
    }
    fn have_props<'a>(
        &'a self,
        _path: &'a DavPath,
    ) -> std::pin::Pin<Box<dyn futures_util::Future<Output = bool> + Send + 'a>> {
        Box::pin(ready(true))
    }

    fn get_prop(&self, dav_path: &DavPath, prop: dav_server::fs::DavProp) -> FsFuture<Vec<u8>> {
        let path = self.normalize_dav_path(dav_path);
        let prop_name = match prop.prefix.as_ref() {
            Some(prefix) => format!("{}:{}", prefix, prop.name),
            None => prop.name.to_string(),
        };
        debug!(path = %path.display(), prop = %prop_name, "fs: get_prop");
        async move {
            if prop.namespace.as_deref() == Some("http://owncloud.org/ns")
                && prop.name == "checksums"
            {
                let file = self.get_file(path).await?.ok_or(FsError::NotFound)?;
                if let Some(sha1) = file.content_hash {
                    let xml = format!(
                        r#"<?xml version="1.0"?>
                        <oc:checksums xmlns:d="DAV:" xmlns:nc="http://nextcloud.org/ns" xmlns:oc="http://owncloud.org/ns">
                            <oc:checksum>sha1:{}</oc:checksum>
                        </oc:checksums>
                    "#,
                        sha1
                    );
                    return Ok(xml.into_bytes());
                }
            }
            Err(FsError::NotImplemented)
        }
            .boxed()
    }

    fn get_quota(&self) -> FsFuture<(u64, Option<u64>)> {
        debug!("fs: get_quota");
        async move {
            let (used, total) = self.drive.get_quota().await.map_err(|err| {
                error!(error = %err, "get quota failed");
                FsError::GeneralFailure
            })?;
            Ok((used, Some(total)))
        }
            .boxed()
    }

    fn create_dir<'a>(&'a self, dav_path: &'a DavPath) -> FsFuture<'a, ()> {
        let path = self.normalize_dav_path(dav_path);
        debug!(path = %path.display(), "fs: create_dir");
        async move {
            if self.read_only {
                return Err(FsError::Forbidden);
            }
            let parent_path = path.parent().ok_or(FsError::NotFound)?;
            let parent_file = self
                .get_file(parent_path.to_path_buf())
                .await?
                .ok_or(FsError::NotFound)?;
            if !parent_file.dir {
                return Err(FsError::Forbidden);
            }
            // check if the folder already exists
            if self.get_file(path.clone()).await?.is_some() {
                return Err(FsError::Exists);
            }
            if let Some(name) = path.file_name() {
                let name = name.to_string_lossy().into_owned();
                // OpenList IgnoreSystemFiles：系统目录 MKCOL 本地成功即可。
                if is_ignored_system_file(&name) {
                    debug!(path = %path.display(), "ignore system dir mkcol");
                    return Ok(());
                }
                self.dir_cache.invalidate(parent_path).await;
                self.drive
                    .create_folder(&parent_file.fid, &name)
                    .await
                    .map_err(|err| {
                        error!(path = %path.display(), error = %err, "create folder failed");
                        FsError::GeneralFailure
                    })?;
                // sleep 1s for quark server to update cache
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                self.dir_cache.invalidate(&path).await;
                self.dir_cache.invalidate_parent(&path).await;
                Ok(())
            } else {
                Err(FsError::Forbidden)
            }
        }
            .boxed()
    }


    fn remove_dir<'a>(&'a self, dav_path: &'a DavPath) -> FsFuture<'a, ()> {
        let path = self.normalize_dav_path(dav_path);
        debug!(path = %path.display(), "fs: remove_dir");
        async move {
            if self.read_only {
                return Err(FsError::Forbidden);
            }

            let file = self
                .get_file(path.clone())
                .await?
                .ok_or(FsError::NotFound)?;
            if !file.dir {
                return Err(FsError::Forbidden);
            }
            let mode = self.drive_mode();
            let path_str = path.display().to_string();
            match self.drive.remove_file(&file.fid, !self.no_trash).await {
                Ok(()) => {
                    self.oplog
                        .log_delete(&path_str, true, mode, "ok")
                        .await;
                }
                Err(err) => {
                    error!(path = %path.display(), error = %err, "remove directory failed");
                    self.oplog
                        .log_delete(&path_str, true, mode, &format!("error:{err:#}"))
                        .await;
                    return Err(FsError::GeneralFailure);
                }
            }
            // sleep 1s for quark server to update cache
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            self.dir_cache.invalidate(&path).await;
            self.dir_cache.invalidate_parent(&path).await;
            Ok(())
        }
            .boxed()
    }

    fn remove_file<'a>(&'a self, dav_path: &'a DavPath) -> FsFuture<'a, ()> {
        let path = self.normalize_dav_path(dav_path);
        debug!(path = %path.display(), "fs: remove_file");
        async move {
            if self.read_only {
                return Err(FsError::Forbidden);
            }

            let file = self
                .get_file(path.clone())
                .await?
                .ok_or(FsError::NotFound)?;
            if !file.file {
                return Err(FsError::Forbidden);
            }
            let mode = self.drive_mode();
            let path_str = path.display().to_string();
            match self.drive.remove_file(&file.fid, !self.no_trash).await {
                Ok(()) => {
                    self.oplog
                        .log_delete(&path_str, false, mode, "ok")
                        .await;
                }
                Err(err) => {
                    error!(path = %path.display(), error = %err, "remove file failed");
                    self.oplog
                        .log_delete(&path_str, false, mode, &format!("error:{err:#}"))
                        .await;
                    return Err(FsError::GeneralFailure);
                }
            }
            // sleep 1s for quark server to update cache
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            self.dir_cache.invalidate_parent(&path).await;
            Ok(())
        }
            .boxed()
    }

    fn copy<'a>(&'a self, from_dav: &'a DavPath, to_dav: &'a DavPath) -> FsFuture<'a, ()> {
        // not support by quark api
        async move {
            Err(FsError::NotImplemented)
        }.boxed()
    }

    fn rename<'a>(&'a self, from_dav: &'a DavPath, to_dav: &'a DavPath) -> FsFuture<'a, ()> {
        let from = self.normalize_dav_path(from_dav);
        let to = self.normalize_dav_path(to_dav);
        debug!(from = %from.display(), to = %to.display(), "fs: rename");
        async move {
            if self.read_only {
                return Err(FsError::Forbidden);
            }

            let is_dir;
            if from.parent() == to.parent() {
                // rename
                if let Some(name) = to.file_name() {
                    let file = self
                        .get_file(from.clone())
                        .await?
                        .ok_or(FsError::NotFound)?;
                    is_dir = file.dir;
                    let name = name.to_string_lossy().into_owned();
                    self.drive
                        .rename_file(&file.fid, &name)
                        .await
                        .map_err(|err| {
                            error!(from = %from.display(), to = %to.display(), error = %err, "rename file failed");
                            FsError::GeneralFailure
                        })?;
                    // sleep 1s for quark server to update cache
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    self.dir_cache.invalidate_parent(&from).await;
                } else {
                    return Err(FsError::Forbidden);
                }
            } else {
                // move
                let file = self
                    .get_file(from.clone())
                    .await?
                    .ok_or(FsError::NotFound)?;
                is_dir = file.dir;
                let to_parent_file = self
                    .get_file(to.parent().unwrap().to_path_buf())
                    .await?
                    .ok_or(FsError::NotFound)?;
                let new_name = to_dav.file_name();
                self.drive
                    .move_file(&file.fid, &to_parent_file.fid)
                    // then rename ...
                    .await
                    .map_err(|err| {
                        error!(from = %from.display(), to = %to.display(), error = %err, "move file failed");
                        FsError::GeneralFailure
                    })?;
                if let Some(to_name) = new_name {
                    if let Some(from_name) = from_dav.file_name(){
                        if from_name != to_name {
                            self.drive.rename_file(&file.fid, to_name)
                                .await
                                .map_err(|err| {
                                    error!(from = %from.display(), to = %to.display(), error = %err, "rename file after move failed");
                                    FsError::GeneralFailure
                                })?;
                        }
                    }
                }
                // sleep 1s for quark server to update cache
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                self.dir_cache.invalidate_parent(&from).await;
                self.dir_cache.invalidate_parent(&to).await;

            }


            // sleep 1s for quark server to update cache
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if is_dir {
                self.dir_cache.invalidate(&from).await;
            }
            self.dir_cache.invalidate_parent(&from).await;
            self.dir_cache.invalidate_parent(&to).await;
            Ok(())
        }
            .boxed()
    }

}

#[derive(Debug, Clone)]
struct UploadState {
    size: u64,
    buffer: BytesMut,
    chunk_count: u64,
    chunk_size: u64,
    chunk: u64,
    upload_id: String,
    upload_url: String,
    sha1: Option<String>,
    task_id: String,
    temp_file_path: String,
    /// true：内容在 memory_data；false：已落盘到 temp_file_path。
    use_memory: bool,
    memory_data: BytesMut,
    /// 已向 UploadMemoryPool 预留的字节，结束/溢出时归还。
    memory_reserved: u64,
    is_finished: bool,
    bucket: String,
    obj_key: String,
    mime_type: String,
    auth_info: String,
    callback: Option<Callback>,
    is_uploading: bool,
    flush_count: u32,
    /// 临时文件/上传占位已交给后台任务；客户端断开时 Drop 不得删文件或摘占位。
    handed_off: bool,
    /// 同路径已有上传在进行。这次 PUT 只丢掉正文，等原任务结束后再回复客户端。
    discard_duplicate: bool,
}

impl Default for UploadState {
    fn default() -> Self {
        Self {
            size: 0,
            buffer: BytesMut::new(),
            chunk_count: 0,
            chunk_size: 0,
            chunk: 1,
            upload_id: String::new(),
            upload_url: "".to_string(),
            sha1: None,
            task_id: "".to_string(),
            temp_file_path: "".to_string(),
            use_memory: false,
            memory_data: BytesMut::new(),
            memory_reserved: 0,
            is_finished: false,
            bucket: "".to_string(),
            obj_key: "".to_string(),
            mime_type: "application/octet-stream".to_string(),
            auth_info: "".to_string(),
            callback: None,
            is_uploading: false,
            flush_count: 0,
            handed_off: false,
            discard_duplicate: false,
        }
    }
}

struct QuarkDavFile {
    fs: QuarkDriveFileSystem,
    file: QuarkFile,
    parent_file_id: String,
    parent_dir: PathBuf,
    current_pos: u64,
    upload_state: UploadState,
    http_download: bool,
    /// 滑动窗口预取：多连接并发拉分块，绕开单流限速。
    prefetch: Option<crate::prefetch::Prefetcher>,
    md5_ctx: Md5Context,
    sha1_ctx: Sha1,
}

impl Debug for QuarkDavFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuarkDavFile")
            .field("file", &self.file)
            .field("parent_file_id", &self.parent_file_id)
            .field("current_pos", &self.current_pos)
            .field("upload_state", &self.upload_state)
            .finish()
    }
}

impl QuarkDavFile {

    fn new(
        fs: QuarkDriveFileSystem,
        file: QuarkFile,
        parent_file_id: String,
        parent_dir: PathBuf,
        size: u64,
        sha1: Option<String>,
    ) -> Self {
        Self {
            fs,
            file,
            parent_file_id,
            parent_dir,
            current_pos: 0,
            prefetch: None,
            upload_state: UploadState {
                size,
                sha1,
                ..Default::default()
            },
            http_download: false,
            md5_ctx: Md5Context::new(),
            sha1_ctx: Sha1::default(),
        }
    }

    async fn prepare_for_upload(&mut self) -> Result<bool, FsError> {
        if self.upload_state.is_finished {
            return Ok(false);
        }
        // OpenList IgnoreSystemFiles：CloudSync 的 .DS_Store/@eaDir 等本地吞掉，不上云。
        if is_ignored_system_file(&self.file.file_name) {
            debug!(file_name = %self.file.file_name, "ignore system file upload");
            if let Some(ref parent) = self.file.parent_path {
                self.fs
                    .remove_uploading_file(parent, &self.file.file_name);
            }
            self.upload_state.is_finished = true;
            return Ok(false);
        }
        if !self.upload_state.is_uploading {
            let path = self.upload_log_path();
            // 原任务已经在传夸克。重试 PUT 的正文不再落盘，避免再写一份 11GB，也不许改进度。
            if self.fs.upload_in_flight(&path) || self.fs.upload_tracker.is_uploading(&path) {
                info!(
                    path = %path,
                    file_name = %self.file.file_name,
                    "discard duplicate PUT body; original upload still running"
                );
                self.upload_state.discard_duplicate = true;
                self.upload_state.is_uploading = true;
                return Ok(true);
            }
            self.upload_state.is_uploading = true;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            self.upload_state.temp_file_path =
                format!("/tmp/{}_{}", timestamp, sanitize_temp_name(&self.file.file_name));
            // 内存池可用则先走内存；limit=0 或预留失败时仍可稍后在写入时落盘。
            self.upload_state.use_memory = self.fs.upload_memory.limit() > 0;
            self.fs
                .upload_tracker
                .start_receiving(&self.upload_log_path(), self.upload_state.size);
        }
        Ok(true)
    }

    /// 归还本上传占用的内存配额（溢出落盘 / 上传结束 / Drop 时调用）。
    fn release_upload_memory(&mut self) {
        let reserved = self.upload_state.memory_reserved;
        if reserved > 0 {
            self.fs.upload_memory.release(reserved);
            self.upload_state.memory_reserved = 0;
        }
        self.upload_state.use_memory = false;
        self.upload_state.memory_data.clear();
    }

    /// 把已在内存中的数据冲到临时文件，并释放配额。
    async fn spill_memory_to_temp(&mut self, extra: &[u8]) -> Result<(), FsError> {
        let temp_path = self.upload_state.temp_file_path.clone();
        if let Some(parent) = Path::new(&temp_path).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .await
            .map_err(|e| {
                error!(file_name = %self.file.file_name, error = %e, path = %temp_path, "spill: create temp failed");
                FsError::GeneralFailure
            })?;
        if !self.upload_state.memory_data.is_empty() {
            file.write_all(&self.upload_state.memory_data)
                .await
                .map_err(|e| {
                    error!(file_name = %self.file.file_name, error = %e, "spill: write memory failed");
                    FsError::GeneralFailure
                })?;
        }
        if !extra.is_empty() {
            file.write_all(extra).await.map_err(|e| {
                error!(file_name = %self.file.file_name, error = %e, "spill: write extra failed");
                FsError::GeneralFailure
            })?;
        }
        file.flush().await.map_err(|e| {
            error!(file_name = %self.file.file_name, error = %e, "spill: flush failed");
            FsError::GeneralFailure
        })?;
        debug!(
            file_name = %self.file.file_name,
            reserved = self.upload_state.memory_reserved,
            used = self.fs.upload_memory.used(),
            limit = self.fs.upload_memory.limit(),
            "upload spilled from memory to temp file"
        );
        self.release_upload_memory();
        Ok(())
    }

    /// 组装当前上传内容来源，供 Cookie / Open 上传读取。
    fn upload_source(&self) -> UploadSource {
        if self.upload_state.use_memory {
            UploadSource::Memory(self.upload_state.memory_data.clone().freeze())
        } else {
            UploadSource::Path(PathBuf::from(&self.upload_state.temp_file_path))
        }
    }

    /// 交接给后台任务时拿走内存，避免 clone 造成峰值翻倍。
    fn take_upload_source(&mut self) -> UploadSource {
        if self.upload_state.use_memory {
            let data = std::mem::take(&mut self.upload_state.memory_data);
            self.upload_state.use_memory = false;
            UploadSource::Memory(data.freeze())
        } else {
            UploadSource::Path(PathBuf::from(&self.upload_state.temp_file_path))
        }
    }

    /// 超过阈值则落盘：分片上传可走文件流，不把整片（甚至整文件）留在堆上。
    async fn ensure_spill_for_stream_upload(&mut self, size: u64) -> Result<(), FsError> {
        // 8MB：常见 API part_size；更大的文件即使接收池允许多，也改为流式。
        const SPILL_THRESHOLD: u64 = 8 * 1024 * 1024;
        if self.upload_state.use_memory && size > SPILL_THRESHOLD {
            self.spill_memory_to_temp(&[]).await?;
        }
        Ok(())
    }

    fn upload_log_path(&self) -> String {
        join_dav_path(
            self.file.parent_path.as_deref().unwrap_or(""),
            &self.file.file_name,
        )
    }

    /// 占位的 fid 是空的。重 PUT 前强制刷一次父目录，认领已经在网盘上的同名文件，
    /// 避免 same_path_reuse=false 再造出第二份。
    async fn bind_existing_remote(&mut self) {
        if !self.file.fid.is_empty() {
            return;
        }
        self.fs.dir_cache.invalidate(self.parent_dir.as_path()).await;
        let path = self.parent_dir.join(&self.file.file_name);
        let Ok(Some(existing)) = self.fs.find_in_cache(&path).await else {
            return;
        };
        if existing.fid.is_empty() {
            return;
        }
        info!(
            file_name = %self.file.file_name,
            fid = %existing.fid,
            size = existing.size,
            "adopt existing remote file before upload"
        );
        self.file.fid = existing.fid;
        self.file.size = existing.size;
        self.file.content_hash = existing.content_hash;
    }

    /// 丢弃客户端重试 PUT 产生的本地副本，并让本次 flush 成功返回（后台仍在传）。
    async fn abandon_duplicate_retry_upload(&mut self) -> Result<(), FsError> {
        self.upload_state.handed_off = false;
        self.delete_temp_file().await?;
        self.release_upload_memory();
        self.upload_state.is_finished = true;
        self.upload_state = UploadState::default();
        Ok(())
    }

    async fn log_upload_result(&self, size: u64, result: &str) {
        let path = self.upload_log_path();
        let status = if result.starts_with("error") {
            "error"
        } else if result.starts_with("skip") {
            "skip"
        } else {
            "ok"
        };
        self.fs.upload_tracker.finish(&path, status, result);
        self.fs
            .oplog
            .log_upload(&path, size, self.fs.drive_mode(), result)
            .await;
    }

    /// CloudSync 超时后会再 PUT 一遍。这次请求必须等到原上传真正结束再回复，
    /// 否则客户端先记成功，后台失败时不会再重试。
    async fn join_inflight_upload(&mut self) -> Result<(), FsError> {
        let path = self.upload_log_path();
        info!(
            path = %path,
            file_name = %self.file.file_name,
            "duplicate PUT waits for in-flight upload"
        );
        self.upload_state.handed_off = false;
        self.delete_temp_file().await?;
        self.release_upload_memory();
        let ok = if let Some(slot) = self.fs.inflight_slot(&path) {
            slot.wait().await
        } else {
            match self.fs.upload_tracker.status_of(&path).as_deref() {
                Some("ok") | Some("skip") => true,
                _ => false,
            }
        };
        self.upload_state.is_finished = true;
        self.upload_state.discard_duplicate = true;
        if ok {
            Ok(())
        } else {
            Err(FsError::GeneralFailure)
        }
    }

    async fn do_flush(&mut self) -> Result<(), FsError> {
        if self.upload_state.discard_duplicate {
            return self.join_inflight_upload().await;
        }
        let size = self.upload_state.size;
        let log_path_early = self.upload_log_path();
        // 接收阶段点了取消：不再开 OSS，直接收尾。
        if self.fs.upload_tracker.is_cancel_requested(&log_path_early) {
            info!(path = %log_path_early, "skip flush: upload cancelled");
            return self.abandon_duplicate_retry_upload().await;
        }
        if let Some(ref parent) = self.file.parent_path {
            self.fs
                .update_uploading_file(parent, &self.file.file_name, size);
        }

        // Compute final SHA-1 and MD5 (all data has been written)
        let sha1 = format!("{:x}", self.sha1_ctx.clone().finalize());
        let md5 = format!("{:x}", self.md5_ctx.clone().compute());
        let local_mtime_ms = self.fs.client_mtime_ms(&self.upload_log_path()).unwrap_or(0);

        // 占位没有 fid。先认领网盘上已有的同名文件，避免再传出第二份。
        self.bind_existing_remote().await;
        let mut old_fid = String::new();
        if !self.file.fid.is_empty() {
            let api_md5 = match self.fs.drive.get_file_md5(&self.file.fid).await {
                Ok(v) => v,
                Err(err) => {
                    debug!(file_name = %self.file.file_name, error = %err,
                           "failed to get cloud file md5, will not treat as content change");
                    None
                }
            };
            match decide_reupload(
                self.file.content_hash.as_deref(),
                api_md5.as_deref(),
                self.file.size,
                &sha1,
                &md5,
                size,
                self.fs.skip_upload_same_size,
            ) {
                ReuploadDecision::Skip(reason) => {
                    info!(file_name = %self.file.file_name, reason, size, "skip re-upload");
                    self.upload_state.is_finished = true;
                    self.log_upload_result(size, reason).await;
                    self.after_flush(true).await?;
                    return Ok(());
                }
                ReuploadDecision::Replace => {
                    old_fid = std::mem::take(&mut self.file.fid);
                }
            }
        }

        // Open：同名可以并存（same_path_reuse=false），新文件成功后再删旧的。
        // Cookie 同名会冲突，仍须先删；删失败就中止，避免脏数据。
        if self.fs.drive.is_open_mode() {
            return self.do_flush_open(&md5, &sha1, size, &old_fid, local_mtime_ms).await;
        }
        if !old_fid.is_empty() {
            if let Err(err) = self.fs.drive.remove_file(&old_fid, !self.fs.no_trash).await {
                error!(file_name = %self.file.file_name, error = %err,
                       "delete file before cookie upload failed");
                return Err(FsError::GeneralFailure);
            }
            old_fid.clear();
        }

        // 空文件走专用路径，避免 chunk_count=0。
        if size == 0 {
            return self.upload_mini_byte_file().await;
        }

        // 须在 up_pre 之前占坑：超时提前返回后客户端重 PUT 不得再开一条 multipart。
        let log_path = self.upload_log_path();
        let Some(inflight_guard) = self.fs.try_begin_upload(&log_path) else {
            info!(
                path = %log_path,
                file_name = %self.file.file_name,
                "skip duplicate cookie upload: already in flight"
            );
            return self.join_inflight_upload().await;
        };

        // up_pre
        let res = self
            .fs
            .drive
            .up_pre(&self.file.file_name, size, &self.parent_file_id, local_mtime_ms)
            .await
            .map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "create file with proof failed");
                FsError::GeneralFailure
            })?;

        if res.data.finish {
            // 秒传
            self.upload_state.is_finished = true;
            self.log_upload_result(size, "ok:rapid").await;
            self.after_flush(true).await?;
            return Ok(());
        }
        self.upload_state.auth_info = res.data.auth_info;
        self.upload_state.callback = Some(res.data.callback.clone());
        self.upload_state.task_id = res.data.task_id.clone();
        self.upload_state.upload_url =
            res.data.upload_url
                .strip_prefix("https://")
                .or_else(|| res.data.upload_url.strip_prefix("http://"))
                .unwrap_or(&res.data.upload_url)
                .to_string();
        self.upload_state.bucket = res.data.bucket;
        self.upload_state.obj_key = res.data.obj_key;
        if res.data.format_type != "" {
            self.upload_state.mime_type = res.data.format_type;
        }

        self.file.fid = res.data.fid.clone();

        self.upload_state.chunk_size = res.metadata.part_size;
        if res.metadata.part_size == 0 {
            error!(file_name = %self.file.file_name, "up_pre returned part_size=0");
            return Err(FsError::GeneralFailure);
        }
        let chunk_count =
            size / res.metadata.part_size + if size % res.metadata.part_size != 0 { 1 } else { 0 };
        self.upload_state.chunk_count = chunk_count;
        let Some(upload_id) = res.data.upload_id else {
            error!("create file with proof failed: missing upload_id");
            return Err(FsError::GeneralFailure);
        };
        self.upload_state.upload_id = upload_id;

        // up_hash (reuse already-computed md5 and sha1)
        let task_id = self.upload_state.task_id.clone();
        let res = self.fs.drive.up_hash(&md5, &sha1, &task_id).await.map_err(|err| {
            error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "hash file failed");
            FsError::GeneralFailure
        })?;
        if res.data.finish {
            self.upload_state.is_finished = true;
            self.log_upload_result(size, "ok:rapid").await;
            self.after_flush(true).await?;
            return Ok(());
        }
        // Spawn upload task so it won't be cancelled if client disconnects.
        // We still await the result — if the client stays connected, it gets the real result.
        // If the client disconnects (e.g. timeout), the spawned task continues uploading.
        let drive = self.fs.drive.clone();
        // 较大文件强制落盘，后续分片 PUT 走文件流，避免整片进内存。
        self.ensure_spill_for_stream_upload(size).await?;
        let memory_reserved = self.upload_state.memory_reserved;
        // 配额交由后台任务归还，避免 self 清空时重复 release。
        self.upload_state.memory_reserved = 0;
        let source = self.take_upload_source();
        let upload_state = self.upload_state.clone();
        let file_name = self.file.file_name.clone();
        let parent_path = self.file.parent_path.as_ref().unwrap().clone();
        let parent_dir = self.parent_dir.clone();
        let fs = self.fs.clone();
        let log_size = size;
        let log_mode = self.fs.drive_mode();
        // 后台任务拥有临时文件生命周期；避免客户端断连时 Drop 删掉还在读的文件。
        self.upload_state.handed_off = true;
        self.fs.upload_tracker.mark_uploading(&log_path, size);
        let tracker = self.fs.upload_tracker.clone();
        let progress_path = log_path.clone();

        let cancel_flag = self.fs.upload_tracker.arm_cancel(&log_path);
        let abort_path = log_path.clone();
        let new_fid = self.file.fid.clone();
        let handle = tokio::spawn(async move {
            // 路径锁留到任务结束（含等列表），挡住同路径的第二次 OSS。
            // 上传槽和快车道只包住真正传字节的那段，等列表不能把并发名额占住。
            let mut inflight_guard = inflight_guard;
            let temp_path = upload_state.temp_file_path.clone();
            let result = {
                let _upload_slot = crate::upload_limit::acquire_upload_slot().await;
                // 大文件 FIFO 快车道：队首全速，其余限速慢传且不可插队。
                let smart = crate::upload_schedule::join(size).await;
                let _path_bind = smart.bind_path(&progress_path);
                tracker.set_lane(&progress_path, smart.lane().as_str());
                let _memory_guard = UploadMemoryGuard {
                    pool: fs.upload_memory.clone(),
                    reserved: memory_reserved,
                };
                async {
                let chunk_size = upload_state.chunk_size as usize;
                let chunk_count = upload_state.chunk_count;
                let mut etags = vec![String::new(); chunk_count as usize];

                let mime_type = &upload_state.mime_type;
                let obj_key = &upload_state.obj_key;
                let bucket = &upload_state.bucket;
                let task_id = &upload_state.task_id;
                let upload_id = &upload_state.upload_id;
                let upload_url = &upload_state.upload_url;

                for chunk_idx in 1..=chunk_count {
                    if cancel_flag.load(std::sync::atomic::Ordering::Acquire) {
                        return Err(FsError::GeneralFailure);
                    }
                    let bytes_to_read = if chunk_idx == chunk_count {
                        let remaining_bytes =
                            upload_state.size as usize - ((chunk_idx - 1) as usize * chunk_size);
                        std::cmp::min(remaining_bytes, chunk_size)
                    } else {
                        chunk_size
                    };
                    let offset = (chunk_idx - 1) * chunk_size as u64;
                    let buf = read_upload_slice(&source, offset, bytes_to_read)
                        .await
                        .map_err(|e| {
                            error!(file_name = %file_name, error = %e, "read upload slice failed");
                            FsError::GeneralFailure
                        })?;
                    let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
                    let utc_time = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
                    let auth_meta = drive
                        .up_part_auth_meta(
                            mime_type,
                            &utc_time,
                            bucket,
                            obj_key,
                            chunk_idx as u32,
                            upload_id,
                        )
                        .await
                        .map_err(|err| {
                            error!(file_name = %file_name, error = %err, "get upload part auth meta failed");
                            FsError::GeneralFailure
                        })?;
                    let auth_info = &upload_state.auth_info;
                    let auth_res = drive.auth(auth_info, &auth_meta, task_id).await.map_err(|err| {
                        error!(file_name = %file_name, error = %err, "auth upload part failed");
                        FsError::GeneralFailure
                    })?;
                    let up_req = UpPartMethodRequest {
                        auth_key: auth_res.data.auth_key,
                        mime_type: upload_state.mime_type.clone(),
                        utc_time,
                        bucket: bucket.clone(),
                        upload_url: upload_url.clone(),
                        obj_key: obj_key.clone(),
                        part_number: chunk_idx as u32,
                        upload_id: upload_id.to_string(),
                        part_bytes: buf,
                    };
                    let put_started = std::time::Instant::now();
                    let res = drive.up_part(up_req).await.map_err(|err| {
                        error!(file_name = %file_name, error = %err, "upload chunk failed");
                        FsError::GeneralFailure
                    })?;
                    let put_ms = put_started.elapsed().as_millis() as u64;
                    let etag_from_up_part = res.ok_or_else(|| {
                        error!(file_name = %file_name, "upload chunk returned empty etag");
                        FsError::GeneralFailure
                    })?;
                    if etag_from_up_part == "finish" {
                        return Ok(true);
                    }
                    etags[(chunk_idx - 1) as usize] = etag_from_up_part;
                    tracker.add_uploaded(&progress_path, bytes_to_read as u64, put_ms);
                    // 慢传 sleep 放在计量之后，速度反映实际上行。
                    tracker.set_lane(&progress_path, smart.lane().as_str());
                    smart.pace(bytes_to_read as u64).await;
                }

                let callback = upload_state.callback.clone().ok_or_else(|| {
                    error!(file_name = %file_name, "missing upload callback");
                    FsError::GeneralFailure
                })?;
                let commit_req = UpAuthAndCommitRequest {
                    md5s: etags,
                    callback,
                    bucket: bucket.clone(),
                    obj_key: obj_key.clone(),
                    upload_id: upload_id.clone(),
                    auth_info: upload_state.auth_info.clone(),
                    task_id: task_id.clone(),
                    upload_url: upload_url.clone(),
                };
                drive.up_auth_and_commit(commit_req).await.map_err(|err| {
                    error!(file_name = %file_name, error = %err, "commit upload failed");
                    FsError::GeneralFailure
                })?;
                drive.finish(obj_key, task_id).await.map_err(|err| {
                    error!(file_name = %file_name, error = %err, "finish upload failed");
                    FsError::GeneralFailure
                })?;
                Ok(false)
                }
                .await
            };

            if !temp_path.is_empty() {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }

            let cancelled = cancel_flag.load(std::sync::atomic::Ordering::Acquire);
            let client_result = if cancelled {
                // request_cancel 已写入 cancelled；此处只做清理。
                fs.remove_uploading_file(&parent_path, &file_name);
                fs.oplog
                    .log_upload(&log_path, log_size, log_mode, "cancelled")
                    .await;
                Ok(())
            } else {
                match result {
                    Ok(_) => {
                        // Cookie 在 up_pre 前已删旧文件，这里 old_fid 为空。
                        fs.complete_background_upload(
                            &tracker,
                            &progress_path,
                            parent_dir.as_path(),
                            &file_name,
                            log_size,
                            &parent_path,
                            &log_path,
                            log_mode,
                            "",
                        )
                        .await;
                        Ok(())
                    }
                    Err(err) => {
                        // 分片失败时丢掉这次预创建的文件，避免残片挡下一次重试。
                        if !new_fid.is_empty() {
                            if let Err(del_err) = fs.drive.remove_file(&new_fid, !fs.no_trash).await {
                                debug!(fid = %new_fid, error = %del_err, "cleanup incomplete cookie upload failed");
                            }
                        }
                        fs.fail_background_upload(
                            &tracker,
                            &progress_path,
                            parent_dir.as_path(),
                            &file_name,
                            &parent_path,
                            &log_path,
                            log_size,
                            log_mode,
                            "上传失败",
                        )
                        .await;
                        Err(err)
                    }
                }
            };
            // 先标结果再丢锁，重试 PUT 才能等到和这次一样的成败。
            inflight_guard.settle(client_result.is_ok());
            client_result
        });
        self.fs
            .upload_tracker
            .bind_abort(&abort_path, handle.abort_handle());

        // Wait for upload to complete, but return early if upload_wait_timeout is reached
        // to avoid client timeout. The spawned task continues uploading in the background.
        let upload_wait_timeout = self.fs.upload_wait_timeout;
        if upload_wait_timeout > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(upload_wait_timeout),
                handle,
            ).await {
                Ok(result) => match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err),
                    Err(join_err) if join_err.is_cancelled() => {}
                    Err(err) => {
                        error!(file_name = %self.file.file_name, error = %err, "upload task join failed");
                        return Err(FsError::GeneralFailure);
                    }
                },
                Err(_) => {
                    // Timeout reached, upload continues in background；占位仍在 uploading。
                    info!(file_name = %self.file.file_name, timeout_secs = upload_wait_timeout,
                          "upload still in progress, returning early to avoid client timeout");
                }
            }
        } else {
            // Wait indefinitely；控制台取消会 abort，按成功收尾避免误报失败。
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(join_err) if join_err.is_cancelled() => {}
                Err(err) => {
                    error!(file_name = %self.file.file_name, error = %err, "upload task join failed");
                    return Err(FsError::GeneralFailure);
                }
            }
        }

        self.upload_state.is_finished = true;
        self.upload_state = UploadState::default();
        Ok(())
    }


    /// Open 模式上传：内容已在内存或临时文件，整段交给 drive。
    /// `old_fid` 非空表示同名旧文件先留着，新文件成功后再删。
    async fn do_flush_open(
        &mut self,
        md5: &str,
        sha1: &str,
        size: u64,
        old_fid: &str,
        local_mtime_ms: u64,
    ) -> Result<(), FsError> {
        let log_path = self.upload_log_path();
        // 与 Cookie 路径相同：提前返回后客户端重试时勿再开一条 Open 上传。
        let Some(inflight_guard) = self.fs.try_begin_upload(&log_path) else {
            info!(
                path = %log_path,
                file_name = %self.file.file_name,
                "skip duplicate open upload: already in flight"
            );
            return self.join_inflight_upload().await;
        };

        // 空上传且既无内存也无文件时，用空内存源即可。
        if size == 0 && self.upload_state.use_memory && self.upload_state.memory_data.is_empty() {
            // keep empty memory source
        } else if !self.upload_state.use_memory {
            let temp_path = self.upload_state.temp_file_path.clone();
            if temp_path.is_empty() || !Path::new(&temp_path).exists() {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let path = if temp_path.is_empty() {
                    format!(
                        "/tmp/{}_{}",
                        timestamp,
                        sanitize_temp_name(&self.file.file_name)
                    )
                } else {
                    temp_path
                };
                if let Some(parent) = Path::new(&path).parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                tokio::fs::write(&path, b"").await.map_err(|e| {
                    error!(file_name = %self.file.file_name, error = %e, "create empty temp file failed");
                    FsError::GeneralFailure
                })?;
                self.upload_state.temp_file_path = path;
            }
        }

        // Open 大文件单片可达数百 MB：先落盘再流式 PUT，低内存也能传完。
        self.ensure_spill_for_stream_upload(size).await?;
        let drive = self.fs.drive.clone();
        let file_name = self.file.file_name.clone();
        let parent_file_id = self.parent_file_id.clone();
        let parent_path = self.file.parent_path.as_ref().unwrap().clone();
        let parent_dir = self.parent_dir.clone();
        let fs = self.fs.clone();
        let memory_reserved = self.upload_state.memory_reserved;
        self.upload_state.memory_reserved = 0;
        let source = self.take_upload_source();
        let temp_path = self.upload_state.temp_file_path.clone();
        let md5 = md5.to_string();
        let sha1 = sha1.to_string();
        let log_mode = self.fs.drive_mode();
        // 后台任务拥有临时文件生命周期；避免客户端断连时 Drop 删掉还在读的文件。
        self.upload_state.handed_off = true;
        self.fs.upload_tracker.mark_uploading(&log_path, size);
        let tracker = self.fs.upload_tracker.clone();
        let progress_path = log_path.clone();
        let cancel_flag = self.fs.upload_tracker.arm_cancel(&log_path);
        let abort_path = log_path.clone();
        let old_fid = old_fid.to_string();

        let handle = tokio::spawn(async move {
            // 同路径锁一直握到等列表结束，避免 CloudSync 超时重 PUT 再开一条 OSS。
            // 并发槽和快车道在字节传完就放开：等列表最多 90 秒，不能把后面的任务堵住。
            let mut inflight_guard = inflight_guard;
            let result = {
                let _upload_slot = crate::upload_limit::acquire_upload_slot().await;
                let smart = crate::upload_schedule::join(size).await;
                let _path_bind = smart.bind_path(&progress_path);
                tracker.set_lane(&progress_path, smart.lane().as_str());
                let _memory_guard = UploadMemoryGuard {
                    pool: fs.upload_memory.clone(),
                    reserved: memory_reserved,
                };
                drive
                    .upload_open_file(
                        &file_name,
                        size,
                        &parent_file_id,
                        &md5,
                        &sha1,
                        &source,
                        Some((&tracker, progress_path.as_str())),
                        Some(&smart),
                        Some(cancel_flag.as_ref()),
                        local_mtime_ms,
                        &old_fid,
                    )
                    .await
            };
            if !temp_path.is_empty() {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }
            let cancelled = cancel_flag.load(std::sync::atomic::Ordering::Acquire);
            let client_result = if cancelled {
                fs.remove_uploading_file(&parent_path, &file_name);
                fs.oplog
                    .log_upload(&log_path, size, log_mode, "cancelled")
                    .await;
                Ok(())
            } else {
                match result {
                    Ok(()) => {
                        fs.complete_background_upload(
                            &tracker,
                            &progress_path,
                            parent_dir.as_path(),
                            &file_name,
                            size,
                            &parent_path,
                            &log_path,
                            log_mode,
                            &old_fid,
                        )
                        .await;
                        Ok(())
                    }
                    Err(err) => {
                        error!(file_name = %file_name, error = %err, "open upload failed");
                        let detail = flatten_error(&format!("{err:#}"));
                        fs.fail_background_upload(
                            &tracker,
                            &progress_path,
                            parent_dir.as_path(),
                            &file_name,
                            &parent_path,
                            &log_path,
                            size,
                            log_mode,
                            &detail,
                        )
                        .await;
                        Err(FsError::GeneralFailure)
                    }
                }
            };
            inflight_guard.settle(client_result.is_ok());
            client_result
        });
        self.fs
            .upload_tracker
            .bind_abort(&abort_path, handle.abort_handle());

        let upload_wait_timeout = self.fs.upload_wait_timeout;
        if upload_wait_timeout > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(upload_wait_timeout),
                handle,
            )
            .await
            {
                Ok(result) => match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err),
                    Err(join_err) if join_err.is_cancelled() => {}
                    Err(err) => {
                        error!(file_name = %self.file.file_name, error = %err, "open upload task join failed");
                        return Err(FsError::GeneralFailure);
                    }
                },
                Err(_) => {
                    info!(file_name = %self.file.file_name, timeout_secs = upload_wait_timeout,
                          "open upload still in progress, returning early to avoid client timeout");
                }
            }
        } else {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(join_err) if join_err.is_cancelled() => {}
                Err(err) => {
                    error!(file_name = %self.file.file_name, error = %err, "open upload task join failed");
                    return Err(FsError::GeneralFailure);
                }
            }
        }

        self.upload_state.is_finished = true;
        self.upload_state = UploadState::default();
        Ok(())
    }

    async fn upload_mini_byte_file(&mut self) -> Result<(), FsError> {
        // Empty file MD5
        let empty_md5 = "d41d8cd98f00b204e9800998ecf8427e";
        let empty_sha1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

        // If old file exists, compare hash before deleting
        if !self.file.fid.is_empty() {
            match self.fs.drive.get_file_md5(&self.file.fid).await {
                Ok(Some(cloud_md5)) if cloud_md5.eq_ignore_ascii_case(empty_md5) => {
                    debug!(file_name = %self.file.file_name,
                           "skip uploading: empty file content hash unchanged");
                    self.upload_state.is_finished = true;
                    self.after_flush(true).await?;
                    return Ok(());
                }
                Ok(_) => {}
                Err(err) => {
                    debug!(file_name = %self.file.file_name, error = %err,
                           "failed to get cloud file md5, proceeding with upload");
                }
            }
            if let Err(err) = self.fs.drive
                .remove_file(&self.file.fid, !self.fs.no_trash).await
            {
                error!(file_name = %self.file.file_name, error = %err,
                       "delete file before upload failed");
                return Err(FsError::GeneralFailure);
            }
            self.file.fid.clear();
        }

        if self.fs.drive.is_open_mode() {
            return self.do_flush_open(
                empty_md5,
                empty_sha1,
                0,
                "",
                self.fs.client_mtime_ms(&self.upload_log_path()).unwrap_or(0),
            ).await;
        }

        // pre -> hash -> commit -> finish
        // up_pre
        let res = self
            .fs
            .drive
            .up_pre(
                &self.file.file_name,
                0,
                &self.parent_file_id,
                self.fs.client_mtime_ms(&self.upload_log_path()).unwrap_or(0),
            )
            .await
            .map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "create file with proof failed");
                FsError::GeneralFailure
            })?;

        if res.data.finish {
            // 秒传
            self.upload_state.is_finished = true;
            self.after_flush(true).await?;
            return Ok(());
        }
        self.upload_state.auth_info = res.data.auth_info;
        self.upload_state.callback = Some(res.data.callback.clone());
        self.upload_state.task_id = res.data.task_id.clone();
        self.upload_state.upload_url =
            res.data.upload_url
                .strip_prefix("https://")
                .or_else(|| res.data.upload_url.strip_prefix("http://"))
                .unwrap_or(&res.data.upload_url)
                .to_string();
        self.upload_state.bucket = res.data.bucket;
        self.upload_state.obj_key = res.data.obj_key;
        if res.data.format_type != "" {
            self.upload_state.mime_type = res.data.format_type;
        }

        self.file.fid = res.data.fid.clone();

        self.upload_state.chunk_size = 0;
        let chunk_count = 1 ;
        self.upload_state.chunk_count = chunk_count;
        let Some(upload_id) = res.data.upload_id else {
            error!("create file with proof failed: missing upload_id");
            return Err(FsError::GeneralFailure);
        };
        self.upload_state.upload_id = upload_id;

        // unHash
        let md5 = "d41d8cd98f00b204e9800998ecf8427e";
        let sha1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let task_id = self.upload_state.task_id.clone();
        let res = self.fs.drive.up_hash(&md5, &sha1, &task_id).await.map_err(|err| {
            error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "hash file failed");
            FsError::GeneralFailure
        })?;
        if res.data.finish {
            self.upload_state.is_finished = true;
            self.after_flush(true).await?;
            return Ok(());
        }
        // 空文件优先走内存源，避免额外落盘。
        self.upload_state.use_memory = true;
        self.upload_state.memory_data.clear();
        self.upload_state.size = 0;
        self.upload_state.temp_file_path.clear();
        self.upload_chunk().await?;
        self.log_upload_result(0, "ok").await;
        self.after_flush(true).await?;

        Ok(())
    }


    async fn consume_buf(&mut self) -> Result<(), FsError> {
        // 流式：不 clone digest、不 to_vec；边写边更新 md5/sha1。
        let bytes = self.upload_state.buffer.split().freeze();
        if bytes.is_empty() {
            return Ok(());
        }
        let add = bytes.len() as u64;

        // 先落盘/入内存成功后再累加 size，避免失败后 size 虚高导致读切片越界。
        if self.upload_state.use_memory {
            if self.fs.upload_memory.try_reserve(add) {
                self.upload_state.memory_reserved += add;
                self.upload_state.memory_data.extend_from_slice(&bytes);
            } else {
                self.spill_memory_to_temp(&bytes).await?;
            }
        } else {
            let temp_path = self.upload_state.temp_file_path.clone();
            if let Some(parent) = Path::new(&temp_path).parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    error!("create_dir_all failed: {}, path: {:?}", e, parent);
                }
            }
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&temp_path)
                .await
                .map_err(|e| {
                    error!("failed to open file: {}, {}", temp_path, e);
                    FsError::GeneralFailure
                })?;
            file.write_all(&bytes).await.map_err(|e| {
                error!(file_name = %self.file.file_name, error = %e, "write to temp file failed");
                FsError::GeneralFailure
            })?;
            file.flush().await.map_err(|e| {
                error!(file_name = %self.file.file_name, error = %e, "flush temp file failed");
                FsError::GeneralFailure
            })?;
        }

        self.upload_state.size += add;
        self.md5_ctx.consume(&bytes);
        self.sha1_ctx.update(&bytes);
        self.fs.upload_tracker.set_received(
            &self.upload_log_path(),
            self.upload_state.size,
            self.upload_state.size,
        );
        Ok(())
    }

    async fn upload_chunk(&mut self) -> Result<(), FsError> {
        let chunk_size = self.upload_state.chunk_size as usize;
        let source = self.upload_source();
        let chunk_count = self.upload_state.chunk_count;
        let mut etags = vec![String::new(); chunk_count as usize];
        let mime_type = &self.upload_state.mime_type;
        let obj_key = &self.upload_state.obj_key;
        let bucket = &self.upload_state.bucket;
        let task_id = &self.upload_state.task_id;
        let upload_id = &self.upload_state.upload_id;
        let upload_url = &self.upload_state.upload_url;

        for chunk_idx in 1..=chunk_count {
            let bytes_to_read = if chunk_idx == chunk_count {
                let remaining_bytes =
                    self.upload_state.size as usize - ((chunk_idx - 1) as usize * chunk_size);
                std::cmp::min(remaining_bytes, chunk_size)
            } else {
                chunk_size
            };
            let offset = (chunk_idx - 1) * chunk_size as u64;
            let buf = read_upload_slice(&source, offset, bytes_to_read)
                .await
                .map_err(|e| {
                    error!(file_name = %self.file.file_name, error = %e, "read upload slice failed");
                    FsError::GeneralFailure
                })?;
            let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
            let utc_time = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            let auth_meta = self
                .fs
                .drive
                .up_part_auth_meta(mime_type, &utc_time, bucket, obj_key, chunk_idx as u32, upload_id)
                .await
                .map_err(|err| {
                    error!(file_name = %self.file.file_name, error = %err, "get upload part auth meta failed");
                    FsError::GeneralFailure
                })?;
            let auth_info = &self.upload_state.auth_info;
            let auth_res = self
                .fs
                .drive
                .auth(auth_info, &auth_meta, task_id)
                .await
                .map_err(|err| {
                    error!(file_name = %self.file.file_name, error = %err, "auth upload part failed");
                    FsError::GeneralFailure
                })?;
            let up_req = UpPartMethodRequest {
                auth_key: auth_res.data.auth_key,
                mime_type: self.upload_state.mime_type.clone(),
                utc_time,
                bucket: bucket.clone(),
                upload_url: upload_url.clone(),
                obj_key: obj_key.clone(),
                part_number: chunk_idx as u32,
                upload_id: upload_id.to_string(),
                part_bytes: buf,
            };
            let res = self.fs.drive.up_part(up_req).await.map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "upload chunk failed");
                FsError::GeneralFailure
            })?;
            let etag_from_up_part = res.ok_or_else(|| {
                error!(file_name = %self.file.file_name, "upload chunk returned empty etag");
                FsError::GeneralFailure
            })?;
            if etag_from_up_part == "finish" {
                return Ok(());
            }
            etags[(chunk_idx - 1) as usize] = etag_from_up_part;
        }
        let callback = self.upload_state.callback.clone().ok_or_else(|| {
            error!(file_name = %self.file.file_name, "missing upload callback");
            FsError::GeneralFailure
        })?;

        let auth_info = &self.upload_state.auth_info;
        let commit_req = UpAuthAndCommitRequest{
            md5s: etags.clone(),
            callback: callback,
            bucket: bucket.clone(),
            obj_key: obj_key.clone(),
            upload_id: upload_id.clone(),
            auth_info: auth_info.clone(),
            task_id: task_id.clone(),
            upload_url: upload_url.clone(),
        };
        // commit
        self.fs.drive.up_auth_and_commit(commit_req).await.map_err(|err| {
            error!(file_name = %self.file.file_name, error = %err, "commit upload failed");
            FsError::GeneralFailure
        })?;
        // finish upload
        self.fs.drive.finish(&obj_key, &task_id).await.map_err(|err| {
            error!(file_name = %self.file.file_name, error = %err, "finish upload failed");
            FsError::GeneralFailure
        })?;

        Ok(())
    }

    async fn delete_temp_file(&self) -> Result<(), FsError> {
        // 已交接给后台任务时由任务自己删，避免与流式读取竞态。
        if self.upload_state.handed_off {
            return Ok(());
        }
        let temp_path = &self.upload_state.temp_file_path;
        if tokio::fs::metadata(temp_path).await.is_ok() {
            if let Err(err) = tokio::fs::remove_file(temp_path).await {
                error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "remove temp file failed");
            }
        }
        Ok(())
    }

    /// `ok=true`：等远程列表可见（CloudSync PUT 后立刻 PROPFIND）。
    /// `ok=false`：失败清理，尽快摘掉 uploading 占位。
    async fn after_flush(&mut self, ok: bool) -> Result<(), FsError> {
        self.delete_temp_file().await?;
        self.release_upload_memory();
        let parent_path = self.file.parent_path.as_ref().unwrap().clone();
        // 后台任务已负责 wait_until / 失败摘占位；此处避免重复或抢删。
        if self.upload_state.handed_off {
            return Ok(());
        }
        if ok {
            let size = if self.upload_state.size > 0 {
                self.upload_state.size
            } else {
                self.file.size
            };
            self.fs
                .wait_until_remote_visible(
                    self.parent_dir.as_path(),
                    &self.file.file_name,
                    size,
                    &parent_path,
                )
                .await;
        } else {
            self.fs
                .remove_uploading_file(&parent_path, &self.file.file_name);
            self.fs.dir_cache.invalidate(self.parent_dir.as_path()).await;
        }
        self.upload_state.is_finished = true;
        self.upload_state = UploadState::default();
        Ok(())
    }

    async fn get_download_url(&self) -> Result<String, FsError> {
        self.fs.drive.get_download_url(&self.file.fid).await.map_err(|err| {
            error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "get download url failed");
            FsError::GeneralFailure
        })
    }

}

impl DavFile for QuarkDavFile {
    fn metadata(&'_ mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: metadata");
        async move {
            let file = self.file.clone();
            Ok(Box::new(file) as Box<dyn DavMetaData>)
        }
            .boxed()
    }

    fn redirect_url(&mut self) -> FsFuture<Option<String>> {
        debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: redirect_url");
        async move {
            if self.file.fid.is_empty() {
                return Err(FsError::NotFound);
            }
            // Open 直链必须带 x_pan_* Cookie，客户端跟 302 拿不到 → 禁止直跳，走代理 Range。
            if self.fs.drive.is_open_mode() {
                return Ok(None);
            }
            let download_url = self
                .fs
                .drive
                .get_download_url(&self.file.fid)
                .await
                .map_err(|err| {
                    error!(
                        file_id = %self.file.fid,
                        error = %err,
                        "get download url for redirect failed"
                    );
                    FsError::GeneralFailure
                })?;
            Ok(Some(download_url))
        }
        .boxed()
    }



    fn seek(&mut self, pos: SeekFrom) -> FsFuture<u64> {
        debug!(
            file_id = %self.file.fid,
            file_name = %self.file.file_name,
            pos = ?pos,
            "file: seek"
        );
        async move {
            let new_pos = match pos {
                SeekFrom::Start(pos) => pos,
                SeekFrom::End(pos) => (self.file.size as i64 + pos) as u64,
                SeekFrom::Current(size) => self.current_pos + size as u64,
            };
            self.current_pos = new_pos;
            Ok(new_pos)
        }
            .boxed()
    }

    /// write file : open -> metadata -> flush -> write_buf/write_byte -> flush
    fn write_buf(&mut self, mut buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()>{
        debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: write_buf");
        async move {
            if !self.prepare_for_upload().await? {
                return Ok(());
            }
            // 原上传还在进行：把重试正文丢掉，等 flush 时再跟原任务的结果。
            if self.upload_state.discard_duplicate {
                return Ok(());
            }
            // 大块写入按 upload_buffer_size 切片消费，边写边哈希，避免巨型 buffer 停留。
            let cap = self.fs.upload_buffer_size.max(64 * 1024);
            while buf.has_remaining() {
                let space = cap
                    .saturating_sub(self.upload_state.buffer.len())
                    .max(1);
                let chunk = buf.chunk();
                let take = chunk.len().min(space);
                self.upload_state.buffer.extend_from_slice(&chunk[..take]);
                buf.advance(take);
                if self.upload_state.buffer.len() >= cap {
                    self.consume_buf().await?;
                }
            }
            if !self.upload_state.buffer.is_empty() {
                self.consume_buf().await?;
            }
            Ok(())
        }
            .boxed()
    }


    fn write_bytes(&mut self, buf: bytes::Bytes) -> FsFuture<()> {
        let buf: Box<dyn Buf + Send> = Box::new(buf);
        self.write_buf(buf)
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<Bytes> {
        debug!(
            file_id = %self.file.fid,
            file_name = %self.file.file_name,
            pos = self.current_pos,
            count = count,
            size = self.file.size,
            "file: read_bytes",
        );
        async move {
            if self.file.fid.is_empty() {
                // upload in progress
                return Err(FsError::NotFound);
            }
            // 检查现有 URL 是否有效
            let is_valid = self.file.download_url.as_ref()
                .map(|url| !is_url_expired(url))
                .unwrap_or(false);

            if !is_valid {
                let new_url = self.get_download_url().await.unwrap();
                self.file.download_url = Some(new_url);
            }
            let download_url = match self.file.download_url.as_ref() {
                Some(url) => url,
                None => {
                    // 详细记录文件信息
                    error!(
                        "文件缺少下载URL: {:?}\n文件元数据: {:#?}",
                        self.file.download_url,
                        self.file);
                    return Err(dav_server::fs::FsError::NotFound);
                }
            };

            if download_url.is_empty() {
                return Err(FsError::NotFound);
            }

            // 预取窗口并发拉分块；单流串行会被夸克压在约 10 Mbps。
            let url = download_url.clone();
            let size = self.file.size;
            let pos = self.current_pos;

            let usable = self
                .prefetch
                .as_ref()
                .map(|p| p.serves(&url, &self.file.fid, pos))
                .unwrap_or(false);
            if !usable {
                self.prefetch = Some(crate::prefetch::Prefetcher::new(
                    self.fs.drive.clone(),
                    url,
                    self.file.fid.clone(),
                    self.fs.chunk_cache.clone(),
                    size,
                    pos,
                ));
            } else {
                self.prefetch.as_mut().unwrap().set_url(url);
            }

            let got = self.prefetch.as_mut().unwrap().read(pos, count).await;
            match got {
                Some(content) if !content.is_empty() => {
                    self.current_pos += content.len() as u64;
                    Ok(content)
                }
                _ => {
                    warn!(pos = pos, "prefetch miss, falling back to direct range read");
                    self.prefetch = None;
                    // 与 Cookie 一致：miss 不先清 URL（Open 先前误清会强制再打 API+302）。
                    // 仅真实下载失败时再清。
                    let fallback_url = if let Some(url) = self.file.download_url.clone() {
                        url
                    } else {
                        self.get_download_url().await?
                    };
                    self.file.download_url = Some(fallback_url.clone());
                    let content = self
                        .fs
                        .drive
                        .download(fallback_url, Some((pos, count)))
                        .await
                        .map_err(|err| {
                            error!(
                                file_id = %self.file.fid,
                                pos = pos,
                                error = %err,
                                "fallback download failed"
                            );
                            self.file.download_url = None;
                            FsError::NotFound
                        })?;
                    self.current_pos += content.len() as u64;
                    Ok(content)
                }
            }
        }
            .boxed()
    }

    fn flush(&mut self) -> FsFuture<()> {
        debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: flush");
        async move {
            if self.upload_state.is_finished {
                debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: flush - already finished");
                return Ok(());
            }

            // 无任何 write 的 flush = 0 字节上传（WebDAV 常见）。
            if !self.upload_state.is_uploading {
                debug!(
                    file_id = %self.file.fid,
                    file_name = %self.file.file_name,
                    "file: flush - empty upload"
                );
                if !self.prepare_for_upload().await? {
                    return Ok(());
                }
                let res = self.upload_mini_byte_file().await;
                if let Err(err) = res {
                    error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "empty flush failed");
                    self.after_flush(false).await?;
                    return Err(err);
                }
                return Ok(());
            }

            let res = self.do_flush().await;
            if let Err(err) = res {
                error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "file: flush failed");
                self.after_flush(false).await?;
                return Err(err);
            }
            Ok(())
        }.boxed()

    }
}

impl Drop for QuarkDavFile {
    /// 客户端中断上传时归还内存配额，并尽量清掉临时文件 / uploading 幽灵项。
    /// 若已 handoff 给后台任务，则不得碰临时文件与占位（否则流式 PUT 会 ENOENT）。
    fn drop(&mut self) {
        // 重试 PUT 和原任务共用占位。这里断开不能把正在传的占位摘掉。
        if self.upload_state.discard_duplicate {
            self.release_upload_memory();
            return;
        }
        if !self.upload_state.handed_off {
            self.release_upload_memory();
        }
        if self.upload_state.is_uploading
            && !self.upload_state.is_finished
            && !self.upload_state.handed_off
        {
            let temp = self.upload_state.temp_file_path.clone();
            let parent = self.file.parent_path.clone();
            let name = self.file.file_name.clone();
            let fs = self.fs.clone();
            if let Some(parent) = parent {
                tokio::spawn(async move {
                    if !temp.is_empty() {
                        let _ = tokio::fs::remove_file(&temp).await;
                    }
                    fs.remove_uploading_file(&parent, &name);
                });
            }
        }
    }
}

/// 后台上传任务结束时自动归还内存池占用。
struct UploadMemoryGuard {
    pool: Arc<UploadMemoryPool>,
    reserved: u64,
}

impl Drop for UploadMemoryGuard {
    fn drop(&mut self) {
        self.pool.release(self.reserved);
    }
}

/// 父目录 + 文件名，根目录下不要拼成 `//name`。
fn join_dav_path(parent: &str, name: &str) -> String {
    if parent.is_empty() || parent == "/" {
        format!("/{name}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), name)
    }
}

/// 把错误收成一行，历史表才能显示分片号和 HTTP 状态，而不是被空格截断。
fn flatten_error(msg: &str) -> String {
    let flat = msg.split_whitespace().collect::<Vec<_>>().join(" ");
    // 操作日志一行展示；更长的错误链仍在控制台 error 日志里。
    flat.chars().take(240).collect()
}

/// 解析群晖 / ownCloud 的 `X-OC-Mtime`。秒或毫秒都收成 unix 毫秒。
pub(crate) fn parse_client_mtime_ms(value: &str) -> Option<u64> {
    let secs: f64 = value.trim().parse().ok()?;
    if !secs.is_finite() || secs <= 0.0 {
        return None;
    }
    // 1e12 以上是毫秒时间戳；群晖 X-OC-Mtime 是 unix 秒。
    if secs >= 1e12 {
        Some(secs as u64)
    } else {
        Some((secs * 1000.0) as u64)
    }
}

/// 同名文件已在网盘上时，要不要再整份上传。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReuploadDecision {
    /// 结果字段，写入操作日志（skip:hash / skip:same_size）。
    Skip(&'static str),
    Replace,
}

/// CloudSync 对账失败会把同一个大文件再 PUT 一次。
/// Open 模式拿不到 md5，不能因为「对不上 hash」就删掉已传好的文件再传 11GB。
fn decide_reupload(
    cloud_hash: Option<&str>,
    api_md5: Option<&str>,
    cloud_size: u64,
    sha1: &str,
    md5: &str,
    local_size: u64,
    force_same_size: bool,
) -> ReuploadDecision {
    if hash_matches(cloud_hash, sha1, md5) || hash_matches(api_md5, sha1, md5) {
        return ReuploadDecision::Skip("skip:hash");
    }
    // 显式开关：只看大小，即使摘要对不上也跳过。
    if force_same_size && local_size > 0 && cloud_size == local_size {
        return ReuploadDecision::Skip("skip:same_size");
    }
    if let Some(h) = api_md5 {
        if is_hex_digest(h, 32) {
            return ReuploadDecision::Replace;
        }
    }
    if let Some(h) = cloud_hash {
        // 只有标准 md5/sha1 对不上才认定内容变了；其它 hash 算法不能当「内容不同」。
        if is_hex_digest(h, 32) || is_hex_digest(h, 40) {
            return ReuploadDecision::Replace;
        }
    }
    if local_size > 0 && cloud_size == local_size {
        return ReuploadDecision::Skip("skip:same_size");
    }
    ReuploadDecision::Replace
}

fn hash_matches(cloud: Option<&str>, sha1: &str, md5: &str) -> bool {
    let Some(h) = cloud.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    eq_hash(h, sha1) || eq_hash(h, md5)
}

fn eq_hash(a: &str, b: &str) -> bool {
    !a.is_empty() && !b.is_empty() && a.eq_ignore_ascii_case(b)
}

fn is_hex_digest(s: &str, len: usize) -> bool {
    let s = s.trim();
    s.len() == len && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// 文件名比较：兼容 Unicode 组合差异（部分客户端 NFC、列表 NFD 时避免「看不到」）。
fn file_names_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // 无第三方 normalize 时：去掉常见组合音标后再比（对中日韩路径基本等价于原串）。
    // 仍相等失败则再比「去零宽字符」形式，兜住拷贝时夹带的不可见字符。
    fn fold(s: &str) -> String {
        s.chars()
            .filter(|c| {
                !matches!(
                    c,
                    '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{FEFF}' | '\u{00AD}'
                )
            })
            .collect()
    }
    fold(a) == fold(b)
}

/// 同路径上传的完成信号。重试 PUT 等这个结果，不再自己回成功。
struct InflightSlot {
    tx: tokio::sync::watch::Sender<Option<bool>>,
    settled: AtomicBool,
}

impl InflightSlot {
    fn new() -> Arc<Self> {
        let (tx, _rx) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            tx,
            settled: AtomicBool::new(false),
        })
    }

    fn settle(&self, ok: bool) {
        self.settled.store(true, Ordering::Release);
        // send 在没有接收者时会丢掉值。重试 PUT 可能稍后再来等，必须留下结果。
        self.tx.send_replace(Some(ok));
    }

    async fn wait(&self) -> bool {
        let mut rx = self.tx.subscribe();
        loop {
            if let Some(ok) = *rx.borrow() {
                return ok;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    }
}

/// 路径级上传互斥：任务结束（成功/失败/提前 return）时自动摘掉，允许合法重试。
struct UploadInflightGuard {
    map: Arc<DashMap<String, Arc<InflightSlot>>>,
    path: String,
    slot: Arc<InflightSlot>,
    settled: bool,
}

impl UploadInflightGuard {
    fn settle(&mut self, ok: bool) {
        self.settled = true;
        self.slot.settle(ok);
    }
}

impl Drop for UploadInflightGuard {
    fn drop(&mut self) {
        if !self.settled {
            self.slot.settle(false);
        }
        self.map.remove(&self.path);
    }
}

/// 从内存或临时文件按偏移读取一段，供 Cookie 分片上传使用。
async fn read_upload_slice(
    source: &UploadSource,
    offset: u64,
    len: usize,
) -> anyhow::Result<Vec<u8>> {
    match source {
        UploadSource::Memory(data) => {
            let start = offset as usize;
            let end = start.saturating_add(len);
            if end > data.len() {
                anyhow::bail!(
                    "memory slice OOB: offset={offset} len={len} size={}",
                    data.len()
                );
            }
            Ok(data[start..end].to_vec())
        }
        UploadSource::Path(path) => {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let mut file = tokio::fs::File::open(path).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut buf = vec![0u8; len];
            if len > 0 {
                file.read_exact(&mut buf).await?;
            }
            Ok(buf)
        }
    }
}

fn sanitize_temp_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            _ => c,
        })
        .collect()
}

fn is_url_expired(url: &str) -> bool {
    if let Ok(oss_url) = ::url::Url::parse(url) {
        let expires = oss_url.query_pairs().find_map(|(k, v)| {
            // Cookie 模式常用 Expires；Open/OSS 也可能是 expires / exp / e（秒级时间戳）
            let key = k.as_ref();
            if key.eq_ignore_ascii_case("expires") || key == "exp" || key == "e" {
                if let Ok(expires) = v.parse::<u64>() {
                    return Some(expires);
                }
            }
            None
        });
        if let Some(expires) = expires {
            let current_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs();
            // 预留 1 分钟
            return current_ts + 60 >= expires;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_memory_pool_parallel_cap() {
        let pool = UploadMemoryPool::new(100);
        assert!(pool.try_reserve(60));
        assert!(!pool.try_reserve(50)); // 60+50 > 100
        assert!(pool.try_reserve(40));
        assert_eq!(pool.used(), 100);
        pool.release(60);
        assert!(pool.try_reserve(50));
        assert_eq!(pool.used(), 90);
    }

    #[test]
    fn test_upload_memory_pool_disabled() {
        let pool = UploadMemoryPool::new(0);
        assert!(!pool.try_reserve(1));
    }

    #[test]
    fn test_is_url_expired_with_past_timestamp() {
        // Expires=0 is definitely in the past
        let url = "https://example.com/file?Expires=0";
        assert!(is_url_expired(url));
    }

    #[test]
    fn test_is_url_expired_with_future_timestamp() {
        // Use a timestamp far in the future (year ~2100)
        let url = "https://example.com/file?Expires=4102444800";
        assert!(!is_url_expired(url));
    }

    #[test]
    fn test_is_url_expired_no_expires_param() {
        let url = "https://example.com/file?key=value";
        // No Expires param => not expired (returns false)
        assert!(!is_url_expired(url));
    }

    #[test]
    fn test_is_url_expired_invalid_url() {
        let url = "not a valid url";
        // Invalid URL => not expired (returns false)
        assert!(!is_url_expired(url));
    }

    #[test]
    fn test_is_url_expired_within_60s_buffer() {
        // Get current time + 30 seconds (within the 60s buffer)
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() + 30;
        let url = format!("https://example.com/file?Expires={}", expires);
        // Should be considered expired (within 60s buffer)
        assert!(is_url_expired(&url));
    }

    #[test]
    fn test_is_url_expired_beyond_60s_buffer() {
        // Get current time + 120 seconds (beyond the 60s buffer)
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() + 120;
        let url = format!("https://example.com/file?Expires={}", expires);
        assert!(!is_url_expired(&url));
    }

    #[test]
    fn test_is_url_expired_empty_string() {
        assert!(!is_url_expired(""));
    }

    #[test]
    fn test_is_url_expired_with_multiple_params() {
        // URL with multiple params, Expires in the middle
        let url = "https://example.com/file?OSSAccessKeyId=xxx&Expires=0&Signature=yyy";
        assert!(is_url_expired(url));
    }

    #[test]
    fn test_is_url_expired_exactly_at_boundary() {
        // Get current time + exactly 60 seconds (at boundary)
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() + 60;
        let url = format!("https://example.com/file?Expires={}", expires);
        // current_ts + 60 >= expires → should be expired at boundary
        assert!(is_url_expired(&url));
    }

    #[test]
    fn test_is_url_expired_non_numeric_expires() {
        let url = "https://example.com/file?Expires=not_a_number";
        // Non-numeric Expires should not cause a panic, returns false
        assert!(!is_url_expired(url));
    }

    #[test]
    fn same_size_without_proof_does_not_reupload() {
        // Open 模式没有 md5，content_hash 为空：同大小应跳过，而不是删了再传。
        let d = decide_reupload(None, None, 11, "abc", "def", 11, false);
        assert_eq!(d, ReuploadDecision::Skip("skip:same_size"));
    }

    #[test]
    fn sha1_mismatch_still_replaces() {
        let cloud = "0123456789abcdef0123456789abcdef01234567";
        let local = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let d = decide_reupload(Some(cloud), None, 11, local, "def", 11, false);
        assert_eq!(d, ReuploadDecision::Replace);
    }

    #[test]
    fn matching_sha1_skips_even_if_size_differs() {
        let sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let d = decide_reupload(Some(sha), None, 1, sha, "def", 11, false);
        assert_eq!(d, ReuploadDecision::Skip("skip:hash"));
    }

    #[test]
    fn different_size_replaces_when_hash_unknown() {
        let d = decide_reupload(None, None, 10, "abc", "def", 11, false);
        assert_eq!(d, ReuploadDecision::Replace);
    }

    #[test]
    fn unknown_hash_algorithm_with_same_size_skips() {
        // 夸克有时给的不是 md5/sha1。不能把「算法不同」当成「内容变了」。
        let d = decide_reupload(Some("not-a-digest"), None, 5, "abc", "def", 5, false);
        assert_eq!(d, ReuploadDecision::Skip("skip:same_size"));
    }

    #[test]
    fn parse_oc_mtime_seconds_and_millis() {
        assert_eq!(parse_client_mtime_ms("1741249367"), Some(1_741_249_367_000));
        assert_eq!(parse_client_mtime_ms("1741249367000"), Some(1_741_249_367_000));
        assert_eq!(parse_client_mtime_ms("0"), None);
        assert_eq!(parse_client_mtime_ms("nope"), None);
    }

    #[test]
    fn flatten_error_is_one_line() {
        let s = flatten_error("上传分片 12 失败:\nHTTP 504");
        assert_eq!(s, "上传分片 12 失败: HTTP 504");
        assert!(!s.contains('\n'));
    }

    #[test]
    fn duplicate_waiter_sees_settled_result_and_failed_drop() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let slot = InflightSlot::new();
            slot.settle(true);
            assert!(slot.wait().await);

            let map: Arc<DashMap<String, Arc<InflightSlot>>> = Arc::new(DashMap::new());
            let slot = InflightSlot::new();
            map.insert("/a.MOV".into(), slot.clone());
            let guard = UploadInflightGuard {
                map: map.clone(),
                path: "/a.MOV".into(),
                slot: slot.clone(),
                settled: false,
            };
            drop(guard);
            assert!(!slot.wait().await);
            assert!(!map.contains_key("/a.MOV"));
        });
    }

    #[test]
    fn join_dav_path_root_has_single_slash() {
        assert_eq!(join_dav_path("/", "a.MOV"), "/a.MOV");
        assert_eq!(join_dav_path("", "a.MOV"), "/a.MOV");
        assert_eq!(join_dav_path("/我的备份", "a.MOV"), "/我的备份/a.MOV");
    }
}