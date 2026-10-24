use std::fmt::{Debug, Formatter};
use std::io::{SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
}

impl QuarkDriveFileSystem {
    #[allow(clippy::too_many_arguments)]
    pub fn new(drive: QuarkDrive, root: String, cache_size: u64, cache_ttl: u64) -> Result<Self> {
        let dir_cache = Cache::new(cache_size, cache_ttl, drive.clone());
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
            upload_wait_timeout: 280,
            oplog: Arc::new(OpLog::new("./logs", 7)),
        })
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

    async fn find_in_cache(&self, path: &Path) -> Result<Option<QuarkFile>, FsError> {
        if let Some(parent) = path.parent() {
            let parent_str = parent.to_string_lossy();
            let file_name = path
                .file_name()
                .ok_or(FsError::NotFound)?
                .to_string_lossy()
                .into_owned();
            let file = self.dir_cache.get_or_insert(&parent_str).await.and_then(|files| {
                for file in &files {
                    if file.file_name == file_name {
                        return Some(file.clone());
                    }
                }
                None
            });
            Ok(file)
        } else {
            let root = QuarkFile::new_root();
            Ok(Some(root))
        }
    }

    async fn get_file(&self, path: PathBuf) -> Result<Option<QuarkFile>, FsError> {
        let file = self.find_in_cache(&path).await?;
        if let Some(file) = file {
            trace!(path = %path.display(), file_id = %file.fid, "file found in cache");
            Ok(Some(file))
        } else {
            // find in drive
            Ok(None)
        }
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

                let file = QuarkFile {
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
                };

                let mut uploading = self.uploading.entry(parent_path.to_str().unwrap().to_string()).or_default();
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
            let files = self.dir_cache.get_or_insert(&path.to_string_lossy())
                .await
                .ok_or(FsError::NotFound)
                .and_then(|files| {
                    Ok(files)
                })?;

            // 创建包含结果的向量
            let mut v: Vec<Result<Box<dyn DavDirEntry>, FsError>> = Vec::with_capacity(files.len());

            // 将每个文件转换为 trait 对象
            for file in files {
                v.push(Ok(Box::new(file))); // 现在类型匹配了
            }

            // 创建流并装箱
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

            // if not found in cache, get from uploading files: self.fs.uploading
            let mut file = self.get_file(path.clone()).await.unwrap_or_else(|_| Option::None);
            if file.is_none() {
                let parent_path = path.parent().ok_or(FsError::NotFound)?;
                let want = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();
                file = self
                    .list_uploading_files(parent_path.to_str().unwrap_or_default())
                    .into_iter()
                    .find(|f| f.file_name == want);
            };

            let file = file.ok_or(FsError::NotFound)?;

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
                self.dir_cache.invalidate(parent_path).await;
                let name = name.to_string_lossy().into_owned();
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
        if !self.upload_state.is_uploading {
            self.upload_state.is_uploading = true;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            self.upload_state.temp_file_path =
                format!("/tmp/{}_{}", timestamp, sanitize_temp_name(&self.file.file_name));
            // 内存池可用则先走内存；limit=0 或预留失败时仍可稍后在写入时落盘。
            self.upload_state.use_memory = self.fs.upload_memory.limit() > 0;
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

    fn upload_log_path(&self) -> String {
        format!(
            "{}/{}",
            self.file.parent_path.as_deref().unwrap_or(""),
            self.file.file_name
        )
    }

    async fn log_upload_result(&self, size: u64, result: &str) {
        self.fs
            .oplog
            .log_upload(&self.upload_log_path(), size, self.fs.drive_mode(), result)
            .await;
    }

    async fn do_flush(&mut self) -> Result<(), FsError> {
        let size = self.upload_state.size;

        // Compute final SHA-1 and MD5 (all data has been written)
        let sha1 = format!("{:x}", self.sha1_ctx.clone().finalize());
        let md5 = format!("{:x}", self.md5_ctx.clone().compute());

        // 本项目重复上传逻辑：hash/同尺寸可跳过；否则先删再传（Cookie/Open 共用）。
        if !self.file.fid.is_empty() {
            let mut skip = false;
            // 列表里的 content_hash（多为 sha1）Open/Cookie 都能用，优先比对。
            if let Some(ref cloud_hash) = self.file.content_hash {
                if cloud_hash.eq_ignore_ascii_case(&sha1)
                    || cloud_hash.eq_ignore_ascii_case(&md5)
                {
                    debug!(file_name = %self.file.file_name, hash = %cloud_hash,
                           "skip uploading: content_hash unchanged");
                    skip = true;
                }
            }
            if !skip {
                match self.fs.drive.get_file_md5(&self.file.fid).await {
                    Ok(Some(cloud_md5)) if cloud_md5.eq_ignore_ascii_case(&md5) => {
                        debug!(file_name = %self.file.file_name, md5 = %md5,
                               "skip uploading: content hash unchanged");
                        skip = true;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        debug!(file_name = %self.file.file_name, error = %err,
                               "failed to get cloud file md5, proceeding with upload");
                    }
                }
            }
            if !skip && self.fs.skip_upload_same_size && self.file.size == size {
                debug!(file_name = %self.file.file_name, size = size,
                       "skip uploading: same size");
                skip = true;
            }
            if skip {
                self.upload_state.is_finished = true;
                self.log_upload_result(size, "skip").await;
                self.after_flush().await?;
                return Ok(());
            }
            // 内容不同：先删旧文件再上传（不用 OpenList same_path_reuse）。
            // 删除失败必须中止，否则同名冲突 / 脏数据。
            if let Err(err) = self.fs.drive
                .remove_file(&self.file.fid, !self.fs.no_trash).await
            {
                error!(file_name = %self.file.file_name, error = %err,
                       "delete file before upload failed");
                return Err(FsError::GeneralFailure);
            }
            self.file.fid.clear();
        }

        // Open API 上传链路与 Cookie 分片鉴权不同，整段交给 drive 完成。
        if self.fs.drive.is_open_mode() {
            return self.do_flush_open(&md5, &sha1, size).await;
        }

        // 空文件走专用路径，避免 chunk_count=0。
        if size == 0 {
            return self.upload_mini_byte_file().await;
        }

        // up_pre
        let res = self
            .fs
            .drive
            .up_pre(&self.file.file_name, size, &self.parent_file_id)
            .await
            .map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "create file with proof failed");
                FsError::GeneralFailure
            })?;

        if res.data.finish {
            // 秒传
            self.upload_state.is_finished = true;
            self.log_upload_result(size, "ok:rapid").await;
            self.after_flush().await?;
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
            self.after_flush().await?;
            return Ok(());
        }
        // Spawn upload task so it won't be cancelled if client disconnects.
        // We still await the result — if the client stays connected, it gets the real result.
        // If the client disconnects (e.g. timeout), the spawned task continues uploading.
        let drive = self.fs.drive.clone();
        let memory_reserved = self.upload_state.memory_reserved;
        // 配额交由后台任务归还，避免 self 清空时重复 release。
        self.upload_state.memory_reserved = 0;
        let source = self.take_upload_source();
        let upload_state = self.upload_state.clone();
        let file_name = self.file.file_name.clone();
        let parent_path = self.file.parent_path.as_ref().unwrap().clone();
        let parent_dir = self.parent_dir.clone();
        let fs = self.fs.clone();
        let log_path = self.upload_log_path();
        let log_size = size;
        let log_mode = self.fs.drive_mode();

        let handle = tokio::spawn(async move {
            let _upload_slot = crate::upload_limit::acquire_upload_slot().await;
            let _memory_guard = UploadMemoryGuard {
                pool: fs.upload_memory.clone(),
                reserved: memory_reserved,
            };
            let temp_path = upload_state.temp_file_path.clone();
            let result = async {
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
                    let res = drive.up_part(up_req).await.map_err(|err| {
                        error!(file_name = %file_name, error = %err, "upload chunk failed");
                        FsError::GeneralFailure
                    })?;
                    let etag_from_up_part = res.ok_or_else(|| {
                        error!(file_name = %file_name, "upload chunk returned empty etag");
                        FsError::GeneralFailure
                    })?;
                    if etag_from_up_part == "finish" {
                        return Ok(true);
                    }
                    etags[(chunk_idx - 1) as usize] = etag_from_up_part;
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
            .await;

            if !temp_path.is_empty() {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }
            fs.remove_uploading_file(&parent_path, &file_name);

            match result {
                Ok(_) => {
                    fs.oplog
                        .log_upload(&log_path, log_size, log_mode, "ok")
                        .await;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    fs.dir_cache.invalidate(parent_dir.as_path()).await;
                    Ok::<(), FsError>(())
                }
                Err(err) => {
                    fs.oplog
                        .log_upload(&log_path, log_size, log_mode, "error")
                        .await;
                    Err(err)
                }
            }
        });

        // Wait for upload to complete, but return early if upload_wait_timeout is reached
        // to avoid client timeout. The spawned task continues uploading in the background.
        let upload_wait_timeout = self.fs.upload_wait_timeout;
        if upload_wait_timeout > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(upload_wait_timeout),
                handle,
            ).await {
                Ok(result) => {
                    // Upload finished within timeout, return real result
                    result.map_err(|err| {
                        error!(file_name = %self.file.file_name, error = %err, "upload task join failed");
                        FsError::GeneralFailure
                    })??;
                }
                Err(_) => {
                    // Timeout reached, upload continues in background
                    info!(file_name = %self.file.file_name, timeout_secs = upload_wait_timeout,
                          "upload still in progress, returning early to avoid client timeout");
                }
            }
        } else {
            // Wait indefinitely
            handle.await.map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "upload task join failed");
                FsError::GeneralFailure
            })??;
        }

        self.upload_state = UploadState::default();
        Ok(())
    }


    /// Open 模式上传：内容已在内存或临时文件，整段交给 drive。
    async fn do_flush_open(&mut self, md5: &str, sha1: &str, size: u64) -> Result<(), FsError> {
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
        let log_path = self.upload_log_path();
        let log_mode = self.fs.drive_mode();

        let handle = tokio::spawn(async move {
            let _upload_slot = crate::upload_limit::acquire_upload_slot().await;
            let _memory_guard = UploadMemoryGuard {
                pool: fs.upload_memory.clone(),
                reserved: memory_reserved,
            };
            let result = drive
                .upload_open_file(
                    &file_name,
                    size,
                    &parent_file_id,
                    &md5,
                    &sha1,
                    &source,
                )
                .await;
            if !temp_path.is_empty() {
                let _ = tokio::fs::remove_file(&temp_path).await;
            }
            fs.remove_uploading_file(&parent_path, &file_name);
            match result {
                Ok(()) => {
                    fs.oplog
                        .log_upload(&log_path, size, log_mode, "ok")
                        .await;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    fs.dir_cache.invalidate(parent_dir.as_path()).await;
                    Ok::<(), FsError>(())
                }
                Err(err) => {
                    error!(file_name = %file_name, error = %err, "open upload failed");
                    fs.oplog
                        .log_upload(&log_path, size, log_mode, &format!("error:{err:#}"))
                        .await;
                    Err(FsError::GeneralFailure)
                }
            }
        });

        let upload_wait_timeout = self.fs.upload_wait_timeout;
        if upload_wait_timeout > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(upload_wait_timeout),
                handle,
            )
            .await
            {
                Ok(result) => {
                    result.map_err(|err| {
                        error!(file_name = %self.file.file_name, error = %err, "open upload task join failed");
                        FsError::GeneralFailure
                    })??;
                }
                Err(_) => {
                    info!(file_name = %self.file.file_name, timeout_secs = upload_wait_timeout,
                          "open upload still in progress, returning early to avoid client timeout");
                }
            }
        } else {
            handle.await.map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "open upload task join failed");
                FsError::GeneralFailure
            })??;
        }

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
                    self.after_flush().await?;
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
            return self.do_flush_open(empty_md5, empty_sha1, 0).await;
        }

        // pre -> hash -> commit -> finish
        // up_pre
        let res = self
            .fs
            .drive
            .up_pre(&self.file.file_name, 0, &self.parent_file_id)
            .await
            .map_err(|err| {
                error!(file_name = %self.file.file_name, error = %err, "create file with proof failed");
                FsError::GeneralFailure
            })?;

        if res.data.finish {
            // 秒传
            self.upload_state.is_finished = true;
            self.after_flush().await?;
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
            self.after_flush().await?;
            return Ok(());
        }
        // 空文件优先走内存源，避免额外落盘。
        self.upload_state.use_memory = true;
        self.upload_state.memory_data.clear();
        self.upload_state.size = 0;
        self.upload_state.temp_file_path.clear();
        self.upload_chunk().await?;
        self.log_upload_result(0, "ok").await;
        self.after_flush().await?;

        Ok(())
    }


    async fn consume_buf(&mut self) -> Result<(), FsError> {
        let mut md5_ctx = self.md5_ctx.clone();
        let mut sha1_ctx = self.sha1_ctx.clone();
        let bytes = self.upload_state.buffer.split().freeze().to_vec();
        let add = bytes.len() as u64;

        // 先落盘/入内存成功后再累加 size，避免失败后 size 虚高导致读切片越界。
        if self.upload_state.use_memory {
            // 并行合计超限则溢出到临时文件。
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

        self.upload_state.size = self.upload_state.size + add;
        md5_ctx.consume(&bytes);
        sha1_ctx.update(&bytes);
        self.md5_ctx = md5_ctx;
        self.sha1_ctx = sha1_ctx;
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
        let temp_path = &self.upload_state.temp_file_path;
        if tokio::fs::metadata(temp_path).await.is_ok() {
            if let Err(err) = tokio::fs::remove_file(temp_path).await {
                error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "remove temp file failed");
            }
        }
        Ok(())
    }

    async fn after_flush(&mut self) -> Result<(), FsError> {
        self.delete_temp_file().await?;
        self.release_upload_memory();
        let parent_path = self.file.parent_path.as_ref().unwrap().as_str();
        self.fs.remove_uploading_file(parent_path, &self.file.file_name);
        self.upload_state = UploadState::default();
        // sleep 1s for quark server to update cache
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        self.fs.dir_cache.invalidate(self.parent_dir.as_path()).await;
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
    fn write_buf(&mut self, buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()>{
        debug!(file_id = %self.file.fid, file_name = %self.file.file_name, "file: write_buf");
        async move {
            if self.prepare_for_upload().await? {
                self.upload_state.buffer.put(buf);
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
                .map(|p| p.serves(&url, pos))
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
                    // Open 直链可能已失效：清缓存后重新取链再读。
                    if self.fs.drive.is_open_mode() {
                        self.file.download_url = None;
                    }
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
                    self.after_flush().await?;
                    return Err(err);
                }
                return Ok(());
            }

            let res = self.do_flush().await;
            if let Err(err) = res {
                error!(file_id = %self.file.fid, file_name = %self.file.file_name, error = %err, "file: flush failed");
                self.after_flush().await?;
                return Err(err);
            }
            Ok(())
        }.boxed()

    }
}

impl Drop for QuarkDavFile {
    /// 客户端中断上传时归还内存配额，并尽量清掉临时文件 / uploading 幽灵项。
    fn drop(&mut self) {
        self.release_upload_memory();
        if self.upload_state.is_uploading && !self.upload_state.is_finished {
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
}