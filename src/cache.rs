use std::path::Path;
use std::time::Duration;

use moka::future::Cache as MokaCache;
use tracing::{debug, info};

use crate::drive::model::QuarkFile;
use crate::drive::QuarkDrive;

#[derive(Clone)]
pub struct Cache {
    inner: MokaCache<String, Vec<QuarkFile>>,
    drive: QuarkDrive,
}

const ONE_PAGE: u32 = 500;

impl Cache {
    /// `max_entries`：目录条数上限；`max_bytes`：加权字节上限。
    /// moka 只用一个 capacity：weigher 取 `max(估算字节, 每条目保底)`，
    /// 从而同时约束「大目录占内存」和「小目录条目数」。
    pub fn new(max_entries: u64, max_bytes: u64, ttl: u64, drive: QuarkDrive) -> Self {
        let max_entries = max_entries.max(1);
        let max_bytes = max_bytes.max(1024);
        let per_entry_floor = (max_bytes / max_entries).max(1);

        let inner = MokaCache::builder()
            .max_capacity(max_bytes)
            .weigher(move |_key: &String, files: &Vec<QuarkFile>| {
                let bytes = estimate_files_bytes(files);
                bytes.max(per_entry_floor).try_into().unwrap_or(u32::MAX)
            })
            .time_to_live(Duration::from_secs(ttl))
            .build();

        info!(
            max_entries = max_entries,
            max_bytes = max_bytes,
            per_entry_floor = per_entry_floor,
            ttl_secs = ttl,
            "dir cache configured (entry+byte dual limit)"
        );

        Self { inner, drive }
    }

    pub async fn get_or_insert(&self, key: &str) -> Option<Vec<QuarkFile>> {
        debug!(key = %key, "cache: get_or_insert");
        if let Some(files) = self.get(key).await {
            return Some(files);
        }
        if key == "/" {
            self.dfs(QuarkFile::new_root(), key, "/").await;
        } else {
            let mut path = Path::new(key);
            let mut dsf_root_file = None;
            while let Some(parent) = path.parent() {
                if let Some(c_files) = self.get(parent.to_str().unwrap()).await {
                    let file_name = path.file_name().and_then(|os_str| os_str.to_str());
                    let found = c_files
                        .iter()
                        .find(|quark_file| Some(quark_file.file_name.as_str()) == file_name)
                        .cloned();
                    if found.is_none() {
                        debug!(key = %key, "cache: no file found for path: {}", path.to_str().unwrap());
                        path = parent;
                        continue;
                    }
                    dsf_root_file = found;
                    break;
                }

                path = parent;

                if path.to_str() == Some("/") {
                    break;
                }
            }
            if path.to_str() == Some("/") {
                self.dfs(QuarkFile::new_root(), key, "/").await;
            } else {
                match dsf_root_file {
                    Some(dsf_root_fil) => {
                        debug!(key = %key, "cache: found root file: {}", dsf_root_fil.file_name);
                        self.dfs(dsf_root_fil, key, path.to_str().unwrap()).await;
                    }
                    None => {
                        debug!(key = %key, "cache: no root file found for path: {}", path.to_str().unwrap());
                        return None;
                    }
                }
            }
        }
        if let Some(files) = self.get(key).await {
            Some(files)
        } else {
            debug!(key = %key, "cache: no files found for key");
            None
        }
    }

    async fn dfs(&self, file: QuarkFile, target_path: &str, dfs_path: &str) {
        if file.dir {
            let mut current_files = Vec::<QuarkFile>::new();
            for page_no in 1..=20 {
                let (files, total) =
                    match self
                        .drive
                        .get_files_by_pdir_fid(&file.fid, page_no, ONE_PAGE)
                        .await
                    {
                        Ok((k, v)) => (k, v),
                        Err(e) => {
                            debug!(
                                error = %e,
                                file_id = &file.fid,
                                file_name = &file.file_name,
                                page_no = page_no,
                                "Failed to get files from drive"
                            );
                            return;
                        }
                    };
                let mut files = files.unwrap();
                for f in files.list.iter_mut() {
                    f.parent_path = Some(dfs_path.to_string());
                }
                let size = files.list.len();
                current_files.extend(files.list);
                if size < ONE_PAGE as usize || page_no >= total / ONE_PAGE + 1 {
                    break;
                }
            }

            self.insert(dfs_path.to_string(), current_files.clone())
                .await;
            debug!("{} in cache", &dfs_path);
            if dfs_path == target_path {
                return;
            }
            for curr_f in current_files {
                let file_path = if dfs_path == "/" {
                    format!("{}{}", dfs_path, curr_f.file_name)
                } else {
                    format!("{}/{}", dfs_path, curr_f.file_name)
                };
                if target_path.starts_with(&file_path) {
                    Box::pin(self.dfs(curr_f, target_path, &file_path)).await;
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Option<Vec<QuarkFile>> {
        debug!(key = %key, "cache: get");
        self.inner.get(key).await
    }

    /// 只读命中，不触发远端 list。上传前认领同名文件时用来判断目录是否已热。
    pub async fn get_cached(&self, key: &str) -> Option<Vec<QuarkFile>> {
        self.get(key).await
    }

    async fn insert(&self, key: String, value: Vec<QuarkFile>) {
        debug!(
            key = %key,
            entries = value.len(),
            approx_bytes = estimate_files_bytes(&value),
            "cache: insert"
        );
        self.inner.insert(key, value).await;
    }

    pub async fn invalidate(&self, path: &Path) {
        let key = path.to_string_lossy().into_owned();
        debug!(path = %path.display(), key = %key, "cache: invalidate");
        self.inner.invalidate(&key).await;
    }

    pub async fn invalidate_parent(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            self.invalidate(parent).await;
        }
    }

    pub fn invalidate_all(&self) {
        debug!("cache: invalidate all");
        self.inner.invalidate_all();
    }
}

/// 粗估目录列表占用的堆内存（含 String 内容）。
fn estimate_files_bytes(files: &[QuarkFile]) -> u64 {
    let mut n = std::mem::size_of_val(files) as u64;
    for f in files {
        n += 64; // 结构体字段开销粗算
        n += f.fid.len() as u64;
        n += f.file_name.len() as u64;
        n += f.pdir_fid.len() as u64;
        n += f.format_type.len() as u64;
        n += f.download_url.as_ref().map(|s| s.len() as u64).unwrap_or(0);
        n += f.content_hash.as_ref().map(|s| s.len() as u64).unwrap_or(0);
        n += f.parent_path.as_ref().map(|s| s.len() as u64).unwrap_or(0);
    }
    n.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_grows_with_name_length() {
        let short = QuarkFile {
            file_name: "a".into(),
            ..QuarkFile::new_root()
        };
        let long = QuarkFile {
            file_name: "x".repeat(1000),
            ..QuarkFile::new_root()
        };
        assert!(estimate_files_bytes(&[long]) > estimate_files_bytes(&[short]));
    }
}
