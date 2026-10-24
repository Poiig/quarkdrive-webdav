//! 上传 / 删除操作审计日志：写文件，同时打到 stdout（Docker / 群晖控制台可见）。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Local, NaiveDate};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::util::format_bytes;

#[derive(Debug, Clone)]
pub struct OpLog {
    dir: PathBuf,
    retention_days: u64,
}

impl OpLog {
    pub fn new(dir: impl Into<PathBuf>, retention_days: u64) -> Self {
        let days = if retention_days == 0 { 7 } else { retention_days };
        Self {
            dir: dir.into(),
            retention_days: days,
        }
    }

    pub fn retention_days(&self) -> u64 {
        self.retention_days
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 启动时清理一次，并每天再清。
    pub fn start_prune_task(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.prune_old().await;
            let mut ticker = tokio::time::interval(Duration::from_secs(24 * 3600));
            loop {
                ticker.tick().await;
                this.prune_old().await;
            }
        });
    }

    pub async fn log_upload(&self, path: &str, size: u64, mode: &str, result: &str) {
        self.write_line(&format!(
            "upload result={result} mode={mode} size={} path={path}",
            format_bytes(size)
        ))
        .await;
    }

    pub async fn log_delete(&self, path: &str, is_dir: bool, mode: &str, result: &str) {
        let kind = if is_dir { "dir" } else { "file" };
        self.write_line(&format!(
            "delete result={result} mode={mode} kind={kind} path={path}"
        ))
        .await;
    }

    async fn write_line(&self, body: &str) {
        let now = Local::now();
        let stamped = format!("{} {}", now.format("%Y-%m-%d %H:%M:%S %z"), body);

        // 同步打到控制台，便于 docker logs / 群晖容器日志查看。
        info!(target: "oplog", "{body}");

        if let Err(err) = fs::create_dir_all(&self.dir).await {
            warn!(error = %err, dir = %self.dir.display(), "create op log dir failed");
            return;
        }
        let file = self
            .dir
            .join(format!("op-{}.log", now.format("%Y-%m-%d")));
        let line = format!("{stamped}\n");
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file)
            .await
        {
            Ok(mut f) => {
                if let Err(err) = f.write_all(line.as_bytes()).await {
                    warn!(error = %err, file = %file.display(), "write op log failed");
                }
            }
            Err(err) => {
                warn!(error = %err, file = %file.display(), "open op log failed");
            }
        }
    }

    pub async fn prune_old(&self) {
        let Ok(mut entries) = fs::read_dir(&self.dir).await else {
            return;
        };
        let cutoff = Local::now().date_naive()
            - ChronoDuration::days(self.retention_days as i64);
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(date_str) = name
                .strip_prefix("op-")
                .and_then(|s| s.strip_suffix(".log"))
            else {
                continue;
            };
            let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") else {
                continue;
            };
            if date < cutoff {
                match fs::remove_file(entry.path()).await {
                    Ok(()) => debug!(file = %name, "pruned old op log"),
                    Err(err) => warn!(error = %err, file = %name, "prune op log failed"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_zero_becomes_seven() {
        let log = OpLog::new("/tmp/oplog-test", 0);
        assert_eq!(log.retention_days(), 7);
    }
}
