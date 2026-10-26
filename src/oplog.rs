//! 上传 / 删除操作审计日志：写文件，同时打到 stdout（Docker / 群晖控制台可见）。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, info, warn};

use crate::util::{format_bytes, now_shanghai};

/// 解析后的一条操作日志，供 Web 历史页展示。
#[derive(Debug, Clone)]
pub struct OpLogEntry {
    pub time: String,
    pub kind: String,
    pub result: String,
    pub mode: String,
    pub size: String,
    pub path: String,
    pub raw: String,
    pub file: String,
}

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
        // 固定 Asia/Shanghai（北京时间）；不再附带 +0800，历史表更易读、少被截断。
        let now = now_shanghai();
        let stamped = format!("{} {}", now.format("%Y-%m-%d %H:%M:%S"), body);

        // 同步打到控制台（须用模块 target，才能过 RUST_LOG=quarkdrive_webdav=info）。
        info!("{stamped}");

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
        let cutoff = now_shanghai().date_naive()
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

    /// 读取近期操作日志（新→旧），供 Web 历史页筛选。
    pub async fn list_entries(
        &self,
        limit: usize,
        kind_filter: Option<&str>,
        result_filter: Option<&str>,
        path_query: Option<&str>,
    ) -> Vec<OpLogEntry> {
        let limit = limit.clamp(1, 2000);
        let mut files = list_op_log_files(&self.dir).await;
        // 新日期在前。
        files.sort_by(|a, b| b.1.cmp(&a.1));
        let mut out = Vec::new();
        for (path, _date, name) in files {
            let Ok(file) = fs::File::open(&path).await else {
                continue;
            };
            let mut lines = Vec::new();
            let mut reader = BufReader::new(file).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                lines.push(line);
            }
            for line in lines.into_iter().rev() {
                let Some(entry) = parse_op_line(&line, &name) else {
                    continue;
                };
                if let Some(k) = kind_filter {
                    if k != "all" && entry.kind != k {
                        continue;
                    }
                }
                if let Some(r) = result_filter {
                    if r != "all" {
                        let okish = entry.result == "ok"
                            || entry.result.starts_with("ok:")
                            || entry.result == "skip";
                        match r {
                            "ok" if !okish => continue,
                            "error" if okish => continue,
                            _ => {}
                        }
                    }
                }
                if let Some(q) = path_query {
                    let q = q.trim();
                    if !q.is_empty()
                        && !entry.path.to_ascii_lowercase().contains(&q.to_ascii_lowercase())
                    {
                        continue;
                    }
                }
                out.push(entry);
                if out.len() >= limit {
                    return out;
                }
            }
        }
        out
    }

    /// 清除日志：`date=YYYY-MM-DD` 删单日，否则删目录下全部 op-*.log。
    pub async fn clear(&self, date: Option<&str>) -> Result<usize, String> {
        if let Some(d) = date {
            let d = d.trim();
            if NaiveDate::parse_from_str(d, "%Y-%m-%d").is_err() {
                return Err("日期格式应为 YYYY-MM-DD".into());
            }
            let path = self.dir.join(format!("op-{d}.log"));
            return match fs::remove_file(&path).await {
                Ok(()) => Ok(1),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
                Err(err) => Err(err.to_string()),
            };
        }
        let files = list_op_log_files(&self.dir).await;
        let mut n = 0usize;
        for (path, _, _) in files {
            match fs::remove_file(&path).await {
                Ok(()) => n += 1,
                Err(err) => warn!(error = %err, file = %path.display(), "clear op log failed"),
            }
        }
        Ok(n)
    }
}

/// 枚举 `op-YYYY-MM-DD.log`。
async fn list_op_log_files(dir: &Path) -> Vec<(PathBuf, NaiveDate, String)> {
    let Ok(mut entries) = fs::read_dir(dir).await else {
        return Vec::new();
    };
    let mut files = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(date_str) = name
            .strip_prefix("op-")
            .and_then(|s| s.strip_suffix(".log"))
        else {
            continue;
        };
        let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") else {
            continue;
        };
        files.push((entry.path(), date, name));
    }
    files
}

/// 取出 `key=` 到下一个已知字段之间的文本。值本身可以含空格。
fn capture_field(body: &str, key: &str) -> String {
    let pat = format!("{key}=");
    let Some(start) = body.find(&pat) else {
        return String::new();
    };
    let rest = &body[start + pat.len()..];
    let end = [" result=", " mode=", " size=", " path=", " kind="]
        .iter()
        .filter_map(|marker| rest.find(marker))
        .min()
        .unwrap_or(rest.len());
    rest[..end].trim().to_string()
}

/// 解析一行控制台/文件日志。格式：`时间 upload|delete ...`
fn parse_op_line(line: &str, file: &str) -> Option<OpLogEntry> {
    // 新：`2026-10-24 14:22:15 upload result=ok ...`
    // 旧：`2026-10-24 14:22:15 +0800 upload result=ok ...`（兼容）
    let rest = line.trim();
    let (time, body) = if let Some(i) = rest.find(" upload ") {
        (&rest[..i], &rest[i + 1..])
    } else if let Some(i) = rest.find(" delete ") {
        (&rest[..i], &rest[i + 1..])
    } else {
        return None;
    };
    let time = time
        .trim()
        .trim_end_matches("+0800")
        .trim_end_matches("UTC+8")
        .trim();
    let kind = if body.starts_with("upload ") {
        "upload"
    } else if body.starts_with("delete ") {
        "delete"
    } else {
        return None;
    };
    // result 里会有空格（「上传分片 12 失败: HTTP 504」）。按下一个字段名截断，
    // 不能按空白切，否则历史里只剩「error:上传分片」。
    let result = capture_field(body, "result");
    let mode = capture_field(body, "mode");
    let size = capture_field(body, "size");
    let path = capture_field(body, "path");
    if result.is_empty() {
        return None;
    }
    Some(OpLogEntry {
        time: time.to_string(),
        kind: kind.into(),
        result,
        mode,
        size,
        path,
        raw: line.to_string(),
        file: file.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_zero_becomes_seven() {
        let log = OpLog::new("/tmp/oplog-test", 0);
        assert_eq!(log.retention_days(), 7);
    }

    #[test]
    fn parse_upload_line_with_size_unit() {
        let line = "2026-10-24 14:22:15 +0800 upload result=ok mode=open size=1.8 MB path=/我的备份/a.HEIC";
        let e = parse_op_line(line, "op-2026-10-24.log").unwrap();
        assert_eq!(e.kind, "upload");
        assert_eq!(e.result, "ok");
        assert_eq!(e.size, "1.8 MB");
        assert_eq!(e.path, "/我的备份/a.HEIC");
        assert_eq!(e.time, "2026-10-24 14:22:15");
    }

    #[test]
    fn parse_upload_line_without_offset() {
        let line = "2026-10-24 21:19:25 upload result=ok mode=open size=3.9 MB path=/我的备份/a.MOV";
        let e = parse_op_line(line, "op-2026-10-24.log").unwrap();
        assert_eq!(e.time, "2026-10-24 21:19:25");
        assert_eq!(e.result, "ok");
    }

    #[test]
    fn parse_upload_error_keeps_spaces_in_result() {
        let line = "2026-10-24 16:34:24 upload result=error:上传分片 12 失败: HTTP 504 mode=open size=11.5 GB path=/我的备份/a.MOV";
        let e = parse_op_line(line, "op-2026-10-24.log").unwrap();
        assert_eq!(e.result, "error:上传分片 12 失败: HTTP 504");
        assert_eq!(e.mode, "open");
        assert_eq!(e.size, "11.5 GB");
        assert_eq!(e.path, "/我的备份/a.MOV");
    }
}
