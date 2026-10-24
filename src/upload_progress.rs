//! 上传进度表：供 Web 控制台展示「正在传 / 刚结束」的文件、百分比与速度。
//!
//! 与 `uploading` 占位（给 CloudSync PROPFIND）分开：这里面向人看的进度，
//! 含接收字节、传到夸克的字节，以及按时间窗口估算的速率。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

use crate::util::format_bytes;

/// 单条上传进度。
#[derive(Debug, Clone)]
pub struct ProgressEntry {
    pub path: String,
    pub size: u64,
    pub received: u64,
    pub uploaded: u64,
    /// receiving | uploading | ok | error | skip
    pub status: String,
    pub detail: String,
    pub updated_ms: u64,
    pub started_ms: u64,
    /// 近窗口瞬时速度（字节/秒）。
    pub speed_bps: u64,
    sample_bytes: u64,
    sample_ms: u64,
}

impl ProgressEntry {
    fn new(path: String, size: u64, status: &str) -> Self {
        let now = now_ms();
        Self {
            path,
            size,
            received: 0,
            uploaded: 0,
            status: status.into(),
            detail: String::new(),
            updated_ms: now,
            started_ms: now,
            speed_bps: 0,
            sample_bytes: 0,
            sample_ms: now,
        }
    }

    /// 当前阶段已完成字节：接收期看 received，上传期看 uploaded（至少不低于 received）。
    pub fn done_bytes(&self) -> u64 {
        if self.status == "receiving" {
            self.received
        } else {
            self.uploaded.max(self.received)
        }
    }

    /// 给页面用的综合进度 0–100。
    pub fn percent(&self) -> u8 {
        if self.size == 0 {
            return if matches!(self.status.as_str(), "ok" | "skip") {
                100
            } else {
                0
            };
        }
        ((self.done_bytes().saturating_mul(100)) / self.size).min(100) as u8
    }

    /// 自开始以来的平均速度（字节/秒）。
    pub fn avg_speed_bps(&self) -> u64 {
        let elapsed = self.updated_ms.saturating_sub(self.started_ms).max(1);
        self.done_bytes().saturating_mul(1000) / elapsed
    }

    /// 按字节差与时间差刷新瞬时速度（至少间隔 400ms，避免抖动）。
    fn note_progress(&mut self, now: u64) {
        self.updated_ms = now;
        let done = self.done_bytes();
        let dt = now.saturating_sub(self.sample_ms);
        if dt >= 400 {
            let db = done.saturating_sub(self.sample_bytes);
            self.speed_bps = db.saturating_mul(1000) / dt;
            self.sample_bytes = done;
            self.sample_ms = now;
        }
    }

    /// 一行可读摘要（含进度与速度）。
    pub fn summary(&self) -> String {
        let active = matches!(self.status.as_str(), "receiving" | "uploading");
        let speed = if active {
            format!(
                " · {}/s（均 {}/s）",
                format_bytes(self.speed_bps),
                format_bytes(self.avg_speed_bps())
            )
        } else if self.started_ms > 0 && self.updated_ms > self.started_ms {
            format!(" · 均 {}/s", format_bytes(self.avg_speed_bps()))
        } else {
            String::new()
        };
        format!(
            "{}% · 接收 {} / 上传 {} / 共 {}{}",
            self.percent(),
            format_bytes(self.received),
            format_bytes(self.uploaded),
            format_bytes(self.size),
            speed
        )
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 进程内进度表（按 path 覆盖更新）。
#[derive(Debug, Clone, Default)]
pub struct UploadTracker {
    inner: Arc<DashMap<String, ProgressEntry>>,
}

impl UploadTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 开始接收（WebDAV PUT 写入时）。
    pub fn start_receiving(&self, path: &str, size: u64) {
        self.inner
            .insert(path.to_string(), ProgressEntry::new(path.to_string(), size, "receiving"));
    }

    /// 更新已从客户端接收的字节数。
    pub fn set_received(&self, path: &str, received: u64, size: u64) {
        let now = now_ms();
        self.inner
            .entry(path.to_string())
            .and_modify(|e| {
                e.received = received;
                if size > 0 {
                    e.size = size;
                }
                if e.status == "receiving" || e.status.is_empty() {
                    e.status = "receiving".into();
                }
                e.note_progress(now);
            })
            .or_insert_with(|| {
                let mut e = ProgressEntry::new(path.to_string(), size, "receiving");
                e.received = received;
                e.note_progress(now);
                e
            });
    }

    /// 进入向夸克上传阶段。
    pub fn mark_uploading(&self, path: &str, size: u64) {
        let now = now_ms();
        self.inner
            .entry(path.to_string())
            .and_modify(|e| {
                e.status = "uploading".into();
                if size > 0 {
                    e.size = size;
                }
                // 切换阶段时重置采样窗口，速度反映 OSS 上传而非接收。
                e.sample_bytes = e.uploaded;
                e.sample_ms = now;
                e.speed_bps = 0;
                e.updated_ms = now;
            })
            .or_insert_with(|| {
                let mut e = ProgressEntry::new(path.to_string(), size, "uploading");
                e.received = size;
                e
            });
    }

    /// 累加已成功 PUT 到 OSS 的字节。
    pub fn add_uploaded(&self, path: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        let now = now_ms();
        if let Some(mut e) = self.inner.get_mut(path) {
            e.uploaded = e.uploaded.saturating_add(delta);
            if e.status == "receiving" {
                e.status = "uploading".into();
            }
            e.note_progress(now);
        }
    }

    /// 结束：ok / error / skip。
    pub fn finish(&self, path: &str, status: &str, detail: &str) {
        let now = now_ms();
        self.inner
            .entry(path.to_string())
            .and_modify(|e| {
                e.status = status.into();
                e.detail = detail.into();
                e.updated_ms = now;
                if status == "ok" || status == "skip" {
                    if e.size > 0 {
                        e.uploaded = e.size;
                        e.received = e.received.max(e.size);
                    }
                }
                // 结束后保留最后瞬时速度；不再刷新采样。
            })
            .or_insert_with(|| {
                let mut e = ProgressEntry::new(path.to_string(), 0, status);
                e.detail = detail.into();
                e
            });
    }

    /// 列出条目：进行中在前，其余按更新时间倒序；顺带丢掉过期已结束项。
    pub fn list(&self) -> Vec<ProgressEntry> {
        let now = now_ms();
        // 已结束超过 1 小时的从内存表摘掉，避免无限涨。
        const DONE_TTL_MS: u64 = 60 * 60 * 1000;
        self.inner.retain(|_, e| {
            let active = matches!(e.status.as_str(), "receiving" | "uploading");
            active || now.saturating_sub(e.updated_ms) < DONE_TTL_MS
        });
        let mut items: Vec<_> = self.inner.iter().map(|e| e.value().clone()).collect();
        items.sort_by(|a, b| {
            let a_active = matches!(a.status.as_str(), "receiving" | "uploading");
            let b_active = matches!(b.status.as_str(), "receiving" | "uploading");
            b_active
                .cmp(&a_active)
                .then_with(|| b.updated_ms.cmp(&a.updated_ms))
        });
        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_and_speed_sample() {
        let t = UploadTracker::new();
        t.start_receiving("/a.bin", 1000);
        t.set_received("/a.bin", 500, 1000);
        let e = t.list().into_iter().next().unwrap();
        assert_eq!(e.percent(), 50);
        assert_eq!(e.status, "receiving");
    }
}
