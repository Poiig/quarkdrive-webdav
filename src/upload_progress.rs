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
    /// 近窗口瞬时速度（字节/秒）；上传阶段为实际上传速度。
    pub speed_bps: u64,
    /// small | fast | slow：智能调度车道（仅大文件区分快/慢）。
    pub lane: String,
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
            lane: String::new(),
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
    /// 接收与上传各占一半，避免「收完就 100%」但 OSS 还在传。
    pub fn percent(&self) -> u8 {
        if matches!(self.status.as_str(), "ok" | "skip") {
            return 100;
        }
        if self.size == 0 {
            return 0;
        }
        let half = self.size;
        let recv = self.received.min(half);
        let up = self.uploaded.min(half);
        // (recv + up) / (2 * size) → 接收满 50%，上传满再 50%
        (((recv + up).saturating_mul(100)) / (half.saturating_mul(2)))
            .min(100) as u8
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

    /// 一行可读摘要：百分比、体积、速度分项写清，避免混成「一个百分比」。
    pub fn summary(&self) -> String {
        let active = matches!(self.status.as_str(), "receiving" | "uploading");
        let speed = if active {
            format!(
                " · 速度 {}/s（均 {}/s）",
                format_bytes(self.speed_bps),
                format_bytes(self.avg_speed_bps())
            )
        } else if self.started_ms > 0 && self.updated_ms > self.started_ms {
            format!(" · 均速 {}/s", format_bytes(self.avg_speed_bps()))
        } else {
            String::new()
        };
        format!(
            "进度 {}% · 体积 收{}/{}/共{}{}",
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
                    // 进入上传阶段时接收侧已结束，按满值计入进度前半段。
                    e.received = e.received.max(size);
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

    /// 更新智能调度车道标签（fast/slow/small）。
    pub fn set_lane(&self, path: &str, lane: &str) {
        if let Some(mut e) = self.inner.get_mut(path) {
            e.lane = lane.into();
            e.updated_ms = now_ms();
        }
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

    /// 列出条目：按开始时间升序固定位置，避免进度刷新时上下跳动。
    pub fn list(&self) -> Vec<ProgressEntry> {
        let now = now_ms();
        // 已结束超过 1 小时的从内存表摘掉，避免无限涨。
        const DONE_TTL_MS: u64 = 60 * 60 * 1000;
        self.inner.retain(|_, e| {
            let active = matches!(e.status.as_str(), "receiving" | "uploading");
            active || now.saturating_sub(e.updated_ms) < DONE_TTL_MS
        });
        let mut items: Vec<_> = self.inner.iter().map(|e| e.value().clone()).collect();
        // 同一文件始终同一槽位：只按 started_ms，不用 updated_ms 重排。
        items.sort_by(|a, b| {
            a.started_ms
                .cmp(&b.started_ms)
                .then_with(|| a.path.cmp(&b.path))
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
        // 接收一半 → 总进度 25%（接收阶段最多 50%）
        assert_eq!(e.percent(), 25);
        assert_eq!(e.status, "receiving");

        t.mark_uploading("/a.bin", 1000);
        t.add_uploaded("/a.bin", 500);
        let e = t.list().into_iter().next().unwrap();
        // 收满 + 上传一半 → 75%
        assert_eq!(e.percent(), 75);
    }
}
