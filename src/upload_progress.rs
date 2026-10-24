//! 上传进度表：供 Web 控制台展示「正在传 / 刚结束」的文件、百分比与速度。
//!
//! 与 `uploading` 占位（给 CloudSync PROPFIND）分开：这里面向人看的进度，
//! 含接收字节、传到夸克的字节，以及按实际上传耗时估算的上行速率。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::task::AbortHandle;

use crate::util::format_bytes;

/// 单条上传进度。
#[derive(Debug, Clone)]
pub struct ProgressEntry {
    pub path: String,
    pub size: u64,
    pub received: u64,
    pub uploaded: u64,
    /// receiving | uploading | ok | error | skip | cancelled
    pub status: String,
    pub detail: String,
    pub updated_ms: u64,
    pub started_ms: u64,
    /// 进入向网盘上传阶段的时间（用于均速，不含接收耗时）。
    pub upload_started_ms: u64,
    /// 近窗口瞬时上行速度（字节/秒）；优先按单次 PUT 耗时计算。
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
            upload_started_ms: 0,
            speed_bps: 0,
            lane: String::new(),
            sample_bytes: 0,
            sample_ms: now,
        }
    }

    /// 速度采样用的字节：接收期看 received，上传期只看 uploaded（不能混入已接收量）。
    pub fn speed_bytes(&self) -> u64 {
        if self.status == "receiving" {
            self.received
        } else {
            self.uploaded
        }
    }

    /// 给页面用的上传进度 0–100：只看传到网盘的字节，接收阶段恒为 0。
    pub fn percent(&self) -> u8 {
        if matches!(self.status.as_str(), "ok" | "skip") {
            return 100;
        }
        if self.size == 0 {
            return 0;
        }
        // 面板展示的是「上传到盘」进度，WebDAV 接收不计入。
        ((self.uploaded.min(self.size).saturating_mul(100)) / self.size).min(100) as u8
    }

    /// 平均速度：上传阶段按实际上传字节 / 上传耗时；接收阶段按接收字节 / 总耗时。
    pub fn avg_speed_bps(&self) -> u64 {
        if self.status == "uploading"
            || (self.uploaded > 0 && matches!(self.status.as_str(), "ok" | "skip" | "error"))
        {
            let start = if self.upload_started_ms > 0 {
                self.upload_started_ms
            } else {
                self.started_ms
            };
            let elapsed = self.updated_ms.saturating_sub(start).max(1);
            return self.uploaded.saturating_mul(1000) / elapsed;
        }
        let elapsed = self.updated_ms.saturating_sub(self.started_ms).max(1);
        self.received.saturating_mul(1000) / elapsed
    }

    /// 按字节差与时间差刷新瞬时速度（至少间隔 400ms，避免抖动）。
    /// 仅用于接收阶段；上传阶段改走 [`note_upload_transfer`]。
    fn note_progress(&mut self, now: u64) {
        self.updated_ms = now;
        let done = self.speed_bytes();
        let dt = now.saturating_sub(self.sample_ms);
        if dt >= 400 {
            let db = done.saturating_sub(self.sample_bytes);
            self.speed_bps = db.saturating_mul(1000) / dt;
            self.sample_bytes = done;
            self.sample_ms = now;
        }
    }

    /// 按单次实际上传耗时刷新瞬时上行速度（不含慢传 sleep）。
    fn note_upload_transfer(&mut self, delta: u64, elapsed_ms: u64, now: u64) {
        self.updated_ms = now;
        self.sample_bytes = self.uploaded;
        self.sample_ms = now;
        let dt = elapsed_ms.max(1);
        let instant = delta.saturating_mul(1000) / dt;
        // 轻微平滑，避免分片间跳变过大，但仍以本次 PUT 为主。
        self.speed_bps = if self.speed_bps == 0 {
            instant
        } else {
            self.speed_bps.saturating_mul(2).saturating_add(instant) / 3
        };
    }

    /// 任务耗时（开始 → 最后更新），供成功/失败展示。
    pub fn elapsed_ms(&self) -> u64 {
        self.updated_ms.saturating_sub(self.started_ms)
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
#[derive(Debug, Clone)]
pub struct UploadTracker {
    inner: Arc<DashMap<String, ProgressEntry>>,
    /// 控制台取消：分片循环轮询此标志。
    cancels: Arc<DashMap<String, Arc<AtomicBool>>>,
    /// 控制台取消：直接 abort 后台 tokio 任务。
    aborts: Arc<DashMap<String, AbortHandle>>,
}

impl Default for UploadTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl UploadTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            cancels: Arc::new(DashMap::new()),
            aborts: Arc::new(DashMap::new()),
        }
    }

    /// 为路径装上取消标志（接收/上传开始时调用）；已有则复用，避免后台持有的 Arc 失效。
    pub fn arm_cancel(&self, path: &str) -> Arc<AtomicBool> {
        if let Some(existing) = self.cancels.get(path) {
            return existing.clone();
        }
        let flag = Arc::new(AtomicBool::new(false));
        self.cancels.insert(path.to_string(), flag.clone());
        flag
    }

    /// 绑定后台任务 AbortHandle，便于控制台立即打断。
    pub fn bind_abort(&self, path: &str, handle: AbortHandle) {
        self.aborts.insert(path.to_string(), handle);
    }

    /// 是否已请求取消（或已标为 cancelled）。
    pub fn is_cancel_requested(&self, path: &str) -> bool {
        if self
            .cancels
            .get(path)
            .map(|f| f.load(Ordering::Acquire))
            .unwrap_or(false)
        {
            return true;
        }
        self.inner
            .get(path)
            .map(|e| e.status == "cancelled")
            .unwrap_or(false)
    }

    /// 控制台取消进行中任务：置标志、abort、标 cancelled。
    pub fn request_cancel(&self, path: &str) -> bool {
        let active = self
            .inner
            .get(path)
            .map(|e| Self::is_active(&e.status))
            .unwrap_or(false);
        if !active {
            return false;
        }
        if let Some(f) = self.cancels.get(path) {
            f.store(true, Ordering::Release);
        }
        if let Some((_, handle)) = self.aborts.remove(path) {
            handle.abort();
        }
        self.finish(path, "cancelled", "已取消");
        true
    }

    /// 任务结束时清掉取消控制，避免泄漏。
    pub fn clear_control(&self, path: &str) {
        self.cancels.remove(path);
        self.aborts.remove(path);
    }

    /// 是否仍在向网盘上传（供重试 PUT 判断，勿与 receiving 混淆）。
    pub fn is_uploading(&self, path: &str) -> bool {
        self.inner
            .get(path)
            .map(|e| e.status == "uploading")
            .unwrap_or(false)
    }

    /// 开始接收（WebDAV PUT 写入时）。
    ///
    /// CloudSync 在服务端提前返回后常会整文件重 PUT；若后台仍在上传，
    /// 绝不能 insert 新条目，否则已传到盘的进度会被清成「接收中」。
    pub fn start_receiving(&self, path: &str, size: u64) {
        if let Some(mut e) = self.inner.get_mut(path) {
            if e.status == "uploading" {
                if size > 0 {
                    e.size = e.size.max(size);
                }
                e.updated_ms = now_ms();
                return;
            }
        }
        self.inner
            .insert(path.to_string(), ProgressEntry::new(path.to_string(), size, "receiving"));
        let _ = self.arm_cancel(path);
    }

    /// 更新已从客户端接收的字节数。
    ///
    /// 上传阶段若客户端重试写入，只记接收量，禁止把 status 降回 receiving。
    pub fn set_received(&self, path: &str, received: u64, size: u64) {
        let now = now_ms();
        self.inner
            .entry(path.to_string())
            .and_modify(|e| {
                if size > 0 {
                    e.size = e.size.max(size);
                }
                // 重试 PUT 的 received 从 0 累加，不能覆盖上传期已记的满收值。
                e.received = e.received.max(received);
                if e.status == "uploading"
                    || matches!(e.status.as_str(), "ok" | "error" | "skip" | "cancelled")
                {
                    e.updated_ms = now;
                    return;
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
                // 切换阶段时重置采样窗口，速度只跟 OSS 上传走。
                e.upload_started_ms = now;
                e.sample_bytes = e.uploaded;
                e.sample_ms = now;
                e.speed_bps = 0;
                e.updated_ms = now;
            })
            .or_insert_with(|| {
                let mut e = ProgressEntry::new(path.to_string(), size, "uploading");
                e.received = size;
                e.upload_started_ms = now;
                e
            });
        let _ = self.arm_cancel(path);
    }

    /// 更新智能调度车道标签（fast/slow/small）。
    pub fn set_lane(&self, path: &str, lane: &str) {
        if let Some(mut e) = self.inner.get_mut(path) {
            e.lane = lane.into();
            e.updated_ms = now_ms();
        }
    }

    /// 累加已成功 PUT 到 OSS 的字节；`elapsed_ms` 为该次实际上传耗时（不含限速 sleep）。
    pub fn add_uploaded(&self, path: &str, delta: u64, elapsed_ms: u64) {
        if delta == 0 {
            return;
        }
        let now = now_ms();
        if let Some(mut e) = self.inner.get_mut(path) {
            if e.status == "receiving" || e.upload_started_ms == 0 {
                e.status = "uploading".into();
                e.upload_started_ms = now.saturating_sub(elapsed_ms.max(1));
            }
            e.uploaded = e.uploaded.saturating_add(delta);
            e.note_upload_transfer(delta, elapsed_ms, now);
        }
    }

    /// 结束：ok / error / skip / cancelled。
    pub fn finish(&self, path: &str, status: &str, detail: &str) {
        let now = now_ms();
        self.inner
            .entry(path.to_string())
            .and_modify(|e| {
                // 已取消后，后台任务收尾勿再覆盖成 error/ok。
                if e.status == "cancelled" && status != "cancelled" {
                    e.updated_ms = now;
                    return;
                }
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
        self.clear_control(path);
        // 完成后顺带裁剪，避免列表无限涨。
        self.prune();
    }

    /// 是否进行中任务。
    fn is_active(status: &str) -> bool {
        matches!(status, "receiving" | "uploading")
    }

    /// 清除已完成/失败/跳过项，返回清除条数。
    pub fn clear_completed(&self) -> usize {
        let before = self.inner.len();
        self.inner.retain(|_, e| Self::is_active(&e.status));
        before.saturating_sub(self.inner.len())
    }

    /// 列表上限：进行中全保留，已完成最多凑到合计 [`MAX_LIST_ITEMS`]。
    pub const MAX_LIST_ITEMS: usize = 30;
    /// 已完成项最长保留（超时也归档删除）。
    const DONE_TTL_MS: u64 = 30 * 60 * 1000;

    /// 裁剪过期与超额的已完成项（进行中永不裁）。
    fn prune(&self) {
        let now = now_ms();
        self.inner.retain(|_, e| {
            Self::is_active(&e.status) || now.saturating_sub(e.updated_ms) < Self::DONE_TTL_MS
        });
        let active_n = self
            .inner
            .iter()
            .filter(|e| Self::is_active(&e.status))
            .count();
        let done_budget = Self::MAX_LIST_ITEMS.saturating_sub(active_n);
        if done_budget >= self.inner.len().saturating_sub(active_n) {
            return;
        }
        let mut done: Vec<(String, u64)> = self
            .inner
            .iter()
            .filter(|e| !Self::is_active(&e.status))
            .map(|e| (e.path.clone(), e.updated_ms))
            .collect();
        // 旧的先删，只留最新的 done_budget 条。
        done.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let drop_n = done.len().saturating_sub(done_budget);
        for (path, _) in done.into_iter().take(drop_n) {
            self.inner.remove(&path);
        }
    }

    /// 列出条目：进行中（新→旧）在上，已完成在下；同组按 started_ms 稳定，避免刷新乱跳。
    pub fn list(&self) -> Vec<ProgressEntry> {
        self.prune();
        let mut items: Vec<_> = self.inner.iter().map(|e| e.value().clone()).collect();
        items.sort_by(|a, b| {
            let a_active = Self::is_active(&a.status);
            let b_active = Self::is_active(&b.status);
            b_active.cmp(&a_active).then_with(|| {
                if a_active && b_active {
                    // 进行中：新任务在上
                    b.started_ms
                        .cmp(&a.started_ms)
                        .then_with(|| a.path.cmp(&b.path))
                } else {
                    b.updated_ms
                        .cmp(&a.updated_ms)
                        .then_with(|| a.path.cmp(&b.path))
                }
            })
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
        t.set_received("/a.bin", 1000, 1000);
        let e = t.list().into_iter().next().unwrap();
        // 接收再满也是 0%：进度只反映上传到盘。
        assert_eq!(e.percent(), 0);
        assert_eq!(e.status, "receiving");

        t.mark_uploading("/a.bin", 1000);
        assert_eq!(t.list()[0].percent(), 0);
        // 500 字节用 500ms → 1000 B/s，不能被「已接收 1000」污染成假高速。
        t.add_uploaded("/a.bin", 500, 500);
        let e = t.list().into_iter().next().unwrap();
        assert_eq!(e.percent(), 50);
        assert_eq!(e.speed_bps, 1000);
        assert!(e.uploaded == 500);
    }

    #[test]
    fn upload_speed_ignores_receive_bytes() {
        let t = UploadTracker::new();
        t.start_receiving("/b.bin", 10_000);
        t.set_received("/b.bin", 10_000, 10_000);
        t.mark_uploading("/b.bin", 10_000);
        t.add_uploaded("/b.bin", 2_000, 1_000); // 2KB / 1s
        let e = t.list().into_iter().next().unwrap();
        assert_eq!(e.speed_bps, 2_000);
        assert!(e.speed_bps < 50_000, "must not treat full received size as one sample");
    }

    #[test]
    fn cancel_marks_cancelled_and_keeps_elapsed() {
        let t = UploadTracker::new();
        t.start_receiving("/c.bin", 1000);
        t.mark_uploading("/c.bin", 1000);
        t.add_uploaded("/c.bin", 100, 100);
        assert!(t.request_cancel("/c.bin"));
        let e = t.list().into_iter().next().unwrap();
        assert_eq!(e.status, "cancelled");
        assert!(e.elapsed_ms() < 60_000);
        assert!(!t.request_cancel("/c.bin"), "already finished");
    }

    #[test]
    fn retry_put_does_not_wipe_uploading_progress() {
        let t = UploadTracker::new();
        t.start_receiving("/big.bin", 10_000);
        t.set_received("/big.bin", 10_000, 10_000);
        t.mark_uploading("/big.bin", 10_000);
        t.add_uploaded("/big.bin", 3_900, 1_000);
        assert_eq!(t.list()[0].percent(), 39);

        // 客户端超时重 PUT：不得清零，也不得降回 receiving。
        t.start_receiving("/big.bin", 10_000);
        t.set_received("/big.bin", 100, 10_000);
        let e = t.list().into_iter().next().unwrap();
        assert_eq!(e.status, "uploading");
        assert_eq!(e.percent(), 39);
        assert_eq!(e.uploaded, 3_900);
        assert!(t.is_uploading("/big.bin"));
    }

    #[test]
    fn clear_completed_keeps_active_and_caps_list() {
        let t = UploadTracker::new();
        for i in 0..40 {
            let p = format!("/done-{i}.bin");
            t.start_receiving(&p, 100);
            t.finish(&p, "ok", "");
        }
        t.start_receiving("/active.bin", 100);
        t.mark_uploading("/active.bin", 100);
        let list = t.list();
        assert!(list.len() <= UploadTracker::MAX_LIST_ITEMS);
        assert!(list.iter().any(|e| e.path == "/active.bin"));
        assert!(list.iter().any(|e| e.status == "uploading"));

        let cleared = t.clear_completed();
        assert!(cleared > 0);
        let list = t.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].path, "/active.bin");
    }
}
