//! 上传并发与分片内存预算。
//!
//! 大文件分片 PUT 已改为流式读取，单任务峰值约一个读缓冲（~64KB～1MB），
//! 不再是「单片数百 MB」。这里仍用「任务数 + 小额预算」限流，避免多路同时
//! 上传把连接/临时缓冲打满；单片再大也能在低内存机器上串行传完。

use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;
use tracing::info;

/// 流式上传时，每个在途任务按此字节计入预算（读缓冲 + HTTP 开销余量）。
pub const STREAM_PEAK_BYTES: u64 = 1024 * 1024;

struct LimiterState {
    jobs: usize,
    used: u64,
}

struct UploadLimiter {
    max_jobs: usize,
    budget: u64,
    state: Mutex<LimiterState>,
    notify: Notify,
}

/// Drop 时归还任务名额与预算。
pub struct UploadPermit {
    limiter: Arc<UploadLimiter>,
    peak_bytes: u64,
}

impl Drop for UploadPermit {
    fn drop(&mut self) {
        {
            let mut st = self.limiter.state.lock().unwrap_or_else(|e| e.into_inner());
            st.jobs = st.jobs.saturating_sub(1);
            st.used = st.used.saturating_sub(self.peak_bytes);
        }
        self.limiter.notify.notify_waiters();
    }
}

static LIMITER: OnceLock<Arc<UploadLimiter>> = OnceLock::new();

/// 进程启动时调用。`budget_bytes==0` 时只按任务数限流。
pub fn init_upload_concurrency(max_jobs: usize, budget_bytes: u64) {
    let max_jobs = max_jobs.max(1).min(32);
    let _ = LIMITER.set(Arc::new(UploadLimiter {
        max_jobs,
        budget: budget_bytes,
        state: Mutex::new(LimiterState { jobs: 0, used: 0 }),
        notify: Notify::new(),
    }));
    info!(
        upload_max_concurrent = max_jobs,
        upload_stream_budget = budget_bytes,
        stream_peak_bytes = STREAM_PEAK_BYTES,
        "upload concurrency limit (stream-friendly)"
    );
}

fn limiter() -> Arc<UploadLimiter> {
    LIMITER.get().cloned().unwrap_or_else(|| {
        Arc::new(UploadLimiter {
            max_jobs: 4,
            budget: 64 * 1024 * 1024,
            state: Mutex::new(LimiterState { jobs: 0, used: 0 }),
            notify: Notify::new(),
        })
    })
}

/// 是否还能再开一个占用 `peak` 的上传任务（纯逻辑，供单测）。
fn can_admit(jobs: usize, used: u64, max_jobs: usize, budget: u64, peak: u64) -> bool {
    if jobs >= max_jobs {
        return false;
    }
    if budget == 0 {
        return true;
    }
    used.saturating_add(peak) <= budget
}

/// 探测容器/主机可用内存（cgroup 优先），用于收紧流式上传并发预算。
pub fn detect_available_memory_bytes() -> Option<u64> {
    cgroup_v2_available()
        .or_else(cgroup_v1_available)
        .or_else(meminfo_available)
}

fn read_u64_file(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    let s = s.trim();
    if s == "max" || s.is_empty() {
        return None;
    }
    s.parse().ok()
}

fn cgroup_v2_available() -> Option<u64> {
    let max = read_u64_file("/sys/fs/cgroup/memory.max")?;
    let cur = read_u64_file("/sys/fs/cgroup/memory.current").unwrap_or(0);
    Some(max.saturating_sub(cur))
}

fn cgroup_v1_available() -> Option<u64> {
    let max = read_u64_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;
    if max > (1u64 << 62) {
        return None;
    }
    let cur = read_u64_file("/sys/fs/cgroup/memory/memory.usage_in_bytes").unwrap_or(0);
    Some(max.saturating_sub(cur))
}

fn meminfo_available() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

/// 流式上传并发预算：配置上限与「可用内存的一小部分」取小，保证低配也能传大文件。
pub fn resolve_stream_budget(configured: u64) -> u64 {
    const FLOOR: u64 = STREAM_PEAK_BYTES;
    let configured = configured.max(FLOOR);
    match detect_available_memory_bytes() {
        Some(avail) => {
            let auto = (avail / 8).max(FLOOR); // 预留目录缓存/预取等
            let budget = configured.min(auto);
            if budget < configured {
                info!(
                    configured = configured,
                    available = avail,
                    budget = budget,
                    "upload stream budget tightened by available memory"
                );
            }
            budget
        }
        None => configured,
    }
}

/// 收紧「上传接收」内存池，避免 `UPLOAD_MEMORY_LIMIT=1GB` 在 2GB 容器里直接被 cgroup OOMKill。
///
/// `configured==0` 表示始终落盘，原样返回。内核 OOMKill 是 SIGKILL，进程来不及打 error 日志。
pub fn resolve_upload_memory_limit(configured: u64) -> u64 {
    if configured == 0 {
        return 0;
    }
    const FLOOR: u64 = 8 * 1024 * 1024;
    match detect_available_memory_bytes() {
        Some(avail) => {
            // 接收池最多占可用内存 1/4，给预取缓存/目录缓存/基线留余量。
            let auto = (avail / 4).max(FLOOR);
            let limited = configured.min(auto);
            if limited < configured {
                tracing::warn!(
                    configured = configured,
                    available = avail,
                    effective = limited,
                    "UPLOAD_MEMORY_LIMIT tightened by available memory (cgroup OOMKill has no app-level error log)"
                );
            }
            limited
        }
        None => configured,
    }
}

/// 拿到名额后再开后台上传；流式路径默认占用 [`STREAM_PEAK_BYTES`]。
pub async fn acquire_upload_slot() -> UploadPermit {
    acquire_upload_slot_with_peak(STREAM_PEAK_BYTES).await
}

/// 自定义峰值占用（一般不必调；保留给测试或特殊路径）。
pub async fn acquire_upload_slot_with_peak(peak_bytes: u64) -> UploadPermit {
    let lim = limiter();
    let peak = peak_bytes.max(1);
    loop {
        {
            let mut st = lim.state.lock().unwrap_or_else(|e| e.into_inner());
            if can_admit(st.jobs, st.used, lim.max_jobs, lim.budget, peak) {
                st.jobs += 1;
                st.used = st.used.saturating_add(peak);
                return UploadPermit {
                    limiter: lim.clone(),
                    peak_bytes: peak,
                };
            }
        }
        lim.notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_admit_respects_budget() {
        assert!(can_admit(0, 0, 4, 3 * STREAM_PEAK_BYTES, STREAM_PEAK_BYTES));
        assert!(can_admit(2, 2 * STREAM_PEAK_BYTES, 4, 3 * STREAM_PEAK_BYTES, STREAM_PEAK_BYTES));
        assert!(!can_admit(3, 3 * STREAM_PEAK_BYTES, 4, 3 * STREAM_PEAK_BYTES, STREAM_PEAK_BYTES));
        assert!(!can_admit(4, 0, 4, 64 * STREAM_PEAK_BYTES, STREAM_PEAK_BYTES));
    }

    #[test]
    fn test_resolve_stream_budget_floor() {
        let b = resolve_stream_budget(100);
        assert!(b >= STREAM_PEAK_BYTES || b == 100.max(STREAM_PEAK_BYTES));
    }

    #[test]
    fn test_resolve_upload_memory_zero_means_spill() {
        assert_eq!(resolve_upload_memory_limit(0), 0);
    }
}
