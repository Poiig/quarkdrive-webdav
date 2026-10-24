//! 大文件智能上传调度：FIFO 快车道（全速）+ 其余限速慢传。
//!
//! 多路大文件同时上传时，只让队列最前的一个吃满带宽；后面的缓慢推进，
//! 新任务只能排到队尾，不能插队。小文件不进此队列，避免拖慢目录里的零碎文件。
//! 控制台可对慢传任务「手动提升全速」（插到队首并抢走快车道）。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::debug;

/// 超过此大小走智能调度（默认 32MB）。
pub const DEFAULT_LARGE_THRESHOLD: u64 = 32 * 1024 * 1024;
/// 非快车道任务的目标速率（默认 512KB/s）。
pub const DEFAULT_SLOW_BPS: u64 = 512 * 1024;

struct QueueState {
    next_ticket: u64,
    /// 等待/持有慢传资格的 FIFO（含当前快车道持有者）。
    order: VecDeque<u64>,
    fast_holder: Option<u64>,
}

struct SmartScheduler {
    large_threshold: u64,
    slow_bps: u64,
    state: Mutex<QueueState>,
    notify: Notify,
}

/// 路径 → 票号/快慢标志，供控制台手动提升全速。
struct LaneBinding {
    ticket: u64,
    is_fast: Arc<AtomicBool>,
}

static SCHEDULER: OnceLock<Arc<SmartScheduler>> = OnceLock::new();
static PATH_LANES: OnceLock<DashMap<String, LaneBinding>> = OnceLock::new();

/// 进程启动时配置阈值与慢传速率；可重复调用（仅首次生效）。
pub fn init(large_threshold: u64, slow_bps: u64) {
    let large_threshold = large_threshold.max(1);
    let slow_bps = slow_bps.max(64 * 1024);
    let _ = SCHEDULER.set(Arc::new(SmartScheduler {
        large_threshold,
        slow_bps,
        state: Mutex::new(QueueState {
            next_ticket: 1,
            order: VecDeque::new(),
            fast_holder: None,
        }),
        notify: Notify::new(),
    }));
    tracing::info!(
        large_threshold,
        slow_bps,
        "upload smart schedule enabled (FIFO fast lane + slow trickle)"
    );
}

fn scheduler() -> Arc<SmartScheduler> {
    SCHEDULER.get().cloned().unwrap_or_else(|| {
        Arc::new(SmartScheduler {
            large_threshold: DEFAULT_LARGE_THRESHOLD,
            slow_bps: DEFAULT_SLOW_BPS,
            state: Mutex::new(QueueState {
                next_ticket: 1,
                order: VecDeque::new(),
                fast_holder: None,
            }),
            notify: Notify::new(),
        })
    })
}

fn path_lanes() -> &'static DashMap<String, LaneBinding> {
    PATH_LANES.get_or_init(DashMap::new)
}

/// 上传车道：小文件 / 大文件快车道 / 大文件慢传。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadLane {
    Small,
    Fast,
    Slow,
}

impl UploadLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Fast => "fast",
            Self::Slow => "slow",
        }
    }
}

/// 持有期内参与调度；Drop 时归还快车道并唤醒队首。
pub struct SmartUploadPermit {
    sched: Arc<SmartScheduler>,
    ticket: Option<u64>,
    is_fast: Arc<AtomicBool>,
    /// 慢传令牌桶：上次结算时刻与已透支字节。
    pace_lock: Mutex<PaceState>,
    slow_bps: u64,
}

struct PaceState {
    last: Instant,
    debt_ns: u64,
}

/// `bind_path` 的 RAII：任务结束时摘掉路径绑定，避免手动提升点到已结束任务。
pub struct PathBindGuard {
    path: String,
}

impl Drop for PathBindGuard {
    fn drop(&mut self) {
        path_lanes().remove(&self.path);
    }
}

impl SmartUploadPermit {
    /// 小文件：不限速、不占快车道。
    fn small() -> Self {
        Self {
            sched: scheduler(),
            ticket: None,
            is_fast: Arc::new(AtomicBool::new(false)),
            pace_lock: Mutex::new(PaceState {
                last: Instant::now(),
                debt_ns: 0,
            }),
            slow_bps: scheduler().slow_bps,
        }
    }

    pub fn lane(&self) -> UploadLane {
        if self.ticket.is_none() {
            return UploadLane::Small;
        }
        if self.is_fast.load(Ordering::Acquire) {
            UploadLane::Fast
        } else {
            UploadLane::Slow
        }
    }

    pub fn is_fast(&self) -> bool {
        self.ticket.is_none() || self.is_fast.load(Ordering::Acquire)
    }

    /// 把路径挂到本票号上，供控制台「提升全速」。
    pub fn bind_path(&self, path: &str) -> Option<PathBindGuard> {
        let ticket = self.ticket?;
        path_lanes().insert(
            path.to_string(),
            LaneBinding {
                ticket,
                is_fast: self.is_fast.clone(),
            },
        );
        Some(PathBindGuard {
            path: path.to_string(),
        })
    }

    /// 每传完一段字节调用：慢传则按目标速率休眠；快车道一空闲立刻被唤醒晋升。
    pub async fn pace(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if self.try_promote() || self.is_fast() {
            return;
        }
        let sleep_for = {
            let mut st = self.pace_lock.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let elapsed_ns = now.duration_since(st.last).as_nanos() as u64;
            st.last = now;
            st.debt_ns = st.debt_ns.saturating_sub(elapsed_ns);
            // need_ns = bytes * 1e9 / slow_bps
            let need_ns = bytes
                .saturating_mul(1_000_000_000)
                .checked_div(self.slow_bps.max(1))
                .unwrap_or(0);
            st.debt_ns = st.debt_ns.saturating_add(need_ns);
            let wait_ns = st.debt_ns;
            if wait_ns > 0 {
                st.debt_ns = 0;
                Duration::from_nanos(wait_ns)
            } else {
                Duration::ZERO
            }
        };
        if sleep_for == Duration::ZERO {
            let _ = self.try_promote();
            return;
        }
        // 可中断等待：快车道释放时 notify，不必睡满整段限速。
        let deadline = Instant::now() + sleep_for;
        while Instant::now() < deadline {
            if self.try_promote() || self.is_fast() {
                // 已晋升全速：清掉剩余债务，后续不再白睡。
                if let Ok(mut st) = self.pace_lock.lock() {
                    st.debt_ns = 0;
                    st.last = Instant::now();
                }
                return;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            // 最长 80ms 切片，兼顾唤醒延迟与 CPU。
            let slice = left.min(Duration::from_millis(80));
            tokio::select! {
                _ = tokio::time::sleep(slice) => {}
                _ = self.sched.notify.notified() => {}
            }
        }
        let _ = self.try_promote();
    }

    /// 队首且快车道空闲时晋升为全速；成功返回 true。
    fn try_promote(&self) -> bool {
        let Some(ticket) = self.ticket else {
            return false;
        };
        if self.is_fast.load(Ordering::Acquire) {
            return true;
        }
        let mut st = self.sched.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.fast_holder.is_some() {
            return false;
        }
        if st.order.front().copied() != Some(ticket) {
            return false;
        }
        st.fast_holder = Some(ticket);
        self.is_fast.store(true, Ordering::Release);
        debug!(ticket, "upload promoted to fast lane");
        true
    }
}

impl Drop for SmartUploadPermit {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        {
            let mut st = self.sched.state.lock().unwrap_or_else(|e| e.into_inner());
            st.order.retain(|t| *t != ticket);
            if st.fast_holder == Some(ticket) {
                st.fast_holder = None;
            }
        }
        // 立刻叫醒正在限速 sleep 的队首任务。
        self.sched.notify.notify_waiters();
        debug!(ticket, "upload left smart schedule");
    }
}

/// 控制台手动把指定路径插到队首并抢走快车道；已是全速/小文件则返回 false。
pub fn promote_path(path: &str) -> bool {
    let map = path_lanes();
    let Some(binding) = map.get(path) else {
        return false;
    };
    let ticket = binding.ticket;
    let mine = binding.is_fast.clone();
    if mine.load(Ordering::Acquire) {
        return false;
    }
    drop(binding);

    let sched = scheduler();
    let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
    if !st.order.iter().any(|t| *t == ticket) {
        return false;
    }
    st.order.retain(|t| *t != ticket);
    st.order.push_front(ticket);
    if let Some(old) = st.fast_holder {
        if old != ticket {
            for e in map.iter() {
                if e.ticket == old {
                    e.is_fast.store(false, Ordering::Release);
                }
            }
        }
    }
    st.fast_holder = Some(ticket);
    mine.store(true, Ordering::Release);
    drop(st);
    sched.notify.notify_waiters();
    debug!(ticket, path, "upload manually promoted to fast lane");
    true
}

/// 加入智能调度：大文件拿 FIFO 票并立即尝试占快车道；小文件直接放行。
pub async fn join(size: u64) -> SmartUploadPermit {
    let sched = scheduler();
    if size < sched.large_threshold {
        return SmartUploadPermit::small();
    }
    let ticket = {
        let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
        let t = st.next_ticket;
        st.next_ticket = st.next_ticket.saturating_add(1).max(1);
        st.order.push_back(t);
        // 空闲则队首立刻占快车道。
        if st.fast_holder.is_none() && st.order.front().copied() == Some(t) {
            st.fast_holder = Some(t);
            debug!(ticket = t, size, "upload entered fast lane");
            let permit = SmartUploadPermit {
                sched: sched.clone(),
                ticket: Some(t),
                is_fast: Arc::new(AtomicBool::new(true)),
                pace_lock: Mutex::new(PaceState {
                    last: Instant::now(),
                    debt_ns: 0,
                }),
                slow_bps: sched.slow_bps,
            };
            return permit;
        }
        debug!(ticket = t, size, "upload entered slow lane (queued)");
        t
    };
    SmartUploadPermit {
        sched,
        ticket: Some(ticket),
        is_fast: Arc::new(AtomicBool::new(false)),
        pace_lock: Mutex::new(PaceState {
            last: Instant::now(),
            debt_ns: 0,
        }),
        slow_bps: scheduler().slow_bps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_labels() {
        assert_eq!(UploadLane::Fast.as_str(), "fast");
        assert_eq!(UploadLane::Slow.as_str(), "slow");
        assert_eq!(UploadLane::Small.as_str(), "small");
    }

    #[test]
    fn small_file_skips_queue() {
        // 默认阈值 32MB，小文件不应占 ticket。
        let p = pollster_join_small();
        assert!(p.ticket.is_none());
        assert_eq!(p.lane(), UploadLane::Small);
        assert!(p.is_fast());
    }

    fn pollster_join_small() -> SmartUploadPermit {
        // 单测里不用 tokio runtime：直接构造 small。
        SmartUploadPermit::small()
    }
}
