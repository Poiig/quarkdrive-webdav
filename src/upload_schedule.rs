//! 大文件智能上传调度：FIFO 快车道（全速）+ 其余限速慢传。
//!
//! 多路大文件同时上传时，只让队列最前的一个吃满带宽；后面的缓慢推进，
//! 新任务只能排到队尾，不能插队。小文件不进此队列，避免拖慢目录里的零碎文件。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

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

static SCHEDULER: OnceLock<Arc<SmartScheduler>> = OnceLock::new();

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
    is_fast: AtomicBool,
    /// 慢传令牌桶：上次结算时刻与已透支字节。
    pace_lock: Mutex<PaceState>,
    slow_bps: u64,
}

struct PaceState {
    last: Instant,
    debt_ns: u64,
}

impl SmartUploadPermit {
    /// 小文件：不限速、不占快车道。
    fn small() -> Self {
        Self {
            sched: scheduler(),
            ticket: None,
            is_fast: AtomicBool::new(false),
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

    /// 每传完一段字节调用：慢传则按目标速率休眠；并尝试晋升为快车道。
    pub async fn pace(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.try_promote();
        if self.is_fast() {
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
        if sleep_for > Duration::ZERO {
            tokio::time::sleep(sleep_for).await;
            self.try_promote();
        }
    }

    /// 队首且快车道空闲时晋升为全速。
    fn try_promote(&self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        if self.is_fast.load(Ordering::Acquire) {
            return;
        }
        let mut st = self.sched.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.fast_holder.is_some() {
            return;
        }
        if st.order.front().copied() != Some(ticket) {
            return;
        }
        st.fast_holder = Some(ticket);
        self.is_fast.store(true, Ordering::Release);
        debug!(ticket, "upload promoted to fast lane");
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
        self.sched.notify.notify_waiters();
        debug!(ticket, "upload left smart schedule");
    }
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
                is_fast: AtomicBool::new(true),
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
        is_fast: AtomicBool::new(false),
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
