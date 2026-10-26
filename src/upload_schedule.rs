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

    /// 慢传任务的目标字节速率；全速/小文件返回 None（不限流）。
    pub fn throttle_bps(&self) -> Option<u64> {
        if self.ticket.is_some() && !self.is_fast() {
            Some(self.slow_bps.max(1))
        } else {
            None
        }
    }

    /// 供流式 PUT 使用的「是否已晋升全速」标志（限流读时可随时停限）。
    pub fn fast_flag(&self) -> Arc<AtomicBool> {
        self.is_fast.clone()
    }

    /// 上传 `bytes` **之前**调用（先付带宽再传）。
    /// 旧逻辑把 sleep 放在 PUT 之后：新慢传任务首片会全速抢带宽，拖累正在跑的快车道。
    pub async fn pace_before(&self, bytes: u64) {
        self.pace_inner(bytes, /*credit_elapsed=*/ false).await;
    }

    /// 兼容旧调用点：等价于 `pace_before`（应在 PUT 前调用）。
    pub async fn pace(&self, bytes: u64) {
        self.pace_before(bytes).await;
    }

    /// 流式读出一小段后调用：按慢传速率补休眠；已晋升则立刻返回。
    pub async fn pace_chunk(&self, bytes: u64) {
        if bytes == 0 || self.is_fast() {
            let _ = self.try_promote();
            return;
        }
        self.pace_inner(bytes, /*credit_elapsed=*/ true).await;
    }

    async fn pace_inner(&self, bytes: u64, credit_elapsed: bool) {
        if bytes == 0 {
            return;
        }
        if self.try_promote() || self.is_fast() {
            return;
        }
        let sleep_for = {
            let mut st = self.pace_lock.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            if credit_elapsed {
                let elapsed_ns = now.duration_since(st.last).as_nanos() as u64;
                st.debt_ns = st.debt_ns.saturating_sub(elapsed_ns);
            }
            st.last = now;
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

/// 控制台手动提升结果：是否成功，以及被挤回慢传的路径（用于刷新 UI）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromoteResult {
    pub ok: bool,
    pub demoted: Vec<String>,
}

/// 控制台手动把指定路径插到队首并抢走快车道；已是全速/小文件则失败。
pub fn promote_path(path: &str) -> PromoteResult {
    let map = path_lanes();
    let Some(binding) = map.get(path) else {
        return PromoteResult {
            ok: false,
            demoted: Vec::new(),
        };
    };
    let ticket = binding.ticket;
    let mine = binding.is_fast.clone();
    if mine.load(Ordering::Acquire) {
        return PromoteResult {
            ok: false,
            demoted: Vec::new(),
        };
    }
    drop(binding);

    let sched = scheduler();
    let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
    if !st.order.iter().any(|t| *t == ticket) {
        return PromoteResult {
            ok: false,
            demoted: Vec::new(),
        };
    }
    st.order.retain(|t| *t != ticket);
    st.order.push_front(ticket);
    let mut demoted = Vec::new();
    if let Some(old) = st.fast_holder {
        if old != ticket {
            for e in map.iter() {
                if e.ticket == old {
                    e.is_fast.store(false, Ordering::Release);
                    demoted.push(e.key().clone());
                }
            }
        }
    }
    st.fast_holder = Some(ticket);
    mine.store(true, Ordering::Release);
    drop(st);
    sched.notify.notify_waiters();
    debug!(ticket, path, demoted = ?demoted, "upload manually promoted to fast lane");
    PromoteResult { ok: true, demoted }
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
    use std::sync::Mutex as StdMutex;

    /// 全局调度器/路径表跨测共享；串行化并清状态，避免并行污染。
    static TEST_LOCK: StdMutex<()> = StdMutex::new(());

    fn reset_global_schedule() {
        init(DEFAULT_LARGE_THRESHOLD, DEFAULT_SLOW_BPS);
        let sched = scheduler();
        let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
        st.order.clear();
        st.fast_holder = None;
        st.next_ticket = 1;
        path_lanes().clear();
    }

    #[test]
    fn lane_labels() {
        assert_eq!(UploadLane::Fast.as_str(), "fast");
        assert_eq!(UploadLane::Slow.as_str(), "slow");
        assert_eq!(UploadLane::Small.as_str(), "small");
    }

    #[test]
    fn slow_lane_exposes_throttle_bps() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        init(1024, 256 * 1024);
        let sched = scheduler();
        let mut st = sched.state.lock().unwrap();
        let t = st.next_ticket;
        st.next_ticket += 1;
        st.order.push_back(t);
        st.fast_holder = Some(999); // 占住快车道，新票只能慢传
        drop(st);
        let p = SmartUploadPermit {
            sched: sched.clone(),
            ticket: Some(t),
            is_fast: Arc::new(AtomicBool::new(false)),
            pace_lock: Mutex::new(PaceState {
                last: Instant::now(),
                debt_ns: 0,
            }),
            slow_bps: 256 * 1024,
        };
        assert_eq!(p.lane(), UploadLane::Slow);
        assert_eq!(p.throttle_bps(), Some(256 * 1024));
        drop(p);
        // Drop 只清本票；假 fast_holder(999) 仍在，显式复位。
        reset_global_schedule();
    }

    #[test]
    fn small_file_skips_queue() {
        // 默认阈值 32MB，小文件不应占 ticket。
        let p = pollster_join_small();
        assert!(p.ticket.is_none());
        assert_eq!(p.lane(), UploadLane::Small);
        assert!(p.is_fast());
    }

    #[test]
    fn manual_promote_demotes_previous_fast() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        // 直接往 PATH_LANES / order 塞两票验证抢车道。
        init(1, 64 * 1024);
        let sched = scheduler();
        let fast_flag = Arc::new(AtomicBool::new(true));
        let slow_flag = Arc::new(AtomicBool::new(false));
        {
            let mut st = sched.state.lock().unwrap();
            st.order.clear();
            st.order.push_back(100);
            st.order.push_back(200);
            st.fast_holder = Some(100);
            st.next_ticket = 201;
        }
        let map = path_lanes();
        map.insert(
            "/fast.bin".into(),
            LaneBinding {
                ticket: 100,
                is_fast: fast_flag.clone(),
            },
        );
        map.insert(
            "/slow.bin".into(),
            LaneBinding {
                ticket: 200,
                is_fast: slow_flag.clone(),
            },
        );

        let result = promote_path("/slow.bin");
        assert!(result.ok);
        assert_eq!(result.demoted, vec!["/fast.bin".to_string()]);
        assert!(slow_flag.load(Ordering::Acquire));
        assert!(!fast_flag.load(Ordering::Acquire));
        {
            let st = sched.state.lock().unwrap();
            assert_eq!(st.fast_holder, Some(200));
            assert_eq!(st.order.front().copied(), Some(200));
        }

        reset_global_schedule();
    }

    fn pollster_join_small() -> SmartUploadPermit {
        // 单测里不用 tokio runtime：直接构造 small。
        SmartUploadPermit::small()
    }
}
