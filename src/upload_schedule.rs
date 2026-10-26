//! 大文件智能上传调度：FIFO 快车道（全速）+ 其余限速慢传。
//!
//! 多路大文件同时上传时，只让队列最前的一个吃满带宽；后面的缓慢推进，
//! 新任务只能排到队尾，不能插队。小文件不进此队列，避免拖慢目录里的零碎文件。
//! 控制台可对慢传任务「手动提升全速」（插到队首并抢走快车道）。
//!
//! 不变量：队列非空时，队首就是快车道。入队、出队、手动提升都在同一把锁里
//! 改写 `is_fast`。慢传任务（尤其是 Open 分片流）只观察这个标志，不必自己轮询
//! 「能不能升」——否则快车道空出来时，正在限速的流永远不会把自己标成全速。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::{debug, info};

/// 超过此大小走智能调度（默认 32MB）。
pub const DEFAULT_LARGE_THRESHOLD: u64 = 32 * 1024 * 1024;
/// 非快车道任务的目标速率（默认 512KB/s）。
pub const DEFAULT_SLOW_BPS: u64 = 512 * 1024;

struct QueueEntry {
    ticket: u64,
    /// 与 permit / 路径绑定 / 流式限速共享，出队时直接翻转。
    is_fast: Arc<AtomicBool>,
}

struct QueueState {
    next_ticket: u64,
    /// FIFO。非空时 `order[0]` 就是快车道持有者。
    order: VecDeque<QueueEntry>,
    fast_holder: Option<u64>,
}

struct SmartScheduler {
    large_threshold: u64,
    slow_bps: u64,
    state: Mutex<QueueState>,
    notify: Arc<Notify>,
}

struct LaneFlip {
    promoted: Option<u64>,
    demoted: Vec<u64>,
}

/// 路径 → 票号/快慢标志，供控制台手动提升全速。
struct LaneBinding {
    ticket: u64,
    is_fast: Arc<AtomicBool>,
}

static SCHEDULER: OnceLock<Arc<SmartScheduler>> = OnceLock::new();
static PATH_LANES: OnceLock<DashMap<String, LaneBinding>> = OnceLock::new();
static LANE_LISTENER: Mutex<Option<Arc<dyn Fn(&str, &str) + Send + Sync>>> = Mutex::new(None);

/// 进程启动时配置阈值与慢传速率；可重复调用（仅首次生效）。
pub fn init(large_threshold: u64, slow_bps: u64) {
    let large_threshold = large_threshold.max(1);
    let slow_bps = slow_bps.max(64 * 1024);
    if SCHEDULER
        .set(Arc::new(new_scheduler(large_threshold, slow_bps)))
        .is_ok()
    {
        tracing::info!(
            large_threshold,
            slow_bps,
            "upload smart schedule enabled (FIFO fast lane + slow trickle)"
        );
    }
}

fn new_scheduler(large_threshold: u64, slow_bps: u64) -> SmartScheduler {
    SmartScheduler {
        large_threshold,
        slow_bps,
        state: Mutex::new(QueueState {
            next_ticket: 1,
            order: VecDeque::new(),
            fast_holder: None,
        }),
        notify: Arc::new(Notify::new()),
    }
}

fn scheduler() -> Arc<SmartScheduler> {
    SCHEDULER
        .get_or_init(|| Arc::new(new_scheduler(DEFAULT_LARGE_THRESHOLD, DEFAULT_SLOW_BPS)))
        .clone()
}

/// 车道变化时刷新控制台标签。后注册的覆盖先前的（进程内只有一个文件系统）。
pub fn set_lane_listener(listener: Arc<dyn Fn(&str, &str) + Send + Sync>) {
    if let Ok(mut slot) = LANE_LISTENER.lock() {
        *slot = Some(listener);
    }
}

fn path_lanes() -> &'static DashMap<String, LaneBinding> {
    PATH_LANES.get_or_init(DashMap::new)
}

/// 队首占快车道，其余一律慢传。调用方必须已持有队列锁。
fn ensure_fast_holder(st: &mut QueueState) -> LaneFlip {
    let mut flip = LaneFlip {
        promoted: None,
        demoted: Vec::new(),
    };
    let Some(front_ticket) = st.order.front().map(|e| e.ticket) else {
        st.fast_holder = None;
        return flip;
    };
    for entry in st.order.iter_mut().skip(1) {
        if entry.is_fast.swap(false, Ordering::AcqRel) {
            flip.demoted.push(entry.ticket);
        }
    }
    if let Some(front) = st.order.front_mut() {
        if !front.is_fast.swap(true, Ordering::AcqRel) {
            flip.promoted = Some(front.ticket);
        }
        st.fast_holder = Some(front.ticket);
    }
    debug_assert_eq!(st.fast_holder, Some(front_ticket));
    flip
}

fn move_to_front(st: &mut QueueState, ticket: u64) -> bool {
    let Some(pos) = st.order.iter().position(|e| e.ticket == ticket) else {
        return false;
    };
    if pos > 0 {
        let entry = st.order.remove(pos).expect("ticket position still valid");
        st.order.push_front(entry);
    }
    true
}

fn paths_for_ticket(ticket: u64) -> Vec<String> {
    path_lanes()
        .iter()
        .filter(|entry| entry.ticket == ticket)
        .map(|entry| entry.key().clone())
        .collect()
}

fn apply_flip(sched: &SmartScheduler, flip: LaneFlip, log_promote: bool) {
    if flip.promoted.is_none() && flip.demoted.is_empty() {
        return;
    }
    let mut updates: Vec<(String, &'static str)> = Vec::new();
    if let Some(ticket) = flip.promoted {
        let paths = paths_for_ticket(ticket);
        if log_promote {
            info!(ticket, ?paths, "upload promoted to fast lane");
        }
        for path in paths {
            updates.push((path, "fast"));
        }
    }
    for ticket in flip.demoted {
        let paths = paths_for_ticket(ticket);
        if log_promote {
            info!(ticket, ?paths, "upload demoted to slow lane");
        }
        for path in paths {
            updates.push((path, "slow"));
        }
    }
    // 先叫醒正在限速的流/pace，再刷 UI。
    sched.notify.notify_waiters();
    if updates.is_empty() {
        return;
    }
    let listener = LANE_LISTENER.lock().ok().and_then(|slot| slot.clone());
    if let Some(listener) = listener {
        for (path, lane) in &updates {
            listener(path, lane);
        }
    }
}

/// 分片流限速：看 `is_fast`，不要在开传时把「当前是慢传」拍成死速率。
#[derive(Clone)]
pub struct ByteThrottle {
    pub bps: u64,
    pub is_fast: Arc<AtomicBool>,
    pub notify: Arc<Notify>,
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
        path_lanes().insert(path.to_string(), LaneBinding {
            ticket,
            is_fast: self.is_fast.clone(),
        });
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

    /// 对齐队列：若本票已是队首，立刻标成全速。分片循环在每片开始时调用。
    pub fn sync_lane(&self) -> UploadLane {
        let _ = self.try_promote();
        self.lane()
    }

    /// 大文件分片流的实时限速参数。全速时流内不休眠，被挤回慢传后下一小块即限速。
    pub fn stream_throttle(&self) -> Option<ByteThrottle> {
        if self.ticket.is_none() {
            return None;
        }
        Some(ByteThrottle {
            bps: self.slow_bps.max(1),
            is_fast: self.is_fast.clone(),
            notify: Arc::clone(&self.sched.notify),
        })
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

    /// 本票已是队首时确保标志为全速。成功（当前就是快车道）返回 true。
    fn try_promote(&self) -> bool {
        let Some(ticket) = self.ticket else {
            return false;
        };
        if self.is_fast.load(Ordering::Acquire) {
            return true;
        }
        let flip = {
            let mut st = self.sched.state.lock().unwrap_or_else(|e| e.into_inner());
            let is_front = st.order.front().map(|entry| entry.ticket) == Some(ticket);
            if !is_front {
                return false;
            }
            ensure_fast_holder(&mut st)
        };
        let became = self.is_fast.load(Ordering::Acquire);
        if became {
            apply_flip(&self.sched, flip, true);
        }
        became
    }
}

impl Drop for SmartUploadPermit {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let flip = {
            let mut st = self.sched.state.lock().unwrap_or_else(|e| e.into_inner());
            st.order.retain(|entry| entry.ticket != ticket);
            // 队首离开后，下一位在这把锁里直接升全速，不等它自己来问。
            ensure_fast_holder(&mut st)
        };
        apply_flip(&self.sched, flip, true);
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
    let flip = {
        let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
        if !move_to_front(&mut st, ticket) {
            return PromoteResult {
                ok: false,
                demoted: Vec::new(),
            };
        }
        ensure_fast_holder(&mut st)
    };
    let demoted = flip
        .demoted
        .iter()
        .flat_map(|ticket| paths_for_ticket(*ticket))
        .collect::<Vec<_>>();
    let ok = mine.load(Ordering::Acquire);
    apply_flip(&sched, flip, true);
    debug!(
        ticket,
        path,
        ?demoted,
        ok,
        "upload manually promoted to fast lane"
    );
    PromoteResult { ok, demoted }
}

/// 加入智能调度：大文件拿 FIFO 票；队首立即全速，其余慢传。小文件直接放行。
pub async fn join(size: u64) -> SmartUploadPermit {
    join_now(size)
}

fn join_now(size: u64) -> SmartUploadPermit {
    let sched = scheduler();
    if size < sched.large_threshold {
        return SmartUploadPermit::small();
    }
    let is_fast = Arc::new(AtomicBool::new(false));
    let ticket = {
        let mut st = sched.state.lock().unwrap_or_else(|e| e.into_inner());
        let ticket = st.next_ticket;
        st.next_ticket = st.next_ticket.saturating_add(1).max(1);
        st.order.push_back(QueueEntry {
            ticket,
            is_fast: Arc::clone(&is_fast),
        });
        let flip = ensure_fast_holder(&mut st);
        drop(st);
        // 刚入队还没 bind_path，这里只改标志；UI 由调用方 set_lane。
        apply_flip(&sched, flip, false);
        ticket
    };
    if is_fast.load(Ordering::Acquire) {
        debug!(ticket, size, "upload entered fast lane");
    } else {
        debug!(ticket, size, "upload entered slow lane (queued)");
    }
    SmartUploadPermit {
        sched: sched.clone(),
        ticket: Some(ticket),
        is_fast,
        pace_lock: Mutex::new(PaceState {
            last: Instant::now(),
            debt_ns: 0,
        }),
        slow_bps: sched.slow_bps,
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

    fn big() -> u64 {
        // 不依赖 init 是否已经用别的阈值抢先注册。
        scheduler().large_threshold.saturating_add(1)
    }

    #[test]
    fn slow_lane_exposes_throttle_bps() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        let fast = join_now(big());
        let slow = join_now(big());
        assert_eq!(fast.lane(), UploadLane::Fast);
        assert_eq!(slow.lane(), UploadLane::Slow);
        assert_eq!(slow.throttle_bps(), Some(slow.slow_bps));
        assert!(fast.throttle_bps().is_none());
        assert!(slow.stream_throttle().is_some());
        drop(slow);
        drop(fast);
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
    fn dropping_fast_promotes_next_without_polling() {
        // 回归：Open 流只读 is_fast，不会自己调用 try_promote。
        // 快车道释放时，下一位的标志必须已经被调度器写上。
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        let fast = join_now(big());
        let next = join_now(big());
        let tail = join_now(big());
        let watched = next.stream_throttle().expect("大文件应带实时限速标志");
        assert!(fast.is_fast());
        assert!(!next.is_fast());
        assert!(!watched.is_fast.load(Ordering::Acquire));
        assert!(!tail.is_fast());
        drop(fast);
        assert!(next.is_fast(), "队首应在上一位 Drop 时直接升全速");
        assert!(
            watched.is_fast.load(Ordering::Acquire),
            "分片流看到的标志必须和调度器是同一个"
        );
        assert!(!tail.is_fast());
        assert_eq!(next.lane(), UploadLane::Fast);
        assert!(next.throttle_bps().is_none());
        drop(next);
        assert!(tail.is_fast());
        drop(tail);
        reset_global_schedule();
    }

    #[test]
    fn dropping_slow_does_not_steal_fast_lane() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        let fast = join_now(big());
        let slow = join_now(big());
        let tail = join_now(big());
        drop(slow);
        assert!(fast.is_fast());
        assert!(!tail.is_fast());
        drop(tail);
        drop(fast);
        reset_global_schedule();
    }

    #[test]
    fn manual_promote_demotes_previous_fast() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_global_schedule();
        let fast = join_now(big());
        let mid = join_now(big());
        let slow = join_now(big());
        let _fast_bind = fast.bind_path("/fast.bin");
        let _mid_bind = mid.bind_path("/mid.bin");
        let _slow_bind = slow.bind_path("/slow.bin");

        let result = promote_path("/slow.bin");
        assert!(result.ok);
        assert_eq!(result.demoted, vec!["/fast.bin".to_string()]);
        assert!(slow.is_fast());
        assert!(!fast.is_fast());
        assert!(!mid.is_fast());
        {
            let sched = scheduler();
            let st = sched.state.lock().unwrap();
            assert_eq!(st.fast_holder, slow.ticket);
            assert_eq!(st.order.front().map(|entry| entry.ticket), slow.ticket);
        }

        drop(slow);
        assert!(fast.is_fast(), "被手动提升的任务结束后，原队首回到快车道");
        assert!(!mid.is_fast());
        drop(fast);
        drop(mid);
        reset_global_schedule();
    }

    fn pollster_join_small() -> SmartUploadPermit {
        // 单测里不用 tokio runtime：直接构造 small。
        SmartUploadPermit::small()
    }
}
