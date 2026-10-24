// ---------------------------------------------------------------------------
// 滑动窗口预取。
//
// ## 为什么需要它
//
// 夸克对**单条连接的持续吞吐**限速，不是对账号带宽限速。同一个 20 GB 文件、
// 同一条直链、同一时刻实测：
//
//   单流（不管块多大、URL 缓存多久）          ~10 Mbps
//   4 并发拉连续区间，持续 60 秒              303 Mbps
//
// 差 30 倍。这是标准的反下载工具设计：短请求满速，长连接掐死。而原来的
// `read_bytes` 是「一次读 = 一次上游请求」的串行模型，正好落在被惩罚的那一档。
//
// 调参救不了：`--cache-ttl 3600 -S 33554432` 之后 20 秒窗口能跑到 67 Mbps，
// **但 60 秒窗口回落到 8.9**——前 20 秒是爆发假象。所以必须改访问模式，不是改参数。
//
// ## 形状
//
// 维持 `ahead` 个 in-flight 的分块请求，`read_bytes` 从最前面那块里切片返回，
// 消费掉一块就补一块。播放器顺序读时窗口一直往前滚；seek 时窗口整个丢掉重建。
//
// 默认常数（可用环境变量覆盖）：
//
//   CHUNK = 16 MB   再小则请求数上去、每次 TTFB 的占比变大
//   AHEAD = 4       实测 8 并发反而掉到 62 Mbps（对端开始限流），4 是拐点
//
// 低配机器可把 PREFETCH_AHEAD=2、PREFETCH_CHUNK_MB=8、CHUNK_CACHE_MB=64。
// ---------------------------------------------------------------------------

use std::collections::VecDeque;
use std::sync::OnceLock;

use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::drive::QuarkDrive;

/// 默认一块多大（字节）。见文件头：16 MB 是实测的拐点。
pub const CHUNK: u64 = 16 * 1024 * 1024;
/// 默认同时在途几块。**不是越大越好**，8 并发实测反而更慢。
pub const AHEAD: usize = 4;
/// 块缓存的默认上限。见 `set_chunk_cache_bytes`。
pub const DEFAULT_CACHE_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct PrefetchConfig {
    pub chunk: u64,
    pub ahead: usize,
    pub download_slots: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            chunk: CHUNK,
            ahead: AHEAD,
            download_slots: AHEAD * 2,
        }
    }
}

static PREFETCH_CFG: OnceLock<PrefetchConfig> = OnceLock::new();
static DOWNLOAD_SLOTS: OnceLock<Semaphore> = OnceLock::new();

/// 进程启动时调用一次。chunk_mb / ahead 来自环境变量。
pub fn init_prefetch_config(chunk_mb: u64, ahead: usize) {
    let chunk_mb = chunk_mb.max(1).min(64);
    let ahead = ahead.clamp(1, 8);
    let cfg = PrefetchConfig {
        chunk: chunk_mb * 1024 * 1024,
        ahead,
        download_slots: ahead.saturating_mul(2).max(2),
    };
    let _ = PREFETCH_CFG.set(cfg);
    info!(
        chunk_mb = chunk_mb,
        ahead = ahead,
        download_slots = cfg.download_slots,
        "prefetch config"
    );
}

pub fn prefetch_config() -> PrefetchConfig {
    *PREFETCH_CFG.get().unwrap_or(&PrefetchConfig::default())
}

/// 已拉回的块，按 (fid, 块号) 共享。
///
/// **必须挂在文件系统层，不能挂在文件句柄上。** dav-server 每个 HTTP 请求都会走
/// 一遍 `open()` 造一个新的 DavFile，而播放器（实测 Infuse）每隔十几秒就换一条
/// 连接重发 range 请求。窗口跟着句柄一起死的话，已经拉回来但还没送出的块全部作废
/// ——实测放大比 1.35×，即 26% 的上游流量是白拉的。
pub type ChunkCache = moka::future::Cache<(String, u64), Bytes>;

/// 建一个按**字节数**限容的块缓存。
pub fn new_chunk_cache(max_bytes: u64) -> ChunkCache {
    moka::future::Cache::builder()
        .max_capacity(max_bytes)
        .weigher(|_k: &(String, u64), v: &Bytes| v.len().try_into().unwrap_or(u32::MAX))
        .time_to_live(std::time::Duration::from_secs(300))
        .build()
}

fn slots() -> &'static Semaphore {
    DOWNLOAD_SLOTS.get_or_init(|| Semaphore::new(prefetch_config().download_slots))
}

struct Pending {
    index: u64,
    task: JoinHandle<Option<Bytes>>,
}

pub struct Prefetcher {
    drive: QuarkDrive,
    url: String,
    fid: String,
    cache: ChunkCache,
    size: u64,
    chunk: u64,
    ahead: usize,
    inflight: VecDeque<Pending>,
    next_index: u64,
    ready: Option<(u64, Bytes)>,
}

impl Prefetcher {
    pub fn new(
        drive: QuarkDrive,
        url: String,
        fid: String,
        cache: ChunkCache,
        size: u64,
        pos: u64,
    ) -> Self {
        let cfg = prefetch_config();
        let mut p = Self {
            drive,
            url,
            fid,
            cache,
            size,
            chunk: cfg.chunk,
            ahead: cfg.ahead,
            inflight: VecDeque::new(),
            next_index: pos / cfg.chunk,
            ready: None,
        };
        p.fill();
        p
    }

    /// 同一 fid 且读位置仍在窗口内即可复用；URL 字符串变化（CDN 刷新）不整窗作废。
    pub fn serves(&self, _url: &str, fid: &str, pos: u64) -> bool {
        if self.fid != fid {
            return false;
        }
        let want = pos / self.chunk;
        let front = self
            .ready
            .as_ref()
            .map(|(i, _)| *i)
            .or_else(|| self.inflight.front().map(|p| p.index));
        match front {
            Some(f) => want >= f && want < self.next_index + self.ahead as u64,
            None => false,
        }
    }

    /// 复用窗口时刷新直链，后续补块走新 URL。
    pub fn set_url(&mut self, url: String) {
        if self.url != url {
            self.url = url;
        }
    }

    fn fill(&mut self) {
        while self.inflight.len() < self.ahead {
            let index = self.next_index;
            let start = index * self.chunk;
            if start >= self.size {
                break;
            }
            let len = std::cmp::min(self.chunk, self.size - start) as usize;
            let drive = self.drive.clone();
            let url = self.url.clone();
            let cache = self.cache.clone();
            let key = (self.fid.clone(), index);
            let task = tokio::spawn(async move {
                let res = cache
                    .try_get_with(key, async move {
                        let _permit = slots()
                            .acquire()
                            .await
                            .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;
                        drive.download(url, Some((start, len))).await
                    })
                    .await;
                match res {
                    Ok(b) => Some(b),
                    Err(e) => {
                        warn!(start = start, len = len, error = %e, "prefetch chunk failed");
                        None
                    }
                }
            });
            self.inflight.push_back(Pending { index, task });
            self.next_index += 1;
        }
    }

    pub async fn read(&mut self, pos: u64, count: usize) -> Option<Bytes> {
        let want = pos / self.chunk;

        loop {
            match &self.ready {
                Some((i, _)) if *i == want => break,
                _ => {
                    self.ready = None;
                    match self.inflight.pop_front() {
                        Some(p) => {
                            let data = p.task.await.ok().flatten()?;
                            if p.index == want {
                                self.ready = Some((p.index, data));
                                self.fill();
                                break;
                            }
                            self.fill();
                        }
                        None => {
                            debug!(pos = pos, "prefetch window exhausted");
                            return None;
                        }
                    }
                }
            }
        }

        let (index, data) = self.ready.as_ref()?;
        let offset = (pos - index * self.chunk) as usize;
        if offset >= data.len() {
            return None;
        }
        let end = std::cmp::min(offset + count, data.len());
        Some(data.slice(offset..end))
    }
}

impl Drop for Prefetcher {
    /// 窗口被丢弃时**不动在途任务**，让它们跑完进共享缓存。
    fn drop(&mut self) {}
}
