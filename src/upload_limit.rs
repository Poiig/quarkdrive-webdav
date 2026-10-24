//! 全局并行上传上限，避免多客户端同时上传把内存/连接打满。

use std::sync::{Arc, OnceLock};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::info;

static UPLOAD_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// 进程启动时调用。`max` 至少为 1。
pub fn init_upload_concurrency(max: usize) {
    let max = max.max(1).min(32);
    let _ = UPLOAD_SLOTS.set(Arc::new(Semaphore::new(max)));
    info!(upload_max_concurrent = max, "upload concurrency limit");
}

fn slots() -> Arc<Semaphore> {
    UPLOAD_SLOTS
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(Semaphore::new(4)))
}

/// 拿到名额后再开后台上传；Drop 时自动归还。
pub async fn acquire_upload_slot() -> OwnedSemaphorePermit {
    slots()
        .acquire_owned()
        .await
        .expect("upload semaphore closed")
}
