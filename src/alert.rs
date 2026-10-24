//! 企业微信群机器人告警：凭证过期、刷新失败、API 报错时推送。

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// 同一类告警最短间隔，避免刷屏。
const DEDUP_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct Alert {
    inner: Option<Arc<AlertInner>>,
}

#[derive(Debug)]
struct AlertInner {
    webhook_key: String,
    client: reqwest::Client,
    last_sent: Mutex<Option<(String, Instant)>>,
}

impl Alert {
    /// `key` 为空则关闭告警。
    pub fn new(webhook_key: Option<String>) -> Self {
        let key = webhook_key
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let inner = key.map(|webhook_key| {
            Arc::new(AlertInner {
                webhook_key,
                client: reqwest::Client::new(),
                last_sent: Mutex::new(None),
            })
        });
        Self { inner }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// 推送文本告警；同类文案在 DEDUP_SECS 内只发一次。
    pub async fn notify(&self, title: &str, detail: &str) {
        let Some(inner) = &self.inner else {
            return;
        };
        let content = format!("【quarkdrive-webdav】{title}\n{detail}");
        let dedup_key = format!("{title}|{detail}");
        {
            let mut last = inner.last_sent.lock().await;
            if let Some((ref prev, at)) = *last {
                if prev == &dedup_key && at.elapsed() < Duration::from_secs(DEDUP_SECS) {
                    debug!(title = %title, "skip duplicate wecom alert");
                    return;
                }
            }
            *last = Some((dedup_key, Instant::now()));
        }
        if let Err(err) = inner.send(&content).await {
            warn!(error = %err, "wecom webhook send failed");
        }
    }
}

impl AlertInner {
    async fn send(&self, content: &str) -> Result<()> {
        let url = format!(
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={}",
            self.webhook_key
        );
        let body = json!({
            "msgtype": "text",
            "text": { "content": content }
        });
        let res = self.client.post(&url).json(&body).send().await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("wecom HTTP {status}: {text}");
        }
        // 企微业务错误：errcode != 0
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
            if v.get("errcode").and_then(|c| c.as_i64()).unwrap_or(0) != 0 {
                anyhow::bail!("wecom api error: {text}");
            }
        }
        Ok(())
    }
}
