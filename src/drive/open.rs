//! 夸克 Open API（对齐 OpenList `drivers/quark_open`）。
//! Cookie 模式走 `drive.quark.cn`；Open 模式走 `open-api-drive.quark.cn`，
//! 用 AppId/SignKey 签请求头，并用 refresh_token 续期 access_token。

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::alert::Alert;

use super::model::QuarkFile;
use super::get_format_type;

/// Open API 默认基址（与 OpenList Conf.api 一致）。
pub const OPEN_API_BASE: &str = "https://open-api-drive.quark.cn";
/// 在线刷新默认地址（与 OpenList meta 默认值一致）。只走这条，不用飞牛刷新。
pub const DEFAULT_RENEW_API: &str = "https://api.oplist.org/quarkyun/renewapi";
/// 飞牛 OAuth 获取授权页。扫码登录仍走飞牛，刷新不走。
const FNNAS_AUTH_URL: &str = "https://oauth.fnnas.com/api/v1/oauth/getAuthUrl";
/// 飞牛 OAuth 用 nonce 换 token。
const FNNAS_EXCHANGE_URL: &str = "https://oauth.fnnas.com/api/v1/oauth/exchangeToken";
const FNNAS_TRIM_APP_ID: &str = "com.trim.cloudstorage";
const OPEN_UA: &str = "quarkdrive-webdav/open";

/// 内置飞牛夸克 Open 应用凭证（不对外暴露为配置项）。
pub const BUILTIN_APP_ID: &str = "93b14d40cbef4e5d91945ec93d26fe8f";
pub const BUILTIN_SIGN_KEY: &str = "6d27122d2d7b41598a1dbb9b5f605e00";

/// Open 模式运行时凭证；token 可在刷新后写回，故用 RwLock 共享。
/// AppId/SignKey 也放进 Arc：扫码换票后要让已经克隆出去的客户端一起换签。
#[derive(Debug, Clone)]
pub struct OpenCredentials {
    pub app_id: Arc<StdRwLock<String>>,
    pub sign_key: Arc<StdRwLock<String>>,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    /// 刷新后可选落盘，避免进程重启后仍用已轮换的 refresh_token。
    pub token_file: Option<String>,
    pub user_id: Arc<RwLock<String>>,
    /// Open API 并发上限。默认 2，避免建目录把接口打出 429。
    pub api_max_concurrent: usize,
    /// 两次 Open 请求的最小间隔（毫秒）。
    pub api_min_interval_ms: u64,
}

/// 磁盘上的 token 文件格式，便于重启后复用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenFile {
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub sign_key: Option<String>,
}

impl OpenCredentials {
    /// 签名固定用内置 AppId/SignKey（与授权页 client_id 成对）。
    /// token 文件只恢复 access/refresh。文件里的密钥若和内置不一致，拿去签名会 10001。
    /// token_file / refresh_token 任一有值即可启动，都没有时仍可起服务，由 Web 页扫码授权补齐。
    pub fn new(refresh_token: String, token_file: Option<String>) -> Result<Self> {
        let mut access = String::new();
        let mut refresh = refresh_token;
        if let Some(ref path) = token_file {
            if Path::new(path).exists() {
                let raw = std::fs::read_to_string(path)
                    .with_context(|| format!("读取 token 文件失败: {path}"))?;
                let saved: TokenFile = serde_json::from_str(&raw)
                    .with_context(|| format!("解析 token 文件失败: {path}"))?;
                if !saved.access_token.is_empty() {
                    access = saved.access_token;
                }
                if !saved.refresh_token.is_empty() {
                    refresh = saved.refresh_token;
                }
                let foreign_key = saved
                    .sign_key
                    .as_deref()
                    .is_some_and(|k| !k.is_empty() && k != BUILTIN_SIGN_KEY)
                    || saved
                        .app_id
                        .as_deref()
                        .is_some_and(|id| !id.is_empty() && id != BUILTIN_APP_ID);
                if foreign_key {
                    // 上次刷新把不成对的 signKey 写进了文件，这里明确丢掉。
                    tracing::warn!(
                        path,
                        "token 文件中的 AppId/SignKey 与内置签名不一致，已忽略"
                    );
                }
            }
        }
        Ok(Self {
            app_id: Arc::new(StdRwLock::new(BUILTIN_APP_ID.to_string())),
            sign_key: Arc::new(StdRwLock::new(BUILTIN_SIGN_KEY.to_string())),
            access_token: Arc::new(RwLock::new(access)),
            refresh_token: Arc::new(RwLock::new(refresh)),
            token_file,
            user_id: Arc::new(RwLock::new(String::new())),
            api_max_concurrent: 2,
            api_min_interval_ms: 200,
        })
    }

    fn app_id_snapshot(&self) -> String {
        self.app_id
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn sign_key_snapshot(&self) -> String {
        self.sign_key
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub async fn has_refresh_token(&self) -> bool {
        !self.refresh_token.read().await.is_empty()
    }

    /// 下载直链需要的 Cookie，与 OpenList `generateAuthCookie` 一致。
    pub async fn auth_cookie(&self) -> String {
        let access = self.access_token.read().await.clone();
        format!(
            "x_pan_client_id={}; x_pan_access_token={}",
            self.app_id_snapshot(),
            access
        )
    }

    /// 刷新成功后写回 token 文件，保证下次启动仍能续期。
    async fn persist_tokens(&self) -> Result<()> {
        let Some(ref path) = self.token_file else {
            return Ok(());
        };
        let data = TokenFile {
            access_token: self.access_token.read().await.clone(),
            refresh_token: self.refresh_token.read().await.clone(),
            app_id: Some(self.app_id_snapshot()),
            sign_key: Some(self.sign_key_snapshot()),
        };
        let json = serde_json::to_string_pretty(&data)?;
        let path_obj = Path::new(path);
        if let Some(parent) = path_obj.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "创建 token 目录失败: {}（请检查挂载卷权限，Docker 需可写）",
                        parent.display()
                    )
                })?;
            }
        }
        // 先写临时文件再 rename，避免写一半崩溃留下空文件。
        let tmp = format!("{path}.tmp");
        std::fs::write(&tmp, &json).with_context(|| {
            format!(
                "写入 token 文件失败: {tmp}（常见原因：容器用户对目录无写权限，请 chown/chmod 挂载目录）"
            )
        })?;
        std::fs::rename(&tmp, path).with_context(|| {
            format!("落盘 token 文件失败: {path}（rename 权限不足？）")
        })?;
        Ok(())
    }
}

/// 手动签名参数：upload_pre 的 proof 依赖同一份 x-pan-token，需与请求头一致。
#[derive(Debug, Clone)]
struct ManualSign {
    tm: String,
    token: String,
    req_id: String,
}

/// Open API 公共错误体。
#[derive(Debug, Clone, Deserialize)]
struct OpenErrorBody {
    #[serde(default)]
    status: i32,
    #[serde(default)]
    errno: i32,
    #[serde(default)]
    error_info: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UserInfoResp {
    #[serde(default)]
    data: UserInfoData,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct UserInfoData {
    #[serde(default)]
    user_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenFile {
    fid: String,
    #[serde(default)]
    parent_fid: String,
    #[serde(default, deserialize_with = "deserialize_filename")]
    filename: String,
    #[serde(default)]
    size: u64,
    /// `"0"` 表示目录（OpenList fileToObj）。
    #[serde(default)]
    file_type: String,
    #[serde(default)]
    content_hash: Option<String>,
    #[serde(default)]
    thumbnail_url: Option<String>,
    #[serde(default)]
    created_at: u64,
    #[serde(default)]
    updated_at: u64,
}

fn deserialize_filename<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let unescaped = htmlescape::decode_html(&s).unwrap_or(s);
    Ok(crate::util::quark_decode_name(&unescaped))
}

impl From<OpenFile> for QuarkFile {
    fn from(f: OpenFile) -> Self {
        let is_dir = f.file_type == "0";
        let format_type = if is_dir {
            String::new()
        } else {
            get_format_type(&f.filename).to_string()
        };
        QuarkFile {
            fid: f.fid,
            file_name: f.filename,
            pdir_fid: f.parent_fid,
            size: f.size,
            format_type,
            status: 1,
            created_at: f.created_at,
            updated_at: f.updated_at,
            dir: is_dir,
            file: !is_dir,
            download_url: None,
            content_hash: f.content_hash,
            parent_path: None,
            thumbnail_url: f.thumbnail_url.filter(|s| !s.is_empty()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct QueryCursor {
    #[serde(default)]
    version: String,
    #[serde(default)]
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FileListResp {
    data: FileListData,
}

#[derive(Debug, Clone, Deserialize)]
struct FileListData {
    #[serde(default)]
    file_list: Vec<OpenFile>,
    #[serde(default)]
    last_page: bool,
    #[serde(default)]
    next_query_cursor: QueryCursor,
}

#[derive(Debug, Clone, Deserialize)]
struct DownloadUrlResp {
    data: DownloadUrlData,
}

#[derive(Debug, Clone, Deserialize)]
struct DownloadUrlData {
    #[serde(default)]
    download_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UpPreResp {
    data: UpPreData,
}

#[derive(Debug, Clone, Deserialize)]
struct UpPreData {
    #[serde(default)]
    finish: bool,
    #[serde(default)]
    task_id: String,
    #[serde(default)]
    fid: String,
    #[serde(default)]
    part_size: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct UpUrlResp {
    data: UpUrlInfo,
}

#[derive(Debug, Clone, Deserialize)]
struct UpUrlInfo {
    #[serde(default)]
    upload_urls: Vec<UploadUrlItem>,
    #[serde(default)]
    common_headers: CommonOssHeaders,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CommonOssHeaders {
    #[serde(rename = "X-Oss-Content-Sha256", default)]
    x_oss_content_sha256: String,
    #[serde(rename = "X-Oss-Date", default)]
    x_oss_date: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UploadUrlItem {
    #[serde(default)]
    part_number: i32,
    #[serde(default)]
    upload_url: String,
    #[serde(default)]
    signature_info: SignatureInfo,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SignatureInfo {
    #[serde(default)]
    signature: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UpFinishResp {
    data: UpFinishData,
}

#[derive(Debug, Clone, Deserialize)]
struct UpFinishData {
    #[serde(default)]
    finish: bool,
    #[serde(default)]
    task_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OnlineRenewResp {
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FnnasResp<T> {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    msg: String,
    data: Option<T>,
}

#[derive(Debug, Clone, Deserialize)]
struct FnnasAuthData {
    #[serde(rename = "authUrlWithNonce", default)]
    auth_url_with_nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FnnasTokenData {
    #[serde(rename = "accessToken", default)]
    access_token: String,
    #[serde(rename = "refreshToken", default)]
    refresh_token: String,
    #[serde(rename = "appId", default)]
    app_id: Option<String>,
    #[serde(rename = "signKey", default)]
    sign_key: Option<String>,
}

/// 生成 `x-pan-tm` / `x-pan-token` / `req_id`（OpenList generateReqSign）。
pub fn generate_req_sign(method: &str, pathname: &str, sign_key: &str) -> (String, String, String) {
    let tm = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string();
    let token_data = format!("{method}&{pathname}&{tm}&{sign_key}");
    let hash = Sha256::digest(token_data.as_bytes());
    let token = hex::encode(hash);
    let req_id = Uuid::new_v4().to_string();
    (tm, token, req_id)
}

/// 全局限流：并发上限 + 最小间隔，降低 CloudSync 扫目录时的 429。
#[derive(Debug)]
struct OpenApiGate {
    sem: Semaphore,
    last_start: Mutex<Option<Instant>>,
    min_interval: Duration,
}

impl OpenApiGate {
    fn new(max_concurrent: usize, min_interval_ms: u64) -> Self {
        Self {
            sem: Semaphore::new(max_concurrent.max(1)),
            last_start: Mutex::new(None),
            min_interval: Duration::from_millis(min_interval_ms.max(1)),
        }
    }

    async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        let permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("Open API 限流信号量已关闭"))?;
        {
            let mut last = self.last_start.lock().await;
            if let Some(prev) = *last {
                let elapsed = prev.elapsed();
                if elapsed < self.min_interval {
                    tokio::time::sleep(self.min_interval - elapsed).await;
                }
            }
            *last = Some(Instant::now());
        }
        Ok(permit)
    }
}

/// Open API 客户端：挂在 `QuarkDrive` 上，负责签名请求与上传。
#[derive(Debug, Clone)]
pub struct OpenClient {
    pub creds: OpenCredentials,
    pub api_base: String,
    alert: Alert,
    /// 授权探测成功后的短缓存，避免每次浏览都打 user/info。
    auth_ok_until: Arc<Mutex<Option<Instant>>>,
    /// 刷新 token 单飞，避免并发请求把日志刷爆。
    refresh_lock: Arc<Mutex<()>>,
    /// Open API 调用节流（含 list/mkdir）。
    api_gate: Arc<OpenApiGate>,
}

impl OpenClient {
    pub fn new(creds: OpenCredentials, alert: Alert) -> Self {
        let max_concurrent = creds.api_max_concurrent.max(1);
        let min_interval_ms = creds.api_min_interval_ms.max(1);
        Self {
            creds,
            api_base: OPEN_API_BASE.to_string(),
            alert,
            auth_ok_until: Arc::new(Mutex::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
            // 默认 2 路 + 200ms：同步建大量目录时比无节流少打很多 list。
            api_gate: Arc::new(OpenApiGate::new(max_concurrent, min_interval_ms)),
        }
    }

    /// 确保有可用 access_token：全空则直接提示扫码，不打 Open API。
    async fn ensure_access_token(&self, client: &ClientWithMiddleware) -> Result<()> {
        if !self.creds.access_token.read().await.is_empty() {
            return Ok(());
        }
        if !self.creds.has_refresh_token().await {
            bail!("尚未授权，请打开 Web 页扫码登录");
        }
        let _guard = self.refresh_lock.lock().await;
        if !self.creds.access_token.read().await.is_empty() {
            return Ok(());
        }
        self.refresh_token(client).await
    }

    /// 启动时拉 user/info，缓存 user_id（上传 proof_seed1 需要）。
    pub async fn init(&self, client: &ClientWithMiddleware) -> Result<()> {
        let mut resp: UserInfoResp = self
            .request(client, "/open/v1/user/info", "GET", None, None)
            .await?;
        if resp.data.user_id.is_empty() {
            bail!("Open API 未能获取 user_id");
        }
        *self.creds.user_id.write().await = std::mem::take(&mut resp.data.user_id);
        Ok(())
    }

    /// 列出目录下全部文件（cursor 分页拉完，供现有 page 缓存一次性吃掉）。
    pub async fn list_all_files(
        &self,
        client: &ClientWithMiddleware,
        parent_fid: &str,
    ) -> Result<Vec<QuarkFile>> {
        let mut files = Vec::new();
        let mut cursor = QueryCursor::default();
        loop {
            let mut body = json!({
                "parent_fid": parent_fid,
                "size": 100,
                "sort": "file_name:asc",
            });
            if !cursor.token.is_empty() {
                body["query_cursor"] = serde_json::to_value(&cursor)?;
            }
            let resp: FileListResp = self
                .request(
                    client,
                    "/open/v1/file/list",
                    "POST",
                    Some(body),
                    None,
                )
                .await?;
            files.extend(resp.data.file_list.into_iter().map(QuarkFile::from));
            if resp.data.last_page {
                break;
            }
            cursor = resp.data.next_query_cursor;
            if cursor.token.is_empty() {
                break;
            }
        }
        Ok(files)
    }

    /// 获取单文件下载直链。
    pub async fn get_download_url(
        &self,
        client: &ClientWithMiddleware,
        fid: &str,
    ) -> Result<String> {
        let body = json!({ "fid": fid });
        let resp: DownloadUrlResp = self
            .request(
                client,
                "/open/v1/file/get_download_url",
                "POST",
                Some(body),
                None,
            )
            .await?;
        if resp.data.download_url.is_empty() {
            bail!("Open API 未返回 download_url, fid={fid}");
        }
        Ok(resp.data.download_url)
    }

    pub async fn create_folder(
        &self,
        client: &ClientWithMiddleware,
        parent_fid: &str,
        name: &str,
    ) -> Result<String> {
        // 调用方（QuarkDrive）已 encode；此处再 encode 一次幂等。
        let name = crate::util::quark_encode_name(name);
        if parent_fid.trim().is_empty() {
            bail!("Open API create_folder: parent_fid 为空");
        }
        let body = json!({
            "dir_path": name,
            "pdir_fid": parent_fid,
        });
        let resp: serde_json::Value = self
            .request(client, "/open/v1/dir", "POST", Some(body), None)
            .await?;
        // 响应里带上新目录 fid，给 WebDAV 占位用，避免随后 PUT 时 pdir_fid 空白。
        let fid = resp
            .pointer("/data/fid")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                resp.get("fid")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();
        if fid.is_empty() {
            warn!(parent_fid = %parent_fid, name = %name, body = %resp, "create_folder response missing fid");
        }
        Ok(fid)
    }

    pub async fn rename_file(
        &self,
        client: &ClientWithMiddleware,
        fid: &str,
        name: &str,
    ) -> Result<()> {
        let name = crate::util::quark_encode_name(name);
        let body = json!({
            "fid": fid,
            "file_name": name,
            "conflict_mode": "REUSE",
        });
        let _: serde_json::Value = self
            .request(
                client,
                "/open/v1/file/rename",
                "POST",
                Some(body),
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn move_file(
        &self,
        client: &ClientWithMiddleware,
        fid: &str,
        to_parent_fid: &str,
    ) -> Result<()> {
        let body = json!({
            "action_type": 1,
            "fid_list": [fid],
            "to_pdir_fid": to_parent_fid,
        });
        let _: serde_json::Value = self
            .request(client, "/open/v1/file/move", "POST", Some(body), None)
            .await?;
        Ok(())
    }

    pub async fn delete_file(&self, client: &ClientWithMiddleware, fid: &str) -> Result<()> {
        self.fid_action(client, fid, "/open/v1/file/delete").await
    }

    /// 用户删除进回收站。半成品清理不要走这里，回收站会越积越多。
    pub async fn trash_file(&self, client: &ClientWithMiddleware, fid: &str) -> Result<()> {
        self.fid_action(client, fid, "/open/v1/file/trash").await
    }

    /// delete 与 trash 的 body 相同，只是路径不同。抓包没有解开 body，与 move/delete 的 fid_list 对齐。
    async fn fid_action(
        &self,
        client: &ClientWithMiddleware,
        fid: &str,
        pathname: &str,
    ) -> Result<()> {
        let body = json!({
            "action_type": 1,
            "fid_list": [fid],
        });
        let _: serde_json::Value = self
            .request(client, pathname, "POST", Some(body), None)
            .await?;
        Ok(())
    }

    /// Open 上传全流程：pre（含 proof）→ 分片 URL → PUT → finish。
    /// `source` 可为内存字节或临时文件，避免小文件强制落盘。
    /// `put_client` 须为无 retry 中间件的裸 Client（流式 body 不可被中间件 clone）。
    pub async fn upload_file(
        &self,
        client: &ClientWithMiddleware,
        put_client: &reqwest::Client,
        file_name: &str,
        size: u64,
        pdir_fid: &str,
        md5: &str,
        sha1: &str,
        source: &UploadSource,
        progress: Option<(&crate::upload_progress::UploadTracker, &str)>,
        smart: Option<&crate::upload_schedule::SmartUploadPermit>,
        cancel: Option<&AtomicBool>,
        local_mtime_ms: u64,
        old_fid: &str,
    ) -> Result<()> {
        // MIME 看客户端原名。落盘名里的扩展名点号已换成间隔号标记，不能再 rsplit('.')。
        let format_type = get_format_type(file_name).to_string();
        let file_name = crate::util::quark_encode_name(file_name);
        let file_name = file_name.as_str();
        let pathname = "/open/v1/file/upload_pre";
        let (tm, x_pan_token, req_id) =
            generate_req_sign("POST", pathname, &self.creds.sign_key_snapshot());
        let (proof_version, proof_seed1, proof_seed2, proof_code1, proof_code2) = self
            .generate_proof(source, size, &x_pan_token)
            .await?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        // 群晖 X-OC-Mtime 带来的是本地文件时间。写进夸克后，列表 mtime 才能对上，
        // 否则 CloudSync 每次扫描都觉得远端更旧，把同一个大文件再 PUT 一遍。
        let stamp_ms = if local_mtime_ms > 0 { local_mtime_ms } else { now_ms };
        let body = json!({
            "file_name": file_name,
            "size": size,
            "format_type": format_type,
            "md5": md5,
            "sha1": sha1,
            "l_created_at": stamp_ms,
            "l_updated_at": stamp_ms,
            "pdir_fid": pdir_fid,
            // 允许同名并存。VFS 等新文件传完再删旧的，分片失败时旧文件还在。
            "same_path_reuse": false,
            "proof_version": proof_version,
            "proof_seed1": proof_seed1,
            "proof_seed2": proof_seed2,
            "proof_code1": proof_code1,
            "proof_code2": proof_code2,
        });
        let manual = ManualSign {
            tm,
            token: x_pan_token,
            req_id,
        };
        let pre: UpPreResp = self
            .request(client, pathname, "POST", Some(body), Some(&manual))
            .await?;
        if pre.data.finish {
            debug!(file_name = %file_name, "open upload finished by rapid upload");
            if let Some((tracker, path)) = progress {
                // 秒传无实际上传耗时，记 1ms 避免除零，速度展示为瞬时完成。
                tracker.add_uploaded(path, size, 1);
            }
            return Ok(());
        }
        if pre.data.task_id.is_empty() {
            bail!("upload_pre 未返回 task_id");
        }
        // 失败时用来删本次预创建的文件；成功的 finish 不会走到清理。
        let new_fid = pre.data.fid.clone();
        let uploaded = async {
        // 夸克 get_upload_urls / finish 的 part list 上限约 256（超出会 part list exceed）。
        let part_size = effective_part_size(size, pre.data.part_size.max(0) as u64);
        let part_info = build_part_info(size, part_size);
        debug!(
            file_name = %file_name,
            size = size,
            part_size = part_size,
            parts = part_info.len(),
            "open upload part plan"
        );

        // 签名 URL / X-Oss-Date 有效期通常约 15 分钟（~900s）。
        // 旧逻辑一次申请最多 256 片再串行 PUT：家宽传 11G 约 15 分钟后剩余分片 403，
        // 进度会精确停在约 1/3（与「34% · 失败 · 均 4MB/s」吻合）。改为小批即时申请。
        let mut etags: Vec<Option<String>> = vec![None; part_info.len()];
        for chunk in part_info.chunks(UPLOAD_URL_BATCH) {
            let (mut common_headers, items) = self
                .fetch_upload_urls(client, &pre.data.task_id, chunk)
                .await?;
            if items.len() != chunk.len() {
                bail!(
                    "get_upload_urls 返回 {} 个 URL，期望 {}",
                    items.len(),
                    chunk.len()
                );
            }
            for item in items {
                if cancel.map(|c| c.load(Ordering::Acquire)).unwrap_or(false) {
                    bail!("上传已取消");
                }
                let idx = (item.part_number.max(1) as u64 - 1) as usize;
                if idx >= part_info.len() {
                    bail!("get_upload_urls 返回非法 part_number={}", item.part_number);
                }
                let this_size = part_info[idx]["part_size"].as_u64().unwrap_or(0);
                if this_size == 0 {
                    continue;
                }
                let offset = idx as u64 * part_size;
                // 路径源：整段流都带着 is_fast。队首被调度器标成全速后，当前分片立刻停限；
                // 被挤回慢传后，下一块就开始限。内存源无法边读边限，只在发送前 pace。
                // 切勿对同一段「先付满再流内再限」——会变成半速。
                let throttle = if let Some(smart) = smart {
                    if let Some((tracker, path)) = progress {
                        tracker.set_lane(path, smart.sync_lane().as_str());
                    }
                    if matches!(source, UploadSource::Memory(_)) {
                        if !smart.is_fast() {
                            smart.pace_before(this_size).await;
                            if let Some((tracker, path)) = progress {
                                tracker.set_lane(path, smart.lane().as_str());
                            }
                        }
                        None
                    } else {
                        smart.stream_throttle()
                    }
                } else {
                    None
                };
                // 流式 PUT：单片可到数百 MB，整段进内存会在低配机器 OOM。
                let put_started = std::time::Instant::now();
                let etag = self
                    .upload_part_with_retry(
                        client,
                        put_client,
                        &pre.data.task_id,
                        &part_info[idx],
                        item,
                        &mut common_headers,
                        source,
                        offset,
                        this_size,
                        throttle.as_ref(),
                    )
                    .await
                    .with_context(|| format!("上传分片 {} 失败", idx + 1))?;
                let put_ms = put_started.elapsed().as_millis() as u64;
                etags[idx] = Some(etag);
                if let Some((tracker, path)) = progress {
                    // 用实际上传耗时算上行速度；慢传 sleep 已在 PUT 前/流内完成。
                    tracker.add_uploaded(path, this_size, put_ms);
                }
                if let Some(smart) = smart {
                    if let Some((tracker, path)) = progress {
                        tracker.set_lane(path, smart.lane().as_str());
                    }
                }
            }
        }

        let mut finish_parts = Vec::with_capacity(part_info.len());
        for (i, part) in part_info.iter().enumerate() {
            let etag = etags[i]
                .clone()
                .ok_or_else(|| anyhow::anyhow!("缺少分片 {} 的 etag", i + 1))?;
            finish_parts.push(json!({
                "part_number": part["part_number"],
                "part_size": part["part_size"],
                "etag": etag,
            }));
        }
        let finish_body = json!({
            "task_id": pre.data.task_id,
            "part_info_list": finish_parts,
        });
        let finish: UpFinishResp = self
            .request(
                client,
                "/open/v1/file/upload_finish",
                "POST",
                Some(finish_body),
                None,
            )
            .await?;
        if !finish.data.finish {
            bail!(
                "upload_finish 未完成, task_id={}",
                finish.data.task_id
            );
        }
        Ok(())
        }
        .await;
        if let Err(err) = uploaded {
            // 分片失败不能把半成品留在网盘：下次 CloudSync 重试会把它当成已存在的同大小文件跳过。
            self.discard_incomplete_upload(client, &new_fid, old_fid).await;
            return Err(err);
        }
        Ok(())
    }

    /// 分片或 finish 失败后丢掉本次预创建的文件。和旧文件是同一个 fid 时不删。
    async fn discard_incomplete_upload(
        &self,
        client: &ClientWithMiddleware,
        fid: &str,
        old_fid: &str,
    ) {
        if fid.is_empty() || fid == old_fid {
            return;
        }
        match self.delete_file(client, fid).await {
            Ok(()) => info!(fid, "removed incomplete open upload after failure"),
            Err(err) => debug!(
                fid,
                error = %err,
                "cleanup incomplete open upload failed (task may not be listed yet)"
            ),
        }
    }

    /// 向 Open API 申请一批分片的签名上传 URL（须在过期前用完）。
    async fn fetch_upload_urls(
        &self,
        client: &ClientWithMiddleware,
        task_id: &str,
        part_info_list: &[serde_json::Value],
    ) -> Result<(CommonOssHeaders, Vec<UploadUrlItem>)> {
        let url_body = json!({
            "task_id": task_id,
            "part_info_list": part_info_list,
        });
        let up_urls: UpUrlResp = self
            .request(
                client,
                "/open/v1/file/get_upload_urls",
                "POST",
                Some(url_body),
                None,
            )
            .await?;
        Ok((up_urls.data.common_headers, up_urls.data.upload_urls))
    }

    /// 分片 PUT：网关错误退避重试；签名/403 过期则重新 get_upload_urls 再传，避免整文件失败。
    async fn upload_part_with_retry(
        &self,
        client: &ClientWithMiddleware,
        put_client: &reqwest::Client,
        task_id: &str,
        part_meta: &serde_json::Value,
        mut item: UploadUrlItem,
        headers: &mut CommonOssHeaders,
        source: &UploadSource,
        offset: u64,
        len: u64,
        throttle: Option<&crate::upload_schedule::ByteThrottle>,
    ) -> Result<String> {
        // OSS 偶发 504/502 很常见（网关超时），多试几次；每次重开文件流。
        const MAX_ATTEMPTS: u32 = 8;
        const MAX_URL_REFRESH: u32 = 3;
        let mut last_err = None;
        let mut url_refreshes = 0u32;
        for attempt in 0..MAX_ATTEMPTS {
            match self
                .upload_part(put_client, &item, headers, source, offset, len, throttle)
                .await
            {
                Ok(etag) => {
                    if attempt > 0 {
                        info!(
                            part = item.part_number,
                            attempt = attempt + 1,
                            "open upload part succeeded after retry"
                        );
                    }
                    return Ok(etag);
                }
                Err(err) => {
                    let expired = is_oss_url_expired_error(&err);
                    let retryable = expired || is_retryable_oss_put_error(&err);
                    warn!(
                        part = item.part_number,
                        attempt = attempt + 1,
                        expired = expired,
                        retryable = retryable,
                        error = %err,
                        "open upload part failed, retrying"
                    );
                    last_err = Some(err);
                    if !retryable || attempt + 1 >= MAX_ATTEMPTS {
                        break;
                    }
                    // 签名过期必须换新 URL；沿用旧 URL 重试只会连续 403。
                    if expired && url_refreshes < MAX_URL_REFRESH {
                        url_refreshes += 1;
                        match self
                            .fetch_upload_urls(client, task_id, std::slice::from_ref(part_meta))
                            .await
                        {
                            Ok((new_headers, items)) => {
                                if let Some(fresh) = items.into_iter().next() {
                                    *headers = new_headers;
                                    item = fresh;
                                    info!(
                                        part = item.part_number,
                                        refresh = url_refreshes,
                                        "refreshed expired OSS upload URL"
                                    );
                                    continue;
                                }
                            }
                            Err(refresh_err) => {
                                warn!(
                                    part = item.part_number,
                                    error = %refresh_err,
                                    "refresh upload URL failed"
                                );
                            }
                        }
                    }
                    // 504 等多等一会再传，退避上限 30s。
                    let delay = (1u64 << attempt).min(30);
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("open upload part failed")))
    }

    async fn upload_part(
        &self,
        put_client: &reqwest::Client,
        item: &UploadUrlItem,
        headers: &CommonOssHeaders,
        source: &UploadSource,
        offset: u64,
        len: u64,
        throttle: Option<&crate::upload_schedule::ByteThrottle>,
    ) -> Result<String> {
        let mut hdrs = HeaderMap::new();
        hdrs.insert(
            "Authorization",
            HeaderValue::from_str(&item.signature_info.signature)?,
        );
        if !headers.x_oss_date.is_empty() {
            hdrs.insert("X-Oss-Date", HeaderValue::from_str(&headers.x_oss_date)?);
        }
        if !headers.x_oss_content_sha256.is_empty() {
            hdrs.insert(
                "X-Oss-Content-Sha256",
                HeaderValue::from_str(&headers.x_oss_content_sha256)?,
            );
        }
        hdrs.insert(CONTENT_LENGTH, HeaderValue::from(len));
        let body = source
            .upload_body(offset, len, throttle)
            .await
            .with_context(|| format!("打开分片 {} 数据流失败", item.part_number))?;
        let res = put_client
            .put(&item.upload_url)
            .headers(hdrs)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        let etag = res
            .headers()
            .get("Etag")
            .or_else(|| res.headers().get("ETag"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        if etag.is_empty() {
            bail!("分片上传未返回 ETag, part={}", item.part_number);
        }
        Ok(etag)
    }

    /// 带签名的 Open 请求；429 退避重试；access_token 过期时刷新并重试一次。
    async fn request<T>(
        &self,
        client: &ClientWithMiddleware,
        pathname: &str,
        method: &str,
        body: Option<serde_json::Value>,
        manual: Option<&ManualSign>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        // 未扫码时 access/refresh 皆空：直接失败，禁止空 token 打 API / 刷刷新日志。
        if let Err(err) = self.ensure_access_token(client).await {
            return Err(err);
        }

        const MAX_RATE_RETRIES: u32 = 5;
        let mut rate_attempt = 0u32;
        loop {
            let result = {
                let _permit = self.api_gate.acquire().await?;
                self.request_once::<T>(client, pathname, method, body.as_ref(), manual, false)
                    .await
            };

            match result {
                Ok(v) => return Ok(v),
                Err(err) if is_rate_limited_error(&err) && rate_attempt < MAX_RATE_RETRIES => {
                    // 2s、4s、8s、16s、32s；限流时勿刷企微，退避后重试。
                    let delay_secs = 2u64 << rate_attempt.min(4);
                    warn!(
                        pathname = %pathname,
                        attempt = rate_attempt + 1,
                        delay_secs,
                        error = %err,
                        "Open API 限流，退避后重试"
                    );
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    rate_attempt += 1;
                    continue;
                }
                Err(err) if is_token_expired_error(&err) => {
                    let _guard = self.refresh_lock.lock().await;
                    {
                        let _permit = self.api_gate.acquire().await?;
                        if let Ok(v) = self
                            .request_once::<T>(
                                client,
                                pathname,
                                method,
                                body.as_ref(),
                                manual,
                                true,
                            )
                            .await
                        {
                            return Ok(v);
                        }
                    }
                    warn!(error = %err, "access_token 失效，尝试刷新后重试");
                    if let Err(refresh_err) = self.refresh_token(client).await {
                        let msg = format!("{refresh_err:#}");
                        if !msg.contains("尚未授权") && !msg.contains("缺少 refresh_token") {
                            self.alert.notify("Open Token 刷新失败", &msg).await;
                        }
                        return Err(refresh_err);
                    }
                    let _permit = self.api_gate.acquire().await?;
                    return self
                        .request_once(client, pathname, method, body.as_ref(), manual, true)
                        .await;
                }
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("尚未授权") || msg.contains("缺少 refresh_token") {
                        return Err(err);
                    }
                    // 限流告警单独归类，且走 alert 去重，避免刷屏。
                    if is_rate_limited_error(&err) {
                        self.alert.notify("Open API 限流", &msg).await;
                    } else if msg.contains("errno")
                        || msg.contains("Open API")
                        || msg.contains("令牌")
                        || msg.contains("token")
                    {
                        self.alert.notify("Open API 请求失败", &msg).await;
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn request_once<T>(
        &self,
        client: &ClientWithMiddleware,
        pathname: &str,
        method: &str,
        body: Option<&serde_json::Value>,
        manual: Option<&ManualSign>,
        _retried: bool,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let sign_key = self.creds.sign_key_snapshot();
        let app_id = self.creds.app_id_snapshot();
        let (tm, token, req_id) = if let Some(m) = manual {
            (m.tm.clone(), m.token.clone(), m.req_id.clone())
        } else {
            generate_req_sign(method, pathname, &sign_key)
        };
        let access = self.creds.access_token.read().await.clone();
        let url = format!(
            "{}{}?req_id={}&access_token={}",
            self.api_base,
            pathname,
            urlencoding_encode(&req_id),
            urlencoding_encode(&access)
        );

        let mut req = match method {
            "GET" => client.get(&url),
            "POST" => client.post(&url),
            "PUT" => client.put(&url),
            "DELETE" => client.delete(&url),
            other => bail!("不支持的 HTTP 方法: {other}"),
        };
        req = req
            .header("Accept", "application/json, text/plain, */*")
            .header("User-Agent", OPEN_UA)
            .header("x-pan-tm", &tm)
            .header("x-pan-token", &token)
            .header("x-pan-client-id", &app_id);
        if let Some(b) = body {
            req = req.json(b);
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await?;
        debug!(
            pathname = %pathname,
            http_status = %status,
            body = %crate::alert::redact_secrets(&text),
            "open api response"
        );

        if let Ok(err_body) = serde_json::from_str::<OpenErrorBody>(&text) {
            // 「不能为空」是未登录，不是过期；避免误走 refresh 风暴。
            if err_body.error_info.contains("access_token不能为空")
                || err_body.error_info.contains("access_token 不能为空")
            {
                bail!("尚未授权，请打开 Web 页扫码登录");
            }
            // 10001 签名失败：飞牛刷新换过的 access_token 和内置签名对不上。
            // 当成过期，改用 OpenList 换票后再试一次。
            if err_body.status == -1
                && (err_body.errno == 11001
                    || err_body.errno == 10001
                    || (err_body.errno == 14001
                        && err_body.error_info.contains("access_token")))
            {
                return Err(TokenExpiredError {
                    info: err_body.error_info,
                }
                .into());
            }
            if err_body.status >= 400 || err_body.errno != 0 {
                if !err_body.error_info.is_empty() {
                    bail!(
                        "Open API 错误: {} (status={}, errno={})",
                        err_body.error_info,
                        err_body.status,
                        err_body.errno
                    );
                }
                if err_body.errno == 429 {
                    bail!("Open API 错误: 限流 (status={}, errno=429)", err_body.status);
                }
            }
        }

        if status.as_u16() == 429 {
            bail!("Open API 错误: 限流 (status=-1, errno=429)");
        }
        if !status.is_success() {
            bail!("Open API HTTP {status}: {text}");
        }

        serde_json::from_str(&text)
            .with_context(|| format!("解析 Open API 响应失败 ({pathname}): {text}"))
    }

    /// 只走 OpenList 在线刷新。飞牛刷新换回的票和内置签名对不上，会一直 10001。
    pub async fn refresh_token(&self, client: &ClientWithMiddleware) -> Result<()> {
        let refresh = self.creds.refresh_token.read().await.clone();
        if refresh.is_empty() {
            bail!("缺少 refresh_token，请打开 Web 页扫码授权");
        }
        let mut last_err = None;
        for attempt in 1..=3 {
            match self.refresh_token_once(client, &refresh).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(attempt, error = %err, "刷新 token 失败");
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_secs(attempt)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("刷新 token 失败")))
    }

    async fn refresh_token_once(
        &self,
        client: &ClientWithMiddleware,
        refresh: &str,
    ) -> Result<()> {
        let (new_refresh, new_access) = self.refresh_via_online(client, refresh).await?;
        self.apply_tokens(new_access, new_refresh).await?;
        info!("Open token 已通过 OpenList 刷新");
        Ok(())
    }

    async fn refresh_via_online(
        &self,
        client: &ClientWithMiddleware,
        refresh: &str,
    ) -> Result<(String, String)> {
        let res = client
            .get(DEFAULT_RENEW_API)
            .query(&[
                ("refresh_ui", refresh),
                ("server_use", "true"),
                ("driver_txt", "quarkyun_oa"),
            ])
            .timeout(Duration::from_secs(20))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    anyhow!("无法访问 renew API（网络超时/连接失败）: {e}")
                } else {
                    anyhow!("请求 renew API 失败: {e}")
                }
            })?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "renew API 返回 HTTP {status}: {}",
                crate::alert::redact_secrets(&text)
            );
        }
        let body: OnlineRenewResp = serde_json::from_str(&text).map_err(|e| {
            anyhow!(
                "renew API 响应无法解析: {e}; body={}",
                crate::alert::redact_secrets(&text)
            )
        })?;
        if body.refresh_token.is_empty() || body.access_token.is_empty() {
            if !body.text.is_empty() {
                bail!("renew API 拒绝刷新（令牌可能已失效）: {}", body.text);
            }
            bail!("renew API 返回空 token，请重新扫码授权");
        }
        Ok((body.refresh_token, body.access_token))
    }

    /// 用扫码得到的 nonce 换 token 并落盘。
    pub async fn complete_login_with_nonce(
        &self,
        client: &ClientWithMiddleware,
        nonce: &str,
    ) -> Result<()> {
        let (access, refresh, _app_id, _sign_key) =
            exchange_token_by_nonce(client, nonce).await?;
        // 授权 client_id 是内置 AppId，签名继续用配套的内置 SignKey。
        self.apply_tokens(access, refresh).await?;
        // 登录后立刻拉 user_id，供后续上传 proof。
        let mut resp: UserInfoResp = self
            .request(client, "/open/v1/user/info", "GET", None, None)
            .await?;
        if !resp.data.user_id.is_empty() {
            *self.creds.user_id.write().await = std::mem::take(&mut resp.data.user_id);
        }
        Ok(())
    }

    /// 探测当前 access/refresh 是否可用。
    pub async fn check_auth(&self, client: &ClientWithMiddleware) -> Result<bool> {
        if !self.creds.has_refresh_token().await
            && self.creds.access_token.read().await.is_empty()
        {
            *self.auth_ok_until.lock().await = None;
            return Ok(false);
        }
        {
            let guard = self.auth_ok_until.lock().await;
            if let Some(until) = *guard {
                if std::time::Instant::now() < until {
                    return Ok(true);
                }
            }
        }
        match self
            .request::<UserInfoResp>(client, "/open/v1/user/info", "GET", None, None)
            .await
        {
            Ok(resp) => {
                let ok = !resp.data.user_id.is_empty();
                if ok {
                    // 上传 proof_seed1 依赖 user_id；探测成功时一并缓存。
                    *self.creds.user_id.write().await = resp.data.user_id.clone();
                }
                *self.auth_ok_until.lock().await = if ok {
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(60))
                } else {
                    None
                };
                Ok(ok)
            }
            Err(err) => {
                *self.auth_ok_until.lock().await = None;
                let msg = err.to_string();
                if msg.contains("尚未授权")
                    || msg.contains("缺少 refresh_token")
                    || msg.contains("令牌")
                    || msg.contains("token")
                    || msg.contains("access_token")
                {
                    Ok(false)
                } else {
                    // 瞬时网络错误不当成未授权，避免误跳扫码页。
                    warn!(error = %err, "Open 授权探测失败（非 token），暂视为仍有效");
                    Ok(true)
                }
            }
        }
    }

    async fn apply_tokens(&self, access: String, refresh: String) -> Result<()> {
        info!("Open API token 已更新");
        *self.creds.access_token.write().await = access;
        *self.creds.refresh_token.write().await = refresh;
        *self.auth_ok_until.lock().await = None;
        if let Err(err) = self.creds.persist_tokens().await {
            // 落盘失败则重启会丢登录；必须醒目提示，不要只 warn 一句。
            let msg = format!("{err:#}");
            error!(error = %msg, "token 落盘失败，重启后需重新扫码");
            self.alert
                .notify("Token 落盘失败", &msg)
                .await;
        } else if let Some(ref path) = self.creds.token_file {
            info!(path = %path, "token 已写入文件");
        }
        Ok(())
    }

    async fn generate_proof(
        &self,
        source: &UploadSource,
        file_size: u64,
        x_pan_token: &str,
    ) -> Result<(String, String, String, String, String)> {
        let user_id = self.creds.user_id.read().await.clone();
        let seed1 = md5_hex(&(user_id + x_pan_token));
        let seed2 = md5_hex(&file_size.to_string());
        let code1 = read_proof_code(source, &seed1, file_size).await?;
        let code2 = read_proof_code(source, &seed2, file_size).await?;
        Ok(("v1".to_string(), seed1, seed2, code1, code2))
    }
}

/// 上传内容来源：内存或临时文件（由 VFS 按内存池决定）。
#[derive(Debug, Clone)]
pub enum UploadSource {
    Memory(bytes::Bytes),
    Path(std::path::PathBuf),
}

impl UploadSource {
    /// 按偏移读取一段，供 proof 校验（通常只有几字节）。
    async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        match self {
            UploadSource::Memory(data) => {
                let start = offset as usize;
                let end = start.saturating_add(len);
                if end > data.len() {
                    bail!(
                        "内存上传越界: offset={offset}, len={len}, size={}",
                        data.len()
                    );
                }
                Ok(data[start..end].to_vec())
            }
            UploadSource::Path(path) => {
                let mut file = tokio::fs::File::open(path)
                    .await
                    .with_context(|| format!("打开临时文件失败: {}", path.display()))?;
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                let mut buf = vec![0u8; len];
                file.read_exact(&mut buf).await?;
                Ok(buf)
            }
        }
    }

    /// 构造分片 PUT 请求体：路径源按块流式读，避免大单片整段进内存。
    /// `throttle`：慢传时按目标速率读出，避免单片 PUT 期间全速抢带宽。
    async fn upload_body(
        &self,
        offset: u64,
        len: u64,
        throttle: Option<&crate::upload_schedule::ByteThrottle>,
    ) -> Result<reqwest::Body> {
        if len == 0 {
            return Ok(reqwest::Body::from(Bytes::new()));
        }
        match self {
            UploadSource::Memory(data) => {
                let start = offset as usize;
                let end = start
                    .checked_add(len as usize)
                    .filter(|e| *e <= data.len())
                    .ok_or_else(|| {
                        anyhow!(
                            "内存上传越界: offset={offset}, len={len}, size={}",
                            data.len()
                        )
                    })?;
                // 内存体无法边读边限速；调用方应已 pace_before。
                Ok(reqwest::Body::from(data.slice(start..end)))
            }
            UploadSource::Path(path) => {
                // 64KB 读缓冲：单片再大也只占固定小内存，低配可传数 GB/数十 GB。
                const STREAM_BUF: usize = 64 * 1024;
                let mut file = tokio::fs::File::open(path)
                    .await
                    .with_context(|| format!("打开临时文件失败: {}", path.display()))?;
                file.seek(std::io::SeekFrom::Start(offset))
                    .await
                    .with_context(|| format!("定位分片 offset={offset} 失败"))?;
                let reader = file.take(len);
                let stream = ReaderStream::with_capacity(reader, STREAM_BUF);
                if let Some(throttle) = throttle {
                    let limited = throttle_byte_stream(stream, throttle.clone());
                    Ok(reqwest::Body::wrap_stream(limited))
                } else {
                    Ok(reqwest::Body::wrap_stream(stream))
                }
            }
        }
    }
}

/// 按目标速率限制字节流。`is_fast` 由调度器写入：升全速立即停限，被挤回慢传后继续限。
fn throttle_byte_stream<S, E>(
    inner: S,
    throttle: crate::upload_schedule::ByteThrottle,
) -> impl futures_util::Stream<Item = Result<Bytes, E>>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    use futures_util::StreamExt;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, E>>(2);
    tokio::spawn(async move {
        let bps = throttle.bps.max(1);
        let is_fast = throttle.is_fast;
        let notify = throttle.notify;
        let mut inner = std::pin::pin!(inner);
        let mut last = Instant::now();
        let mut debt_ns = 0u64;
        while let Some(item) = inner.next().await {
            match item {
                Ok(bytes) => {
                    if !is_fast.load(Ordering::Acquire) {
                        let n = bytes.len() as u64;
                        let now = Instant::now();
                        let elapsed_ns = now.duration_since(last).as_nanos() as u64;
                        last = now;
                        debt_ns = debt_ns.saturating_sub(elapsed_ns);
                        let need_ns = n
                            .saturating_mul(1_000_000_000)
                            .checked_div(bps)
                            .unwrap_or(0);
                        debt_ns = debt_ns.saturating_add(need_ns);
                        if debt_ns > 0 {
                            let sleep_for = Duration::from_nanos(debt_ns);
                            debt_ns = 0;
                            let deadline = Instant::now() + sleep_for;
                            while Instant::now() < deadline {
                                if is_fast.load(Ordering::Acquire) {
                                    break;
                                }
                                let left = deadline.saturating_duration_since(Instant::now());
                                if left.is_zero() {
                                    break;
                                }
                                let slice = left.min(Duration::from_millis(50));
                                let notified = notify.notified();
                                tokio::pin!(notified);
                                if is_fast.load(Ordering::Acquire) {
                                    break;
                                }
                                tokio::select! {
                                    _ = tokio::time::sleep(slice) => {}
                                    _ = &mut notified => {}
                                }
                            }
                            last = Instant::now();
                        }
                    }
                    if tx.send(Ok(bytes)).await.is_err() {
                        return;
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            }
        }
    });
    futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

/// 获取飞牛授权 URL，供 `open-login` 子命令使用。
pub async fn fetch_auth_url(client: &ClientWithMiddleware, redirect_frontend: &str) -> Result<String> {
    let body = json!({
        "authType": 4,
        "grantType": "authorization_code",
        "redirectUrlToFrontend": redirect_frontend,
        "trimAppId": FNNAS_TRIM_APP_ID,
    });
    let res = client
        .post(FNNAS_AUTH_URL)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: FnnasResp<FnnasAuthData> = res.json().await?;
    if parsed.code != 0 {
        bail!("获取授权 URL 失败: {}", parsed.msg);
    }
    let url = parsed
        .data
        .ok_or_else(|| anyhow!("授权响应缺少 data"))?
        .auth_url_with_nonce;
    if url.is_empty() {
        bail!("授权 URL 为空");
    }
    Ok(url)
}

/// 用授权回调里的 nonce 换取 access/refresh token。
pub async fn exchange_token_by_nonce(
    client: &ClientWithMiddleware,
    nonce: &str,
) -> Result<(String, String, Option<String>, Option<String>)> {
    let body = json!({
        "authType": 4,
        "nonce": nonce,
        "trimAppId": FNNAS_TRIM_APP_ID,
    });
    let res = client
        .post(FNNAS_EXCHANGE_URL)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: FnnasResp<FnnasTokenData> = res.json().await?;
    if parsed.code != 0 {
        bail!("交换 token 失败: {}", parsed.msg);
    }
    let data = parsed.data.ok_or_else(|| anyhow!("交换 token 无 data"))?;
    if data.access_token.is_empty() || data.refresh_token.is_empty() {
        bail!("交换 token 返回空值");
    }
    Ok((
        data.access_token,
        data.refresh_token,
        data.app_id,
        data.sign_key,
    ))
}

/// OSS 分片 PUT 是否值得重试（网关超时/限流/瞬时网络错误）。
fn is_retryable_oss_put_error(err: &anyhow::Error) -> bool {
    let s = format!("{err:#}").to_ascii_lowercase();
    s.contains("504")
        || s.contains("502")
        || s.contains("503")
        || s.contains("429")
        || s.contains("408")
        || s.contains("gateway time-out")
        || s.contains("gateway timeout")
        || s.contains("timed out")
        || s.contains("timeout")
        || s.contains("connection reset")
        || s.contains("connection refused")
        || s.contains("error sending request")
        || s.contains("broken pipe")
}

/// 签名 URL / X-Oss-Date 过期类错误：必须重新 get_upload_urls，不能原地重试。
fn is_oss_url_expired_error(err: &anyhow::Error) -> bool {
    let s = format!("{err:#}").to_ascii_lowercase();
    s.contains("403")
        || s.contains("401")
        || s.contains("request has expired")
        || s.contains("security token expired")
        || s.contains("signaturedoesnotmatch")
        || s.contains("signature does not match")
        || s.contains("accessdenied")
        || s.contains("access denied")
        || (s.contains("expired") && (s.contains("oss") || s.contains("url") || s.contains("token")))
}

/// 夸克 Open `get_upload_urls` / `upload_finish` 单次 part list 上限约 256。
const MAX_UPLOAD_PARTS: u64 = 256;
/// 每次申请的上传 URL 数。签名约 15 分钟失效，一次申太多会导致大文件中途 403。
const UPLOAD_URL_BATCH: usize = 2;

/// 在 API 建议的 part_size 基础上，保证总分片数不超过 MAX_UPLOAD_PARTS。
fn effective_part_size(file_size: u64, api_part_size: u64) -> u64 {
    const DEFAULT: u64 = 4 * 1024 * 1024;
    const ALIGN: u64 = 1024 * 1024; // 按 1MB 对齐，便于 OSS
    let base = if api_part_size > 0 { api_part_size } else { DEFAULT };
    if file_size == 0 {
        return base.max(ALIGN);
    }
    let min_needed = (file_size + MAX_UPLOAD_PARTS - 1) / MAX_UPLOAD_PARTS;
    let aligned = ((min_needed + ALIGN - 1) / ALIGN) * ALIGN;
    base.max(aligned).max(ALIGN)
}

fn build_part_info(total: u64, part_size: u64) -> Vec<serde_json::Value> {
    // 对齐 OpenList `_getPartInfo`：0 字节不生成分片，避免 OSS EntityTooSmall。
    let mut parts = Vec::new();
    if total == 0 || part_size == 0 {
        return parts;
    }
    let mut left = total;
    let mut part_number = 1i64;
    while left > 0 {
        let size = std::cmp::min(part_size, left);
        parts.push(json!({
            "part_number": part_number,
            "part_size": size,
        }));
        left -= size;
        part_number += 1;
    }
    parts
}

async fn read_proof_code(
    source: &UploadSource,
    proof_seed: &str,
    file_size: u64,
) -> Result<String> {
    let (start, end) = proof_range(proof_seed, file_size)?;
    let length = end.saturating_sub(start);
    if length == 0 || file_size == 0 {
        return Ok(String::new());
    }
    let buf = source.read_at(start, length as usize).await?;
    Ok(general_purpose::STANDARD.encode(buf))
}

fn proof_range(proof_seed: &str, file_size: u64) -> Result<(u64, u64)> {
    if file_size == 0 {
        return Ok((0, 0));
    }
    let md5 = md5_hex(proof_seed);
    let tmp_str = &md5[..16];
    let tmp_int = u64::from_str_radix(tmp_str, 16)
        .with_context(|| format!("解析 proof hex 失败: {tmp_str}"))?;
    let index = tmp_int % file_size;
    let end = std::cmp::min(index + 8, file_size);
    Ok((index, end))
}

fn md5_hex(s: &str) -> String {
    format!("{:x}", md5::compute(s.as_bytes()))
}

fn urlencoding_encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[derive(Debug)]
struct TokenExpiredError {
    info: String,
}

impl std::fmt::Display for TokenExpiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "token expired: {}", self.info)
    }
}

impl std::error::Error for TokenExpiredError {}

fn is_rate_limited_error(err: &anyhow::Error) -> bool {
    let s = err.to_string();
    s.contains("errno=429")
        || s.contains("errno: 429")
        || s.contains("限流")
        || s.contains("HTTP 429")
        || s.contains("Too Many Requests")
}

fn is_token_expired_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TokenExpiredError>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_req_sign_stable_shape() {
        let (tm, token, req_id) = generate_req_sign("GET", "/open/v1/user/info", "key");
        assert_eq!(tm.len(), 13);
        assert_eq!(token.len(), 64);
        assert!(!req_id.is_empty());
        let expected = {
            let data = format!("GET&/open/v1/user/info&{tm}&key");
            hex::encode(Sha256::digest(data.as_bytes()))
        };
        assert_eq!(token, expected);
    }

    #[test]
    fn test_proof_range_empty() {
        assert_eq!(proof_range("abc", 0).unwrap(), (0, 0));
    }

    #[test]
    fn test_proof_range_within_file() {
        let (start, end) = proof_range("seed", 100).unwrap();
        assert!(start < 100);
        assert!(end <= 100);
        assert!(end >= start);
        assert!(end - start <= 8);
    }

    #[test]
    fn test_build_part_info() {
        let parts = build_part_info(10, 4);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0]["part_size"], 4);
        assert_eq!(parts[2]["part_size"], 2);
    }

    #[test]
    fn test_oss_url_expired_detection() {
        assert!(is_oss_url_expired_error(&anyhow!("HTTP 403 Forbidden")));
        assert!(is_oss_url_expired_error(&anyhow!("Request has expired")));
        assert!(is_oss_url_expired_error(&anyhow!("SignatureDoesNotMatch")));
        assert!(!is_oss_url_expired_error(&anyhow!("HTTP 504 Gateway Timeout")));
        assert!(is_retryable_oss_put_error(&anyhow!("HTTP 504 Gateway Timeout")));
    }

    #[test]
    fn token_file_keeps_builtin_sign_keys() {
        let path = std::env::temp_dir().join(format!(
            "quark-token-keys-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            r#"{"access_token":"acc","refresh_token":"ref","app_id":"app-from-file","sign_key":"key-from-file"}"#,
        )
        .unwrap();
        let creds = OpenCredentials::new(String::new(), Some(path.display().to_string())).unwrap();
        // 文件里的另一套密钥不能覆盖内置签名，否则请求 errno=10001。
        assert_eq!(creds.app_id_snapshot(), BUILTIN_APP_ID);
        assert_eq!(creds.sign_key_snapshot(), BUILTIN_SIGN_KEY);
        let access = creds.access_token.try_read().unwrap().clone();
        assert_eq!(access, "acc");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rate_limited_detection() {
        assert!(is_rate_limited_error(&anyhow!(
            "Open API 错误: 限流 (status=-1, errno=429)"
        )));
        assert!(is_rate_limited_error(&anyhow!("HTTP 429 Too Many Requests")));
        assert!(!is_rate_limited_error(&anyhow!("Open API 错误: bad request")));
    }

    #[test]
    fn upload_url_batch_keeps_urls_fresh() {
        // 批太大时家宽单批耗时会逼近签名 15 分钟有效期。
        assert!(UPLOAD_URL_BATCH >= 1);
        assert!(UPLOAD_URL_BATCH <= 8);
        assert!(UPLOAD_URL_BATCH < MAX_UPLOAD_PARTS as usize);
    }

    #[test]
    fn test_open_file_to_quark_dir() {
        let f = OpenFile {
            fid: "1".into(),
            parent_fid: "0".into(),
            filename: "dir".into(),
            size: 0,
            file_type: "0".into(),
            content_hash: None,
            thumbnail_url: None,
            created_at: 1,
            updated_at: 2,
        };
        let q: QuarkFile = f.into();
        assert!(q.dir);
        assert!(!q.file);
    }

    #[test]
    fn test_effective_part_size_caps_at_256_parts() {
        // 用户日志里的 ~2.1GB MOV：4MB 分片会 ~537 片 → part list exceed。
        let size = 2u64 * 1024 * 1024 * 1024 + 100 * 1024 * 1024;
        let ps = effective_part_size(size, 4 * 1024 * 1024);
        let parts = build_part_info(size, ps);
        assert!(parts.len() as u64 <= MAX_UPLOAD_PARTS);
        assert!(ps >= 9 * 1024 * 1024); // ceil(2.1G/256) 对齐到 1MB ≈ 9MB
        // 小文件保持 API 建议值，不强行放大。
        assert_eq!(effective_part_size(8 * 1024 * 1024, 4 * 1024 * 1024), 4 * 1024 * 1024);
        // 超大文件也不得超过 256 片。
        let huge = 40u64 * 1024 * 1024 * 1024;
        let huge_ps = effective_part_size(huge, 4 * 1024 * 1024);
        assert!(build_part_info(huge, huge_ps).len() as u64 <= MAX_UPLOAD_PARTS);
    }
}
