use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use anyhow::bail;
use clap::{Parser, Subcommand};
use dashmap::DashMap;
use dav_server::{memls::MemLs, DavHandler};
#[cfg(unix)]
use futures_util::stream::StreamExt;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
#[cfg(unix)]
use {signal_hook::consts::signal::*, signal_hook_tokio::Signals};

use cache::Cache;
use drive::open::{
    exchange_token_by_nonce, fetch_auth_url, OpenCredentials, DEFAULT_RENEW_API, OPEN_API_BASE,
};
use drive::*;
use vfs::QuarkDriveFileSystem;
use webdav::WebDavServer;

mod cache;
mod drive;
mod prefetch;
mod vfs;
mod webdav;
use tokio::time::interval;

#[derive(Parser, Debug)]
#[command(name = "quarkdrive-webdav", about, version, author)]
#[command(args_conflicts_with_subcommands = true)]
struct Opt {
    /// Listen host
    #[arg(long, env = "HOST", default_value = "0.0.0.0")]
    host: String,
    /// Listen port
    #[arg(short, env = "PORT", default_value = "8080")]
    port: u16,

    /// Cookie 模式凭证（与 Open 模式二选一）
    #[arg(long, env = "QUARK_COOKIE")]
    quark_cookie: Option<String>,

    /// 启用夸克 Open API（对齐 OpenList quark_open）
    #[arg(long, env = "QUARK_OPEN", default_value_t = false)]
    quark_open: bool,

    /// Open AppId（x-pan-client-id）
    #[arg(long, env = "QUARK_APP_ID")]
    quark_app_id: Option<String>,

    /// Open SignKey（签名私钥）
    #[arg(long, env = "QUARK_SIGN_KEY")]
    quark_sign_key: Option<String>,

    /// Open access_token（可空，启动后会用 refresh_token 刷新）
    #[arg(long, env = "QUARK_ACCESS_TOKEN", default_value = "")]
    quark_access_token: String,

    /// Open refresh_token（Open 模式必填，除非 token 文件已有）
    #[arg(long, env = "QUARK_REFRESH_TOKEN", default_value = "")]
    quark_refresh_token: String,

    /// 是否用在线 API 刷新 token（缺文档时本地刷新可能不可用）
    #[arg(long, env = "QUARK_USE_ONLINE_API", default_value_t = true)]
    quark_use_online_api: bool,

    /// 在线刷新 API 地址
    #[arg(long, env = "QUARK_RENEW_API", default_value = DEFAULT_RENEW_API)]
    quark_renew_api: String,

    /// token 落盘路径；刷新后写回，重启可复用
    #[arg(long, env = "QUARK_TOKEN_FILE")]
    quark_token_file: Option<String>,

    /// WebDAV authentication username
    #[arg(short = 'U', long, env = "WEBDAV_AUTH_USER")]
    auth_user: Option<String>,
    /// WebDAV authentication password
    #[arg(short = 'W', long, env = "WEBDAV_AUTH_PASSWORD")]
    auth_password: Option<String>,
    /// Automatically generate index.html
    #[arg(short = 'I', long)]
    auto_index: bool,
    /// Read/download buffer size in bytes, defaults to 10MB
    #[arg(short = 'S', long, default_value = "10485760")]
    read_buffer_size: usize,
    /// Upload buffer size in bytes, defaults to 16MB
    #[arg(long, default_value = "16777216")]
    upload_buffer_size: usize,
    /// Directory entries cache size
    #[arg(long, default_value = "1000")]
    cache_size: u64,
    /// Directory entries cache expiration time in seconds
    #[arg(long, default_value = "600")]
    cache_ttl: u64,
    /// 分块预取缓存上限（MB）。播放器换连接时可命中已拉回的块；0 关闭。
    #[arg(long, env = "CHUNK_CACHE_MB", default_value = "128")]
    chunk_cache_mb: u64,
    /// Root directory path
    #[arg(long, env = "WEBDAV_ROOT", default_value = "/")]
    root: String,
    /// Delete file permanently instead of trashing it
    #[arg(long)]
    no_trash: bool,
    /// Enable read only mode
    #[arg(long)]
    read_only: bool,
    /// TLS certificate file path
    #[arg(long, env = "TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// TLS private key file path
    #[arg(long, env = "TLS_KEY")]
    tls_key: Option<PathBuf>,
    /// Prefix to be stripped off when handling request.
    #[arg(long, env = "WEBDAV_STRIP_PREFIX")]
    strip_prefix: Option<String>,
    /// Enable debug log
    #[arg(long)]
    debug: bool,
    /// Disable self auto upgrade
    #[arg(long)]
    no_self_upgrade: bool,
    /// Skip uploading same size file
    #[arg(long)]
    skip_upload_same_size: bool,
    /// Prefer downloading using HTTP protocol
    #[arg(long)]
    prefer_http_download: bool,
    /// Enable 302 redirect when possible
    #[arg(long)]
    redirect: bool,

    #[command(subcommand)]
    subcommands: Option<Commands>,

    #[arg(long, env = "REFRESH_CACHE_SECS_INTERVAL", default_value = "300")]
    refresh_cache_secs_interval: u64,

    /// Max seconds to wait for upload completion before returning early to client.
    /// If upload is still in progress after this timeout, return success to avoid
    /// client timeout while upload continues in the background.
    /// Set to 0 to wait indefinitely. Defaults to 280 seconds.
    #[arg(long, env = "UPLOAD_WAIT_TIMEOUT", default_value = "280")]
    upload_wait_timeout: u64,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scan QRCode
    #[command(subcommand)]
    Qr(QrCommand),
    /// 夸克 Open OAuth 登录（飞牛授权流，对齐 OpenList）
    #[command(subcommand)]
    Open(OpenCommand),
}

#[derive(Subcommand, Debug)]
enum QrCommand {
    /// Scan QRCode login to get a token
    Login,
    /// Generate a QRCode
    Generate,
    /// Query the QRCode login result
    #[command(arg_required_else_help = true)]
    Query {
        /// Query parameter sid
        #[arg(long)]
        sid: String,
    },
}

#[derive(Subcommand, Debug)]
enum OpenCommand {
    /// 打印授权 URL，浏览器打开后完成登录
    Login {
        /// 授权完成后前端回调基址（飞牛要求）
        #[arg(long, default_value = "https://api.oplist.org/quarkyun/callback")]
        redirect: String,
    },
    /// 用回调 URL 中的 nonce 换取 token，并可选写入 token 文件
    Exchange {
        /// 授权回调里的 nonce
        #[arg(long)]
        nonce: String,
        /// 写入路径；不指定则只打印到 stdout
        #[arg(long)]
        token_file: Option<String>,
    },
}

pub fn start_periodic_invalidate(cache: Arc<Cache>, secs: u64) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(secs));
        loop {
            ticker.tick().await;
            cache.invalidate_all();
        }
    });
}

/// 根据 Cookie / Open 参数组装 DriveConfig，避免两套凭证混用。
fn build_drive_config(opt: &Opt) -> anyhow::Result<DriveConfig> {
    let use_open = opt.quark_open
        || opt.quark_app_id.is_some()
        || opt.quark_sign_key.is_some()
        || !opt.quark_refresh_token.is_empty()
        || opt.quark_token_file.is_some();

    if use_open {
        let app_id = opt
            .quark_app_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Open 模式需要 --quark-app-id / QUARK_APP_ID"))?;
        let sign_key = opt
            .quark_sign_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Open 模式需要 --quark-sign-key / QUARK_SIGN_KEY"))?;
        let open = OpenCredentials::new(
            app_id,
            sign_key,
            opt.quark_access_token.clone(),
            opt.quark_refresh_token.clone(),
            opt.quark_use_online_api,
            opt.quark_renew_api.clone(),
            opt.quark_token_file.clone(),
        )?;
        return Ok(DriveConfig {
            api_base_url: OPEN_API_BASE.to_string(),
            cookie: Arc::new(DashMap::new()),
            open: Some(open),
        });
    }

    let cookie_str = opt.quark_cookie.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "请指定 Cookie 模式凭证 QUARK_COOKIE，或启用 Open 模式（QUARK_APP_ID / QUARK_SIGN_KEY / QUARK_REFRESH_TOKEN）"
        )
    })?;
    let init_cookie = Arc::new(DashMap::new());
    for pair in cookie_str.split(';') {
        if let Some((k, v)) = pair.trim().split_once('=') {
            init_cookie.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(DriveConfig {
        api_base_url: "https://drive.quark.cn".to_string(),
        cookie: init_cookie,
        open: None,
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    if env::var("RUST_LOG").is_err() {
        if opt.debug {
            unsafe {
                env::set_var("RUST_LOG", "quarkdrive_webdav=debug,reqwest=debug");
            }
        } else {
            unsafe {
                env::set_var("RUST_LOG", "quarkdrive_webdav=info,reqwest=warn");
            }
        }
    }
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::time())
        .init();

    if let Some(Commands::Open(cmd)) = &opt.subcommands {
        return handle_open_command(cmd).await;
    }

    let drive_config = build_drive_config(&opt)?;
    let auth_user = opt.auth_user;
    let auth_password = opt.auth_password;
    if (auth_user.is_some() && auth_password.is_none())
        || (auth_user.is_none() && auth_password.is_some())
    {
        bail!("auth-user and auth-password must be specified together.");
    }

    let tls_config = match (opt.tls_cert, opt.tls_key) {
        (Some(cert), Some(key)) => Some((cert, key)),
        (None, None) => None,
        _ => bail!("tls-cert and tls-key must be specified together."),
    };
    let drive = QuarkDrive::new(drive_config)?;
    if drive.is_open_mode() {
        info!("使用夸克 Open API 模式");
        drive.init_open().await?;
    }
    let mut fs = QuarkDriveFileSystem::new(drive, opt.root, opt.cache_size, opt.cache_ttl)?;
    fs.set_no_trash(opt.no_trash)
        .set_read_only(opt.read_only)
        .set_upload_buffer_size(opt.upload_buffer_size)
        .set_skip_upload_same_size(opt.skip_upload_same_size)
        .set_prefer_http_download(opt.prefer_http_download)
        .set_upload_wait_timeout(opt.upload_wait_timeout)
        .set_chunk_cache_bytes(opt.chunk_cache_mb * 1024 * 1024);
    let cache = Arc::new(fs.dir_cache.clone());
    start_periodic_invalidate(cache.clone(), opt.refresh_cache_secs_interval);
    let fs_for_browser = fs.clone();
    let strip_prefix = opt.strip_prefix.clone();
    let mut dav_server_builder = DavHandler::builder()
        .filesystem(Box::new(fs))
        .locksystem(MemLs::new())
        .read_buf_size(opt.read_buffer_size)
        .autoindex(opt.auto_index)
        .redirect(opt.redirect);
    if let Some(prefix) = opt.strip_prefix {
        dav_server_builder = dav_server_builder.strip_prefix(prefix);
    }

    let dav_server = dav_server_builder.build_handler();
    debug!(
        read_buffer_size = opt.read_buffer_size,
        auto_index = opt.auto_index,
        "webdav handler initialized"
    );

    let server = WebDavServer {
        host: opt.host,
        port: opt.port,
        auth_user,
        auth_password,
        tls_config,
        handler: dav_server,
        fs: fs_for_browser,
        strip_prefix,
    };

    #[cfg(not(unix))]
    server.serve().await?;
    #[cfg(unix)]
    {
        let signals = Signals::new([SIGHUP])?;
        let handle = signals.handle();
        let signals_task = tokio::spawn(handle_signals(signals, cache));

        server.serve().await?;

        // Terminate the signal stream.
        handle.close();
        signals_task.await?;
    }
    Ok(())
}

/// 处理 `open login` / `open exchange`，方便拿到 refresh_token。
async fn handle_open_command(cmd: &OpenCommand) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("quarkdrive-webdav/open-login")
        .build()?;
    // middleware 客户端与驱动层一致，便于复用同一套 HTTP 栈。
    let client = reqwest_middleware::ClientBuilder::new(client).build();
    match cmd {
        OpenCommand::Login { redirect } => {
            let url = fetch_auth_url(&client, redirect).await?;
            println!("{url}");
            eprintln!("请在浏览器打开上述链接完成授权。");
            eprintln!(
                "授权完成后从回调 URL 取出 nonce，执行: quarkdrive-webdav open exchange --nonce <nonce>"
            );
        }
        OpenCommand::Exchange { nonce, token_file } => {
            let (access, refresh, app_id, sign_key) =
                exchange_token_by_nonce(&client, nonce).await?;
            println!("access_token={access}");
            println!("refresh_token={refresh}");
            if let Some(id) = &app_id {
                println!("app_id={id}");
            }
            if let Some(key) = &sign_key {
                println!("sign_key={key}");
            }
            if let Some(path) = token_file {
                let data = drive::TokenFile {
                    access_token: access,
                    refresh_token: refresh,
                    app_id,
                    sign_key,
                };
                if let Some(parent) = std::path::Path::new(path).parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                std::fs::write(path, serde_json::to_string_pretty(&data)?)?;
                eprintln!("已写入 {path}");
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn handle_signals(mut signals: Signals, dir_cache: Arc<Cache>) {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGHUP => {
                dir_cache.invalidate_all();
                info!("directory cache invalidated by SIGHUP");
            }
            _ => unreachable!(),
        }
    }
}
