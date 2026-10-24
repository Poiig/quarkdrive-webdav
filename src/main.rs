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

use alert::Alert;
use cache::Cache;
use drive::open::{OpenCredentials, OPEN_API_BASE};
use drive::*;
use oplog::OpLog;
use util::parse_byte_size;
use vfs::QuarkDriveFileSystem;
use webdav::WebDavServer;

mod alert;
mod cache;
mod drive;
mod oplog;
mod prefetch;
mod upload_limit;
mod util;
mod vfs;
mod webdav;
use tokio::time::interval;

/// tracing 时间戳固定为 Asia/Shanghai（UTC+8），不依赖容器 TZ。
struct ShanghaiTimer;

impl tracing_subscriber::fmt::time::FormatTime for ShanghaiTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", util::now_shanghai().format("%Y-%m-%d %H:%M:%S %z"))
    }
}

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

    /// 启用夸克 Open 模式（浏览器打开页面即可扫码授权）
    #[arg(long, env = "QUARK_OPEN", default_value_t = false)]
    quark_open: bool,

    /// Open refresh_token（可选；也可只靠 token 文件或 Web 扫码）
    #[arg(long, env = "QUARK_REFRESH_TOKEN", default_value = "")]
    quark_refresh_token: String,

    /// token 落盘路径；扫码/刷新后写回
    #[arg(long, env = "QUARK_TOKEN_FILE", default_value = "./quark-token.json")]
    quark_token_file: String,

    /// 企业微信机器人 webhook key（只填 key）
    #[arg(long, env = "WECOM_WEBHOOK_KEY")]
    wecom_webhook_key: Option<String>,

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
    /// 上传内存上限，支持 64MB / 1GB；并行合计，超限落盘。0=始终落盘（Cookie/Open 通用）
    #[arg(long, env = "UPLOAD_MEMORY_LIMIT", default_value = "64MB")]
    upload_memory_limit: String,
    /// 上传/删除操作日志目录
    #[arg(long, env = "OP_LOG_DIR", default_value = "./logs")]
    op_log_dir: String,
    /// 操作日志保留天数，默认 7；未设置即为 7
    #[arg(long, env = "OP_LOG_RETENTION_DAYS", default_value_t = 7)]
    op_log_retention_days: u64,
    /// 目录缓存最多保留多少个目录（与字节上限共同约束）
    #[arg(long, env = "DIR_CACHE_SIZE", default_value = "1000")]
    cache_size: u64,
    /// 目录缓存字节上限，如 64MB；大目录列表按估算字节计入
    #[arg(long, env = "DIR_CACHE_BYTES", default_value = "64MB")]
    cache_bytes: String,
    /// Directory entries cache expiration time in seconds
    #[arg(long, env = "DIR_CACHE_TTL", default_value = "600")]
    cache_ttl: u64,
    /// 分块预取缓存上限（MB）。0 关闭
    #[arg(long, env = "CHUNK_CACHE_MB", default_value = "128")]
    chunk_cache_mb: u64,
    /// 预取分块大小（MB），实测 16 为拐点；低配可降到 8
    #[arg(long, env = "PREFETCH_CHUNK_MB", default_value_t = 16)]
    prefetch_chunk_mb: u64,
    /// 预取并发窗口（1–8），默认 4；低配可降到 2
    #[arg(long, env = "PREFETCH_AHEAD", default_value_t = 4)]
    prefetch_ahead: usize,
    /// 同时进行的后台上传上限，防止多客户端打满内存/连接
    #[arg(long, env = "UPLOAD_MAX_CONCURRENT", default_value_t = 4)]
    upload_max_concurrent: usize,
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

    /// 等待上传完成的最长秒数。默认 0=一直等到夸克上传完成再返回客户端；
    /// 设为正数则超时后先返回、后台继续传（旧「快返回」行为）。
    #[arg(long, env = "UPLOAD_WAIT_TIMEOUT", default_value_t = 0)]
    upload_wait_timeout: u64,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scan QRCode（旧 Cookie 扫码，保留兼容）
    #[command(subcommand)]
    Qr(QrCommand),
}

#[derive(Subcommand, Debug)]
enum QrCommand {
    Login,
    Generate,
    #[command(arg_required_else_help = true)]
    Query {
        #[arg(long)]
        sid: String,
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

/// 组装 DriveConfig：Open 用内置 AppId/SignKey，Cookie 用 QUARK_COOKIE。
fn build_drive_config(opt: &Opt, alert: Alert) -> anyhow::Result<DriveConfig> {
    if opt.quark_open {
        let open = OpenCredentials::new(
            opt.quark_refresh_token.clone(),
            Some(opt.quark_token_file.clone()),
        )?;
        return Ok(DriveConfig {
            api_base_url: OPEN_API_BASE.to_string(),
            cookie: Arc::new(DashMap::new()),
            open: Some(open),
            alert,
        });
    }

    let cookie_str = opt.quark_cookie.clone().ok_or_else(|| {
        anyhow::anyhow!("请设置 QUARK_COOKIE，或使用 --quark-open / QUARK_OPEN=true")
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
        alert,
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
    // 输出到 stdout，Docker / 群晖容器控制台可直接看；时间固定 Asia/Shanghai。
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(ShanghaiTimer)
        .with_ansi(false)
        .with_writer(std::io::stdout)
        .init();

    let alert = Alert::new(opt.wecom_webhook_key.clone());
    if alert.enabled() {
        info!("已启用企业微信告警");
    }

    let upload_memory_limit = parse_byte_size(&opt.upload_memory_limit)?;
    info!(
        upload_memory_limit = %opt.upload_memory_limit,
        upload_memory_bytes = upload_memory_limit,
        "upload memory limit"
    );
    prefetch::init_prefetch_config(opt.prefetch_chunk_mb, opt.prefetch_ahead);
    upload_limit::init_upload_concurrency(opt.upload_max_concurrent);

    let drive_config = build_drive_config(&opt, alert)?;
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
        info!("使用夸克 Open 模式（可在浏览器扫码授权）");
        drive.init_open().await?;
    } else {
        let _ = drive.check_cookie_auth().await;
    }
    let oplog = Arc::new(OpLog::new(
        &opt.op_log_dir,
        opt.op_log_retention_days,
    ));
    info!(
        op_log_dir = %oplog.dir().display(),
        retention_days = oplog.retention_days(),
        "operation log enabled"
    );
    oplog.start_prune_task();

    let cache_bytes = parse_byte_size(&opt.cache_bytes)?;
    let mut fs =
        QuarkDriveFileSystem::new(drive, opt.root, opt.cache_size, cache_bytes, opt.cache_ttl)?;
    fs.set_no_trash(opt.no_trash)
        .set_read_only(opt.read_only)
        .set_upload_buffer_size(opt.upload_buffer_size)
        .set_upload_memory_limit(upload_memory_limit)
        .set_skip_upload_same_size(opt.skip_upload_same_size)
        .set_prefer_http_download(opt.prefer_http_download)
        .set_upload_wait_timeout(opt.upload_wait_timeout)
        .set_chunk_cache_bytes(opt.chunk_cache_mb * 1024 * 1024)
        .set_oplog(oplog);
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

        handle.close();
        signals_task.await?;
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
