use std::future::Future;
use std::io;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use anyhow::Result;
use dav_server::{body::Body, DavConfig, DavHandler};
use headers::{authorization::Basic, Authorization, HeaderMapExt};
use http_body_util::BodyExt;
use hyper::service::Service;
use hyper::{Method, Request, Response};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::vfs::QuarkDriveFileSystem;

/// 浏览器表单登录后的会话 Cookie 名（WebDAV 客户端仍走 Basic）。
const WEB_SESSION_COOKIE: &str = "qd_session";

pub struct WebDavServer {
    pub host: String,
    pub port: u16,
    pub auth_user: Option<String>,
    pub auth_password: Option<String>,
    pub tls_config: Option<(PathBuf, PathBuf)>,
    pub handler: DavHandler,
    pub fs: QuarkDriveFileSystem,
    pub strip_prefix: Option<String>,
}

impl WebDavServer {
    pub async fn serve(self) -> Result<()> {
        let addr = (self.host, self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::from(io::ErrorKind::AddrNotAvailable))?;

        let make_svc = MakeSvc {
            auth_user: self.auth_user.clone(),
            auth_password: self.auth_password.clone(),
            handler: self.handler.clone(),
            fs: self.fs.clone(),
            strip_prefix: self.strip_prefix.clone(),
        };

        let listener = TcpListener::bind(&addr).await?;
        info!("listening on http://{}", listener.local_addr()?);

        loop {
            let (tcp, _) = listener.accept().await?;
            let io = TokioIo::new(tcp);
            let make_svc = make_svc.clone();

            tokio::spawn(async move {
                let service = match make_svc.call(()).await {
                    Ok(service) => service,
                    Err(_) => return,
                };

                if let Err(e) = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await
                {
                    // 客户端（如 CloudSync）等不及 PUT 完成就断开时很常见，不是服务故障。
                    let msg = e.to_string();
                    if msg.contains("connection closed before message completed")
                        || msg.contains("connection reset")
                        || msg.contains("broken pipe")
                    {
                        debug!("HTTP client disconnected: {}", msg);
                    } else {
                        error!("HTTP serve error: {}", e);
                    }
                }
            });
        }

        // 循环会持续运行，实际不会到达这里
        Ok(())
    }
}

#[derive(Clone)]
pub struct QuarkDriveWebDav {
    auth_user: Option<String>,
    auth_password: Option<String>,
    handler: DavHandler,
    fs: QuarkDriveFileSystem,
    strip_prefix: Option<String>,
}

impl QuarkDriveWebDav {
    /// 是否走 HTML 浏览层（目录列表 / 图片预览）。
    /// 对齐常见浏览器；WebDAV 客户端通常带 Depth，排除之。
    /// `?raw=1` / `?download=1` 强制走原始下载（预览页拉图用）。
    fn is_browser_request(req: &Request<hyper::body::Incoming>) -> bool {
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return false;
        }
        if Self::wants_raw_bytes(req) {
            return false;
        }
        // PROPFIND / 网盘客户端常见；不要当成浏览器。
        if req.headers().contains_key("depth") {
            return false;
        }
        if let Some(accept) = req.headers().get("accept") {
            if let Ok(accept_str) = accept.to_str() {
                if accept_str.contains("text/html") {
                    return true;
                }
            }
        }
        if let Some(ua) = req.headers().get("user-agent") {
            if let Ok(ua) = ua.to_str() {
                let ua = ua.to_ascii_lowercase();
                if ua.contains("mozilla")
                    || ua.contains("chrome")
                    || ua.contains("safari")
                    || ua.contains("edg/")
                    || ua.contains("firefox")
                {
                    return true;
                }
            }
        }
        false
    }

    fn wants_raw_bytes(req: &Request<hyper::body::Incoming>) -> bool {
        req.uri()
            .query()
            .unwrap_or("")
            .split('&')
            .any(|p| p == "raw=1" || p == "download=1" || p.starts_with("raw=") || p == "raw")
    }

    /// `?thumb=1`：同源代理夸克缩略图（避免浏览器直链 CDN 被防盗链打成「图片加载失败」）。
    fn wants_thumb(req: &Request<hyper::body::Incoming>) -> bool {
        req.uri()
            .query()
            .unwrap_or("")
            .split('&')
            .any(|p| p == "thumb=1" || p.starts_with("thumb=") || p == "thumb")
    }

    fn compute_fs_path(&self, req_path: &str) -> PathBuf {
        let path = if let Some(ref prefix) = self.strip_prefix {
            let prefix = prefix.trim_end_matches('/');
            req_path
                .strip_prefix(prefix)
                .unwrap_or(req_path)
        } else {
            req_path
        };

        let path = path.trim_start_matches('/');
        let path = path.trim_end_matches('/');
        let path = percent_decode(path);

        if self.fs.root == Path::new("/") {
            if path.is_empty() {
                PathBuf::from("/")
            } else {
                PathBuf::from("/").join(&path)
            }
        } else if path.is_empty() {
            self.fs.root.clone()
        } else {
            self.fs.root.join(&path)
        }
    }

    /// Open 授权相关浏览器路径（状态页 / 扫码入口）。
    fn is_quark_auth_path(path: &str) -> bool {
        path == "/__quark_auth"
            || path.starts_with("/__quark_auth/")
            || path == "/__quark_auth/start"
            || path == "/__quark_auth/callback"
    }

    /// 仅 OAuth 回调可跳过账号校验（夸克重定向不会带 WebDAV 账号/会话）。
    fn is_quark_auth_callback(path: &str) -> bool {
        path == "/__quark_auth/callback"
    }

    /// 浏览器登录/登出页：未鉴权可访问，避免再弹 Basic 对话框。
    fn is_web_login_path(path: &str) -> bool {
        path == "/__login" || path == "/__logout"
    }

    /// 控制台：上传进度 / 历史。
    fn is_console_path(path: &str) -> bool {
        path == "/__uploads"
            || path == "/__uploads.json"
            || path == "/__history"
            || path.starts_with("/__history/")
    }

    /// 未登录时是否应展示 HTML 登录页（浏览器），而非 WWW-Authenticate 弹窗。
    fn wants_web_login_page(req: &Request<hyper::body::Incoming>) -> bool {
        if Self::is_browser_request(req) {
            return true;
        }
        if let Some(accept) = req.headers().get("accept") {
            if let Ok(accept_str) = accept.to_str() {
                if accept_str.contains("text/html") {
                    return true;
                }
            }
        }
        if let Some(ua) = req.headers().get("user-agent") {
            if let Ok(ua) = ua.to_str() {
                let ua = ua.to_ascii_lowercase();
                if ua.contains("mozilla")
                    || ua.contains("chrome")
                    || ua.contains("safari")
                    || ua.contains("edg/")
                    || ua.contains("firefox")
                {
                    return true;
                }
            }
        }
        false
    }

    /// Basic 正确，或浏览器会话 Cookie 有效。
    fn is_authenticated(
        &self,
        req: &Request<hyper::body::Incoming>,
        auth_user: &str,
        auth_password: &str,
    ) -> bool {
        if let Some(Authorization(basic)) = req.headers().typed_get::<Authorization<Basic>>() {
            if basic.username() == auth_user && basic.password() == auth_password {
                return true;
            }
        }
        let expected = web_session_token(auth_user, auth_password);
        cookie_value(req, WEB_SESSION_COOKIE)
            .map(|v| v == expected)
            .unwrap_or(false)
    }

    /// 表单登录 / 登出：校验后发 Cookie，替代浏览器 Basic 弹窗。
    async fn handle_web_login(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Response<Body> {
        let path = req.uri().path().to_string();
        let query = parse_query(req.uri().query().unwrap_or(""));

        if path == "/__logout" {
            return Response::builder()
                .status(302)
                .header("Location", "/__login")
                .header(
                    "Set-Cookie",
                    format!(
                        "{WEB_SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
                    ),
                )
                .body(Body::from("logged out"))
                .unwrap();
        }

        let (auth_user, auth_password) = match (&self.auth_user, &self.auth_password) {
            (Some(u), Some(p)) => (u.clone(), p.clone()),
            _ => {
                return Response::builder()
                    .status(302)
                    .header("Location", "/")
                    .body(Body::from("auth disabled"))
                    .unwrap();
            }
        };

        if req.method() == Method::POST {
            let body_bytes = match req.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => {
                    return html_response(
                        400,
                        render_web_login_page(Some("无法读取登录表单"), "/"),
                    );
                }
            };
            let body = String::from_utf8_lossy(&body_bytes);
            let form = parse_query(body.trim());
            let user = form.get("username").map(|s| s.as_str()).unwrap_or("");
            let pass = form.get("password").map(|s| s.as_str()).unwrap_or("");
            let next = sanitize_next(
                form
                    .get("next")
                    .map(|s| s.as_str())
                    .or_else(|| query.get("next").map(|s| s.as_str())),
            );
            if user == auth_user && pass == auth_password {
                let token = web_session_token(&auth_user, &auth_password);
                return Response::builder()
                    .status(302)
                    .header("Location", next)
                    .header(
                        "Set-Cookie",
                        format!(
                            "{WEB_SESSION_COOKIE}={token}; Path=/; Max-Age={}; HttpOnly; SameSite=Lax",
                            60 * 60 * 24 * 30 // 30 天：浏览器会话，改密码后旧 Cookie 立即失效
                        ),
                    )
                    .body(Body::from("ok"))
                    .unwrap();
            }
            return html_response(
                401,
                render_web_login_page(Some("用户名或密码错误"), &next),
            );
        }

        let next = sanitize_next(query.get("next").map(|s| s.as_str()));
        if cookie_value(&req, WEB_SESSION_COOKIE)
            .map(|v| v == web_session_token(&auth_user, &auth_password))
            .unwrap_or(false)
        {
            return Response::builder()
                .status(302)
                .header("Location", next)
                .body(Body::from("already logged in"))
                .unwrap();
        }
        html_response(200, render_web_login_page(None, &next))
    }

    async fn handle_browser_request(
        &self,
        req: &Request<hyper::body::Incoming>,
    ) -> Option<Response<Body>> {
        let req_path = req.uri().path();
        if Self::is_console_path(req_path) {
            return Some(self.handle_console(req).await);
        }
        if Self::is_quark_auth_path(req_path) {
            return Some(self.handle_quark_auth(req).await);
        }

        // Open 模式：授权失效时引导扫码（调用方须已通过 Web 会话或 Basic）。
        if self.fs.drive.is_open_mode() {
            match self.fs.drive.check_open_auth().await {
                Ok(true) => {}
                Ok(false) => {
                    return Some(html_response(200, render_open_login_page(false, None)));
                }
                Err(err) => {
                    let msg = format!("{err:#}");
                    self.fs.drive.alert().notify("Open 授权检测失败", &msg).await;
                    return Some(html_response(
                        200,
                        render_open_login_page(false, Some(&msg)),
                    ));
                }
            }
        }

        let fs_path = self.compute_fs_path(req_path);
        let is_head = req.method() == Method::HEAD;
        debug!(req_path = %req_path, fs_path = %fs_path.display(), "browser: checking path");

        // 缩略图代理必须先于目录/预览 HTML，供 <img src="?thumb=1"> 使用。
        if Self::wants_thumb(req) {
            return Some(self.proxy_thumbnail(&fs_path, is_head).await);
        }

        // 能列出 → 一定是目录。失败时再查条目类型，避免落到 dav-server 对目录 GET 返回 405
        //（与 OpenList 对目录 HEAD/GET 行为对齐）。
        let cache_key = fs_path.to_string_lossy().into_owned();
        if let Some(files) = self.fs.dir_cache.get_or_insert(&cache_key).await {
            debug!(req_path = %req_path, count = files.len(), "browser: directory listing");
            if is_head {
                return Some(
                    Response::builder()
                        .status(200)
                        .header("Content-Type", "text/html; charset=utf-8")
                        .header("Content-Length", "0")
                        .body(Body::from(""))
                        .unwrap(),
                );
            }
            return Some(html_response(200, render_directory_html(req_path, &files)));
        }

        let entry = self.lookup_entry(&fs_path).await;
        if entry.as_ref().map(|f| f.dir).unwrap_or(fs_path == Path::new("/")) {
            let html = render_dir_error_page(
                req_path,
                "目录加载失败，请刷新重试。若刚授权，稍等片刻再进。",
            );
            return Some(html_response(200, html));
        }

        // 图片：浏览器内预览（含 HEIC，前端 heic2any 转 JPEG）。
        if let Some(file) = entry {
            if is_previewable_image(&file.file_name) {
                if is_head {
                    return Some(
                        Response::builder()
                            .status(200)
                            .header("Content-Type", "text/html; charset=utf-8")
                            .header("Content-Length", "0")
                            .body(Body::from(""))
                            .unwrap(),
                    );
                }
                let has_thumb = file
                    .thumbnail_url
                    .as_deref()
                    .map(|u| !u.is_empty())
                    .unwrap_or(false);
                return Some(html_response(
                    200,
                    render_image_preview_page(req_path, &file.file_name, has_thumb),
                ));
            }
        }

        // 其它文件交给 WebDAV 下载。
        None
    }

    /// 服务端代拉夸克 thumbnail_url，带 pan.quark.cn Referer，绕过浏览器防盗链。
    async fn proxy_thumbnail(&self, fs_path: &Path, is_head: bool) -> Response<Body> {
        let Some(file) = self.lookup_entry(fs_path).await else {
            return Response::builder()
                .status(404)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Body::from("file not found"))
                .unwrap();
        };
        let Some(thumb) = file.thumbnail_url.as_deref().filter(|u| !u.is_empty()) else {
            return Response::builder()
                .status(404)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Body::from("no thumbnail"))
                .unwrap();
        };
        match self.fs.drive.fetch_thumbnail(thumb).await {
            Ok((bytes, ctype)) => {
                let ctype = if ctype.to_ascii_lowercase().starts_with("image/") {
                    ctype
                } else {
                    "image/jpeg".to_string()
                };
                if is_head {
                    return Response::builder()
                        .status(200)
                        .header("Content-Type", ctype)
                        .header("Content-Length", bytes.len().to_string())
                        .header("Cache-Control", "private, max-age=300")
                        .body(Body::from(""))
                        .unwrap();
                }
                Response::builder()
                    .status(200)
                    .header("Content-Type", ctype)
                    .header("Content-Length", bytes.len().to_string())
                    .header("Cache-Control", "private, max-age=300")
                    .body(Body::from(bytes))
                    .unwrap()
            }
            Err(err) => {
                error!(
                    path = %fs_path.display(),
                    file_name = %file.file_name,
                    error = %err,
                    "proxy thumbnail failed"
                );
                Response::builder()
                    .status(502)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(Body::from(format!("thumbnail fetch failed: {err:#}")))
                    .unwrap()
            }
        }
    }

    /// 在父目录缓存中查找当前路径对应条目。
    async fn lookup_entry(&self, fs_path: &Path) -> Option<crate::drive::QuarkFile> {
        if fs_path == Path::new("/") {
            return Some(crate::drive::QuarkFile::new_root());
        }
        let parent = fs_path.parent()?;
        let name = fs_path.file_name()?.to_str()?;
        let parent_key = if parent == Path::new("") {
            "/".to_string()
        } else {
            parent.to_string_lossy().into_owned()
        };
        let files = self.fs.dir_cache.get_or_insert(&parent_key).await?;
        files.into_iter().find(|f| f.file_name == name)
    }

    async fn handle_quark_auth(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        if !self.fs.drive.is_open_mode() {
            return html_response(
                400,
                "<p>当前不是 Open 模式，无需扫码授权。</p>".to_string(),
            );
        }
        let path = req.uri().path();
        let query = req.uri().query().unwrap_or("");

        if path.ends_with("/callback") {
            let nonce = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("nonce="))
                .map(|v| percent_decode(v))
                .unwrap_or_default();
            if nonce.is_empty() {
                return html_response(
                    400,
                    render_open_login_page(false, Some("回调缺少 nonce，请重试扫码")),
                );
            }
            return match self.fs.drive.complete_open_login(&nonce).await {
                Ok(()) => {
                    self.fs.dir_cache.invalidate_all();
                    Response::builder()
                        .status(302)
                        .header("Location", "/")
                        .body(Body::from("授权成功，正在跳转…"))
                        .unwrap()
                }
                Err(err) => {
                    let msg = format!("{err:#}");
                    html_response(200, render_open_login_page(false, Some(&msg)))
                }
            };
        }

        if path.ends_with("/start") {
            // 回调指回本服务；优先用请求 Host，便于 Docker/反代。
            let host = req
                .headers()
                .get("x-forwarded-host")
                .or_else(|| req.headers().get("host"))
                .and_then(|v| v.to_str().ok())
                .unwrap_or("127.0.0.1:8080");
            let proto = req
                .headers()
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("http");
            let redirect = format!("{proto}://{host}/__quark_auth/callback");
            return match self.fs.drive.open_auth_url(&redirect).await {
                Ok(url) => Response::builder()
                    .status(302)
                    .header("Location", url)
                    .body(Body::from("redirecting to quark oauth"))
                    .unwrap(),
                Err(err) => {
                    let msg = format!("获取授权链接失败: {err:#}");
                    self.fs.drive.alert().notify("获取 Open 授权链接失败", &msg).await;
                    html_response(200, render_open_login_page(false, Some(&msg)))
                }
            };
        }

        // /__quark_auth 状态页
        match self.fs.drive.check_open_auth().await {
            Ok(true) => html_response(200, render_open_login_page(true, None)),
            Ok(false) => html_response(200, render_open_login_page(false, None)),
            Err(err) => {
                let msg = format!("{err:#}");
                html_response(200, render_open_login_page(false, Some(&msg)))
            }
        }
    }

    /// Web 控制台：上传进度与操作历史（须已通过浏览器会话或 Basic）。
    async fn handle_console(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        let path = req.uri().path();
        let query = req.uri().query().unwrap_or("");
        if path == "/__uploads.json" {
            let items = self.fs.upload_tracker().list();
            let list: Vec<serde_json::Value> = items
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "path": e.path,
                        "size": e.size,
                        "received": e.received,
                        "uploaded": e.uploaded,
                        "status": e.status,
                        "detail": e.detail,
                        "percent": e.percent(),
                        "speed_bps": e.speed_bps,
                        "avg_speed_bps": e.avg_speed_bps(),
                        "summary": e.summary(),
                        "updated_ms": e.updated_ms,
                    })
                })
                .collect();
            let body = serde_json::to_string(&serde_json::json!({ "items": list }))
                .unwrap_or_else(|_| r#"{"items":[]}"#.into());
            return Response::builder()
                .status(200)
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Cache-Control", "no-store")
                .body(Body::from(body))
                .unwrap();
        }
        if path == "/__uploads" {
            return html_response(200, render_uploads_page(&self.fs.upload_tracker().list()));
        }
        if path == "/__history" {
            let params = parse_query(query);
            if req.method() == Method::POST
                || params.get("clear").map(|v| v == "1" || v == "true").unwrap_or(false)
            {
                let date = params.get("date").map(|s| s.as_str()).filter(|s| !s.is_empty());
                match self.fs.oplog().clear(date).await {
                    Ok(n) => {
                        return Response::builder()
                            .status(302)
                            .header(
                                "Location",
                                format!("/__history?cleared={n}&kind=all&result=all"),
                            )
                            .body(Body::from("cleared"))
                            .unwrap();
                    }
                    Err(err) => {
                        return html_response(
                            400,
                            render_history_page(vec![], &params, Some(&err)),
                        );
                    }
                }
            }
            let kind = params.get("kind").map(|s| s.as_str()).unwrap_or("upload");
            let result = params.get("result").map(|s| s.as_str()).unwrap_or("all");
            let q = params.get("q").map(|s| s.as_str());
            let limit = params
                .get("limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(200usize);
            let entries = self
                .fs
                .oplog()
                .list_entries(
                    limit,
                    Some(kind),
                    Some(result),
                    q.filter(|s| !s.is_empty()),
                )
                .await;
            return html_response(200, render_history_page(entries, &params, None));
        }
        html_response(404, "<p>not found</p>".into())
    }
}

/// 解析 `a=1&b=2` 查询串。
fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let mut it = part.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        m.insert(percent_decode(k), percent_decode(v));
    }
    m
}

fn html_response(status: u16, html: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Body::from(html))
        .unwrap()
}

/// 由账号密码派生会话令牌；改密后旧 Cookie 自动失效，无需服务端存会话。
fn web_session_token(user: &str, password: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"quarkdrive-webdav|web-session|v1|");
    h.update(user.as_bytes());
    h.update(b"|");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}

/// 从 Cookie 头取出指定名称的值。
fn cookie_value<B>(req: &Request<B>, name: &str) -> Option<String> {
    let raw = req.headers().get("cookie")?.to_str().ok()?;
    for part in raw.split(';') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == name {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

/// 登录成功后的回跳地址：仅允许站内相对路径，防开放重定向。
fn sanitize_next(next: Option<&str>) -> String {
    match next {
        Some(n) if n.starts_with('/') && !n.starts_with("//") && !n.contains('\r') && !n.contains('\n') => {
            n.to_string()
        }
        _ => "/".to_string(),
    }
}

/// 未登录浏览器跳转到登录页时编码 next。
fn login_redirect_location(req_path: &str, query: Option<&str>) -> String {
    let mut next = req_path.to_string();
    if let Some(q) = query {
        if !q.is_empty() {
            next.push('?');
            next.push_str(q);
        }
    }
    let next = sanitize_next(Some(&next));
    let enc = percent_encoding::utf8_percent_encode(
        &next,
        percent_encoding::NON_ALPHANUMERIC,
    );
    format!("/__login?next={enc}")
}

/// WebDAV 账号的浏览器登录页（替代 Basic 弹窗）。
fn render_web_login_page(error: Option<&str>, next: &str) -> String {
    let err = error
        .map(|e| format!(r#"<p class="err" role="alert">{}</p>"#, html_escape(e)))
        .unwrap_or_default();
    let next_attr = html_escape(next);
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>登录 · QuarkDrive WebDAV</title>
<style>
:root{{
  --bg0:#101820; --bg1:#1e3a4c; --card:#eef6f4;
  --ink:#12222b; --muted:#4a6670; --line:#b7c9cf;
  --accent:#0e7490; --accent-hover:#0891b2; --danger:#b91c1c;
}}
*{{box-sizing:border-box}}
body{{
  margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
  font-family:"Segoe UI","PingFang SC","Noto Sans SC",sans-serif;
  color:var(--ink);
  background:
    radial-gradient(900px 480px at 10% 15%, rgba(14,116,144,.4), transparent 55%),
    radial-gradient(720px 400px at 90% 85%, rgba(34,197,94,.12), transparent 50%),
    linear-gradient(160deg, var(--bg0), var(--bg1) 60%, #25485c);
}}
.card{{
  width:min(400px,calc(100% - 32px));
  background:var(--card);
  padding:36px 32px 28px;
  box-shadow:0 24px 60px rgba(0,0,0,.35);
}}
.brand{{
  font-size:1.35rem;font-weight:700;letter-spacing:.02em;margin:0 0 6px;
}}
.sub{{margin:0 0 22px;color:var(--muted);font-size:14px;line-height:1.5}}
label{{display:block;font-size:12px;color:var(--muted);margin:0 0 6px}}
input{{
  width:100%;padding:11px 12px;margin:0 0 14px;border:1px solid var(--line);
  background:#fff;font-size:15px;color:var(--ink);
}}
input:focus{{outline:2px solid rgba(14,116,144,.35);border-color:var(--accent)}}
button{{
  width:100%;margin-top:6px;padding:12px 14px;border:0;cursor:pointer;
  background:var(--accent);color:#fff;font-size:15px;font-weight:600;
}}
button:hover{{background:var(--accent-hover)}}
.err{{
  background:#fef2f2;color:var(--danger);padding:10px 12px;margin:0 0 14px;font-size:13px;
}}
.hint{{margin:18px 0 0;font-size:12px;color:var(--muted);line-height:1.55}}
</style></head><body>
<main class="card">
  <h1 class="brand">QuarkDrive WebDAV</h1>
  <p class="sub">使用 WebDAV 账号登录控制台与网盘浏览</p>
  {err}
  <form method="post" action="/__login" autocomplete="on">
    <input type="hidden" name="next" value="{next_attr}"/>
    <label for="username">用户名</label>
    <input id="username" name="username" type="text" autocomplete="username" required autofocus/>
    <label for="password">密码</label>
    <input id="password" name="password" type="password" autocomplete="current-password" required/>
    <button type="submit">登录</button>
  </form>
  <p class="hint">WebDAV 客户端（RaiDrive / CloudSync 等）仍使用协议自带的 Basic 认证，不受此页影响。</p>
</main>
</body></html>"#
    )
}

/// Open 模式登录/状态页：有效则提示正常，无效则引导扫码。
fn render_open_login_page(ok: bool, error: Option<&str>) -> String {
    let status = if ok {
        r#"<p class="ok">授权有效，可正常使用 WebDAV / 浏览文件。</p>
           <p><a href="/">进入网盘目录</a></p>"#
    } else {
        r#"<p class="bad">授权无效或未登录。</p>
           <p>请点击下方按钮，用夸克账号完成扫码/登录授权。</p>
           <p><a class="btn" href="/__quark_auth/start">扫码授权</a></p>"#
    };
    let err = error
        .map(|e| format!(r#"<pre class="err">{}</pre>"#, html_escape(e)))
        .unwrap_or_default();
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>夸克 Open 授权</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:640px;margin:40px auto;padding:0 16px;line-height:1.6}}
.ok{{color:#0a7}} .bad{{color:#c33}} .err{{background:#f6f6f6;padding:12px;overflow:auto}}
.btn{{display:inline-block;background:#1677ff;color:#fff;padding:10px 18px;border-radius:6px;text-decoration:none}}
.nav a{{margin-right:12px;color:#2563eb}}
</style></head><body>
<p class="nav"><a href="/">目录</a><a href="/__uploads">上传进度</a><a href="/__history">上传历史</a><a href="/__quark_auth">授权</a><a href="/__logout">退出</a></p>
<h1>夸克 Open 授权</h1>
{status}
{err}
<p><a href="/__quark_auth">刷新状态</a></p>
</body></html>"#
    )
}

/// 上传进度页：每 2 秒刷新，展示文件、百分比与速度。
fn render_uploads_page(items: &[crate::upload_progress::ProgressEntry]) -> String {
    let mut rows = String::new();
    if items.is_empty() {
        rows.push_str(r#"<tr><td colspan="5" class="empty">当前没有进行中的上传</td></tr>"#);
    }
    for e in items {
        let pct = e.percent();
        let status_class = match e.status.as_str() {
            "ok" | "skip" => "ok",
            "error" => "bad",
            "uploading" | "receiving" => "run",
            _ => "",
        };
        let speed = format!(
            "{}/s<br/><span class=\"avg\">均 {}/s</span>",
            html_escape(&crate::util::format_bytes(e.speed_bps)),
            html_escape(&crate::util::format_bytes(e.avg_speed_bps()))
        );
        rows.push_str(&format!(
            r#"<tr>
<td class="path">{path}</td>
<td class="status {status_class}">{status}</td>
<td class="pct"><div class="bar"><i style="width:{pct}%"></i></div>{pct}% · 收 {recv} / 传 {up} / 共 {total}</td>
<td class="speed">{speed}</td>
<td class="detail">{detail}</td>
</tr>"#,
            path = html_escape(&e.path),
            status = html_escape(&e.status),
            status_class = status_class,
            pct = pct,
            recv = html_escape(&crate::util::format_bytes(e.received)),
            up = html_escape(&crate::util::format_bytes(e.uploaded)),
            total = html_escape(&crate::util::format_bytes(e.size)),
            speed = speed,
            detail = html_escape(&e.detail),
        ));
    }
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<meta http-equiv="refresh" content="2"/>
<title>上传进度</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:24px auto;padding:0 16px;color:#222;background:#f7f7f5}}
.nav a{{margin-right:14px;color:#1d4ed8;text-decoration:none}}
h1{{font-size:1.25rem;margin:12px 0 16px}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{padding:10px 12px;border-bottom:1px solid #eee;text-align:left;font-size:14px;vertical-align:top}}
th{{color:#666;font-weight:600}}
.path{{word-break:break-all;max-width:320px}}
.status.ok{{color:#0a7}} .status.bad{{color:#c33}} .status.run{{color:#b45309}}
.bar{{height:8px;background:#eee;margin-bottom:4px;overflow:hidden}}
.bar i{{display:block;height:100%;background:#2563eb}}
.speed{{white-space:nowrap;font-variant-numeric:tabular-nums}}
.speed .avg{{color:#888;font-size:12px}}
.empty{{color:#888;text-align:center}}
.detail{{color:#666;font-size:12px;max-width:200px;word-break:break-all}}
</style></head><body>
<p class="nav"><a href="/">目录</a><a href="/__uploads">上传进度</a><a href="/__history">上传历史</a><a href="/__quark_auth">授权</a><a href="/__logout">退出</a></p>
<h1>上传进度 <span style="font-size:13px;color:#888;font-weight:400">每 2 秒刷新 · 含瞬时/平均速度</span></h1>
<table>
<thead><tr><th>文件</th><th>状态</th><th>进度</th><th>速度</th><th>备注</th></tr></thead>
<tbody>{rows}</tbody>
</table>
</body></html>"#
    )
}

/// 操作历史页：支持 kind/result/path 筛选与清除。
fn render_history_page(
    entries: Vec<crate::oplog::OpLogEntry>,
    params: &std::collections::HashMap<String, String>,
    error: Option<&str>,
) -> String {
    let kind = params.get("kind").map(|s| s.as_str()).unwrap_or("upload");
    let result = params.get("result").map(|s| s.as_str()).unwrap_or("all");
    let q = params.get("q").cloned().unwrap_or_default();
    let cleared = params.get("cleared").cloned().unwrap_or_default();
    let err = error
        .map(|e| format!(r#"<p class="bad">{}</p>"#, html_escape(e)))
        .unwrap_or_default();
    let notice = if !cleared.is_empty() {
        format!(r#"<p class="ok">已清除 {cleared} 个日志文件</p>"#)
    } else {
        String::new()
    };
    let mut rows = String::new();
    if entries.is_empty() {
        rows.push_str(r#"<tr><td colspan="5" class="empty">没有匹配的记录</td></tr>"#);
    }
    for e in &entries {
        let rc = if e.result.starts_with("error") {
            "bad"
        } else {
            "ok"
        };
        rows.push_str(&format!(
            r#"<tr>
<td class="time">{time}</td>
<td>{kind}</td>
<td class="{rc}">{result}</td>
<td>{size}</td>
<td class="path">{path}</td>
</tr>"#,
            time = html_escape(&e.time),
            kind = html_escape(&e.kind),
            rc = rc,
            result = html_escape(&e.result),
            size = html_escape(&e.size),
            path = html_escape(&e.path),
        ));
    }
    let sel = |cur: &str, want: &str| if cur == want { " selected" } else { "" };
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>上传历史</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:24px auto;padding:0 16px;color:#222;background:#f7f7f5}}
.nav a{{margin-right:14px;color:#1d4ed8;text-decoration:none}}
h1{{font-size:1.25rem;margin:12px 0}}
form.filters{{display:flex;flex-wrap:wrap;gap:8px;align-items:end;margin:12px 0 16px;background:#fff;padding:12px}}
form.filters label{{display:flex;flex-direction:column;font-size:12px;color:#666;gap:4px}}
form.filters input,form.filters select{{padding:6px 8px;font-size:14px}}
form.filters button,.btn{{padding:7px 12px;font-size:14px;border:1px solid #ccc;background:#fff;cursor:pointer}}
form.filters button.primary{{background:#2563eb;color:#fff;border-color:#2563eb}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{padding:8px 10px;border-bottom:1px solid #eee;text-align:left;font-size:13px}}
th{{color:#666}}
.path{{word-break:break-all}}
.ok{{color:#0a7}} .bad{{color:#c33}} .empty{{color:#888;text-align:center}}
.danger{{color:#b91c1c}}
</style></head><body>
<p class="nav"><a href="/">目录</a><a href="/__uploads">上传进度</a><a href="/__history">上传历史</a><a href="/__quark_auth">授权</a><a href="/__logout">退出</a></p>
<h1>操作历史</h1>
{err}{notice}
<form class="filters" method="get" action="/__history">
  <label>类型<select name="kind">
    <option value="all"{k_all}>全部</option>
    <option value="upload"{k_up}>上传</option>
    <option value="delete"{k_del}>删除</option>
  </select></label>
  <label>结果<select name="result">
    <option value="all"{r_all}>全部</option>
    <option value="ok"{r_ok}>成功/跳过</option>
    <option value="error"{r_err}>失败</option>
  </select></label>
  <label>路径包含<input name="q" value="{q}" placeholder="关键词"/></label>
  <button class="primary" type="submit">筛选</button>
</form>
<form class="filters" method="get" action="/__history" onsubmit="return confirm('确认清除？不可恢复');">
  <input type="hidden" name="clear" value="1"/>
  <label>清除日期（空=全部）<input name="date" placeholder="YYYY-MM-DD"/></label>
  <button class="danger" type="submit">清除日志</button>
</form>
<table>
<thead><tr><th>时间</th><th>类型</th><th>结果</th><th>大小</th><th>路径</th></tr></thead>
<tbody>{rows}</tbody>
</table>
</body></html>"#,
        err = err,
        notice = notice,
        k_all = sel(kind, "all"),
        k_up = sel(kind, "upload"),
        k_del = sel(kind, "delete"),
        r_all = sel(result, "all"),
        r_ok = sel(result, "ok"),
        r_err = sel(result, "error"),
        q = html_escape(&q),
        rows = rows,
    )
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

fn format_size(size: u64) -> String {
    crate::util::format_bytes(size)
}

fn format_timestamp(ts_millis: u64) -> String {
    let secs = (ts_millis / 1000) as i64;
    let dt = chrono::DateTime::from_timestamp(secs, 0);
    match dt {
        Some(dt) => {
            use chrono::FixedOffset;
            let china = FixedOffset::east_opt(8 * 3600).unwrap();
            dt.with_timezone(&china).format("%Y-%m-%d %H:%M").to_string()
        }
        None => "-".to_string(),
    }
}

fn render_directory_html(req_path: &str, files: &[crate::drive::QuarkFile]) -> String {
    let display_path = percent_decode(req_path);
    let display_path = if display_path.is_empty() || display_path == "/" {
        "/".to_string()
    } else {
        display_path
    };

    let req_path_normalized = if req_path.ends_with('/') || req_path.is_empty() {
        req_path.to_string()
    } else {
        format!("{}/", req_path)
    };

    // Build breadcrumbs
    let mut breadcrumbs = String::from(r#"<a href="/">根目录</a>"#);
    if display_path != "/" {
        let parts: Vec<&str> = display_path.trim_matches('/').split('/').collect();
        let mut href = String::new();
        for (i, part) in parts.iter().enumerate() {
            href.push('/');
            href.push_str(&percent_encode_path(part));
            if i == parts.len() - 1 {
                breadcrumbs.push_str(&format!(
                    r#" / <span class="current">{}</span>"#,
                    html_escape(part)
                ));
            } else {
                breadcrumbs.push_str(&format!(
                    r#" / <a href="{}">{}</a>"#,
                    html_escape(&format!("{}/", href)),
                    html_escape(part)
                ));
            }
        }
    }

    // Separate dirs and files, sort by name
    let mut dirs: Vec<&crate::drive::QuarkFile> = files.iter().filter(|f| f.dir).collect();
    let mut regular_files: Vec<&crate::drive::QuarkFile> = files.iter().filter(|f| f.file).collect();
    dirs.sort_by(|a, b| a.file_name.to_lowercase().cmp(&b.file_name.to_lowercase()));
    regular_files.sort_by(|a, b| a.file_name.to_lowercase().cmp(&b.file_name.to_lowercase()));

    let mut rows = String::new();

    // Parent directory link
    if display_path != "/" {
        rows.push_str(r#"<tr class="parent"><td class="icon">📁</td><td class="name"><a href="../">..</a></td><td class="size">-</td><td class="date">-</td></tr>"#);
    }

    for dir in &dirs {
        let name = html_escape(&dir.file_name);
        let href = format!("{}{}/", req_path_normalized, percent_encode_path(&dir.file_name));
        let date = format_timestamp(dir.updated_at);
        rows.push_str(&format!(
            r#"<tr class="dir"><td class="icon">📁</td><td class="name"><a href="{}">{}</a></td><td class="size">-</td><td class="date">{}</td></tr>"#,
            html_escape(&href), name, date
        ));
    }

    for file in &regular_files {
        let name = html_escape(&file.file_name);
        let href = format!("{}{}", req_path_normalized, percent_encode_path(&file.file_name));
        let size = format_size(file.size);
        let date = format_timestamp(file.updated_at);
        let icon = file_icon(&file.file_name);
        rows.push_str(&format!(
            r#"<tr class="file"><td class="icon">{}</td><td class="name"><a href="{}">{}</a></td><td class="size">{}</td><td class="date">{}</td></tr>"#,
            icon, html_escape(&href), name, size, date
        ));
    }

    let total = dirs.len() + regular_files.len();

    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>QuarkDrive - {title}</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='12' fill='%232563eb'/%3E%3Ctext x='32' y='40' font-family='Arial,Helvetica,sans-serif' font-size='26' font-weight='bold' fill='white' text-anchor='middle'%3EQW%3C/text%3E%3C/svg%3E">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }}
.container {{ max-width: 960px; margin: 0 auto; padding: 20px; }}
.header {{ background: #fff; border-radius: 8px; padding: 16px 24px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.header h1 {{ font-size: 18px; font-weight: 600; color: #1a1a1a; margin-bottom: 8px; }}
.breadcrumb {{ font-size: 14px; color: #666; }}
.breadcrumb a {{ color: #2563eb; text-decoration: none; }}
.breadcrumb a:hover {{ text-decoration: underline; }}
.breadcrumb .current {{ color: #333; font-weight: 500; }}
.content {{ background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }}
table {{ width: 100%; border-collapse: collapse; }}
thead {{ background: #fafafa; }}
th {{ text-align: left; padding: 12px 16px; font-size: 13px; font-weight: 600; color: #666; border-bottom: 1px solid #eee; }}
td {{ padding: 10px 16px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }}
tr:hover {{ background: #f8fafc; }}
tr.parent:hover {{ background: #f0f7ff; }}
tr.dir:hover {{ background: #f0f7ff; }}
.icon {{ width: 32px; text-align: center; }}
.name {{ word-break: break-all; }}
.name a {{ color: #1a1a1a; text-decoration: none; }}
.name a:hover {{ color: #2563eb; text-decoration: underline; }}
.dir .name a {{ font-weight: 500; }}
.size {{ width: 100px; text-align: right; color: #888; white-space: nowrap; }}
.date {{ width: 160px; color: #888; white-space: nowrap; }}
.footer {{ text-align: center; padding: 16px; font-size: 12px; color: #aaa; }}
.footer a {{ color: #aaa; text-decoration: none; }}
.footer a:hover {{ color: #2563eb; text-decoration: underline; }}
@media (max-width: 640px) {{
  .container {{ padding: 10px; }}
  .date {{ display: none; }}
  th:last-child {{ display: none; }}
  .size {{ width: 80px; }}
}}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1><a href="https://github.com/chenqimiao/quarkdrive-webdav" target="_blank" style="color:inherit;text-decoration:none;">QuarkDrive WebDAV</a></h1>
    <div class="breadcrumb">{breadcrumbs}</div>
    <div style="margin-top:8px;font-size:13px"><a href="/__uploads" style="color:#2563eb;margin-right:12px">上传进度</a><a href="/__history" style="color:#2563eb;margin-right:12px">上传历史</a><a href="/__quark_auth" style="color:#2563eb;margin-right:12px">授权</a><a href="/__logout" style="color:#2563eb">退出</a></div>
  </div>
  <div class="content">
    <table>
      <thead><tr><th class="icon"></th><th>名称</th><th class="size">大小</th><th class="date">修改时间</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <div class="footer">{total} 个项目 · <a href="https://github.com/chenqimiao/quarkdrive-webdav" target="_blank">GitHub</a></div>
</div>
</body>
</html>"#,
        title = html_escape(&display_path),
        breadcrumbs = breadcrumbs,
        rows = rows,
        total = total,
    )
}

fn percent_encode_path(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

fn file_icon(name: &str) -> &'static str {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "ico" | "heic" | "heif"
        | "avif" => "🖼️",
        "mp4" | "avi" | "mkv" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "ts" => "🎬",
        "mp3" | "wav" | "flac" | "aac" | "ogg" | "wma" | "m4a" => "🎵",
        "pdf" => "📕",
        "doc" | "docx" => "📝",
        "xls" | "xlsx" => "📊",
        "ppt" | "pptx" => "📎",
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" => "📦",
        "txt" | "md" | "log" | "csv" => "📄",
        "exe" | "msi" | "dmg" | "app" | "deb" | "rpm" => "⚙️",
        "html" | "css" | "js" | "json" | "xml" | "yaml" | "yml" | "toml" => "💻",
        "rs" | "py" | "java" | "c" | "cpp" | "go" | "rb" | "php" | "sh" => "💻",
        _ => "📄",
    }
}

fn is_previewable_image(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    matches!(
        ext.as_str(),
        "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "ico" | "heic" | "heif" | "avif"
    )
}

fn is_heic_like(name: &str) -> bool {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    matches!(ext.as_str(), "heic" | "heif" | "heics" | "heifs")
}

fn render_dir_error_page(req_path: &str, message: &str) -> String {
    let path = html_escape(&percent_decode(req_path));
    let msg = html_escape(message);
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>目录加载失败</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:640px;margin:40px auto;padding:0 16px;line-height:1.6}}
.bad{{color:#c33}} a{{color:#2563eb}}
</style></head><body>
<h1>目录加载失败</h1>
<p class="bad">{msg}</p>
<p>路径：<code>{path}</code></p>
<p><a href="javascript:location.reload()">刷新</a> · <a href="../">返回上级</a> · <a href="/">根目录</a></p>
</body></html>"#
    )
}

/// 浏览器图片预览；HEIC 优先同源代理缩略图（`?thumb=1`），失败再试 heic2any。
fn render_image_preview_page(req_path: &str, file_name: &str, has_thumbnail: bool) -> String {
    let title = html_escape(file_name);
    let qsep = if req_path.contains('?') { "&" } else { "?" };
    let raw_url = html_escape(&format!("{req_path}{qsep}raw=1"));
    // 同源代理，不把夸克 CDN 地址塞进页面（直链会被防盗链拦成「图片加载失败」）。
    let thumb_js = if has_thumbnail {
        format!(
            "const THUMB_URL = {};",
            serde_json::to_string(&format!("{req_path}{qsep}thumb=1"))
                .unwrap_or_else(|_| "null".into())
        )
    } else {
        "const THUMB_URL = null;".to_string()
    };
    let parent = {
        let p = req_path.trim_end_matches('/');
        match p.rfind('/') {
            Some(0) | None => "/".to_string(),
            Some(i) => format!("{}/", &p[..i]),
        }
    };
    let parent = html_escape(&parent);
    let heic = is_heic_like(file_name);
    let body_script = if heic {
        r#"
<script>
function errText(e) {
  if (e == null) return 'unknown';
  if (typeof e === 'string') return e;
  if (e instanceof Error) return e.message || String(e);
  if (typeof e === 'object') {
    var m = e.message || e.msg || e.error || e.reason;
    if (typeof m === 'string' && m) {
      return (e.code != null ? (String(e.code) + ': ') : '') + m;
    }
    if (e.code != null) return String(e.code);
    try {
      var s = JSON.stringify(e);
      if (s && s !== '{}' && s !== 'null') return s;
    } catch (_) {}
  }
  try { return String(e); } catch (_) { return 'unknown error'; }
}
function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = src;
    s.async = true;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error('脚本加载失败: ' + src));
    document.head.appendChild(s);
  });
}
function showImg(url, note) {
  return new Promise((resolve, reject) => {
    const img = document.getElementById('preview');
    const status = document.getElementById('status');
    img.onload = () => { status.textContent = note || ''; resolve(); };
    img.onerror = () => { reject(new Error('图片加载失败')); };
    img.src = url;
    img.hidden = false;
  });
}
(async () => {
  const status = document.getElementById('status');
  let thumbOk = false;
  // 1) 同源缩略图代理（服务端带 Referer 拉夸克 CDN）
  if (THUMB_URL) {
    status.textContent = '正在加载预览…';
    try {
      await showImg(THUMB_URL, '当前为云端缩略图预览；可下载原文件查看全图');
      thumbOk = true;
    } catch (_) {
      status.textContent = '缩略图不可用，尝试原图转换…';
    }
  } else {
    status.textContent = '正在加载转换库…';
  }
  try {
    if (typeof heic2any !== 'function') {
      const cdns = [
        'https://cdn.jsdelivr.net/npm/heic2any@0.0.4/dist/heic2any.min.js',
        'https://unpkg.com/heic2any@0.0.4/dist/heic2any.min.js',
        'https://fastly.jsdelivr.net/npm/heic2any@0.0.4/dist/heic2any.min.js'
      ];
      let loaded = false;
      for (const src of cdns) {
        try { await loadScript(src); if (typeof heic2any === 'function') { loaded = true; break; } }
        catch (_) {}
      }
      if (!loaded) throw new Error('无法加载 heic2any（CDN 不可达）');
    }
    const res = await fetch(RAW_URL, {
      credentials: 'include',
      headers: { 'Accept': '*/*' },
      cache: 'no-store'
    });
    if (!res.ok) throw new Error('下载失败 HTTP ' + res.status);
    const ctype = (res.headers.get('content-type') || '').toLowerCase();
    if (ctype.includes('text/html')) throw new Error('拿到的是网页而不是图片');
    const buf = await res.arrayBuffer();
    if (!buf || buf.byteLength < 32) throw new Error('文件过小 (' + (buf && buf.byteLength) + ' bytes)');
    let lastErr = null;
    for (const t of ['image/heic', 'image/heif', 'application/octet-stream']) {
      try {
        const out = await heic2any({
          blob: new Blob([buf], { type: t }),
          toType: 'image/jpeg',
          quality: 0.92,
          multiple: false
        });
        const jpeg = Array.isArray(out) ? out[0] : out;
        if (!jpeg) throw new Error('转换结果为空');
        await showImg(URL.createObjectURL(jpeg), '');
        return;
      } catch (e) { lastErr = e; }
    }
    throw lastErr || new Error('HEIC 转换失败');
  } catch (e) {
    if (thumbOk) {
      status.textContent = '原图转换失败（' + errText(e) + '），已显示缩略图；可下载原文件或用 Safari 查看全图';
      return;
    }
    status.innerHTML = 'HEIC 预览失败：' + errText(e)
      + '。<br/>可 <a href="' + RAW_URL + '" download style="color:#8ab4ff">下载原文件</a>，或使用 Safari。';
  }
})();
</script>"#
    } else {
        r#"
<script>
document.getElementById('preview').src = RAW_URL;
document.getElementById('preview').hidden = false;
document.getElementById('status').textContent = '';
</script>"#
    };
    format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
body{{margin:0;background:#111;color:#eee;font-family:system-ui,sans-serif}}
.bar{{display:flex;gap:12px;align-items:center;padding:10px 16px;background:#1a1a1a;flex-wrap:wrap}}
.bar a{{color:#8ab4ff;text-decoration:none}}
.wrap{{display:flex;justify-content:center;align-items:center;min-height:calc(100vh - 48px);padding:16px;flex-direction:column}}
img{{max-width:100%;max-height:calc(100vh - 80px);object-fit:contain}}
#status{{padding:16px;color:#ccc;text-align:center;line-height:1.6}}
</style></head><body>
<div class="bar">
  <a href="{parent}">← 返回</a>
  <strong>{title}</strong>
  <a href="{raw_url}" download>下载原文件</a>
</div>
<div class="wrap">
  <p id="status">加载中…</p>
  <img id="preview" alt="{title}" hidden/>
</div>
<script>const RAW_URL = "{raw_url}"; {thumb_js}</script>
{body_script}
</body></html>"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- format_size tests ---

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_kb() {
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
    }

    #[test]
    fn test_format_size_mb() {
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 5), "5.0 MB");
    }

    #[test]
    fn test_format_size_gb() {
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_format_size_tb() {
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024), "1.0 TB");
        assert_eq!(format_size(1024u64 * 1024 * 1024 * 1024 * 3), "3.0 TB");
    }

    // --- format_timestamp tests ---

    #[test]
    fn test_format_timestamp_normal() {
        // 2024-01-01 08:00 CST = 2024-01-01 00:00 UTC = 1704067200000 ms
        let result = format_timestamp(1704067200000);
        assert_eq!(result, "2024-01-01 08:00");
    }

    #[test]
    fn test_format_timestamp_zero() {
        // 0 ms => 1970-01-01 08:00 CST
        let result = format_timestamp(0);
        assert_eq!(result, "1970-01-01 08:00");
    }

    // --- html_escape tests ---

    #[test]
    fn test_html_escape_all_entities() {
        assert_eq!(html_escape("&<>\"'"), "&amp;&lt;&gt;&quot;&#x27;");
    }

    #[test]
    fn test_html_escape_empty() {
        assert_eq!(html_escape(""), "");
    }

    #[test]
    fn test_web_session_token_stable_and_sensitive() {
        let a = web_session_token("admin", "secret");
        let b = web_session_token("admin", "secret");
        assert_eq!(a, b);
        assert_ne!(a, web_session_token("admin", "other"));
        assert_ne!(a, web_session_token("other", "secret"));
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn test_sanitize_next_blocks_open_redirect() {
        assert_eq!(sanitize_next(Some("/__uploads")), "/__uploads");
        assert_eq!(sanitize_next(Some("/a?b=1")), "/a?b=1");
        assert_eq!(sanitize_next(Some("//evil.com")), "/");
        assert_eq!(sanitize_next(Some("https://evil.com")), "/");
        assert_eq!(sanitize_next(None), "/");
    }

    #[test]
    fn test_login_redirect_location_encodes_next() {
        let loc = login_redirect_location("/__uploads", Some("x=1"));
        assert!(loc.starts_with("/__login?next="));
        // next 整体做 percent-encode，避免 query 二次解析打断路径。
        assert!(!loc.contains("/__login?next=/__uploads"));
        assert!(loc.contains("uploads"));
        assert!(loc.contains("x"));
    }

    #[test]
    fn test_html_escape_no_special() {
        assert_eq!(html_escape("hello world"), "hello world");
    }

    #[test]
    fn test_html_escape_mixed() {
        assert_eq!(
            html_escape("a & b < c > d"),
            "a &amp; b &lt; c &gt; d"
        );
    }

    // --- file_icon tests ---

    #[test]
    fn test_file_icon_image() {
        assert_eq!(file_icon("photo.jpg"), "🖼️");
        assert_eq!(file_icon("photo.PNG"), "🖼️");
        assert_eq!(file_icon("photo.Jpeg"), "🖼️");
        assert_eq!(file_icon("IMG_001.HEIC"), "🖼️");
        assert!(is_previewable_image("a.heic"));
        assert!(is_heic_like("a.heif"));
        assert!(!is_heic_like("a.jpg"));
    }

    #[test]
    fn test_file_icon_video() {
        assert_eq!(file_icon("movie.mp4"), "🎬");
        assert_eq!(file_icon("movie.MKV"), "🎬");
    }

    #[test]
    fn test_file_icon_audio() {
        assert_eq!(file_icon("song.mp3"), "🎵");
        assert_eq!(file_icon("song.FLAC"), "🎵");
    }

    #[test]
    fn test_file_icon_document() {
        assert_eq!(file_icon("report.pdf"), "📕");
        assert_eq!(file_icon("report.doc"), "📝");
        assert_eq!(file_icon("data.xlsx"), "📊");
        assert_eq!(file_icon("slides.pptx"), "📎");
    }

    #[test]
    fn test_file_icon_archive() {
        assert_eq!(file_icon("archive.zip"), "📦");
        assert_eq!(file_icon("archive.tar"), "📦");
    }

    #[test]
    fn test_file_icon_code() {
        assert_eq!(file_icon("main.rs"), "💻");
        assert_eq!(file_icon("app.js"), "💻");
        assert_eq!(file_icon("config.yaml"), "💻");
    }

    #[test]
    fn test_file_icon_unknown() {
        assert_eq!(file_icon("file.xyz"), "📄");
        assert_eq!(file_icon("noext"), "📄");
    }

    // --- percent_encode_path / percent_decode tests ---

    #[test]
    fn test_percent_encode_path_chinese() {
        let encoded = percent_encode_path("你好");
        assert!(!encoded.contains("你"));
        let decoded = percent_decode(&encoded);
        assert_eq!(decoded, "你好");
    }

    #[test]
    fn test_percent_encode_path_special_chars() {
        let encoded = percent_encode_path("file name (1).txt");
        assert!(!encoded.contains(' '));
        let decoded = percent_decode(&encoded);
        assert_eq!(decoded, "file name (1).txt");
    }

    #[test]
    fn test_percent_decode_empty() {
        assert_eq!(percent_decode(""), "");
    }

    #[test]
    fn test_percent_encode_roundtrip() {
        let original = "测试文件 & 文档.pdf";
        let encoded = percent_encode_path(original);
        let decoded = percent_decode(&encoded);
        assert_eq!(decoded, original);
    }

    // --- render_directory_html tests ---

    #[test]
    fn test_render_directory_html_root_empty() {
        let files: Vec<crate::drive::QuarkFile> = vec![];
        let html = render_directory_html("/", &files);
        assert!(html.contains("QuarkDrive"));
        assert!(html.contains("0 个项目"));
        // root should not have parent link
        assert!(!html.contains("href=\"../\""));
    }

    #[test]
    fn test_render_directory_html_with_files() {
        let files = vec![
            crate::drive::QuarkFile {
                fid: "1".to_string(),
                file_name: "子目录".to_string(),
                pdir_fid: "0".to_string(),
                size: 0,
                format_type: "".to_string(),
                status: 1,
                created_at: 1704067200000,
                updated_at: 1704067200000,
                dir: true,
                file: false,
                download_url: None,
                content_hash: None,
                parent_path: None,
                thumbnail_url: None,
            },
            crate::drive::QuarkFile {
                fid: "2".to_string(),
                file_name: "test.txt".to_string(),
                pdir_fid: "0".to_string(),
                size: 1024,
                format_type: "text/plain".to_string(),
                status: 1,
                created_at: 1704067200000,
                updated_at: 1704067200000,
                dir: false,
                file: true,
                download_url: None,
                content_hash: None,
                parent_path: None,
                thumbnail_url: None,
            },
        ];
        let html = render_directory_html("/docs", &files);
        assert!(html.contains("2 个项目"));
        // subdirectory should have parent link
        assert!(html.contains("href=\"../\""));
        // should contain the directory and file names
        assert!(html.contains("子目录"));
        assert!(html.contains("test.txt"));
        // file size should be formatted
        assert!(html.contains("1.0 KB"));
    }

    #[test]
    fn test_render_directory_html_sorting() {
        let files = vec![
            crate::drive::QuarkFile {
                fid: "1".to_string(),
                file_name: "b.txt".to_string(),
                pdir_fid: "0".to_string(),
                size: 100,
                format_type: "text/plain".to_string(),
                status: 1,
                created_at: 0,
                updated_at: 0,
                dir: false,
                file: true,
                download_url: None,
                content_hash: None,
                parent_path: None,
                thumbnail_url: None,
            },
            crate::drive::QuarkFile {
                fid: "2".to_string(),
                file_name: "a.txt".to_string(),
                pdir_fid: "0".to_string(),
                size: 200,
                format_type: "text/plain".to_string(),
                status: 1,
                created_at: 0,
                updated_at: 0,
                dir: false,
                file: true,
                download_url: None,
                content_hash: None,
                parent_path: None,
                thumbnail_url: None,
            },
        ];
        let html = render_directory_html("/", &files);
        let pos_a = html.find("a.txt").unwrap();
        let pos_b = html.find("b.txt").unwrap();
        // a.txt should come before b.txt (sorted alphabetically)
        assert!(pos_a < pos_b);
    }

    #[test]
    fn test_heic_preview_page_errtext_and_thumb() {
        let html = render_image_preview_page("/photos/a.HEIC", "a.HEIC", true);
        assert!(html.contains("function errText"));
        assert!(html.contains("THUMB_URL"));
        // 同源代理，不内嵌夸克 CDN。
        assert!(html.contains("thumb=1"));
        assert!(!html.contains("https://img.example"));
        assert!(html.contains("heic2any"));
        assert!(html.contains("raw=1"));
        assert!(html.contains("thumbOk"));
        let no_thumb = render_image_preview_page("/x.heic", "x.heic", false);
        assert!(no_thumb.contains("THUMB_URL = null"));
    }

    // --- compute_fs_path tests ---

    fn create_test_webdav(root: &str, strip_prefix: Option<&str>) -> QuarkDriveWebDav {
        use std::sync::Arc;
        use dashmap::DashMap;
        let cookie = Arc::new(DashMap::new());
        cookie.insert("test".to_string(), "value".to_string());
        let config = crate::drive::DriveConfig {
            api_base_url: "https://drive.quark.cn".to_string(),
            cookie,
            open: None,
            alert: crate::alert::Alert::new(None),
        };
        let drive = crate::drive::QuarkDrive::new(config).unwrap();
        let fs = crate::vfs::QuarkDriveFileSystem::new(
            drive,
            root.to_string(),
            100,
            64 * 1024 * 1024,
            60,
        )
        .unwrap();
        let handler = DavHandler::builder()
            .filesystem(Box::new(fs.clone()))
            .build_handler();
        QuarkDriveWebDav {
            auth_user: None,
            auth_password: None,
            handler,
            fs,
            strip_prefix: strip_prefix.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_compute_fs_path_root() {
        let webdav = create_test_webdav("/", None);
        assert_eq!(webdav.compute_fs_path("/"), std::path::PathBuf::from("/"));
    }

    #[test]
    fn test_compute_fs_path_simple_file() {
        let webdav = create_test_webdav("/", None);
        assert_eq!(webdav.compute_fs_path("/test.txt"), std::path::PathBuf::from("/test.txt"));
    }

    #[test]
    fn test_compute_fs_path_nested() {
        let webdav = create_test_webdav("/", None);
        assert_eq!(
            webdav.compute_fs_path("/docs/report.pdf"),
            std::path::PathBuf::from("/docs/report.pdf")
        );
    }

    #[test]
    fn test_compute_fs_path_with_strip_prefix() {
        let webdav = create_test_webdav("/", Some("/dav"));
        // "/dav/test.txt" after stripping "/dav" → "/test.txt"
        assert_eq!(
            webdav.compute_fs_path("/dav/test.txt"),
            std::path::PathBuf::from("/test.txt")
        );
    }

    #[test]
    fn test_compute_fs_path_strip_prefix_root() {
        let webdav = create_test_webdav("/", Some("/dav"));
        assert_eq!(webdav.compute_fs_path("/dav/"), std::path::PathBuf::from("/"));
        assert_eq!(webdav.compute_fs_path("/dav"), std::path::PathBuf::from("/"));
    }

    #[test]
    fn test_compute_fs_path_with_custom_root() {
        let webdav = create_test_webdav("/myroot", None);
        assert_eq!(webdav.compute_fs_path("/"), std::path::PathBuf::from("/myroot"));
        assert_eq!(
            webdav.compute_fs_path("/test.txt"),
            std::path::PathBuf::from("/myroot/test.txt")
        );
    }

    #[test]
    fn test_compute_fs_path_trailing_slash() {
        let webdav = create_test_webdav("/", None);
        assert_eq!(
            webdav.compute_fs_path("/docs/"),
            std::path::PathBuf::from("/docs")
        );
    }

    #[test]
    fn test_compute_fs_path_percent_encoded() {
        let webdav = create_test_webdav("/", None);
        // "%E6%B5%8B%E8%AF%95" is percent-encoded "测试"
        let path = webdav.compute_fs_path("/%E6%B5%8B%E8%AF%95.txt");
        assert_eq!(path, std::path::PathBuf::from("/测试.txt"));
    }

    #[test]
    fn test_compute_fs_path_custom_root_and_prefix() {
        let webdav = create_test_webdav("/myroot", Some("/dav"));
        assert_eq!(
            webdav.compute_fs_path("/dav/file.txt"),
            std::path::PathBuf::from("/myroot/file.txt")
        );
    }

    // --- Digest header tests ---

    #[test]
    fn test_digest_hex_md5_to_base64() {
        use base64::Engine;
        // API normally returns hex MD5; we convert to base64 per RFC 3230
        let hex_md5 = "d41d8cd98f00b204e9800998ecf8427e"; // MD5 of empty string
        let md5_bytes = hex::decode(hex_md5).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&md5_bytes);
        assert_eq!(b64, "1B2M2Y8AsgTpgAmY7PhCfg==");
        let header_value = format!("md5={}", b64);
        assert!(hyper::header::HeaderValue::from_str(&header_value).is_ok());
    }

    #[test]
    fn test_digest_hex_md5_hello_world() {
        use base64::Engine;
        let hex_md5 = "5eb63bbbe01eeed093cb22bb8f5acdc3"; // MD5 of "hello world"
        let md5_bytes = hex::decode(hex_md5).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&md5_bytes);
        // Verify round-trip
        let decoded = base64::engine::general_purpose::STANDARD.decode(&b64).unwrap();
        assert_eq!(hex::encode(&decoded), hex_md5);
    }

    #[test]
    fn test_digest_fallback_already_base64() {
        // If hex::decode fails, the value is used as-is (base64 fallback)
        let already_b64 = "XrY7u+Ae7tCTyyK7j1rNww==";
        assert!(hex::decode(already_b64).is_err()); // not valid hex
        let header_value = format!("md5={}", already_b64);
        assert!(hyper::header::HeaderValue::from_str(&header_value).is_ok());
    }

    #[test]
    fn test_digest_header_format() {
        use base64::Engine;
        let hex_md5 = "d3bdad63084cd4dc4e7120ec48a11082"; // real file MD5
        let md5_bytes = hex::decode(hex_md5).unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&md5_bytes);
        let header_value = format!("md5={}", b64);
        assert!(hyper::header::HeaderValue::from_str(&header_value).is_ok());
        assert!(header_value.starts_with("md5="));
    }

    // --- Want-Digest header parsing ---

    #[test]
    fn test_want_digest_contains_md5() {
        let val = "md5";
        assert!(val.to_lowercase().contains("md5"));
    }

    #[test]
    fn test_want_digest_contains_md5_mixed_case() {
        let val = "MD5;q=1, SHA-256;q=0.5";
        assert!(val.to_lowercase().contains("md5"));
    }

    #[test]
    fn test_want_digest_no_md5() {
        let val = "sha-256, sha-512";
        assert!(!val.to_lowercase().contains("md5"));
    }

    #[test]
    fn test_want_digest_logic() {
        // Simulate the Want-Digest decision logic from webdav.rs
        fn should_add_digest(want_digest: &Option<String>) -> bool {
            match want_digest {
                Some(val) => val.to_lowercase().contains("md5"),
                None => true, // No header = add by default
            }
        }

        assert!(should_add_digest(&None));
        assert!(should_add_digest(&Some("md5".to_string())));
        assert!(should_add_digest(&Some("MD5;q=1, SHA-256".to_string())));
        assert!(!should_add_digest(&Some("sha-256".to_string())));
        assert!(!should_add_digest(&Some("sha-512".to_string())));
    }
}

impl Service<Request<hyper::body::Incoming>> for QuarkDriveWebDav {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        let should_auth = self.auth_user.is_some() && self.auth_password.is_some();
        let dav_server = self.handler.clone();
        let auth_user = self.auth_user.clone();
        let auth_pwd = self.auth_password.clone();
        let is_browser = Self::is_browser_request(&req);
        let wants_login_page = Self::wants_web_login_page(&req);
        let req_path = req.uri().path().to_string();
        let req_query = req.uri().query().map(|s| s.to_string());
        let req_method = req.method().clone();
        let want_digest = req
            .headers()
            .get("want-digest")
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()));
        let browser_handler = self.clone();

        Box::pin(async move {
            // OAuth 回调与登录/登出页可跳过鉴权；扫码/控制台须先登录。
            if Self::is_quark_auth_callback(&req_path) {
                return Ok(browser_handler.handle_quark_auth(&req).await);
            }
            if Self::is_web_login_path(&req_path) {
                return Ok(browser_handler.handle_web_login(req).await);
            }

            let mut resp = if should_auth {
                let auth_user_val = auth_user.clone().unwrap();
                let auth_pwd_val = auth_pwd.clone().unwrap();

                if !browser_handler.is_authenticated(&req, &auth_user_val, &auth_pwd_val) {
                    if wants_login_page {
                        return Ok(Response::builder()
                            .status(302)
                            .header(
                                "Location",
                                login_redirect_location(&req_path, req_query.as_deref()),
                            )
                            .body(Body::from("redirect to login"))
                            .unwrap());
                    }
                    return Ok(Response::builder()
                        .status(401)
                        .header("WWW-Authenticate", "Basic realm=\"quarkdrive-webdav\"")
                        .body(Body::from("Authentication required"))
                        .unwrap());
                }

                if is_browser {
                    if let Some(resp) = browser_handler.handle_browser_request(&req).await {
                        return Ok(resp);
                    }
                }

                let config = DavConfig::new().principal(auth_user_val);
                dav_server.handle_with(config, req).await
            } else {
                if is_browser {
                    if let Some(resp) = browser_handler.handle_browser_request(&req).await {
                        return Ok(resp);
                    }
                }

                dav_server.handle(req).await
            };

            // RFC 3230: Add Digest header for GET 200 responses
            if req_method == Method::GET && resp.status() == hyper::StatusCode::OK {
                let should_add = match &want_digest {
                    Some(val) => val.to_lowercase().contains("md5"),
                    None => true,
                };
                if should_add {
                    let fs_path = browser_handler.compute_fs_path(&req_path);
                    if let Some(md5_val) =
                        browser_handler.fs.get_file_md5_for_path(&fs_path).await
                    {
                        // API normally returns hex MD5; convert to base64 per RFC 3230
                        let b64 = if let Ok(md5_bytes) = hex::decode(&md5_val) {
                            use base64::Engine;
                            base64::engine::general_purpose::STANDARD.encode(&md5_bytes)
                        } else {
                            // Already base64 or other format, use as-is
                            md5_val
                        };
                        if let Ok(val) =
                            hyper::header::HeaderValue::from_str(&format!("md5={}", b64))
                        {
                            resp.headers_mut().insert("digest", val);
                        }
                    }
                }
            }

            Ok(resp)
        })
    }
}

#[derive(Clone)]
pub struct MakeSvc {
    pub auth_user: Option<String>,
    pub auth_password: Option<String>,
    pub handler: DavHandler,
    pub fs: QuarkDriveFileSystem,
    pub strip_prefix: Option<String>,
}

impl Service<()> for MakeSvc {
    type Response = QuarkDriveWebDav;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, _: ()) -> Self::Future {
        let auth_user = self.auth_user.clone();
        let auth_password = self.auth_password.clone();
        let handler = self.handler.clone();
        let fs = self.fs.clone();
        let strip_prefix = self.strip_prefix.clone();

        Box::pin(async move {
            Ok(QuarkDriveWebDav {
                auth_user,
                auth_password,
                handler,
                fs,
                strip_prefix,
            })
        })
    }
}
