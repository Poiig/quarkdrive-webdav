# quarkdrive-webdav

夸克网盘 WebDAV 服务。上游项目：[chenqimiao/quarkdrive-webdav](https://github.com/chenqimiao/quarkdrive-webdav)。

支持两种模式：

| 模式 | 说明 |
|------|------|
| **Cookie** | 使用网页 Cookie |
| **Open** | 浏览器打开服务页扫码授权（推荐） |

镜像：`ghcr.io/poiig/quarkdrive-webdav:latest`

## Docker Compose（推荐）

仓库自带 [`docker-compose.yml`](docker-compose.yml)，默认 Open 模式：

```bash
docker compose up -d
```

Cookie 模式改 compose 里的环境变量即可（文件内有注释示例）。

## Docker Run

### Cookie 模式

```bash
docker run -d --name=quarkdrive-webdav --restart=unless-stopped -p 8080:8080 \
  -e TZ=Asia/Shanghai \
  -e QUARK_COOKIE='你的cookie' \
  -e WEBDAV_AUTH_USER=admin \
  -e WEBDAV_AUTH_PASSWORD=admin \
  -e UPLOAD_MEMORY_LIMIT=64MB \
  -e WECOM_WEBHOOK_KEY='企微机器人key' \
  -e OP_LOG_RETENTION_DAYS=7 \
  -v $(pwd)/logs:/app/logs \
  ghcr.io/poiig/quarkdrive-webdav:latest
```

### Open 模式

```bash
docker run -d --name=quarkdrive-webdav --restart=unless-stopped -p 8080:8080 \
  -e TZ=Asia/Shanghai \
  -e QUARK_OPEN=true \
  -e QUARK_TOKEN_FILE=/data/quark-token.json \
  -e WEBDAV_AUTH_USER=admin \
  -e WEBDAV_AUTH_PASSWORD=admin \
  -e UPLOAD_MEMORY_LIMIT=512MB \
  -e WECOM_WEBHOOK_KEY='企微机器人key' \
  -e OP_LOG_RETENTION_DAYS=7 \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/app/logs \
  ghcr.io/poiig/quarkdrive-webdav:latest
```

启动后浏览器打开 `http://主机:8080`：

1. 若已授权 → 直接浏览网盘  
2. 若未授权/过期 → 页面提示扫码，点「扫码授权」完成登录  

Token 会写入 `QUARK_TOKEN_FILE`，重启可复用。挂载的 `/data` 目录必须对容器可写，否则扫码成功也无法落盘，重启后需重新登录。

## 常用环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `QUARK_COOKIE` | Cookie 模式凭证 | — |
| `QUARK_OPEN` | `true` 启用 Open 模式 | `false` |
| `QUARK_TOKEN_FILE` | Open token 落盘路径 | `./quark-token.json` |
| `QUARK_REFRESH_TOKEN` | 可选，已有 refresh token 时可直接填 | 空 |
| `UPLOAD_MEMORY_LIMIT` | **仅**限制「接收上传内容」的内存池（并行合计，超限落盘）；**不是**容器/进程 RSS 上限；`0`=始终落盘 | `64MB` |
| `UPLOAD_MAX_CONCURRENT` | 同时后台上传数上限 | `4` |
| `DIR_CACHE_SIZE` | 目录缓存最多目录数 | `1000` |
| `DIR_CACHE_BYTES` | 目录缓存字节上限，如 `64MB` | `64MB` |
| `CHUNK_CACHE_MB` | 播放预取块缓存（MB），`0` 关闭 | `128` |
| `PREFETCH_CHUNK_MB` | 预取分块大小（MB） | `16` |
| `PREFETCH_AHEAD` | 预取并发窗口（1–8） | `4` |
| `WECOM_WEBHOOK_KEY` | 企业微信机器人 key；过期/报错时推送 | — |
| `OP_LOG_DIR` | 上传/删除操作日志目录 | `./logs` |
| `OP_LOG_RETENTION_DAYS` | 操作日志保留天数 | `7` |
| `WEBDAV_AUTH_USER` / `WEBDAV_AUTH_PASSWORD` | WebDAV 账号密码 | — |
| `PORT` | 端口 | `8080` |

低配机器可参考：`UPLOAD_MEMORY_LIMIT=64MB UPLOAD_MAX_CONCURRENT=2 DIR_CACHE_BYTES=32MB CHUNK_CACHE_MB=64 PREFETCH_CHUNK_MB=8 PREFETCH_AHEAD=2`。

### 内存说明（为何 RSS 会高于 `UPLOAD_MEMORY_LIMIT`）

进程占用 ≈ 上传接收池 + 目录缓存 + 播放预取缓存 + 流式上传缓冲 + Rust/HTTP 基线，大约：

- `UPLOAD_MEMORY_LIMIT`（默认 64MB；只限「接收上传内容」，不是容器总内存）
- `DIR_CACHE_BYTES`（默认 64MB）
- `CHUNK_CACHE_MB`（默认 **128MB**）
- 预取在途：约 `PREFETCH_AHEAD × PREFETCH_CHUNK_MB`（默认 4×16 ≈ **64MB**/活跃下载）
- **大文件上传**：分片 PUT 已改为**文件流式**（约 64KB 读缓冲），不再「并发 × 单片大小」；大于 8MB 会先落盘再传，低配也能传数 GB/数十 GB

因此不必为了传大文件把 `UPLOAD_MEMORY_LIMIT` 调到 512MB/1GB——过大更容易在收文件阶段把容器打满。  
启动时若配置值超过「可用内存的约 1/4」，会自动收紧并打 warn。

**关于「重启了但没有 OOM 异常」**：Linux cgroup **OOMKill 发送 SIGKILL**，进程来不及打 Rust error/panic；Docker `restart: unless-stopped` 会静默拉起，日志里只看到重新 `listening`。可到宿主机查：

```bash
docker inspect quarkdrive-webdav --format '{{.State.OOMKilled}} {{.RestartCount}}'
dmesg -T | grep -i -E 'oom|killed process' | tail
```

## 免责声明

仅供学习研究，请遵守当地法规与夸克服务条款。使用风险自负。
