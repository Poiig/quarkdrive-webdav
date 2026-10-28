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

启动后浏览器打开 `http://主机:8080`（若配置了 `WEBDAV_AUTH_*`，会进入 **`/__login` 登录页**，不再弹浏览器 Basic 对话框）：

1. 在登录页输入 WebDAV 账号密码后：若夸克已授权 → 浏览网盘；未授权/过期 → 再进入扫码页  
2. 目录页**右上角**：上传进度面板（默认展开；文件名/大小/进度条/实际上传速度，约 0.5s 刷新）、「历史」弹窗；另有授权 / 退出  
3. **智能上传**：大于 `UPLOAD_LARGE_THRESHOLD` 的文件 FIFO 排队——队首全速，其余按 `UPLOAD_SLOW_BPS` 慢传，新任务只能排到队尾  
4. WebDAV 客户端仍走协议 Basic；OAuth 回调 `/__quark_auth/callback` 仍跳过鉴权

Token 会写入 `QUARK_TOKEN_FILE`，重启可复用。挂载的 `/data` 目录必须对容器可写，否则扫码成功也无法落盘，重启后需重新登录。

## 常用环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `QUARK_COOKIE` | Cookie 模式凭证 | — |
| `QUARK_OPEN` | `true` 启用 Open 模式 | `false` |
| `QUARK_TOKEN_FILE` | Open token 落盘路径 | `./quark-token.json` |
| `QUARK_REFRESH_TOKEN` | 可选，已有 refresh token 时可直接填 | 空 |
| `OPEN_API_MAX_CONCURRENT` | Open API 同时在途请求数。调大容易 429 | `2` |
| `OPEN_API_MIN_INTERVAL_MS` | 两次 Open API 请求的最小间隔（毫秒） | `200` |
| `UPLOAD_MEMORY_LIMIT` | **仅**限制「接收上传内容」的内存池（并行合计，超限落盘）；**不是**容器/进程 RSS 上限；`0`=始终落盘 | `64MB` |
| `UPLOAD_MAX_CONCURRENT` | 同时后台上传数上限 | `4` |
| `UPLOAD_LARGE_THRESHOLD` | 智能调度大文件阈值（FIFO 快车道） | `32MB` |
| `UPLOAD_SLOW_BPS` | 非快车道大文件慢传速率 | `512KB` |
| `UPLOAD_WAIT_TIMEOUT` | 已忽略。WebDAV 一律等夸克上传成功后才返回成功，避免群晖把未传完的文件记成已同步 | 忽略 |
| `RELOCATE_DEFER_MIN_SIZE` | CloudSync **先删后传**时，不小于此大小的删除才延迟进回收站；`0`=全部先挂起，等 PUT **比 hash 一致**再跳过或 relocate（不一致则照常重传，保证内容一致） | `0` |

**Cloud Sync 加密**：群晖会在 WebDAV **当前路径的同级**创建 `@SynologyCloudSync` 并写入 `cloudsync_encrypt.info`。服务器地址请指向**可写的具体目录**（例如 `http://host:8080/备份目录`），不要填只读聚合根；Open 模式容量接口为空时已按「不限容量」回报，避免误判不可写。

**Open API 限流（errno=429）**：服务端已对 Open 请求做并发/间隔节流（默认 2 路、间隔 200ms，可用 `OPEN_API_MAX_CONCURRENT` / `OPEN_API_MIN_INTERVAL_MS` 调整），并对 429 **退避重试**；建目录不再疯狂轮询列表。若仍频繁限流，可把 Cloud Sync 任务错开、降低并发同步任务数，或暂时减小同步范围。access token 过期或签名失败（errno=10001）时，用 OpenList 在线接口刷新，不走飞牛刷新。
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
