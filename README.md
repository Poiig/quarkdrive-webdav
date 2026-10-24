# quarkdrive-webdav

夸克网盘 WebDAV 服务（本仓库 fork）。支持两种接入方式：

- **Cookie 模式**：沿用网页 Cookie（`QUARK_COOKIE`）
- **Open 模式**：对齐 [OpenList `quark_open`](https://github.com/OpenListTeam/OpenList/tree/main/drivers/quark_open)，用 AppId / SignKey + Refresh Token 调 `open-api-drive.quark.cn`

另含滑动窗口预取（合并上游 [PR #53](https://github.com/chenqimiao/quarkdrive-webdav/pull/53)）：多连接并发拉分块，改善 Infuse / nPlayer 播大文件卡顿。

## 功能

- Docker / 二进制 / Cargo 安装
- WebDAV 读写（列表、下载、上传、重命名、移动、删除、建目录）
- 目录缓存、分块预取缓存
- Open 模式自动刷新 access_token（在线 API + 飞牛本地回退）

## 安装

### 从源码编译

```bash
cargo build --release
# 产物：target/release/quarkdrive-webdav
```

### Cargo 安装

```bash
cargo install --git https://github.com/Poiig/quarkdrive-webdav --locked
```

## Cookie 模式（原方式）

```bash
quarkdrive-webdav \
  --quark-cookie '你的cookie' \
  -U '用户名' -W '密码' -p 8080
```

Docker（`main` / tag 推送后由 CI 自动构建到 GHCR）：

```bash
docker run -d --name=quarkdrive-webdav --restart=unless-stopped -p 8080:8080 \
  -e QUARK_COOKIE='your quark cookie' \
  -e WEBDAV_AUTH_USER=admin \
  -e WEBDAV_AUTH_PASSWORD=admin \
  ghcr.io/poiig/quarkdrive-webdav:latest
```

## Open 模式（推荐，对齐 OpenList）

需要三样东西：

| 字段 | 说明 |
|------|------|
| `QUARK_APP_ID` | 应用 AppId（`x-pan-client-id`） |
| `QUARK_SIGN_KEY` | 签名私钥 SignKey |
| `QUARK_REFRESH_TOKEN` | 刷新令牌（授权后拿到） |

AppId / SignKey 是应用凭证；**Refresh Token 是账号授权结果**，必须扫码/登录一次才能拿到。

### 还要不要去 api.oplist.org？

**不必须。** OpenList 去 [api.oplist.org](https://api.oplist.org/) 选「夸克 Open」扫码，只是拿 Refresh Token 的一种途径。本仓库提供等价的本地流程（背后同样走飞牛 OAuth）：

```bash
# 1. 打印授权链接，浏览器打开并登录夸克
quarkdrive-webdav open login

# 2. 从回调 URL 取出 nonce，换 token（可写入文件）
quarkdrive-webdav open exchange --nonce '<nonce>' --token-file ./quark-token.json
```

也可以继续用 OpenList / oplist 页面授权，把拿到的 Refresh Token 填进来即可。

运行时 token 刷新默认走 `https://api.oplist.org/quarkyun/renewapi`（可用 `QUARK_RENEW_API` 改）；失败会回退飞牛 `refreshToken`。建议配 `QUARK_TOKEN_FILE`，刷新后写回磁盘，重启不用重扫。

### 启动示例

```bash
quarkdrive-webdav \
  --quark-open \
  --quark-app-id '你的AppId' \
  --quark-sign-key '你的SignKey' \
  --quark-refresh-token '你的RefreshToken' \
  --quark-token-file ./quark-token.json \
  -U admin -W admin -p 8080
```

Docker Compose：

```yaml
services:
  quarkdrive-webdav:
    image: ghcr.io/poiig/quarkdrive-webdav:latest
    container_name: quarkdrive-webdav
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
    environment:
      - QUARK_OPEN=true
      - QUARK_APP_ID=你的AppId
      - QUARK_SIGN_KEY=你的SignKey
      - QUARK_REFRESH_TOKEN=你的RefreshToken
      - QUARK_TOKEN_FILE=/data/quark-token.json
      - QUARK_USE_ONLINE_API=true
      - WEBDAV_AUTH_USER=admin
      - WEBDAV_AUTH_PASSWORD=admin
      - CHUNK_CACHE_MB=128
```

镜像由 `.github/workflows/docker.yml` 自动构建：

- push `main` → `ghcr.io/poiig/quarkdrive-webdav:latest` + `sha-xxxxxxx`
- push `v*` tag → 对应版本号标签
- PR → 只构建不推送
- 也可在 Actions 里手动 `workflow_dispatch`

GHCR 私有包需先登录：`echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin`，并在包设置里授权自己的账号读取。

启动后用 WebDAV 客户端或浏览器访问 `http://主机:8080`。

## 常用参数

| 参数 / 环境变量 | 说明 | 默认 |
|-----------------|------|------|
| `--quark-cookie` / `QUARK_COOKIE` | Cookie 模式凭证 | — |
| `--quark-open` / `QUARK_OPEN` | 启用 Open 模式 | `false` |
| `--quark-app-id` / `QUARK_APP_ID` | Open AppId | — |
| `--quark-sign-key` / `QUARK_SIGN_KEY` | Open SignKey | — |
| `--quark-refresh-token` / `QUARK_REFRESH_TOKEN` | Open Refresh Token | — |
| `--quark-access-token` / `QUARK_ACCESS_TOKEN` | Open Access Token（可空，会刷新） | 空 |
| `--quark-token-file` / `QUARK_TOKEN_FILE` | token 落盘路径 | — |
| `--quark-use-online-api` / `QUARK_USE_ONLINE_API` | 用在线 API 刷新 token | `true` |
| `--quark-renew-api` / `QUARK_RENEW_API` | 在线刷新地址 | `https://api.oplist.org/quarkyun/renewapi` |
| `--chunk-cache-mb` / `CHUNK_CACHE_MB` | 预取块缓存（MB），`0` 关闭 | `128` |
| `-U` / `WEBDAV_AUTH_USER` | WebDAV 用户名 | — |
| `-W` / `WEBDAV_AUTH_PASSWORD` | WebDAV 密码 | — |
| `-p` / `PORT` | 监听端口 | `8080` |

## 免责声明

仅供学习研究，请遵守当地法规与夸克服务条款。使用风险自负（含账号限制等）。
