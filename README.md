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
  -e QUARK_COOKIE='你的cookie' \
  -e WEBDAV_AUTH_USER=admin \
  -e WEBDAV_AUTH_PASSWORD=admin \
  -e UPLOAD_MEMORY_LIMIT=64MB \
  -e WECOM_WEBHOOK_KEY='企微机器人key' \
  ghcr.io/poiig/quarkdrive-webdav:latest
```

### Open 模式

```bash
docker run -d --name=quarkdrive-webdav --restart=unless-stopped -p 8080:8080 \
  -e QUARK_OPEN=true \
  -e QUARK_TOKEN_FILE=/data/quark-token.json \
  -e WEBDAV_AUTH_USER=admin \
  -e WEBDAV_AUTH_PASSWORD=admin \
  -e UPLOAD_MEMORY_LIMIT=512MB \
  -e WECOM_WEBHOOK_KEY='企微机器人key' \
  -v $(pwd)/data:/data \
  ghcr.io/poiig/quarkdrive-webdav:latest
```

启动后浏览器打开 `http://主机:8080`：

1. 若已授权 → 直接浏览网盘  
2. 若未授权/过期 → 页面提示扫码，点「扫码授权」完成登录  

Token 会写入 `QUARK_TOKEN_FILE`，重启可复用。

## 常用环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `QUARK_COOKIE` | Cookie 模式凭证 | — |
| `QUARK_OPEN` | `true` 启用 Open 模式 | `false` |
| `QUARK_TOKEN_FILE` | Open token 落盘路径 | `./quark-token.json` |
| `QUARK_REFRESH_TOKEN` | 可选，已有 refresh token 时可直接填 | 空 |
| `UPLOAD_MEMORY_LIMIT` | 上传内存上限（并行合计），如 `64MB`/`1GB`；`0`=始终落盘 | `64MB` |
| `WECOM_WEBHOOK_KEY` | 企业微信机器人 key；过期/报错时推送 | — |
| `WEBDAV_AUTH_USER` / `WEBDAV_AUTH_PASSWORD` | WebDAV 账号密码 | — |
| `PORT` | 端口 | `8080` |

## 免责声明

仅供学习研究，请遵守当地法规与夸克服务条款。使用风险自负。
