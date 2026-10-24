#!/bin/sh
set -eu

# 挂载卷常为 root 所属；进程以 app 运行，需保证可写，否则扫码 token 落不了盘。
mkdir -p /data /app/logs
chown -R app:app /data /app/logs 2>/dev/null || true
chmod -R u+rwX /data /app/logs 2>/dev/null || true

# 清理临时上传残留
crond

exec su -s /bin/sh app -c "/usr/local/bin/quarkdrive-webdav"
