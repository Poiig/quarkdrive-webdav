# 运行阶段：极简 Alpine 镜像
# 二进制文件由 CI 预先交叉编译，通过 TARGETARCH 自动选择对应架构
FROM alpine:3.19

ARG TARGETARCH

RUN apk add --no-cache cronie findutils && rm -rf /var/cache/apk/*
RUN addgroup -S app && adduser -S app -G app -s /bin/sh
WORKDIR /app

COPY bin/${TARGETARCH}/quarkdrive-webdav /usr/local/bin/quarkdrive-webdav
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/quarkdrive-webdav /entrypoint.sh \
    && mkdir -p /data /app/logs \
    && chown -R app:app /data /app/logs

# 临时文件定时清理
RUN echo "0 3 * * * find /tmp -type f -amin +15 -mmin +15 -cmin +15 -delete" > /etc/crontabs/root

USER root
ENTRYPOINT ["/entrypoint.sh"]
