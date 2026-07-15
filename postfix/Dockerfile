FROM golang:1.23-alpine AS receiver-builder

WORKDIR /build
COPY postfix/mail-receiver-lmtpd.go .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/mail-receiver mail-receiver-lmtpd.go

# ============================================================
# Postfix 收件容器
# ============================================================
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

RUN apt-get update && apt-get install -y --no-install-recommends \
    postfix \
    python3 \
    curl \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 复制配置
COPY postfix/main.cf /etc/postfix/main.cf
COPY postfix/master.cf /etc/postfix/master.cf
COPY --from=receiver-builder /build/mail-receiver /usr/local/bin/mail-receiver
COPY postfix/entrypoint.sh /entrypoint.sh

RUN chmod +x /usr/local/bin/mail-receiver /entrypoint.sh

# 域名映射与 Postfix 队列一起放在持久 spool 卷中。
RUN touch /var/spool/postfix/virtual_domains

EXPOSE 25

CMD ["/entrypoint.sh"]
