#!/bin/bash
set -e

# ============================================================
# Postfix 容器入口脚本
# - 启动常驻 Go LMTP daemon (mail-receiver)
# - 动态从数据库加载域名到 virtual_domains regexp map
# - Postfix 通过内置 lmtp 客户端走 unix socket 投递
# ============================================================

echo "==> Starting Postfix mail receiver (LMTP daemon)..."

chmod +x /usr/local/bin/mail-receiver

# socket 必须放在 Postfix chroot 内的 private/ 目录
SOCK_DIR=/var/spool/postfix/private
mkdir -p "$SOCK_DIR"
SOCK_PATH="$SOCK_DIR/mail-receiver"
export LMTPD_SOCKET="$SOCK_PATH"
export API_URL="${API_URL:-http://api:8080}"
export LMTPD_HOSTNAME="${SMTP_HOSTNAME:-mail.example.com}"
export DOMAIN_MAP="${DOMAIN_MAP:-/var/spool/postfix/virtual_domains}"

# 启动 LMTP daemon（后台），异常退出会让容器整体重启
/usr/local/bin/mail-receiver &
RECEIVER_PID=$!

# 等待 socket 出现，最多 5 秒
for i in $(seq 1 50); do
    if [ -S "$SOCK_PATH" ]; then break; fi
    sleep 0.1
done
if [ ! -S "$SOCK_PATH" ]; then
    echo "mail-receiver socket not ready: $SOCK_PATH" >&2
    exit 1
fi
chown postfix:postfix "$SOCK_PATH" 2>/dev/null || true

# API/数据库是域名映射的唯一真源。合法的“零活跃域名”也是空文件，
# 因此只在文件不存在时创建，绝不能在重启时用 SMTP_HOSTNAME 回填。
if [ ! -e "$DOMAIN_MAP" ]; then
    touch "$DOMAIN_MAP"
fi

cat > /usr/local/bin/sync-domains.sh << 'SCRIPT'
#!/bin/bash
set -e

API_URL="${API_URL:-http://api:8080}"
DOMAIN_MAP="${DOMAIN_MAP:-/var/spool/postfix/virtual_domains}"
DOMAINS=$(curl -sf "$API_URL/internal/domains" 2>/dev/null || echo "")
if [ -n "$DOMAINS" ]; then
    if echo "$DOMAINS" | python3 -c '
import json
import re
import sys

def normalize_domain(value):
    if not isinstance(value, str):
        return ""
    return value.strip().lower().rstrip(".")

def regexp_line(pattern):
    return f"/^{pattern}$/     OK"

def exact_pattern(domain):
    return re.escape(domain)

def wildcard_pattern(base_domain):
    labels = re.escape(base_domain)
    return rf"[^.]+(\.[^.]+)*\.{labels}"

def is_wildcard_record(record, domain):
    domain_type = str(record.get("domain_type", "")).strip().lower()
    return (
        domain.startswith("*.")
        or domain_type in {
            "wildcard",
            "wildcard_subdomain",
            "subdomain",
            "multi_level",
            "multi_level_subdomain",
        }
    )

def is_active_record(record):
    active = record.get("is_active", True)
    if isinstance(active, str):
        return active.strip().lower() not in {"", "0", "false", "no", "off"}
    return bool(active)

def truthy(value):
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off"}
    return bool(value)

try:
    data = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(1)

if isinstance(data, dict):
    records = data.get("domains")
elif isinstance(data, list):
    records = data
else:
    sys.exit(1)

if not isinstance(records, list):
    sys.exit(1)

lines = []
seen = set()

for record in records:
    if not isinstance(record, dict):
        continue
    if not is_active_record(record):
        continue

    domain = normalize_domain(record.get("domain"))
    base_domain = normalize_domain(record.get("base_domain"))
    if domain.startswith("*."):
        base_domain = normalize_domain(domain[2:])
    elif not base_domain:
        base_domain = domain

    if not base_domain:
        continue

    supports_single = truthy(record.get("supports_single"))
    supports_wildcard = truthy(record.get("supports_wildcard"))
    if not supports_single and not supports_wildcard:
        wildcard = is_wildcard_record(record, domain)
        supports_single = not wildcard
        supports_wildcard = wildcard

    for pattern in (
        exact_pattern(base_domain) if supports_single else "",
        wildcard_pattern(base_domain) if supports_wildcard else "",
    ):
        if not pattern:
            continue
        line = regexp_line(pattern)
        if line not in seen:
            seen.add(line)
            lines.append(line)

sys.stdout.write("\n".join(lines))
' > "$DOMAIN_MAP.new"; then
        if cmp -s "$DOMAIN_MAP.new" "$DOMAIN_MAP"; then
            rm -f "$DOMAIN_MAP.new"
        else
            mv "$DOMAIN_MAP.new" "$DOMAIN_MAP"
            postfix reload 2>/dev/null || true
        fi
    else
        rm -f "$DOMAIN_MAP.new"
    fi
fi
SCRIPT
chmod +x /usr/local/bin/sync-domains.sh

/usr/local/bin/sync-domains.sh || true
(while true; do sleep 60; /usr/local/bin/sync-domains.sh; done) &

postconf -e "myhostname=${SMTP_HOSTNAME:-mail.example.com}"
postconf -e "virtual_mailbox_domains=regexp:${DOMAIN_MAP}"
postconf -e "virtual_transport=lmtp:unix:private/mail-receiver"

set_postfix_uint() {
    key="$1"
    value="$2"
    case "$value" in
        ''|*[!0-9]*)
            echo "invalid Postfix setting $key=$value" >&2
            exit 1
            ;;
    esac
    postconf -e "$key=$value"
}

# Keep the service paths unchanged while allowing deployments to tune the
# actual SMTP and LMTP concurrency for their CPU, memory, and storage.
set_postfix_uint default_process_limit "${POSTFIX_DEFAULT_PROCESS_LIMIT:-500}"
set_postfix_uint smtpd_client_connection_count_limit "${POSTFIX_SMTPD_CLIENT_CONNECTION_COUNT_LIMIT:-200}"
set_postfix_uint smtpd_client_connection_rate_limit "${POSTFIX_SMTPD_CLIENT_CONNECTION_RATE_LIMIT:-0}"
set_postfix_uint default_destination_concurrency_limit "${POSTFIX_DEFAULT_DESTINATION_CONCURRENCY_LIMIT:-200}"
set_postfix_uint lmtp_destination_concurrency_limit "${POSTFIX_LMTP_DESTINATION_CONCURRENCY_LIMIT:-128}"

# The per-client limit above is not a global smtpd cap. Keep a separate master
# service limit so traffic from many source IPs cannot consume all 500 default
# Postfix process slots and starve cleanup/LMTP queue draining.
SMTPD_PROCESS_LIMIT="${POSTFIX_SMTPD_PROCESS_LIMIT:-200}"
case "$SMTPD_PROCESS_LIMIT" in
    ''|*[!0-9]*)
        echo "invalid Postfix smtpd process limit: $SMTPD_PROCESS_LIMIT" >&2
        exit 1
        ;;
esac
postconf -M "smtp/inet=smtp inet n - n - $SMTPD_PROCESS_LIMIT smtpd"

trap "kill $RECEIVER_PID 2>/dev/null; exit 0" TERM INT

postfix start

# 监控 receiver 进程：退出则停止 postfix，让容器重启
wait $RECEIVER_PID
echo "mail-receiver exited, stopping postfix" >&2
postfix stop
exit 1
