#!/bin/bash
set -e

# 生成 PgBouncer 用户列表（明文密码，用于 scram-sha-256 透传）
PGBOUNCER_AUTH_FILE="/etc/pgbouncer/userlist.txt"
echo "\"${POSTGRES_USER}\" \"${POSTGRES_PASSWORD}\"" > "$PGBOUNCER_AUTH_FILE"

PGBOUNCER_CONFIG="/tmp/pgbouncer.ini"
cp /etc/pgbouncer/pgbouncer.ini "$PGBOUNCER_CONFIG"

set_uint() {
    key="$1"
    value="$2"
    case "$value" in
        ''|*[!0-9]*)
            echo "invalid PgBouncer setting $key=$value" >&2
            exit 1
            ;;
    esac
    sed -i "s/^${key}[[:space:]]*=.*/${key} = ${value}/" "$PGBOUNCER_CONFIG"
}

pool_mode="${PGBOUNCER_POOL_MODE:-transaction}"
case "$pool_mode" in
    session|transaction) ;;
    *)
        echo "invalid PgBouncer pool_mode=$pool_mode (use transaction or session)" >&2
        exit 1
        ;;
esac
sed -i "s/^pool_mode[[:space:]]*=.*/pool_mode = ${pool_mode}/" "$PGBOUNCER_CONFIG"

set_uint max_client_conn "${PGBOUNCER_MAX_CLIENT_CONN:-2000}"
set_uint default_pool_size "${PGBOUNCER_DEFAULT_POOL_SIZE:-64}"
set_uint min_pool_size "${PGBOUNCER_MIN_POOL_SIZE:-16}"
set_uint reserve_pool_size "${PGBOUNCER_RESERVE_POOL_SIZE:-16}"

exec pgbouncer "$PGBOUNCER_CONFIG"
