# TempMail

一个自托管临时邮件服务平台，支持多域名、用户自助提交域名、MX 自动验证与自动禁用、API Key 鉴权及 Web 管理后台。
部署教程可看https://linux.do/t/topic/1667604
---

## 功能特性

| 功能 | 说明 |
|------|------|
| 邮箱管理 | 按需创建临时邮箱，可配置 TTL（默认 30 分钟），自动清理 |
| 多域名池 | 多个域名轮流供用户创建邮箱，管理员或普通用户均可提交新域名 |
| MX 自动验证 | 提交域名后后台每 30 秒轮询 MX 记录，通过即自动激活，无需管理员确认 |
| 域名健康监控 | 每 6 小时重检已激活域名，MX 失效自动暂停（`status=disabled`）|
| IP / Hostname 分离 | 服务器 IP 与邮件主机名通过环境变量或后台设置注入，不写入代码 |
| API Key 鉴权 | 每用户独立 API Key（`X-API-Key` 头），速率限制 500 次/分钟 |
| OAuth 登录 | 支持 Linux DO Connect 与 GitHub OAuth 登录，可在后台独立开关 |
| 管理后台 | Web GUI 管理账户、域名、邮件、系统配置（含 SMTP Hostname）|
| Dashboard 统计 | 实时展示邮箱数、邮件数、域名数、账户数 |
| 公告系统 | 管理员可设置公告，用户登录后显示 |
| 速率限制 | Redis 滑动窗口，默认 500 请求/60 秒/令牌 |
| 连接池 | PgBouncer 事务模式，支持 2000 并发客户端 |

---

## 快速启动

### 前置条件

- Docker 20.10+
- Docker Compose v2+
- 公网 IP / 域名（用于接收邮件）

### 1. 克隆并配置

```bash
git clone <repo-url>
cd tempmail
cp .env.example .env
# 编辑 .env，填写 SMTP_SERVER_IP 和 SMTP_HOSTNAME
```

### 2. 启动服务

```bash
docker compose up -d
```

六个容器会自动启动：`postgres`、`pgbouncer`、`redis`、`api`、`frontend`（Nginx）、`postfix`。

### 3. 获取管理员 API Key

首次启动后，管理员 Key 会写入 `data/admin.key`：

```bash
cat data/admin.key
# tm_admin_<自动生成的随机密钥>
```

也可查看容器日志：

```bash
docker compose logs api | grep "ADMIN API KEY"
```

### 4. 访问 Web 界面

浏览器打开 `http://<服务器IP>`，在登录页输入管理员 API Key 登录。

---

## 环境变量

在项目根目录 `.env` 文件中配置（**所有含服务器 IP / 域名的信息均在此处填写，不写入代码**）：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SMTP_SERVER_IP` | *(必填)* | 服务器公网 IP，用于 MX 验证与 SPF 生成 |
| `SMTP_HOSTNAME` | *(推荐填写)* | 邮件服务器主机名，如 `mail.yourdomain.com`。设置后用户添加域名只需一条 MX 记录，无需 A 记录 |
| `API_DB_DSN` | `postgres://tempmail:...@pgbouncer:6432/tempmail` | API 数据库连接串（经 PgBouncer）|
| `API_REDIS_ADDR` | `redis:6379` | Redis 连接地址 |
| `API_PORT` | `8080` | API 监听端口 |
| `API_DB_MAX_CONNS` | `128` | API 到 PgBouncer 的最大客户端连接数 |
| `API_DB_MIN_CONNS` | `16` | API 预热数据库连接数 |
| `API_DELIVERY_BATCH_ENABLED` | `true` | 是否启用单封内部投递微批；不改变 API 路径或响应，可设为 `false` 快速回退 |
| `API_DELIVERY_BATCH_MAX` | `64` | 单封内部投递微批的最大邮件数（允许 1–256，不改变 API 路径）|
| `API_DELIVERY_BATCH_WAIT` | `1ms` | 收集并发单封投递的最长等待时间（允许 100µs–100ms）|
| `PGBOUNCER_DEFAULT_POOL_SIZE` | `64` | PgBouncer 到 PostgreSQL 的常规连接数 |
| `PGBOUNCER_RESERVE_POOL_SIZE` | `16` | PgBouncer 预留连接数 |
| `POSTFIX_DEFAULT_PROCESS_LIMIT` | `500` | Postfix 服务默认进程上限 |
| `POSTFIX_SMTPD_PROCESS_LIMIT` | `200` | SMTP 服务全局进程上限，避免多来源流量挤占 LMTP |
| `POSTFIX_SMTPD_CLIENT_CONNECTION_COUNT_LIMIT` | `200` | 单来源 IP 同时 SMTP 连接上限 |
| `POSTFIX_SMTPD_CLIENT_CONNECTION_RATE_LIMIT` | `0` | 单来源建连速率；`0` 为关闭 |
| `POSTFIX_LMTP_DESTINATION_CONCURRENCY_LIMIT` | `128` | Postfix 到 LMTP daemon 的投递并发 |
| `API_RATE_LIMIT` | `500` | 每令牌每窗口期最大请求数 |
| `API_RATE_WINDOW` | `60` | 速率窗口（秒）|
| `GITHUB_CLIENT_ID` | *(可选)* | GitHub OAuth App 的 Client ID，也可在后台系统设置中填写 |
| `GITHUB_CLIENT_SECRET` | *(可选)* | GitHub OAuth App 的 Client Secret，也可在后台系统设置中填写 |
| `GITHUB_REDIRECT_URL` | *(可选)* | GitHub OAuth 回调地址，如 `https://your-domain.com/public/auth/github/callback` |
| `ADMIN_KEY_FILE` | `/data/admin.key` | 管理员 Key 写入路径（容器内）|

`.env` 示例：

```dotenv
SMTP_SERVER_IP=1.2.3.4
SMTP_HOSTNAME=mail.yourdomain.com
GITHUB_REDIRECT_URL=https://your-domain.com/public/auth/github/callback
```

> `SMTP_SERVER_IP` / `SMTP_HOSTNAME` 也可在管理后台「系统设置」中修改，DB 值优先于环境变量。
> GitHub 登录需要在 GitHub OAuth App 中把 Authorization callback URL 设置为 `/public/auth/github/callback` 对应的完整公网地址，并在后台开启「GitHub 登录」。

### 吞吐压测

仓库提供两个只使用现有调用路径的压测命令：

```bash
cd api

# API → PgBouncer → PostgreSQL；响应成功代表事务已提交
go run ./cmd/deliverbench \
  -url http://127.0.0.1:8080 \
  -recipient mailbox@example.com \
  -requests 30000 -concurrency 128 -raw-bytes 1024

# SMTP → Postfix queue → LMTP → 原 internal API → PostgreSQL
go run ./cmd/smtpbench \
  -addr 127.0.0.1:25 \
  -recipient mailbox@example.com \
  -messages 10000 -concurrency 200 -messages-per-connection 10 \
  -database-dsn "$BENCH_DATABASE_DSN"
```

`BENCH_DATABASE_DSN` 必须是压测进程可访问的 PostgreSQL/PgBouncer 地址。SMTP 端到端测试应使用没有其他流量的专用邮箱，并在开始前确认队列为空；发送完成后先等待 `postqueue -p` 为空，再确认该邮箱的数据库增量精确等于 `accepted`。未传 `-database-dsn` 且未设置 `BENCH_DATABASE_DSN` 时，命令只报告 Postfix 接收速率，不能代表最终落盘吞吐。

---

## 添加邮件域名

任意已登录用户均可提交域名，管理员可在后台直接添加。

### 方式一：用户自助提交（推荐）

1. 登录后进入「域名列表」→「提交域名」
2. 填写域名，系统会展示所需 DNS 记录
3. 在 DNS 面板完成配置后提交：
   - **MX 已生效** → 立即激活加入域名池
   - **MX 未生效** → 进入待验证队列，后台每 30 秒自动重试，通过后自动激活

### 方式二：管理员直接添加

登录管理后台 → 域名管理 → 手动添加（跳过 MX 检测，立即激活）。

### 所需 DNS 记录

**已配置 `SMTP_HOSTNAME`（推荐）**——仅需 2 条记录：

```
MX   @   mail.yourdomain.com   优先级 10
TXT  @   v=spf1 ip4:<服务器IP> ~all
```

> `mail.yourdomain.com` 为 `SMTP_HOSTNAME` 的值，A 记录由该主机名自身提供，用户域名无需额外 A 记录。

**未配置 `SMTP_HOSTNAME`**——需 3 条记录：

```
MX   @              mail.example.com   优先级 10
A    mail           <服务器公网 IP>
TXT  @              v=spf1 ip4:<服务器公网 IP> ~all
```

### 多级子域名邮箱

如需生成并接收类似 `user@gmail.outlook.mail.com.net.example.com` 的多级子域邮箱，请提交通配域名：

```
*.example.com
```

DNS 需要能覆盖多层通配子域。已配置 `SMTP_HOSTNAME` 时推荐：

```
MX   *   mail.yourdomain.com   优先级 10
TXT  *   v=spf1 ip4:<服务器IP> ~all
```

未配置 `SMTP_HOSTNAME` 时：

```
MX   *      mail.example.com   优先级 10
A    mail   <服务器公网 IP>
TXT  *      v=spf1 ip4:<服务器公网 IP> ~all
```

说明：

- 前端新建邮箱默认使用普通单级域名，即 `mode=single`。
- API 新建邮箱未传 `mode` 时优先使用多级域名；如果没有已激活的 `*.example.com` 通配域名，会自动回退到普通单级域名，兼容旧调用。
- 如需 API 创建普通邮箱，请显式传 `{"mode":"single"}`。
- 提交后系统会用类似 `mx-check.gmail.outlook.mail.com.net.example.com` 的多级主机名验证 MX 是否已经覆盖多层子域。

---

## API 使用

所有 `/api/*` 请求需在 Header 携带：

```
Authorization: Bearer tm_xxxxxxxxxxxx
```

也兼容 `?api_key=tm_xxxxxxxxxxxx` 查询参数。

### 常用接口

```bash
BASE="http://<服务器IP>"
KEY="your_api_key"

# 获取可用域名（无需登录）
curl "$BASE/public/domains"

# 获取公开设置（无需登录）
curl "$BASE/public/settings"

# 创建邮箱（随机生成普通单域名或 10-14 级多级域名邮箱；缺少对应域名类型时自动回退）
curl -X POST "$BASE/api/mailboxes" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"address":"test"}'

# 创建普通单级域名邮箱
curl -X POST "$BASE/api/mailboxes" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"mode":"single","address":"test","domain":"mail.example.com"}'

# 指定通配基础域名创建多级域名邮箱
curl -X POST "$BASE/api/mailboxes" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"mode":"multi","address":"test","domain":"example.com"}'

# 列出邮箱
curl "$BASE/api/mailboxes" -H "Authorization: Bearer $KEY"

# 读取邮件
curl "$BASE/api/mailboxes/<mailbox-id>/emails" -H "Authorization: Bearer $KEY"

# 提交域名（任意登录用户）
curl -X POST "$BASE/api/domains/submit" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain":"example.com"}'

# 提交通配域名（多级邮箱）
curl -X POST "$BASE/api/domains/submit" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain":"*.example.com"}'

# 查询域名验证状态
curl "$BASE/api/domains/<domain-id>/status" -H "Authorization: Bearer $KEY"

# 获取统计（无需登录）
curl "$BASE/public/stats"
```

### 速率限制响应头

每个响应会返回：

```
X-RateLimit-Limit: 500
X-RateLimit-Remaining: 499
X-RateLimit-Reset: 1735000000
```

---

## 数据库迁移

| 文件 | 用途 |
|------|------|
| `sql/init.sql` | 全量初始化（新库使用）|
| `sql/migrate_v2.sql` | v1 → v2：添加邮箱 `expires_at` 字段 |
| `sql/migrate_v3.sql` | v2 → v3：域名 `status`、`mx_checked_at`，新增系统配置项（含 `smtp_hostname`）|
| `sql/migrate_v4.sql` | v3 → v4：Linux DO Connect 登录与登录方式开关 |
| `sql/migrate_v5.sql` | v4 → v5：累计收件统计 |
| `sql/migrate_v6.sql` | v5 → v6：账户收件统计 |
| `sql/migrate_v7.sql` | v6 → v7：自定义站点 Logo |
| `sql/migrate_v8.sql` | v7 → v8：多级/通配域名支持 |
| `sql/migrate_v9.sql` | v8 → v9：GitHub OAuth 登录 |
| `sql/migrate_v10.sql` | v9 → v10：分别跟踪单域名与通配子域名能力 |
| `sql/migrate_v11.sql` | v10 → v11：邮箱累计收件数扩展为 BIGINT |

对已运行的库按版本顺序执行尚未应用的迁移。例如从 v10 升级：

```bash
docker exec -i $(docker compose ps -q postgres) \
  psql -U tempmail -d tempmail < sql/migrate_v11.sql
```

---

## 项目结构

```
tempmail/
├── api/                  # Go API 服务
│   ├── main.go           # 路由、中间件、后台 goroutine
│   ├── config/           # 环境变量配置
│   ├── handler/          # HTTP 处理器
│   ├── middleware/        # 鉴权、速率限制
│   ├── model/            # 数据结构
│   └── store/            # 数据库操作
├── frontend/             # 静态 SPA（Nginx 托管）
│   ├── index.html
│   ├── css/style.css
│   └── js/app.js
├── nginx/                # Nginx 反向代理配置
├── postfix/              # Postfix 邮件接收
├── pgbouncer/            # PgBouncer 连接池配置
├── sql/                  # 数据库 DDL 与迁移脚本
├── data/                 # 运行时数据（admin.key 在此，已 gitignore）
├── docker-compose.yml
└── .env                  # 敏感配置（已 gitignore，不含硬编码 IP）
```

---

## 后台 Goroutine

| Goroutine | 间隔 | 功能 |
|-----------|------|------|
| 邮箱清理器 | 1 分钟 | 删除 `expires_at` 已过期的邮箱及其邮件 |
| MX 域名验证器（待验证） | 30 秒 | 轮询 `status='pending'` 的域名，MX 检测通过则自动激活 |
| MX 域名健康巡检（已激活） | 6 小时 | 重检所有 `status='active'` 的域名，MX 失效则自动禁用 |
| Admin Key 写入 | 启动 1 秒后执行一次 | 将管理员 API Key 写入 `ADMIN_KEY_FILE` |

---

## 许可证

MIT
