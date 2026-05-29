package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"tempmail/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// emailCounters 维护写入的累计数，避免每封邮件都 UPDATE 同一行。
// 由后台 flusher 周期性合并到数据库。
type emailCounters struct {
	mu      sync.Mutex
	total   int64
	mailbox map[uuid.UUID]int64
}

func (c *emailCounters) inc(mailboxID uuid.UUID) {
	c.mu.Lock()
	c.total++
	if c.mailbox == nil {
		c.mailbox = make(map[uuid.UUID]int64)
	}
	c.mailbox[mailboxID]++
	c.mu.Unlock()
}

// snapshot 返回当前累计的内存计数（不清零，用于实时查询补偿）。
func (c *emailCounters) snapshot() (int64, map[uuid.UUID]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[uuid.UUID]int64, len(c.mailbox))
	for k, v := range c.mailbox {
		cp[k] = v
	}
	return c.total, cp
}

// drain 取出累计值并清零，供 flush 使用。
func (c *emailCounters) drain() (int64, map[uuid.UUID]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.total
	mb := c.mailbox
	c.total = 0
	c.mailbox = nil
	return total, mb
}

type Store struct {
	pool     *pgxpool.Pool
	counters emailCounters
}

// New 创建带连接池的 Store（高并发核心）
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	// 连接池：不限并发，由 PgBouncer 统一管控实际 PG 连接数
	cfg.MaxConns = 500
	cfg.MinConns = 20
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	// PgBouncer transaction 模式不支持 named prepared statements。
	// pgx v5 默认使用 QueryExecModeCacheStatement（会发送 Parse/Bind/Execute），
	// 多个连接复用同一个后端连接时会触发 "prepared statement already in use"。
	// 改为 SimpleProtocol：直接发送明文 SQL，完全绕过服务端 prepared statement 机制。
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect db: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// ==================== Account ====================

func (s *Store) GetAccountByAPIKey(ctx context.Context, apiKey string) (*model.Account, error) {
	var a model.Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at
		 FROM accounts WHERE api_key = $1 AND is_active = TRUE`, apiKey,
	).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) GetOrCreateLinuxDOAccount(ctx context.Context, linuxDOID, username string) (*model.Account, error) {
	var a model.Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at
		 FROM accounts WHERE linuxdo_id = $1 AND is_active = TRUE`, linuxDOID,
	).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err == nil {
		return &a, nil
	}
	if err != pgx.ErrNoRows {
		return nil, err
	}

	apiKey := generateAPIKey()
	for i := 0; i < 5; i++ {
		name := username
		if i > 0 {
			name = fmt.Sprintf("%s_%d", username, i+1)
		}
		err = s.pool.QueryRow(ctx,
			`INSERT INTO accounts (username, api_key, linuxdo_id) VALUES ($1, $2, $3)
			 RETURNING id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at`,
			name, apiKey, linuxDOID,
		).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
		if err == nil {
			return &a, nil
		}
	}
	return nil, errors.New("failed to create Linux DO account")
}

func (s *Store) GetOrCreateGitHubAccount(ctx context.Context, gitHubID, username string) (*model.Account, error) {
	var a model.Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at
		 FROM accounts WHERE github_id = $1 AND is_active = TRUE`, gitHubID,
	).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err == nil {
		return &a, nil
	}
	if err != pgx.ErrNoRows {
		return nil, err
	}

	apiKey := generateAPIKey()
	for i := 0; i < 5; i++ {
		name := username
		if i > 0 {
			name = fmt.Sprintf("%s_%d", username, i+1)
		}
		err = s.pool.QueryRow(ctx,
			`INSERT INTO accounts (username, api_key, github_id) VALUES ($1, $2, $3)
			 RETURNING id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at`,
			name, apiKey, gitHubID,
		).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
		if err == nil {
			return &a, nil
		}
	}
	return nil, errors.New("failed to create GitHub account")
}

func (s *Store) CreateAccount(ctx context.Context, username string) (*model.Account, error) {
	apiKey := generateAPIKey()
	var a model.Account
	err := s.pool.QueryRow(ctx,
		`INSERT INTO accounts (username, api_key) VALUES ($1, $2)
		 RETURNING id, username, api_key, COALESCE(linuxdo_id, ''), is_admin, is_active, created_at, updated_at`,
		username, apiKey,
	).Scan(&a.ID, &a.Username, &a.APIKey, &a.LinuxDOID, &a.IsAdmin, &a.IsActive, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) DeleteAccount(ctx context.Context, accountID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	return err
}

func (s *Store) ListAccounts(ctx context.Context, page, size int) ([]model.AccountWithStats, int, error) {
	var total int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx,
		`SELECT
			a.id, a.username, a.api_key, COALESCE(a.linuxdo_id, ''), a.is_admin, a.is_active, a.created_at, a.updated_at,
			COUNT(DISTINCT m.id)::INT AS mailbox_count,
			COUNT(DISTINCT m.id) FILTER (WHERE m.expires_at > NOW())::INT AS active_mailbox_count,
			COUNT(e.id)::INT AS current_email_count,
			COALESCE(SUM(m.received_email_count), 0)::INT AS received_email_count
		 FROM accounts a
		 LEFT JOIN mailboxes m ON m.account_id = a.id
		 LEFT JOIN emails e ON e.mailbox_id = m.id
		 GROUP BY a.id
		 ORDER BY a.created_at DESC LIMIT $1 OFFSET $2`,
		size, (page-1)*size,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	accounts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[model.AccountWithStats])
	if err != nil {
		return nil, 0, err
	}
	return accounts, total, nil
}

// GetAdminAPIKey 获取第一个管理员账号的 API Key（用于写入 admin.key 文件）
func (s *Store) GetAdminAPIKey(ctx context.Context) (string, error) {
	var apiKey string
	err := s.pool.QueryRow(ctx,
		`SELECT api_key FROM accounts WHERE is_admin = TRUE ORDER BY created_at LIMIT 1`,
	).Scan(&apiKey)
	return apiKey, err
}

// ==================== Domain ====================

func (s *Store) AddDomain(ctx context.Context, domain string) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, is_active, status) VALUES ($1, TRUE, 'active')
		 RETURNING id, domain, is_active, status, created_at, mx_checked_at`,
		strings.ToLower(domain),
	).Scan(&d.ID, &d.Domain, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// AddDomainPending 添加待验证域名（后台轮询 MX 记录）
func (s *Store) AddDomainPending(ctx context.Context, domain string) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, is_active, status) VALUES ($1, FALSE, 'pending')
		 ON CONFLICT (domain) DO UPDATE
		   SET status = CASE WHEN domains.status = 'active' THEN 'active' ELSE 'pending' END,
		       is_active = CASE WHEN domains.status = 'active' THEN TRUE ELSE FALSE END
		 RETURNING id, domain, is_active, status, created_at, mx_checked_at`,
		strings.ToLower(domain),
	).Scan(&d.ID, &d.Domain, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) ListDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at FROM domains ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

func (s *Store) GetActiveDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at FROM domains WHERE is_active = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

func (s *Store) GetRandomActiveDomain(ctx context.Context) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at FROM domains
		 WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1`,
	).Scan(&d.ID, &d.Domain, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// GetDomainByName 按域名字符串查找活跃域名，供创建邮箱时指定域名使用
func (s *Store) GetDomainByName(ctx context.Context, domain string) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE domain = $1 AND is_active = TRUE`,
		strings.ToLower(domain),
	).Scan(&d.ID, &d.Domain, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) GetDomainByID(ctx context.Context, domainID int) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at FROM domains WHERE id = $1`,
		domainID,
	).Scan(&d.ID, &d.Domain, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// ListPendingDomains 返回所有待验证域名
func (s *Store) ListPendingDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE status = 'pending'
		 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

// PromoteDomainToActive 验证通过，激活域名
func (s *Store) PromoteDomainToActive(ctx context.Context, domainID int) error {
	now := time.Now()
	_, err := s.pool.Exec(ctx,
		`UPDATE domains SET is_active = TRUE, status = 'active', mx_checked_at = $1 WHERE id = $2`,
		now, domainID)
	return err
}

// TouchDomainCheckTime 更新最后检测时间
func (s *Store) TouchDomainCheckTime(ctx context.Context, domainID int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE domains SET mx_checked_at = NOW() WHERE id = $1`, domainID)
	return err
}

// DisableDomainMX MX检测失败，自动停用域名
func (s *Store) DisableDomainMX(ctx context.Context, domainID int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE domains SET is_active = FALSE, status = 'disabled', mx_checked_at = NOW() WHERE id = $1`,
		domainID)
	return err
}

func (s *Store) DeleteDomain(ctx context.Context, domainID int) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM domains WHERE id = $1`, domainID)
	return err
}

func (s *Store) ToggleDomain(ctx context.Context, domainID int, active bool) error {
	status := "disabled"
	if active {
		status = "active"
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE domains SET is_active = $1, status = $2 WHERE id = $3`, active, status, domainID)
	return err
}

// GetStats 返回全局统计数据
func (s *Store) GetStats(ctx context.Context) (*model.Stats, error) {
	var st model.Stats
	err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM mailboxes)                         AS total_mailboxes,
		  (SELECT COUNT(*) FROM mailboxes WHERE expires_at > NOW()) AS active_mailboxes,
		  COALESCE((SELECT NULLIF(value, '')::INT FROM app_settings WHERE key = 'total_emails_received'), (SELECT COUNT(*) FROM emails)) AS total_emails,
		  (SELECT COUNT(*) FROM domains WHERE is_active = TRUE)    AS active_domains,
		  (SELECT COUNT(*) FROM domains WHERE status = 'pending')  AS pending_domains,
		  (SELECT COUNT(*) FROM accounts WHERE is_active = TRUE)   AS total_accounts
	`).Scan(
		&st.TotalMailboxes, &st.ActiveMailboxes,
		&st.TotalEmails, &st.ActiveDomains,
		&st.PendingDomains, &st.TotalAccounts,
	)
	if err != nil {
		return nil, err
	}
	// 加上还没 flush 到数据库的内存计数，让前台看到接近实时值。
	pendingTotal, _ := s.counters.snapshot()
	st.TotalEmails += int(pendingTotal)
	return &st, nil
}

// ==================== Mailbox ====================

func (s *Store) CreateMailbox(ctx context.Context, accountID uuid.UUID, address string, domainID int, fullAddress string, ttlMinutes int) (*model.Mailbox, error) {
	if ttlMinutes <= 0 {
		ttlMinutes = 30
	}
	expiresAt := time.Now().Add(time.Duration(ttlMinutes) * time.Minute)
	var m model.Mailbox
	err := s.pool.QueryRow(ctx,
		`INSERT INTO mailboxes (account_id, address, domain_id, full_address, expires_at)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, account_id, address, domain_id, full_address, created_at, expires_at`,
		accountID, address, domainID, fullAddress, expiresAt,
	).Scan(&m.ID, &m.AccountID, &m.Address, &m.DomainID, &m.FullAddress, &m.CreatedAt, &m.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Store) ListMailboxes(ctx context.Context, accountID uuid.UUID, page, size int) ([]model.Mailbox, int, error) {
	var total int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM mailboxes WHERE account_id = $1`, accountID).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx,
		`SELECT id, account_id, address, domain_id, full_address, created_at, expires_at
		 FROM mailboxes WHERE account_id = $1
		 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
		accountID, size, (page-1)*size,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	mailboxes, err := pgx.CollectRows(rows, pgx.RowToStructByPos[model.Mailbox])
	if err != nil {
		return nil, 0, err
	}
	return mailboxes, total, nil
}

func (s *Store) GetMailbox(ctx context.Context, mailboxID uuid.UUID, accountID uuid.UUID) (*model.Mailbox, error) {
	var m model.Mailbox
	err := s.pool.QueryRow(ctx,
		`SELECT id, account_id, address, domain_id, full_address, created_at, expires_at
		 FROM mailboxes WHERE id = $1 AND account_id = $2`,
		mailboxID, accountID,
	).Scan(&m.ID, &m.AccountID, &m.Address, &m.DomainID, &m.FullAddress, &m.CreatedAt, &m.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Store) DeleteMailbox(ctx context.Context, mailboxID uuid.UUID, accountID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM mailboxes WHERE id = $1 AND account_id = $2`, mailboxID, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) GetMailboxByFullAddress(ctx context.Context, fullAddress string) (*model.Mailbox, error) {
	var m model.Mailbox
	err := s.pool.QueryRow(ctx,
		`SELECT id, account_id, address, domain_id, full_address, created_at, expires_at
		 FROM mailboxes WHERE full_address = $1`,
		strings.ToLower(fullAddress),
	).Scan(&m.ID, &m.AccountID, &m.Address, &m.DomainID, &m.FullAddress, &m.CreatedAt, &m.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DeleteExpiredMailboxes 刪除已过期的邮箱（及其所有邮件）
func (s *Store) DeleteExpiredMailboxes(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM mailboxes WHERE expires_at < NOW()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// CheckDomainMX 检测域名MX记录是否指向指定服务器IP
func CheckDomainMX(domain, serverIP string) (matched bool, mxHosts []string, status string) {
	mxRecords, err := net.LookupMX(domain)
	if err != nil {
		return false, nil, fmt.Sprintf("DNS查询失败: %v", err)
	}
	if len(mxRecords) == 0 {
		return false, nil, "未找到MX记录，请先配置MX记录"
	}
	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")
		mxHosts = append(mxHosts, host)
		addrs, err := net.LookupHost(host)
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			if addr == serverIP {
				return true, mxHosts, fmt.Sprintf("✓ MX记录匹配：%s → %s", host, addr)
			}
		}
	}
	return false, mxHosts, fmt.Sprintf("MX记录(%s)未指向本服务器(%s)", strings.Join(mxHosts, ","), serverIP)
}

// ==================== Email ====================

func (s *Store) InsertEmail(ctx context.Context, mailboxID uuid.UUID, sender, subject, bodyText, bodyHTML, raw string) (*model.Email, error) {
	var e model.Email
	err := s.pool.QueryRow(ctx,
		`INSERT INTO emails (mailbox_id, sender, subject, body_text, body_html, raw_message, size_bytes)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id, mailbox_id, sender, subject, body_text, body_html, raw_message, size_bytes, received_at`,
		mailboxID, sender, subject, bodyText, bodyHTML, raw, len(raw),
	).Scan(&e.ID, &e.MailboxID, &e.Sender, &e.Subject, &e.BodyText, &e.BodyHTML, &e.RawMessage, &e.SizeBytes, &e.ReceivedAt)
	if err != nil {
		return nil, err
	}
	// 热点累加放入内存计数器，由 RunStatsFlusher 异步 flush，避免高并发争抢同一行。
	s.counters.inc(mailboxID)
	return &e, nil
}

// RunStatsFlusher 周期性把内存累计写回数据库。
// 在 main.go 启动后调用一次即可（goroutine 内运行）。
func (s *Store) RunStatsFlusher(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flushCounters(context.Background())
			return
		case <-t.C:
			s.flushCounters(ctx)
		}
	}
}

func (s *Store) flushCounters(ctx context.Context) {
	total, mailboxes := s.counters.drain()
	if total == 0 && len(mailboxes) == 0 {
		return
	}

	if total > 0 {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO app_settings (key, value) VALUES ('total_emails_received', $1::TEXT)
			ON CONFLICT (key) DO UPDATE
			SET value = ((COALESCE(NULLIF(app_settings.value, ''), '0')::BIGINT + EXCLUDED.value::BIGINT)::TEXT), updated_at = NOW()
		`, total)
		if err != nil {
			// 失败时把计数加回去，下次再 flush。
			s.counters.mu.Lock()
			s.counters.total += total
			s.counters.mu.Unlock()
		}
	}

	for id, n := range mailboxes {
		if n == 0 {
			continue
		}
		_, err := s.pool.Exec(ctx,
			`UPDATE mailboxes SET received_email_count = received_email_count + $1 WHERE id = $2`,
			n, id,
		)
		if err != nil {
			s.counters.mu.Lock()
			if s.counters.mailbox == nil {
				s.counters.mailbox = make(map[uuid.UUID]int64)
			}
			s.counters.mailbox[id] += n
			s.counters.mu.Unlock()
		}
	}
}

func (s *Store) ListEmails(ctx context.Context, mailboxID uuid.UUID, page, size int) ([]model.EmailSummary, int, error) {
	var total int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx,
		`SELECT id, sender, subject, size_bytes, received_at
		 FROM emails WHERE mailbox_id = $1
		 ORDER BY received_at DESC LIMIT $2 OFFSET $3`,
		mailboxID, size, (page-1)*size,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	emails, err := pgx.CollectRows(rows, pgx.RowToStructByPos[model.EmailSummary])
	if err != nil {
		return nil, 0, err
	}
	return emails, total, nil
}

func (s *Store) GetEmail(ctx context.Context, emailID uuid.UUID, mailboxID uuid.UUID) (*model.Email, error) {
	var e model.Email
	err := s.pool.QueryRow(ctx,
		`SELECT id, mailbox_id, sender, subject, body_text, body_html, raw_message, size_bytes, received_at
		 FROM emails WHERE id = $1 AND mailbox_id = $2`,
		emailID, mailboxID,
	).Scan(&e.ID, &e.MailboxID, &e.Sender, &e.Subject, &e.BodyText, &e.BodyHTML, &e.RawMessage, &e.SizeBytes, &e.ReceivedAt)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *Store) DeleteEmail(ctx context.Context, emailID uuid.UUID, mailboxID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM emails WHERE id = $1 AND mailbox_id = $2`, emailID, mailboxID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ==================== Helpers ====================

func generateAPIKey() string {
	b := make([]byte, 24)
	rand.Read(b)
	return "tm_" + hex.EncodeToString(b)
}

func GenerateRandomAddress() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := 10
	result := make([]byte, length)
	for i := range result {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		result[i] = chars[n.Int64()]
	}
	return string(result)
}
