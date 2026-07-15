package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"regexp"
	"sort"
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

func (c *emailCounters) incMany(mailboxIDs []uuid.UUID) {
	if len(mailboxIDs) == 0 {
		return
	}
	c.mu.Lock()
	c.total += int64(len(mailboxIDs))
	if c.mailbox == nil {
		c.mailbox = make(map[uuid.UUID]int64)
	}
	for _, mailboxID := range mailboxIDs {
		c.mailbox[mailboxID]++
	}
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

type PoolOptions struct {
	MaxConns int32
	MinConns int32
}

const (
	DomainTypeExact    = "exact"
	DomainTypeWildcard = "wildcard"

	DomainCapabilitySingle   = "single"
	DomainCapabilityWildcard = "wildcard"
)

var domainLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type DomainInUseError struct {
	MailboxCount int
}

func (e *DomainInUseError) Error() string {
	return fmt.Sprintf("domain is used by %d mailbox(es)", e.MailboxCount)
}

// New 创建带连接池的 Store（高并发核心）
func New(ctx context.Context, dsn string) (*Store, error) {
	return NewWithOptions(ctx, dsn, PoolOptions{})
}

func NewWithOptions(ctx context.Context, dsn string, options PoolOptions) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	if options.MaxConns <= 0 {
		options.MaxConns = 128
	}
	if options.MinConns < 0 {
		options.MinConns = 0
	} else if options.MinConns == 0 {
		options.MinConns = 16
	}
	if options.MinConns > options.MaxConns {
		options.MinConns = options.MaxConns
	}
	cfg.MaxConns = options.MaxConns
	cfg.MinConns = options.MinConns
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
			(SELECT COUNT(*)::BIGINT FROM mailboxes m WHERE m.account_id = a.id) AS mailbox_count,
			(SELECT COUNT(*)::BIGINT FROM mailboxes m WHERE m.account_id = a.id AND m.expires_at > NOW()) AS active_mailbox_count,
			(SELECT COUNT(*)::BIGINT
			 FROM emails e
			 JOIN mailboxes m ON m.id = e.mailbox_id
			 WHERE m.account_id = a.id) AS current_email_count,
			COALESCE((SELECT SUM(m.received_email_count)::BIGINT FROM mailboxes m WHERE m.account_id = a.id), 0) AS received_email_count
		 FROM accounts a
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
	canonicalDomain, domainType, baseDomain := NormalizeDomain(domain)
	supportsSingle := domainType == DomainTypeExact
	supportsWildcard := domainType == DomainTypeWildcard
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status)
		 VALUES ($1, $2, $3, $4, $5, TRUE, 'active')
		 RETURNING id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at`,
		canonicalDomain, domainType, baseDomain, supportsSingle, supportsWildcard,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) AddDomainWithCapabilities(ctx context.Context, domain string, supportsSingle, supportsWildcard bool) (*model.Domain, error) {
	_, domainType, baseDomain := NormalizeDomain(domain)
	canonicalDomain := baseDomain
	if existing, err := s.GetDomainByName(ctx, baseDomain); err == nil {
		canonicalDomain = existing.Domain
	}
	if supportsWildcard {
		domainType = DomainTypeWildcard
	}
	active := supportsSingle || supportsWildcard
	status := "pending"
	if active {
		status = "active"
	}
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, mx_checked_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		 ON CONFLICT (domain) DO UPDATE
		   SET domain_type = EXCLUDED.domain_type,
		       base_domain = EXCLUDED.base_domain,
		       supports_single = EXCLUDED.supports_single,
		       supports_wildcard = EXCLUDED.supports_wildcard,
		       is_active = EXCLUDED.is_active,
		       status = EXCLUDED.status,
		       mx_checked_at = NOW()
		 RETURNING id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at`,
		canonicalDomain, domainType, baseDomain, supportsSingle, supportsWildcard, active, status,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// AddDomainPending 添加待验证域名（后台轮询 MX 记录）
func (s *Store) AddDomainPending(ctx context.Context, domain string) (*model.Domain, error) {
	canonicalDomain, domainType, baseDomain := NormalizeDomain(domain)
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status)
		 VALUES ($1, $2, $3, FALSE, FALSE, FALSE, 'pending')
		 ON CONFLICT (domain) DO UPDATE
		   SET status = CASE WHEN domains.is_active THEN 'active' ELSE 'pending' END,
		       is_active = domains.is_active,
		       domain_type = EXCLUDED.domain_type,
		       base_domain = EXCLUDED.base_domain
		 RETURNING id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at`,
		canonicalDomain, domainType, baseDomain,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) ListDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

func (s *Store) GetActiveDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE is_active = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

func (s *Store) GetRandomActiveDomain(ctx context.Context) (*model.Domain, error) {
	return s.GetRandomActiveDomainByCapability(ctx, DomainCapabilitySingle)
}

func (s *Store) GetRandomActiveDomainByType(ctx context.Context, domainType string) (*model.Domain, error) {
	if domainType == DomainTypeWildcard {
		return s.GetRandomActiveDomainByCapability(ctx, DomainCapabilityWildcard)
	}
	if domainType == DomainTypeExact {
		return s.GetRandomActiveDomainByCapability(ctx, DomainCapabilitySingle)
	}
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE is_active = TRUE AND domain_type = $1
		 ORDER BY RANDOM() LIMIT 1`,
		domainType,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) GetRandomActiveDomainByCapability(ctx context.Context, capability string) (*model.Domain, error) {
	condition := "supports_single = TRUE"
	if capability == DomainCapabilityWildcard {
		condition = "supports_wildcard = TRUE"
	}
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE is_active = TRUE AND %s
		 ORDER BY RANDOM() LIMIT 1`, condition),
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// GetDomainByName 按域名字符串查找活跃域名，供创建邮箱时指定域名使用
func (s *Store) GetDomainByName(ctx context.Context, domain string) (*model.Domain, error) {
	canonicalDomain, _, baseDomain := NormalizeDomain(domain)
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains
		 WHERE is_active = TRUE AND (domain = $1 OR base_domain = $2)
		 ORDER BY CASE WHEN domain = $1 THEN 0 ELSE 1 END
		 LIMIT 1`,
		canonicalDomain, baseDomain,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) GetDomainByID(ctx context.Context, domainID int) (*model.Domain, error) {
	var d model.Domain
	err := s.pool.QueryRow(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE id = $1`,
		domainID,
	).Scan(&d.ID, &d.Domain, &d.DomainType, &d.BaseDomain, &d.SupportsSingle, &d.SupportsWildcard, &d.IsActive, &d.Status, &d.CreatedAt, &d.MxCheckedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// ListPendingDomains 返回所有待验证域名
func (s *Store) ListPendingDomains(ctx context.Context) ([]model.Domain, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, domain, domain_type, base_domain, supports_single, supports_wildcard, is_active, status, created_at, mx_checked_at
		 FROM domains WHERE status = 'pending'
		 ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[model.Domain])
}

func (s *Store) UpdateDomainCapabilities(ctx context.Context, domainID int, supportsSingle, supportsWildcard bool) error {
	active := supportsSingle || supportsWildcard
	status := "pending"
	if active {
		status = "active"
	}
	domainType := DomainTypeExact
	if supportsWildcard {
		domainType = DomainTypeWildcard
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE domains
		 SET supports_single = $1,
		     supports_wildcard = $2,
		     is_active = $3,
		     status = $4,
		     domain_type = $5,
		     mx_checked_at = NOW()
		 WHERE id = $6`,
		supportsSingle, supportsWildcard, active, status, domainType, domainID)
	return err
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
		`UPDATE domains
		 SET is_active = FALSE,
		     status = 'disabled',
		     supports_single = FALSE,
		     supports_wildcard = FALSE,
		     mx_checked_at = NOW()
		 WHERE id = $1`,
		domainID)
	return err
}

func (s *Store) DeleteDomain(ctx context.Context, domainID int) error {
	var mailboxCount int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM mailboxes WHERE domain_id = $1`,
		domainID,
	).Scan(&mailboxCount); err != nil {
		return err
	}
	if mailboxCount > 0 {
		return &DomainInUseError{MailboxCount: mailboxCount}
	}

	tag, err := s.pool.Exec(ctx, `DELETE FROM domains WHERE id = $1`, domainID)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

func (s *Store) ToggleDomain(ctx context.Context, domainID int, active bool) error {
	status := "disabled"
	if active {
		status = "active"
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE domains
		 SET is_active = $1,
		     status = $2,
		     supports_single = CASE WHEN $1 THEN supports_single ELSE FALSE END,
		     supports_wildcard = CASE WHEN $1 THEN supports_wildcard ELSE FALSE END
		 WHERE id = $3`,
		active, status, domainID)
	return err
}

// GetStats 返回全局统计数据
func (s *Store) GetStats(ctx context.Context) (*model.Stats, error) {
	var st model.Stats
	err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM mailboxes)                         AS total_mailboxes,
		  (SELECT COUNT(*) FROM mailboxes WHERE expires_at > NOW()) AS active_mailboxes,
			  COALESCE((SELECT NULLIF(value, '')::BIGINT FROM app_settings WHERE key = 'total_emails_received'), (SELECT COUNT(*) FROM emails)) AS total_emails,
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
	st.TotalEmails += pendingTotal
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

// NormalizeDomain 将用户输入规整为存储格式。
// "*.example.com" 作为 wildcard 记录存储，base_domain 为 "example.com"。
func NormalizeDomain(domain string) (canonicalDomain, domainType, baseDomain string) {
	d := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(domain, ".")))
	if strings.HasPrefix(d, "*.") {
		base := strings.TrimPrefix(d, "*.")
		return "*." + base, DomainTypeWildcard, base
	}
	return d, DomainTypeExact, d
}

func DomainMXLookupName(domain string) string {
	_, domainType, baseDomain := NormalizeDomain(domain)
	if domainType == DomainTypeWildcard {
		return "mx-check.gmail.outlook.mail.com.net." + baseDomain
	}
	return baseDomain
}

type MXCheckDetail struct {
	Kind    string   `json:"kind"`
	Name    string   `json:"name"`
	Matched bool     `json:"matched"`
	MXHosts []string `json:"mx_hosts"`
	Status  string   `json:"status"`
}

// CheckDomainMX 检测域名MX记录是否指向指定服务器IP。
// 域名激活只要求单域名或通配子域名其中一种能力可用。
func CheckDomainMX(domain, serverIP string) (matched bool, mxHosts []string, status string) {
	matched, details, status := CheckDomainMXDetails(domain, serverIP)
	seen := make(map[string]bool)
	for _, detail := range details {
		for _, host := range detail.MXHosts {
			if !seen[host] {
				seen[host] = true
				mxHosts = append(mxHosts, host)
			}
		}
	}
	return matched, mxHosts, status
}

func CheckDomainMXDetails(domain, serverIP string) (matched bool, details []MXCheckDetail, status string) {
	_, _, baseDomain := NormalizeDomain(domain)
	targets := []MXCheckDetail{
		{Kind: "single", Name: baseDomain},
		{Kind: "wildcard", Name: DomainMXLookupName("*." + baseDomain)},
	}

	anyMatched := false
	statuses := make([]string, 0, len(targets))
	for _, target := range targets {
		detail := checkMXName(target.Kind, target.Name, serverIP)
		if detail.Matched {
			anyMatched = true
		}
		statuses = append(statuses, fmt.Sprintf("%s: %s", detail.Name, detail.Status))
		details = append(details, detail)
	}
	return anyMatched, details, strings.Join(statuses, "；")
}

func DomainCapabilitiesFromMX(details []MXCheckDetail) (supportsSingle, supportsWildcard bool) {
	for _, detail := range details {
		switch detail.Kind {
		case "single", "exact", "base":
			if detail.Matched {
				supportsSingle = true
			}
		case "wildcard":
			if detail.Matched {
				supportsWildcard = true
			}
		}
	}
	return supportsSingle, supportsWildcard
}

func checkMXName(kind, lookupName, serverIP string) MXCheckDetail {
	detail := MXCheckDetail{Kind: kind, Name: lookupName}
	mxRecords, err := net.LookupMX(lookupName)
	if err != nil {
		detail.Status = fmt.Sprintf("DNS查询失败: %v", err)
		return detail
	}
	if len(mxRecords) == 0 {
		detail.Status = "未找到MX记录"
		return detail
	}
	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")
		detail.MXHosts = append(detail.MXHosts, host)
		addrs, err := net.LookupHost(host)
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			if addr == serverIP {
				detail.Matched = true
				detail.Status = fmt.Sprintf("MX记录匹配：%s → %s", host, addr)
				return detail
			}
		}
	}
	detail.Status = fmt.Sprintf("MX记录(%s)未指向本服务器(%s)", strings.Join(detail.MXHosts, ","), serverIP)
	return detail
}

// ==================== Email ====================

// DeliveredEmail is an internal persistence result. Ordinal is one-based and
// matches the recipient's position in the request. Unknown recipients are
// returned with Delivered=false so the API can preserve its response order.
type DeliveredEmail struct {
	Ordinal   int64
	Recipient string
	EmailID   uuid.UUID
	MailboxID uuid.UUID
	Delivered bool
}

func (s *Store) InsertEmail(ctx context.Context, mailboxID uuid.UUID, sender, subject, bodyText, bodyHTML, raw string) (*model.Email, error) {
	e := model.Email{
		MailboxID:  mailboxID,
		Sender:     sender,
		Subject:    subject,
		BodyText:   bodyText,
		BodyHTML:   bodyHTML,
		RawMessage: raw,
		SizeBytes:  len(raw),
	}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO emails (mailbox_id, sender, subject, body_text, body_html, raw_message, size_bytes)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id, received_at`,
		mailboxID, sender, subject, bodyText, bodyHTML, raw, len(raw),
	).Scan(&e.ID, &e.ReceivedAt)
	if err != nil {
		return nil, err
	}
	// 热点累加放入内存计数器，由 RunStatsFlusher 异步 flush，避免高并发争抢同一行。
	s.counters.inc(mailboxID)
	return &e, nil
}

// DeliverEmails atomically resolves recipients and persists all matching
// deliveries in one PostgreSQL statement. It preserves input order, original
// recipient spelling, and duplicate recipients for API compatibility.
func (s *Store) DeliverEmails(ctx context.Context, recipients []string, sender, subject, bodyText, bodyHTML, raw string) ([]DeliveredEmail, error) {
	if len(recipients) == 0 {
		return []DeliveredEmail{}, nil
	}

	rows, err := s.pool.Query(ctx, `
		WITH input AS MATERIALIZED (
			SELECT recipient, ordinality::BIGINT AS ordinal
			FROM unnest($1::TEXT[]) WITH ORDINALITY AS u(recipient, ordinality)
		),
		matched AS MATERIALIZED (
			SELECT
				i.ordinal,
				i.recipient,
				m.id AS mailbox_id,
				gen_random_uuid() AS email_id
			FROM input AS i
			JOIN mailboxes AS m ON m.full_address = lower(i.recipient)
		),
		inserted AS (
			INSERT INTO emails (id, mailbox_id, sender, subject, body_text, body_html, raw_message, size_bytes)
			SELECT email_id, mailbox_id, $2, $3, $4, $5, $6, $7
			FROM matched
			ORDER BY ordinal
			RETURNING id
		)
		SELECT
			i.ordinal,
			i.recipient,
			COALESCE(m.email_id, '00000000-0000-0000-0000-000000000000'::UUID),
			COALESCE(m.mailbox_id, '00000000-0000-0000-0000-000000000000'::UUID),
			(ins.id IS NOT NULL) AS delivered
		FROM input AS i
		LEFT JOIN matched AS m ON m.ordinal = i.ordinal
		LEFT JOIN inserted AS ins ON ins.id = m.email_id
		ORDER BY i.ordinal`,
		recipients, sender, subject, bodyText, bodyHTML, raw, len(raw),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DeliveredEmail, 0, len(recipients))
	for rows.Next() {
		var result DeliveredEmail
		if err := rows.Scan(
			&result.Ordinal,
			&result.Recipient,
			&result.EmailID,
			&result.MailboxID,
			&result.Delivered,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(results) != len(recipients) {
		return nil, fmt.Errorf("delivery result count mismatch: got %d, want %d", len(results), len(recipients))
	}

	deliveredMailboxIDs := make([]uuid.UUID, 0, len(results))
	for _, result := range results {
		if result.Delivered {
			deliveredMailboxIDs = append(deliveredMailboxIDs, result.MailboxID)
		}
	}
	s.counters.incMany(deliveredMailboxIDs)
	return results, nil
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
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			s.flushCounters(flushCtx)
			cancel()
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

	mailboxIDs := make([]uuid.UUID, 0, len(mailboxes))
	for id := range mailboxes {
		mailboxIDs = append(mailboxIDs, id)
	}
	sort.Slice(mailboxIDs, func(i, j int) bool {
		return strings.Compare(mailboxIDs[i].String(), mailboxIDs[j].String()) < 0
	})
	type mailboxDelta struct {
		ID    string `json:"id"`
		Value int64  `json:"value"`
	}
	deltas := make([]mailboxDelta, len(mailboxIDs))
	for i, id := range mailboxIDs {
		deltas[i] = mailboxDelta{ID: id.String(), Value: mailboxes[id]}
	}
	deltasJSON, err := json.Marshal(deltas)

	var tx pgx.Tx
	if err == nil {
		tx, err = s.pool.Begin(ctx)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `
			INSERT INTO app_settings (key, value) VALUES ('total_emails_received', $1::TEXT)
			ON CONFLICT (key) DO UPDATE
			SET value = ((COALESCE(NULLIF(app_settings.value, ''), '0')::BIGINT + EXCLUDED.value::BIGINT)::TEXT), updated_at = NOW()
		`, total)
	}
	if err == nil && len(mailboxIDs) > 0 {
		_, err = tx.Exec(ctx, `
			UPDATE mailboxes AS m
			SET received_email_count = m.received_email_count + delta.value
			FROM jsonb_to_recordset($1::JSONB) AS delta(id UUID, value BIGINT)
			WHERE m.id = delta.id`,
			string(deltasJSON),
		)
	}
	if err == nil {
		err = tx.Commit(ctx)
	} else if tx != nil {
		_ = tx.Rollback(ctx)
	}
	if err == nil {
		return
	}

	// The transaction is all-or-nothing. Restore the entire drained snapshot
	// exactly once so a transient database failure cannot lose or double-count.
	s.counters.mu.Lock()
	s.counters.total += total
	if s.counters.mailbox == nil {
		s.counters.mailbox = make(map[uuid.UUID]int64, len(mailboxes))
	}
	for id, n := range mailboxes {
		s.counters.mailbox[id] += n
	}
	s.counters.mu.Unlock()
	if ctx.Err() == nil {
		// Avoid noisy shutdown logs while still surfacing live flush failures.
		log.Printf("[stats] flush failed: %v", err)
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

func RandomBool() bool {
	n, _ := rand.Int(rand.Reader, big.NewInt(2))
	return n.Int64() == 1
}

func GenerateMultiLevelDomain(baseDomain string) string {
	providers := []string{"gmail", "outlook", "hotmail", "yahoo", "icloud", "proton", "aol", "zoho"}
	mailWords := []string{"mail", "email", "inbox", "smtp", "mx", "webmail", "post"}
	tlds := []string{"com", "net", "org", "io", "co", "app", "dev"}
	pools := [][]string{providers, mailWords, tlds}

	pick := func(values []string) string {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(values))))
		return values[n.Int64()]
	}
	pickPool := func() []string {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(pools))))
		return pools[n.Int64()]
	}

	levelOffset, _ := rand.Int(rand.Reader, big.NewInt(5))
	labelCount := 10 + int(levelOffset.Int64())
	labels := make([]string, 0, labelCount)
	for len(labels) < labelCount {
		labels = append(labels, pick(pickPool()))
	}
	return strings.Join(labels, ".") + "." + strings.ToLower(strings.TrimSpace(baseDomain))
}

func JoinCustomSubdomain(subdomain, baseDomain string) (string, error) {
	labels := strings.Split(strings.ToLower(strings.Trim(strings.TrimSpace(subdomain), ".")), ".")
	if len(labels) == 0 {
		return "", fmt.Errorf("subdomain is required")
	}
	for _, label := range labels {
		if !domainLabelPattern.MatchString(label) {
			return "", fmt.Errorf("invalid subdomain label: %s", label)
		}
	}
	return strings.Join(labels, ".") + "." + strings.ToLower(strings.TrimSpace(baseDomain)), nil
}
