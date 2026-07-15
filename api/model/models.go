package model

import (
	"time"

	"github.com/google/uuid"
)

// ==================== 数据模型 ====================

type Account struct {
	ID        uuid.UUID `json:"id"`
	Username  string    `json:"username"`
	APIKey    string    `json:"api_key"`
	LinuxDOID string    `json:"linuxdo_id,omitempty"`
	IsAdmin   bool      `json:"is_admin"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AccountWithStats struct {
	Account
	MailboxCount       int64 `json:"mailbox_count"`
	ActiveMailboxCount int64 `json:"active_mailbox_count"`
	CurrentEmailCount  int64 `json:"current_email_count"`
	ReceivedEmailCount int64 `json:"received_email_count"`
}

type Domain struct {
	ID               int        `json:"id"`
	Domain           string     `json:"domain"`
	DomainType       string     `json:"domain_type"` // exact | wildcard
	BaseDomain       string     `json:"base_domain"`
	SupportsSingle   bool       `json:"supports_single"`
	SupportsWildcard bool       `json:"supports_wildcard"`
	IsActive         bool       `json:"is_active"`
	Status           string     `json:"status"` // active | pending | disabled
	CreatedAt        time.Time  `json:"created_at"`
	MxCheckedAt      *time.Time `json:"mx_checked_at,omitempty"`
}

type Stats struct {
	TotalMailboxes  int64 `json:"total_mailboxes"`
	ActiveMailboxes int64 `json:"active_mailboxes"`
	TotalEmails     int64 `json:"total_emails"`
	ActiveDomains   int64 `json:"active_domains"`
	PendingDomains  int64 `json:"pending_domains"`
	TotalAccounts   int64 `json:"total_accounts"`
}

type Mailbox struct {
	ID          uuid.UUID `json:"id"`
	AccountID   uuid.UUID `json:"account_id"`
	Address     string    `json:"address"`
	DomainID    int       `json:"domain_id"`
	FullAddress string    `json:"full_address"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Email struct {
	ID         uuid.UUID `json:"id"`
	MailboxID  uuid.UUID `json:"mailbox_id"`
	Sender     string    `json:"sender"`
	Subject    string    `json:"subject"`
	BodyText   string    `json:"body_text"`
	BodyHTML   string    `json:"body_html"`
	RawMessage string    `json:"raw_message,omitempty"`
	SizeBytes  int       `json:"size_bytes"`
	ReceivedAt time.Time `json:"received_at"`
}

// ==================== 请求/响应 ====================

type CreateAccountReq struct {
	Username string `json:"username" binding:"required,min=2,max=64"`
}

type CreateAccountResp struct {
	ID       uuid.UUID `json:"id"`
	Username string    `json:"username"`
	APIKey   string    `json:"api_key"`
}

type AddDomainReq struct {
	Domain string `json:"domain" binding:"required,fqdn"`
}

type DNSInstruction struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Value    string `json:"value"`
	Priority int    `json:"priority,omitempty"`
}

type AddDomainResp struct {
	Domain       Domain           `json:"domain"`
	DNSRecords   []DNSInstruction `json:"dns_records"`
	Instructions string           `json:"instructions"`
}

type CreateMailboxReq struct {
	Address   string `json:"address,omitempty"` // 可选，为空则随机生成
	Domain    string `json:"domain,omitempty"`
	DomainID  int    `json:"domain_id,omitempty"`
	Mode      string `json:"mode,omitempty"`      // single | multi，未传时随机
	Subdomain string `json:"subdomain,omitempty"` // multi 模式下自定义多级子域名前缀
}

type CreateMailboxResp struct {
	Mailbox Mailbox `json:"mailbox"`
}

type ListResp[T any] struct {
	Data  []T `json:"data"`
	Total int `json:"total"`
	Page  int `json:"page"`
	Size  int `json:"size"`
}

type EmailSummary struct {
	ID         uuid.UUID `json:"id"`
	Sender     string    `json:"sender"`
	Subject    string    `json:"subject"`
	SizeBytes  int       `json:"size_bytes"`
	ReceivedAt time.Time `json:"received_at"`
}
