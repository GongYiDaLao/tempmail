package handler

import (
	"net/http"

	"tempmail/store"

	"github.com/gin-gonic/gin"
)

type SettingHandler struct {
	store *store.Store
}

func NewSettingHandler(s *store.Store) *SettingHandler {
	return &SettingHandler{store: s}
}

// GET /public/settings → 返回前端需要的公开配置
func (h *SettingHandler) GetPublic(c *gin.Context) {
	regOpen, err := h.store.GetSetting(c.Request.Context(), "registration_open")
	if err != nil {
		regOpen = "false"
	}
	siteTitle, _ := h.store.GetSetting(c.Request.Context(), "site_title")
	siteLogoURL, _ := h.store.GetSetting(c.Request.Context(), "site_logo_url")
	smtpIP, _ := h.store.GetSetting(c.Request.Context(), "smtp_server_ip")
	smtpHostname, _ := h.store.GetSetting(c.Request.Context(), "smtp_hostname")
	announce, _ := h.store.GetSetting(c.Request.Context(), "announcement")
	keyLogin, err := h.store.GetSetting(c.Request.Context(), "key_login_enabled")
	if err != nil {
		keyLogin = "true"
	}
	linuxDOLogin, err := h.store.GetSetting(c.Request.Context(), "linuxdo_login_enabled")
	if err != nil {
		linuxDOLogin = "false"
	}
	gitHubLogin, err := h.store.GetSetting(c.Request.Context(), "github_login_enabled")
	if err != nil {
		gitHubLogin = "false"
	}
	c.JSON(http.StatusOK, gin.H{
		"registration_open":     regOpen == "true",
		"key_login_enabled":     keyLogin != "false",
		"linuxdo_login_enabled": linuxDOLogin == "true",
		"github_login_enabled":  gitHubLogin == "true",
		"site_title":            siteTitle,
		"site_logo_url":         siteLogoURL,
		"smtp_server_ip":        smtpIP,
		"smtp_hostname":         smtpHostname,
		"announcement":          announce,
	})
}

// GET /api/admin/settings → 读取所有设置（管理员）
func (h *SettingHandler) AdminGetAll(c *gin.Context) {
	settings, err := h.store.GetAllSettings(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if secret := settings["linuxdo_client_secret"]; secret != "" {
		settings["linuxdo_client_secret_set"] = "true"
		settings["linuxdo_client_secret"] = ""
	} else {
		settings["linuxdo_client_secret_set"] = "false"
	}
	if secret := settings["github_client_secret"]; secret != "" {
		settings["github_client_secret_set"] = "true"
		settings["github_client_secret"] = ""
	} else {
		settings["github_client_secret_set"] = "false"
	}
	c.JSON(http.StatusOK, settings)
}

// PUT /api/admin/settings → 更新设置（管理员）
func (h *SettingHandler) AdminUpdate(c *gin.Context) {
	var req map[string]string
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 白名单：已知配置项
	allowed := map[string]bool{
		"registration_open":      true,
		"rate_limit_enabled":     true,
		"max_mailboxes_per_user": true,
		"smtp_server_ip":         true,
		"smtp_hostname":          true,
		"site_title":             true,
		"site_logo_url":          true,
		"announcement":           true,
		"default_domain":         true,
		"mailbox_ttl_minutes":    true,
		"key_login_enabled":      true,
		"linuxdo_login_enabled":  true,
		"linuxdo_client_id":      true,
		"linuxdo_client_secret":  true,
		"linuxdo_redirect_url":   true,
		"github_login_enabled":   true,
		"github_client_id":       true,
		"github_client_secret":   true,
		"github_redirect_url":    true,
	}

	for k, v := range req {
		if !allowed[k] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown setting key: " + k})
			return
		}
		if (k == "linuxdo_client_secret" || k == "github_client_secret") && v == "" {
			continue
		}
		if err := h.store.SetSetting(c.Request.Context(), k, v); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "settings updated"})
}
