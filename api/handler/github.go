package handler

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tempmail/store"

	"github.com/gin-gonic/gin"
)

const (
	gitHubAuthURL     = "https://github.com/login/oauth/authorize"
	gitHubTokenURL    = "https://github.com/login/oauth/access_token"
	gitHubUserInfoURL = "https://api.github.com/user"
	gitHubStateCookie = "tm_github_state"
)

type GitHubHandler struct {
	store           *store.Store
	envClientID     string
	envClientSecret string
	envRedirectURL  string
	httpClient      *http.Client
}

func NewGitHubHandler(s *store.Store, clientID, clientSecret, redirectURL string) *GitHubHandler {
	return &GitHubHandler{
		store:           s,
		envClientID:     clientID,
		envClientSecret: clientSecret,
		envRedirectURL:  redirectURL,
		httpClient:      &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *GitHubHandler) Start(c *gin.Context) {
	if !h.loginEnabled(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub login is disabled"})
		return
	}
	cfg := h.oauthConfig(c)
	if cfg.clientID == "" || cfg.clientSecret == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub OAuth config is incomplete"})
		return
	}
	state, err := randomState()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create oauth state"})
		return
	}
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(gitHubStateCookie, state, 600, "/", "", false, true)

	q := url.Values{}
	q.Set("client_id", cfg.clientID)
	q.Set("redirect_uri", cfg.redirectURL)
	q.Set("response_type", "code")
	q.Set("scope", "read:user")
	q.Set("state", state)
	q.Set("allow_signup", "true")
	c.Redirect(http.StatusFound, gitHubAuthURL+"?"+q.Encode())
}

func (h *GitHubHandler) Callback(c *gin.Context) {
	if !h.loginEnabled(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub login is disabled"})
		return
	}
	cfg := h.oauthConfig(c)
	if cfg.clientID == "" || cfg.clientSecret == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "GitHub OAuth config is incomplete"})
		return
	}
	state := c.Query("state")
	cookieState, err := c.Cookie(gitHubStateCookie)
	if err != nil || state == "" || subtle.ConstantTimeCompare([]byte(state), []byte(cookieState)) != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid oauth state"})
		return
	}
	c.SetCookie(gitHubStateCookie, "", -1, "/", "", false, true)

	if oauthErr := c.Query("error"); oauthErr != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "GitHub OAuth error: " + oauthErr})
		return
	}
	code := c.Query("code")
	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing oauth code"})
		return
	}

	token, err := h.exchangeCode(code, cfg)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	profile, err := h.fetchUserInfo(token)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	gitHubID := profile.IDString()
	if gitHubID == "" {
		c.JSON(http.StatusBadGateway, gin.H{"error": "GitHub profile missing id"})
		return
	}

	username := gitHubUsername(profile)
	account, err := h.store.GetOrCreateGitHubAccount(c.Request.Context(), gitHubID, username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	apiKeyJSON, _ := json.Marshal(account.APIKey)
	accountJSON, _ := json.Marshal(gin.H{
		"id":         account.ID,
		"username":   account.Username,
		"is_admin":   account.IsAdmin,
		"created_at": account.CreatedAt,
	})
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, fmt.Sprintf(`<!doctype html><html><body><script>
localStorage.setItem('tm_apikey', %s);
localStorage.setItem('tm_account', %s);
location.replace('/');
</script></body></html>`, string(apiKeyJSON), string(accountJSON)))
}

func (h *GitHubHandler) loginEnabled(c *gin.Context) bool {
	enabled, err := h.store.GetSetting(c.Request.Context(), "github_login_enabled")
	return err == nil && enabled == "true"
}

type gitHubOAuthConfig struct {
	clientID     string
	clientSecret string
	redirectURL  string
}

func (h *GitHubHandler) oauthConfig(c *gin.Context) gitHubOAuthConfig {
	clientID, _ := h.store.GetSetting(c.Request.Context(), "github_client_id")
	clientSecret, _ := h.store.GetSetting(c.Request.Context(), "github_client_secret")
	redirectURL, _ := h.store.GetSetting(c.Request.Context(), "github_redirect_url")
	if clientID == "" {
		clientID = h.envClientID
	}
	if clientSecret == "" {
		clientSecret = h.envClientSecret
	}
	if redirectURL == "" {
		redirectURL = h.envRedirectURL
	}
	if redirectURL == "" {
		redirectURL = requestBaseURL(c) + "/public/auth/github/callback"
	}
	return gitHubOAuthConfig{clientID: clientID, clientSecret: clientSecret, redirectURL: redirectURL}
}

func (h *GitHubHandler) exchangeCode(code string, cfg gitHubOAuthConfig) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", cfg.redirectURL)
	form.Set("client_id", cfg.clientID)
	form.Set("client_secret", cfg.clientSecret)

	req, err := http.NewRequest(http.MethodPost, gitHubTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "TempMail")
	res, err := h.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("GitHub token exchange failed: HTTP %d", res.StatusCode)
	}
	var data struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	if data.Error != "" {
		if data.Description != "" {
			return "", fmt.Errorf("GitHub token exchange failed: %s", data.Description)
		}
		return "", fmt.Errorf("GitHub token exchange failed: %s", data.Error)
	}
	if data.AccessToken == "" {
		return "", fmt.Errorf("GitHub token exchange returned no access_token")
	}
	return data.AccessToken, nil
}

func (h *GitHubHandler) fetchUserInfo(token string) (*gitHubProfile, error) {
	req, err := http.NewRequest(http.MethodGet, gitHubUserInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "TempMail")
	res, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("GitHub userinfo failed: HTTP %d", res.StatusCode)
	}
	var profile gitHubProfile
	if err := json.Unmarshal(body, &profile); err != nil {
		return nil, err
	}
	return &profile, nil
}

type gitHubProfile struct {
	ID    json.RawMessage `json:"id"`
	Login string          `json:"login"`
	Name  string          `json:"name"`
}

func gitHubUsername(profile *gitHubProfile) string {
	for _, v := range []string{profile.Login, profile.Name, "github_" + profile.IDString()} {
		v = strings.TrimSpace(v)
		if v != "" {
			return normalizeUsernameWithFallback(v, "github_user")
		}
	}
	return "github_user"
}

func (p *gitHubProfile) IDString() string {
	var s string
	if err := json.Unmarshal(p.ID, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	decoder := json.NewDecoder(strings.NewReader(string(p.ID)))
	decoder.UseNumber()
	if err := decoder.Decode(&n); err == nil {
		return n.String()
	}
	return strings.TrimSpace(string(p.ID))
}
