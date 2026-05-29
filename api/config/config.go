package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port                string
	DBDSN               string
	RedisAddr           string
	RedisPassword       string
	RateLimit           int
	RateWindow          int    // seconds
	SMTPServerIP        string // 仅从 SMTP_SERVER_IP 环境变量读取
	SMTPHostname        string // 邮件服务器场指向的 hostname，不硬编码
	LinuxDOClientID     string
	LinuxDOClientSecret string
	LinuxDORedirectURL  string
	GitHubClientID      string
	GitHubClientSecret  string
	GitHubRedirectURL   string
}

func Load() *Config {
	rl, _ := strconv.Atoi(getEnv("RATE_LIMIT", "500"))
	rw, _ := strconv.Atoi(getEnv("RATE_WINDOW", "60"))

	return &Config{
		// ★ PORT：API 容器内监听端口，默认 8080。
		// 由 .env 中的 API_PORT 注入。修改此端口后需同步：
		//   1. .env / .env.example 的 API_PORT
		//   2. docker-compose.yml api.ports 右边数字
		//   3. nginx/default.conf 所有 proxy_pass http://api:8080
		//   4. postfix/entrypoint.sh curl http://api:8080
		//   5. postfix/mail-receiver.py API_URL 默认值
		Port:  getEnv("PORT", "8080"),
		DBDSN: getEnv("DB_DSN", ""),
		// ★ RedisAddr：Redis 容器内部地址，格式 "host:port"。
		// 默认 "redis:6379"，"redis" 是 Docker 内部服务名，不需要修改。
		RedisAddr:           getEnv("REDIS_ADDR", "redis:6379"),
		RedisPassword:       getEnv("REDIS_PASSWORD", ""),
		RateLimit:           rl,
		RateWindow:          rw,
		SMTPServerIP:        os.Getenv("SMTP_SERVER_IP"),
		SMTPHostname:        os.Getenv("SMTP_HOSTNAME"),
		LinuxDOClientID:     os.Getenv("LINUXDO_CLIENT_ID"),
		LinuxDOClientSecret: os.Getenv("LINUXDO_CLIENT_SECRET"),
		LinuxDORedirectURL:  os.Getenv("LINUXDO_REDIRECT_URL"),
		GitHubClientID:      os.Getenv("GITHUB_CLIENT_ID"),
		GitHubClientSecret:  os.Getenv("GITHUB_CLIENT_SECRET"),
		GitHubRedirectURL:   os.Getenv("GITHUB_REDIRECT_URL"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
