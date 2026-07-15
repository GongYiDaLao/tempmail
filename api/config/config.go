package config

import (
	"os"
	"strconv"
	"time"
)

const (
	deliveryBatchMaxDefault  = 64
	deliveryBatchMaxLimit    = 256
	deliveryBatchWaitDefault = time.Millisecond
	deliveryBatchWaitMin     = 100 * time.Microsecond
	deliveryBatchWaitMax     = 100 * time.Millisecond
)

type Config struct {
	Port                 string
	DBDSN                string
	DBMaxConns           int32
	DBMinConns           int32
	DeliveryBatchEnabled bool
	DeliveryBatchMax     int
	DeliveryBatchWait    time.Duration
	RedisAddr            string
	RedisPassword        string
	RateLimit            int
	RateWindow           int    // seconds
	SMTPServerIP         string // 仅从 SMTP_SERVER_IP 环境变量读取
	SMTPHostname         string // 邮件服务器场指向的 hostname，不硬编码
	LinuxDOClientID      string
	LinuxDOClientSecret  string
	LinuxDORedirectURL   string
	GitHubClientID       string
	GitHubClientSecret   string
	GitHubRedirectURL    string
}

func Load() *Config {
	rl, _ := strconv.Atoi(getEnv("RATE_LIMIT", "500"))
	rw, _ := strconv.Atoi(getEnv("RATE_WINDOW", "60"))
	dbMaxConns := getEnvInt32("DB_MAX_CONNS", 128)
	dbMinConns := getEnvInt32("DB_MIN_CONNS", 16)
	if dbMinConns > dbMaxConns {
		dbMinConns = dbMaxConns
	}
	deliveryBatchMax := getEnvInt("DELIVERY_BATCH_MAX", deliveryBatchMaxDefault)
	if deliveryBatchMax > deliveryBatchMaxLimit {
		deliveryBatchMax = deliveryBatchMaxDefault
	}
	deliveryBatchWait := getEnvDuration("DELIVERY_BATCH_WAIT", deliveryBatchWaitDefault)
	if deliveryBatchWait < deliveryBatchWaitMin || deliveryBatchWait > deliveryBatchWaitMax {
		deliveryBatchWait = deliveryBatchWaitDefault
	}

	return &Config{
		// PORT：API 容器内监听端口，默认 8080。
		// 由 .env 中的 API_PORT 注入。修改此端口后需同步：
		//   1. .env / .env.example 的 API_PORT
		//   2. docker-compose.yml api.ports 右边数字
		//   3. nginx/default.conf 所有 proxy_pass http://api:8080
		//   4. postfix/entrypoint.sh curl http://api:8080
		//   5. postfix/mail-receiver.py API_URL 默认值
		Port:                 getEnv("PORT", "8080"),
		DBDSN:                getEnv("DB_DSN", ""),
		DBMaxConns:           dbMaxConns,
		DBMinConns:           dbMinConns,
		DeliveryBatchEnabled: getEnvBool("DELIVERY_BATCH_ENABLED", true),
		DeliveryBatchMax:     deliveryBatchMax,
		DeliveryBatchWait:    deliveryBatchWait,
		// RedisAddr：Redis 容器内部地址，格式 "host:port"。
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

func getEnvBool(key string, fallback bool) bool {
	v, err := strconv.ParseBool(getEnv(key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(getEnv(key, strconv.Itoa(fallback)))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(getEnv(key, fallback.String()))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func getEnvInt32(key string, fallback int32) int32 {
	v, err := strconv.ParseInt(getEnv(key, strconv.FormatInt(int64(fallback), 10)), 10, 32)
	if err != nil || v < 1 {
		return fallback
	}
	return int32(v)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
