package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tempmail/config"
	"tempmail/handler"
	"tempmail/middleware"
	"tempmail/store"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()

	// ==================== 连接数据库 ====================
	ctx := context.Background()
	db, err := store.New(ctx, cfg.DBDSN)
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	defer db.Close()
	log.Println("✓ Database connected")

	// ==================== 连接 Redis ====================
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           0,
		PoolSize:     0, // 0 = 不限（自动按 CPU 核心数 * 10）
		MinIdleConns: 20,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect redis: %v", err)
	}
	defer rdb.Close()
	log.Println("✓ Redis connected")

	// ==================== Gin 路由 ====================
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// CORS：允许前端跨域访问
	r.Use(cors.New(cors.Config{
		AllowOrigins:  []string{"*"},
		AllowMethods:  []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:  []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders: []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
		MaxAge:        12 * time.Hour,
	}))

	// 健康检查（无需认证）
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "time": time.Now().Unix()})
	})

	// 初始化 handlers
	accountH := handler.NewAccountHandler(db)
	domainH := handler.NewDomainHandler(db, cfg.SMTPServerIP, cfg.SMTPHostname)
	mailboxH := handler.NewMailboxHandler(db)
	emailH := handler.NewEmailHandler(db)
	settingH := handler.NewSettingHandler(db)
	registerH := handler.NewRegisterHandler(db)
	statsH := handler.NewStatsHandler(db)
	linuxDOH := handler.NewLinuxDOHandler(db, cfg.LinuxDOClientID, cfg.LinuxDOClientSecret, cfg.LinuxDORedirectURL)
	gitHubH := handler.NewGitHubHandler(db, cfg.GitHubClientID, cfg.GitHubClientSecret, cfg.GitHubRedirectURL)

	// 公开路由（无需认证）
	public := r.Group("/public")
	{
		public.GET("/settings", settingH.GetPublic)
		public.POST("/key-login", accountH.KeyLogin)
		public.POST("/register", registerH.Register)
		public.GET("/stats", statsH.Get)
		public.GET("/auth/linuxdo", linuxDOH.Start)
		public.GET("/auth/linuxdo/callback", linuxDOH.Callback)
		public.GET("/auth/github", gitHubH.Start)
		public.GET("/auth/github/callback", gitHubH.Callback)
	}

	// API 路由组（需要认证 + 速率限制）
	api := r.Group("/api")
	api.Use(middleware.Auth(db))
	api.Use(middleware.RateLimit(rdb, cfg.RateLimit, cfg.RateWindow))
	{
		// 当前用户
		api.GET("/me", accountH.Me)

		// 域名池（所有用户可查看）
		api.GET("/domains", domainH.List)
		api.GET("/domains/:id/status", domainH.GetStatus) // 任意用户可轮询域名状态
		api.GET("/stats", statsH.Get)
		// 任意已登录用户可提交域名进行 MX 自动验证
		api.POST("/domains/submit", domainH.Submit)

		// 邮箱管理
		api.POST("/mailboxes", mailboxH.Create)
		api.GET("/mailboxes", mailboxH.List)
		api.DELETE("/mailboxes/:id", mailboxH.Delete)

		// 邮件管理
		api.GET("/mailboxes/:id/emails", emailH.List)
		api.GET("/mailboxes/:id/emails/:email_id", emailH.Get)
		api.DELETE("/mailboxes/:id/emails/:email_id", emailH.Delete)
		// 管理员路由
		admin := api.Group("/admin")
		admin.Use(middleware.AdminOnly())
		{
			admin.POST("/accounts", accountH.Create)
			admin.GET("/accounts", accountH.List)
			admin.DELETE("/accounts/:id", accountH.Delete)

			admin.POST("/domains", domainH.Add)
			admin.DELETE("/domains/:id", domainH.Delete)
			admin.PUT("/domains/:id/toggle", domainH.Toggle)
			admin.POST("/domains/mx-import", domainH.MXImport)
			admin.POST("/domains/mx-register", domainH.MXRegister)
			admin.GET("/domains/:id/status", domainH.GetStatus)

			// 系统设置管理
			admin.GET("/settings", settingH.AdminGetAll)
			admin.PUT("/settings", settingH.AdminUpdate)
		}
	}

	// 内部邮件投递接口（Postfix pipe 调用，仅内部网络）
	internal := r.Group("/internal")
	{
		// 域名列表（供 Postfix 同步）
		internal.GET("/domains", func(c *gin.Context) {
			domains, err := db.ListDomains(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"domains": domains})
		})

		internal.POST("/deliver", func(c *gin.Context) {
			var req struct {
				Recipient string `json:"recipient" binding:"required"`
				Sender    string `json:"sender"`
				Subject   string `json:"subject"`
				BodyText  string `json:"body_text"`
				BodyHTML  string `json:"body_html"`
				Raw       string `json:"raw"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			// 查找收件邮箱
			mailbox, err := db.GetMailboxByFullAddress(c.Request.Context(), req.Recipient)
			if err != nil {
				// 邮箱不存在，静默丢弃
				c.JSON(http.StatusOK, gin.H{"status": "discarded", "reason": "unknown recipient"})
				return
			}

			// 存储邮件
			email, err := db.InsertEmail(c.Request.Context(),
				mailbox.ID, req.Sender, req.Subject, req.BodyText, req.BodyHTML, req.Raw)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			c.JSON(http.StatusOK, gin.H{"status": "delivered", "email_id": email.ID})
		})

		// 批量投递：一次 HTTP 调用写入多个收件人。
		// 用于 LMTP 守护进程把同一会话的多个 RCPT 合并提交。
		// 任意一条失败会返回 5xx，调用方应当重试整批，避免丢邮件。
		internal.POST("/deliver-batch", func(c *gin.Context) {
			var req struct {
				Recipients []string `json:"recipients" binding:"required,min=1"`
				Sender     string   `json:"sender"`
				Subject    string   `json:"subject"`
				BodyText   string   `json:"body_text"`
				BodyHTML   string   `json:"body_html"`
				Raw        string   `json:"raw"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			results := make([]gin.H, 0, len(req.Recipients))
			for _, rcpt := range req.Recipients {
				mailbox, err := db.GetMailboxByFullAddress(c.Request.Context(), rcpt)
				if err != nil {
					results = append(results, gin.H{"recipient": rcpt, "status": "discarded", "reason": "unknown recipient"})
					continue
				}
				email, err := db.InsertEmail(c.Request.Context(),
					mailbox.ID, req.Sender, req.Subject, req.BodyText, req.BodyHTML, req.Raw)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "recipient": rcpt})
					return
				}
				results = append(results, gin.H{"recipient": rcpt, "status": "delivered", "email_id": email.ID})
			}
			c.JSON(http.StatusOK, gin.H{"results": results})
		})
	}

	// ==================== 邮箱自动过期清理 ====================
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		log.Println("✓ Mailbox expiry cleaner started (TTL=30min, interval=1min)")
		for range ticker.C {
			if deleted, err := db.DeleteExpiredMailboxes(context.Background()); err != nil {
				log.Printf("[cleaner] error: %v", err)
			} else if deleted > 0 {
				log.Printf("[cleaner] deleted %d expired mailboxes", deleted)
			}
		}
	}()

	// ==================== 邮件统计计数器后台 flush ====================
	go db.RunStatsFlusher(ctx, time.Second)
	log.Println("✓ Email stats flusher started (interval=1s)")

	// ==================== MX 自动验证轮询 ====================
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		log.Println("✓ MX domain verifier started (pending check=30s, active re-check=6h)")
		reCheckTicker := time.NewTicker(6 * time.Hour)
		defer reCheckTicker.Stop()
		for {
			select {
			case <-ticker.C:
				// 处理待验证域名
				pendingDomains, err := db.ListPendingDomains(context.Background())
				if err != nil {
					log.Printf("[mx-verifier] list pending error: %v", err)
					continue
				}
				if len(pendingDomains) == 0 {
					continue
				}
				serverIP := cfg.SMTPServerIP
				if serverIP == "" {
					serverIP, _ = db.GetSetting(context.Background(), "smtp_server_ip")
				}
				for _, d := range pendingDomains {
					matched, _, mxStatus := store.CheckDomainMX(d.Domain, serverIP)
					db.TouchDomainCheckTime(context.Background(), d.ID)
					if matched {
						if err := db.PromoteDomainToActive(context.Background(), d.ID); err != nil {
							log.Printf("[mx-verifier] promote %s error: %v", d.Domain, err)
						} else {
							log.Printf("[mx-verifier] ✓ %s MX verified, domain activated", d.Domain)
						}
					} else {
						log.Printf("[mx-verifier] waiting: %s — %s", d.Domain, mxStatus)
					}
				}

			case <-reCheckTicker.C:
				// 每 6 小时重新检测所有已激活域名，MX 失效则自动停用
				activeDomains, err := db.GetActiveDomains(context.Background())
				if err != nil {
					log.Printf("[mx-recheck] list active error: %v", err)
					continue
				}
				serverIP := cfg.SMTPServerIP
				if serverIP == "" {
					serverIP, _ = db.GetSetting(context.Background(), "smtp_server_ip")
				}
				log.Printf("[mx-recheck] checking %d active domains", len(activeDomains))
				for _, d := range activeDomains {
					matched, _, mxStatus := store.CheckDomainMX(d.Domain, serverIP)
					db.TouchDomainCheckTime(context.Background(), d.ID)
					if !matched {
						if err := db.DisableDomainMX(context.Background(), d.ID); err != nil {
							log.Printf("[mx-recheck] disable %s error: %v", d.Domain, err)
						} else {
							log.Printf("[mx-recheck] ⚠ %s MX no longer valid (%s), domain disabled", d.Domain, mxStatus)
						}
					}
				}
			}
		}
	}()

	// ==================== 写出管理员 API Key 文件 ====================
	go func() {
		// 等待 DB 就绪后再读取（延迟 1 秒）
		time.Sleep(1 * time.Second)
		adminKey, err := db.GetAdminAPIKey(context.Background())
		if err != nil {
			log.Printf("[adminkey] could not fetch admin key: %v", err)
			return
		}
		keyFile := os.Getenv("ADMIN_KEY_FILE")
		if keyFile == "" {
			keyFile = "/data/admin.key"
		}
		if err := os.MkdirAll(filepath.Dir(keyFile), 0700); err == nil {
			content := "# TempMail Admin API Key\n# Auto-generated on startup — keep this secret!\n\nADMIN_API_KEY=" + adminKey + "\n"
			if err := os.WriteFile(keyFile, []byte(content), 0600); err != nil {
				log.Printf("[adminkey] write file error: %v", err)
			} else {
				log.Printf("✓ Admin API Key written to %s", keyFile)
			}
		}
		log.Printf("✴ ADMIN API KEY: %s", adminKey)
	}()

	// ==================== 启动服务 ====================
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("✓ API server listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server exited")
}
