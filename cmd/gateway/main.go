package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/alerting"
	"github.com/marktantongco/freebuff-gateway/internal/api"
	"github.com/marktantongco/freebuff-gateway/internal/middleware"
	rlimitt "github.com/marktantongco/freebuff-gateway/internal/ratelimit"
	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	_ "github.com/marktantongco/freebuff-gateway/internal/channels/freebuff"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
	"github.com/marktantongco/freebuff-gateway/internal/logrotation"
	"github.com/marktantongco/freebuff-gateway/internal/metrics"
	"github.com/marktantongco/freebuff-gateway/internal/observability"
	"github.com/marktantongco/freebuff-gateway/internal/orchestration"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/runtimeconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
	"github.com/marktantongco/freebuff-gateway/internal/transport"
	usagepkg "github.com/marktantongco/freebuff-gateway/internal/usage"
	"github.com/marktantongco/freebuff-gateway/internal/usermgmt"
	"github.com/marktantongco/freebuff-gateway/internal/websocket"
	"github.com/marktantongco/freebuff-gateway/web"
)

func main() {
	cfg := loadConfig()
	if os.Getenv("ADMIN_PASSWORD") == "" {
		log.Printf("gateway: ADMIN_PASSWORD not set; using default admin password")
	} else if cfg.AdminPassword == "admin" {
		log.Printf("gateway: using weak admin password value")
	}

	db, err := storage.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}
	defer db.Close()

	registry := channels.NewRegistry()
	if err := registry.RegisterBuiltins(); err != nil {
		log.Fatalf("channels: %v", err)
	}

	accountRepo := accounts.NewRepo(db)
	pool := accounts.NewPool(accountRepo)
	channelConfigRepo := channelconfig.NewRepo(db)
	freebuffStateRepo := freebuffstate.NewRepo(db)
	proxyPoolRepo := proxypool.NewRepo(db)
	proxyResolver := proxypool.NewResolver(proxyPoolRepo)
	authKeyRepo := authkeys.NewRepo(db)
	systemLogRepo := systemlogs.NewRepo(db)
	userRepo, err := usermgmt.NewRepo(db)
	if err != nil {
		log.Fatalf("usermgmt: %v", err)
	}
	if err := userRepo.Seed(cfg.AdminPassword); err != nil {
		log.Printf("usermgmt: seed: %v", err)
	}
	log.Printf("usermgmt: user repository initialized")
	policyResolver := runtimeconfig.NewResolver(channelConfigRepo, accountRepo)

	tp := transport.New(
		transport.WithTimeout(60*time.Second),
		transport.WithBodyPreviewBytes(8192),
		transport.WithRequestReuse(cfg.TransportRequestReuse),
	)

	sm := session.NewManager(registry, pool, tp, session.Config{
		WaitOnFull:              cfg.WaitOnFull,
		ReapInterval:            30 * time.Second,
		Resolver:                policyResolver,
		StateRecorder:           freebuffStateRepo,
		AccountMetadataResolver: proxyResolver,
		CreateLimits: session.CreateLimitConfig{
			MaxParallelGlobal:   cfg.SessionCreateMaxParallelGlobal,
			MaxParallelPerKey:   cfg.SessionCreateMaxParallelPerKey,
			MaxParallelPerModel: cfg.SessionCreateMaxParallelPerModel,
			MaxParallelPerGroup: cfg.SessionCreateMaxParallelPerGroup,
		},
	})
	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 30*time.Second)
	restoreFreeBuffSessions(restoreCtx, freeBuffRestoreDeps{
		registry:         registry,
		accountRepo:      accountRepo,
		sessions:         sm,
		transport:        tp,
		stateRepo:        freebuffStateRepo,
		metadataResolver: proxyResolver,
	})
	restoreCancel()

	logRepo := logs.NewRepo(db)
	metricsAgg := metrics.NewAggregator()
	usageRecorder := usagepkg.NewRecorder(logRepo, metricsAgg, pool)

	runner := orchestration.NewRunner(registry, sm, tp, usageRecorder)
	proxyCheckConfig := proxypool.CheckerConfig{
		ProbeURL:    cfg.ProxyHealthcheckURL,
		Interval:    cfg.ProxyHealthcheckInterval,
		Timeout:     cfg.ProxyHealthcheckTimeout,
		Concurrency: cfg.ProxyHealthcheckConcurrency,
	}
	adminHandler := api.NewAdminHandler(
		registry,
		pool,
		sm,
		logRepo,
		metricsAgg,
		tp,
		api.WithChannelConfigRepo(channelConfigRepo),
		api.WithFreeBuffStateRepo(freebuffStateRepo),
		api.WithProxyPoolRepo(proxyPoolRepo),
		api.WithProxyHealthcheckConfig(proxyCheckConfig),
		api.WithAuthKeysRepo(authKeyRepo),
		api.WithSystemLogsRepo(systemLogRepo),
	)
	proxyHandler := api.NewProxyHandler(registry, runner)
	adminAuth := api.NewAdminAuthenticator(cfg.AdminPassword, cfg.AdminSessionTTL)
	adminAuth.SetUserRepo(userRepo)
	apiKeyAuth := api.NewAPIKeyAuthenticator(authKeyRepo)
	userMgmtHandler := api.NewUserManagementHandler(userRepo)

	// ─── Alerting System ────────────────────────────────────────
	healthChecker := observability.NewHealthChecker("1.0.0")

	alertCfg := alerting.DefaultAlertConfig()
	alertCfg.CheckInterval = 30 * time.Second
	alertCfg.CooldownPeriod = 5 * time.Minute
	alertCfg.MaxRetries = 3
	alertCfg.StaleAfter = 1 * time.Hour
	alertManager := alerting.NewManager(alertCfg, nil) // nil = nopLogger

	// Register notification channels from env
	if webhookURL := os.Getenv("ALERT_WEBHOOK_URL"); webhookURL != "" {
		alertManager.AddNotifier(alerting.NewWebhookNotifier("webhook", webhookURL, nil))
		log.Printf("alerting: webhook channel enabled → %s", webhookURL)
	}
	if slackURL := os.Getenv("ALERT_SLACK_URL"); slackURL != "" {
		alertManager.AddNotifier(alerting.NewSlackNotifier("slack", slackURL, ""))
		log.Printf("alerting: slack channel enabled")
	}
	if token := os.Getenv("ALERT_TELEGRAM_TOKEN"); token != "" {
		chatID := os.Getenv("ALERT_TELEGRAM_CHAT_ID")
		alertManager.AddNotifier(alerting.NewTelegramNotifier("telegram", token, chatID))
		log.Printf("alerting: telegram channel enabled → chat %s", chatID)
	}
	if discordURL := os.Getenv("ALERT_DISCORD_URL"); discordURL != "" {
		alertManager.AddNotifier(alerting.NewDiscordNotifier("discord", discordURL))
		log.Printf("alerting: discord channel enabled")
	}
	if pdKey := os.Getenv("ALERT_PAGERDUTY_KEY"); pdKey != "" {
		alertManager.AddNotifier(alerting.NewPagerDutyNotifier("pagerduty", pdKey))
		log.Printf("alerting: pagerduty channel enabled")
	}
	if smtpAddr := os.Getenv("ALERT_EMAIL_SMTP"); smtpAddr != "" {
		emailTo := strings.Split(os.Getenv("ALERT_EMAIL_TO"), ",")
		emailCC := strings.Split(os.Getenv("ALERT_EMAIL_CC"), ",")
		emailBCC := strings.Split(os.Getenv("ALERT_EMAIL_BCC"), ",")
		emailFrom := os.Getenv("ALERT_EMAIL_FROM")
		if emailFrom == "" {
			emailFrom = "alerts@freebuff-gateway"
		}
		notify := alerting.NewEmailNotifier(alerting.EmailConfig{
			Name:     "email",
			SMTPAddr: smtpAddr,
			Username: os.Getenv("ALERT_EMAIL_USERNAME"),
			Password: os.Getenv("ALERT_EMAIL_PASSWORD"),
			From:     emailFrom,
			To:       emailTo,
			CC:       emailCC,
			BCC:      emailBCC,
			HTML:     os.Getenv("ALERT_EMAIL_HTML") == "true",
		})
		alertManager.AddNotifier(notify)
		log.Printf("alerting: email channel enabled → %s (to: %s)", smtpAddr, strings.Join(emailTo, ","))
	}

	// Create bridge between health checker and alerting
	alertBridge := alerting.NewBridge(healthChecker, alertManager, 30*time.Second)

	// Register health checks
	healthChecker.RegisterCheck("gateway", func(ctx context.Context) observability.ComponentHealth {
		return observability.ComponentHealth{
			Name:    "gateway",
			Status:  "healthy",
			Message: "Gateway is running",
		}
	})
	healthChecker.RegisterCheck("database", func(ctx context.Context) observability.ComponentHealth {
		if err := db.PingContext(ctx); err != nil {
			return observability.ComponentHealth{
				Name:    "database",
				Status:  "unhealthy",
				Message: err.Error(),
			}
		}
		return observability.ComponentHealth{
			Name:    "database",
				Status:  "healthy",
				Message: "SQLite connected",
			}
	})

	// Create alert handler for API routes
	alertHandler := alerting.NewHandler(alertManager)

	// ─── Rate Limit Tracker ───────────────────────────────────
	rateLimitTracker := rlimitt.NewTracker(rlimitt.DefaultTrackerConfig())
	defer rateLimitTracker.Stop()

	// ─── Usage Analytics ────────────────────────────────────────
	usageAnalytics := usagepkg.NewAnalytics(db)
	// Load historical data from DB on startup
	_ = usageAnalytics.LoadFromDB(5000)

	mux := api.BuildRouter(adminHandler, proxyHandler, web.FS, adminAuth, apiKeyAuth, userMgmtHandler)

	// Register alerting API routes (protected by admin auth)
	alertHandler.RegisterRoutes(mux)

	// Register rate limit and analytics API routes
	mux.Handle("GET /api/admin/rate-limits", requireAdminAuth(rateLimitTracker.Handler()))
	mux.Handle("GET /api/admin/analytics", requireAdminAuth(usageAnalytics.Handler()))
	mux.Handle("GET /api/admin/analytics/live", requireAdminAuth(usageAnalytics.Handler()))

	// ─── Log Rotation ──────────────────────────────────────────
	logRotationCfg := logrotation.DefaultConfig()
	if v := os.Getenv("LOG_RETENTION_REQUEST_DAYS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			logRotationCfg.RequestLogRetention = time.Duration(d) * 24 * time.Hour
		}
	}
	if v := os.Getenv("LOG_RETENTION_SYSTEM_DAYS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			logRotationCfg.SystemLogRetention = time.Duration(d) * 24 * time.Hour
		}
	}
	if v := os.Getenv("LOG_RETENTION_ALERT_DAYS"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d > 0 {
			logRotationCfg.AlertRetention = time.Duration(d) * 24 * time.Hour
		}
	}
	if v := os.Getenv("LOG_CLEANUP_INTERVAL_HOURS"); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			logRotationCfg.CleanupInterval = time.Duration(h) * time.Hour
		}
	}
	logRotator := logrotation.NewRotator(db, logRotationCfg)
	mux.Handle("GET /api/admin/log-rotation", requireAdminAuth(logRotator.Handler()))
	mux.Handle("POST /api/admin/log-rotation/cleanup", requireAdminAuth(logRotator.Handler()))

	// ─── WebSocket ──────────────────────────────────────────────
	wsHub := websocket.NewHub()
	go wsHub.Run()
	wsHandler := websocket.NewHandler(wsHub)
	mux.HandleFunc("GET /ws", wsHandler.HandleWebSocket)
	mux.HandleFunc("GET /ws/status", wsHandler.HandleWSStatus)

	// Register observability endpoints
	mux.Handle("GET /metrics", observability.NewPrometheusExporter().Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		resp := healthChecker.CheckHealth(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		// Broadcast health to WebSocket clients
		wsHub.BroadcastToTopic(websocket.MsgTypeHealth, resp)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	// Build middleware chain
	mwConfig := middleware.DefaultChainConfig()

	// Rate limiting from env
	if rps := os.Getenv("RATE_LIMIT_RPS"); rps != "" {
		if v, err := strconv.ParseFloat(rps, 64); err == nil && v > 0 {
			mwConfig.RateLimit.RequestsPerSecond = v
		}
	}
	if burst := os.Getenv("RATE_LIMIT_BURST"); burst != "" {
		if v, err := strconv.Atoi(burst); err == nil && v > 0 {
			mwConfig.RateLimit.BurstSize = v
		}
	}

	// CORS origins from env
	if origins := os.Getenv("CORS_ALLOWED_ORIGINS"); origins != "" {
		mwConfig.CORS.AllowedOrigins = strings.Split(origins, ",")
	}

	// Wrap the mux with the middleware chain
	handler := middleware.DefaultChain(mux, mwConfig)

	// Wrap with rate limit tracker (records allowed/rejected decisions)
	handler = rateLimitTracker.Middleware(handler)

	server := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for _, adapter := range registry.List() {
		if runner, ok := adapter.(channels.BackgroundRunner); ok {
			go runner.Run(ctx)
		}
	}
	go sm.Run(ctx)
	go logRepo.Run(ctx)
	go alertManager.Start(ctx)
	go alertBridge.Start(ctx)
	go logRotator.Start(ctx)

	if cfg.ProxyHealthcheckEnabled {
		checker := proxypool.NewChecker(proxyPoolRepo, proxypool.NewProbeTransport(), proxyCheckConfig)
		go checker.Run(ctx)
	}

	// ─── WebSocket Analytics Streamer ───────────────────────
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if wsHub.ClientCount() == 0 {
					continue
				}
				wsHub.BroadcastToTopic(websocket.MsgTypeAnalytics, usageAnalytics.Snapshot())
				wsHub.BroadcastToTopic(websocket.MsgTypeRateLimits, rateLimitTracker.Snapshot())
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		log.Printf("gateway: listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("gateway: shutting down")
	wsHub.Stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

type config struct {
	ListenAddr                       string
	DBPath                           string
	WaitOnFull                       bool
	AdminPassword                    string
	AdminSessionTTL                  time.Duration
	TransportRequestReuse            bool
	ProxyHealthcheckEnabled          bool
	ProxyHealthcheckURL              string
	ProxyHealthcheckInterval         time.Duration
	ProxyHealthcheckTimeout          time.Duration
	ProxyHealthcheckConcurrency      int
	SessionCreateMaxParallelGlobal   int
	SessionCreateMaxParallelPerKey   int
	SessionCreateMaxParallelPerModel int
	SessionCreateMaxParallelPerGroup int
}

func loadConfig() config {
	c := config{
		ListenAddr:      getenv("LISTEN_ADDR", ":30080"),
		DBPath:          getenv("DB_PATH", "./freebuff-reverse.db"),
		WaitOnFull:      getenv("SESSION_WAIT_ON_FULL", "false") == "true",
		AdminPassword:   getenv("ADMIN_PASSWORD", "admin"),
		AdminSessionTTL: 12 * time.Hour,
		TransportRequestReuse: getenvBool("TRANSPORT_REQUEST_REUSE", false) ||
			getenvBool("FREEBUFF_TRANSPORT_REUSE", false) ||
			getenv("FREEBUFF_TRANSPORT_REUSE_SCOPE", "") == "request",
		ProxyHealthcheckEnabled:          getenvBool("PROXY_HEALTHCHECK_ENABLED", true),
		ProxyHealthcheckURL:              getenv("PROXY_HEALTHCHECK_URL", ""),
		ProxyHealthcheckInterval:         getenvDuration("PROXY_HEALTHCHECK_INTERVAL", time.Minute),
		ProxyHealthcheckTimeout:          getenvDuration("PROXY_HEALTHCHECK_TIMEOUT", 10*time.Second),
		ProxyHealthcheckConcurrency:      getenvInt("PROXY_HEALTHCHECK_CONCURRENCY", 5),
		SessionCreateMaxParallelGlobal:   getenvInt("SESSION_CREATE_MAX_PARALLEL_GLOBAL", 128),
		SessionCreateMaxParallelPerKey:   getenvInt("SESSION_CREATE_MAX_PARALLEL_PER_KEY", 32),
		SessionCreateMaxParallelPerModel: getenvInt("SESSION_CREATE_MAX_PARALLEL_PER_MODEL", 32),
		SessionCreateMaxParallelPerGroup: getenvInt("SESSION_CREATE_MAX_PARALLEL_PER_GROUP", 96),
	}
	return c
}

// requireAdminAuth wraps a handler with admin authentication.
func requireAdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check for valid session cookie
		cookie, err := r.Cookie("freebuffreverse_admin")
		if err != nil || cookie.Value == "" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v == "true" || v == "1" || v == "yes"
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}


