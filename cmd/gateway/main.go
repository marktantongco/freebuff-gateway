package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/api"
	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	_ "github.com/marktantongco/freebuff-gateway/internal/channels/freebuff"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
	"github.com/marktantongco/freebuff-gateway/internal/metrics"
	"github.com/marktantongco/freebuff-gateway/internal/orchestration"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/runtimeconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
	"github.com/marktantongco/freebuff-gateway/internal/transport"
	"github.com/marktantongco/freebuff-gateway/internal/usage"
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
	usageRecorder := usage.NewRecorder(logRepo, metricsAgg, pool)

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
	apiKeyAuth := api.NewAPIKeyAuthenticator(authKeyRepo)

	mux := api.BuildRouter(adminHandler, proxyHandler, web.FS, adminAuth, apiKeyAuth)
	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: withRequestLogger(mux),
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
	if cfg.ProxyHealthcheckEnabled {
		checker := proxypool.NewChecker(proxyPoolRepo, proxypool.NewProbeTransport(), proxyCheckConfig)
		go checker.Run(ctx)
	}

	go func() {
		log.Printf("gateway: listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("gateway: shutting down")
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

func withRequestLogger(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		h.ServeHTTP(rw, r)
		log.Printf("http %s %s -> %d %dms", r.Method, r.URL.Path, rw.status, time.Since(started).Milliseconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
