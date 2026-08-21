package benchmarks

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/alerting"
	"github.com/marktantongco/freebuff-gateway/internal/api"
	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	_ "github.com/marktantongco/freebuff-gateway/internal/channels/freebuff"
	"github.com/marktantongco/freebuff-gateway/internal/config"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
	"github.com/marktantongco/freebuff-gateway/internal/middleware"
	"github.com/marktantongco/freebuff-gateway/internal/observability"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/queue"
	"github.com/marktantongco/freebuff-gateway/internal/runtimeconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
	"github.com/marktantongco/freebuff-gateway/internal/transport"
	"github.com/marktantongco/freebuff-gateway/web"
)

// ═══════════════════════════════════════════════════════════════
// HTTP Server Benchmarks
// ═══════════════════════════════════════════════════════════════

func setupBenchServer(b *testing.B) *httptest.Server {
	b.Helper()

	dbPath := filepath.Join(b.TempDir(), "bench.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	b.Cleanup(func() { db.Close() })

	registry := channels.NewRegistry()
	if err := registry.RegisterBuiltins(); err != nil {
		b.Fatalf("register builtins: %v", err)
	}

	accountRepo := accounts.NewRepo(db)
	pool := accounts.NewPool(accountRepo)
	channelConfigRepo := channelconfig.NewRepo(db)
	freebuffStateRepo := freebuffstate.NewRepo(db)
	proxyPoolRepo := proxypool.NewRepo(db)
	resolver := proxypool.NewResolver(proxyPoolRepo)
	authKeyRepo := authkeys.NewRepo(db)
	systemLogRepo := systemlogs.NewRepo(db)
	policyResolver := runtimeconfig.NewResolver(channelConfigRepo, accountRepo)

	tp := transport.New(transport.WithTimeout(10*time.Second))
	sm := session.NewManager(registry, pool, tp, session.Config{
		WaitOnFull:           false,
		ReapInterval:         30 * time.Second,
		Resolver:             policyResolver,
		StateRecorder:        freebuffStateRepo,
		AccountMetadataResolver: resolver,
	})

	logRepo := logs.NewRepo(db)
	adminHandler := api.NewAdminHandler(registry, pool, sm, logRepo, nil, tp,
		api.WithChannelConfigRepo(channelConfigRepo),
		api.WithFreeBuffStateRepo(freebuffStateRepo),
		api.WithProxyPoolRepo(proxyPoolRepo),
		api.WithAuthKeysRepo(authKeyRepo),
		api.WithSystemLogsRepo(systemLogRepo),
	)
	proxyHandler := api.NewProxyHandler(registry, nil)
	adminAuth := api.NewAdminAuthenticator("bench", 1*time.Hour)
	apiKeyAuth := api.NewAPIKeyAuthenticator(authKeyRepo)

	mux := api.BuildRouter(adminHandler, proxyHandler, web.FS, adminAuth, apiKeyAuth, nil)

	alertManager := alerting.NewManager(alerting.DefaultAlertConfig(), nil)
	alertHandler := alerting.NewHandler(alertManager)
	alertHandler.RegisterRoutes(mux)

	healthChecker := observability.NewHealthChecker("bench")
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		resp := healthChecker.CheckHealth(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mwConfig := middleware.DefaultChainConfig()
	handler := middleware.DefaultChain(mux, mwConfig)

	return httptest.NewServer(handler)
}

func BenchmarkHealthEndpoint(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Get(server.URL + "/healthz")
		resp.Body.Close()
	}
}

func BenchmarkHealthEndpointParallel(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := http.Get(server.URL + "/healthz")
			resp.Body.Close()
		}
	})
}

func BenchmarkDashboard(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Get(server.URL + "/dashboard")
		resp.Body.Close()
	}
}

func BenchmarkMetrics(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Get(server.URL + "/metrics")
		resp.Body.Close()
	}
}

func BenchmarkLogin(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	body, _ := json.Marshal(map[string]string{"password": "bench"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Post(server.URL+"/api/admin/login", "application/json", bytes.NewReader(body))
		resp.Body.Close()
	}
}

func BenchmarkAlertList(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Get(server.URL + "/api/alerts")
		resp.Body.Close()
	}
}

func BenchmarkMiddlewareChain(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("GET", server.URL+"/healthz", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("X-Request-ID", "bench-test-id-12345")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}
}

func BenchmarkCORSPreflight(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("OPTIONS", server.URL+"/v1/chat/completions", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("Access-Control-Request-Method", "POST")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}
}

func BenchmarkConcurrentHealth(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := http.Get(server.URL + "/healthz")
			resp.Body.Close()
		}
	})
}

// ═══════════════════════════════════════════════════════════════
// Queue Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkQueueEnqueue(b *testing.B) {
	q := queue.NewQueue(queue.DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, "bench", queue.PriorityNormal, func(ctx context.Context) error {
			return nil
		})
	}
}

func BenchmarkQueueEnqueueDequeue(b *testing.B) {
	q := queue.NewQueue(queue.DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, "bench", queue.PriorityNormal, func(ctx context.Context) error {
			return nil
		})
		q.TryDequeue()
	}
}

func BenchmarkQueuePriority(b *testing.B) {
	q := queue.NewQueue(queue.DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	priorities := []int{
		queue.PriorityUrgent, queue.PriorityHigh, queue.PriorityNormal,
		queue.PriorityLow, queue.PriorityBulk,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, "bench", priorities[i%len(priorities)], func(ctx context.Context) error {
			return nil
		})
	}
}

func BenchmarkQueueConcurrent(b *testing.B) {
	q := queue.NewQueue(queue.DefaultQueueConfig())
	defer q.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Enqueue(ctx, "bench", queue.PriorityNormal, func(ctx context.Context) error {
				return nil
			})
		}
	})
}

// ═══════════════════════════════════════════════════════════════
// Middleware Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkRequestID(b *testing.B) {
	handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkCORS(b *testing.B) {
	config := middleware.DefaultCORSConfig()
	handler := middleware.CORS(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkRateLimiter(b *testing.B) {
	config := middleware.RateLimitConfig{
		RequestsPerSecond: 10000,
		BurstSize:         10000,
	}
	handler := middleware.RateLimit(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkRecovery(b *testing.B) {
	handler := middleware.Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

func BenchmarkFullMiddlewareChain(b *testing.B) {
	config := middleware.DefaultChainConfig()
	handler := middleware.DefaultChain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}), config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}

// ═══════════════════════════════════════════════════════════════
// Alerting Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkAlertManagerEvaluate(b *testing.B) {
	m := alerting.NewManager(alerting.DefaultAlertConfig(), nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Evaluate(ctx, map[string]alerting.ComponentHealth{
			"memory": {Name: "memory", Status: alerting.HealthStatusHealthy, Message: "ok"},
		})
	}
}

func BenchmarkAlertFingerprint(b *testing.B) {
	alert := &alerting.Alert{
		Name:     "benchmark-alert",
		Severity: alerting.SeverityWarning,
		Source:   "benchmark",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		alert.Fingerprint()
	}
}

func BenchmarkAlertStats(b *testing.B) {
	m := alerting.NewManager(alerting.DefaultAlertConfig(), nil)
	ctx := context.Background()

	// Create some alerts
	m.Evaluate(ctx, map[string]alerting.ComponentHealth{
		"memory":   {Name: "memory", Status: alerting.HealthStatusUnhealthy, Message: "critical"},
		"database": {Name: "database", Status: alerting.HealthStatusDegraded, Message: "warning"},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Stats()
	}
}

// ═══════════════════════════════════════════════════════════════
// Observability Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkHealthChecker(b *testing.B) {
	hc := observability.NewHealthChecker("bench")
	hc.RegisterCheck("test", func(ctx context.Context) observability.ComponentHealth {
		return observability.ComponentHealth{
			Name:    "test",
			Status:  "healthy",
			Message: "ok",
		}
	})

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hc.CheckHealth(ctx)
	}
}

func BenchmarkPrometheusExporter(b *testing.B) {
	exporter := observability.NewPrometheusExporter()
	counter := exporter.NewCounter("bench_counter", "Test counter")
	gauge := exporter.NewGauge("bench_gauge", "Test gauge")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter.Inc()
			gauge.Set(int64(i % 1000))
	}
}

// ═══════════════════════════════════════════════════════════════
// Memory Allocation Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkHealthJSONMarshal(b *testing.B) {
	health := map[string]interface{}{
		"status":    "healthy",
		"version":   "1.0.0",
		"uptime":    123456789,
		"goroutines": 18,
		"components": map[string]interface{}{
			"database": map[string]interface{}{
				"name":    "database",
				"status":  "healthy",
				"message": "SQLite connection OK",
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		json.Marshal(health)
	}
}

func BenchmarkAlertJSONMarshal(b *testing.B) {
	alert := &alerting.Alert{
		ID:       "test-123",
		Name:     "High CPU",
		Severity: alerting.SeverityCritical,
		State:    alerting.StateFiring,
		Source:   "monitoring/cpu",
		Message:  "CPU usage at 95%",
		FiredAt:  time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		json.Marshal(alert)
	}
}

// ═══════════════════════════════════════════════════════════════
// Load Test Simulation
// ═══════════════════════════════════════════════════════════════

func BenchmarkLoadTest100RPS(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, _ := http.Get(server.URL + "/healthz")
		resp.Body.Close()
	}
}

func BenchmarkLoadTestConcurrent10(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := http.Get(server.URL + "/healthz")
			resp.Body.Close()
		}
	})
}

func BenchmarkLoadTestMixed(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	endpoints := []string{"/healthz", "/readyz", "/metrics", "/api/alerts", "/api/alerts/stats"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			resp, _ := http.Get(server.URL + endpoints[i%len(endpoints)])
			resp.Body.Close()
			i++
		}
	})
}

// ═══════════════════════════════════════════════════════════════
// Throughput Test
// ═══════════════════════════════════════════════════════════════

func BenchmarkThroughput(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	var wg sync.WaitGroup
	concurrency := 10
	requestsPerWorker := b.N / concurrency
	if requestsPerWorker < 1 {
		requestsPerWorker = 1
	}

	b.ResetTimer()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				resp, _ := http.Get(server.URL + "/healthz")
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
}

// ═══════════════════════════════════════════════════════════════
// Latency Benchmarks
// ═══════════════════════════════════════════════════════════════

func BenchmarkLatencyHealth(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		resp, _ := http.Get(server.URL + "/healthz")
		resp.Body.Close()
		_ = time.Since(start)
	}
}

func BenchmarkLatencyLogin(b *testing.B) {
	server := setupBenchServer(b)
	defer server.Close()

	body, _ := json.Marshal(map[string]string{"password": "bench"})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		resp, _ := http.Post(server.URL+"/api/admin/login", "application/json", bytes.NewReader(body))
		resp.Body.Close()
		_ = time.Since(start)
	}
}

// ═══════════════════════════════════════════════════════════════
// Benchmark Helper
// ═══════════════════════════════════════════════════════════════

func BenchmarkConfigLoad(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config.DefaultConfig()
	}
}

func BenchmarkMiddlewareChainCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config := middleware.DefaultChainConfig()
		_ = config
	}
}
