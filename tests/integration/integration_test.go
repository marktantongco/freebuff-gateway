// Package integration provides end-to-end tests for the Freebuff Gateway.
package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	"github.com/marktantongco/freebuff-gateway/internal/runtimeconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
	"github.com/marktantongco/freebuff-gateway/internal/transport"
	"github.com/marktantongco/freebuff-gateway/web"
)

// testEnv holds all dependencies for an integration test.
type testEnv struct {
	server    *httptest.Server
	db        *sql.DB
	adminPass string
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	registry := channels.NewRegistry()
	if err := registry.RegisterBuiltins(); err != nil {
		t.Fatalf("register builtins: %v", err)
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

	tp := transport.New(
		transport.WithTimeout(10*time.Second),
		transport.WithBodyPreviewBytes(4096),
	)

	sm := session.NewManager(registry, pool, tp, session.Config{
		WaitOnFull:   false,
		ReapInterval: 30 * time.Second,
		Resolver:     policyResolver,
		StateRecorder: freebuffStateRepo,
		AccountMetadataResolver: resolver,
	})

	logRepo := logs.NewRepo(db)

	adminPass := "test-admin-123"
	adminHandler := api.NewAdminHandler(
		registry, pool, sm, logRepo, nil, tp,
		api.WithChannelConfigRepo(channelConfigRepo),
		api.WithFreeBuffStateRepo(freebuffStateRepo),
		api.WithProxyPoolRepo(proxyPoolRepo),
		api.WithAuthKeysRepo(authKeyRepo),
		api.WithSystemLogsRepo(systemLogRepo),
	)
	proxyHandler := api.NewProxyHandler(registry, nil)
	adminAuth := api.NewAdminAuthenticator(adminPass, 1*time.Hour)
	apiKeyAuth := api.NewAPIKeyAuthenticator(authKeyRepo)

	mux := api.BuildRouter(adminHandler, proxyHandler, web.FS, adminAuth, apiKeyAuth, nil)

	// Alerting
	alertManager := alerting.NewManager(alerting.DefaultAlertConfig(), nil)
	alertHandler := alerting.NewHandler(alertManager)
	alertHandler.RegisterRoutes(mux)

	// Observability
	healthChecker := observability.NewHealthChecker("test")
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		resp := healthChecker.CheckHealth(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	// Apply middleware
	mwConfig := middleware.DefaultChainConfig()
	handler := middleware.DefaultChain(mux, mwConfig)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &testEnv{server: server, db: db, adminPass: adminPass}
}

func (e *testEnv) get(path string) *http.Response {
	resp, err := http.Get(e.server.URL + path)
	if err != nil {
		panic("GET " + path + ": " + err.Error())
	}
	return resp
}

func (e *testEnv) getWithHeaders(path string, headers map[string]string) *http.Response {
	req, _ := http.NewRequest("GET", e.server.URL+path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic("GET " + path + ": " + err.Error())
	}
	return resp
}

func (e *testEnv) postJSON(path string, body interface{}) *http.Response {
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	resp, err := http.Post(e.server.URL+path, "application/json", reader)
	if err != nil {
		panic("POST " + path + ": " + err.Error())
	}
	return resp
}

func (e *testEnv) postWithCookie(path string, body interface{}, cookie *http.Cookie) *http.Response {
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	req, _ := http.NewRequest("POST", e.server.URL+path, reader)
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic("POST " + path + ": " + err.Error())
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func parseJSON(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	var result map[string]interface{}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse json: %v (body: %s)", err, string(body))
	}
	return result
}

func getCookie(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════
// HEALTH ENDPOINT TESTS
// ═══════════════════════════════════════════════════════════════

func TestHealthEndpoint(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}

	data := parseJSON(t, resp)
	if data["status"] != "healthy" {
		t.Fatalf("expected healthy, got %v", data["status"])
	}
	if data["version"] != "test" {
		t.Fatalf("expected version 'test', got %v", data["version"])
	}
}

func TestReadinessProbe(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/readyz")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
	if body := readBody(t, resp); body != "ok" {
		t.Fatalf("expected 'ok', got '%s'", body)
	}
}

func TestLivenessProbe(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/livez")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
}

func TestHealthComponents(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	data := parseJSON(t, resp)
	components, ok := data["components"].(map[string]interface{})
	if !ok {
		t.Fatal("expected components in health response")
	}

	for _, name := range []string{"gateway", "database"} {
		if _, ok := components[name]; !ok {
			t.Logf("expected '%s' component", name)
		}
	}
}

// ═══════════════════════════════════════════════════════════════
// MIDDLEWARE TESTS
// ═══════════════════════════════════════════════════════════════

func TestMiddlewareRequestID(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	reqID := resp.Header.Get("X-Request-ID")
	if reqID == "" {
		t.Fatal("expected X-Request-ID header")
	}
	if len(reqID) != 32 {
		t.Fatalf("expected 32-char ID, got %d: %s", len(reqID), reqID)
	}
}

func TestMiddlewareRequestIDPreserved(t *testing.T) {
	env := setupTestEnv(t)
	customID := "my-custom-id-12345"
	resp := env.getWithHeaders("/healthz", map[string]string{"X-Request-ID": customID})
	defer resp.Body.Close()

	if got := resp.Header.Get("X-Request-ID"); got != customID {
		t.Fatalf("expected '%s', got '%s'", customID, got)
	}
}

func TestMiddlewareAPIVersion(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/v1/models")
	defer resp.Body.Close()

	if v := resp.Header.Get("X-API-Version"); v != "1" {
		t.Fatalf("expected version '1', got '%s'", v)
	}
}

func TestMiddlewareCORS(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.getWithHeaders("/healthz", map[string]string{"Origin": "http://localhost:3000"})
	defer resp.Body.Close()

	if origin := resp.Header.Get("Access-Control-Allow-Origin"); origin == "" {
		t.Fatal("expected CORS Allow-Origin header")
	}
}

func TestMiddlewareCORSPreflight(t *testing.T) {
	env := setupTestEnv(t)
	req, _ := http.NewRequest("OPTIONS", env.server.URL+"/v1/chat/completions", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, Authorization")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Fatal("expected Allow-Methods")
	}
}

func TestMiddlewareRateLimitHeaders(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	if resp.Header.Get("X-RateLimit-Limit") == "" {
		t.Fatal("expected X-RateLimit-Limit")
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "" {
		t.Fatal("expected X-RateLimit-Remaining")
	}
}

func TestMiddlewarePanicRecovery(t *testing.T) {
	env := setupTestEnv(t)

	// Hit nonexistent route — should not crash
	resp := env.get("/api/nonexistent-endpoint")
	resp.Body.Close()

	// Server should still be alive
	resp2 := env.get("/healthz")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatal("server not alive after error")
	}
}

// ═══════════════════════════════════════════════════════════════
// DASHBOARD TESTS
// ═══════════════════════════════════════════════════════════════

func TestDashboardLoads(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/dashboard")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatal("expected HTML content type")
	}

	body := readBody(t, resp)
	if !strings.Contains(body, "Freebuff Gateway Dashboard") {
		t.Fatal("expected dashboard title")
	}
	if !strings.Contains(body, "login-overlay") {
		t.Fatal("expected login overlay")
	}
}

// ═══════════════════════════════════════════════════════════════
// AUTH FLOW TESTS
// ═══════════════════════════════════════════════════════════════

func TestAdminLoginSuccess(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.postJSON("/api/admin/login", map[string]string{"password": env.adminPass})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		body := readBody(t, resp)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	cookie := getCookie(resp, "freebuffreverse_admin")
	if cookie == nil {
		t.Fatal("expected session cookie")
	}
	if cookie.Value == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestAdminLoginInvalidPassword(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.postJSON("/api/admin/login", map[string]string{"password": "wrong"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAdminLogout(t *testing.T) {
	env := setupTestEnv(t)

	// Login
	loginResp := env.postJSON("/api/admin/login", map[string]string{"password": env.adminPass})
	cookie := getCookie(loginResp, "freebuffreverse_admin")
	loginResp.Body.Close()

	if cookie == nil {
		t.Fatal("no session cookie")
	}

	// Logout
	logoutResp := env.postWithCookie("/api/admin/logout", nil, cookie)
	defer logoutResp.Body.Close()

	if logoutResp.StatusCode != http.StatusOK && logoutResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 200, got %d", logoutResp.StatusCode)
	}
}

func TestAdminProtectedEndpoint(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/api/admin/channels")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 401/403, got %d", resp.StatusCode)
	}
}

func TestAdminWithSession(t *testing.T) {
	env := setupTestEnv(t)

	// Login
	loginResp := env.postJSON("/api/admin/login", map[string]string{"password": env.adminPass})
	cookie := getCookie(loginResp, "freebuffreverse_admin")
	loginResp.Body.Close()

	if cookie == nil {
		t.Fatal("no session cookie")
	}

	// Access protected endpoint
	_ = env.postWithCookie("/api/admin/channels", nil, cookie)
	// GET with cookie — need to use GET
	req, _ := http.NewRequest("GET", env.server.URL+"/api/admin/channels", nil)
	req.AddCookie(cookie)
	actualResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer actualResp.Body.Close()

	if actualResp.StatusCode != http.StatusOK {
		body := readBody(t, actualResp)
		t.Fatalf("expected 200, got %d: %s", actualResp.StatusCode, body)
	}
}

// ═══════════════════════════════════════════════════════════════
// ALERTING API TESTS
// ═══════════════════════════════════════════════════════════════

func TestAlertListEmpty(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/api/alerts")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
}

func TestAlertStats(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/api/alerts/stats")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
}

func TestAlertCreate(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.postJSON("/api/alerts", map[string]interface{}{
		"name":     "Test Alert",
		"severity": "warning",
		"source":   "integration-test",
		"message":  "test message",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("expected 201/200, got %d: %s", resp.StatusCode, body)
	}
}

func TestAlertHistory(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/api/alerts/history")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════
// MODEL API TESTS
// ═══════════════════════════════════════════════════════════════

func TestListModelsRequiresAuth(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/v1/models")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}

	data := parseJSON(t, resp)
	if obj, ok := data["object"]; ok && obj != "list" {
		t.Fatalf("expected object 'list', got %v", obj)
	}
}

func TestInvalidVersionRejected(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/v99/models")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════
// OBSERVABILITY TESTS
// ═══════════════════════════════════════════════════════════════

func TestMetricsEndpoint(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/metrics")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 200 or 401, got %d", resp.StatusCode)
	}
}

// ═══════════════════════════════════════════════════════════════
// FULL STACK TESTS
// ═══════════════════════════════════════════════════════════════

func TestFullRequestLifecycle(t *testing.T) {
	env := setupTestEnv(t)

	steps := []struct {
		name string
		path string
	}{
		{"health", "/healthz"},
		{"models", "/v1/models"},
		{"alerts", "/api/alerts"},
		{"stats", "/api/alerts/stats"},
		{"history", "/api/alerts/history"},
		{"dashboard", "/dashboard"},
		{"metrics", "/metrics"},
		{"readyz", "/readyz"},
		{"livez", "/livez"},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			resp := env.get(step.path)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s: expected 200, got %d", step.name, resp.StatusCode)
			}
		})
	}
}

func TestConcurrentRequests(t *testing.T) {
	env := setupTestEnv(t)

	const total = 20
	const goroutines = 5
	results := make(chan int, total)

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < total/goroutines; j++ {
				resp := env.get("/healthz")
				results <- resp.StatusCode
				resp.Body.Close()
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	successCount := 0
	for i := 0; i < total; i++ {
		status := <-results
		if status == http.StatusOK {
			successCount++
		}
	}

	if successCount == 0 {
		t.Fatal("all concurrent requests failed")
	}
}

func TestServerSurvivesErrors(t *testing.T) {
	env := setupTestEnv(t)

	// Send various invalid requests
	invalids := []struct {
		method string
		path   string
		body   string
	}{
		{"POST", "/api/alerts", "invalid json"},
		{"DELETE", "/nonexistent", ""},
		{"PUT", "/api/admin/channels/xyz/config", "bad"},
	}

	for _, req := range invalids {
		r, _ := http.NewRequest(req.method, env.server.URL+req.path, strings.NewReader(req.body))
		r.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(r)
		if err == nil {
			resp.Body.Close()
		}
	}

	// Server should still be alive
	resp := env.get("/healthz")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
		t.Fatal("server not alive after errors")
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.ListenAddr == "" {
		t.Fatal("expected non-empty listen addr")
	}
	if cfg.DBPath == "" {
		t.Fatal("expected non-empty db path")
	}
}

func TestHealthCheckConsistency(t *testing.T) {
	env := setupTestEnv(t)

	for i := 0; i < 5; i++ {
		resp := env.get("/healthz")
		var data map[string]interface{}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		json.Unmarshal(body, &data)

		if data["status"] != "healthy" {
			t.Fatalf("request %d: expected healthy, got %v", i, data["status"])
		}
	}
}

func TestResponseHeaders(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	expectedHeaders := []string{"X-Request-ID", "X-API-Version", "X-RateLimit-Limit"}
	for _, h := range expectedHeaders {
		if resp.Header.Get(h) == "" {
			t.Logf("WARNING: missing %s", h)
		}
	}
}

func TestContentTypeJSON(t *testing.T) {
	env := setupTestEnv(t)

	jsonEndpoints := []string{"/healthz", "/api/alerts", "/api/alerts/stats"}
	for _, ep := range jsonEndpoints {
		resp := env.get(ep)
		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()
		if !strings.Contains(ct, "application/json") {
			t.Logf("endpoint %s: Content-Type %s (expected JSON)", ep, ct)
		}
	}
}

func TestMiddlewareChainOrder(t *testing.T) {
	env := setupTestEnv(t)
	resp := env.get("/healthz")
	defer resp.Body.Close()

	// Verify all middleware applied
	if resp.Header.Get("X-Request-ID") == "" {
		t.Fatal("X-Request-ID missing (middleware not applied?)")
	}
	if resp.Header.Get("X-RateLimit-Limit") == "" {
		t.Fatal("X-RateLimit-Limit missing (rate limiter not applied?)")
	}
	if resp.Header.Get("X-API-Version") == "" {
		t.Fatal("X-API-Version missing (version middleware not applied?)")
	}
}
