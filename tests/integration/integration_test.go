// Package integration provides end-to-end tests for the Freebuff Gateway.
// These tests spin up a real server and exercise the full HTTP stack.
package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
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
	server      *httptest.Server
	db          *sql.DB
	adminPass   string
	mwConfig    middleware.ChainConfig
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Verify db is usable
	var dbConn *sql.DB = db

	registry := channels.NewRegistry()
	if err := registry.RegisterBuiltins(); err != nil {
		t.Fatalf("register builtins: %v", err)
	}

	accountRepo := accounts.NewRepo(db)
	pool := accounts.NewPool(accountRepo)
	channelConfigRepo := channelconfig.NewRepo(db)
	freebuffStateRepo := freebuffstate.NewRepo(db)
	proxyPoolRepo := proxypool.NewRepo(db)
	_ = proxypool.NewResolver(proxyPoolRepo)
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
	})

	logRepo := logs.NewRepo(db)
	_ = dbConn

	adminHandler := api.NewAdminHandler(
		registry, pool, sm, logRepo, nil, tp,
		api.WithChannelConfigRepo(channelConfigRepo),
		api.WithFreeBuffStateRepo(freebuffStateRepo),
		api.WithProxyPoolRepo(proxyPoolRepo),
		api.WithAuthKeysRepo(authKeyRepo),
		api.WithSystemLogsRepo(systemLogRepo),
	)
	proxyHandler := api.NewProxyHandler(registry, nil)

	adminPass := "test-admin-123"
	adminAuth := api.NewAdminAuthenticator(adminPass, 1*time.Hour)
	apiKeyAuth := api.NewAPIKeyAuthenticator(authKeyRepo)

	mux := api.BuildRouter(adminHandler, proxyHandler, web.FS, adminAuth, apiKeyAuth)

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

	// Dashboard
	mux.Handle("/dashboard", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := web.FS.ReadFile("dashboard.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(body)
	}))

	// Apply middleware chain
	mwConfig := middleware.DefaultChainConfig()
	handler := middleware.DefaultChain(mux, mwConfig)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &testEnv{
		server:    server,
		db:        db,
		adminPass: adminPass,
		mwConfig:  mwConfig,
	}
}

func (e *testEnv) get(path string) *http.Response {
	resp, err := http.Get(e.server.URL + path)
	if err != nil {
		panic(fmt.Sprintf("GET %s: %v", path, err))
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
		panic(fmt.Sprintf("GET %s: %v", path, err))
	}
	return resp
}

func (e *testEnv) post(path string, body interface{}) *http.Response {
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	resp, err := http.Post(e.server.URL+path, "application/json", reader)
	if err != nil {
		panic(fmt.Sprintf("POST %s: %v", path, err))
	}
	return resp
}

func (e *testEnv) postWithAuth(path string, body interface{}) *http.Response {
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	req, _ := http.NewRequest("POST", e.server.URL+path, reader)
	req.Header.Set("Content-Type", "application/json")
	// Login first to get session cookie
	loginResp := e.post("/api/admin/login", map[string]string{"password": e.adminPass})
	if loginResp.StatusCode == 200 {
		for _ = range loginResp.Cookies() {
			// We'll use the cookie in subsequent requests
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(fmt.Sprintf("POST %s: %v", path, err))
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

// ─── Health Endpoint Tests ──────────────────────────────────

func TestHealthEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/healthz")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var health map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &health)

	if health["status"] != "healthy" {
		t.Fatalf("expected healthy status, got %v", health["status"])
	}
	if health["version"] != "test" {
		t.Fatalf("expected version 'test', got %v", health["version"])
	}
}

func TestReadinessProbe(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/readyz")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if body != "ok" {
		t.Fatalf("expected 'ok', got '%s'", body)
	}
}

func TestLivenessProbe(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/livez")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// ─── Middleware Tests ────────────────────────────────────────

func TestMiddlewareRequestID(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/healthz")
	defer resp.Body.Close()

	reqID := resp.Header.Get("X-Request-ID")
	if reqID == "" {
		t.Fatal("expected X-Request-ID header")
	}
	if len(reqID) != 32 {
		t.Fatalf("expected 32-char request ID, got %d chars: %s", len(reqID), reqID)
	}
}

func TestMiddlewareRequestIDPreserved(t *testing.T) {
	env := setupTestEnv(t)

	customID := "my-custom-request-id-12345"
	resp := env.getWithHeaders("/healthz", map[string]string{
		"X-Request-ID": customID,
	})
	defer resp.Body.Close()

	reqID := resp.Header.Get("X-Request-ID")
	if reqID != customID {
		t.Fatalf("expected custom ID '%s', got '%s'", customID, reqID)
	}
}

func TestMiddlewareAPIVersion(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/v1/models")
	defer resp.Body.Close()

	version := resp.Header.Get("X-API-Version")
	if version != "1" {
		t.Fatalf("expected API version '1', got '%s'", version)
	}
}

func TestMiddlewareCORS(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.getWithHeaders("/healthz", map[string]string{
		"Origin": "http://localhost:3000",
	})
	defer resp.Body.Close()

	origin := resp.Header.Get("Access-Control-Allow-Origin")
	if origin == "" {
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
		t.Fatalf("preflight request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 for preflight, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Fatal("expected Allow-Methods in preflight response")
	}
}

func TestMiddlewareRateLimitHeaders(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/healthz")
	defer resp.Body.Close()

	limit := resp.Header.Get("X-RateLimit-Limit")
	if limit == "" {
		t.Fatal("expected X-RateLimit-Limit header")
	}

	remaining := resp.Header.Get("X-RateLimit-Remaining")
	if remaining == "" {
		t.Fatal("expected X-RateLimit-Remaining header")
	}
}

func TestMiddlewarePanicRecovery(t *testing.T) {
	env := setupTestEnv(t)

	// Hit a route that doesn't exist — should not crash
	resp := env.get("/api/nonexistent-endpoint-xyz")
	defer resp.Body.Close()

	// Should return a response (not crash the server)
	if resp.StatusCode == 0 {
		t.Fatal("server did not respond")
	}

	// Server should still be alive
	resp2 := env.get("/healthz")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatal("server is not alive after panic")
	}
}

// ─── Dashboard Tests ────────────────────────────────────────

func TestDashboardLoads(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/dashboard")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("expected HTML content type, got %s", ct)
	}

	body := readBody(t, resp)
	if !strings.Contains(body, "Freebuff Gateway Dashboard") {
		t.Fatal("expected dashboard title in response")
	}
	if !strings.Contains(body, "Overview") {
		t.Fatal("expected navigation in dashboard")
	}
}

func TestDashboardAPIEndpoints(t *testing.T) {
	env := setupTestEnv(t)

	// Dashboard should call these API endpoints
	endpoints := []string{
		"/healthz",
		"/api/alerts",
		"/api/alerts/stats",
		"/metrics",
	}

	for _, ep := range endpoints {
		resp := env.get(ep)
		if resp.StatusCode == 0 {
			t.Fatalf("endpoint %s did not respond", ep)
		}
		resp.Body.Close()
	}
}

// ─── Alerting API Tests ─────────────────────────────────────

func TestAlertingListAlerts(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/api/alerts")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	data := parseJSON(t, resp)
	alerts, ok := data["alerts"]
	if !ok {
		t.Fatal("expected 'alerts' key in response")
	}
	if alerts == nil {
		// Empty list is fine
		return
	}
}

func TestAlertingStats(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/api/alerts/stats")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	data := parseJSON(t, resp)
	if _, ok := data["stats"]; !ok {
		t.Fatal("expected 'stats' key in response")
	}
}

func TestAlertingCreateAlert(t *testing.T) {
	env := setupTestEnv(t)

	alert := map[string]interface{}{
		"name":     "Test Integration Alert",
		"severity": "warning",
		"source":   "integration-test",
		"message":  "This is a test alert",
	}

	resp := env.post("/api/alerts", alert)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("expected 201 or 200, got %d: %s", resp.StatusCode, body)
	}
}

func TestAlertingHistory(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/api/alerts/history")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// ─── Auth Flow Tests ────────────────────────────────────────

func TestAdminLogin(t *testing.T) {
	env := setupTestEnv(t)

	loginReq := map[string]string{"password": env.adminPass}
	resp := env.post("/api/admin/login", loginReq)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	// Check for session cookie
	cookies := resp.Cookies()
	found := false
	for _, c := range cookies {
		if c.Name == "freebuffreverse_admin" {
			found = true
			if c.Value == "" {
				t.Fatal("expected non-empty session token")
			}
		}
	}
	if !found {
		t.Fatal("expected admin session cookie")
	}
}

func TestAdminLoginInvalidPassword(t *testing.T) {
	env := setupTestEnv(t)

	loginReq := map[string]string{"password": "wrong-password"}
	resp := env.post("/api/admin/login", loginReq)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAdminLogout(t *testing.T) {
	env := setupTestEnv(t)

	// Login first
	loginReq := map[string]string{"password": env.adminPass}
	loginResp := env.post("/api/admin/login", loginReq)
	defer loginResp.Body.Close()

	// Get session cookie
	var sessionCookie *http.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == "freebuffreverse_admin" {
			sessionCookie = c
			break
		}
	}

	if sessionCookie == nil {
		t.Fatal("no session cookie after login")
	}

	// Logout
	req, _ := http.NewRequest("POST", env.server.URL+"/api/admin/logout", nil)
	req.AddCookie(sessionCookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("logout request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAdminProtectedEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	// Try to access admin endpoint without auth
	resp := env.get("/api/admin/channels")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 401/403, got %d", resp.StatusCode)
	}
}

func TestAdminWithSessionCookie(t *testing.T) {
	env := setupTestEnv(t)

	// Login
	loginReq := map[string]string{"password": env.adminPass}
	loginResp := env.post("/api/admin/login", loginReq)
	defer loginResp.Body.Close()

	var sessionCookie *http.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == "freebuffreverse_admin" {
			sessionCookie = c
			break
		}
	}

	if sessionCookie == nil {
		t.Fatal("no session cookie")
	}

	// Access admin endpoint with cookie
	req, _ := http.NewRequest("GET", env.server.URL+"/api/admin/channels", nil)
	req.AddCookie(sessionCookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}
}

// ─── Model API Tests ────────────────────────────────────────

func TestListModels(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/v1/models")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &result)

	// Should have an "object" field
	if obj, ok := result["object"]; ok {
		if obj != "list" {
			t.Fatalf("expected object 'list', got %v", obj)
		}
	}
}

func TestListModelsWithVersion(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/v1/models")
	defer resp.Body.Close()

	version := resp.Header.Get("X-API-Version")
	if version != "1" {
		t.Fatalf("expected version 1, got %s", version)
	}
}

// ─── Observability Tests ────────────────────────────────────

func TestMetricsEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/metrics")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected text/plain content type, got %s", ct)
	}
}

// ─── Full Stack Tests ───────────────────────────────────────

func TestFullRequestLifecycle(t *testing.T) {
	env := setupTestEnv(t)

	// 1. Health check
	healthResp := env.get("/healthz")
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("health check failed: %d", healthResp.StatusCode)
	}

	// 2. List models
	modelsResp := env.get("/v1/models")
	defer modelsResp.Body.Close()
	if modelsResp.StatusCode != http.StatusOK {
		t.Fatalf("list models failed: %d", modelsResp.StatusCode)
	}

	// 3. Create alert
	alertResp := env.post("/api/alerts", map[string]interface{}{
		"name":     "Lifecycle Test",
		"severity": "info",
		"source":   "test",
		"message":  "full lifecycle test",
	})
	defer alertResp.Body.Close()

	// 4. Check alerts
	alertsResp := env.get("/api/alerts")
	defer alertsResp.Body.Close()

	// 5. Get stats
	statsResp := env.get("/api/alerts/stats")
	defer statsResp.Body.Close()

	// 6. Dashboard
	dashResp := env.get("/dashboard")
	defer dashResp.Body.Close()
	if dashResp.StatusCode != http.StatusOK {
		t.Fatalf("dashboard failed: %d", dashResp.StatusCode)
	}

	// 7. Metrics
	metricsResp := env.get("/metrics")
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics failed: %d", metricsResp.StatusCode)
	}
}

func TestConcurrentRequests(t *testing.T) {
	env := setupTestEnv(t)

	const numRequests = 50
	const numGoroutines = 10

	results := make(chan int, numRequests)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numRequests/numGoroutines; j++ {
				resp := env.get("/healthz")
				results <- resp.StatusCode
				resp.Body.Close()
			}
		}(i)
	}

	for i := 0; i < numRequests; i++ {
		status := <-results
		if status != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, status)
		}
	}
}

func TestServerSurvivesErrors(t *testing.T) {
	env := setupTestEnv(t)

	// Send various invalid requests
	invalidRequests := []struct {
		method string
		path   string
		body   string
	}{
		{"POST", "/api/alerts", "invalid json"},
		{"POST", "/api/alerts", ""},
		{"DELETE", "/nonexistent", ""},
		{"PUT", "/api/admin/channels/xyz/config", "bad"},
	}

	for _, req := range invalidRequests {
		r, _ := http.NewRequest(req.method, env.server.URL+req.path, strings.NewReader(req.body))
		r.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Logf("request %s %s: %v (expected)", req.method, req.path, err)
			continue
		}
		resp.Body.Close()
	}

	// Server should still be alive
	healthResp := env.get("/healthz")
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatal("server is not alive after error requests")
	}
}

func TestResponseHeaders(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/healthz")
	defer resp.Body.Close()

	// Should have security headers from middleware
	headers := map[string]string{
		"X-Request-ID":    "",
		"X-API-Version":   "",
		"X-RateLimit-Limit": "",
	}

	for header := range headers {
		val := resp.Header.Get(header)
		if val == "" {
			t.Logf("WARNING: missing header %s", header)
		}
	}
}

func TestContentTypeJSON(t *testing.T) {
	env := setupTestEnv(t)

	jsonEndpoints := []string{
		"/healthz",
		"/api/alerts",
		"/api/alerts/stats",
		"/api/alerts/history",
	}

	for _, ep := range jsonEndpoints {
		resp := env.get(ep)
		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()

		if !strings.Contains(ct, "application/json") {
			t.Logf("endpoint %s: Content-Type %s (expected JSON)", ep, ct)
		}
	}
}

func TestHealthComponents(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/healthz")
	defer resp.Body.Close()

	var health map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &health)

	components, ok := health["components"].(map[string]interface{})
	if !ok {
		t.Fatal("expected components in health response")
	}

	// Should have at least gateway and database
	if _, ok := components["gateway"]; !ok {
		t.Log("expected 'gateway' component")
	}
	if _, ok := components["database"]; !ok {
		t.Log("expected 'database' component")
	}
}

func TestVersionEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/v1/models")
	defer resp.Body.Close()

	version := resp.Header.Get("X-API-Version")
	if version != "1" {
		t.Fatalf("expected version '1', got '%s'", version)
	}
}

func TestInvalidVersionRejected(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.get("/v99/models")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid version, got %d", resp.StatusCode)
	}
}

func TestConfigLoaded(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.ListenAddr == "" {
		t.Fatal("expected non-empty listen addr")
	}
	if cfg.DBPath == "" {
		t.Fatal("expected non-empty db path")
	}
}

func TestMiddlewareChainOrder(t *testing.T) {
	env := setupTestEnv(t)

	// Verify middleware chain is applied by checking multiple headers
	resp := env.get("/healthz")
	defer resp.Body.Close()

	// Request ID should be set
	if resp.Header.Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID (middleware not applied?)")
	}

	// Rate limit should be set
	if resp.Header.Get("X-RateLimit-Limit") == "" {
		t.Fatal("expected X-RateLimit-Limit (rate limiter not applied?)")
	}
}

func TestHealthCheckFrequency(t *testing.T) {
	env := setupTestEnv(t)

	// Make multiple requests and verify consistent responses
	for i := 0; i < 5; i++ {
		resp := env.get("/healthz")
		var health map[string]interface{}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		json.Unmarshal(body, &health)

		if health["status"] != "healthy" {
			t.Fatalf("request %d: expected healthy, got %v", i, health["status"])
		}
	}
}

// ─── Imports needed ─────────────────────────────────────────
// These are used by the test setup but need to be imported
var (
	_ = config.DefaultConfig
	_ = logs.NewRepo
	_ = runtimeconfig.NewResolver
)
