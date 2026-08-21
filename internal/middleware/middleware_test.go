package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- RequestID Tests ---

func TestRequestIDGenerated(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := GetRequestID(r.Context())
		if id == "" {
			t.Fatal("expected non-empty request ID")
		}
		w.Write([]byte(id))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID in response")
	}
}

func TestRequestIDFromHeader(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := GetRequestID(r.Context())
		if id != "my-custom-id" {
			t.Fatalf("expected 'my-custom-id', got '%s'", id)
		}
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Request-ID", "my-custom-id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") != "my-custom-id" {
		t.Fatal("expected custom ID in response")
	}
}

func TestRequestIDFromCustomHeader(t *testing.T) {
	handler := RequestIDFromHeader("X-Custom-ID")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := GetRequestID(r.Context())
		if id != "custom-123" {
			t.Fatalf("expected 'custom-123', got '%s'", id)
		}
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Custom-ID", "custom-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
}

func TestRequestIDContextEmpty(t *testing.T) {
	// Empty context (no request ID set)
	id := GetRequestID(context.Background())
	if id != "" {
		t.Fatalf("expected empty, got '%s'", id)
	}
}

// --- CORS Tests ---

func TestCORSPreflight(t *testing.T) {
	config := DefaultCORSConfig()
	handler := CORS(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler for preflight")
	}))

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatal("expected Allow-Methods header")
	}
}

func TestCORSAllowedOrigin(t *testing.T) {
	config := CORSConfig{
		AllowedOrigins: []string{"http://example.com"},
		AllowedMethods: []string{"GET", "POST"},
	}

	handler := CORS(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "http://example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "http://example.com" {
		t.Fatalf("expected allowed origin, got: %s", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSBlockedOrigin(t *testing.T) {
	config := CORSConfig{
		AllowedOrigins: []string{"http://safe.com"},
	}

	handler := CORS(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "http://evil.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("expected no Allow-Origin for blocked origin")
	}
}

// --- Recovery Tests ---

func TestRecoveryCatchesPanics(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Fatal("expected error message in response")
	}
}

func TestRecoveryNoPanic(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRecoveryWithLogger(t *testing.T) {
	var logged bool
	handler := RecoveryWithLogger(func(format string, args ...interface{}) {
		logged = true
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !logged {
		t.Fatal("expected logger to be called")
	}
}

// --- Rate Limiter Tests ---

func TestRateLimitAllows(t *testing.T) {
	config := RateLimitConfig{
		RequestsPerSecond: 100,
		BurstSize:         10,
		CleanupInterval:   0, // disable cleanup for test
	}

	handler := RateLimit(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}
}

func TestRateLimitBlocks(t *testing.T) {
	config := RateLimitConfig{
		RequestsPerSecond: 0.1, // very slow refill
		BurstSize:         2,
		CleanupInterval:   0,
	}

	handler := RateLimit(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	// Exhaust burst
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	// Should be rate limited now
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "1.2.3.4:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("expected remaining=0, got %s", rec.Header().Get("X-RateLimit-Remaining"))
	}
}

func TestRateLimitDifferentKeys(t *testing.T) {
	config := RateLimitConfig{
		RequestsPerSecond: 100,
		BurstSize:         1,
		CleanupInterval:   0,
	}

	handler := RateLimit(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	// IP1 uses its token
	req1 := httptest.NewRequest("GET", "/", nil)
	req1.RemoteAddr = "1.1.1.1:1111"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// IP2 should have its own token
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.RemoteAddr = "2.2.2.2:2222"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("different IP should have own bucket, got %d", rec2.Code)
	}
}

func TestRateLimitByKey(t *testing.T) {
	config := RateLimitConfig{
		RequestsPerSecond: 100,
		BurstSize:         1,
		CleanupInterval:   0,
	}

	handler := RateLimitByKey(config, func(r *http.Request) string {
		return r.Header.Get("X-API-Key")
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	// Key "abc" uses token
	req1 := httptest.NewRequest("GET", "/", nil)
	req1.Header.Set("X-API-Key", "abc")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Key "xyz" should have its own token
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.Header.Set("X-API-Key", "xyz")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("different API key should have own bucket, got %d", rec2.Code)
	}
}

func TestRateLimiterStats(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         10,
		CleanupInterval:   0,
	})
	defer rl.Stop()

	rl.getBucket("key1")
	rl.getBucket("key2")

	stats := rl.Stats()
	if stats.ActiveKeys != 2 {
		t.Fatalf("expected 2 active keys, got %d", stats.ActiveKeys)
	}
}

// --- Validator Tests ---

func TestValidateMethodNotAllowed(t *testing.T) {
	config := ValidationConfig{
		AllowedMethods: []string{"GET", "POST"},
	}

	handler := Validate(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("DELETE", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestValidateMissingHeader(t *testing.T) {
	config := ValidationConfig{
		RequiredHeaders: []string{"Authorization"},
	}

	handler := Validate(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestValidateBodySize(t *testing.T) {
	config := ValidationConfig{
		MaxBodySize: 10,
	}

	handler := Validate(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	body := strings.NewReader(strings.Repeat("x", 100))
	req := httptest.NewRequest("POST", "/", body)
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestValidateContentType(t *testing.T) {
	config := ValidationConfig{
		ContentTypeCheck: true,
	}

	handler := Validate(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("POST", "/", strings.NewReader("data"))
	// No Content-Type header
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestValidateQueryParams(t *testing.T) {
	config := ValidationConfig{
		MaxQueryParams: 2,
	}

	handler := Validate(config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/?a=1&b=2&c=3", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

// --- Version Tests ---

func TestVersionFromPath(t *testing.T) {
	api := DefaultAPIVersion()
	handler := Version(api)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := GetAPIVersion(r.Context())
		w.Write([]byte(v))
	}))

	req := httptest.NewRequest("GET", "/v1/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Body.String() != "1" {
		t.Fatalf("expected version '1', got '%s'", rec.Body.String())
	}
}

func TestVersionFromHeader(t *testing.T) {
	api := APIVersion{
		Header:  "X-API-Version",
		Current: "1",
		Valid:   []string{"1", "2"},
	}

	handler := Version(api)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := GetAPIVersion(r.Context())
		w.Write([]byte(v))
	}))

	req := httptest.NewRequest("GET", "/models", nil)
	req.Header.Set("X-API-Version", "2")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Body.String() != "2" {
		t.Fatalf("expected version '2', got '%s'", rec.Body.String())
	}
}

func TestVersionInvalid(t *testing.T) {
	api := DefaultAPIVersion()
	handler := Version(api)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	}))

	req := httptest.NewRequest("GET", "/v99/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestVersionDefault(t *testing.T) {
	api := DefaultAPIVersion()
	handler := Version(api)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := GetAPIVersion(r.Context())
		w.Write([]byte(v))
	}))

	req := httptest.NewRequest("GET", "/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Body.String() != "1" {
		t.Fatalf("expected default version '1', got '%s'", rec.Body.String())
	}
}

// --- Chain Tests ---

func TestChainOrder(t *testing.T) {
	var order []string

	mw1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "1-before")
			next.ServeHTTP(w, r)
			order = append(order, "1-after")
		})
	}

	mw2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "2-before")
			next.ServeHTTP(w, r)
			order = append(order, "2-after")
		})
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		w.Write([]byte("ok"))
	})

	chained := Chain(handler, mw1, mw2)
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	expected := "1-before 2-before handler 2-after 1-after"
	got := strings.Join(order, " ")
	if got != expected {
		t.Fatalf("expected order '%s', got '%s'", expected, got)
	}
}

func TestDefaultChain(t *testing.T) {
	config := DefaultChainConfig()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	chained := DefaultChain(handler, config)
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	// Should have request ID
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID in response")
	}

	// Should have version
	if rec.Header().Get("X-API-Version") == "" {
		t.Fatal("expected X-API-Version in response")
	}
}

// --- extractClientKey Tests ---

func TestExtractClientKeyXFF(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	key := extractClientKey(req)
	if key != "1.2.3.4" {
		t.Fatalf("expected '1.2.3.4', got '%s'", key)
	}
}

func TestExtractClientKeyXRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Real-IP", "9.8.7.6")
	key := extractClientKey(req)
	if key != "9.8.7.6" {
		t.Fatalf("expected '9.8.7.6', got '%s'", key)
	}
}

func TestExtractClientKeyRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "4.3.2.1:8080"
	key := extractClientKey(req)
	if key != "4.3.2.1" {
		t.Fatalf("expected '4.3.2.1', got '%s'", key)
	}
}

// --- Integration Test ---

func TestFullMiddlewareStack(t *testing.T) {
	config := DefaultChainConfig()

	var version string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = GetRequestID(r.Context()) // verify it exists
		version = GetAPIVersion(r.Context())
		w.Write([]byte("ok"))
	})

	chained := DefaultChain(handler, config)

	req := httptest.NewRequest("GET", "/v1/test", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Verify request ID was set
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID")
	}

	// Verify version was extracted
	if version != "1" {
		t.Fatalf("expected version '1', got '%s'", version)
	}

	// Verify CORS headers
	if rec.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("expected CORS headers")
	}

	// Verify rate limit headers
	if rec.Header().Get("X-RateLimit-Limit") == "" {
		t.Fatal("expected rate limit headers")
	}
}

func TestFullStackPanicRecovery(t *testing.T) {
	config := DefaultChainConfig()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("integration test panic")
	})

	chained := DefaultChain(handler, config)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 from panic, got %d", rec.Code)
	}

	// Should still have request ID even on panic
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID even on panic")
	}
}

func TestCORSHeadersWithRateLimit(t *testing.T) {
	config := DefaultChainConfig()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	chained := DefaultChain(handler, config)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()
	chained.ServeHTTP(rec, req)

	// Should have both CORS and rate limit headers
	if rec.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("expected CORS origin")
	}
	if rec.Header().Get("X-RateLimit-Limit") == "" {
		t.Fatal("expected rate limit")
	}
	if rec.Header().Get("X-RateLimit-Remaining") == "" {
		t.Fatal("expected rate limit remaining")
	}
}

func TestRateLimitCleanup(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         10,
		CleanupInterval:   100 * time.Millisecond,
	})

	rl.getBucket("test-key")

	stats := rl.Stats()
	if stats.ActiveKeys != 1 {
		t.Fatalf("expected 1 key, got %d", stats.ActiveKeys)
	}

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	stats = rl.Stats()
	if stats.ActiveKeys != 0 {
		t.Fatalf("expected 0 keys after cleanup, got %d", stats.ActiveKeys)
	}

	rl.Stop()
}
