package authkeys

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_SkipPaths(t *testing.T) {
	m := NewMiddleware(nil, DefaultMiddlewareConfig())

	tests := []struct {
		path   string
		method string
		skip   bool
	}{
		{"/healthz", "GET", true},
		{"/readyz", "GET", true},
		{"/favicon.ico", "GET", true},
		{"/v1/models", "GET", false},
		{"/v1/chat/completions", "POST", false},
		{"/api/admin/login", "POST", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			got := m.shouldSkip(req)
			if got != tt.skip {
				t.Errorf("shouldSkip(%s %s) = %v, want %v", tt.method, tt.path, got, tt.skip)
			}
		})
	}
}

func TestMiddleware_SkipMethods(t *testing.T) {
	m := NewMiddleware(nil, DefaultMiddlewareConfig())

	req := httptest.NewRequest("OPTIONS", "/v1/models", nil)
	if !m.shouldSkip(req) {
		t.Error("OPTIONS method should be skipped")
	}

	req = httptest.NewRequest("GET", "/v1/models", nil)
	if m.shouldSkip(req) {
		t.Error("GET method should not be skipped")
	}
}

func TestMiddleware_Authenticate_NoKey(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: false,
	})

	req := httptest.NewRequest("GET", "/v1/models", nil)
	user, method := m.authenticate(req)

	if user != nil {
		t.Error("Expected nil user without API key")
	}
	if method != AuthMethodNone {
		t.Errorf("Expected AuthMethodNone, got %v", method)
	}
}

func TestMiddleware_Authenticate_BearerToken(t *testing.T) {
	// Create a mock repo with a test key
	// Note: This test validates the middleware logic, not the actual key validation
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: false,
	})

	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test-key")

	// Without a real repo, this will return nil (key not found)
	user, method := m.authenticate(req)
	if user != nil {
		t.Error("Expected nil user with invalid key")
	}
	if method != AuthMethodNone {
		t.Errorf("Expected AuthMethodNone, got %v", method)
	}
}

func TestMiddleware_Authenticate_XAPIKey(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: false,
	})

	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("X-API-Key", "sk-test-key")

	// Without a real repo, this will return nil
	user, method := m.authenticate(req)
	if user != nil {
		t.Error("Expected nil user with invalid key")
	}
	if method != AuthMethodNone {
		t.Errorf("Expected AuthMethodNone, got %v", method)
	}
}

func TestMiddleware_Authenticate_QueryParam(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: false,
	})

	req := httptest.NewRequest("GET", "/v1/models?api_key=sk-test-key", nil)

	// Without a real repo, this will return nil
	user, method := m.authenticate(req)
	if user != nil {
		t.Error("Expected nil user with invalid key")
	}
	if method != AuthMethodNone {
		t.Errorf("Expected AuthMethodNone, got %v", method)
	}
}

func TestMiddleware_Wrap_SkipPath(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{"/healthz"},
		SkipMethods: []string{},
		RequireAuth: true,
	})

	handler := m.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 for skipped path, got %d", w.Code)
	}
}

func TestMiddleware_Wrap_RequireAuth(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: true,
	})

	handler := m.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/v1/models", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 without auth, got %d", w.Code)
	}
}

func TestMiddleware_Wrap_NoAuthRequired(t *testing.T) {
	m := NewMiddleware(nil, MiddlewareConfig{
		SkipPaths:   []string{},
		SkipMethods: []string{},
		RequireAuth: false,
	})

	handler := m.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/v1/models", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 when auth not required, got %d", w.Code)
	}
}

func TestDefaultMiddlewareConfig(t *testing.T) {
	config := DefaultMiddlewareConfig()

	if !config.RequireAuth {
		t.Error("Default RequireAuth should be true")
	}
	if len(config.SkipPaths) == 0 {
		t.Error("Default SkipPaths should not be empty")
	}
}

func TestMiddleware_UpdateConfig(t *testing.T) {
	m := NewMiddleware(nil, DefaultMiddlewareConfig())

	newConfig := MiddlewareConfig{
		SkipPaths:   []string{"/custom"},
		SkipMethods: []string{},
		RequireAuth: false,
	}

	m.UpdateConfig(newConfig)

	// Verify the config was updated
	req := httptest.NewRequest("GET", "/custom", nil)
	if !m.shouldSkip(req) {
		t.Error("Custom skip path should be skipped")
	}
}

func TestMiddleware_RemoveSkipPath(t *testing.T) {
	m := NewMiddleware(nil, DefaultMiddlewareConfig())

	m.AddSkipPath("/custom")
	m.RemoveSkipPath("/healthz")

	req := httptest.NewRequest("GET", "/custom", nil)
	if !m.shouldSkip(req) {
		t.Error("Custom path should be skipped")
	}

	req = httptest.NewRequest("GET", "/healthz", nil)
	if m.shouldSkip(req) {
		t.Error("/healthz should not be skipped after removal")
	}
}

func TestGetUser_NoUser(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	user := GetUser(req.Context())
	if user != nil {
		t.Error("Expected nil user from empty context")
	}
}

func TestGetAuthMethod_NoMethod(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	method := GetAuthMethod(req.Context())
	if method != "" {
		t.Errorf("Expected empty auth method, got %v", method)
	}
}
