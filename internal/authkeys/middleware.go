package authkeys

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Context key types
type contextKey string

const (
	// AuthenticatedUserKey is the context key for the authenticated user
	AuthenticatedUserKey contextKey = "authenticated_user"
	// AuthMethodKey is the context key for the authentication method used
	AuthMethodKey contextKey = "auth_method"
)

// AuthMethod represents the authentication method used
type AuthMethod string

const (
	AuthMethodNone     AuthMethod = "none"
	AuthMethodAPIKey   AuthMethod = "api_key"
	AuthMethodBearer   AuthMethod = "bearer"
	AuthMethodAdmin    AuthMethod = "admin_session"
	AuthMethodInternal AuthMethod = "internal"
)

// AuthenticatedUser represents an authenticated user
type AuthenticatedUser struct {
	KeyID      string     `json:"key_id"`
	KeyName    string     `json:"key_name"`
	AuthMethod AuthMethod `json:"auth_method"`
	AuthTime   time.Time  `json:"auth_time"`
}

// MiddlewareConfig holds configuration for auth middleware
type MiddlewareConfig struct {
	// SkipPaths lists paths that should skip authentication
	SkipPaths []string
	// SkipMethods lists methods that should skip authentication
	SkipMethods []string
	// RequireAuth enables/disables auth requirement
	RequireAuth bool
}

// DefaultMiddlewareConfig returns default middleware configuration
func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		SkipPaths: []string{
			"/healthz",
			"/readyz",
			"/favicon.ico",
		},
		SkipMethods: []string{
			"OPTIONS",
		},
		RequireAuth: true,
	}
}

// Middleware is the main auth middleware
type Middleware struct {
	repo   *Repo
	config MiddlewareConfig
	mu     sync.RWMutex
}

// NewMiddleware creates a new auth middleware
func NewMiddleware(repo *Repo, config MiddlewareConfig) *Middleware {
	return &Middleware{
		repo:   repo,
		config: config,
	}
}

// Wrap wraps an http.Handler with authentication
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if path should skip auth
		if m.shouldSkip(r) {
			next.ServeHTTP(w, r)
			return
		}

		// Try to authenticate
		user, method := m.authenticate(r)

		if user == nil && m.config.RequireAuth {
			http.Error(w, `{"error": "unauthorized", "message": "valid API key required"}`, http.StatusUnauthorized)
			return
		}

		// Add user to context
		if user != nil {
			ctx := context.WithValue(r.Context(), AuthenticatedUserKey, user)
			ctx = context.WithValue(ctx, AuthMethodKey, method)
			r = r.WithContext(ctx)
		}

		next.ServeHTTP(w, r)
	})
}

// WrapFunc wraps an http.HandlerFunc with authentication
func (m *Middleware) WrapFunc(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.Wrap(http.HandlerFunc(next)).ServeHTTP(w, r)
	}
}

// authenticate tries to authenticate the request
func (m *Middleware) authenticate(r *http.Request) (*AuthenticatedUser, AuthMethod) {
	// Try Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		// Bearer token
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if user := m.validateToken(token); user != nil {
				return user, AuthMethodBearer
			}
		}
	}

	// Try X-API-Key header
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		if user := m.validateKey(apiKey); user != nil {
			return user, AuthMethodAPIKey
		}
	}

	// Try query parameter
	if apiKey := r.URL.Query().Get("api_key"); apiKey != "" {
		if user := m.validateKey(apiKey); user != nil {
			return user, AuthMethodAPIKey
		}
	}

	return nil, AuthMethodNone
}

// validateToken validates a bearer token
func (m *Middleware) validateToken(token string) *AuthenticatedUser {
	return m.validateKey(token)
}

// validateKey validates an API key
func (m *Middleware) validateKey(key string) *AuthenticatedUser {
	if m.repo == nil {
		return nil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}

	// Use the existing Authenticate method
	rec, err := m.repo.Authenticate(key)
	if err != nil {
		return nil
	}

	return &AuthenticatedUser{
		KeyID:      rec.ID,
		KeyName:    rec.Name,
		AuthMethod: AuthMethodAPIKey,
		AuthTime:   time.Now(),
	}
}

// shouldSkip returns true if the request should skip authentication
func (m *Middleware) shouldSkip(r *http.Request) bool {
	// Skip methods
	for _, method := range m.config.SkipMethods {
		if r.Method == method {
			return true
		}
	}

	// Skip paths
	path := r.URL.Path
	for _, skipPath := range m.config.SkipPaths {
		if path == skipPath || strings.HasPrefix(path, skipPath+"/") {
			return true
		}
	}

	return false
}

// GetUser returns the authenticated user from context
func GetUser(ctx context.Context) *AuthenticatedUser {
	user, _ := ctx.Value(AuthenticatedUserKey).(*AuthenticatedUser)
	return user
}

// GetAuthMethod returns the auth method from context
func GetAuthMethod(ctx context.Context) AuthMethod {
	method, _ := ctx.Value(AuthMethodKey).(AuthMethod)
	return method
}

// UpdateConfig updates the middleware configuration
func (m *Middleware) UpdateConfig(config MiddlewareConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = config
}

// AddSkipPath adds a path to skip list
func (m *Middleware) AddSkipPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.SkipPaths = append(m.config.SkipPaths, path)
}

// RemoveSkipPath removes a path from skip list
func (m *Middleware) RemoveSkipPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, p := range m.config.SkipPaths {
		if p == path {
			m.config.SkipPaths = append(m.config.SkipPaths[:i], m.config.SkipPaths[i+1:]...)
			return
		}
	}
}

// Suppress unused import warnings
var _ = context.Background
