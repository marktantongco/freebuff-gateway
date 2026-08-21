package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
)

const adminSessionCookie = "freebuffreverse_admin"

type AdminAuthenticator struct {
	password string
	ttl      time.Duration

	mu       sync.Mutex
	sessions map[string]time.Time
}

type adminAuthStatus struct {
	Authenticated bool  `json:"authenticated"`
	ExpiresAt     int64 `json:"expires_at,omitempty"`
}

func NewAdminAuthenticator(password string, ttl time.Duration) *AdminAuthenticator {
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &AdminAuthenticator{
		password: strings.TrimSpace(password),
		ttl:      ttl,
		sessions: make(map[string]time.Time),
	}
}

func (a *AdminAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if !a.passwordMatches(req.Password) {
		writeJSONError(w, http.StatusUnauthorized, "invalid password")
		return
	}
	token, err := randomToken()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	expires := time.Now().Add(a.ttl)
	a.mu.Lock()
	a.sessions[tokenHash(token)] = expires
	a.mu.Unlock()
	http.SetCookie(w, a.sessionCookie(r, token, expires, int(a.ttl.Seconds())))
	writeJSON(w, http.StatusOK, adminAuthStatus{Authenticated: true, ExpiresAt: expires.Unix()})
}

func (a *AdminAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	if token, ok := a.sessionToken(r); ok {
		a.mu.Lock()
		delete(a.sessions, tokenHash(token))
		a.mu.Unlock()
	}
	http.SetCookie(w, a.sessionCookie(r, "", time.Unix(0, 0), -1))
	w.WriteHeader(http.StatusNoContent)
}

func (a *AdminAuthenticator) Me(w http.ResponseWriter, r *http.Request) {
	if expires, ok := a.validSession(r); ok {
		writeJSON(w, http.StatusOK, adminAuthStatus{Authenticated: true, ExpiresAt: expires.Unix()})
		return
	}
	writeJSON(w, http.StatusOK, adminAuthStatus{Authenticated: false})
}

func (a *AdminAuthenticator) Require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := a.validSession(r); !ok {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (a *AdminAuthenticator) passwordMatches(password string) bool {
	expected := []byte(a.password)
	actual := []byte(strings.TrimSpace(password))
	if len(expected) == 0 {
		return false
	}
	if len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare(expected, actual) == 1
}

func (a *AdminAuthenticator) validSession(r *http.Request) (time.Time, bool) {
	token, ok := a.sessionToken(r)
	if !ok {
		return time.Time{}, false
	}
	hash := tokenHash(token)
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	expires, ok := a.sessions[hash]
	if !ok {
		return time.Time{}, false
	}
	if !expires.After(now) {
		delete(a.sessions, hash)
		return time.Time{}, false
	}
	return expires, true
}

func (a *AdminAuthenticator) sessionToken(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(adminSessionCookie)
	if err != nil {
		return "", false
	}
	token := strings.TrimSpace(cookie.Value)
	return token, token != ""
}

func (a *AdminAuthenticator) sessionCookie(r *http.Request, token string, expires time.Time, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     adminSessionCookie,
		Value:    token,
		Path:     "/",
		Expires:  expires,
		MaxAge:   maxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   requestIsSecure(r),
	}
}

type APIKeyAuthenticator struct {
	repo *authkeys.Repo
}

func NewAPIKeyAuthenticator(repo *authkeys.Repo) *APIKeyAuthenticator {
	return &APIKeyAuthenticator{repo: repo}
}

func (a *APIKeyAuthenticator) Require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a == nil || a.repo == nil {
			writeJSONError(w, http.StatusUnauthorized, "api key required")
			return
		}
		key := extractAPIKey(r)
		if key == "" {
			writeJSONError(w, http.StatusUnauthorized, "api key required")
			return
		}
		if _, err := a.repo.Authenticate(key); err != nil {
			writeJSONError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		next(w, r)
	}
}

func extractAPIKey(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[len("Bearer "):])
	}
	if key := strings.TrimSpace(r.Header.Get("x-api-key")); key != "" {
		return key
	}
	if key := strings.TrimSpace(r.Header.Get("api-key")); key != "" {
		return key
	}
	return ""
}

func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func requestIsSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}
