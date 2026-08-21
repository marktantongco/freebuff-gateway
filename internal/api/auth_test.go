package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func TestAdminAuthenticatorLoginMeAndRequire(t *testing.T) {
	auth := NewAdminAuthenticator("admin-pass", time.Hour)

	loginReq := httptest.NewRequest(http.MethodPost, "/api/admin/login", strings.NewReader(`{"password":"admin-pass"}`))
	loginRec := httptest.NewRecorder()
	auth.Login(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login status = %d body=%s", loginRec.Code, loginRec.Body.String())
	}
	cookies := loginRec.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != adminSessionCookie {
		t.Fatalf("login cookies = %+v", cookies)
	}

	meReq := httptest.NewRequest(http.MethodGet, "/api/admin/me", nil)
	meReq.AddCookie(cookies[0])
	meRec := httptest.NewRecorder()
	auth.Me(meRec, meReq)
	if meRec.Code != http.StatusOK || !strings.Contains(meRec.Body.String(), `"authenticated":true`) {
		t.Fatalf("me = %d %s", meRec.Code, meRec.Body.String())
	}

	protected := auth.Require(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	protectedReq := httptest.NewRequest(http.MethodGet, "/api/admin/channels", nil)
	protectedReq.AddCookie(cookies[0])
	protectedRec := httptest.NewRecorder()
	protected(protectedRec, protectedReq)
	if protectedRec.Code != http.StatusNoContent {
		t.Fatalf("protected status = %d body=%s", protectedRec.Code, protectedRec.Body.String())
	}
}

func TestAPIKeyAuthenticatorSupportsBearerAndXAPIKey(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "api-auth.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := authkeys.NewRepo(db)
	if _, err := repo.CreateWithKey("client", "sk-router-test"); err != nil {
		t.Fatalf("create key: %v", err)
	}
	auth := NewAPIKeyAuthenticator(repo)
	handler := auth.Require(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	for _, tc := range []struct {
		name   string
		header string
		value  string
	}{
		{name: "bearer", header: "Authorization", value: "Bearer sk-router-test"},
		{name: "x-api-key", header: "x-api-key", value: "sk-router-test"},
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set(tc.header, tc.value)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s status = %d body=%s", tc.name, rec.Code, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing key status = %d", rec.Code)
	}
}
