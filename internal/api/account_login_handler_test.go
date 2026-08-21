package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

type fakeAccountLoginAdapter struct {
	id     string
	result *channels.AccountLoginResult
}

func (a *fakeAccountLoginAdapter) ID() string { return a.id }

func (a *fakeAccountLoginAdapter) InboundPathPrefix() string { return "/channels/" + a.id }

func (a *fakeAccountLoginAdapter) SessionPolicy() channels.SessionPolicy {
	return channels.NoopSessionPolicy{TTL: time.Hour}
}

func (a *fakeAccountLoginAdapter) AuthFlow() channels.AuthFlow { return nil }

func (a *fakeAccountLoginAdapter) PrepareOutbound(_ context.Context, _ *channels.Lease, _ *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return nil, nil
}

func (a *fakeAccountLoginAdapter) ClassifyResponse(int, http.Header, []byte) channels.ResponseClass {
	return channels.ClassOk
}

func (a *fakeAccountLoginAdapter) AccountAuthMethods() []channels.AccountAuthMethod {
	return []channels.AccountAuthMethod{
		{
			ID:                 "token",
			Label:              "Token",
			Kind:               channels.AccountAuthKindCredential,
			RequiresCredential: true,
		},
		{
			ID:                 "external",
			Label:              "External",
			Kind:               channels.AccountAuthKindExternalLink,
			CompletionMode:     channels.AccountLoginCompletionPoll,
			RequiresCredential: false,
		},
	}
}

func (a *fakeAccountLoginAdapter) StartAccountLogin(context.Context, string, channels.Transport) (*channels.AccountLoginStartResult, error) {
	return &channels.AccountLoginStartResult{
		SessionID:        "login-1",
		LoginURL:         "https://provider.test/login",
		ExpiresAt:        time.Now().Add(time.Minute).Unix(),
		PollAfterSeconds: 3,
		CompletionMode:   channels.AccountLoginCompletionPoll,
	}, nil
}

func (a *fakeAccountLoginAdapter) PollAccountLogin(context.Context, string, channels.Transport) (*channels.AccountLoginResult, error) {
	return a.result, nil
}

func (a *fakeAccountLoginAdapter) CompleteAccountLogin(context.Context, string, channels.AccountLoginCompleteRequest, channels.Transport) (*channels.AccountLoginResult, error) {
	return a.result, nil
}

type noopTransport struct{}

func (noopTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return &channels.OutboundResponse{Status: http.StatusOK, Headers: http.Header{}}, nil
}

func TestAccountLoginCreatesAccountWithoutTokenInResponse(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	registry := channels.NewRegistry()
	adapter := &fakeAccountLoginAdapter{
		id: "demo",
		result: &channels.AccountLoginResult{
			Completed:  true,
			Credential: "secret-token",
			UserName:   "Ada",
			UserEmail:  "ada@example.test",
			UserID:     "user-1",
			Metadata: map[string]any{
				"auth_method": "external",
				"user_id":     "user-1",
			},
		},
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	repo := accounts.NewRepo(db)
	pool := accounts.NewPool(repo)
	tp := noopTransport{}
	sm := session.NewManager(registry, pool, tp, session.Config{})
	handler := NewAdminHandler(registry, pool, sm, nil, nil, tp)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/admin/channels/{id}/account-logins", handler.StartAccountLogin)
	mux.HandleFunc("GET /api/admin/channels/{id}/account-logins/{sessionID}", handler.PollAccountLogin)

	startBody := `{"method_id":"external","priority":70}`
	startReq := httptest.NewRequest(http.MethodPost, "/api/admin/channels/demo/account-logins", strings.NewReader(startBody))
	startRec := httptest.NewRecorder()
	mux.ServeHTTP(startRec, startReq)
	if startRec.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", startRec.Code, startRec.Body.String())
	}

	pollReq := httptest.NewRequest(http.MethodGet, "/api/admin/channels/demo/account-logins/login-1", nil)
	pollRec := httptest.NewRecorder()
	mux.ServeHTTP(pollRec, pollReq)
	if pollRec.Code != http.StatusOK {
		t.Fatalf("poll status = %d body=%s", pollRec.Code, pollRec.Body.String())
	}
	if strings.Contains(pollRec.Body.String(), "secret-token") {
		t.Fatalf("response leaked credential: %s", pollRec.Body.String())
	}

	var response accountLoginStatusResp
	if err := json.Unmarshal(pollRec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Completed || response.Account == nil || response.UserName != "Ada" {
		t.Fatalf("unexpected response: %+v", response)
	}

	records, err := repo.ListAll()
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("account count = %d, want 1", len(records))
	}
	if records[0].Credential != "secret-token" {
		t.Fatalf("credential was not stored")
	}
	if records[0].Name != "Ada" {
		t.Fatalf("name = %q, want Ada", records[0].Name)
	}
	if records[0].Metadata["auth_method"] != "external" {
		t.Fatalf("metadata was not merged: %+v", records[0].Metadata)
	}
}
