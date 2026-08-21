package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
)

type fakeCredentialImportAdapter struct {
	id                 string
	refreshState       channels.State
	refreshErr         error
	refreshCalls       int
	lastRefreshAccount channels.Account
	schedulerAccounts  int
	schedulerSessions  int
	schedulerReq       channels.SchedulerSnapshotRequest
	protocolResult     *channels.AccountCredentialImport
	protocolErr        error
	protocolCalls      int
	protocolRaw        []string
	protocolProfiles   []channels.TransportProfile
}

type fakeTransport struct{}

func (fakeTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return &channels.OutboundResponse{Status: http.StatusOK, Headers: http.Header{}}, nil
}

func (a *fakeCredentialImportAdapter) ID() string { return a.id }

func (a *fakeCredentialImportAdapter) InboundPathPrefix() string { return "/channels/" + a.id }

func (a *fakeCredentialImportAdapter) SessionPolicy() channels.SessionPolicy {
	return channels.NoopSessionPolicy{TTL: time.Hour}
}

func (a *fakeCredentialImportAdapter) AuthFlow() channels.AuthFlow { return nil }

func (a *fakeCredentialImportAdapter) PrepareOutbound(context.Context, *channels.Lease, *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return nil, nil
}

func (a *fakeCredentialImportAdapter) ClassifyResponse(int, http.Header, []byte) channels.ResponseClass {
	return channels.ClassOk
}

func (a *fakeCredentialImportAdapter) RefreshAccountState(_ context.Context, acc channels.Account, _ channels.Transport) (channels.State, error) {
	a.refreshCalls++
	a.lastRefreshAccount = acc
	if a.refreshErr != nil {
		return nil, a.refreshErr
	}
	if a.refreshState != nil {
		return a.refreshState, nil
	}
	return channels.State{}, nil
}

func (a *fakeCredentialImportAdapter) ModelCatalog() []channels.ModelInfo {
	return []channels.ModelInfo{
		{ID: "demo/model", Aliases: []string{"demo"}, AgentID: "demo-agent", QuotaGroup: "unlimited", Enabled: true},
	}
}

func (a *fakeCredentialImportAdapter) SchedulerSnapshot(_ context.Context, req channels.SchedulerSnapshotRequest) (any, error) {
	a.schedulerAccounts = len(req.Accounts)
	a.schedulerSessions = len(req.Sessions)
	a.schedulerReq = req
	return map[string]any{
		"status":         "ok",
		"accounts_seen":  a.schedulerAccounts,
		"sessions_seen":  a.schedulerSessions,
		"queue_pressure": 0,
	}, nil
}

func (a *fakeCredentialImportAdapter) ImportAccountCredential(raw string) (*channels.AccountCredentialImport, error) {
	var parsed struct {
		AuthToken string `json:"authToken"`
		User      struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"user"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, err
	}
	return &channels.AccountCredentialImport{
		Name:       parsed.User.Name,
		Credential: parsed.AuthToken,
		Metadata: map[string]any{
			"auth_method":         "credential",
			"freebuff_user_id":    parsed.User.ID,
			"freebuff_user_name":  parsed.User.Name,
			"freebuff_user_email": parsed.User.Email,
		},
	}, nil
}

func (a *fakeCredentialImportAdapter) RunGitHubProtocolLogin(_ context.Context, raw string, _ channels.Transport, profile channels.TransportProfile) (*channels.AccountCredentialImport, error) {
	a.protocolCalls++
	a.protocolRaw = append(a.protocolRaw, raw)
	a.protocolProfiles = append(a.protocolProfiles, profile)
	if a.protocolErr != nil {
		return nil, a.protocolErr
	}
	if a.protocolResult != nil {
		return a.protocolResult, nil
	}
	return &channels.AccountCredentialImport{
		Name:       "Protocol User",
		Credential: "protocol-token",
		Metadata:   map[string]any{"freebuff_user_id": "protocol-user"},
	}, nil
}

type fakeProtocolStatusError struct {
	status string
	msg    string
}

func (e fakeProtocolStatusError) Error() string { return e.msg }

func (e fakeProtocolStatusError) ProtocolLoginStatus() string { return e.status }

func TestListFreeBuffModelsReturnsCatalog(t *testing.T) {
	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: "freebuff"}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(registry, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/freebuff/models", nil)
	rec := httptest.NewRecorder()

	handler.ListFreeBuffModels(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body []channels.ModelInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 || body[0].ID != "demo/model" || body[0].QuotaGroup != "unlimited" {
		t.Fatalf("catalog = %+v", body)
	}
}

func TestListFreeBuffSchedulerReturnsProviderSnapshot(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuff-scheduler-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "Ada",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	stateRepo := freebuffstate.NewRepo(db)
	if err := stateRepo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      freebuffstate.ChannelID,
		AccountID:      account.ID,
		LocalSessionID: "sess-1",
		State: channels.State{
			"freebuff_instance_id":     "inst-pro",
			"freebuff_model":           "deepseek/deepseek-v4-pro",
			"freebuff_status":          "active",
			"freebuff_expires_at_unix": time.Now().Add(30 * time.Minute).Unix(),
			"freebuff_rate_limits_by_model": map[string]any{
				"deepseek/deepseek-v4-pro": map[string]any{
					"model":       "deepseek/deepseek-v4-pro",
					"limit":       5,
					"resetAt":     time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
					"recentCount": 3,
				},
			},
		},
	}); err != nil {
		t.Fatalf("record freebuff state: %v", err)
	}
	adapter := &fakeCredentialImportAdapter{id: freebuffstate.ChannelID}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(registry, accounts.NewPool(accountRepo), nil, nil, nil, nil, WithFreeBuffStateRepo(stateRepo))

	req := httptest.NewRequest(http.MethodGet, "/api/admin/freebuff/scheduler", nil)
	rec := httptest.NewRecorder()
	handler.ListFreeBuffScheduler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" || body["accounts_seen"].(float64) != 1 {
		t.Fatalf("scheduler body = %+v", body)
	}
	if len(adapter.schedulerReq.Accounts) != 1 {
		t.Fatalf("scheduler request accounts = %+v, want one account", adapter.schedulerReq.Accounts)
	}
	facts := adapter.schedulerReq.Accounts[0].ProviderFacts
	if facts[freebuffstate.SchedulerFactPremiumWindowTouched] != true ||
		facts[freebuffstate.SchedulerFactPremiumRemaining] != 2 {
		t.Fatalf("scheduler provider facts = %+v, want persisted premium facts", facts)
	}
}

func TestAdminAuthKeyEndpointsCreateListAndDelete(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "admin-auth-keys.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	keyRepo := authkeys.NewRepo(db)
	handler := NewAdminHandler(nil, nil, nil, nil, nil, nil, WithAuthKeysRepo(keyRepo))
	createReq := httptest.NewRequest(http.MethodPost, "/api/admin/auth-keys", strings.NewReader(`{"name":"client"}`))
	createRec := httptest.NewRecorder()

	handler.CreateAuthKey(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", createRec.Code, createRec.Body.String())
	}
	var created authkeys.CreatedRecord
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created: %v", err)
	}
	if created.ID == "" || !strings.HasPrefix(created.Key, "sk-") || created.KeyPrefix == "" {
		t.Fatalf("created key = %+v", created)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/admin/auth-keys", nil)
	listRec := httptest.NewRecorder()
	handler.ListAuthKeys(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	if strings.Contains(listRec.Body.String(), created.Key) {
		t.Fatalf("list leaked secret key: %s", listRec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/admin/auth-keys/"+created.ID, nil)
	deleteReq.SetPathValue("id", created.ID)
	deleteRec := httptest.NewRecorder()
	handler.DeleteAuthKey(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d body=%s", deleteRec.Code, deleteRec.Body.String())
	}
}

func TestProxyPoolCreateAndAccountBindingAreRedacted(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-binding-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	proxyRepo := proxypool.NewRepo(db)
	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: freebuffstate.ChannelID}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		nil,
		WithProxyPoolRepo(proxyRepo),
	)

	createProxyReq := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies", strings.NewReader(`{
		"name":"HK 01",
		"proxy_url":"http://user:secret@example.com:7890",
		"notes":"ops"
	}`))
	createProxyRec := httptest.NewRecorder()
	handler.CreateFreeBuffProxy(createProxyRec, createProxyReq)
	if createProxyRec.Code != http.StatusCreated {
		t.Fatalf("create proxy status = %d body=%s", createProxyRec.Code, createProxyRec.Body.String())
	}
	if strings.Contains(createProxyRec.Body.String(), "secret") {
		t.Fatalf("proxy response leaked password: %s", createProxyRec.Body.String())
	}
	var proxy proxyEntryView
	if err := json.Unmarshal(createProxyRec.Body.Bytes(), &proxy); err != nil {
		t.Fatalf("decode proxy: %v", err)
	}
	if proxy.ID == "" || proxy.URLRedacted != "http://user:***@example.com:7890" {
		t.Fatalf("proxy view = %+v", proxy)
	}

	credential := `{"authToken":"token-1","user":{"id":"u1","name":"Ada","email":"ada@example.com"}}`
	createAccountReq := httptest.NewRequest(http.MethodPost, "/api/admin/accounts", strings.NewReader(fmt.Sprintf(`{
		"channel_id":"freebuff",
		"credential":%q,
		"proxy_id":%q
	}`, credential, proxy.ID)))
	createAccountRec := httptest.NewRecorder()
	handler.CreateAccount(createAccountRec, createAccountReq)
	if createAccountRec.Code != http.StatusCreated {
		t.Fatalf("create account status = %d body=%s", createAccountRec.Code, createAccountRec.Body.String())
	}
	if strings.Contains(createAccountRec.Body.String(), "secret") || strings.Contains(createAccountRec.Body.String(), "_proxy_url") {
		t.Fatalf("account response leaked proxy secret/runtime URL: %s", createAccountRec.Body.String())
	}
	var account accountView
	if err := json.Unmarshal(createAccountRec.Body.Bytes(), &account); err != nil {
		t.Fatalf("decode account: %v", err)
	}
	if account.ProxyBinding == nil || account.ProxyBinding.ProxyID != proxy.ID || account.ProxyBinding.Status != "active" {
		t.Fatalf("proxy binding = %+v", account.ProxyBinding)
	}
	if got, _ := proxypool.ProxyIDFromMetadata(account.Metadata); got != proxy.ID {
		t.Fatalf("metadata proxy_id = %q, want %q", got, proxy.ID)
	}
}

func TestImportFreeBuffProxiesReturnsEmptyArrays(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-import-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	handler := NewAdminHandler(
		channels.NewRegistry(),
		nil,
		nil,
		nil,
		nil,
		nil,
		WithProxyPoolRepo(proxypool.NewRepo(db)),
	)

	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies/import", strings.NewReader(`{
		"text":"HK 01 ---- http://user:secret@example.com:7890"
	}`))
	rec := httptest.NewRecorder()
	handler.ImportFreeBuffProxies(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("import status = %d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, `"failures":null`) || strings.Contains(body, `"proxies":null`) || strings.Contains(body, `"skipped_proxies":null`) {
		t.Fatalf("import response returned null arrays: %s", body)
	}
	var decoded importProxiesResp
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode import response: %v", err)
	}
	if decoded.Created != 1 || decoded.Skipped != 0 || len(decoded.Proxies) != 1 || len(decoded.Failures) != 0 {
		t.Fatalf("import response = %+v", decoded)
	}

	emptyReq := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies/import", strings.NewReader(`{"text":""}`))
	emptyRec := httptest.NewRecorder()
	handler.ImportFreeBuffProxies(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusOK {
		t.Fatalf("empty import status = %d body=%s", emptyRec.Code, emptyRec.Body.String())
	}
	emptyBody := emptyRec.Body.String()
	if strings.Contains(emptyBody, `"failures":null`) || strings.Contains(emptyBody, `"proxies":null`) || strings.Contains(emptyBody, `"skipped_proxies":null`) {
		t.Fatalf("empty import response returned null arrays: %s", emptyBody)
	}
}

func TestImportFreeBuffProxiesSkipsDuplicates(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-import-dedup-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	handler := NewAdminHandler(
		channels.NewRegistry(),
		nil,
		nil,
		nil,
		nil,
		nil,
		WithProxyPoolRepo(proxypool.NewRepo(db)),
	)

	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies/import", strings.NewReader(`{
		"text":"HK 01 ---- HTTP://user:secret@Example.COM:7890\nHK changed ---- http://user:secret@example.com:7890"
	}`))
	rec := httptest.NewRecorder()
	handler.ImportFreeBuffProxies(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("import status = %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("import response leaked password: %s", rec.Body.String())
	}
	var decoded importProxiesResp
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode import response: %v", err)
	}
	if decoded.Created != 1 || decoded.Skipped != 1 || len(decoded.Proxies) != 1 || len(decoded.SkippedProxies) != 1 {
		t.Fatalf("import response = %+v", decoded)
	}
	if decoded.SkippedProxies[0].Proxy.Name != "HK 01" {
		t.Fatalf("duplicate changed existing proxy: %+v", decoded.SkippedProxies[0].Proxy)
	}
}

func TestCreateFreeBuffProxyRejectsDuplicate(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-create-dedup-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	handler := NewAdminHandler(
		channels.NewRegistry(),
		nil,
		nil,
		nil,
		nil,
		nil,
		WithProxyPoolRepo(proxypool.NewRepo(db)),
	)

	body := `{"name":"HK 01","proxy_url":"http://user:secret@example.com:7890"}`
	firstReq := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies", strings.NewReader(body))
	firstRec := httptest.NewRecorder()
	handler.CreateFreeBuffProxy(firstRec, firstReq)
	if firstRec.Code != http.StatusCreated {
		t.Fatalf("first create status = %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	secondReq := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies", strings.NewReader(body))
	secondRec := httptest.NewRecorder()
	handler.CreateFreeBuffProxy(secondRec, secondReq)
	if secondRec.Code != http.StatusConflict {
		t.Fatalf("second create status = %d body=%s", secondRec.Code, secondRec.Body.String())
	}
}

func TestTestFreeBuffProxyUpdatesHealthFields(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-test-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://probe.test/json" {
			t.Fatalf("probe URL = %q", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","country":"United States","regionName":"Texas","city":"Dallas","query":"198.51.100.10"}`))
	}))
	defer proxyServer.Close()

	proxyRepo := proxypool.NewRepo(db)
	rec := &proxypool.Record{
		Name:     "Test proxy",
		ProxyURL: proxyServer.URL,
		IsActive: false,
	}
	if err := proxyRepo.Create(rec); err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	handler := NewAdminHandler(
		channels.NewRegistry(),
		nil,
		nil,
		nil,
		nil,
		nil,
		WithProxyPoolRepo(proxyRepo),
		WithProxyHealthcheckConfig(proxypool.CheckerConfig{
			ProbeURL: "http://probe.test/json",
			Timeout:  time.Second,
		}),
	)

	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/proxies/"+rec.ID+"/test", nil)
	req.SetPathValue("id", rec.ID)
	resp := httptest.NewRecorder()
	handler.TestFreeBuffProxy(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("test status = %d body=%s", resp.Code, resp.Body.String())
	}
	if strings.Contains(resp.Body.String(), "secret") {
		t.Fatalf("test response leaked proxy secret: %s", resp.Body.String())
	}
	var decoded proxyEntryView
	if err := json.Unmarshal(resp.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.HealthStatus != proxypool.HealthHealthy || decoded.LatencyMS == nil || *decoded.LatencyMS <= 0 {
		t.Fatalf("decoded health = %+v", decoded)
	}
	if decoded.ExitIP != "198.51.100.10" || decoded.Country != "United States" || decoded.Region != "Texas" || decoded.City != "Dallas" {
		t.Fatalf("decoded location = %+v", decoded)
	}
}

func TestRefreshFreeBuffAccountAppliesBoundProxy(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "proxy-refresh-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	proxyRepo := proxypool.NewRepo(db)
	proxy := &proxypool.Record{
		Name:     "HK 01",
		ProxyURL: "http://user:secret@example.com:7890",
		IsActive: true,
	}
	if err := proxyRepo.Create(proxy); err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "Ada",
		Credential: "secret-token",
		IsActive:   true,
		Metadata:   map[string]any{proxypool.MetadataProxyID: proxy.ID},
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	adapter := &fakeCredentialImportAdapter{id: freebuffstate.ChannelID}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		fakeTransport{},
		WithFreeBuffStateRepo(freebuffstate.NewRepo(db)),
		WithProxyPoolRepo(proxyRepo),
	)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/"+account.ID+"/refresh", nil)
	req.SetPathValue("id", account.ID)
	rec := httptest.NewRecorder()
	handler.RefreshFreeBuffAccount(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := proxypool.ProxyURLFromMetadata(adapter.lastRefreshAccount.Metadata); got != proxy.ProxyURL {
		t.Fatalf("refresh proxy URL = %q, want %q", got, proxy.ProxyURL)
	}
}

func TestListFreeBuffAccountsReturnsQuotaSnapshotsWithoutCredential(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuff-accounts-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "Ada",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	stateRepo := freebuffstate.NewRepo(db)
	if err := stateRepo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      freebuffstate.ChannelID,
		AccountID:      account.ID,
		LocalSessionID: "sess-1",
		State: channels.State{
			"freebuff_instance_id":      "inst-1",
			"freebuff_model":            "deepseek/deepseek-v4-pro",
			"freebuff_access_tier":      "limited",
			"freebuff_expires_at_unix":  int64(1778916612),
			"freebuff_raw_session_json": `{"instanceId":"inst-1"}`,
			"freebuff_rate_limits_by_model": map[string]any{
				"deepseek/deepseek-v4-pro": map[string]any{
					"model":         "deepseek/deepseek-v4-pro",
					"limit":         5,
					"period":        "pacific_day",
					"resetTimeZone": "America/Los_Angeles",
					"resetAt":       "2026-05-16T07:00:00.000Z",
					"windowHours":   24,
					"recentCount":   4,
				},
			},
		},
	}); err != nil {
		t.Fatalf("record state: %v", err)
	}

	handler := NewAdminHandler(
		nil,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		nil,
		WithFreeBuffStateRepo(stateRepo),
	)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/freebuff/accounts", nil)
	rec := httptest.NewRecorder()

	handler.ListFreeBuffAccounts(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret-token") || strings.Contains(rec.Body.String(), "raw_session_json") {
		t.Fatalf("response leaked secret/raw state: %s", rec.Body.String())
	}
	var body []freeBuffAccountView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 || body[0].ID != account.ID {
		t.Fatalf("accounts = %+v", body)
	}
	if body[0].UpstreamState == nil || body[0].UpstreamState.AccessTier != "limited" {
		t.Fatalf("upstream state = %+v", body[0].UpstreamState)
	}
	if len(body[0].QuotaSnapshots) != 1 || body[0].QuotaSnapshots[0].LimitCount != 5 {
		t.Fatalf("quota snapshots = %+v", body[0].QuotaSnapshots)
	}
	if len(body[0].UpstreamSessions) != 1 || body[0].UpstreamSessions[0].InstanceID != "inst-1" {
		t.Fatalf("upstream sessions = %+v", body[0].UpstreamSessions)
	}
}

func TestRefreshFreeBuffAccountPersistsUpstreamState(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuff-refresh-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "Ada",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	adapter := &fakeCredentialImportAdapter{
		id: freebuffstate.ChannelID,
		refreshState: channels.State{
			"freebuff_status":           "active",
			"freebuff_instance_id":      "inst-refresh",
			"freebuff_model":            "moonshotai/kimi-k2.6",
			"freebuff_access_tier":      "limited",
			"freebuff_expires_at_unix":  int64(1778916612),
			"freebuff_raw_session_json": `{"status":"active","instanceId":"inst-refresh"}`,
			"freebuff_rate_limits_by_model": map[string]any{
				"moonshotai/kimi-k2.6": map[string]any{
					"model":         "moonshotai/kimi-k2.6",
					"limit":         5,
					"period":        "pacific_day",
					"resetTimeZone": "America/Los_Angeles",
					"resetAt":       "2026-05-16T07:00:00.000Z",
					"windowHours":   24,
					"recentCount":   3,
				},
			},
		},
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	stateRepo := freebuffstate.NewRepo(db)
	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		noopTransport{},
		WithFreeBuffStateRepo(stateRepo),
	)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/"+account.ID+"/refresh", nil)
	req.SetPathValue("id", account.ID)
	rec := httptest.NewRecorder()

	handler.RefreshFreeBuffAccount(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if adapter.refreshCalls != 1 || adapter.lastRefreshAccount.ID != account.ID {
		t.Fatalf("refresh calls/account = %d/%+v", adapter.refreshCalls, adapter.lastRefreshAccount)
	}
	var body freeBuffAccountView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.UpstreamState == nil || body.UpstreamState.AccessTier != "limited" {
		t.Fatalf("upstream state = %+v", body.UpstreamState)
	}
	if len(body.QuotaSnapshots) != 1 || body.QuotaSnapshots[0].RecentCount != 3 {
		t.Fatalf("quota snapshots = %+v", body.QuotaSnapshots)
	}
	if len(body.UpstreamSessions) != 1 || body.UpstreamSessions[0].InstanceID != "inst-refresh" {
		t.Fatalf("upstream sessions = %+v", body.UpstreamSessions)
	}
	if strings.Contains(rec.Body.String(), "secret-token") || strings.Contains(rec.Body.String(), "raw_session_json") {
		t.Fatalf("response leaked secret/raw state: %s", rec.Body.String())
	}
}

func TestFreeBuffGitHubProtocolLoginRunsThroughRouteWithoutProxy(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "freebuff-protocol-login.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	systemLogRepo := systemlogs.NewRepo(db)
	adapter := &fakeCredentialImportAdapter{
		id: freebuffstate.ChannelID,
		protocolResult: &channels.AccountCredentialImport{
			Name:       "Ada Protocol",
			Credential: "freebuff-protocol-token",
			Metadata: map[string]any{
				"freebuff_user_id":    "fb-u1",
				"freebuff_user_email": "ada@example.com",
				"session_token":       "metadata-token",
				"totp_secret":         "metadata-totp",
				"safe_note":           "kept",
			},
		},
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}

	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		fakeTransport{},
		WithSystemLogsRepo(systemLogRepo),
	)
	mux := BuildRouter(handler, NewProxyHandler(registry, nil), nil, nil, nil, nil)

	credentials := "ada----pw-secret----totp-secret"
	body, err := json.Marshal(map[string]any{
		"credentials":  credentials,
		"skip_refresh": true,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/github-protocol-login", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var accepted freeBuffGitHubAutoLoginResp
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	if accepted.JobID == "" || accepted.MethodID != freeBuffGitHubLoginMethodProtocol || accepted.Completed {
		t.Fatalf("accepted response = %+v", accepted)
	}

	final := waitFreeBuffGitHubProtocolJobViaMux(t, mux, accepted.JobID)
	response := rec.Body.String() + "\n" + fmt.Sprintf("%+v", final)
	for _, secret := range []string{"pw-secret", "totp-secret", "freebuff-protocol-token", "metadata-token", "metadata-totp"} {
		if strings.Contains(response, secret) {
			t.Fatalf("response leaked %q: %s", secret, response)
		}
	}
	if final.Status != "ok" || len(final.Imported) != 1 || final.Imported[0].Status != "created" || len(final.Failures) != 0 {
		t.Fatalf("final response = %+v", final)
	}
	if adapter.protocolCalls != 1 || len(adapter.protocolRaw) != 1 || adapter.protocolRaw[0] != credentials {
		t.Fatalf("protocol calls/raw = %d/%+v", adapter.protocolCalls, adapter.protocolRaw)
	}

	records, err := accountRepo.ListByChannel(freebuffstate.ChannelID)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want one", len(records))
	}
	got := records[0]
	if got.Credential != "freebuff-protocol-token" {
		t.Fatalf("stored credential = %q, want protocol token", got.Credential)
	}
	if got.Metadata["auth_method"] != freeBuffGitHubLoginMethodProtocol || got.Metadata["github_login"] != "ada" || got.Metadata["safe_note"] != "kept" {
		t.Fatalf("metadata missing expected fields: %+v", got.Metadata)
	}
	if _, ok := got.Metadata["session_token"]; ok {
		t.Fatalf("metadata retained secret token: %+v", got.Metadata)
	}
	if _, ok := got.Metadata["totp_secret"]; ok {
		t.Fatalf("metadata retained totp secret: %+v", got.Metadata)
	}
	if _, ok := proxypool.ProxyIDFromMetadata(got.Metadata); ok {
		t.Fatalf("metadata has proxy id without proxy request: %+v", got.Metadata)
	}

	logs, err := systemLogRepo.List(systemlogs.Query{JobID: accepted.JobID, Limit: 20})
	if err != nil {
		t.Fatalf("list system logs: %v", err)
	}
	events := map[string]systemlogs.Entry{}
	for _, entry := range logs {
		events[entry.Event] = entry
		if strings.Contains(fmt.Sprintf("%+v", entry), "pw-secret") || strings.Contains(fmt.Sprintf("%+v", entry), "totp-secret") {
			t.Fatalf("system log leaked credentials: %+v", entry)
		}
	}
	if events["protocol_started"].Metadata["method_id"] != freeBuffGitHubLoginMethodProtocol || events["protocol_completed"].Metadata["method_id"] != freeBuffGitHubLoginMethodProtocol {
		t.Fatalf("protocol logs missing method metadata: %+v", logs)
	}
}

func TestFreeBuffGitHubProtocolLoginPassesOptionalProxy(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "freebuff-protocol-proxy.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	proxyRepo := proxypool.NewRepo(db)
	proxy := &proxypool.Record{
		Name:         "US 02",
		ProxyURL:     "socks5://user:secret@example.com:1080",
		IsActive:     true,
		HealthStatus: proxypool.HealthHealthy,
	}
	if err := proxyRepo.Create(proxy); err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	adapter := &fakeCredentialImportAdapter{
		id: freebuffstate.ChannelID,
		protocolResult: &channels.AccountCredentialImport{
			Name:       "Proxy User",
			Credential: "proxy-protocol-token",
			Metadata:   map[string]any{"freebuff_user_id": "fb-proxy"},
		},
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accountRepo),
		nil,
		nil,
		nil,
		fakeTransport{},
		WithProxyPoolRepo(proxyRepo),
	)

	body, err := json.Marshal(map[string]any{
		"credentials":  "proxy-user----pw-secret----totp-secret",
		"proxy_id":     proxy.ID,
		"skip_refresh": true,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/github-protocol-login", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	handler.FreeBuffGitHubProtocolLogin(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var accepted freeBuffGitHubAutoLoginResp
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	final := waitFreeBuffGitHubProtocolJob(t, handler, accepted.JobID)
	if final.Status != "ok" || len(final.Imported) != 1 {
		t.Fatalf("final response = %+v", final)
	}
	if len(adapter.protocolProfiles) != 1 || adapter.protocolProfiles[0].ProxyURL != proxy.ProxyURL {
		t.Fatalf("protocol profile proxy = %+v, want %q", adapter.protocolProfiles, proxy.ProxyURL)
	}
	if strings.Contains(fmt.Sprintf("%+v\n%s", final, rec.Body.String()), "secret") {
		t.Fatalf("protocol response leaked proxy or credential secret: initial=%s final=%+v", rec.Body.String(), final)
	}
	records, err := accountRepo.ListByChannel(freebuffstate.ChannelID)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want one", len(records))
	}
	if got, ok := proxypool.ProxyIDFromMetadata(records[0].Metadata); !ok || got != proxy.ID {
		t.Fatalf("stored proxy id = %q/%v, want %q metadata=%+v", got, ok, proxy.ID, records[0].Metadata)
	}
}

func TestFreeBuffGitHubProtocolLoginFailureStatusIsSanitized(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuff-protocol-failure.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	adapter := &fakeCredentialImportAdapter{
		id:          freebuffstate.ChannelID,
		protocolErr: fakeProtocolStatusError{status: "captcha_required", msg: "captcha for pw-secret and totp-secret at https://freebuff.com/callback?code=secret-code&state=secret-state"},
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewAdminHandler(registry, accounts.NewPool(accounts.NewRepo(db)), nil, nil, nil, fakeTransport{})
	body := strings.NewReader(`{"credentials":"ada----pw-secret----totp-secret","skip_refresh":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/github-protocol-login", body)
	rec := httptest.NewRecorder()
	handler.FreeBuffGitHubProtocolLogin(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var accepted freeBuffGitHubAutoLoginResp
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	final := waitFreeBuffGitHubProtocolJob(t, handler, accepted.JobID)
	if final.Status != "failed" || len(final.Failures) != 1 || final.Failures[0].Status != "captcha_required" {
		t.Fatalf("final response = %+v", final)
	}
	bodyText := fmt.Sprintf("%+v", final)
	for _, secret := range []string{"pw-secret", "totp-secret", "secret-code", "secret-state"} {
		if strings.Contains(bodyText, secret) {
			t.Fatalf("failure response leaked %q: %+v", secret, final)
		}
	}
	if !strings.Contains(bodyText, "code=[redacted]") || !strings.Contains(bodyText, "state=[redacted]") {
		t.Fatalf("failure response did not redact OAuth query values: %+v", final)
	}
}

func waitFreeBuffGitHubProtocolJob(t *testing.T, handler *AdminHandler, jobID string) freeBuffGitHubAutoLoginResp {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		req := httptest.NewRequest(http.MethodGet, "/api/admin/freebuff/accounts/github-protocol-login/"+jobID, nil)
		req.SetPathValue("jobID", jobID)
		rec := httptest.NewRecorder()
		handler.GetFreeBuffGitHubProtocolLogin(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("poll status = %d body=%s", rec.Code, rec.Body.String())
		}
		var final freeBuffGitHubAutoLoginResp
		if err := json.Unmarshal(rec.Body.Bytes(), &final); err != nil {
			t.Fatalf("decode poll response: %v", err)
		}
		if final.Completed {
			return final
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete; latest = %+v", final)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitFreeBuffGitHubProtocolJobViaMux(t *testing.T, mux http.Handler, jobID string) freeBuffGitHubAutoLoginResp {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		req := httptest.NewRequest(http.MethodGet, "/api/admin/freebuff/accounts/github-protocol-login/"+jobID, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("poll status = %d body=%s", rec.Code, rec.Body.String())
		}
		var final freeBuffGitHubAutoLoginResp
		if err := json.Unmarshal(rec.Body.Bytes(), &final); err != nil {
			t.Fatalf("decode poll response: %v", err)
		}
		if final.Completed {
			return final
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete; latest = %+v", final)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFreeBuffGitHubAutoLoginRoutesAreRemoved(t *testing.T) {
	handler := NewAdminHandler(nil, accounts.NewPool(accounts.NewRepo(nil)), nil, nil, nil, nil)
	mux := BuildRouter(handler, NewProxyHandler(nil, nil), nil, nil, nil, nil)

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/api/admin/freebuff/accounts/github-auto-login"},
		{method: http.MethodGet, path: "/api/admin/freebuff/accounts/github-auto-login/job-a"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d body=%s, want 404", tc.method, tc.path, rec.Code, rec.Body.String())
		}
	}
}

func TestFreeBuffGitHubProtocolLoginRejectsEmptyCredentials(t *testing.T) {
	handler := NewAdminHandler(nil, accounts.NewPool(accounts.NewRepo(nil)), nil, nil, nil, nil)
	body := strings.NewReader(`{"credentials":"  "}`)
	req := httptest.NewRequest(http.MethodPost, "/api/admin/freebuff/accounts/github-protocol-login", body)
	rec := httptest.NewRecorder()

	handler.FreeBuffGitHubProtocolLogin(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
}

func TestListLogsReturnsEmptyArray(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "usage-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	handler := NewAdminHandler(nil, nil, nil, logs.NewRepo(db), nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/logs?limit=100", nil)
	rec := httptest.NewRecorder()

	handler.ListLogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "[]\n" {
		t.Fatalf("body = %q, want []", rec.Body.String())
	}
}

func TestListSystemLogsFiltersByJob(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "systemlogs-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	systemLogRepo := systemlogs.NewRepo(db)
	if err := systemLogRepo.Append(systemlogs.Entry{
		Level:     systemlogs.LevelWarn,
		Component: freeBuffProtocolLoginLogComponent,
		Event:     "job_partial_failed",
		Message:   "partial",
		JobID:     "job-a",
		CreatedAt: 100,
	}); err != nil {
		t.Fatalf("append target log: %v", err)
	}
	if err := systemLogRepo.Append(systemlogs.Entry{
		Level:     systemlogs.LevelInfo,
		Component: freeBuffProtocolLoginLogComponent,
		Event:     "job_completed",
		Message:   "done",
		JobID:     "job-b",
		CreatedAt: 200,
	}); err != nil {
		t.Fatalf("append other log: %v", err)
	}

	handler := NewAdminHandler(nil, nil, nil, nil, nil, nil, WithSystemLogsRepo(systemLogRepo))
	req := httptest.NewRequest(http.MethodGet, "/api/admin/system-logs?component="+freeBuffProtocolLoginLogComponent+"&job_id=job-a&limit=100", nil)
	rec := httptest.NewRecorder()

	handler.ListSystemLogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body []systemlogs.Entry
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 || body[0].JobID != "job-a" || body[0].Event != "job_partial_failed" {
		t.Fatalf("body = %+v, want job-a log", body)
	}
}

func TestAppendSystemLogRedactsOAuthQueryValues(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "systemlogs-redact.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	systemLogRepo := systemlogs.NewRepo(db)
	handler := NewAdminHandler(nil, nil, nil, nil, nil, nil, WithSystemLogsRepo(systemLogRepo))
	handler.appendFreeBuffProtocolLoginLog("job-a", systemlogs.LevelWarn, "job_partial_failed", "failed url=https://freebuff.com/login?auth_code=secret-code", map[string]any{
		"first_failure": "callback url=https://freebuff.com/login?auth_code=secret-code&ok=1",
	})

	req := httptest.NewRequest(http.MethodGet, "/api/admin/system-logs?job_id=job-a", nil)
	rec := httptest.NewRecorder()
	handler.ListSystemLogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "secret-code") {
		t.Fatalf("system log leaked auth code: %s", body)
	}
	if !strings.Contains(body, "auth_code=[redacted]") {
		t.Fatalf("system log did not include redacted marker: %s", body)
	}
}

func TestListChannelsReturnsOperationalStats(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "channels-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: "demo"}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	accountRepo := accounts.NewRepo(db)
	activeAccount := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token-a",
		IsActive:   true,
	}
	inactiveAccount := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Grace",
		Credential: "token-b",
		IsActive:   false,
	}
	if err := accountRepo.Create(activeAccount); err != nil {
		t.Fatalf("create active account: %v", err)
	}
	if err := accountRepo.Create(inactiveAccount); err != nil {
		t.Fatalf("create inactive account: %v", err)
	}
	logRepo := logs.NewRepo(db)
	for _, entry := range []logs.Entry{
		{
			ChannelID:     "demo",
			AccountID:     activeAccount.ID,
			Method:        "POST",
			Path:          "/v1/chat",
			Status:        200,
			ResponseClass: "ok",
			TokensIn:      4,
			TokensOut:     6,
			TokensKnown:   true,
		},
		{
			ChannelID:     "demo",
			AccountID:     inactiveAccount.ID,
			Method:        "POST",
			Path:          "/v1/chat",
			Status:        429,
			ResponseClass: "rate_limited",
			TokensIn:      1,
			TokensOut:     2,
			TokensKnown:   true,
		},
	} {
		logRepo.Append(entry)
	}
	logRepo.Run(doneNow{})

	handler := NewAdminHandler(registry, accounts.NewPool(accountRepo), nil, logRepo, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/channels", nil)
	rec := httptest.NewRecorder()

	handler.ListChannels(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body []channelView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("channel count = %d, want 1: %+v", len(body), body)
	}
	got := body[0]
	if got.AccountCount != 2 || got.ActiveAccountCount != 1 {
		t.Fatalf("account stats = %d/%d, want 2/1", got.AccountCount, got.ActiveAccountCount)
	}
	if got.RequestCount != 2 || got.TotalTokens != 13 {
		t.Fatalf("usage stats = requests %d tokens %d, want 2/13", got.RequestCount, got.TotalTokens)
	}
	if got.EffectivePolicy.MaxConcurrentPerSession <= 0 {
		t.Fatalf("effective session policy missing: %+v", got.EffectivePolicy)
	}
}

func TestChannelConfigUpdatePersistsAndListShowsEffectivePolicy(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "channel-config-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: "demo"}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	channelRepo := channelconfig.NewRepo(db)
	handler := NewAdminHandler(
		registry,
		accounts.NewPool(accounts.NewRepo(db)),
		nil,
		nil,
		nil,
		nil,
		WithChannelConfigRepo(channelRepo),
	)
	body := strings.NewReader(`{"max_concurrent_per_session":2,"wait_on_full":true}`)
	req := httptest.NewRequest(http.MethodPut, "/api/admin/channels/demo/config", body)
	req.SetPathValue("id", "demo")
	rec := httptest.NewRecorder()

	handler.UpdateChannelConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	stored, err := channelRepo.Get("demo")
	if err != nil {
		t.Fatalf("get stored config: %v", err)
	}
	if stored.Config.MaxConcurrentPerSession != 2 || stored.Config.WaitOnFull == nil || !*stored.Config.WaitOnFull {
		t.Fatalf("stored config = %+v, want max=2 wait=true", stored.Config)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/admin/channels", nil)
	listRec := httptest.NewRecorder()
	handler.ListChannels(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listBody []channelView
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listBody) != 1 {
		t.Fatalf("channel count = %d, want 1", len(listBody))
	}
	got := listBody[0]
	if got.Config.MaxConcurrentPerSession != 2 || got.Config.WaitOnFull == nil || !*got.Config.WaitOnFull {
		t.Fatalf("list config = %+v", got.Config)
	}
	if got.EffectivePolicy.MaxConcurrentPerSession != 2 || !got.EffectivePolicy.WaitOnFull {
		t.Fatalf("effective policy = %+v, want max=2 wait=true", got.EffectivePolicy)
	}
}

func TestCreateAccountImportsProviderCredentialJSON(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: "demo"}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	repo := accounts.NewRepo(db)
	pool := accounts.NewPool(repo)
	handler := NewAdminHandler(registry, pool, nil, nil, nil, nil)

	credentialJSON := `{"authToken":"secret-token","user":{"id":"user-1","name":"Ada","email":"ada@example.test"}}`
	body, err := json.Marshal(map[string]any{
		"channel_id": "demo",
		"credential": credentialJSON,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/admin/accounts", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()

	handler.CreateAccount(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret-token") {
		t.Fatalf("response leaked credential: %s", rec.Body.String())
	}
	records, err := repo.ListAll()
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("account count = %d, want 1", len(records))
	}
	got := records[0]
	if got.Name != "Ada" {
		t.Fatalf("name = %q, want Ada", got.Name)
	}
	if got.Credential != "secret-token" {
		t.Fatalf("credential = %q, want imported token", got.Credential)
	}
	if got.Priority != 50 || got.RPMLimit != 0 || got.QuotaTotal != 0 || !got.IsActive {
		t.Fatalf("defaults = priority %d rpm %d quota %d active %t, want pool-managed defaults", got.Priority, got.RPMLimit, got.QuotaTotal, got.IsActive)
	}
	if got.Metadata["auth_method"] != "credential" || got.Metadata["freebuff_user_email"] != "ada@example.test" {
		t.Fatalf("metadata = %+v, want imported identity", got.Metadata)
	}
}

func TestBatchUpdateAccountsUpdatesPoolFields(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "batch-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token-a",
		IsActive:   true,
	}
	second := &accounts.Record{
		ChannelID:        "demo",
		Name:             "Grace",
		Credential:       "token-b",
		QuotaTotal:       50,
		QuotaPeriod:      "day",
		QuotaUsed:        30,
		QuotaPeriodStart: accounts.QuotaBucketStart(time.Now().AddDate(0, 0, -1), "day"),
		IsActive:         true,
	}
	for _, rec := range []*accounts.Record{first, second} {
		if err := repo.Create(rec); err != nil {
			t.Fatalf("create account %s: %v", rec.Name, err)
		}
	}
	handler := NewAdminHandler(nil, accounts.NewPool(repo), nil, nil, nil, nil)
	body, err := json.Marshal(map[string]any{
		"account_ids": []string{first.ID, second.ID, first.ID},
		"patch": map[string]any{
			"priority":                           80,
			"rpm_limit":                          25,
			"quota_total":                        1000,
			"quota_period":                       "week",
			"reset_quota_used":                   true,
			"is_active":                          false,
			"session_max_concurrent_per_session": 2,
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/admin/accounts/batch", strings.NewReader(string(body)))
	recorder := httptest.NewRecorder()

	handler.BatchUpdateAccounts(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var resp batchAccountUpdateResp
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Affected != 2 {
		t.Fatalf("affected = %d, want 2", resp.Affected)
	}
	for _, id := range []string{first.ID, second.ID} {
		got, err := repo.Get(id)
		if err != nil {
			t.Fatalf("get account %s: %v", id, err)
		}
		if got.Priority != 80 || got.RPMLimit != 25 || got.IsActive {
			t.Fatalf("account %s scheduling = priority %d rpm %d active %t", id, got.Priority, got.RPMLimit, got.IsActive)
		}
		if got.QuotaTotal != 1000 || got.QuotaPeriod != "week" || got.QuotaUsed != 0 || got.QuotaPeriodStart == 0 {
			t.Fatalf("account %s quota = total %d period %q used %d start %d", id, got.QuotaTotal, got.QuotaPeriod, got.QuotaUsed, got.QuotaPeriodStart)
		}
		sessionMeta, ok := got.Metadata["session"].(map[string]any)
		if !ok || sessionMeta["max_concurrent_per_session"].(float64) != 2 {
			t.Fatalf("account %s session metadata = %+v, want max concurrency 2", id, got.Metadata)
		}
	}
}

func TestUpdateAccountSessionConcurrencyOverride(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "account-session-config-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token-a",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	handler := NewAdminHandler(nil, accounts.NewPool(repo), nil, nil, nil, nil)

	body := strings.NewReader(`{"session_max_concurrent_per_session":3}`)
	req := httptest.NewRequest(http.MethodPut, "/api/admin/accounts/"+account.ID, body)
	req.SetPathValue("id", account.ID)
	rec := httptest.NewRecorder()
	handler.UpdateAccount(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var view accountView
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode account view: %v", err)
	}
	if view.SessionConfig.MaxConcurrentPerSession == nil || *view.SessionConfig.MaxConcurrentPerSession != 3 {
		t.Fatalf("session config = %+v, want max 3", view.SessionConfig)
	}

	body = strings.NewReader(`{"clear_session_max_concurrent_per_session":true}`)
	req = httptest.NewRequest(http.MethodPut, "/api/admin/accounts/"+account.ID, body)
	req.SetPathValue("id", account.ID)
	rec = httptest.NewRecorder()
	handler.UpdateAccount(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("clear status = %d body=%s", rec.Code, rec.Body.String())
	}
	got, err := repo.Get(account.ID)
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	if _, exists := got.Metadata["session"]; exists {
		t.Fatalf("session metadata was not cleared: %+v", got.Metadata)
	}
}

func TestBatchUpdateAccountsRejectsQuotaWithoutPeriod(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "batch-validation-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token-a",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	handler := NewAdminHandler(nil, accounts.NewPool(repo), nil, nil, nil, nil)
	body, err := json.Marshal(map[string]any{
		"account_ids": []string{account.ID},
		"patch": map[string]any{
			"quota_total": 1000,
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/admin/accounts/batch", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()

	handler.BatchUpdateAccounts(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
	got, err := repo.Get(account.ID)
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	if got.QuotaTotal != 0 {
		t.Fatalf("quota changed to %d, want unchanged zero", got.QuotaTotal)
	}
}

func TestListUsageAccountsEnrichesLogStatsWithAccounts(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "usage-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "secret",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	logRepo := logs.NewRepo(db)
	logRepo.Append(logs.Entry{
		ChannelID:     "demo",
		AccountID:     account.ID,
		Method:        "POST",
		Path:          "/v1/chat",
		Model:         "model-a",
		Status:        200,
		ResponseClass: "ok",
		LatencyMS:     25,
		TokensIn:      4,
		TokensOut:     6,
		TokensKnown:   true,
		CreatedAt:     time.Now().Unix(),
	})
	logRepo.Run(doneNow{})

	handler := NewAdminHandler(nil, accounts.NewPool(accountRepo), nil, logRepo, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/usage/accounts?range=all", nil)
	rec := httptest.NewRecorder()

	handler.ListUsageAccounts(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body []usageAccountView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("body count = %d, want 1: %+v", len(body), body)
	}
	if body[0].AccountName != "Ada" || body[0].TotalRequests != 1 || body[0].TopModel != "model-a" {
		t.Fatalf("unexpected usage account body: %+v", body[0])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/admin/usage/accounts?range=all&search=%2Fv1%2Fchat", nil)
	rec = httptest.NewRecorder()

	handler.ListUsageAccounts(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("search status = %d body=%s", rec.Code, rec.Body.String())
	}
	body = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode search body: %v", err)
	}
	if len(body) != 1 || body[0].AccountID != account.ID {
		t.Fatalf("search body = %+v, want matched account usage", body)
	}
}

func TestListUsageEventsIncludesFirstResponseAndTokens(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "usage-events-api.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "secret",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	logRepo := logs.NewRepo(db)
	logRepo.Append(logs.Entry{
		ChannelID:       "demo",
		AccountID:       account.ID,
		Method:          "POST",
		Path:            "/v1/chat",
		Stream:          true,
		Model:           "model-a",
		Status:          200,
		ResponseClass:   "ok",
		LatencyMS:       45,
		FirstResponseMS: 11,
		TokensIn:        7,
		TokensOut:       9,
		TokensKnown:     true,
		PhaseTimings: map[string]any{
			"session_acquire_ms": int64(3),
			"first_content_ms":   int64(14),
		},
		CreatedAt: time.Now().Unix(),
	})
	logRepo.Run(doneNow{})

	handler := NewAdminHandler(nil, accounts.NewPool(accountRepo), nil, logRepo, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/admin/usage/events?range=all", nil)
	rec := httptest.NewRecorder()

	handler.ListUsageEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body []usageEventView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("body count = %d, want 1: %+v", len(body), body)
	}
	got := body[0]
	if got.AccountName != "Ada" || got.FirstResponseMS != 11 || got.TokensIn != 7 || got.TokensOut != 9 || got.TokensTotal != 16 {
		t.Fatalf("unexpected usage event body: %+v", got)
	}
	if got.PhaseTimings == nil || int(got.PhaseTimings["first_content_ms"].(float64)) != 14 {
		t.Fatalf("unexpected phase timings: %+v", got.PhaseTimings)
	}
}

type doneNow struct{}

func (doneNow) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
