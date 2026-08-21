package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
	"github.com/marktantongco/freebuff-gateway/internal/metrics"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/runtimeconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
)

type AdminHandler struct {
	registry       *channels.Registry
	accounts       *accounts.Pool
	sessions       *session.Manager
	channelConfigs *channelconfig.Repo
	freebuffStates *freebuffstate.Repo
	proxies        *proxypool.Repo
	proxyResolver  *proxypool.Resolver
	authKeys       *authkeys.Repo
	logs           *logs.Repo
	systemLogs     *systemlogs.Repo
	metrics        MetricsReader
	transport      channels.Transport

	proxyCheckConfig proxypool.CheckerConfig

	loginMu       sync.Mutex
	loginDrafts   map[string]accountLoginDraft
	autoLoginMu   sync.Mutex
	autoLoginJobs map[string]*freeBuffAutoLoginJob
}

type AdminOption func(*AdminHandler)

func WithChannelConfigRepo(repo *channelconfig.Repo) AdminOption {
	return func(h *AdminHandler) {
		h.channelConfigs = repo
	}
}

func WithFreeBuffStateRepo(repo *freebuffstate.Repo) AdminOption {
	return func(h *AdminHandler) {
		h.freebuffStates = repo
	}
}

func WithAuthKeysRepo(repo *authkeys.Repo) AdminOption {
	return func(h *AdminHandler) {
		h.authKeys = repo
	}
}

func WithSystemLogsRepo(repo *systemlogs.Repo) AdminOption {
	return func(h *AdminHandler) {
		h.systemLogs = repo
	}
}

func WithProxyPoolRepo(repo *proxypool.Repo) AdminOption {
	return func(h *AdminHandler) {
		h.proxies = repo
		h.proxyResolver = proxypool.NewResolver(repo)
	}
}

func WithProxyHealthcheckConfig(cfg proxypool.CheckerConfig) AdminOption {
	return func(h *AdminHandler) {
		h.proxyCheckConfig = cfg
	}
}

type MetricsReader interface {
	Snapshot(time.Duration) []metrics.Row
	Series(time.Duration) metrics.Series
}

func NewAdminHandler(
	reg *channels.Registry,
	pool *accounts.Pool,
	sm *session.Manager,
	lr *logs.Repo,
	ma MetricsReader,
	tp channels.Transport,
	opts ...AdminOption,
) *AdminHandler {
	h := &AdminHandler{
		registry:         reg,
		accounts:         pool,
		sessions:         sm,
		logs:             lr,
		metrics:          ma,
		transport:        tp,
		proxyCheckConfig: proxypool.DefaultCheckerConfig(),
		loginDrafts:      make(map[string]accountLoginDraft),
		autoLoginJobs:    make(map[string]*freeBuffAutoLoginJob),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

type channelView struct {
	ID                 string                       `json:"id"`
	InboundPath        string                       `json:"inbound_path"`
	HasAuthFlow        bool                         `json:"has_auth_flow"`
	SessionTTLSecs     int64                        `json:"session_ttl_secs"`
	Config             channelConfigView            `json:"config"`
	EffectivePolicy    channelSessionPolicyView     `json:"effective_session_policy"`
	ActiveSessions     int                          `json:"active_sessions"`
	AccountCount       int                          `json:"account_count"`
	ActiveAccountCount int                          `json:"active_account_count"`
	RequestCount       int64                        `json:"request_count"`
	TotalTokens        int64                        `json:"total_tokens"`
	AuthMethods        []channels.AccountAuthMethod `json:"auth_methods"`
}

type channelConfigView struct {
	MaxConcurrentPerSession int   `json:"max_concurrent_per_session"`
	WaitOnFull              *bool `json:"wait_on_full"`
}

type channelSessionPolicyView struct {
	MaxConcurrentPerSession int  `json:"max_concurrent_per_session"`
	WaitOnFull              bool `json:"wait_on_full"`
}

type updateChannelConfigReq struct {
	MaxConcurrentPerSession *int  `json:"max_concurrent_per_session"`
	WaitOnFull              *bool `json:"wait_on_full"`
}

func (h *AdminHandler) ListChannels(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		writeJSON(w, http.StatusOK, []channelView{})
		return
	}
	sessionCounts := map[string]int{}
	if h.sessions != nil {
		sessionCounts = h.sessions.CountByChannel()
	}
	accountCounts, err := h.channelAccountCounts()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	usageCounts, err := h.channelUsageCounts()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	configs, err := h.channelConfigMap()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := []channelView{}
	for _, a := range h.registry.List() {
		policy := a.SessionPolicy()
		accountCount := accountCounts[a.ID()]
		usage := usageCounts[a.ID()]
		cfg := channelconfig.Config{}
		if rec, ok := configs[a.ID()]; ok {
			cfg = rec.Config
		}
		v := channelView{
			ID:                 a.ID(),
			InboundPath:        a.InboundPathPrefix(),
			HasAuthFlow:        a.AuthFlow() != nil,
			Config:             channelConfigViewFromConfig(cfg),
			ActiveSessions:     sessionCounts[a.ID()],
			AccountCount:       accountCount.total,
			ActiveAccountCount: accountCount.active,
			RequestCount:       usage.RequestCount,
			TotalTokens:        usage.TotalTokens,
			AuthMethods:        accountAuthMethods(a),
		}
		if policy != nil {
			v.SessionTTLSecs = int64(policy.SessionTTL().Seconds())
			v.EffectivePolicy = h.effectiveChannelPolicyView(policy, cfg)
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *AdminHandler) ListFreeBuffModels(w http.ResponseWriter, r *http.Request) {
	const channelID = "freebuff"
	if h.registry == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	adapter, ok := h.registry.Get(channelID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	provider, ok := adapter.(channels.ModelCatalogProvider)
	if !ok {
		writeJSON(w, http.StatusOK, []channels.ModelInfo{})
		return
	}
	writeJSON(w, http.StatusOK, provider.ModelCatalog())
}

type createAuthKeyReq struct {
	Name string `json:"name"`
}

func (h *AdminHandler) ListAuthKeys(w http.ResponseWriter, r *http.Request) {
	if h.authKeys == nil {
		writeJSON(w, http.StatusOK, []authkeys.Record{})
		return
	}
	keys, err := h.authKeys.List()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

func (h *AdminHandler) CreateAuthKey(w http.ResponseWriter, r *http.Request) {
	if h.authKeys == nil {
		writeJSONError(w, http.StatusInternalServerError, "auth key store unavailable")
		return
	}
	var req createAuthKeyReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeJSONError(w, http.StatusBadRequest, "name required")
		return
	}
	created, err := h.authKeys.Create(req.Name)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (h *AdminHandler) DeleteAuthKey(w http.ResponseWriter, r *http.Request) {
	if h.authKeys == nil {
		writeJSONError(w, http.StatusInternalServerError, "auth key store unavailable")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	if err := h.authKeys.Delete(id); err != nil {
		if errors.Is(err, authkeys.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "auth key not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type freeBuffAccountView struct {
	*accounts.Record
	SessionConfig    accountSessionConfigView        `json:"session_config"`
	ProxyBinding     *proxyBindingView               `json:"proxy_binding,omitempty"`
	Runtime          *accounts.Snapshot              `json:"runtime,omitempty"`
	UpstreamState    *freebuffstate.AccountState     `json:"upstream_state,omitempty"`
	QuotaSnapshots   []freebuffstate.QuotaSnapshot   `json:"quota_snapshots"`
	UpstreamSessions []freebuffstate.UpstreamSession `json:"upstream_sessions"`
}

type freeBuffRefreshError struct {
	AccountID string `json:"account_id"`
	Error     string `json:"error"`
}

type freeBuffBatchRefreshResp struct {
	Refreshed int                    `json:"refreshed"`
	Failed    int                    `json:"failed"`
	Errors    []freeBuffRefreshError `json:"errors"`
	Accounts  []freeBuffAccountView  `json:"accounts"`
}

func (h *AdminHandler) ListFreeBuffAccounts(w http.ResponseWriter, r *http.Request) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		writeJSON(w, http.StatusOK, []freeBuffAccountView{})
		return
	}
	records, err := h.accounts.Repo().ListByChannel(freebuffstate.ChannelID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	runtimes := h.accountRuntimeMap()
	out := make([]freeBuffAccountView, 0, len(records))
	for _, rec := range records {
		view, err := h.freeBuffAccountView(r.Context(), rec, runtimes)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, view)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *AdminHandler) ListFreeBuffScheduler(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	adapter, ok := h.registry.Get(freebuffstate.ChannelID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	provider, ok := adapter.(channels.SchedulerSnapshotProvider)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "unavailable",
			"reason": "scheduler_snapshot_unsupported",
		})
		return
	}
	accounts, err := h.freeBuffSchedulerAccounts(r.Context(), adapter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sessions := []channels.SessionCandidate{}
	pendingCreates := []channels.SessionCreateCandidate{}
	if h.sessions != nil {
		sessions = h.sessions.SchedulingCandidates(freebuffstate.ChannelID)
		pendingCreates = h.sessions.PendingCreateCandidates(freebuffstate.ChannelID)
	}
	snapshot, err := provider.SchedulerSnapshot(r.Context(), channels.SchedulerSnapshotRequest{
		Accounts:       accounts,
		Sessions:       sessions,
		PendingCreates: pendingCreates,
		Now:            time.Now(),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *AdminHandler) freeBuffSchedulerAccounts(ctx context.Context, adapter channels.ChannelAdapter) ([]channels.AccountCandidate, error) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		return []channels.AccountCandidate{}, nil
	}
	maxSessions := 1
	if policy := adapter.SessionPolicy(); policy != nil {
		maxSessions = policy.MaxSessionsPerAccount()
	}
	candidates, err := h.accounts.SlotCandidates(freebuffstate.ChannelID, maxSessions, nil)
	if err != nil {
		return nil, err
	}
	if h.freebuffStates != nil {
		now := time.Now()
		for i := range candidates {
			facts, err := h.freebuffStates.SchedulerFacts(ctx, candidates[i].Account.ID, now)
			if err != nil {
				return nil, err
			}
			if len(facts) > 0 {
				candidates[i].ProviderFacts = facts
			}
		}
	}
	return candidates, nil
}

func (h *AdminHandler) RefreshFreeBuffAccount(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	rec, err := h.refreshFreeBuffRecordState(r.Context(), id)
	if err != nil {
		h.writeFreeBuffRefreshError(w, err)
		return
	}
	view, err := h.freeBuffAccountView(r.Context(), rec, h.accountRuntimeMap())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) RefreshFreeBuffAccounts(w http.ResponseWriter, r *http.Request) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		writeJSONError(w, http.StatusInternalServerError, "accounts unavailable")
		return
	}
	records, err := h.accounts.Repo().ListByChannel(freebuffstate.ChannelID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := freeBuffBatchRefreshResp{
		Errors:   []freeBuffRefreshError{},
		Accounts: []freeBuffAccountView{},
	}
	runtimes := h.accountRuntimeMap()
	for _, rec := range records {
		if !rec.IsActive {
			continue
		}
		if err := h.refreshFreeBuffRecord(r.Context(), rec); err != nil {
			resp.Failed++
			resp.Errors = append(resp.Errors, freeBuffRefreshError{AccountID: rec.ID, Error: err.Error()})
			continue
		}
		resp.Refreshed++
		view, err := h.freeBuffAccountView(r.Context(), rec, runtimes)
		if err != nil {
			resp.Failed++
			resp.Errors = append(resp.Errors, freeBuffRefreshError{AccountID: rec.ID, Error: err.Error()})
			continue
		}
		resp.Accounts = append(resp.Accounts, view)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *AdminHandler) refreshFreeBuffRecordState(ctx context.Context, accountID string) (*accounts.Record, error) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		return nil, errors.New("accounts unavailable")
	}
	rec, err := h.accounts.Repo().Get(accountID)
	if err != nil {
		return nil, err
	}
	if rec.ChannelID != freebuffstate.ChannelID {
		return nil, accounts.ErrNotFound
	}
	if err := h.refreshFreeBuffRecord(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (h *AdminHandler) refreshFreeBuffRecord(ctx context.Context, rec *accounts.Record) error {
	if rec == nil {
		return accounts.ErrNotFound
	}
	if h.freebuffStates == nil {
		return errors.New("freebuff state store unavailable")
	}
	if h.registry == nil {
		return errors.New("channel registry unavailable")
	}
	if h.transport == nil {
		return errors.New("transport unavailable")
	}
	adapter, ok := h.registry.Get(freebuffstate.ChannelID)
	if !ok {
		return errors.New("freebuff channel not found")
	}
	refresher, ok := adapter.(channels.AccountStateRefresher)
	if !ok {
		return errors.New("freebuff channel does not support refresh")
	}
	acc, err := h.resolvedChannelAccount(ctx, rec)
	if err != nil {
		return err
	}
	state, err := refresher.RefreshAccountState(ctx, acc, h.transport)
	if err != nil {
		return err
	}
	return h.freebuffStates.RecordSessionState(ctx, session.StateEvent{
		ChannelID: freebuffstate.ChannelID,
		AccountID: rec.ID,
		State:     state,
	})
}

func (h *AdminHandler) writeFreeBuffRefreshError(w http.ResponseWriter, err error) {
	if errors.Is(err, accounts.ErrNotFound) {
		writeJSONError(w, http.StatusNotFound, "freebuff account not found")
		return
	}
	if errors.Is(err, channels.ErrAccountUnavailable) {
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

func (h *AdminHandler) accountRuntimeMap() map[string]accounts.Snapshot {
	runtimes := map[string]accounts.Snapshot{}
	if h.accounts == nil {
		return runtimes
	}
	for _, snapshot := range h.accounts.Snapshot() {
		runtimes[snapshot.AccountID] = snapshot
	}
	return runtimes
}

func (h *AdminHandler) freeBuffAccountView(ctx context.Context, rec *accounts.Record, runtimes map[string]accounts.Snapshot) (freeBuffAccountView, error) {
	view := freeBuffAccountView{
		Record:           rec,
		SessionConfig:    accountViewForRecord(rec).SessionConfig,
		QuotaSnapshots:   []freebuffstate.QuotaSnapshot{},
		UpstreamSessions: []freebuffstate.UpstreamSession{},
	}
	if rec == nil {
		return view, nil
	}
	binding, err := h.proxyBindingView(ctx, rec.Metadata)
	if err != nil {
		return view, err
	}
	view.ProxyBinding = binding
	if runtime, ok := runtimes[rec.ID]; ok {
		runtime := runtime
		view.Runtime = &runtime
	}
	if h.freebuffStates == nil {
		return view, nil
	}
	upstream, err := h.freebuffStates.GetAccountState(ctx, rec.ID)
	if err != nil && !errors.Is(err, freebuffstate.ErrNotFound) {
		return view, err
	}
	view.UpstreamState = upstream
	quotas, err := h.freebuffStates.ListQuotaSnapshots(ctx, rec.ID)
	if err != nil {
		return view, err
	}
	view.QuotaSnapshots = quotas
	sessions, err := h.freebuffStates.ListUpstreamSessions(ctx, rec.ID)
	if err != nil {
		return view, err
	}
	view.UpstreamSessions = sessions
	return view, nil
}

func (h *AdminHandler) UpdateChannelConfig(w http.ResponseWriter, r *http.Request) {
	channelID := strings.TrimSpace(r.PathValue("id"))
	if channelID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	if h.registry == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	adapter, ok := h.registry.Get(channelID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	if h.channelConfigs == nil {
		writeJSONError(w, http.StatusInternalServerError, "channel config unavailable")
		return
	}
	var req updateChannelConfigReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.MaxConcurrentPerSession == nil {
		writeJSONError(w, http.StatusBadRequest, "max_concurrent_per_session required")
		return
	}
	if req.WaitOnFull == nil {
		writeJSONError(w, http.StatusBadRequest, "wait_on_full required")
		return
	}
	if err := channelconfig.ValidateMaxConcurrentPerSession(*req.MaxConcurrentPerSession); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg := channelconfig.Config{
		MaxConcurrentPerSession: *req.MaxConcurrentPerSession,
		WaitOnFull:              req.WaitOnFull,
	}
	if _, err := h.channelConfigs.Upsert(channelID, channelID, true, cfg); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if h.sessions != nil {
		h.sessions.RefreshChannelPolicy(channelID)
	}
	writeJSON(w, http.StatusOK, struct {
		Config          channelConfigView        `json:"config"`
		EffectivePolicy channelSessionPolicyView `json:"effective_session_policy"`
	}{
		Config:          channelConfigViewFromConfig(cfg),
		EffectivePolicy: h.effectiveChannelPolicyView(adapter.SessionPolicy(), cfg),
	})
}

func (h *AdminHandler) channelConfigMap() (map[string]*channelconfig.Record, error) {
	if h.channelConfigs == nil {
		return map[string]*channelconfig.Record{}, nil
	}
	return h.channelConfigs.ListMap()
}

func channelConfigViewFromConfig(cfg channelconfig.Config) channelConfigView {
	return channelConfigView{
		MaxConcurrentPerSession: cfg.MaxConcurrentPerSession,
		WaitOnFull:              cfg.WaitOnFull,
	}
}

func (h *AdminHandler) effectiveChannelPolicyView(policy channels.SessionPolicy, cfg channelconfig.Config) channelSessionPolicyView {
	fallbackMax := 1
	if policy != nil {
		fallbackMax = policy.MaxConcurrentPerSession()
	}
	fallbackWait := false
	if h.sessions != nil {
		fallbackWait = h.sessions.DefaultWaitOnFull()
	}
	return channelSessionPolicyView{
		MaxConcurrentPerSession: cfg.EffectiveMaxConcurrentPerSession(fallbackMax),
		WaitOnFull:              cfg.EffectiveWaitOnFull(fallbackWait),
	}
}

type channelAccountCount struct {
	total  int
	active int
}

func (h *AdminHandler) channelAccountCounts() (map[string]channelAccountCount, error) {
	out := map[string]channelAccountCount{}
	if h.accounts == nil || h.accounts.Repo() == nil {
		return out, nil
	}
	records, err := h.accounts.Repo().ListAll()
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		count := out[rec.ChannelID]
		count.total++
		if rec.IsActive {
			count.active++
		}
		out[rec.ChannelID] = count
	}
	return out, nil
}

func (h *AdminHandler) channelUsageCounts() (map[string]logs.ChannelUsage, error) {
	out := map[string]logs.ChannelUsage{}
	if h.logs == nil {
		return out, nil
	}
	items, err := h.logs.ChannelUsage(logs.UsageQuery{
		Range: logs.TimeRange{Label: "all"},
	})
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		out[item.ChannelID] = item
	}
	return out, nil
}

type accountView struct {
	*accounts.Record
	SessionConfig accountSessionConfigView `json:"session_config"`
	ProxyBinding  *proxyBindingView        `json:"proxy_binding,omitempty"`
	Runtime       *accounts.Snapshot       `json:"runtime,omitempty"`
}

type accountSessionConfigView struct {
	MaxConcurrentPerSession *int `json:"max_concurrent_per_session"`
}

func (h *AdminHandler) ListAccounts(w http.ResponseWriter, r *http.Request) {
	channelID := r.URL.Query().Get("channel")
	var records []*accounts.Record
	var err error
	if channelID != "" {
		records, err = h.accounts.Repo().ListByChannel(channelID)
	} else {
		records, err = h.accounts.Repo().ListAll()
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	snapshots := map[string]accounts.Snapshot{}
	for _, s := range h.accounts.Snapshot() {
		snapshots[s.AccountID] = s
	}
	out := make([]accountView, 0, len(records))
	for _, rec := range records {
		v, err := h.accountView(r.Context(), rec)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if s, ok := snapshots[rec.ID]; ok {
			s := s
			v.Runtime = &s
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, out)
}

func accountViewForRecord(rec *accounts.Record) accountView {
	view := accountView{Record: rec}
	if rec == nil {
		return view
	}
	if max, ok := runtimeconfig.AccountMaxConcurrentPerSession(rec.Metadata); ok {
		max := max
		view.SessionConfig.MaxConcurrentPerSession = &max
	}
	return view
}

func (h *AdminHandler) accountView(ctx context.Context, rec *accounts.Record) (accountView, error) {
	view := accountViewForRecord(rec)
	if rec == nil {
		return view, nil
	}
	binding, err := h.proxyBindingView(ctx, rec.Metadata)
	if err != nil {
		return view, err
	}
	view.ProxyBinding = binding
	return view, nil
}

func (h *AdminHandler) proxyBindingView(ctx context.Context, metadata map[string]any) (*proxyBindingView, error) {
	proxyID, ok := proxypool.ProxyIDFromMetadata(metadata)
	if !ok {
		return nil, nil
	}
	view := &proxyBindingView{
		ProxyID:  proxyID,
		Status:   "missing",
		IsActive: false,
	}
	if h.proxies == nil {
		return view, nil
	}
	rec, err := h.proxies.Get(proxyID)
	if err != nil {
		if errors.Is(err, proxypool.ErrNotFound) {
			return view, nil
		}
		return nil, err
	}
	view.Name = rec.Name
	view.URLRedacted = rec.RedactedURL()
	view.IsActive = rec.IsActive
	if rec.IsActive {
		view.Status = "active"
	} else {
		view.Status = "disabled"
	}
	_ = ctx
	return view, nil
}

func (h *AdminHandler) resolvedChannelAccount(ctx context.Context, rec *accounts.Record) (channels.Account, error) {
	acc := rec.ToChannel()
	if h.proxyResolver == nil {
		acc.Metadata = proxypool.ClearProxyRuntime(acc.Metadata)
		return acc, nil
	}
	metadata, err := h.proxyResolver.ResolveAccountMetadata(ctx, acc)
	if err != nil {
		return acc, err
	}
	acc.Metadata = metadata
	return acc, nil
}

func (h *AdminHandler) applyProxyBinding(ctx context.Context, channelID string, metadata map[string]any, proxyID string, proxyProvided, clearProxy bool) (map[string]any, error) {
	if !proxyProvided && !clearProxy {
		return proxypool.ClearProxyRuntime(metadata), nil
	}
	if channelID != freebuffstate.ChannelID {
		return metadata, proxyBindingError{status: http.StatusBadRequest, message: "proxy binding is only supported for freebuff accounts"}
	}
	if clearProxy {
		return proxypool.SetProxyID(metadata, ""), nil
	}
	proxyID = strings.TrimSpace(proxyID)
	if proxyID == "" {
		return proxypool.SetProxyID(metadata, ""), nil
	}
	if _, err := h.activeProxyRecord(ctx, proxyID); err != nil {
		return metadata, err
	}
	return proxypool.SetProxyID(metadata, proxyID), nil
}

func (h *AdminHandler) activeProxyRecord(_ context.Context, proxyID string) (*proxypool.Record, error) {
	if h.proxies == nil {
		return nil, proxyBindingError{status: http.StatusInternalServerError, message: "proxy pool unavailable"}
	}
	rec, err := h.proxies.Get(strings.TrimSpace(proxyID))
	if err != nil {
		if errors.Is(err, proxypool.ErrNotFound) {
			return nil, proxyBindingError{status: http.StatusNotFound, message: "proxy not found"}
		}
		return nil, err
	}
	if !rec.IsActive {
		return nil, proxyBindingError{status: http.StatusBadRequest, message: "proxy is disabled"}
	}
	return rec, nil
}

type proxyBindingError struct {
	status  int
	message string
}

func (e proxyBindingError) Error() string { return e.message }

func (h *AdminHandler) writeProxyBindingError(w http.ResponseWriter, err error) {
	var httpErr proxyBindingError
	if errors.As(err, &httpErr) {
		writeJSONError(w, httpErr.status, httpErr.message)
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

type createAccountReq struct {
	ChannelID                      string `json:"channel_id"`
	Name                           string `json:"name"`
	Credential                     string `json:"credential"`
	ProxyID                        string `json:"proxy_id"`
	SessionMaxConcurrentPerSession *int   `json:"session_max_concurrent_per_session"`
}

type updateAccountReq struct {
	Name                                *string        `json:"name"`
	Credential                          *string        `json:"credential"`
	Priority                            *int           `json:"priority"`
	RPMLimit                            *int           `json:"rpm_limit"`
	QuotaTotal                          *int64         `json:"quota_total"`
	QuotaPeriod                         *string        `json:"quota_period"`
	IsActive                            *bool          `json:"is_active"`
	Metadata                            map[string]any `json:"metadata"`
	ProxyID                             *string        `json:"proxy_id"`
	ClearProxy                          *bool          `json:"clear_proxy"`
	SessionMaxConcurrentPerSession      *int           `json:"session_max_concurrent_per_session"`
	ClearSessionMaxConcurrentPerSession *bool          `json:"clear_session_max_concurrent_per_session"`
}

type batchAccountUpdateReq struct {
	AccountIDs []string          `json:"account_ids"`
	Patch      batchAccountPatch `json:"patch"`
}

type batchAccountPatch struct {
	Priority                            *int    `json:"priority"`
	RPMLimit                            *int    `json:"rpm_limit"`
	QuotaTotal                          *int64  `json:"quota_total"`
	QuotaPeriod                         *string `json:"quota_period"`
	ResetQuotaUsed                      *bool   `json:"reset_quota_used"`
	IsActive                            *bool   `json:"is_active"`
	SessionMaxConcurrentPerSession      *int    `json:"session_max_concurrent_per_session"`
	ClearSessionMaxConcurrentPerSession *bool   `json:"clear_session_max_concurrent_per_session"`
}

type batchAccountUpdateResp struct {
	Affected int `json:"affected"`
}

func (h *AdminHandler) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	req.ChannelID = strings.TrimSpace(req.ChannelID)
	req.Name = strings.TrimSpace(req.Name)
	req.Credential = strings.TrimSpace(req.Credential)
	if req.ChannelID == "" || req.Credential == "" {
		writeJSONError(w, http.StatusBadRequest, "channel_id, credential required")
		return
	}
	name := req.Name
	credential := req.Credential
	var metadata map[string]any
	if h.registry != nil {
		if adapter, ok := h.registry.Get(req.ChannelID); ok {
			if importer, ok := adapter.(channels.AccountCredentialImporter); ok {
				imported, err := importer.ImportAccountCredential(req.Credential)
				if err != nil {
					writeJSONError(w, http.StatusBadRequest, err.Error())
					return
				}
				if imported == nil || strings.TrimSpace(imported.Credential) == "" {
					writeJSONError(w, http.StatusBadRequest, "credential import failed")
					return
				}
				credential = strings.TrimSpace(imported.Credential)
				if name == "" {
					name = strings.TrimSpace(imported.Name)
				}
				metadata = mergeMetadata(metadata, imported.Metadata)
			}
		}
	}
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "name required")
		return
	}
	if req.SessionMaxConcurrentPerSession != nil {
		if err := channelconfig.ValidateMaxConcurrentPerSession(*req.SessionMaxConcurrentPerSession); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		metadata = runtimeconfig.SetAccountMaxConcurrentPerSession(metadata, *req.SessionMaxConcurrentPerSession)
	}
	var err error
	metadata, err = h.applyProxyBinding(r.Context(), req.ChannelID, metadata, req.ProxyID, req.ProxyID != "", false)
	if err != nil {
		h.writeProxyBindingError(w, err)
		return
	}
	rec := &accounts.Record{
		ChannelID:  req.ChannelID,
		Name:       name,
		Credential: credential,
		IsActive:   true,
		Metadata:   metadata,
	}
	if err := h.accounts.Repo().Create(rec); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	view, err := h.accountView(r.Context(), rec)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (h *AdminHandler) UpdateAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	rec, err := h.accounts.Repo().Get(id)
	if err != nil {
		if errors.Is(err, accounts.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "account not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var req updateAccountReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name != nil {
		rec.Name = *req.Name
	}
	if req.Credential != nil && *req.Credential != "" {
		rec.Credential = *req.Credential
	}
	if req.Priority != nil {
		rec.Priority = *req.Priority
	}
	if req.RPMLimit != nil {
		rec.RPMLimit = *req.RPMLimit
	}
	if req.QuotaPeriod != nil && !accounts.IsQuotaPeriod(*req.QuotaPeriod) {
		writeJSONError(w, http.StatusBadRequest, "invalid quota_period")
		return
	}
	if req.QuotaTotal != nil {
		period := rec.QuotaPeriod
		if req.QuotaPeriod != nil {
			period = *req.QuotaPeriod
		}
		quotaTotal, quotaPeriod, quotaUsed, quotaStart, err := normalizeQuota(*req.QuotaTotal, period, time.Now())
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		periodChanged := rec.QuotaPeriod != quotaPeriod
		wasUnconfigured := rec.QuotaTotal <= 0
		rec.QuotaTotal = quotaTotal
		rec.QuotaPeriod = quotaPeriod
		if quotaTotal <= 0 || periodChanged || wasUnconfigured {
			rec.QuotaUsed = quotaUsed
			rec.QuotaPeriodStart = quotaStart
		}
	} else if req.QuotaPeriod != nil && rec.QuotaTotal > 0 && *req.QuotaPeriod != rec.QuotaPeriod {
		quotaTotal, quotaPeriod, quotaUsed, quotaStart, err := normalizeQuota(rec.QuotaTotal, *req.QuotaPeriod, time.Now())
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		rec.QuotaTotal = quotaTotal
		rec.QuotaPeriod = quotaPeriod
		rec.QuotaUsed = quotaUsed
		rec.QuotaPeriodStart = quotaStart
	}
	if req.IsActive != nil {
		rec.IsActive = *req.IsActive
	}
	if req.Metadata != nil {
		rec.Metadata = req.Metadata
	}
	clearProxy := req.ClearProxy != nil && *req.ClearProxy
	proxyID := ""
	proxyProvided := req.ProxyID != nil
	if req.ProxyID != nil {
		proxyID = *req.ProxyID
	}
	rec.Metadata, err = h.applyProxyBinding(r.Context(), rec.ChannelID, rec.Metadata, proxyID, proxyProvided, clearProxy)
	if err != nil {
		h.writeProxyBindingError(w, err)
		return
	}
	if err := applyAccountSessionConfigPatch(
		rec,
		req.SessionMaxConcurrentPerSession,
		req.ClearSessionMaxConcurrentPerSession,
	); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.accounts.Repo().Update(rec); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if h.sessions != nil {
		h.sessions.RefreshAccountPolicy(rec.ID)
	}
	view, err := h.accountView(r.Context(), rec)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *AdminHandler) BatchUpdateAccounts(w http.ResponseWriter, r *http.Request) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		writeJSONError(w, http.StatusInternalServerError, "accounts unavailable")
		return
	}
	var req batchAccountUpdateReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	accountIDs := uniqueAccountIDs(req.AccountIDs)
	if len(accountIDs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "account_ids required")
		return
	}
	if !hasBatchAccountPatch(req.Patch) {
		writeJSONError(w, http.StatusBadRequest, "patch required")
		return
	}
	normalizeBatchAccountPatch(&req.Patch)
	if err := validateBatchAccountPatch(req.Patch); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	repo := h.accounts.Repo()
	records := make([]*accounts.Record, 0, len(accountIDs))
	for _, id := range accountIDs {
		rec, err := repo.Get(id)
		if err != nil {
			if errors.Is(err, accounts.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "account not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		records = append(records, rec)
	}

	now := time.Now()
	for _, rec := range records {
		if err := applyBatchAccountPatch(rec, req.Patch, now); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := repo.Update(rec); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if h.sessions != nil {
		for _, rec := range records {
			h.sessions.RefreshAccountPolicy(rec.ID)
		}
	}
	writeJSON(w, http.StatusOK, batchAccountUpdateResp{Affected: len(records)})
}

func (h *AdminHandler) DeleteAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	if err := h.accounts.Repo().Delete(id); err != nil {
		if errors.Is(err, accounts.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "account not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) ListSessions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.sessions.Snapshot())
}

func (h *AdminHandler) ListLogs(w http.ResponseWriter, r *http.Request) {
	q := logs.Query{
		ChannelID: r.URL.Query().Get("channel"),
		AccountID: r.URL.Query().Get("account"),
	}
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if n, err := strconv.Atoi(limit); err == nil {
			q.Limit = n
		}
	}
	entries, err := h.logs.List(q)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entries == nil {
		entries = []logs.Entry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *AdminHandler) ListMetrics(w http.ResponseWriter, r *http.Request) {
	duration, err := parseMetricsWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid window")
		return
	}
	if h.metrics == nil {
		writeJSON(w, http.StatusOK, []metrics.Row{})
		return
	}
	writeJSON(w, http.StatusOK, h.metrics.Snapshot(duration))
}

func (h *AdminHandler) ListMetricSeries(w http.ResponseWriter, r *http.Request) {
	duration, err := parseMetricsWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid window")
		return
	}
	if h.metrics == nil {
		writeJSON(w, http.StatusOK, metrics.EmptySeries(time.Now(), duration))
		return
	}
	writeJSON(w, http.StatusOK, h.metrics.Series(duration))
}

type usageAccountView struct {
	AccountID           string             `json:"account_id"`
	AccountName         string             `json:"account_name"`
	ChannelID           string             `json:"channel_id"`
	IsActive            bool               `json:"is_active"`
	QuotaTotal          int64              `json:"quota_total"`
	QuotaUsed           int64              `json:"quota_used"`
	QuotaPeriod         string             `json:"quota_period"`
	QuotaPeriodStart    int64              `json:"quota_period_start"`
	Runtime             *accounts.Snapshot `json:"runtime,omitempty"`
	TotalRequests       int64              `json:"total_requests"`
	SuccessCount        int64              `json:"success_count"`
	FailureCount        int64              `json:"failure_count"`
	SuccessRate         float64            `json:"success_rate"`
	TokensIn            int64              `json:"tokens_in"`
	TokensOut           int64              `json:"tokens_out"`
	TotalTokens         int64              `json:"total_tokens"`
	AvgLatencyMS        float64            `json:"avg_latency_ms"`
	LastRequestAt       int64              `json:"last_request_at"`
	TopModel            string             `json:"top_model"`
	ConsecutiveFailures int                `json:"consecutive_failures"`
	OnCooldown          bool               `json:"on_cooldown"`
}

type usageEventView struct {
	ID              string         `json:"id"`
	ChannelID       string         `json:"channel_id"`
	AccountID       string         `json:"account_id,omitempty"`
	AccountName     string         `json:"account_name,omitempty"`
	SessionID       string         `json:"session_id,omitempty"`
	Method          string         `json:"method"`
	Path            string         `json:"path"`
	Stream          bool           `json:"stream"`
	SelectionKey    string         `json:"selection_key,omitempty"`
	Model           string         `json:"model,omitempty"`
	Status          int            `json:"status"`
	ResponseClass   string         `json:"response_class"`
	LatencyMS       int64          `json:"latency_ms"`
	FirstResponseMS int64          `json:"first_response_ms"`
	TokensIn        int            `json:"tokens_in,omitempty"`
	TokensOut       int            `json:"tokens_out,omitempty"`
	TokensTotal     int            `json:"tokens_total"`
	PhaseTimings    map[string]any `json:"phase_timings,omitempty"`
	Error           string         `json:"error,omitempty"`
	CreatedAt       int64          `json:"created_at"`
}

func (h *AdminHandler) ListUsageSummary(w http.ResponseWriter, r *http.Request) {
	q, err := usageQueryFromRequest(r, 100)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.logs == nil {
		writeJSON(w, http.StatusOK, logs.UsageSummary{
			Range:   q.Range.Label,
			StartAt: q.Range.StartAt,
			EndAt:   q.Range.EndAt,
		})
		return
	}
	summary, err := h.logs.UsageSummary(q)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h *AdminHandler) ListUsageAccounts(w http.ResponseWriter, r *http.Request) {
	q, err := usageQueryFromRequest(r, 500)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	views := map[string]usageAccountView{}
	if h.logs != nil {
		stats, err := h.logs.AccountUsage(q)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, item := range stats {
			views[usageAccountKey(item.ChannelID, item.AccountID)] = usageAccountView{
				AccountID:     item.AccountID,
				ChannelID:     item.ChannelID,
				TotalRequests: item.TotalRequests,
				SuccessCount:  item.SuccessCount,
				FailureCount:  item.FailureCount,
				SuccessRate:   successRate(item.SuccessCount, item.FailureCount),
				TokensIn:      item.TokensIn,
				TokensOut:     item.TokensOut,
				TotalTokens:   item.TotalTokens,
				AvgLatencyMS:  item.AvgLatencyMS,
				LastRequestAt: item.LastRequestAt,
				TopModel:      item.TopModel,
			}
		}
	}

	if h.accounts != nil && h.accounts.Repo() != nil {
		records, err := usageAccountRecords(h.accounts.Repo(), q.ChannelID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		runtimes := map[string]accounts.Snapshot{}
		for _, snapshot := range h.accounts.Snapshot() {
			runtimes[snapshot.AccountID] = snapshot
		}
		for _, rec := range records {
			key := usageAccountKey(rec.ChannelID, rec.ID)
			view := views[key]
			view.AccountID = rec.ID
			view.AccountName = rec.Name
			view.ChannelID = rec.ChannelID
			view.IsActive = rec.IsActive
			view.QuotaTotal = rec.QuotaTotal
			view.QuotaUsed = rec.QuotaUsed
			view.QuotaPeriod = rec.QuotaPeriod
			view.QuotaPeriodStart = rec.QuotaPeriodStart
			if runtime, ok := runtimes[rec.ID]; ok {
				runtime := runtime
				view.Runtime = &runtime
				view.ConsecutiveFailures = runtime.ConsecutiveFailures
				view.OnCooldown = runtime.OnCooldown
			}
			views[key] = view
		}
	}

	out := make([]usageAccountView, 0, len(views))
	for _, view := range views {
		if q.Search != "" && view.TotalRequests == 0 && !usageAccountMatches(view, q.Search) {
			continue
		}
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalRequests != out[j].TotalRequests {
			return out[i].TotalRequests > out[j].TotalRequests
		}
		if out[i].LastRequestAt != out[j].LastRequestAt {
			return out[i].LastRequestAt > out[j].LastRequestAt
		}
		if out[i].ChannelID != out[j].ChannelID {
			return out[i].ChannelID < out[j].ChannelID
		}
		return out[i].AccountName < out[j].AccountName
	})
	if len(out) > q.Limit && q.Limit > 0 {
		out = out[:q.Limit]
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *AdminHandler) ListUsageEvents(w http.ResponseWriter, r *http.Request) {
	q, err := usageQueryFromRequest(r, 100)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.logs == nil {
		writeJSON(w, http.StatusOK, []usageEventView{})
		return
	}
	entries, err := h.logs.UsageEvents(q)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	accountNames := map[string]string{}
	if h.accounts != nil && h.accounts.Repo() != nil {
		records, err := h.accounts.Repo().ListAll()
		if err == nil {
			for _, rec := range records {
				accountNames[rec.ID] = rec.Name
			}
		}
	}
	out := make([]usageEventView, 0, len(entries))
	for _, entry := range entries {
		out = append(out, usageEventView{
			ID:              entry.ID,
			ChannelID:       entry.ChannelID,
			AccountID:       entry.AccountID,
			AccountName:     accountNames[entry.AccountID],
			SessionID:       entry.SessionID,
			Method:          entry.Method,
			Path:            entry.Path,
			Stream:          entry.Stream,
			SelectionKey:    entry.SelectionKey,
			Model:           entry.Model,
			Status:          entry.Status,
			ResponseClass:   entry.ResponseClass,
			LatencyMS:       entry.LatencyMS,
			FirstResponseMS: entry.FirstResponseMS,
			TokensIn:        entry.TokensIn,
			TokensOut:       entry.TokensOut,
			TokensTotal:     entry.TokensIn + entry.TokensOut,
			PhaseTimings:    entry.PhaseTimings,
			Error:           entry.Error,
			CreatedAt:       entry.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func parseMetricsWindow(window string) (time.Duration, error) {
	if window == "" {
		window = "1m"
	}
	switch window {
	case "1m":
		return time.Minute, nil
	case "1h":
		return time.Hour, nil
	default:
		return 0, errors.New("invalid window")
	}
}

func usageQueryFromRequest(r *http.Request, defaultLimit int) (logs.UsageQuery, error) {
	q := r.URL.Query()
	rangeValue, err := parseUsageRange(q.Get("range"), q.Get("start"), q.Get("end"), time.Now())
	if err != nil {
		return logs.UsageQuery{}, err
	}
	return logs.UsageQuery{
		Range:     rangeValue,
		ChannelID: strings.TrimSpace(q.Get("channel")),
		AccountID: strings.TrimSpace(q.Get("account")),
		Search:    strings.TrimSpace(q.Get("search")),
		Limit:     parseUsageLimit(q.Get("limit"), defaultLimit),
	}, nil
}

func parseUsageRange(rangeName string, startRaw string, endRaw string, now time.Time) (logs.TimeRange, error) {
	now = now.Local()
	label := strings.TrimSpace(rangeName)
	if label == "" {
		label = "today"
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	out := logs.TimeRange{Label: label, EndAt: now.Unix()}
	switch label {
	case "today":
		out.StartAt = today.Unix()
	case "7d":
		out.StartAt = today.AddDate(0, 0, -6).Unix()
	case "14d":
		out.StartAt = today.AddDate(0, 0, -13).Unix()
	case "30d":
		out.StartAt = today.AddDate(0, 0, -29).Unix()
	case "all":
		out.StartAt = 0
	default:
		return logs.TimeRange{}, errors.New("invalid range")
	}
	if startRaw != "" {
		start, err := strconv.ParseInt(startRaw, 10, 64)
		if err != nil || start < 0 {
			return logs.TimeRange{}, errors.New("invalid start")
		}
		out.StartAt = start
		out.Label = "custom"
	}
	if endRaw != "" {
		end, err := strconv.ParseInt(endRaw, 10, 64)
		if err != nil || end < 0 {
			return logs.TimeRange{}, errors.New("invalid end")
		}
		out.EndAt = end
		out.Label = "custom"
	}
	if out.StartAt > 0 && out.EndAt > 0 && out.StartAt > out.EndAt {
		return logs.TimeRange{}, errors.New("invalid range bounds")
	}
	return out, nil
}

func parseUsageLimit(raw string, fallback int) int {
	limit := fallback
	if raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	if limit <= 0 {
		return fallback
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func usageAccountRecords(repo *accounts.Repo, channelID string) ([]*accounts.Record, error) {
	if channelID != "" {
		return repo.ListByChannel(channelID)
	}
	return repo.ListAll()
}

func usageAccountKey(channelID string, accountID string) string {
	return channelID + "\x00" + accountID
}

func successRate(success int64, failure int64) float64 {
	total := success + failure
	if total <= 0 {
		return 0
	}
	return float64(success) / float64(total) * 100
}

func usageAccountMatches(view usageAccountView, search string) bool {
	search = strings.ToLower(strings.TrimSpace(search))
	if search == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		view.AccountID,
		view.AccountName,
		view.ChannelID,
		view.TopModel,
		view.QuotaPeriod,
	}, " "))
	return strings.Contains(haystack, search)
}

func uniqueAccountIDs(ids []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func hasBatchAccountPatch(p batchAccountPatch) bool {
	if p.ResetQuotaUsed != nil && *p.ResetQuotaUsed {
		return true
	}
	if p.ClearSessionMaxConcurrentPerSession != nil && *p.ClearSessionMaxConcurrentPerSession {
		return true
	}
	return p.Priority != nil ||
		p.RPMLimit != nil ||
		p.QuotaTotal != nil ||
		p.QuotaPeriod != nil ||
		p.IsActive != nil ||
		p.SessionMaxConcurrentPerSession != nil
}

func normalizeBatchAccountPatch(p *batchAccountPatch) {
	if p.QuotaPeriod != nil {
		period := strings.TrimSpace(*p.QuotaPeriod)
		p.QuotaPeriod = &period
	}
}

func validateBatchAccountPatch(p batchAccountPatch) error {
	if p.Priority != nil && *p.Priority < 0 {
		return errors.New("invalid priority")
	}
	if p.RPMLimit != nil && *p.RPMLimit < 0 {
		return errors.New("invalid rpm_limit")
	}
	if p.QuotaPeriod != nil && *p.QuotaPeriod != "" && !accounts.IsQuotaPeriod(*p.QuotaPeriod) {
		return errors.New("invalid quota_period")
	}
	if p.QuotaTotal != nil && *p.QuotaTotal > 0 && (p.QuotaPeriod == nil || *p.QuotaPeriod == "") {
		return errors.New("quota_period required")
	}
	if p.SessionMaxConcurrentPerSession != nil {
		if p.ClearSessionMaxConcurrentPerSession != nil && *p.ClearSessionMaxConcurrentPerSession {
			return errors.New("session concurrency set and clear are mutually exclusive")
		}
		if err := channelconfig.ValidateMaxConcurrentPerSession(*p.SessionMaxConcurrentPerSession); err != nil {
			return err
		}
	}
	return nil
}

func applyBatchAccountPatch(rec *accounts.Record, p batchAccountPatch, now time.Time) error {
	if p.Priority != nil {
		rec.Priority = *p.Priority
	}
	if p.RPMLimit != nil {
		rec.RPMLimit = *p.RPMLimit
	}
	if p.QuotaTotal != nil {
		period := ""
		if p.QuotaPeriod != nil {
			period = *p.QuotaPeriod
		}
		quotaTotal, quotaPeriod, quotaUsed, quotaStart, err := normalizeQuota(*p.QuotaTotal, period, now)
		if err != nil {
			return err
		}
		periodChanged := rec.QuotaPeriod != quotaPeriod
		wasUnconfigured := rec.QuotaTotal <= 0
		rec.QuotaTotal = quotaTotal
		rec.QuotaPeriod = quotaPeriod
		if quotaTotal <= 0 || periodChanged || wasUnconfigured {
			rec.QuotaUsed = quotaUsed
			rec.QuotaPeriodStart = quotaStart
		}
	} else if p.QuotaPeriod != nil && rec.QuotaTotal > 0 && *p.QuotaPeriod != rec.QuotaPeriod {
		quotaTotal, quotaPeriod, quotaUsed, quotaStart, err := normalizeQuota(rec.QuotaTotal, *p.QuotaPeriod, now)
		if err != nil {
			return err
		}
		rec.QuotaTotal = quotaTotal
		rec.QuotaPeriod = quotaPeriod
		rec.QuotaUsed = quotaUsed
		rec.QuotaPeriodStart = quotaStart
	}
	if p.ResetQuotaUsed != nil && *p.ResetQuotaUsed {
		rec.QuotaUsed = 0
		if rec.QuotaTotal > 0 && rec.QuotaPeriod != "" {
			rec.QuotaPeriodStart = accounts.QuotaBucketStart(now, rec.QuotaPeriod)
		} else {
			rec.QuotaPeriodStart = 0
		}
	}
	if p.IsActive != nil {
		rec.IsActive = *p.IsActive
	}
	if err := applyAccountSessionConfigPatch(
		rec,
		p.SessionMaxConcurrentPerSession,
		p.ClearSessionMaxConcurrentPerSession,
	); err != nil {
		return err
	}
	return nil
}

func applyAccountSessionConfigPatch(rec *accounts.Record, max *int, clear *bool) error {
	if rec == nil {
		return nil
	}
	if max != nil && clear != nil && *clear {
		return errors.New("session concurrency set and clear are mutually exclusive")
	}
	if max != nil {
		if err := channelconfig.ValidateMaxConcurrentPerSession(*max); err != nil {
			return err
		}
		rec.Metadata = runtimeconfig.SetAccountMaxConcurrentPerSession(rec.Metadata, *max)
		return nil
	}
	if clear != nil && *clear {
		rec.Metadata = runtimeconfig.ClearAccountMaxConcurrentPerSession(rec.Metadata)
	}
	return nil
}

func normalizeQuota(total int64, period string, now time.Time) (int64, string, int64, int64, error) {
	if total <= 0 {
		return 0, "", 0, 0, nil
	}
	if period == "" {
		return 0, "", 0, 0, errors.New("quota_period required")
	}
	if !accounts.IsQuotaPeriod(period) {
		return 0, "", 0, 0, errors.New("invalid quota_period")
	}
	return total, period, 0, accounts.QuotaBucketStart(now, period), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
