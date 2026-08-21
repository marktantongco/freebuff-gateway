package freebuff

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/phasetiming"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
)

const (
	ID = "freebuff"

	defaultBaseURL     = "https://www.codebuff.com"
	defaultAuthBaseURL = "https://freebuff.com"
	defaultGitHubURL   = "https://github.com"
	userAgent          = "github.com/marktantongco/freebuff-gateway/freebuff"

	agentID = "base2-free"

	stateInstanceID        = "freebuff_instance_id"
	stateModel             = "freebuff_model"
	stateAccessTier        = "freebuff_access_tier"
	stateAdmittedAtUnix    = "freebuff_admitted_at_unix"
	stateExpiresAtUnix     = "freebuff_expires_at_unix"
	stateRemainingMs       = "freebuff_remaining_ms"
	stateRateLimit         = "freebuff_rate_limit"
	stateRateLimitsByModel = "freebuff_rate_limits_by_model"
	stateRawSessionJSON    = "freebuff_raw_session_json"
	stateStatus            = "freebuff_status"
	stateMessage           = "freebuff_message"
	stateBaseURL           = "freebuff_base_url"
	stateAdsSessionID      = "freebuff_ads_session_id"
	stateDeviceOS          = "freebuff_device_os"
	stateDeviceTZ          = "freebuff_device_tz"
	stateDeviceLocale      = "freebuff_device_locale"
	stateDeviceBrowserUA   = "freebuff_device_browser_ua"
	stateMeID              = "freebuff_me_id"
	stateMeEmail           = "freebuff_me_email"
	stateSessionRunID      = "freebuff_session_run_id"
	stateSessionRunCreated = "freebuff_session_run_created_at_unix"
	stateSessionRunExpires = "freebuff_session_run_expires_at_unix"

	keyPrefix      = ID + "|"
	keyInvalidJSON = ID + "|__invalid_json__"
	keyUnsupported = ID + "|__unsupported__"
)

var (
	ErrUnsupportedPath = errors.New("freebuff: unsupported path")
	ErrStreaming       = errors.New("freebuff: streaming is not supported by the buffered transport")
	ErrInvalidJSON     = errors.New("freebuff: invalid json body")
)

type Option func(*Adapter)

type Adapter struct {
	baseURL     string
	authBaseURL string
	githubURL   string
	sessionTTL  time.Duration

	mu               sync.Mutex
	runtimes         map[string]runtime
	loginSessions    map[string]accountLoginSession
	async            *asyncSideEffects
	scheduler        *premiumScheduler
	runSetupMode     freebuffRunSetupMode
	sessionRunMaxAge time.Duration
}

type runtime struct {
	credential          string
	baseURL             string
	transport           channels.Transport
	transportProfile    channels.TransportProfile
	runID               string
	parentRunID         string
	childRunID          string
	sessionRunID        string
	sessionRunCreatedAt time.Time
	sessionRunExpiresAt time.Time
	adsSessionID        string
	meProfile           meInfo
	device              deviceProfile
}

type meInfo struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func New(opts ...Option) *Adapter {
	a := &Adapter{
		baseURL:          defaultBaseURL,
		authBaseURL:      defaultAuthBaseURL,
		githubURL:        defaultGitHubURL,
		sessionTTL:       time.Hour,
		runtimes:         make(map[string]runtime),
		loginSessions:    make(map[string]accountLoginSession),
		async:            newAsyncSideEffects(defaultADSQueueSize, defaultFinalizeQueueSize),
		scheduler:        newPremiumScheduler(defaultSchedulerConfig()),
		runSetupMode:     defaultRunSetupMode(),
		sessionRunMaxAge: defaultSessionRunMaxAge(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func WithBaseURL(raw string) Option {
	return func(a *Adapter) {
		if normalized := normalizeBaseURL(raw); normalized != "" {
			a.baseURL = normalized
		}
	}
}

func WithAuthBaseURL(raw string) Option {
	return func(a *Adapter) {
		if normalized := normalizeBaseURL(raw); normalized != "" {
			a.authBaseURL = normalized
		}
	}
}

func WithGitHubBaseURL(raw string) Option {
	return func(a *Adapter) {
		if normalized := normalizeBaseURL(raw); normalized != "" {
			a.githubURL = normalized
		}
	}
}

func WithSessionTTL(ttl time.Duration) Option {
	return func(a *Adapter) {
		if ttl > 0 {
			a.sessionTTL = ttl
		}
	}
}

func WithAsyncSideEffectLimits(adsQueueSize, finalizeQueueSize int, runTreeQueueSize ...int) Option {
	return func(a *Adapter) {
		a.async = newAsyncSideEffects(adsQueueSize, finalizeQueueSize, runTreeQueueSize...)
	}
}

func WithRunSetupMode(mode string) Option {
	return func(a *Adapter) {
		a.runSetupMode = parseRunSetupMode(mode)
	}
}

func WithSessionRunMaxAge(maxAge time.Duration) Option {
	return func(a *Adapter) {
		if maxAge > 0 {
			a.sessionRunMaxAge = maxAge
		}
	}
}

func WithSchedulerConfig(cfg SchedulerConfig) Option {
	return func(a *Adapter) {
		a.scheduler = newPremiumScheduler(cfg)
	}
}

func (a *Adapter) ID() string { return ID }

func (a *Adapter) InboundPathPrefix() string { return "/channels/" + ID }

func (a *Adapter) SessionPolicy() channels.SessionPolicy { return a }

func (a *Adapter) AuthFlow() channels.AuthFlow { return nil }

func (a *Adapter) Run(ctx context.Context) { a.async.Run(ctx, a) }

func (a *Adapter) SelectionKey(in *channels.InboundRequest) string {
	if in == nil {
		return keyUnsupported
	}
	switch in.Path {
	case "/v1/chat/completions":
		_, model, _, err := decodeChatBody(in.Body)
		if err != nil {
			return keyInvalidJSON
		}
		return keyPrefix + model
	case "/v1/messages":
		_, model, stream, err := decodeAnthropicBody(in.Body)
		if err != nil {
			return keyInvalidJSON
		}
		if !stream {
			return keyUnsupported
		}
		return keyPrefix + model
	default:
		return keyUnsupported
	}
}

func (a *Adapter) SessionTTL() time.Duration { return a.sessionTTL }

func (a *Adapter) MaxSessionsPerAccount() int { return 1 }

func (a *Adapter) MaxConcurrentPerSession() int { return 2 }

func (a *Adapter) ClassifySessionCreate(key string, _ *channels.InboundRequest) channels.SessionCreateLabels {
	model, err := modelFromKey(key)
	if err != nil {
		return channels.SessionCreateLabels{Model: key, QuotaGroup: ID}
	}
	group, _ := quotaGroupForModel(model)
	return channels.SessionCreateLabels{Model: model, QuotaGroup: group}
}

func (a *Adapter) SessionExpiresAt(state channels.State) (time.Time, bool) {
	if state == nil {
		return time.Time{}, false
	}
	switch v := state[stateExpiresAtUnix].(type) {
	case int64:
		if v > 0 {
			return time.Unix(v, 0), true
		}
	case int:
		if v > 0 {
			return time.Unix(int64(v), 0), true
		}
	case float64:
		if v > 0 {
			return time.Unix(int64(v), 0), true
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return time.Unix(n, 0), true
		}
	}
	return time.Time{}, false
}

func (a *Adapter) CreateSession(ctx context.Context, acc channels.Account, key string, tp channels.Transport) (channels.State, error) {
	model, err := modelFromKey(key)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(acc.Credential) == "" {
		return nil, fmt.Errorf("freebuff: account %q has empty credential", acc.ID)
	}
	baseURL := a.baseURLFor(acc)

	rt := runtime{
		credential:       acc.Credential,
		baseURL:          baseURL,
		transport:        tp,
		transportProfile: accountTransportProfile(acc),
		adsSessionID:     generateAdsSessionID(),
		device:           generateDeviceProfile(),
	}

	session, err := a.fetchActiveSession(ctx, tp, baseURL, acc.Credential, "", rt.transportProfile)
	if err != nil {
		return nil, err
	}

	if me, meErr := a.fetchMe(ctx, tp, baseURL, acc.Credential, rt.transportProfile); meErr == nil {
		rt.meProfile = me
	}
	_ = a.sendAdsBeforeSession(ctx, tp, baseURL, acc.Credential, rt.adsSessionID, rt.device, rt.transportProfile)

	if session.Status == "active" && session.InstanceID != "" {
		if session.Model == model {
			state := mergeRuntimeState(sessionState(baseURL, session), rt)
			state, rt, err = a.ensureSessionRun(ctx, acc.ID, key, rt, model, state)
			if err != nil {
				return nil, err
			}
			if a.scheduler != nil {
				a.scheduler.observeSession(acc.ID, model, session, state)
			}
			a.setRuntime(acc.ID, key, rt)
			return state, nil
		}
		if !canReclaimActiveSession(session.Model, model) {
			if group, ok := quotaGroupForModel(session.Model); ok && group == QuotaGroupPremiumShared {
				return nil, channels.AccountUnavailablef("freebuff: active premium session model %q preserved; requested %q", session.Model, model)
			}
			return nil, channels.AccountUnavailablef("freebuff: active session model %q is not reclaimable for requested %q", session.Model, model)
		}
		afterRelease, err := a.releaseReclaimableSession(ctx, tp, baseURL, acc.Credential, rt.transportProfile, session.Model, model)
		if err != nil {
			return nil, err
		}
		session.mergeQuotaFallback(afterRelease)
	}
	if premiumQuotaExhausted(model, session, time.Now()) {
		if a.scheduler != nil {
			a.scheduler.markPremiumDepleted(acc.ID, model, session)
		}
		return nil, channels.AccountUnavailablef("freebuff: premium session quota exhausted for model %q", model)
	}

	created, err := a.joinSession(ctx, tp, baseURL, acc.Credential, model, rt.transportProfile)
	if err != nil {
		return nil, err
	}
	if created.InstanceID == "" {
		if created.Status != "" {
			return nil, channels.AccountUnavailablef("freebuff: session status %q for requested model %q", created.Status, model)
		}
		return nil, fmt.Errorf("freebuff: session response missing instance id")
	}
	if created.Model != "" && created.Model != model {
		return nil, channels.AccountUnavailablef("freebuff: assigned model %q does not match requested %q", created.Model, model)
	}
	if created.Model == "" {
		created.Model = model
	}
	created.mergeQuotaFallback(session)

	state := mergeRuntimeState(sessionState(baseURL, created), rt)
	state, rt, err = a.ensureSessionRun(ctx, acc.ID, key, rt, model, state)
	if err != nil {
		return nil, err
	}
	if a.scheduler != nil {
		a.scheduler.observeSession(acc.ID, model, created, state)
	}
	a.setRuntime(acc.ID, key, rt)
	a.sendHealthz(ctx, tp, baseURL, rt.transportProfile)
	return state, nil
}

func (a *Adapter) CanReclaimSessionForRequest(state channels.State, in *channels.InboundRequest) bool {
	if state == nil || in == nil {
		return false
	}
	activeModel := state.String(stateModel)
	if activeModel == "" {
		return false
	}
	key := a.SelectionKey(in)
	requestedModel, err := modelFromKey(key)
	if err != nil {
		return false
	}
	return canReclaimActiveSession(activeModel, requestedModel)
}

func (a *Adapter) ReclaimedSessionState(state channels.State) (channels.State, bool) {
	if state == nil || state.String(stateInstanceID) == "" {
		return nil, false
	}
	out := make(channels.State, len(state)+1)
	for k, v := range state {
		out[k] = v
	}
	out[stateStatus] = "ended"
	return out, true
}

func (a *Adapter) ReclaimSessionForRequest(ctx context.Context, acc channels.Account, state channels.State, in *channels.InboundRequest, tp channels.Transport) (channels.State, bool, error) {
	if !a.CanReclaimSessionForRequest(state, in) {
		return nil, false, nil
	}
	if state == nil || state.String(stateInstanceID) == "" {
		return nil, false, nil
	}
	if strings.TrimSpace(acc.Credential) == "" {
		return nil, false, channels.AccountUnavailablef("freebuff: account %q has empty credential", acc.ID)
	}
	if tp == nil {
		return nil, false, channels.AccountUnavailablef("freebuff: transport unavailable for reclaim")
	}
	requestedModel, err := modelFromKey(a.SelectionKey(in))
	if err != nil {
		return nil, false, err
	}
	activeModel := state.String(stateModel)
	baseURL := firstNonEmptyString(state.String(stateBaseURL), a.baseURLFor(acc))
	a.finishSessionRun(ctx, acc.ID, keyPrefix+CanonicalModel(activeModel), state, "reclaimed")
	afterRelease, err := a.releaseReclaimableSession(ctx, tp, baseURL, acc.Credential, accountTransportProfile(acc), activeModel, requestedModel)
	if err != nil {
		return nil, false, err
	}
	reclaimed, ok := a.ReclaimedSessionState(state)
	if !ok {
		return nil, false, nil
	}
	if afterRelease.RawJSON != "" {
		reclaimed[stateRawSessionJSON] = afterRelease.RawJSON
	}
	return reclaimed, true, nil
}

func (a *Adapter) releaseReclaimableSession(ctx context.Context, tp channels.Transport, baseURL, credential string, transportProfile channels.TransportProfile, activeModel, requestedModel string) (upstreamSession, error) {
	if _, err := a.releaseSession(ctx, tp, baseURL, credential, transportProfile); err != nil {
		return upstreamSession{}, channels.AccountUnavailablef("freebuff: release unlimited session model %q for requested %q: %v", activeModel, requestedModel, err)
	}
	afterRelease, err := a.fetchActiveSession(ctx, tp, baseURL, credential, "", transportProfile)
	if err != nil {
		return upstreamSession{}, channels.AccountUnavailablef("freebuff: verify released session model %q for requested %q: %v", activeModel, requestedModel, err)
	}
	if afterRelease.Status == "active" && afterRelease.InstanceID != "" {
		return upstreamSession{}, channels.AccountUnavailablef("freebuff: active session model %q remains after release for requested %q", afterRelease.Model, requestedModel)
	}
	return afterRelease, nil
}

func (a *Adapter) RefreshAccountState(ctx context.Context, acc channels.Account, tp channels.Transport) (channels.State, error) {
	if strings.TrimSpace(acc.Credential) == "" {
		return nil, fmt.Errorf("freebuff: account %q has empty credential", acc.ID)
	}
	baseURL := a.baseURLFor(acc)
	transportProfile := accountTransportProfile(acc)
	session, err := a.fetchActiveSession(ctx, tp, baseURL, acc.Credential, "", transportProfile)
	if err != nil {
		return nil, err
	}
	return sessionState(baseURL, session), nil
}

func (a *Adapter) RestoreSession(ctx context.Context, acc channels.Account, key string, state channels.State, tp channels.Transport) (channels.State, bool, error) {
	model, err := modelFromKey(key)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(acc.Credential) == "" {
		return nil, false, fmt.Errorf("freebuff: account %q has empty credential", acc.ID)
	}
	if tp == nil {
		return nil, false, fmt.Errorf("freebuff: transport unavailable for restore")
	}
	instanceID := ""
	if state != nil {
		instanceID = state.String(stateInstanceID)
		if persistedModel := state.String(stateModel); persistedModel != "" && CanonicalModel(persistedModel) != model {
			return nil, false, nil
		}
	}
	if instanceID == "" {
		return nil, false, nil
	}

	baseURL := firstNonEmptyString(state.String(stateBaseURL), a.baseURLFor(acc))
	rt := runtime{
		credential:       acc.Credential,
		baseURL:          baseURL,
		transport:        tp,
		transportProfile: accountTransportProfile(acc),
		adsSessionID:     generateAdsSessionID(),
		device:           generateDeviceProfile(),
	}
	session, err := a.fetchActiveSession(ctx, tp, baseURL, acc.Credential, instanceID, rt.transportProfile)
	if err != nil {
		return nil, false, err
	}
	if session.Status != "active" || session.InstanceID == "" {
		return nil, false, nil
	}
	if session.InstanceID != instanceID {
		return nil, false, nil
	}
	if session.Model == "" || CanonicalModel(session.Model) != model {
		return nil, false, nil
	}
	if expires := parseExpiresAtUnix(session.ExpiresAt); expires > 0 && expires <= time.Now().Unix() {
		return nil, false, nil
	}

	restored := mergeRuntimeState(sessionState(baseURL, session), rt)
	restored, rt, err = a.ensureSessionRun(ctx, acc.ID, key, rt, model, restored)
	if err != nil {
		return nil, false, err
	}
	if a.scheduler != nil {
		a.scheduler.observeSession(acc.ID, model, session, restored)
	}
	a.setRuntime(acc.ID, key, rt)
	return restored, true, nil
}

func (a *Adapter) ClassifySessionHealth(_ channels.State, c channels.ResponseClass) channels.Verdict {
	switch c {
	case channels.ClassAuthExpired, channels.ClassFatal:
		return channels.VerdictExpire
	default:
		return channels.VerdictHealthy
	}
}

func (a *Adapter) Heartbeat(_ context.Context, _ channels.Account, _ channels.State, _ channels.Transport) error {
	return nil
}

func (a *Adapter) PrepareOutbound(ctx context.Context, lease *channels.Lease, in *channels.InboundRequest) (*channels.OutboundRequest, error) {
	if in == nil || in.Path != "/v1/chat/completions" {
		return nil, ErrUnsupportedPath
	}
	body, model, stream, err := decodeChatBody(in.Body)
	if err != nil {
		return nil, err
	}
	if stream {
		return nil, ErrStreaming
	}
	if lease == nil {
		return nil, fmt.Errorf("freebuff: nil lease")
	}
	if stateModel := lease.State.String(stateModel); stateModel != "" && stateModel != model {
		return nil, fmt.Errorf("freebuff: lease model %q does not match request model %q", stateModel, model)
	}
	rt, ok := a.runtimeFor(lease.AccountID, lease.Key)
	if !ok {
		return nil, fmt.Errorf("freebuff: runtime not found for session")
	}

	instanceID := lease.State.String(stateInstanceID)
	if instanceID == "" {
		return nil, fmt.Errorf("freebuff: session missing instance id")
	}

	userMsg := extractUserMessage(body)
	adsStarted := time.Now()
	adsQueued := a.sendAdsWithMessage(ctx, rt.transport, rt.baseURL, rt.credential, rt.adsSessionID, rt.device, userMsg, rt.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_ads_ms", adsStarted)
	recordFreeBuffPhase(ctx, "freebuff_ads_enqueue_ms", adsStarted)
	if userMsg != "" {
		recordFreeBuffBool(ctx, "freebuff_ads_async", true)
		recordFreeBuffBool(ctx, "freebuff_ads_enqueued", adsQueued)
	}

	runSetupStarted := time.Now()
	runSetup, rt, err := a.prepareFreeBuffRun(ctx, lease.AccountID, lease.Key, rt, model)
	recordFreeBuffPhase(ctx, "freebuff_run_setup_ms", runSetupStarted)
	if err != nil {
		return nil, err
	}
	rt.parentRunID = runSetup.parentRunID
	rt.childRunID = runSetup.childRunID

	a.setRuntimeWithRuns(lease.AccountID, lease.Key, rt)
	if a.effectiveRunSetupMode() != freebuffRunSetupModeSessionReuse {
		a.setRunID(lease.AccountID, lease.Key, runSetup.parentRunID)
	}

	outBody := buildCodebuffBody(body, model, runSetup.parentRunID, instanceID, false)
	addCodebuffStop(outBody)
	payload, err := json.Marshal(outBody)
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal outbound: %w", err)
	}

	return &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(rt.baseURL, "/api/v1/chat/completions"),
		Headers:          codebuffHeaders(rt.credential),
		Body:             payload,
		TransportProfile: rt.applyTransportProfile(freebuffChatTransportProfile()),
	}, nil
}

func (a *Adapter) PrepareStreamOutbound(ctx context.Context, lease *channels.Lease, in *channels.InboundRequest) (*channels.OutboundRequest, error) {
	if in == nil {
		return nil, ErrUnsupportedPath
	}
	var body map[string]any
	var model string
	var stream bool
	var err error
	switch in.Path {
	case "/v1/chat/completions":
		body, model, stream, err = decodeChatBody(in.Body)
	case "/v1/messages":
		anthropicBody, anthropicModel, anthropicStream, decodeErr := decodeAnthropicBody(in.Body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		body = anthropicToOpenAI(anthropicBody, anthropicModel)
		model = anthropicModel
		stream = anthropicStream
	default:
		return nil, ErrUnsupportedPath
	}
	if err != nil {
		return nil, err
	}
	if !stream {
		return nil, fmt.Errorf("freebuff: request is not streaming")
	}
	if lease == nil {
		return nil, fmt.Errorf("freebuff: nil lease")
	}
	if stateModel := lease.State.String(stateModel); stateModel != "" && stateModel != model {
		return nil, fmt.Errorf("freebuff: lease model %q does not match request model %q", stateModel, model)
	}
	rt, ok := a.runtimeFor(lease.AccountID, lease.Key)
	if !ok {
		return nil, fmt.Errorf("freebuff: runtime not found for session")
	}

	instanceID := lease.State.String(stateInstanceID)
	if instanceID == "" {
		return nil, fmt.Errorf("freebuff: session missing instance id")
	}

	userMsg := extractUserMessage(body)
	adsStarted := time.Now()
	adsQueued := a.sendAdsWithMessage(ctx, rt.transport, rt.baseURL, rt.credential, rt.adsSessionID, rt.device, userMsg, rt.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_ads_ms", adsStarted)
	recordFreeBuffPhase(ctx, "freebuff_ads_enqueue_ms", adsStarted)
	if userMsg != "" {
		recordFreeBuffBool(ctx, "freebuff_ads_async", true)
		recordFreeBuffBool(ctx, "freebuff_ads_enqueued", adsQueued)
	}

	runSetupStarted := time.Now()
	runSetup, rt, err := a.prepareFreeBuffRun(ctx, lease.AccountID, lease.Key, rt, model)
	recordFreeBuffPhase(ctx, "freebuff_run_setup_ms", runSetupStarted)
	if err != nil {
		return nil, err
	}
	rt.parentRunID = runSetup.parentRunID
	rt.childRunID = runSetup.childRunID

	a.setRuntimeWithRuns(lease.AccountID, lease.Key, rt)
	if a.effectiveRunSetupMode() != freebuffRunSetupModeSessionReuse {
		a.setRunID(lease.AccountID, lease.Key, runSetup.parentRunID)
	}

	outBody := buildCodebuffBody(body, model, runSetup.parentRunID, instanceID, true)
	addCodebuffStop(outBody)
	payload, err := json.Marshal(outBody)
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal stream outbound: %w", err)
	}
	headers := codebuffHeaders(rt.credential)
	headers.Set("Accept", "text/event-stream")
	headers.Set("Accept-Encoding", "identity")
	return &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(rt.baseURL, "/api/v1/chat/completions"),
		Headers:          headers,
		Body:             payload,
		TransportProfile: rt.applyTransportProfile(freebuffChatTransportProfile()),
	}, nil
}

func (a *Adapter) ClassifyStreamResponse(status int, headers http.Header, bodyPreview []byte) channels.ResponseClass {
	return a.ClassifyResponse(status, headers, bodyPreview)
}

func (a *Adapter) RetryOutbound(ctx context.Context, lease *channels.Lease, _ *channels.InboundRequest, outcome channels.RetryOutcome) (*channels.OutboundRequest, bool, error) {
	if lease == nil || outcome.Request == nil {
		return nil, false, nil
	}
	if !isRunIDStateError(outcome.Status, outcome.BodyPreview) {
		return nil, false, nil
	}
	rt, ok := a.runtimeFor(lease.AccountID, lease.Key)
	if !ok {
		return nil, false, fmt.Errorf("freebuff: runtime not found for run retry")
	}
	model := lease.State.String(stateModel)
	if model == "" {
		if _, decodedModel, _, err := decodeChatBody(outcome.Request.Body); err == nil {
			model = decodedModel
		}
	}
	if model == "" {
		return nil, false, fmt.Errorf("freebuff: missing model for run retry")
	}
	replaced, err := a.replaceSessionRun(ctx, lease.AccountID, lease.Key, rt, model)
	if err != nil {
		return nil, false, err
	}
	retryRequest, err := requestWithRunID(outcome.Request, replaced.sessionRunID)
	if err != nil {
		return nil, false, err
	}
	recordFreeBuffBool(ctx, "freebuff_session_run_replaced", true)
	return retryRequest, true, nil
}

func isRunIDStateError(status int, preview []byte) bool {
	if status < 400 || len(preview) == 0 {
		return false
	}
	body := strings.ToLower(string(preview))
	return strings.Contains(body, "runid not running") || strings.Contains(body, "runid not found")
}

func (a *Adapter) replaceSessionRun(ctx context.Context, accountID, key string, rt runtime, model string) (runtime, error) {
	rt.sessionRunID = ""
	rt.parentRunID = ""
	rt.sessionRunCreatedAt = time.Time{}
	rt.sessionRunExpiresAt = time.Time{}
	state := channels.State{}
	nextState, nextRuntime, err := a.ensureSessionRun(ctx, accountID, key, rt, model, state)
	_ = nextState
	if err != nil {
		return rt, err
	}
	a.setRuntimeWithRuns(accountID, key, nextRuntime)
	return nextRuntime, nil
}

func requestWithRunID(req *channels.OutboundRequest, runID string) (*channels.OutboundRequest, error) {
	if req == nil || runID == "" {
		return nil, fmt.Errorf("freebuff: cannot retry without request and run id")
	}
	var body map[string]any
	dec := json.NewDecoder(bytes.NewReader(req.Body))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		return nil, fmt.Errorf("freebuff: decode retry body: %w", err)
	}
	meta, _ := body["codebuff_metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		body["codebuff_metadata"] = meta
	}
	meta["run_id"] = runID
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal retry body: %w", err)
	}
	out := *req
	out.Headers = req.Headers.Clone()
	out.Body = payload
	return &out, nil
}

type freebuffRunSetup struct {
	parentRunID string
	childRunID  string
}

func (a *Adapter) effectiveRunSetupMode() freebuffRunSetupMode {
	if a.runSetupMode == "" {
		return freebuffRunSetupModeSessionReuse
	}
	return a.runSetupMode
}

func (a *Adapter) ensureSessionRun(ctx context.Context, accountID, key string, rt runtime, model string, state channels.State) (channels.State, runtime, error) {
	if a.effectiveRunSetupMode() != freebuffRunSetupModeSessionReuse {
		return state, rt, nil
	}
	if rt.sessionRunID != "" && !sessionRunExpired(rt.sessionRunExpiresAt) {
		return stateWithSessionRun(state, rt), rt, nil
	}
	started := time.Now()
	parentStarted := time.Now()
	runID, err := a.createRun(ctx, rt.transport, rt.baseURL, rt.credential, model, rt.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_create_parent_run_ms", parentStarted)
	if err != nil {
		return nil, rt, err
	}
	rt.sessionRunID = runID
	rt.parentRunID = runID
	rt.sessionRunCreatedAt = started
	rt.sessionRunExpiresAt = a.sessionRunExpiresAt(state, started)
	a.enqueueSessionRunTree(ctx, rt)
	return stateWithSessionRun(state, rt), rt, nil
}

func (a *Adapter) enqueueSessionRunTree(ctx context.Context, rt runtime) {
	if rt.sessionRunID == "" {
		return
	}
	if err := ctx.Err(); err != nil {
		return
	}
	enqueued := a.async.enqueueRunTree(runTreeJob{
		transport:        rt.transport,
		baseURL:          rt.baseURL,
		credential:       rt.credential,
		transportProfile: rt.transportProfile,
		parentRunID:      rt.sessionRunID,
	})
	if !enqueued {
		logRunTreeQueueFull()
	}
}

func (a *Adapter) sessionRunExpiresAt(state channels.State, started time.Time) time.Time {
	expiresAt := started.Add(a.sessionRunMaxAge)
	if a.sessionRunMaxAge <= 0 {
		expiresAt = started.Add(time.Hour)
	}
	if stateExpires, ok := sessionStateExpiresAt(state); ok && stateExpires.Before(expiresAt) {
		expiresAt = stateExpires
	}
	return expiresAt
}

func stateWithSessionRun(state channels.State, rt runtime) channels.State {
	out := cloneFreeBuffState(state)
	if rt.sessionRunID != "" {
		out[stateSessionRunID] = rt.sessionRunID
	}
	if !rt.sessionRunCreatedAt.IsZero() {
		out[stateSessionRunCreated] = rt.sessionRunCreatedAt.Unix()
	}
	if !rt.sessionRunExpiresAt.IsZero() {
		out[stateSessionRunExpires] = rt.sessionRunExpiresAt.Unix()
	}
	return out
}

func cloneFreeBuffState(state channels.State) channels.State {
	if len(state) == 0 {
		return channels.State{}
	}
	out := make(channels.State, len(state))
	for key, value := range state {
		out[key] = value
	}
	return out
}

func sessionRunExpired(expiresAt time.Time) bool {
	return !expiresAt.IsZero() && !time.Now().Before(expiresAt)
}

func sessionStateExpiresAt(state channels.State) (time.Time, bool) {
	if state == nil {
		return time.Time{}, false
	}
	switch v := state[stateExpiresAtUnix].(type) {
	case int64:
		if v > 0 {
			return time.Unix(v, 0), true
		}
	case int:
		if v > 0 {
			return time.Unix(int64(v), 0), true
		}
	case float64:
		if v > 0 {
			return time.Unix(int64(v), 0), true
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return time.Unix(n, 0), true
		}
	}
	return time.Time{}, false
}

func (a *Adapter) prepareFreeBuffRun(ctx context.Context, accountID, key string, rt runtime, model string) (freebuffRunSetup, runtime, error) {
	mode := a.effectiveRunSetupMode()
	recordFreeBuffString(ctx, "freebuff_run_setup_mode", mode.String())

	if mode == freebuffRunSetupModeSessionReuse {
		if rt.sessionRunID != "" && !sessionRunExpired(rt.sessionRunExpiresAt) {
			recordFreeBuffBool(ctx, "freebuff_session_run_reused", true)
			return freebuffRunSetup{parentRunID: rt.sessionRunID}, rt, nil
		}
		state := channels.State{}
		if !rt.sessionRunExpiresAt.IsZero() {
			state[stateSessionRunExpires] = rt.sessionRunExpiresAt.Unix()
		}
		nextState, nextRuntime, err := a.ensureSessionRun(ctx, accountID, key, rt, model, state)
		_ = nextState
		if err != nil {
			return freebuffRunSetup{}, rt, err
		}
		recordFreeBuffBool(ctx, "freebuff_session_run_reused", false)
		return freebuffRunSetup{parentRunID: nextRuntime.sessionRunID}, nextRuntime, nil
	}

	parentStarted := time.Now()
	parentRunID, err := a.createRun(ctx, rt.transport, rt.baseURL, rt.credential, model, rt.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_create_parent_run_ms", parentStarted)
	if err != nil {
		return freebuffRunSetup{}, rt, err
	}

	if mode == freebuffRunSetupModeParentSyncAsyncTree {
		enqueueStarted := time.Now()
		enqueued := a.async.enqueueRunTree(runTreeJob{
			transport:        rt.transport,
			baseURL:          rt.baseURL,
			credential:       rt.credential,
			transportProfile: rt.transportProfile,
			parentRunID:      parentRunID,
		})
		recordFreeBuffPhase(ctx, "freebuff_run_tree_async_enqueue_ms", enqueueStarted)
		recordFreeBuffBool(ctx, "freebuff_run_tree_async_enqueued", enqueued)
		recordFreeBuffBool(ctx, "freebuff_run_tree_async_dropped", !enqueued)
		if !enqueued {
			logRunTreeQueueFull()
		}
		return freebuffRunSetup{parentRunID: parentRunID}, rt, nil
	}

	childCreateStarted := time.Now()
	childRunID, err := a.createChildRun(ctx, rt.transport, rt.baseURL, rt.credential, parentRunID, rt.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_create_child_run_ms", childCreateStarted)
	if err != nil {
		return freebuffRunSetup{}, rt, err
	}

	childStart := time.Now()
	parallelStarted := time.Now()
	if err := a.completeFreeBuffRunTree(ctx, rt.transport, rt.baseURL, rt.credential, rt.transportProfile, parentRunID, childRunID, childStart); err != nil {
		recordFreeBuffPhase(ctx, "freebuff_setup_parallel_wait_ms", parallelStarted)
		return freebuffRunSetup{}, rt, err
	}
	recordFreeBuffPhase(ctx, "freebuff_setup_parallel_wait_ms", parallelStarted)
	return freebuffRunSetup{parentRunID: parentRunID, childRunID: childRunID}, rt, nil
}

func (a *Adapter) completeFreeBuffRunTree(
	ctx context.Context,
	tp channels.Transport,
	baseURL string,
	credential string,
	transportProfile channels.TransportProfile,
	parentRunID string,
	childRunID string,
	childStart time.Time,
) error {
	childErr := make(chan error, 1)
	parentErr := make(chan error, 1)

	go func() {
		childStepStarted := time.Now()
		if err := a.recordRunStep(ctx, tp, baseURL, credential, childRunID, "", "completed", 1, childStart, transportProfile); err != nil {
			recordFreeBuffPhase(ctx, "freebuff_child_step_ms", childStepStarted)
			childErr <- err
			return
		}
		recordFreeBuffPhase(ctx, "freebuff_child_step_ms", childStepStarted)
		childFinishStarted := time.Now()
		err := a.finishChildRun(ctx, tp, baseURL, credential, childRunID, transportProfile)
		recordFreeBuffPhase(ctx, "freebuff_child_finish_ms", childFinishStarted)
		childErr <- err
	}()

	go func() {
		parentStepStarted := time.Now()
		err := a.recordParentStep1(ctx, tp, baseURL, credential, parentRunID, []string{childRunID}, childStart, transportProfile)
		recordFreeBuffPhase(ctx, "freebuff_parent_step_ms", parentStepStarted)
		parentErr <- err
	}()

	if err := errors.Join(<-childErr, <-parentErr); err != nil {
		return err
	}
	return nil
}

func recordFreeBuffPhase(ctx context.Context, name string, started time.Time) {
	if trace := phasetiming.FromContext(ctx); trace != nil {
		trace.Duration(name, time.Since(started))
	}
}

func recordFreeBuffBool(ctx context.Context, name string, value bool) {
	if trace := phasetiming.FromContext(ctx); trace != nil {
		trace.Bool(name, value)
	}
}

func recordFreeBuffString(ctx context.Context, name string, value string) {
	if trace := phasetiming.FromContext(ctx); trace != nil {
		trace.String(name, value)
	}
}

func (a *Adapter) ClassifyResponse(status int, _ http.Header, bodyPreview []byte) channels.ResponseClass {
	body := strings.ToLower(string(bodyPreview))
	if strings.Contains(body, "deployment_outside_hours") {
		return channels.ClassRetryable
	}
	if strings.Contains(body, "free_mode_rate_limited") {
		return channels.ClassRateLimited
	}
	switch {
	case status >= 200 && status < 300:
		return channels.ClassOk
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return channels.ClassAuthExpired
	case status == http.StatusTooManyRequests:
		return channels.ClassRateLimited
	case status >= 500:
		return channels.ClassRetryable
	case strings.Contains(body, "rate limit") || strings.Contains(body, "too many requests"):
		return channels.ClassRateLimited
	default:
		return channels.ClassFatal
	}
}

func (a *Adapter) Finalize(ctx context.Context, lease *channels.Lease, outcome channels.FinalizeOutcome) error {
	if lease == nil || lease.State == nil {
		return nil
	}

	rt, ok := a.runtimeFor(lease.AccountID, lease.Key)
	if !ok {
		return fmt.Errorf("freebuff: runtime not found for finalizer")
	}
	if rt.sessionRunID != "" {
		recordFreeBuffBool(ctx, "freebuff_parent_finish_deferred", true)
		return nil
	}

	runID := runIDFromOutboundRequest(outcome.Request)
	if runID == "" {
		runID = a.popRunID(lease.AccountID, lease.Key)
	}
	if runID == "" {
		return nil
	}

	status := "failed"
	steps := 2
	job := finalizeJob{
		transport:        rt.transport,
		baseURL:          rt.baseURL,
		credential:       rt.credential,
		transportProfile: rt.transportProfile,
		runID:            runID,
		status:           status,
		steps:            steps,
		startedAt:        time.Now(),
	}
	if outcome.Err == nil && outcome.Class == channels.ClassOk && outcome.Status >= 200 && outcome.Status < 300 {
		status = "completed"
		steps = 3
		job.status = status
		job.steps = steps
		job.messageID = responseMessageID(outcome.Response)
		job.recordStep = true
	}
	enqueueStarted := time.Now()
	if a.async.enqueueFinalize(job) {
		recordFreeBuffPhase(ctx, "freebuff_finalize_enqueue_ms", enqueueStarted)
		return nil
	}
	recordFreeBuffPhase(ctx, "freebuff_finalize_enqueue_ms", enqueueStarted)
	logFinalizerQueueFull(status)
	inlineCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeInlineTimeout)
	defer cancel()
	if err := job.run(inlineCtx, a); err != nil {
		return fmt.Errorf("freebuff: finalize inline fallback: %w", err)
	}
	return nil
}

func (a *Adapter) FinalizeSession(ctx context.Context, event channels.SessionFinalizeEvent) {
	if event.ChannelID != "" && event.ChannelID != ID {
		return
	}
	a.finishSessionRun(ctx, event.AccountID, event.SelectionKey, event.State, event.Reason)
}

func (a *Adapter) finishSessionRun(ctx context.Context, accountID, key string, state channels.State, reason string) {
	rt, ok := a.takeRuntime(accountID, key)
	if !ok || rt.sessionRunID == "" {
		return
	}
	if state != nil {
		if stateRunID := state.String(stateSessionRunID); stateRunID != "" && stateRunID != rt.sessionRunID {
			return
		}
	}
	status := "completed"
	if reason == "failed" {
		status = "failed"
	}
	job := finalizeJob{
		transport:        rt.transport,
		baseURL:          rt.baseURL,
		credential:       rt.credential,
		transportProfile: rt.transportProfile,
		runID:            rt.sessionRunID,
		status:           status,
		steps:            3,
		startedAt:        time.Now(),
	}
	if a.async.enqueueFinalize(job) {
		return
	}
	logFinalizerQueueFull(status)
	inlineCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeInlineTimeout)
	defer cancel()
	if err := job.run(inlineCtx, a); err != nil {
		logFinalizerQueueFull(status)
	}
}

func accountTransportProfile(acc channels.Account) channels.TransportProfile {
	return proxypool.ApplyTransportProfile(acc.Metadata, channels.TransportProfile{})
}

func (rt runtime) applyTransportProfile(profile channels.TransportProfile) channels.TransportProfile {
	return proxypool.MergeTransportProfile(profile, rt.transportProfile)
}

func responseMessageID(resp *channels.OutboundResponse) string {
	if resp == nil || len(resp.Body) == 0 {
		return ""
	}
	var decoded struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err == nil && decoded.ID != "" {
		return decoded.ID
	}
	for _, line := range strings.Split(string(resp.Body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		if err := json.Unmarshal([]byte(payload), &decoded); err == nil && decoded.ID != "" {
			return decoded.ID
		}
	}
	return ""
}

func (a *Adapter) TokenUsage(_ *channels.OutboundRequest, resp *channels.OutboundResponse) (int, int, bool) {
	if resp == nil || len(resp.Body) == 0 {
		return 0, 0, false
	}
	var decoded struct {
		Usage struct {
			PromptTokens     json.Number `json:"prompt_tokens"`
			CompletionTokens json.Number `json:"completion_tokens"`
		} `json:"usage"`
	}
	dec := json.NewDecoder(bytes.NewReader(resp.Body))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return 0, 0, false
	}
	in, inOK := numberInt(decoded.Usage.PromptTokens)
	out, outOK := numberInt(decoded.Usage.CompletionTokens)
	if !inOK && !outOK {
		return 0, 0, false
	}
	return in, out, true
}

func (a *Adapter) baseURLFor(acc channels.Account) string {
	if acc.Metadata != nil {
		if v, ok := acc.Metadata["base_url"].(string); ok {
			if normalized := normalizeBaseURL(v); normalized != "" {
				return normalized
			}
		}
	}
	return a.baseURL
}

func (a *Adapter) setRuntime(accountID, key string, rt runtime) {
	a.mu.Lock()
	a.runtimes[runtimeKey(accountID, key)] = rt
	a.mu.Unlock()
}

func (a *Adapter) runtimeFor(accountID, key string) (runtime, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rt, ok := a.runtimes[runtimeKey(accountID, key)]
	return rt, ok
}

func (a *Adapter) takeRuntime(accountID, key string) (runtime, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rtKey := runtimeKey(accountID, key)
	rt, ok := a.runtimes[rtKey]
	if ok {
		delete(a.runtimes, rtKey)
	}
	return rt, ok
}

func (a *Adapter) setRunID(accountID, key, runID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	rtKey := runtimeKey(accountID, key)
	rt, ok := a.runtimes[rtKey]
	if !ok {
		return
	}
	rt.runID = runID
	a.runtimes[rtKey] = rt
}

func (a *Adapter) popRunID(accountID, key string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	rtKey := runtimeKey(accountID, key)
	rt, ok := a.runtimes[rtKey]
	if !ok {
		return ""
	}
	runID := rt.runID
	rt.runID = ""
	a.runtimes[rtKey] = rt
	return runID
}

func runtimeKey(accountID, key string) string { return accountID + "|" + key }

func (a *Adapter) setRuntimeWithRuns(accountID, key string, rt runtime) {
	a.mu.Lock()
	a.runtimes[runtimeKey(accountID, key)] = rt
	a.mu.Unlock()
}

func extractUserMessage(body map[string]any) string {
	msgs, _ := body["messages"].([]any)
	for _, raw := range msgs {
		msg, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if stringValue(msg["role"]) != "user" {
			continue
		}
		content := msg["content"]
		switch v := content.(type) {
		case string:
			return "<user_message>" + v + "</user_message>"
		case []any:
			switch {
			case len(v) == 0:
				continue
			case len(v) == 1:
				if block, ok := v[0].(map[string]any); ok {
					return "<user_message>" + stringValue(block["text"]) + "</user_message>"
				}
			default:
				var parts []string
				for _, b := range v {
					if block, ok := b.(map[string]any); ok {
						if t := stringValue(block["text"]); t != "" {
							parts = append(parts, t)
						}
					}
				}
				return "<user_message>" + strings.Join(parts, "\n") + "</user_message>"
			}
		}
	}
	return ""
}

func modelFromKey(key string) (string, error) {
	switch key {
	case keyInvalidJSON:
		return "", ErrInvalidJSON
	case keyUnsupported:
		return "", ErrUnsupportedPath
	}
	if !strings.HasPrefix(key, keyPrefix) {
		return "", fmt.Errorf("freebuff: invalid selection key %q", key)
	}
	model := strings.TrimPrefix(key, keyPrefix)
	if model == "" {
		return "", fmt.Errorf("freebuff: empty model")
	}
	profile, ok := ModelProfileFor(model)
	if !ok || !profile.Enabled {
		return "", fmt.Errorf("freebuff: unsupported model %q", model)
	}
	return model, nil
}

func normalizeBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

func joinURL(base, path string) string {
	u, err := url.Parse(base)
	if err != nil {
		return strings.TrimRight(base, "/") + path
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func clientID() string {
	var b [5]byte
	if _, err := rand.Read(b[:]); err == nil {
		return "p" + hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("p%x", time.Now().UnixNano()%100000000)
}
