package freebuff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/phasetiming"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
)

type recordedRequest struct {
	Method           string
	URL              string
	Headers          http.Header
	Body             []byte
	TransportProfile channels.TransportProfile
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestFreeBuffReuseKeyIsAccountScopedAndRedacted(t *testing.T) {
	first := freebuffAPITransportProfile("secret-token-a").ReuseKey
	second := freebuffAPITransportProfile("secret-token-b").ReuseKey
	if first == "" || second == "" || first == second {
		t.Fatalf("reuse keys = %q / %q, want distinct account-scoped keys", first, second)
	}
	if strings.Contains(first, "secret-token-a") || strings.Contains(second, "secret-token-b") {
		t.Fatalf("reuse key leaked raw credential: %q / %q", first, second)
	}
}

type sequenceTransport struct {
	t        *testing.T
	mu       sync.Mutex
	requests []recordedRequest
	respond  func(*channels.OutboundRequest, int) (*channels.OutboundResponse, error)
}

type failingTransport struct {
	err error
}

func (tp failingTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return nil, tp.err
}

func (tp *sequenceTransport) Do(_ context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	tp.t.Helper()
	if req == nil {
		tp.t.Fatal("nil request")
	}
	tp.mu.Lock()
	idx := len(tp.requests)
	tp.requests = append(tp.requests, recordedRequest{
		Method:           req.Method,
		URL:              req.URL,
		Headers:          req.Headers.Clone(),
		Body:             append([]byte(nil), req.Body...),
		TransportProfile: req.TransportProfile,
	})
	tp.mu.Unlock()
	resp, err := tp.respond(req, idx)
	if err != nil {
		if defaultResp := tp.defaultRespond(req); defaultResp != nil {
			return defaultResp, nil
		}
		return nil, err
	}
	return resp, nil
}

func runAsyncSideEffects(t *testing.T, a *Adapter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("async side effects worker did not stop")
		}
	})
}

func waitForCondition(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", label)
		case <-tick.C:
		}
	}
}

func captureProcessLog(t *testing.T) *lockedBuffer {
	t.Helper()
	var buf lockedBuffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	})
	return &buf
}

func (tp *sequenceTransport) defaultRespond(req *channels.OutboundRequest) *channels.OutboundResponse {
	switch {
	case strings.Contains(req.URL, "/api/v1/me"):
		return jsonResponse(200, map[string]any{"id": "test-id", "email": "test@test.com"})
	case strings.Contains(req.URL, "/api/v1/ads/impression"):
		return jsonResponse(200, map[string]any{})
	case strings.Contains(req.URL, "/api/v1/ads"):
		return jsonResponse(200, map[string]any{"impUrl": "https://ads.test/imp"})
	case strings.Contains(req.URL, "/api/healthz"):
		return jsonResponse(200, map[string]any{"status": "ok"})
	case strings.Contains(req.URL, "/api/v1/agent-runs") && strings.Contains(string(req.Body), "context-pruner") && strings.Contains(string(req.Body), "START"):
		return jsonResponse(200, map[string]any{"runId": "child-run-id"})
	case strings.Contains(req.URL, "/api/v1/agent-runs") && strings.Contains(string(req.Body), "START"):
		return jsonResponse(200, map[string]any{"runId": "session-run-id"})
	case strings.Contains(req.URL, "/api/v1/agent-runs") && strings.Contains(string(req.Body), "FINISH"):
		return jsonResponse(200, map[string]any{})
	case strings.Contains(req.URL, "/api/v1/agent-runs/") && strings.Contains(req.URL, "/steps"):
		return jsonResponse(200, map[string]any{"stepId": "step-aux"})
	}
	return nil
}

func TestSelectionKeyCanonicalizesModel(t *testing.T) {
	a := New()
	in := &channels.InboundRequest{
		ChannelID: ID,
		Path:      "/v1/chat/completions",
		Body:      []byte(`{"model":"deepseekv4pro","stream":true}`),
	}
	if got, want := a.SelectionKey(in), "freebuff|deepseek/deepseek-v4-pro"; got != want {
		t.Fatalf("selection key = %q, want %q", got, want)
	}
}

func TestSelectionKeyCanonicalizesFlashModel(t *testing.T) {
	a := New()
	in := &channels.InboundRequest{
		ChannelID: ID,
		Path:      "/v1/chat/completions",
		Body:      []byte(`{"model":"deepseek-v4-flash","stream":true}`),
	}
	if got, want := a.SelectionKey(in), "freebuff|deepseek/deepseek-v4-flash"; got != want {
		t.Fatalf("selection key = %q, want %q", got, want)
	}
}

func TestModelCatalogAgentIDs(t *testing.T) {
	cases := []struct {
		model string
		want  string
	}{
		{model: "minimax/minimax-m2.7", want: "base2-free"},
		{model: "deepseek/deepseek-v4-flash", want: "base2-free-deepseek-flash"},
		{model: "moonshotai/kimi-k2.6", want: "base2-free-kimi"},
		{model: "deepseek/deepseek-v4-pro", want: "base2-free-deepseek"},
	}
	for _, tc := range cases {
		t.Run(tc.model, func(t *testing.T) {
			got, err := AgentIDForModel(tc.model)
			if err != nil {
				t.Fatalf("agent id: %v", err)
			}
			if got != tc.want {
				t.Fatalf("agent id = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestModelCatalogKeepsGLMParseOnlyUntilConfirmed(t *testing.T) {
	profile, ok := ModelProfileFor("glm-5.1")
	if !ok {
		t.Fatal("expected GLM profile to remain in catalog for quota parsing")
	}
	if profile.Enabled {
		t.Fatalf("GLM profile enabled = true, want parse-only disabled profile: %+v", profile)
	}
	if _, err := AgentIDForModel("z-ai/glm-5.1"); err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("expected disabled GLM to be unsupported for runtime use, got %v", err)
	}
}

func TestCreateSessionRejectsDifferentActiveModel(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL, "/api/v1/freebuff/session") {
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-existing",
				"model":      "deepseek/deepseek-v4-pro",
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err == nil || !strings.Contains(err.Error(), "active premium session") || !errors.Is(err, channels.ErrAccountUnavailable) {
		t.Fatalf("expected model mismatch error, got %v", err)
	}
}

func TestCreateSessionReclaimsUnlimitedActiveModel(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var deleteCalled, joinCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && !deleteCalled:
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		case req.Method == http.MethodDelete && path == "/api/v1/freebuff/session":
			deleteCalled = true
			if got := req.Headers.Get("x-freebuff-instance-id"); got != "" {
				t.Fatalf("delete instance id = %q, want empty", got)
			}
			return jsonResponse(200, map[string]any{
				"status":     "ended",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && deleteCalled:
			return jsonResponse(200, map[string]any{"status": "none"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			joinCalled = true
			if got := req.Headers.Get("x-freebuff-model"); got != "deepseek/deepseek-v4-pro" {
				t.Fatalf("join model = %q, want pro", got)
			}
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-pro",
				"model":      "deepseek/deepseek-v4-pro",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err != nil {
		t.Fatalf("create session after reclaim: %v", err)
	}
	if got := state.String(stateInstanceID); got != "inst-pro" {
		t.Fatalf("instance id = %q, want inst-pro", got)
	}
	if !deleteCalled || !joinCalled {
		t.Fatalf("delete=%v join=%v, want both true", deleteCalled, joinCalled)
	}
}

func TestCreateSessionPreservesPremiumActiveModel(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL, "/api/v1/freebuff/session") {
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-pro",
				"model":      "deepseek/deepseek-v4-pro",
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-flash", tp)
	if err == nil || !errors.Is(err, channels.ErrAccountUnavailable) || !strings.Contains(err.Error(), "active premium session") {
		t.Fatalf("expected premium preservation error, got %v", err)
	}
}

func TestCreateSessionReleaseFailureMarksAccountUnavailable(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var releaseCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		switch {
		case req.Method == http.MethodGet && strings.Contains(req.URL, "/api/v1/freebuff/session") && !releaseCalled:
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodDelete && strings.Contains(req.URL, "/api/v1/freebuff/session"):
			releaseCalled = true
			return &channels.OutboundResponse{
				Status:      http.StatusInternalServerError,
				Headers:     http.Header{},
				Body:        []byte(`{"error":"busy"}`),
				BodyPreview: []byte(`{"error":"busy"}`),
			}, nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err == nil || !errors.Is(err, channels.ErrAccountUnavailable) || !strings.Contains(err.Error(), "release unlimited session") {
		t.Fatalf("expected release failure account-unavailable error, got %v", err)
	}
}

func TestCreateSessionRejectsWhenReleasedSessionRemainsActive(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var deleteCalled, recheckCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && !deleteCalled:
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodDelete && path == "/api/v1/freebuff/session":
			deleteCalled = true
			return jsonResponse(200, map[string]any{
				"status":     "ended",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && deleteCalled:
			recheckCalled = true
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err == nil || !errors.Is(err, channels.ErrAccountUnavailable) || !strings.Contains(err.Error(), "remains after release") {
		t.Fatalf("expected remaining active session account-unavailable error, got %v", err)
	}
	if !deleteCalled || !recheckCalled {
		t.Fatalf("delete=%v recheck=%v, want both true", deleteCalled, recheckCalled)
	}
}

func TestCreateSessionRejectsWrongAssignedModelAfterReclaim(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var deleteCalled, recheckCalled, joinCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && !deleteCalled:
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodDelete && path == "/api/v1/freebuff/session":
			deleteCalled = true
			return jsonResponse(200, map[string]any{
				"status":     "ended",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session" && deleteCalled && !recheckCalled:
			recheckCalled = true
			return jsonResponse(200, map[string]any{"status": "none"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			joinCalled = true
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-wrong",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err == nil || !errors.Is(err, channels.ErrAccountUnavailable) || !strings.Contains(err.Error(), "assigned model") {
		t.Fatalf("expected wrong assigned model account-unavailable error, got %v", err)
	}
	if !deleteCalled || !recheckCalled || !joinCalled {
		t.Fatalf("delete=%v recheck=%v join=%v, want all true", deleteCalled, recheckCalled, joinCalled)
	}
}

func TestCreateSessionSkipsPremiumWhenSharedQuotaExhausted(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var sessionChecked bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL, "/api/v1/freebuff/session") {
			sessionChecked = true
			return jsonResponse(200, map[string]any{
				"status":            "none",
				"rateLimitsByModel": premiumRateLimits("deepseek/deepseek-v4-pro", 5, 5, time.Now().Add(time.Hour)),
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err == nil || !errors.Is(err, channels.ErrAccountUnavailable) || !strings.Contains(err.Error(), "premium session quota exhausted") {
		t.Fatalf("expected premium quota account-unavailable error, got %v", err)
	}
	if !sessionChecked {
		t.Fatal("session check was not performed")
	}
}

func TestCreateSessionReusesActivePremiumEvenWhenSharedQuotaExhausted(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL, "/api/v1/freebuff/session") {
			return jsonResponse(200, map[string]any{
				"status":            "active",
				"instanceId":        "inst-pro",
				"model":             "deepseek/deepseek-v4-pro",
				"expiresAt":         time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
				"rateLimitsByModel": premiumRateLimits("deepseek/deepseek-v4-pro", 5, 5, time.Now().Add(time.Hour)),
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err != nil {
		t.Fatalf("create session should reuse active premium despite exhausted start quota: %v", err)
	}
	if state.String(stateInstanceID) != "inst-pro" {
		t.Fatalf("state = %+v, want reused active session", state)
	}
}

func TestCreateSessionAllowsUnlimitedWhenPremiumQuotaExhausted(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var joinCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":            "none",
				"rateLimitsByModel": premiumRateLimits("deepseek/deepseek-v4-pro", 5, 5, time.Now().Add(time.Hour)),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			joinCalled = true
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-flash", tp)
	if err != nil {
		t.Fatalf("create unlimited session with exhausted premium quota: %v", err)
	}
	if state.String(stateInstanceID) != "inst-flash" {
		t.Fatalf("state = %+v, want flash session", state)
	}
	if !joinCalled {
		t.Fatal("join session was not called")
	}
}

func TestCreateSessionRejectsUnsupportedModelBeforeJoin(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		t.Fatal("unsupported model should not call upstream")
		return nil, nil
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|unknown/model", tp)
	if err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("expected unsupported model error, got %v", err)
	}
	if len(tp.requests) != 0 {
		t.Fatalf("expected no upstream requests, got %d", len(tp.requests))
	}
}

func TestAccountProxyMetadataAppliedToSessionAndOutboundProfiles(t *testing.T) {
	const proxyURL = "http://user:pass@127.0.0.1:7890"
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		if got := req.TransportProfile.ProxyURL; got != proxyURL {
			t.Fatalf("proxy URL for %s %s = %q, want %q", req.Method, req.URL, got, proxyURL)
		}
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-proxy",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "base2-free"):
			return jsonResponse(200, map[string]any{"runId": "run-proxy"}), nil
		}
		return nil, errors.New("unexpected request")
	}
	acc := account()
	acc.Metadata = map[string]any{proxypool.MetadataProxyURL: proxyURL}
	state, err := a.CreateSession(context.Background(), acc, "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-proxy", acc.ID, ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if got := out.TransportProfile.ProxyURL; got != proxyURL {
		t.Fatalf("chat proxy URL = %q, want %q", got, proxyURL)
	}
}

func TestCreateSessionRejectsDisabledGLMBeforeJoin(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		t.Fatal("disabled GLM model should not call upstream")
		return nil, nil
	}

	_, err := a.CreateSession(context.Background(), account(), "freebuff|z-ai/glm-5.1", tp)
	if err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("expected disabled GLM error, got %v", err)
	}
	if len(tp.requests) != 0 {
		t.Fatalf("expected no upstream requests, got %d", len(tp.requests))
	}
}

func TestCreateSessionCapturesHARSessionFields(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var joinCalled bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":            "none",
				"rateLimitsByModel": harRateLimits(1.1),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			joinCalled = true
			return jsonResponse(200, map[string]any{
				"status":      "active",
				"accessTier":  "full",
				"instanceId":  "inst-kimi",
				"model":       "moonshotai/kimi-k2.6",
				"admittedAt":  "2026-05-16T06:30:12.385Z",
				"expiresAt":   "2026-05-16T07:30:12.385Z",
				"remainingMs": 3599623,
				"rateLimit": map[string]any{
					"model":         "moonshotai/kimi-k2.6",
					"limit":         5,
					"period":        "pacific_day",
					"resetTimeZone": "America/Los_Angeles",
					"resetAt":       "2026-05-16T07:00:00.000Z",
					"windowHours":   24,
					"recentCount":   2.1,
				},
				"rateLimitsByModel": harRateLimits(2.1),
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|moonshotai/kimi-k2.6", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if !joinCalled {
		t.Fatal("join session was not called")
	}
	if got := state.String(stateAccessTier); got != "full" {
		t.Fatalf("access tier = %q, want full", got)
	}
	if got := state[stateRemainingMs]; got != int64(3599623) {
		t.Fatalf("remaining ms = %#v, want int64", got)
	}
	if _, ok := state[stateRateLimit].(upstreamRateLimit); !ok {
		t.Fatalf("missing selected rate limit in state: %+v", state)
	}
	limits, ok := state[stateRateLimitsByModel].(map[string]upstreamRateLimit)
	if !ok {
		t.Fatalf("missing rate limits by model in state: %+v", state)
	}
	if _, ok := limits["z-ai/glm-5.1"]; !ok {
		t.Fatalf("expected z-ai/glm-5.1 quota snapshot, got %+v", limits)
	}
	expiresAt, ok := a.SessionExpiresAt(state)
	if !ok || expiresAt.Unix() != int64(1778916612) {
		t.Fatalf("expires = %v/%v, want unix 1778916612", expiresAt, ok)
	}
}

func TestCreateSessionCarriesInactiveQuotaSnapshotIntoJoinedSession(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":            "none",
				"accessTier":        "limited",
				"rateLimitsByModel": harRateLimits(4.5),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-pro",
				"model":      "deepseek/deepseek-v4-pro",
				"expiresAt":  "2026-05-16T07:30:12.385Z",
			}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	limits, ok := state[stateRateLimitsByModel].(map[string]upstreamRateLimit)
	if !ok {
		t.Fatalf("missing fallback rate limits by model in state: %+v", state)
	}
	if got := limits["deepseek/deepseek-v4-pro"].RecentCount; got != 4.5 {
		t.Fatalf("pro recent count = %v, want 4.5", got)
	}
	if got := state.String(stateAccessTier); got != "limited" {
		t.Fatalf("access tier = %q, want limited", got)
	}
	raw, ok := state[stateRawSessionJSON].(string)
	if !ok || !strings.Contains(raw, "inst-pro") {
		t.Fatalf("raw session json = %#v, want joined session body", state[stateRawSessionJSON])
	}
}

func TestRefreshAccountStateCapturesNoneStatusWithoutDefaultModel(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if idx != 0 {
			return nil, errors.New("unexpected request")
		}
		if req.Method != http.MethodGet || mustPath(t, req.URL) != "/api/v1/freebuff/session" {
			t.Fatalf("unexpected refresh request: %s %s", req.Method, req.URL)
		}
		return jsonResponse(200, map[string]any{
			"status":            "none",
			"accessTier":        "limited",
			"rateLimitsByModel": harRateLimits(4),
		}), nil
	}

	state, err := a.RefreshAccountState(context.Background(), account(), tp)
	if err != nil {
		t.Fatalf("refresh account state: %v", err)
	}
	if got := state.String(stateStatus); got != "none" {
		t.Fatalf("status = %q, want none", got)
	}
	if got := state.String(stateModel); got != "" {
		t.Fatalf("model = %q, want empty when upstream has no active session", got)
	}
	if _, ok := state[stateRateLimitsByModel].(map[string]upstreamRateLimit); !ok {
		t.Fatalf("missing quota snapshots in refresh state: %+v", state)
	}
}

func TestRestoreSessionValidatesWithInstanceGETAndRebuildsRuntime(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			if got := req.Headers.Get("x-freebuff-instance-id"); got != "restore-instance" {
				t.Fatalf("restore instance header = %q, want restore-instance", got)
			}
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "restore-instance",
				"model":      "deepseek/deepseek-v4-pro",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "deepseek") && strings.Contains(string(req.Body), "START"):
			return jsonResponse(200, map[string]any{"runId": "restore-run"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, valid, err := a.RestoreSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", channels.State{
		stateInstanceID: "restore-instance",
		stateModel:      "deepseek/deepseek-v4-pro",
	}, tp)
	if err != nil {
		t.Fatalf("restore session: %v", err)
	}
	if !valid {
		t.Fatalf("restore session valid = false, want true")
	}
	if got := state.String(stateInstanceID); got != "restore-instance" {
		t.Fatalf("instance id = %q, want restore-instance", got)
	}
	if got := state.String(stateModel); got != "deepseek/deepseek-v4-pro" {
		t.Fatalf("model = %q, want deepseek/deepseek-v4-pro", got)
	}
	if got := state.String(stateSessionRunID); got != "restore-run" {
		t.Fatalf("session run id = %q, want restore-run", got)
	}
	if _, ok := a.runtimeFor("acc-1", "freebuff|deepseek/deepseek-v4-pro"); !ok {
		t.Fatalf("runtime was not rebuilt")
	}
	if len(tp.requests) != 2 {
		t.Fatalf("restore made %d requests, want GET plus parent run START", len(tp.requests))
	}
}

func TestRestoreSessionRejectsMismatchedUpstreamSession(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if idx != 0 {
			return nil, errors.New("unexpected request")
		}
		if req.Method != http.MethodGet || mustPath(t, req.URL) != "/api/v1/freebuff/session" {
			t.Fatalf("unexpected restore request: %s %s", req.Method, req.URL)
		}
		if got := req.Headers.Get("x-freebuff-instance-id"); got != "restore-instance" {
			t.Fatalf("restore instance header = %q, want restore-instance", got)
		}
		return jsonResponse(200, map[string]any{
			"status":     "active",
			"instanceId": "restore-instance",
			"model":      "minimax/minimax-m2.7",
			"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		}), nil
	}

	state, valid, err := a.RestoreSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-pro", channels.State{
		stateInstanceID: "restore-instance",
		stateModel:      "deepseek/deepseek-v4-pro",
	}, tp)
	if err != nil {
		t.Fatalf("restore session: %v", err)
	}
	if valid {
		t.Fatalf("restore session valid = true, want false")
	}
	if state != nil {
		t.Fatalf("restore state = %+v, want nil", state)
	}
	if _, ok := a.runtimeFor("acc-1", "freebuff|deepseek/deepseek-v4-pro"); ok {
		t.Fatalf("runtime was rebuilt for mismatched session")
	}
	for _, req := range tp.requests {
		if req.Method == http.MethodPost {
			t.Fatalf("restore unexpectedly posted to upstream")
		}
	}
}

func TestPrepareOutboundCreatesRunAndInjectsMetadata(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	var stateMu sync.Mutex
	var joinChecked, runStarted, stepRecorded, runFinished bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			joinChecked = true
			if got := req.Headers.Get("User-Agent"); got != "Bun/1.3.11" {
				t.Fatalf("session user-agent = %q, want Bun/1.3.11", got)
			}
			if got := req.Headers.Get("Content-Type"); got != "" {
				t.Fatalf("session content-type = %q, want empty", got)
			}
			if got := req.Headers.Get("Accept-Encoding"); got != "identity" {
				t.Fatalf("session accept-encoding = %q, want identity", got)
			}
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "base2-free") && strings.Contains(string(req.Body), "START"):
			runStarted = true
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode start body: %v", err)
			}
			if body["action"] != "START" || body["agentId"] != agentID {
				t.Fatalf("unexpected start body: %+v", body)
			}
			return jsonResponse(200, map[string]any{"runId": "run-1"}), nil
		case req.Method == http.MethodPost && strings.Contains(path, "/api/v1/agent-runs/") && strings.Contains(path, "/steps") && strings.Contains(string(req.Body), `"stepNumber":2`):
			stateMu.Lock()
			stepRecorded = true
			stateMu.Unlock()
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode step body: %v", err)
			}
			if body["stepNumber"] != float64(2) || body["status"] != "completed" {
				t.Fatalf("unexpected step body: %+v", body)
			}
			return jsonResponse(200, map[string]any{"stepId": "step-1"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "FINISH") && strings.Contains(string(req.Body), "run-1"):
			stateMu.Lock()
			runFinished = true
			stateMu.Unlock()
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode finish body: %v", err)
			}
			if body["action"] != "FINISH" || body["runId"] != "run-1" || body["status"] != "completed" {
				t.Fatalf("unexpected finish body: %+v", body)
			}
			return jsonResponse(200, map[string]any{"ok": true}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if got, want := mustPath(t, out.URL), "/api/v1/chat/completions"; got != want {
		t.Fatalf("out path = %s, want %s", got, want)
	}
	if out.Headers.Get("Authorization") != "Bearer secret-token" {
		t.Fatalf("authorization header not set")
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatalf("decode outbound body: %v", err)
	}
	if body["model"] != "minimax/minimax-m2.7" || body["stream"] != false {
		t.Fatalf("unexpected model/stream: %+v", body)
	}
	meta := body["codebuff_metadata"].(map[string]any)
	if meta["freebuff_instance_id"] != "inst-1" || meta["run_id"] != "run-1" || meta["cost_mode"] != "free" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	provider := body["provider"].(map[string]any)
	if provider["data_collection"] != "deny" {
		t.Fatalf("unexpected provider: %+v", provider)
	}

	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	waitForCondition(t, "async finalization", func() bool {
		stateMu.Lock()
		defer stateMu.Unlock()
		return stepRecorded && runFinished
	})
	stateMu.Lock()
	defer stateMu.Unlock()
	if !joinChecked || !runStarted || !stepRecorded || !runFinished {
		t.Fatalf("join=%v runStart=%v step=%v finish=%v, want all true", joinChecked, runStarted, stepRecorded, runFinished)
	}
}

func TestSessionRunReuseCreatesRunAtSessionStartAndReusesAcrossRequests(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var parentStartCount int
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "base2-free") && strings.Contains(string(req.Body), "START"):
			parentStartCount++
			return jsonResponse(200, map[string]any{"runId": "session-run-1"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if got := state.String(stateSessionRunID); got != "session-run-1" {
		t.Fatalf("session run id in state = %q, want session-run-1", got)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	outA, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"a"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound A: %v", err)
	}
	outB, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"b"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound B: %v", err)
	}
	if got := runIDFromOutboundRequest(outA); got != "session-run-1" {
		t.Fatalf("outbound A run id = %q, want session-run-1", got)
	}
	if got := runIDFromOutboundRequest(outB); got != "session-run-1" {
		t.Fatalf("outbound B run id = %q, want session-run-1", got)
	}
	if parentStartCount != 1 {
		t.Fatalf("parent run starts = %d, want 1", parentStartCount)
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Request: outA, Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize A: %v", err)
	}
	if got := len(a.async.finalize); got != 0 {
		t.Fatalf("per-request finalize queue length = %d, want 0", got)
	}
}

func TestSessionRunReuseFinishesParentOnSessionFinalize(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(2, 2, 2))
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	var parentFinished bool
	var mu sync.Mutex
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "session-run-1"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "FINISH") && strings.Contains(body, "session-run-1"):
			mu.Lock()
			parentFinished = true
			mu.Unlock()
			return jsonResponse(200, map[string]any{"ok": true}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Request: out, Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("request finalize: %v", err)
	}
	mu.Lock()
	finishedBeforeSessionEnd := parentFinished
	mu.Unlock()
	if finishedBeforeSessionEnd {
		t.Fatal("parent run was finished during request finalization")
	}
	a.FinalizeSession(context.Background(), channels.SessionFinalizeEvent{
		ChannelID:      ID,
		AccountID:      "acc-1",
		LocalSessionID: "sess-1",
		SelectionKey:   "freebuff|minimax/minimax-m2.7",
		State:          state,
		Reason:         "expired",
	})
	waitForCondition(t, "session parent finish", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return parentFinished
	})
}

func TestSessionRunRetryReplacesRejectedRunID(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var parentStartCount int
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
				"expiresAt":  time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "base2-free") && strings.Contains(string(req.Body), "START"):
			parentStartCount++
			if parentStartCount == 1 {
				return jsonResponse(200, map[string]any{"runId": "session-run-1"}), nil
			}
			return jsonResponse(200, map[string]any{"runId": "session-run-2"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	retry, ok, err := a.RetryOutbound(context.Background(), lease, nil, channels.RetryOutcome{
		Request:     out,
		Status:      http.StatusBadRequest,
		BodyPreview: []byte(`{"message":"runId Not Running: session-run-1"}`),
	})
	if err != nil {
		t.Fatalf("retry outbound: %v", err)
	}
	if !ok {
		t.Fatal("retry outbound ok = false, want true")
	}
	if got := runIDFromOutboundRequest(retry); got != "session-run-2" {
		t.Fatalf("retry run id = %q, want session-run-2", got)
	}
	if parentStartCount != 2 {
		t.Fatalf("parent run starts = %d, want 2", parentStartCount)
	}
}

func TestPrepareOutboundParallelizesChildSetupAndParentStep(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	tp := &sequenceTransport{t: t}
	childStepBlocked := make(chan struct{})
	parentStepSeen := make(chan struct{})
	releaseChildStep := make(chan struct{})
	var childStepOnce sync.Once
	var parentStepOnce sync.Once

	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "run-parent"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "context-pruner") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "child-run"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/child-run/steps":
			childStepOnce.Do(func() { close(childStepBlocked) })
			<-releaseChildStep
			return jsonResponse(200, map[string]any{"stepId": "child-step"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "FINISH") && strings.Contains(body, "child-run"):
			return jsonResponse(200, map[string]any{"ok": true}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/run-parent/steps":
			if !strings.Contains(body, `"childRunIds":["child-run"]`) {
				return nil, errors.New("parent step missing childRunIds")
			}
			parentStepOnce.Do(func() { close(parentStepSeen) })
			return jsonResponse(200, map[string]any{"stepId": "parent-step"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	done := make(chan error, 1)
	go func() {
		_, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
			ChannelID: ID,
			Method:    http.MethodPost,
			Path:      "/v1/chat/completions",
			Headers:   http.Header{},
			Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
		})
		done <- err
	}()

	select {
	case <-childStepBlocked:
	case <-time.After(time.Second):
		t.Fatal("child step did not start")
	}
	select {
	case <-parentStepSeen:
	case <-time.After(time.Second):
		t.Fatal("parent step waited for child step/finish instead of running in parallel")
	}
	close(releaseChildStep)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("prepare outbound: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare outbound did not finish after releasing child step")
	}
}

func TestPrepareOutboundRecordsDetailedRunSetupTimings(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "run-parent"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	_, err := a.PrepareOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	snapshot := trace.Snapshot()
	if got := snapshot["freebuff_run_setup_mode"]; got != string(freebuffRunSetupModeSyncParallel) {
		t.Fatalf("freebuff_run_setup_mode = %#v, want sync_parallel in %+v", got, snapshot)
	}
	for _, key := range []string{
		"freebuff_run_setup_ms",
		"freebuff_create_parent_run_ms",
		"freebuff_create_child_run_ms",
		"freebuff_child_step_ms",
		"freebuff_child_finish_ms",
		"freebuff_parent_step_ms",
		"freebuff_setup_parallel_wait_ms",
	} {
		if _, ok := snapshot[key]; !ok {
			t.Fatalf("phase %q missing from %+v", key, snapshot)
		}
	}
}

func TestPrepareOutboundSyncModeReturnsRunTreeFailure(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "run-parent"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "context-pruner") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "child-run"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/child-run/steps":
			return jsonResponse(500, map[string]any{"error": "step failed"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/run-parent/steps":
			return jsonResponse(200, map[string]any{"stepId": "parent-step"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	_, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err == nil || !strings.Contains(err.Error(), "record run step failed") {
		t.Fatalf("prepare outbound error = %v, want child step failure", err)
	}
}

func TestPrepareOutboundAsyncRunTreeReturnsBeforeChildSetupAndCompletes(t *testing.T) {
	a := New(
		WithBaseURL("https://codebuff.test"),
		WithRunSetupMode(string(freebuffRunSetupModeParentSyncAsyncTree)),
		WithAsyncSideEffectLimits(2, 2, 2),
	)
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	childCreateStarted := make(chan struct{})
	releaseChildCreate := make(chan struct{})
	childFinishSeen := make(chan struct{})
	parentStepSeen := make(chan struct{})
	var childCreateOnce sync.Once
	var childFinishOnce sync.Once
	var parentStepOnce sync.Once

	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "run-parent"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "context-pruner") && strings.Contains(body, "START"):
			childCreateOnce.Do(func() { close(childCreateStarted) })
			<-releaseChildCreate
			return jsonResponse(200, map[string]any{"runId": "child-run"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/child-run/steps":
			return jsonResponse(200, map[string]any{"stepId": "child-step"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "FINISH") && strings.Contains(body, "child-run"):
			childFinishOnce.Do(func() { close(childFinishSeen) })
			return jsonResponse(200, map[string]any{"ok": true}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs/run-parent/steps":
			if !strings.Contains(body, `"childRunIds":["child-run"]`) {
				return nil, errors.New("parent step missing childRunIds")
			}
			parentStepOnce.Do(func() { close(parentStepSeen) })
			return jsonResponse(200, map[string]any{"stepId": "parent-step"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	started := time.Now()
	out, err := a.PrepareOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("prepare outbound took %s, want parent-run plus enqueue only", elapsed)
	}
	if got := runIDFromOutboundRequest(out); got != "run-parent" {
		t.Fatalf("outbound run id = %q, want run-parent", got)
	}
	snapshot := trace.Snapshot()
	if got := snapshot["freebuff_run_setup_mode"]; got != string(freebuffRunSetupModeParentSyncAsyncTree) {
		t.Fatalf("freebuff_run_setup_mode = %#v, want async mode in %+v", got, snapshot)
	}
	if got, ok := snapshot["freebuff_run_tree_async_enqueued"].(bool); !ok || !got {
		t.Fatalf("freebuff_run_tree_async_enqueued = %#v, want true in %+v", snapshot["freebuff_run_tree_async_enqueued"], snapshot)
	}
	if got, ok := snapshot["freebuff_run_tree_async_dropped"].(bool); !ok || got {
		t.Fatalf("freebuff_run_tree_async_dropped = %#v, want false in %+v", snapshot["freebuff_run_tree_async_dropped"], snapshot)
	}
	for _, key := range []string{"freebuff_create_parent_run_ms", "freebuff_run_tree_async_enqueue_ms"} {
		if _, ok := snapshot[key]; !ok {
			t.Fatalf("phase %q missing from %+v", key, snapshot)
		}
	}
	select {
	case <-childCreateStarted:
	case <-time.After(time.Second):
		t.Fatal("async child run creation did not start")
	}
	close(releaseChildCreate)
	select {
	case <-childFinishSeen:
	case <-time.After(time.Second):
		t.Fatal("async child run was not finished")
	}
	select {
	case <-parentStepSeen:
	case <-time.After(time.Second):
		t.Fatal("async parent step was not recorded")
	}
}

func TestPrepareStreamOutboundAsyncRunTreeEnqueuesWithoutChildSetup(t *testing.T) {
	a := New(
		WithBaseURL("https://codebuff.test"),
		WithRunSetupMode(string(freebuffRunSetupModeParentSyncAsyncTree)),
		WithAsyncSideEffectLimits(1, 1, 1),
	)
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		body := string(req.Body)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "base2-free") && strings.Contains(body, "START"):
			return jsonResponse(200, map[string]any{"runId": "run-parent"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(body, "context-pruner"):
			t.Fatal("child run setup should not run on caller path")
		}
		return nil, errors.New("unexpected request")
	}

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	out, err := a.PrepareStreamOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare stream outbound: %v", err)
	}
	if got := runIDFromOutboundRequest(out); got != "run-parent" {
		t.Fatalf("outbound run id = %q, want run-parent", got)
	}
	if got := len(a.async.runTree); got != 1 {
		t.Fatalf("run tree queue length = %d, want 1", got)
	}
	snapshot := trace.Snapshot()
	if got := snapshot["freebuff_run_setup_mode"]; got != string(freebuffRunSetupModeParentSyncAsyncTree) {
		t.Fatalf("freebuff_run_setup_mode = %#v, want async mode in %+v", got, snapshot)
	}
	if got, ok := snapshot["freebuff_run_tree_async_enqueued"].(bool); !ok || !got {
		t.Fatalf("freebuff_run_tree_async_enqueued = %#v, want true in %+v", snapshot["freebuff_run_tree_async_enqueued"], snapshot)
	}
}

func TestPrepareOutboundUsesModelSpecificAgent(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var runStarted bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-flash",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "deepseek-flash") && strings.Contains(string(req.Body), "START"):
			runStarted = true
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode start body: %v", err)
			}
			if body["agentId"] != "base2-free-deepseek-flash" {
				t.Fatalf("agentId = %v, want flash agent", body["agentId"])
			}
			return jsonResponse(200, map[string]any{"runId": "run-flash"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-flash", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|deepseek/deepseek-v4-flash", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if got := runIDFromOutboundRequest(out); got != "run-flash" {
		t.Fatalf("run id from outbound = %q, want run-flash", got)
	}
	if !runStarted {
		t.Fatal("run start was not called")
	}
}

func TestFinalizeUsesRunIDFromOutcomeRequest(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	var runCount int
	var finishedMu sync.Mutex
	finished := map[string]bool{}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "deepseek/deepseek-v4-flash",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "deepseek-flash") && strings.Contains(string(req.Body), "START"):
			runCount++
			return jsonResponse(200, map[string]any{"runId": []string{"run-a", "run-b"}[runCount-1]}), nil
		case req.Method == http.MethodPost && strings.Contains(path, "/api/v1/agent-runs/") && strings.Contains(path, "/steps") && strings.Contains(string(req.Body), `"status":"completed"`) && strings.Contains(string(req.Body), `"stepNumber":2`):
			return jsonResponse(200, map[string]any{"stepId": "step"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "FINISH") && !strings.Contains(string(req.Body), `"totalSteps":1`):
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode finish body: %v", err)
			}
			runID := body["runId"].(string)
			if runID != "run-a" && runID != "run-b" {
				t.Fatalf("finished runId = %v, want run-a or run-b", body["runId"])
			}
			finishedMu.Lock()
			finished[runID] = true
			finishedMu.Unlock()
			return jsonResponse(200, map[string]any{"ok": true}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|deepseek/deepseek-v4-flash", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|deepseek/deepseek-v4-flash", state, func(channels.Verdict) {})
	outA, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"a"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound A: %v", err)
	}
	outB, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"b"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound B: %v", err)
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Request: outA, Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize A: %v", err)
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Request: outB, Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize B: %v", err)
	}
	waitForCondition(t, "both async finalizers", func() bool {
		finishedMu.Lock()
		defer finishedMu.Unlock()
		return finished["run-a"] && finished["run-b"]
	})
}

func TestPrepareStreamOutboundCreatesRunAndPreservesStream(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	var stateMu sync.Mutex
	var runStarted, stepRecorded, runFinished bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), agentID) && strings.Contains(string(req.Body), "START"):
			runStarted = true
			return jsonResponse(200, map[string]any{"runId": "run-stream"}), nil
		case req.Method == http.MethodPost && strings.Contains(path, "/api/v1/agent-runs/") && strings.Contains(path, "/steps") && strings.Contains(string(req.Body), `"status":"completed"`) && strings.Contains(string(req.Body), `"stepNumber":2`):
			stateMu.Lock()
			stepRecorded = true
			stateMu.Unlock()
			return jsonResponse(200, map[string]any{"stepId": "step-stream"}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "FINISH") && strings.Contains(string(req.Body), "run-stream"):
			stateMu.Lock()
			runFinished = true
			stateMu.Unlock()
			var body map[string]any
			if err := json.Unmarshal(req.Body, &body); err != nil {
				t.Fatalf("decode finish body: %v", err)
			}
			if body["action"] != "FINISH" || body["runId"] != "run-stream" || body["status"] != "completed" {
				t.Fatalf("unexpected finish body: %+v", body)
			}
			return jsonResponse(200, map[string]any{"ok": true}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	out, err := a.PrepareStreamOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare stream outbound: %v", err)
	}
	if got, want := mustPath(t, out.URL), "/api/v1/chat/completions"; got != want {
		t.Fatalf("out path = %s, want %s", got, want)
	}
	if out.Headers.Get("Accept") != "text/event-stream" || out.Headers.Get("Accept-Encoding") != "identity" {
		t.Fatalf("stream headers not set: %+v", out.Headers)
	}
	var body map[string]any
	if err := json.Unmarshal(out.Body, &body); err != nil {
		t.Fatalf("decode outbound body: %v", err)
	}
	if body["model"] != "minimax/minimax-m2.7" || body["stream"] != true {
		t.Fatalf("unexpected model/stream: %+v", body)
	}
	meta := body["codebuff_metadata"].(map[string]any)
	if meta["freebuff_instance_id"] != "inst-1" || meta["run_id"] != "run-stream" || meta["cost_mode"] != "free" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	for _, key := range []string{"freebuff_ads_ms", "freebuff_run_setup_ms"} {
		if _, ok := trace.Snapshot()[key]; !ok {
			t.Fatalf("phase %q missing from %+v", key, trace.Snapshot())
		}
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	waitForCondition(t, "async stream finalization", func() bool {
		stateMu.Lock()
		defer stateMu.Unlock()
		return stepRecorded && runFinished
	})
	stateMu.Lock()
	defer stateMu.Unlock()
	if !runStarted || !stepRecorded || !runFinished {
		t.Fatalf("runStart=%v step=%v finish=%v, want all true", runStarted, stepRecorded, runFinished)
	}
}

func TestPrepareOutboundDropsADSWhenQueueFull(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(1, 1))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), agentID) && strings.Contains(string(req.Body), "START"):
			return jsonResponse(200, map[string]any{"runId": "run-queue-full"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	_, err = a.PrepareOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	snapshot := trace.Snapshot()
	if got, ok := snapshot["freebuff_ads_async"].(bool); !ok || !got {
		t.Fatalf("freebuff_ads_async = %#v, want true in %+v", snapshot["freebuff_ads_async"], snapshot)
	}
	if got, ok := snapshot["freebuff_ads_enqueued"].(bool); !ok || got {
		t.Fatalf("freebuff_ads_enqueued = %#v, want false when queue is full in %+v", snapshot["freebuff_ads_enqueued"], snapshot)
	}
}

func TestPrepareStreamOutboundDoesNotWaitForSlowADS(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(4, 1))
	runAsyncSideEffects(t, a)
	tp := &sequenceTransport{t: t}
	adsStarted := make(chan struct{})
	releaseADS := make(chan struct{})
	var adsOnce sync.Once
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseADS) })
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodPost && path == "/api/v1/ads":
			adsOnce.Do(func() { close(adsStarted) })
			<-releaseADS
			return jsonResponse(200, map[string]any{"impUrl": "https://ads.test/imp"}), nil
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), agentID) && strings.Contains(string(req.Body), "START"):
			return jsonResponse(200, map[string]any{"runId": "run-stream"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	select {
	case <-adsStarted:
	case <-time.After(time.Second):
		t.Fatal("async ads did not start")
	}

	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	started := time.Now()
	_, err = a.PrepareStreamOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
	})
	elapsed := time.Since(started)
	releaseOnce.Do(func() { close(releaseADS) })
	if err != nil {
		t.Fatalf("prepare stream outbound: %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("prepare stream took %s while ads was blocked, want bounded enqueue cost", elapsed)
	}
}

func TestFinalizeEnqueuesWithoutRemoteCallOnCallerPath(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(1, 1), WithRunSetupMode(string(freebuffRunSetupModeSyncParallel)))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), agentID) && strings.Contains(string(req.Body), "START"):
			return jsonResponse(200, map[string]any{"runId": "run-finalize"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"hi"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}

	started := time.Now()
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{Request: out, Status: 200, Class: channels.ClassOk}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("finalize took %s, want enqueue-only caller path", elapsed)
	}
	if got := len(a.async.finalize); got != 1 {
		t.Fatalf("finalize queue length = %d, want 1", got)
	}
}

func TestAsyncADSErrorIsSanitizedInProcessLog(t *testing.T) {
	buf := captureProcessLog(t)
	a := New(WithAsyncSideEffectLimits(2, 1))
	runAsyncSideEffects(t, a)
	queued := a.sendAdsWithMessage(
		context.Background(),
		failingTransport{err: errors.New("upstream ads unavailable")},
		"https://codebuff.test",
		"secret-token",
		"ads-session",
		generateDeviceProfile(),
		"secret prompt that must not be logged",
	)
	if !queued {
		t.Fatal("ads job was not queued")
	}
	waitForCondition(t, "async ads failure log", func() bool {
		return strings.Contains(buf.String(), "freebuff: async ads failed")
	})
	logs := buf.String()
	if strings.Contains(logs, "secret prompt") || strings.Contains(logs, "secret-token") {
		t.Fatalf("process log leaked sensitive data: %s", logs)
	}
}

func TestClassifyResponse(t *testing.T) {
	a := New()
	cases := []struct {
		name   string
		status int
		body   string
		want   channels.ResponseClass
	}{
		{name: "ok", status: 200, want: channels.ClassOk},
		{name: "auth", status: 401, want: channels.ClassAuthExpired},
		{name: "rate", status: 429, want: channels.ClassRateLimited},
		{name: "free mode short window limit", status: 400, body: `{"error":"free_mode_rate_limited"}`, want: channels.ClassRateLimited},
		{name: "outside hours", status: 400, body: `{"code":"DEPLOYMENT_OUTSIDE_HOURS"}`, want: channels.ClassRetryable},
		{name: "server", status: 502, want: channels.ClassRetryable},
		{name: "bad request", status: 400, want: channels.ClassFatal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.ClassifyResponse(tc.status, http.Header{}, []byte(tc.body)); got != tc.want {
				t.Fatalf("class = %s, want %s", got.String(), tc.want.String())
			}
		})
	}
}

func harRateLimits(recent float64) map[string]any {
	return map[string]any{
		"deepseek/deepseek-v4-pro": map[string]any{
			"model":         "deepseek/deepseek-v4-pro",
			"limit":         5,
			"period":        "pacific_day",
			"resetTimeZone": "America/Los_Angeles",
			"resetAt":       "2026-05-16T07:00:00.000Z",
			"windowHours":   24,
			"recentCount":   recent,
		},
		"moonshotai/kimi-k2.6": map[string]any{
			"model":         "moonshotai/kimi-k2.6",
			"limit":         5,
			"period":        "pacific_day",
			"resetTimeZone": "America/Los_Angeles",
			"resetAt":       "2026-05-16T07:00:00.000Z",
			"windowHours":   24,
			"recentCount":   recent,
		},
		"z-ai/glm-5.1": map[string]any{
			"model":         "z-ai/glm-5.1",
			"limit":         5,
			"period":        "pacific_day",
			"resetTimeZone": "America/Los_Angeles",
			"resetAt":       "2026-05-16T07:00:00.000Z",
			"windowHours":   24,
			"recentCount":   recent,
		},
	}
}

func premiumRateLimits(model string, limit int, recent float64, resetAt time.Time) map[string]any {
	return map[string]any{
		model: map[string]any{
			"model":         model,
			"limit":         limit,
			"period":        "pacific_day",
			"resetTimeZone": "America/Los_Angeles",
			"resetAt":       resetAt.UTC().Format(time.RFC3339Nano),
			"windowHours":   24,
			"recentCount":   recent,
		},
	}
}

func TestTokenUsage(t *testing.T) {
	a := New()
	in, out, ok := a.TokenUsage(nil, &channels.OutboundResponse{
		Body: []byte(`{"usage":{"prompt_tokens":7,"completion_tokens":11}}`),
	})
	if !ok || in != 7 || out != 11 {
		t.Fatalf("usage = (%d,%d,%v), want (7,11,true)", in, out, ok)
	}
}

func TestAccountAuthMethodsExposeCredentialAndGitHub(t *testing.T) {
	a := New()
	methods := a.AccountAuthMethods()
	if len(methods) != 3 {
		t.Fatalf("method count = %d, want 3", len(methods))
	}
	if methods[0].ID != "credential" || !methods[0].RequiresCredential || methods[0].CredentialInputMode != channels.AccountCredentialInputModeImport {
		t.Fatalf("unexpected credential method: %+v", methods[0])
	}
	if methods[1].ID != "github" || methods[1].CompletionMode != channels.AccountLoginCompletionPoll {
		t.Fatalf("unexpected github method: %+v", methods[1])
	}
	if methods[2].ID != "github_protocol" || !methods[2].RequiresCredential || methods[2].CredentialInputMode != channels.AccountCredentialInputModeGitHubProtocol {
		t.Fatalf("unexpected github protocol method: %+v", methods[2])
	}
}

func TestImportAccountCredentialJSON(t *testing.T) {
	a := New()
	result, err := a.ImportAccountCredential(`{
		"authToken": "secret-token",
		"user": {
			"id": "user-1",
			"name": "Ada",
			"email": "ada@example.test"
		}
	}`)
	if err != nil {
		t.Fatalf("import credential: %v", err)
	}
	if result.Credential != "secret-token" {
		t.Fatalf("credential = %q, want token", result.Credential)
	}
	if result.Name != "Ada" {
		t.Fatalf("name = %q, want Ada", result.Name)
	}
	if result.Metadata["auth_method"] != "credential" || result.Metadata["freebuff_user_email"] != "ada@example.test" {
		t.Fatalf("metadata = %+v, want non-secret identity", result.Metadata)
	}
	if _, leaked := result.Metadata["authToken"]; leaked {
		t.Fatalf("metadata leaked token: %+v", result.Metadata)
	}
}

func TestImportAccountCredentialRawToken(t *testing.T) {
	a := New()
	result, err := a.ImportAccountCredential(" raw-token ")
	if err != nil {
		t.Fatalf("import credential: %v", err)
	}
	if result.Credential != "raw-token" {
		t.Fatalf("credential = %q, want trimmed token", result.Credential)
	}
	if result.Name != "freebuff-credential" {
		t.Fatalf("name = %q, want fallback", result.Name)
	}
}

func TestImportAccountCredentialRejectsJSONWithoutToken(t *testing.T) {
	a := New()
	if _, err := a.ImportAccountCredential(`{"user":{"name":"Ada"}}`); err == nil || !strings.Contains(err.Error(), "missing token") {
		t.Fatalf("expected missing token error, got %v", err)
	}
}

func TestStartGitHubAccountLoginRequestsCLIAuthCode(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if idx != 0 {
			t.Fatalf("unexpected request index %d", idx)
		}
		if req.Method != http.MethodPost || mustPath(t, req.URL) != "/api/auth/cli/code" {
			t.Fatalf("unexpected login request: %s %s", req.Method, req.URL)
		}
		if req.Headers.Get("User-Agent") != freebuffLoginUserAgent {
			t.Fatalf("user-agent = %q", req.Headers.Get("User-Agent"))
		}
		var body map[string]string
		if err := json.Unmarshal(req.Body, &body); err != nil {
			t.Fatalf("decode login body: %v", err)
		}
		if !strings.HasPrefix(body["fingerprintId"], "enhanced-") {
			t.Fatalf("fingerprintId = %q", body["fingerprintId"])
		}
		return jsonResponse(200, map[string]any{
			"fingerprintId":   body["fingerprintId"],
			"fingerprintHash": "hash-1",
			"loginUrl":        "https://freebuff.test/login?auth_code=abc",
			"expiresAt":       int64(1778900310097),
		}), nil
	}

	start, err := a.StartAccountLogin(context.Background(), "github", tp)
	if err != nil {
		t.Fatalf("start login: %v", err)
	}
	if start.SessionID == "" || start.LoginURL == "" {
		t.Fatalf("start response missing fields: %+v", start)
	}
	if got, want := start.ExpiresAt, int64(1778900310); got != want {
		t.Fatalf("expires_at = %d, want %d", got, want)
	}
	if start.CompletionMode != channels.AccountLoginCompletionPoll || start.PollAfterSeconds != freebuffLoginPollSeconds {
		t.Fatalf("unexpected completion metadata: %+v", start)
	}
}

func TestPollGitHubAccountLoginPendingOnAuthFailure(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   "enhanced-abc",
		FingerprintHash: "hash-1",
		ExpiresAtRaw:    time.Now().Add(time.Minute).UnixMilli(),
		ExpiresAtUnix:   time.Now().Add(time.Minute).Unix(),
	})
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if idx != 0 {
			t.Fatalf("unexpected request index %d", idx)
		}
		if req.Method != http.MethodGet || mustPath(t, req.URL) != "/api/auth/cli/status" {
			t.Fatalf("unexpected poll request: %s %s", req.Method, req.URL)
		}
		if req.Headers.Get("User-Agent") != freebuffLoginUserAgent {
			t.Fatalf("user-agent = %q", req.Headers.Get("User-Agent"))
		}
		return jsonResponse(401, map[string]any{"error": "Authentication failed"}), nil
	}

	result, err := a.PollAccountLogin(context.Background(), "enhanced-abc", tp)
	if err != nil {
		t.Fatalf("poll login: %v", err)
	}
	if result.Completed {
		t.Fatalf("expected pending result, got %+v", result)
	}
}

func TestPollGitHubAccountLoginCompletesWithNestedToken(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   "enhanced-abc",
		FingerprintHash: "hash-1",
		ExpiresAtRaw:    time.Now().Add(time.Minute).UnixMilli(),
		ExpiresAtUnix:   time.Now().Add(time.Minute).Unix(),
	})
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if idx != 0 {
			t.Fatalf("unexpected request index %d", idx)
		}
		return jsonResponse(200, map[string]any{
			"user": map[string]any{
				"id":        "user-1",
				"name":      "Ada",
				"email":     "ada@example.test",
				"authToken": "token-nested",
			},
		}), nil
	}

	result, err := a.PollAccountLogin(context.Background(), "enhanced-abc", tp)
	if err != nil {
		t.Fatalf("poll login: %v", err)
	}
	if !result.Completed || result.Credential != "token-nested" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.UserName != "Ada" || result.UserEmail != "ada@example.test" || result.UserID != "user-1" {
		t.Fatalf("unexpected user metadata: %+v", result)
	}
	if result.Metadata["auth_method"] != "github" || result.Metadata["freebuff_fingerprint_id"] != "enhanced-abc" {
		t.Fatalf("unexpected metadata: %+v", result.Metadata)
	}
	if _, ok := a.loginSession("enhanced-abc"); ok {
		t.Fatalf("login session was not cleaned up")
	}
}

func TestPollGitHubAccountLoginCompletesWithTopLevelToken(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   "enhanced-abc",
		FingerprintHash: "hash-1",
		ExpiresAtRaw:    time.Now().Add(time.Minute).UnixMilli(),
		ExpiresAtUnix:   time.Now().Add(time.Minute).Unix(),
	})
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		return jsonResponse(200, map[string]any{"authToken": "token-top"}), nil
	}

	result, err := a.PollAccountLogin(context.Background(), "enhanced-abc", tp)
	if err != nil {
		t.Fatalf("poll login: %v", err)
	}
	if !result.Completed || result.Credential != "token-top" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestPollGitHubAccountLoginErrorsOnServerStatus(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   "enhanced-abc",
		FingerprintHash: "hash-1",
		ExpiresAtRaw:    time.Now().Add(time.Minute).UnixMilli(),
		ExpiresAtUnix:   time.Now().Add(time.Minute).Unix(),
	})
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		return jsonResponse(500, map[string]any{"error": "upstream down"}), nil
	}

	if _, err := a.PollAccountLogin(context.Background(), "enhanced-abc", tp); err == nil {
		t.Fatalf("expected server status error")
	}
}

func TestPollGitHubAccountLoginExpiresSession(t *testing.T) {
	a := New(WithAuthBaseURL("https://freebuff.test"))
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   "enhanced-abc",
		FingerprintHash: "hash-1",
		ExpiresAtRaw:    time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtUnix:   time.Now().Add(-time.Minute).Unix(),
	})
	tp := &sequenceTransport{t: t}
	tp.respond = func(_ *channels.OutboundRequest, _ int) (*channels.OutboundResponse, error) {
		t.Fatalf("expired session should not be polled")
		return nil, nil
	}

	if _, err := a.PollAccountLogin(context.Background(), "enhanced-abc", tp); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired error, got %v", err)
	}
	if _, ok := a.loginSession("enhanced-abc"); ok {
		t.Fatalf("expired login session was not cleaned up")
	}
}

func account() channels.Account {
	return channels.Account{
		ID:         "acc-1",
		ChannelID:  ID,
		Name:       "freebuff",
		Credential: "secret-token",
	}
}

func jsonResponse(status int, v any) *channels.OutboundResponse {
	body, _ := json.Marshal(v)
	return &channels.OutboundResponse{
		Status:      status,
		Headers:     http.Header{},
		Body:        body,
		BodyPreview: body,
	}
}

func mustPath(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return u.Path
}
