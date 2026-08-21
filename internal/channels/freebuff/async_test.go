package freebuff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/phasetiming"
)

type asyncTestTransport struct {
	t       *testing.T
	mu      sync.Mutex
	records []recordedRequest
	respond func(context.Context, *channels.OutboundRequest, int) (*channels.OutboundResponse, error)
}

func (tp *asyncTestTransport) Do(ctx context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	tp.t.Helper()
	if req == nil {
		tp.t.Fatal("nil request")
	}
	tp.mu.Lock()
	idx := len(tp.records)
	tp.records = append(tp.records, recordedRequest{
		Method:  req.Method,
		URL:     req.URL,
		Headers: req.Headers.Clone(),
		Body:    append([]byte(nil), req.Body...),
	})
	tp.mu.Unlock()
	if tp.respond != nil {
		resp, err := tp.respond(ctx, req, idx)
		if resp != nil || err != nil {
			return resp, err
		}
	}
	return asyncDefaultResponse(tp.t, req), nil
}

func asyncDefaultResponse(t *testing.T, req *channels.OutboundRequest) *channels.OutboundResponse {
	t.Helper()
	path := mustPath(t, req.URL)
	switch {
	case path == "/api/v1/ads":
		return jsonResponse(http.StatusOK, map[string]any{"impUrl": "https://ads.test/imp"})
	case path == "/api/v1/ads/impression":
		return jsonResponse(http.StatusOK, map[string]any{})
	case path == "/api/healthz":
		return jsonResponse(http.StatusOK, map[string]any{"status": "ok"})
	case path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "START"):
		return jsonResponse(http.StatusOK, map[string]any{"runId": "run-async"})
	case strings.Contains(path, "/api/v1/agent-runs/") && strings.Contains(path, "/steps"):
		return jsonResponse(http.StatusOK, map[string]any{"stepId": "step-async"})
	case path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "FINISH"):
		return jsonResponse(http.StatusOK, map[string]any{"ok": true})
	default:
		t.Fatalf("unexpected async request %s %s body=%s", req.Method, path, string(req.Body))
		return nil
	}
}

func TestAsyncADSDoesNotBlockPrepareOutbound(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(2, 2))
	releaseADS := make(chan struct{})
	adsDone := make(chan struct{})
	var adsCalls int
	var adsDoneOnce sync.Once
	tp := &asyncTestTransport{t: t}
	tp.respond = func(ctx context.Context, req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if mustPath(t, req.URL) == "/api/v1/ads" {
			select {
			case <-releaseADS:
				tp.mu.Lock()
				adsCalls++
				if adsCalls >= 2 {
					adsDoneOnce.Do(func() { close(adsDone) })
				}
				tp.mu.Unlock()
				return jsonResponse(http.StatusOK, map[string]any{"impUrl": "https://ads.test/imp"}), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	go a.Run(runCtx)

	model := "minimax/minimax-m2.7"
	lease := testRuntimeLease(a, tp, model)
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	started := time.Now()
	out, err := a.PrepareOutbound(ctx, lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      []byte(`{"model":"minimax-m2.7","messages":[{"role":"user","content":"slow ads should not block"}]}`),
	})
	if err != nil {
		t.Fatalf("prepare outbound: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("prepare outbound took %s, want async ADS enqueue only", elapsed)
	}
	if got := mustPath(t, out.URL); got != "/api/v1/chat/completions" {
		t.Fatalf("out path = %s", got)
	}
	snapshot := trace.Snapshot()
	for _, key := range []string{"freebuff_ads_ms", "freebuff_ads_enqueue_ms", "freebuff_run_setup_ms"} {
		if _, ok := snapshot[key]; !ok {
			t.Fatalf("phase %q missing from %+v", key, snapshot)
		}
	}
	if snapshot["freebuff_ads_async"] != true || snapshot["freebuff_ads_enqueued"] != true {
		t.Fatalf("async ads flags missing from %+v", snapshot)
	}
	close(releaseADS)
	select {
	case <-adsDone:
	case <-time.After(time.Second):
		t.Fatal("async ADS job did not drain")
	}
	cancelRun()
}

func TestAsyncADSQueueFullDropsWithoutBlockingAndLogs(t *testing.T) {
	a := New(WithAsyncSideEffectLimits(1, 1))
	tp := &asyncTestTransport{t: t}
	restore, logs := captureAsyncLogs()
	defer restore()

	first := a.sendAdsWithMessage(context.Background(), tp, "https://codebuff.test", "secret-token", "ads-1", deviceProfile{}, "secret prompt")
	secondStart := time.Now()
	second := a.sendAdsWithMessage(context.Background(), tp, "https://codebuff.test", "secret-token", "ads-1", deviceProfile{}, "secret prompt")
	if !first {
		t.Fatal("first ADS enqueue failed")
	}
	if second {
		t.Fatal("second ADS enqueue succeeded, want queue-full drop")
	}
	if elapsed := time.Since(secondStart); elapsed > 100*time.Millisecond {
		t.Fatalf("queue-full ADS enqueue took %s", elapsed)
	}
	logText := logs.String()
	if !strings.Contains(logText, "freebuff: async ads dropped stage=with_message reason=queue_full") {
		t.Fatalf("missing queue full log: %s", logText)
	}
	if strings.Contains(logText, "secret-token") || strings.Contains(logText, "secret prompt") {
		t.Fatalf("queue full log leaked sensitive data: %s", logText)
	}
}

func TestAsyncADSErrorLoggingIsSanitized(t *testing.T) {
	a := New(WithAsyncSideEffectLimits(2, 1))
	tp := &asyncTestTransport{t: t}
	tp.respond = func(ctx context.Context, req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if mustPath(t, req.URL) == "/api/v1/ads" {
			return nil, errors.New("upstream failed with Bearer secret-token")
		}
		return nil, nil
	}
	restore, logs := captureAsyncLogs()
	defer restore()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go a.Run(runCtx)

	if !a.sendAdsWithMessage(context.Background(), tp, "https://codebuff.test", "secret-token", "ads-1", deviceProfile{}, "hello") {
		t.Fatal("ADS enqueue failed")
	}
	waitForAsyncLog(t, logs, "freebuff: async ads failed stage=with_message")
	logText := logs.String()
	if strings.Contains(logText, "secret-token") || strings.Contains(logText, "Bearer secret-token") {
		t.Fatalf("ADS error log leaked credential: %s", logText)
	}
	if !strings.Contains(logText, "Bearer <redacted>") {
		t.Fatalf("ADS error log did not show redacted credential marker: %s", logText)
	}
}

func TestAsyncFinalizerReturnsBeforeSlowRemoteWork(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"), WithAsyncSideEffectLimits(1, 2))
	releaseFinalize := make(chan struct{})
	finalizeDone := make(chan struct{})
	var finalizeDoneOnce sync.Once
	tp := &asyncTestTransport{t: t}
	tp.respond = func(ctx context.Context, req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if mustPath(t, req.URL) == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "FINISH") {
			select {
			case <-releaseFinalize:
				finalizeDoneOnce.Do(func() { close(finalizeDone) })
				return jsonResponse(http.StatusOK, map[string]any{"ok": true}), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	go a.Run(runCtx)

	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	started := time.Now()
	if err := a.Finalize(ctx, lease, channels.FinalizeOutcome{
		Request:  outboundWithRunID(t, "run-slow"),
		Response: jsonResponse(http.StatusOK, map[string]any{"id": "msg-1"}),
		Status:   http.StatusOK,
		Class:    channels.ClassOk,
	}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("finalize took %s, want enqueue-only latency", elapsed)
	}
	if _, ok := trace.Snapshot()["freebuff_finalize_enqueue_ms"]; !ok {
		t.Fatalf("missing finalize enqueue phase: %+v", trace.Snapshot())
	}
	close(releaseFinalize)
	select {
	case <-finalizeDone:
	case <-time.After(time.Second):
		t.Fatal("async finalizer job did not drain")
	}
	cancelRun()
}

func TestAsyncFinalizerQueueFullRunsInlineFallbackWithoutRunIDLog(t *testing.T) {
	a := New(WithAsyncSideEffectLimits(1, 1))
	tp := &asyncTestTransport{t: t}
	lease := testRuntimeLease(a, tp, "minimax/minimax-m2.7")
	restore, logs := captureAsyncLogs()
	defer restore()

	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{
		Request: outboundWithRunID(t, "run-buffered"),
		Status:  http.StatusOK,
		Class:   channels.ClassOk,
	}); err != nil {
		t.Fatalf("first finalize: %v", err)
	}
	outbound := outboundWithRunID(t, "run-secret")
	if got := runIDFromOutboundRequest(outbound); got != "run-secret" {
		t.Fatalf("run id helper = %q", got)
	}
	if err := a.Finalize(context.Background(), lease, channels.FinalizeOutcome{
		Request: outbound,
		Status:  http.StatusOK,
		Class:   channels.ClassOk,
	}); err != nil {
		t.Fatalf("inline fallback finalize: %v", err)
	}
	logText := logs.String()
	if !strings.Contains(logText, "freebuff: async finalizer queue full status=completed action=inline_fallback") {
		t.Fatalf("missing finalizer queue full log: %s", logText)
	}
	if strings.Contains(logText, "run-secret") || strings.Contains(logText, "run-buffered") {
		t.Fatalf("finalizer queue full log leaked run id: %s", logText)
	}
}

func TestAsyncFinalizerErrorLoggingIsSanitized(t *testing.T) {
	logText := sanitizeAsyncError(errors.New("upstream /api/v1/agent-runs/run-secret/steps failed with Bearer secret-token"))
	if strings.Contains(logText, "run-secret") || strings.Contains(logText, "secret-token") {
		t.Fatalf("finalizer error log leaked sensitive data: %s", logText)
	}
	if !strings.Contains(logText, "/api/v1/agent-runs/<redacted>/steps") || !strings.Contains(logText, "Bearer <redacted>") {
		t.Fatalf("finalizer error log missing redaction markers: %s", logText)
	}
}

func TestAsyncFinalizerWorkerLogsSanitizedError(t *testing.T) {
	a := New(WithAsyncSideEffectLimits(1, 1))
	tp := &asyncTestTransport{t: t}
	tp.respond = func(ctx context.Context, req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if strings.Contains(req.URL, "/steps") {
			return nil, errors.New("upstream /api/v1/agent-runs/run-secret/steps failed with Bearer secret-token")
		}
		return nil, nil
	}
	restore, logs := captureAsyncLogs()
	defer restore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.async.runFinalize(ctx, a)
	}()

	if !a.async.enqueueFinalize(finalizeJob{
		transport:  tp,
		baseURL:    "https://codebuff.test",
		credential: "secret-token",
		runID:      "run-secret",
		status:     "completed",
		steps:      3,
		recordStep: true,
		startedAt:  time.Now(),
	}) {
		t.Fatal("finalizer enqueue failed")
	}
	waitForAsyncLog(t, logs, "freebuff: async finalizer failed status=completed")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("finalizer worker did not stop")
	}
	logText := logs.String()
	if strings.Contains(logText, "run-secret") || strings.Contains(logText, "secret-token") {
		t.Fatalf("finalizer worker log leaked sensitive data: %s", logText)
	}
	if !strings.Contains(logText, "/api/v1/agent-runs/<redacted>/steps") || !strings.Contains(logText, "Bearer <redacted>") {
		t.Fatalf("finalizer worker log missing redaction markers: %s", logText)
	}
}

func TestAsyncRunTreeWorkerLogsSanitizedError(t *testing.T) {
	a := New(WithAsyncSideEffectLimits(1, 1, 1))
	tp := &asyncTestTransport{t: t}
	tp.respond = func(ctx context.Context, req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		if mustPath(t, req.URL) == "/api/v1/agent-runs" && strings.Contains(string(req.Body), "context-pruner") {
			return nil, errors.New("upstream /api/v1/agent-runs/run-secret failed with Bearer secret-token")
		}
		return nil, nil
	}
	restore, logs := captureAsyncLogs()
	defer restore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.async.runRunTree(ctx, a)
	}()

	if !a.async.enqueueRunTree(runTreeJob{
		transport:   tp,
		baseURL:     "https://codebuff.test",
		credential:  "secret-token",
		parentRunID: "run-secret",
	}) {
		t.Fatal("run tree enqueue failed")
	}
	waitForAsyncLog(t, logs, "freebuff: async run tree failed")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run tree worker did not stop")
	}
	logText := logs.String()
	if strings.Contains(logText, "run-secret") || strings.Contains(logText, "secret-token") {
		t.Fatalf("run tree worker log leaked sensitive data: %s", logText)
	}
	if !strings.Contains(logText, "/api/v1/agent-runs/<redacted>") || !strings.Contains(logText, "Bearer <redacted>") {
		t.Fatalf("run tree worker log missing redaction markers: %s", logText)
	}
}

func testRuntimeLease(a *Adapter, tp channels.Transport, model string) *channels.Lease {
	key := keyPrefix + model
	a.setRuntime("acc-1", key, runtime{
		credential:   "secret-token",
		baseURL:      "https://codebuff.test",
		transport:    tp,
		adsSessionID: "ads-1",
		device: deviceProfile{
			OS:        "linux",
			Timezone:  "UTC",
			Locale:    "en-US",
			BrowserUA: "test",
		},
	})
	return channels.NewLease("sess-1", "acc-1", ID, key, channels.State{
		stateInstanceID: "inst-1",
		stateModel:      model,
	}, func(channels.Verdict) {})
}

func outboundWithRunID(t *testing.T, runID string) *channels.OutboundRequest {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"codebuff_metadata": map[string]any{"run_id": runID},
	})
	if err != nil {
		t.Fatalf("marshal outbound: %v", err)
	}
	return &channels.OutboundRequest{Body: body}
}

type asyncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *asyncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *asyncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureAsyncLogs() (func(), *asyncLogBuffer) {
	buf := &asyncLogBuffer{}
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	return func() {
		log.SetOutput(oldWriter)
		if oldWriter == nil {
			log.SetOutput(os.Stderr)
		}
		log.SetFlags(oldFlags)
	}, buf
}

func waitForAsyncLog(t *testing.T, logs *asyncLogBuffer, needle string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), needle) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for log %q; logs=%s", needle, logs.String())
}
