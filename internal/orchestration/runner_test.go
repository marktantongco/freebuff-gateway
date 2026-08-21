package orchestration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"freebuff-reverse/internal/accounts"
	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/logs"
	"freebuff-reverse/internal/metrics"
	"freebuff-reverse/internal/session"
	"freebuff-reverse/internal/storage"
	"freebuff-reverse/internal/usage"
)

type fakeAdapter struct {
	id string
}

func (a fakeAdapter) ID() string                { return a.id }
func (a fakeAdapter) InboundPathPrefix() string { return "/channels/" + a.id }
func (a fakeAdapter) SessionPolicy() channels.SessionPolicy {
	return channels.NoopSessionPolicy{TTL: time.Hour, MaxPerAccount: 1, MaxConcurrency: 10}
}
func (a fakeAdapter) AuthFlow() channels.AuthFlow { return nil }
func (a fakeAdapter) PrepareOutbound(_ context.Context, _ *channels.Lease, in *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return &channels.OutboundRequest{Method: in.Method, URL: "https://example.test" + in.Path, Headers: http.Header{}, Body: in.Body}, nil
}
func (a fakeAdapter) ClassifyResponse(status int, _ http.Header, _ []byte) channels.ResponseClass {
	if status >= 200 && status < 300 {
		return channels.ClassOk
	}
	return channels.ClassFatal
}

type fakeTokenAdapter struct {
	fakeAdapter
}

func (a fakeTokenAdapter) TokenUsage(_ *channels.OutboundRequest, _ *channels.OutboundResponse) (int, int, bool) {
	return 4, 6, true
}

type fakeFinalizerAdapter struct {
	fakeAdapter
	calls int
	errs  []error
}

func (a *fakeFinalizerAdapter) Finalize(_ context.Context, _ *channels.Lease, outcome channels.FinalizeOutcome) error {
	a.calls++
	a.errs = append(a.errs, outcome.Err)
	return nil
}

type fakeStreamFinalizerAdapter struct {
	fakeFinalizerAdapter
}

func (a *fakeStreamFinalizerAdapter) PrepareStreamOutbound(_ context.Context, _ *channels.Lease, in *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return &channels.OutboundRequest{Method: in.Method, URL: "https://example.test" + in.Path, Headers: http.Header{}, Body: in.Body}, nil
}

func (a *fakeStreamFinalizerAdapter) ClassifyStreamResponse(status int, headers http.Header, bodyPreview []byte) channels.ResponseClass {
	return a.ClassifyResponse(status, headers, bodyPreview)
}

type fakeStreamRewriterAdapter struct {
	fakeStreamFinalizerAdapter
	rewrites int
}

func (a *fakeStreamRewriterAdapter) RewriteStream(_ context.Context, _ *channels.Lease, _ *channels.InboundRequest, _ io.Reader, downstream channels.StreamWriter) error {
	a.rewrites++
	if _, err := downstream.Write([]byte("rewritten")); err != nil {
		return err
	}
	downstream.Flush()
	return nil
}

type fakeStreamUsageRewriterAdapter struct {
	fakeStreamFinalizerAdapter
}

func (a *fakeStreamUsageRewriterAdapter) RewriteStreamWithUsage(_ context.Context, _ *channels.Lease, _ *channels.InboundRequest, _ io.Reader, downstream channels.StreamWriter) (channels.TokenUsage, error) {
	if _, err := downstream.Write([]byte("rewritten")); err != nil {
		return channels.TokenUsage{}, err
	}
	downstream.Flush()
	return channels.TokenUsage{InputTokens: 5, OutputTokens: 8, Known: true}, nil
}

type fakeTransport struct {
	firstResponseMS int64
}

func (t fakeTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return &channels.OutboundResponse{Status: 200, Headers: http.Header{}, Body: []byte(`ok`), BodyPreview: []byte(`ok`), FirstResponseMS: t.firstResponseMS}, nil
}

type errTransport struct{}

func (errTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return nil, errors.New("network down")
}

type fakeStreamTransport struct {
	status          int
	body            []byte
	err             error
	firstResponseMS int64
}

func (t fakeStreamTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return &channels.OutboundResponse{Status: 200, Headers: http.Header{}, Body: []byte(`ok`), BodyPreview: []byte(`ok`), FirstResponseMS: t.firstResponseMS}, nil
}

func (t fakeStreamTransport) DoStream(context.Context, *channels.OutboundRequest) (*channels.OutboundStreamResponse, error) {
	if t.err != nil {
		return nil, t.err
	}
	status := t.status
	if status == 0 {
		status = 200
	}
	return &channels.OutboundStreamResponse{
		Status:          status,
		Headers:         http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:            io.NopCloser(bytes.NewReader(t.body)),
		BodyPreview:     t.body,
		FirstResponseMS: t.firstResponseMS,
	}, nil
}

type scopedFakeTransport struct {
	fakeStreamTransport
	scopes int
	closes int
}

func (t *scopedFakeTransport) WithRequestScope(ctx context.Context) (context.Context, func()) {
	t.scopes++
	return ctx, func() {
		t.closes++
	}
}

func TestRunnerCallsFinalizerOnSuccess(t *testing.T) {
	adapter := &fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}
	runner, _, _, closeDB := newTestRunner(t, adapter, 100)
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("expected finalizer once, got %d", adapter.calls)
	}
	if adapter.errs[0] != nil {
		t.Fatalf("expected nil outcome err, got %v", adapter.errs[0])
	}
}

func TestRunnerClosesRequestScopedTransportAfterExecute(t *testing.T) {
	tp := &scopedFakeTransport{}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, fakeAdapter{id: "demo"}, 100, tp)
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if tp.scopes != 1 || tp.closes != 1 {
		t.Fatalf("scopes/closes = %d/%d, want 1/1", tp.scopes, tp.closes)
	}
}

func TestRunnerCallsFinalizerOnTransportError(t *testing.T) {
	adapter := &fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, errTransport{})
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err == nil {
		t.Fatalf("expected execute error")
	}
	if adapter.calls != 1 {
		t.Fatalf("expected finalizer once, got %d", adapter.calls)
	}
	if adapter.errs[0] == nil {
		t.Fatalf("expected transport error in outcome")
	}
}

func TestRunnerStreamCallsFinalizerOnSuccess(t *testing.T) {
	adapter := &fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeStreamTransport{body: []byte("data: one\n\n")})
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	sink := &bufferStreamSink{}
	outcome, err := execution.Pump(sink)
	if err != nil {
		t.Fatalf("pump: %v", err)
	}
	if got, want := sink.String(), "data: one\n\n"; got != want {
		t.Fatalf("stream body = %q, want %q", got, want)
	}
	if outcome.Class != channels.ClassOk {
		t.Fatalf("class = %s, want ok", outcome.Class.String())
	}
	if adapter.calls != 1 {
		t.Fatalf("expected finalizer once, got %d", adapter.calls)
	}
	if adapter.errs[0] != nil {
		t.Fatalf("expected nil finalizer err, got %v", adapter.errs[0])
	}
}

func TestRunnerClosesRequestScopedTransportAfterStreamPump(t *testing.T) {
	adapter := &fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}}
	tp := &scopedFakeTransport{fakeStreamTransport: fakeStreamTransport{body: []byte("data: one\n\n")}}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, tp)
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	if tp.scopes != 1 || tp.closes != 0 {
		t.Fatalf("before pump scopes/closes = %d/%d, want 1/0", tp.scopes, tp.closes)
	}
	if _, err := execution.Pump(&bufferStreamSink{}); err != nil {
		t.Fatalf("pump: %v", err)
	}
	if tp.closes != 1 {
		t.Fatalf("closes after pump = %d, want 1", tp.closes)
	}
}

func TestRunnerStreamUsesRewriterOnlyForOKResponses(t *testing.T) {
	adapter := &fakeStreamRewriterAdapter{
		fakeStreamFinalizerAdapter: fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}},
	}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeStreamTransport{status: 400, body: []byte(`{"error":"bad request"}`)})
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	sink := &bufferStreamSink{}
	outcome, err := execution.Pump(sink)
	if err == nil {
		t.Fatalf("expected non-ok stream response error")
	}
	if got, want := sink.String(), `{"error":"bad request"}`; got != want {
		t.Fatalf("stream body = %q, want %q", got, want)
	}
	if outcome.Class != channels.ClassFatal {
		t.Fatalf("class = %s, want fatal", outcome.Class.String())
	}
	if !strings.Contains(err.Error(), `{"error":"bad request"}`) {
		t.Fatalf("error = %v, want upstream preview", err)
	}
	if adapter.rewrites != 0 {
		t.Fatalf("rewriter was called for non-ok response")
	}
}

func TestRunnerStreamFinalizerReceivesPumpError(t *testing.T) {
	adapter := &fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}}
	runner, _, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeStreamTransport{body: []byte("data: one\n\n")})
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	outcome, err := execution.Pump(failingStreamSink{})
	if err == nil {
		t.Fatalf("expected pump error")
	}
	if outcome.Class != channels.ClassRetryable {
		t.Fatalf("class = %s, want retryable", outcome.Class.String())
	}
	if adapter.calls != 1 {
		t.Fatalf("expected finalizer once, got %d", adapter.calls)
	}
	if adapter.errs[0] == nil {
		t.Fatalf("expected pump error in finalizer outcome")
	}
}

func TestRunnerStreamRecordsUsage(t *testing.T) {
	adapter := &fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}}
	runner, logRepo, metricsAgg, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeStreamTransport{body: []byte("data: one\n\n")})
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	if _, err := execution.Pump(&bufferStreamSink{}); err != nil {
		t.Fatalf("pump: %v", err)
	}

	rows := metricsAgg.Snapshot(time.Minute)
	if len(rows) != 1 || rows[0].RPM != 1 || rows[0].TPM != 0 {
		t.Fatalf("unexpected metrics rows: %+v", rows)
	}

	logRepo.Run(testDone{})
	entries, err := logRepo.List(logs.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(entries) != 1 || entries[0].ResponseClass != "ok" || entries[0].Status != 200 || entries[0].TokensKnown {
		t.Fatalf("unexpected log entries: %+v", entries)
	}
}

func TestRunnerStreamRecordsTokenUsageFromRewriter(t *testing.T) {
	adapter := &fakeStreamUsageRewriterAdapter{fakeStreamFinalizerAdapter: fakeStreamFinalizerAdapter{fakeFinalizerAdapter: fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}}}
	runner, logRepo, metricsAgg, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeStreamTransport{body: []byte("data: one\n\n"), firstResponseMS: 17})
	defer closeDB()

	execution, err := runner.ExecuteStream(context.Background(), inbound("demo"))
	if err != nil {
		t.Fatalf("execute stream: %v", err)
	}
	if _, err := execution.Pump(&bufferStreamSink{}); err != nil {
		t.Fatalf("pump: %v", err)
	}

	rows := metricsAgg.Snapshot(time.Minute)
	if len(rows) != 1 || rows[0].RPM != 1 || rows[0].TPM != 13 {
		t.Fatalf("unexpected metrics rows: %+v", rows)
	}

	logRepo.Run(testDone{})
	entries, err := logRepo.List(logs.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(entries) != 1 || entries[0].TokensIn != 5 || entries[0].TokensOut != 8 || !entries[0].TokensKnown {
		t.Fatalf("unexpected token log entries: %+v", entries)
	}
	if entries[0].FirstResponseMS < 17 {
		t.Fatalf("first response latency = %d, want at least 17", entries[0].FirstResponseMS)
	}
}

func TestRunnerBlocksQuotaOnReusedSession(t *testing.T) {
	runner, _, _, closeDB := newTestRunner(t, fakeAdapter{id: "demo"}, 2)
	defer closeDB()

	for i := 0; i < 2; i++ {
		if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
			t.Fatalf("execute %d: %v", i, err)
		}
	}
	if _, err := runner.Execute(context.Background(), inbound("demo")); !errors.Is(err, accounts.ErrQuotaExceeded) {
		t.Fatalf("expected quota exceeded on third request, got %v", err)
	}
}

func TestRunnerRecordsTokenMetrics(t *testing.T) {
	runner, logRepo, metricsAgg, closeDB := newTestRunner(t, fakeTokenAdapter{fakeAdapter: fakeAdapter{id: "demo"}}, 100)
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	rows := metricsAgg.Snapshot(time.Minute)
	if len(rows) != 1 || rows[0].RPM != 1 || rows[0].TPM != 10 {
		t.Fatalf("unexpected metrics rows: %+v", rows)
	}

	logRepo.Run(testDone{})
	entries, err := logRepo.List(logs.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(entries) != 1 || entries[0].TokensIn != 4 || entries[0].TokensOut != 6 || !entries[0].TokensKnown {
		t.Fatalf("unexpected log entries: %+v", entries)
	}
}

func TestRunnerRecordsFirstResponseLatency(t *testing.T) {
	runner, logRepo, _, closeDB := newTestRunnerWithTransport(t, fakeTokenAdapter{fakeAdapter: fakeAdapter{id: "demo"}}, 100, fakeTransport{firstResponseMS: 23})
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
		t.Fatalf("execute: %v", err)
	}

	logRepo.Run(testDone{})
	entries, err := logRepo.List(logs.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(entries) != 1 || entries[0].FirstResponseMS < 23 {
		t.Fatalf("unexpected first response latency entries: %+v", entries)
	}
}

func TestRunnerRecordsPhaseTimings(t *testing.T) {
	adapter := &fakeFinalizerAdapter{fakeAdapter: fakeAdapter{id: "demo"}}
	runner, logRepo, _, closeDB := newTestRunnerWithTransport(t, adapter, 100, fakeTransport{firstResponseMS: 23})
	defer closeDB()

	if _, err := runner.Execute(context.Background(), inbound("demo")); err != nil {
		t.Fatalf("execute: %v", err)
	}

	logRepo.Run(testDone{})
	entries, err := logRepo.List(logs.Query{Limit: 10})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %+v, want one", entries)
	}
	for _, key := range []string{"session_acquire_ms", "prepare_total_ms", "transport_ttfb_ms", "finalize_ms", "total_ms"} {
		if _, ok := entries[0].PhaseTimings[key]; !ok {
			t.Fatalf("phase %q missing from %+v", key, entries[0].PhaseTimings)
		}
	}
}

func newTestRunner(t *testing.T, adapter channels.ChannelAdapter, quotaTotal int64) (*Runner, *logs.Repo, *metrics.Aggregator, func()) {
	return newTestRunnerWithTransport(t, adapter, quotaTotal, fakeTransport{})
}

func newTestRunnerWithTransport(t *testing.T, adapter channels.ChannelAdapter, quotaTotal int64, tp channels.Transport) (*Runner, *logs.Repo, *metrics.Aggregator, func()) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "runner.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	accountRepo := accounts.NewRepo(db)
	if err := accountRepo.Create(&accounts.Record{
		ChannelID:        adapter.ID(),
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       quotaTotal,
		QuotaPeriod:      "day",
		QuotaPeriodStart: accounts.QuotaBucketStart(time.Now(), "day"),
	}); err != nil {
		t.Fatalf("create account: %v", err)
	}

	registry := channels.NewRegistry()
	registry.MustRegister(adapter)
	pool := accounts.NewPool(accountRepo)
	manager := session.NewManager(registry, pool, tp, session.Config{WaitOnFull: false, ReapInterval: time.Hour})
	logRepo := logs.NewRepo(db)
	metricsAgg := metrics.NewAggregator()
	usageRecorder := usage.NewRecorder(logRepo, metricsAgg, pool)
	return NewRunner(registry, manager, tp, usageRecorder), logRepo, metricsAgg, func() { _ = db.Close() }
}

func inbound(channelID string) *channels.InboundRequest {
	return &channels.InboundRequest{
		ChannelID: channelID,
		Method:    http.MethodPost,
		Path:      "/v1/chat",
		Headers:   http.Header{},
		Body:      []byte(`{}`),
	}
}

type testDone struct{}

func (testDone) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

type bufferStreamSink struct {
	bytes.Buffer
}

func (s *bufferStreamSink) Flush() {}

type failingStreamSink struct{}

func (failingStreamSink) Write([]byte) (int, error) {
	return 0, errors.New("downstream closed")
}

func (failingStreamSink) Flush() {}
