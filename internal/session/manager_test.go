package session

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"freebuff-reverse/internal/accounts"
	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/channels/freebuff"
	"freebuff-reverse/internal/storage"
)

type testAdapter struct {
	id        string
	policy    channels.SessionPolicy
	scheduler channels.SessionScheduler
}

func (a *testAdapter) ID() string { return a.id }
func (a *testAdapter) InboundPathPrefix() string {
	return "/channels/" + a.id
}
func (a *testAdapter) SessionPolicy() channels.SessionPolicy { return a.policy }
func (a *testAdapter) AuthFlow() channels.AuthFlow           { return nil }
func (a *testAdapter) ScheduleSession(ctx context.Context, req channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	if a.scheduler == nil {
		return channels.SessionScheduleDecision{}, nil
	}
	return a.scheduler.ScheduleSession(ctx, req)
}
func (a *testAdapter) PrepareOutbound(context.Context, *channels.Lease, *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return nil, nil
}
func (a *testAdapter) ClassifyResponse(int, http.Header, []byte) channels.ResponseClass {
	return channels.ClassOk
}

type testResolver struct {
	max  int
	wait bool
}

type capacityLimitedScheduler struct{}

func (s capacityLimitedScheduler) ScheduleSession(context.Context, channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	return channels.SessionScheduleDecision{}, channels.CapacityLimitedf("premium_capacity_limited")
}

type scriptedScheduler struct {
	mu        sync.Mutex
	decisions []channels.SessionScheduleDecision
	requests  []channels.SessionScheduleRequest
}

func (s *scriptedScheduler) ScheduleSession(_ context.Context, req channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	if len(s.decisions) == 0 {
		return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate}, nil
	}
	decision := s.decisions[0]
	s.decisions = s.decisions[1:]
	if decision.Action == "" {
		decision.Action = channels.SessionScheduleCreate
	}
	return decision, nil
}

type pendingCreateCapScheduler struct {
	mu          sync.Mutex
	maxPending  int
	waitTimeout time.Duration
	barrier     int
	waiting     int
	released    bool
	release     chan struct{}
	requests    []channels.SessionScheduleRequest
}

func (s *pendingCreateCapScheduler) ScheduleSession(ctx context.Context, req channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	pending := 0
	for _, create := range req.PendingCreates {
		if create.Key == req.SelectionKey {
			pending++
		}
	}

	s.mu.Lock()
	s.requests = append(s.requests, req)
	if pending == 0 && s.barrier > 0 && !s.released {
		s.waiting++
		if s.waiting >= s.barrier {
			s.released = true
			close(s.release)
		}
		release := s.release
		s.mu.Unlock()
		select {
		case <-release:
		case <-ctx.Done():
			return channels.SessionScheduleDecision{}, ctx.Err()
		}
		return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate}, nil
	}
	s.mu.Unlock()

	if pending >= s.maxPending {
		return channels.SessionScheduleDecision{
			Action:      channels.SessionScheduleWait,
			WaitTimeout: s.waitTimeout,
			Reason:      "test_model_cap",
		}, nil
	}
	return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate}, nil
}

func (r *testResolver) ResolveSessionPolicy(string, string, RuntimePolicy) RuntimePolicy {
	return RuntimePolicy{MaxConcurrentPerSession: r.max, WaitOnFull: r.wait}
}

type expiryPolicy struct {
	channels.NoopSessionPolicy
	expiresAt time.Time
}

func (p expiryPolicy) SessionExpiresAt(channels.State) (time.Time, bool) {
	return p.expiresAt, true
}

type recordingStateRecorder struct {
	events []StateEvent
}

func (r *recordingStateRecorder) RecordSessionState(_ context.Context, event StateEvent) error {
	r.events = append(r.events, event)
	return nil
}

type accountUnavailablePolicy struct {
	channels.NoopSessionPolicy
	rejectID string
	attempts []string
}

func (p *accountUnavailablePolicy) CreateSession(_ context.Context, acc channels.Account, _ string, _ channels.Transport) (channels.State, error) {
	p.attempts = append(p.attempts, acc.ID)
	if acc.ID == p.rejectID {
		return nil, channels.AccountUnavailablef("test account locked")
	}
	return channels.State{"chosen_account": acc.ID}, nil
}

type blockingCreatePolicy struct {
	channels.NoopSessionPolicy
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (p *blockingCreatePolicy) CreateSession(ctx context.Context, acc channels.Account, key string, tp channels.Transport) (channels.State, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.NoopSessionPolicy.CreateSession(ctx, acc, key, tp)
}

type multiBlockingCreatePolicy struct {
	channels.NoopSessionPolicy
	started chan string
	release chan struct{}
}

func (p *multiBlockingCreatePolicy) CreateSession(ctx context.Context, acc channels.Account, key string, tp channels.Transport) (channels.State, error) {
	p.started <- acc.ID
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	state, err := p.NoopSessionPolicy.CreateSession(ctx, acc, key, tp)
	if state == nil {
		state = channels.State{}
	}
	state["chosen_account"] = acc.ID
	return state, err
}

type sessionTransport struct {
	responses []*channels.OutboundResponse
	requests  []*channels.OutboundRequest
}

func (tp *sessionTransport) Do(_ context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	if resp := tp.defaultRespond(req); resp != nil {
		return resp, nil
	}
	tp.requests = append(tp.requests, req)
	idx := len(tp.requests) - 1
	if idx >= len(tp.responses) {
		return nil, errors.New("unexpected request")
	}
	return tp.responses[idx], nil
}

func (tp *sessionTransport) defaultRespond(req *channels.OutboundRequest) *channels.OutboundResponse {
	switch {
	case strings.Contains(req.URL, "/api/v1/me"):
		return jsonSessionResponse(map[string]any{"id": "test-id", "email": "test@test.com"})
	case strings.Contains(req.URL, "/api/v1/ads/impression"):
		return jsonSessionResponse(map[string]any{})
	case strings.Contains(req.URL, "/api/v1/ads"):
		return jsonSessionResponse(map[string]any{"impUrl": "https://ads.test/imp"})
	case strings.Contains(req.URL, "/api/healthz"):
		return jsonSessionResponse(map[string]any{"status": "ok"})
	case strings.Contains(req.URL, "/api/v1/agent-runs") && strings.Contains(string(req.Body), "context-pruner") && strings.Contains(string(req.Body), "START"):
		return jsonSessionResponse(map[string]any{"runId": "child-run-id"})
	case strings.Contains(req.URL, "/api/v1/agent-runs") && strings.Contains(string(req.Body), "FINISH") && strings.Contains(string(req.Body), `"totalSteps":1`):
		return jsonSessionResponse(map[string]any{})
	case strings.Contains(req.URL, "/api/v1/agent-runs/") && strings.Contains(req.URL, "/steps"):
		return jsonSessionResponse(map[string]any{"stepId": "step-aux"})
	}
	return nil
}

type reclaimingAdapter struct {
	id      string
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (a *reclaimingAdapter) ID() string { return a.id }
func (a *reclaimingAdapter) InboundPathPrefix() string {
	return "/channels/" + a.id
}
func (a *reclaimingAdapter) SessionPolicy() channels.SessionPolicy { return a }
func (a *reclaimingAdapter) AuthFlow() channels.AuthFlow           { return nil }
func (a *reclaimingAdapter) PrepareOutbound(context.Context, *channels.Lease, *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return nil, nil
}
func (a *reclaimingAdapter) ClassifyResponse(int, http.Header, []byte) channels.ResponseClass {
	return channels.ClassOk
}
func (a *reclaimingAdapter) SelectionKey(in *channels.InboundRequest) string {
	if in == nil {
		return ""
	}
	return string(in.Body)
}
func (a *reclaimingAdapter) SessionTTL() time.Duration { return time.Hour }
func (a *reclaimingAdapter) MaxSessionsPerAccount() int {
	return 1
}
func (a *reclaimingAdapter) MaxConcurrentPerSession() int {
	return 1
}
func (a *reclaimingAdapter) CreateSession(_ context.Context, _ channels.Account, key string, _ channels.Transport) (channels.State, error) {
	return channels.State{"model": key}, nil
}
func (a *reclaimingAdapter) ClassifySessionHealth(channels.State, channels.ResponseClass) channels.Verdict {
	return channels.VerdictHealthy
}
func (a *reclaimingAdapter) Heartbeat(context.Context, channels.Account, channels.State, channels.Transport) error {
	return nil
}
func (a *reclaimingAdapter) CanReclaimSessionForRequest(state channels.State, in *channels.InboundRequest) bool {
	return state.String("model") != a.SelectionKey(in)
}
func (a *reclaimingAdapter) ReclaimedSessionState(state channels.State) (channels.State, bool) {
	return channels.State{"model": state.String("model"), "status": "ended"}, true
}
func (a *reclaimingAdapter) ReclaimSessionForRequest(ctx context.Context, _ channels.Account, state channels.State, in *channels.InboundRequest, _ channels.Transport) (channels.State, bool, error) {
	if !a.CanReclaimSessionForRequest(state, in) {
		return nil, false, nil
	}
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	reclaimed, ok := a.ReclaimedSessionState(state)
	return reclaimed, ok, nil
}

func TestManagerRetriesNextAccountWhenProviderMarksAccountUnavailable(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-retry.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{
		ChannelID:  "demo",
		Name:       "first",
		Credential: "token-1",
		Priority:   100,
		IsActive:   true,
	}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	second := &accounts.Record{
		ChannelID:  "demo",
		Name:       "second",
		Credential: "token-2",
		Priority:   50,
		IsActive:   true,
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}
	policy := &accountUnavailablePolicy{
		NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		rejectID: first.ID,
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{id: "demo", policy: policy}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release(channels.VerdictHealthy)

	if lease.AccountID != second.ID {
		t.Fatalf("lease account = %s, want second account %s", lease.AccountID, second.ID)
	}
	if len(policy.attempts) != 2 || policy.attempts[0] != first.ID || policy.attempts[1] != second.ID {
		t.Fatalf("attempts = %+v, want first then second", policy.attempts)
	}
}

func TestManagerCreatesNewSessionWhenMatchingSessionFull(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-full-new.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 50, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	defer firstLease.Release(channels.VerdictHealthy)

	secondLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire second while first is full: %v", err)
	}
	defer secondLease.Release(channels.VerdictHealthy)

	if firstLease.SessionID == secondLease.SessionID {
		t.Fatalf("second lease reused full session %s", firstLease.SessionID)
	}
	if secondLease.AccountID != second.ID {
		t.Fatalf("second lease account = %s, want %s", secondLease.AccountID, second.ID)
	}
}

func TestManagerWaitOnFullCreatesNewSessionBeforeWaiting(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-wait-full-new.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 50, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{WaitOnFull: true})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	defer firstLease.Release(channels.VerdictHealthy)

	done := make(chan struct{})
	go func() {
		defer close(done)
		secondLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
		if err != nil {
			t.Errorf("acquire second while first is full: %v", err)
			return
		}
		defer secondLease.Release(channels.VerdictHealthy)
		if secondLease.SessionID == firstLease.SessionID {
			t.Errorf("second lease reused full session %s", firstLease.SessionID)
		}
		if secondLease.AccountID != second.ID {
			t.Errorf("second lease account = %s, want %s", secondLease.AccountID, second.ID)
		}
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("wait-on-full acquire blocked instead of creating another eligible session")
	}
}

func TestManagerCreatesSameModelSessionsInParallelUpToCreateLimit(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-parallel-create.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	for i, name := range []string{"first", "second", "third"} {
		rec := &accounts.Record{ChannelID: "demo", Name: name, Credential: "token", Priority: 100 - i, IsActive: true}
		if err := repo.Create(rec); err != nil {
			t.Fatalf("create account %s: %v", name, err)
		}
	}
	policy := &multiBlockingCreatePolicy{
		NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		started: make(chan string, 3),
		release: make(chan struct{}),
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{id: "demo", policy: policy}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{
		WaitOnFull: true,
		CreateLimits: CreateLimitConfig{
			MaxParallelGlobal:   2,
			MaxParallelPerKey:   2,
			MaxParallelPerModel: 2,
			MaxParallelPerGroup: 2,
		},
	})

	type result struct {
		lease *channels.Lease
		err   error
	}
	results := make(chan result, 3)
	for i := 0; i < 3; i++ {
		go func() {
			lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
			results <- result{lease: lease, err: err}
		}()
	}

	started := map[string]struct{}{}
	for len(started) < 2 {
		select {
		case accountID := <-policy.started:
			started[accountID] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("started creates = %+v, want two parallel creates", started)
		}
	}
	select {
	case accountID := <-policy.started:
		t.Fatalf("third create started before limit released: %s", accountID)
	case <-time.After(50 * time.Millisecond):
	}

	close(policy.release)
	for i := 0; i < 3; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("acquire result %d: %v", i, res.err)
			}
			defer res.lease.Release(channels.VerdictHealthy)
		case <-time.After(time.Second):
			t.Fatalf("missing acquire result %d", i)
		}
	}
}

func TestManagerRevalidatesSchedulerAfterCreatePermit(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-revalidate-create-permit.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	for i, name := range []string{"first", "second"} {
		rec := &accounts.Record{ChannelID: "demo", Name: name, Credential: "token", Priority: 100 - i, IsActive: true}
		if err := repo.Create(rec); err != nil {
			t.Fatalf("create account %s: %v", name, err)
		}
	}
	policy := &multiBlockingCreatePolicy{
		NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
	scheduler := &pendingCreateCapScheduler{
		maxPending:  1,
		waitTimeout: 200 * time.Millisecond,
		barrier:     2,
		release:     make(chan struct{}),
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{id: "demo", policy: policy, scheduler: scheduler}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{
		WaitOnFull: true,
		CreateLimits: CreateLimitConfig{
			MaxParallelGlobal:   2,
			MaxParallelPerKey:   2,
			MaxParallelPerModel: 2,
			MaxParallelPerGroup: 2,
		},
	})

	type result struct {
		lease *channels.Lease
		err   error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			lease, err := manager.Acquire(ctx, "demo", &channels.InboundRequest{ChannelID: "demo"})
			results <- result{lease: lease, err: err}
		}()
	}

	select {
	case <-policy.started:
	case <-time.After(time.Second):
		t.Fatal("first create did not start")
	}
	select {
	case accountID := <-policy.started:
		t.Fatalf("second create started before scheduler capacity changed: %s", accountID)
	case <-time.After(50 * time.Millisecond):
	}

	close(policy.release)
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("acquire result %d: %v", i, res.err)
			}
			defer res.lease.Release(channels.VerdictHealthy)
		case <-time.After(time.Second):
			t.Fatalf("missing acquire result %d", i)
		}
	}
}

func TestManagerFullSessionWithoutWaitReturnsNoCapacityWithoutBlocking(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-full-no-wait.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	defer firstLease.Release(channels.VerdictHealthy)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err = manager.Acquire(ctx, "demo", &channels.InboundRequest{ChannelID: "demo"})
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("acquire err = %v, want ErrNoCapacity", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("acquire took %s, want immediate no-capacity failure", elapsed)
	}
}

func TestManagerPropagatesSchedulerCapacityLimited(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-scheduler-capacity.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		scheduler: capacityLimitedScheduler{},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	_, err = manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if !errors.Is(err, channels.ErrCapacityLimited) {
		t.Fatalf("acquire err = %v, want capacity limited", err)
	}
}

func TestManagerUsesSchedulerPreferredAccountOrder(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-scheduler-preferred.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 10, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}
	preferredDecision := channels.SessionScheduleDecision{
		Action:              channels.SessionScheduleCreate,
		PreferredAccountIDs: []string{second.ID, first.ID},
		Reason:              "test_preferred_order",
	}
	scheduler := &scriptedScheduler{decisions: []channels.SessionScheduleDecision{
		preferredDecision,
		preferredDecision,
	}}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		scheduler: scheduler,
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release(channels.VerdictHealthy)
	if lease.AccountID != second.ID {
		t.Fatalf("lease account = %s, want scheduler preferred %s", lease.AccountID, second.ID)
	}
	if len(scheduler.requests) != 2 || len(scheduler.requests[0].Accounts) != 2 || len(scheduler.requests[1].Accounts) != 2 {
		t.Fatalf("scheduler requests = %+v, want account candidates", scheduler.requests)
	}
}

func TestManagerSchedulerWaitTimesOutAsCapacityLimited(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-scheduler-wait.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	finished := make(chan struct{})
	scheduler := &scriptedScheduler{decisions: []channels.SessionScheduleDecision{
		{Action: channels.SessionScheduleCreate, Reason: "test_create"},
		{Action: channels.SessionScheduleCreate, Reason: "test_create_revalidated"},
		{Action: channels.SessionScheduleWait, WaitTimeout: 20 * time.Millisecond, Reason: "premium_queue_reuse", Finish: func() { close(finished) }},
	}}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		scheduler: scheduler,
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	defer firstLease.Release(channels.VerdictHealthy)

	start := time.Now()
	_, err = manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if !errors.Is(err, channels.ErrCapacityLimited) {
		t.Fatalf("acquire err = %v, want scheduler capacity limited", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("acquire elapsed = %s, want bounded scheduler wait", elapsed)
	}
	select {
	case <-finished:
	default:
		t.Fatal("scheduler wait finish callback was not called")
	}
}

func TestManagerCollapsedCreateFullSessionCreatesAnotherSession(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-collapsed-create-full.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 50, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}

	policy := &blockingCreatePolicy{
		NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{id: "demo", policy: policy}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	type acquireResult struct {
		lease *channels.Lease
		err   error
	}
	results := make(chan acquireResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
			results <- acquireResult{lease: lease, err: err}
		}()
	}

	select {
	case <-policy.started:
	case <-time.After(time.Second):
		t.Fatal("create session did not start")
	}
	time.Sleep(50 * time.Millisecond)
	close(policy.release)

	var leases []*channels.Lease
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("acquire %d: %v", i, res.err)
			}
			leases = append(leases, res.lease)
		case <-time.After(time.Second):
			t.Fatalf("acquire %d timed out", i)
		}
	}
	defer leases[0].Release(channels.VerdictHealthy)
	defer leases[1].Release(channels.VerdictHealthy)

	if leases[0].SessionID == leases[1].SessionID {
		t.Fatalf("leases used same full session %s", leases[0].SessionID)
	}
	if leases[0].AccountID == leases[1].AccountID {
		t.Fatalf("leases used same account %s, want two accounts", leases[0].AccountID)
	}
	if snapshots := manager.Snapshot(); len(snapshots) != 2 {
		t.Fatalf("snapshots = %+v, want two sessions", snapshots)
	}
}

func TestManagerUsesSecondMatchingSessionWhenFirstFull(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-full-second.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 50, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	defer firstLease.Release(channels.VerdictHealthy)
	secondLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	secondSessionID := secondLease.SessionID
	secondAccountID := secondLease.AccountID
	secondLease.Release(channels.VerdictHealthy)

	thirdLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire third: %v", err)
	}
	defer thirdLease.Release(channels.VerdictHealthy)
	if thirdLease.SessionID != secondSessionID || thirdLease.AccountID != secondAccountID {
		t.Fatalf("third lease = session %s account %s, want second session %s account %s", thirdLease.SessionID, thirdLease.AccountID, secondSessionID, secondAccountID)
	}
}

func TestManagerWaitOnFullStillWaitsForExistingSession(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-wait-full.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{WaitOnFull: true})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}

	type acquireResult struct {
		lease *channels.Lease
		err   error
	}
	done := make(chan acquireResult, 1)
	go func() {
		lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
		done <- acquireResult{lease: lease, err: err}
	}()

	select {
	case res := <-done:
		t.Fatalf("wait-on-full acquire completed before slot release: %+v", res)
	case <-time.After(50 * time.Millisecond):
	}

	firstLease.Release(channels.VerdictHealthy)
	res := <-done
	if res.err != nil {
		t.Fatalf("waiting acquire: %v", res.err)
	}
	defer res.lease.Release(channels.VerdictHealthy)
	if res.lease.SessionID != firstLease.SessionID {
		t.Fatalf("waiting lease session = %s, want existing %s", res.lease.SessionID, firstLease.SessionID)
	}
	if snapshots := manager.Snapshot(); len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want one existing session", snapshots)
	}
}

func TestManagerExpiresIdleFreeBuffUnlimitedSessionBeforeDifferentModel(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-freebuff-reclaim.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuff.ID,
		Name:       "freebuff",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	tp := &sessionTransport{responses: []*channels.OutboundResponse{
		{Status: http.StatusNoContent, Headers: http.Header{}},
		jsonSessionResponse(map[string]any{"status": "active", "instanceId": "inst-flash", "model": "deepseek/deepseek-v4-flash", "expiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}),
		jsonSessionResponse(map[string]any{"runId": "session-run-flash"}),
		jsonSessionResponse(map[string]any{"status": "ended", "instanceId": "inst-flash", "model": "deepseek/deepseek-v4-flash"}),
		jsonSessionResponse(map[string]any{"status": "none"}),
		jsonSessionResponse(map[string]any{"status": "none"}),
		jsonSessionResponse(map[string]any{"status": "active", "instanceId": "inst-pro", "model": "deepseek/deepseek-v4-pro", "expiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}),
		jsonSessionResponse(map[string]any{"runId": "session-run-pro"}),
	}}
	adapter := freebuff.New(freebuff.WithBaseURL("https://codebuff.test"))
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	recorder := &recordingStateRecorder{}
	manager := NewManager(registry, accounts.NewPool(repo), tp, Config{StateRecorder: recorder})

	flashLease, err := manager.Acquire(context.Background(), freebuff.ID, freebuffRequest("deepseek/deepseek-v4-flash"))
	if err != nil {
		t.Fatalf("acquire flash: %v", err)
	}
	flashSessionID := flashLease.SessionID
	flashLease.Release(channels.VerdictHealthy)

	proLease, err := manager.Acquire(context.Background(), freebuff.ID, freebuffRequest("deepseek/deepseek-v4-pro"))
	if err != nil {
		t.Fatalf("acquire pro after reclaim: %v", err)
	}
	defer proLease.Release(channels.VerdictHealthy)
	if proLease.SessionID == flashSessionID {
		t.Fatalf("pro reused reclaimed flash local session %s", flashSessionID)
	}
	if proLease.AccountID != account.ID {
		t.Fatalf("pro lease account = %s, want %s", proLease.AccountID, account.ID)
	}
	if len(tp.requests) != 8 {
		t.Fatalf("request count = %d, want 8", len(tp.requests))
	}
	if tp.requests[3].Method != http.MethodDelete {
		t.Fatalf("request 3 method = %s, want DELETE", tp.requests[3].Method)
	}
	if tp.requests[4].Method != http.MethodGet {
		t.Fatalf("request 4 method = %s, want GET release verification", tp.requests[4].Method)
	}
	if got := tp.requests[6].Headers.Get("x-freebuff-model"); got != "deepseek/deepseek-v4-pro" {
		t.Fatalf("joined model = %q, want pro", got)
	}
	snapshots := manager.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want only pro local session", snapshots)
	}
	if snapshots[0].ID == flashSessionID {
		t.Fatalf("reclaimed flash session still present: %+v", snapshots[0])
	}
	if got := snapshots[0].Details["freebuff_model"]; got != "deepseek/deepseek-v4-pro" {
		t.Fatalf("remaining session model = %#v, want pro", got)
	}
	if len(recorder.events) != 3 {
		t.Fatalf("recorded events = %d, want flash active, flash ended, pro active", len(recorder.events))
	}
	if got := recorder.events[1].State.String("freebuff_status"); got != "ended" {
		t.Fatalf("reclaimed state status = %q, want ended", got)
	}
}

func TestManagerDoesNotLeaseSessionWhileReclaiming(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-reclaiming-session.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	first := &accounts.Record{ChannelID: "demo", Name: "first", Credential: "token-1", Priority: 100, IsActive: true}
	second := &accounts.Record{ChannelID: "demo", Name: "second", Credential: "token-2", Priority: 50, IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first account: %v", err)
	}
	if err := repo.Create(second); err != nil {
		t.Fatalf("create second account: %v", err)
	}

	adapter := &reclaimingAdapter{
		id:      "demo",
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	firstLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{
		ChannelID: "demo",
		Body:      []byte("model-a"),
	})
	if err != nil {
		t.Fatalf("acquire first model: %v", err)
	}
	reclaimingSessionID := firstLease.SessionID
	firstLease.Release(channels.VerdictHealthy)

	type acquireResult struct {
		lease *channels.Lease
		err   error
	}
	reclaimDone := make(chan acquireResult, 1)
	go func() {
		lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{
			ChannelID: "demo",
			Body:      []byte("model-b"),
		})
		reclaimDone <- acquireResult{lease: lease, err: err}
	}()

	select {
	case <-adapter.started:
	case <-time.After(time.Second):
		t.Fatal("reclaim did not start")
	}

	secondLease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{
		ChannelID: "demo",
		Body:      []byte("model-a"),
	})
	if err != nil {
		t.Fatalf("acquire while reclaiming: %v", err)
	}
	defer secondLease.Release(channels.VerdictHealthy)
	if secondLease.SessionID == reclaimingSessionID {
		t.Fatalf("acquired reclaiming session %s", reclaimingSessionID)
	}

	close(adapter.release)
	var reclaimed acquireResult
	select {
	case reclaimed = <-reclaimDone:
	case <-time.After(time.Second):
		t.Fatal("reclaiming acquire did not finish")
	}
	if reclaimed.err != nil {
		t.Fatalf("reclaiming acquire: %v", reclaimed.err)
	}
	defer reclaimed.lease.Release(channels.VerdictHealthy)
	if reclaimed.lease.SessionID == reclaimingSessionID {
		t.Fatalf("reclaiming acquire reused expired session %s", reclaimingSessionID)
	}
}

func TestManagerDoesNotRecordEndedWhenFreeBuffReclaimReleaseFails(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-freebuff-reclaim-fail.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuff.ID,
		Name:       "freebuff",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	tp := &sessionTransport{responses: []*channels.OutboundResponse{
		{Status: http.StatusNoContent, Headers: http.Header{}},
		jsonSessionResponse(map[string]any{"status": "active", "instanceId": "inst-flash", "model": "deepseek/deepseek-v4-flash", "expiresAt": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}),
		jsonSessionResponse(map[string]any{"runId": "session-run-flash"}),
		{
			Status:      http.StatusInternalServerError,
			Headers:     http.Header{},
			Body:        []byte(`{"error":"busy"}`),
			BodyPreview: []byte(`{"error":"busy"}`),
		},
	}}
	adapter := freebuff.New(freebuff.WithBaseURL("https://codebuff.test"))
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	recorder := &recordingStateRecorder{}
	manager := NewManager(registry, accounts.NewPool(repo), tp, Config{StateRecorder: recorder})

	flashLease, err := manager.Acquire(context.Background(), freebuff.ID, freebuffRequest("deepseek/deepseek-v4-flash"))
	if err != nil {
		t.Fatalf("acquire flash: %v", err)
	}
	flashSessionID := flashLease.SessionID
	flashLease.Release(channels.VerdictHealthy)

	_, err = manager.Acquire(context.Background(), freebuff.ID, freebuffRequest("deepseek/deepseek-v4-pro"))
	if err == nil {
		t.Fatal("acquire pro succeeded, want capacity error after reclaim release failure")
	}
	if len(tp.requests) != 4 {
		t.Fatalf("request count = %d, want initial fetch, join, session run, failed DELETE", len(tp.requests))
	}
	if tp.requests[3].Method != http.MethodDelete {
		t.Fatalf("request 3 method = %s, want DELETE", tp.requests[3].Method)
	}
	snapshots := manager.Snapshot()
	if len(snapshots) != 1 || snapshots[0].ID != flashSessionID {
		t.Fatalf("snapshots = %+v, want unreclaimed flash session", snapshots)
	}
	if len(recorder.events) != 1 {
		t.Fatalf("recorded events = %d, want only flash active", len(recorder.events))
	}
	if got := recorder.events[0].State.String("freebuff_status"); got != "active" {
		t.Fatalf("recorded status = %q, want active", got)
	}
}

func TestManagerReturnsNoEligibleWhenEveryCandidateUnavailable(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-all-unavailable.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "first",
		Credential: "token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	policy := &accountUnavailablePolicy{
		NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
		rejectID: account.ID,
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{id: "demo", policy: policy}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	_, err = manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if !errors.Is(err, accounts.ErrNoEligibleAccount) || !errors.Is(err, channels.ErrAccountUnavailable) {
		t.Fatalf("acquire err = %v, want no eligible wrapping account unavailable", err)
	}
}

func TestManagerRefreshChannelPolicyResizesActiveSession(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-refresh.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	resolver := &testResolver{max: 1}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{Resolver: resolver})

	lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lease.Release(channels.VerdictHealthy)

	snapshots := manager.Snapshot()
	if len(snapshots) != 1 || snapshots[0].MaxConcurrency != 1 {
		t.Fatalf("initial snapshots = %+v, want max concurrency 1", snapshots)
	}
	resolver.max = 2
	resolver.wait = true
	manager.RefreshChannelPolicy("demo")

	snapshots = manager.Snapshot()
	if len(snapshots) != 1 || snapshots[0].MaxConcurrency != 2 {
		t.Fatalf("refreshed snapshots = %+v, want max concurrency 2", snapshots)
	}
	session := manager.byID[snapshots[0].ID]
	if session == nil || !session.waitOnFullEnabled() {
		t.Fatalf("wait_on_full was not refreshed")
	}
}

func TestManagerUsesProviderSessionExpiryWhenEarlierThanTTL(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-expiry.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	expiresAt := time.Now().Add(3 * time.Minute)
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: expiryPolicy{NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		}, expiresAt: expiresAt},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{})

	lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lease.Release(channels.VerdictHealthy)

	snapshots := manager.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want one", snapshots)
	}
	got := time.Unix(snapshots[0].ExpiresAtUnix, 0)
	if got.Before(expiresAt.Add(-2*time.Second)) || got.After(expiresAt.Add(2*time.Second)) {
		t.Fatalf("session expires at %s, want near provider expiry %s", got, expiresAt)
	}
}

func TestManagerRegisterRestoredSessionRecordsAndReleasesAccountSlot(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-restore.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	expiresAt := time.Now().Add(50 * time.Millisecond)
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: expiryPolicy{NoopSessionPolicy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 2,
		}, expiresAt: expiresAt},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	recorder := &recordingStateRecorder{}
	pool := accounts.NewPool(repo)
	manager := NewManager(registry, pool, nil, Config{StateRecorder: recorder})
	state := channels.State{"freebuff_model": "deepseek/deepseek-v4-pro"}

	registered, err := manager.RegisterRestoredSession(context.Background(), "demo", account.ID, "demo|deepseek/deepseek-v4-pro", state)
	if err != nil {
		t.Fatalf("register restored session: %v", err)
	}
	if !registered {
		t.Fatal("register restored session returned false, want true")
	}
	state["freebuff_model"] = "mutated-after-register"

	snapshots := manager.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want one restored session", snapshots)
	}
	if snapshots[0].AccountID != account.ID || snapshots[0].Key != "demo|deepseek/deepseek-v4-pro" {
		t.Fatalf("snapshot = %+v, want restored account and key", snapshots[0])
	}
	if snapshots[0].Details["freebuff_model"] != "deepseek/deepseek-v4-pro" {
		t.Fatalf("snapshot details = %+v, want cloned state", snapshots[0].Details)
	}
	if snapshots[0].MaxConcurrency != 2 {
		t.Fatalf("max concurrency = %d, want 2", snapshots[0].MaxConcurrency)
	}
	if len(recorder.events) != 1 || recorder.events[0].LocalSessionID != snapshots[0].ID {
		t.Fatalf("recorded events = %+v, snapshot id %s", recorder.events, snapshots[0].ID)
	}
	poolSnapshots := pool.Snapshot()
	if len(poolSnapshots) != 1 || poolSnapshots[0].AccountID != account.ID || poolSnapshots[0].SessionCount != 1 {
		t.Fatalf("pool snapshots = %+v, want restored account count 1", poolSnapshots)
	}

	duplicate, err := manager.RegisterRestoredSession(context.Background(), "demo", account.ID, "demo|deepseek/deepseek-v4-pro", channels.State{"freebuff_model": "duplicate"})
	if err != nil {
		t.Fatalf("duplicate restore: %v", err)
	}
	if duplicate {
		t.Fatal("duplicate restored session registered, want skip")
	}
	if got := pool.Snapshot()[0].SessionCount; got != 1 {
		t.Fatalf("pool count after duplicate = %d, want 1", got)
	}

	time.Sleep(80 * time.Millisecond)
	manager.reap()
	if snapshots := manager.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("snapshots after expiry = %+v, want none", snapshots)
	}
	if got := pool.Snapshot()[0].SessionCount; got != 0 {
		t.Fatalf("pool count after restored session expiry = %d, want 0", got)
	}
}

func TestManagerRecordsSessionStateAfterCreate(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "manager-recorder.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  "demo",
		Name:       "Ada",
		Credential: "token",
		IsActive:   true,
	}
	if err := repo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&testAdapter{
		id: "demo",
		policy: channels.NoopSessionPolicy{
			TTL:            time.Hour,
			MaxPerAccount:  1,
			MaxConcurrency: 1,
		},
	}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	recorder := &recordingStateRecorder{}
	manager := NewManager(registry, accounts.NewPool(repo), nil, Config{StateRecorder: recorder})

	lease, err := manager.Acquire(context.Background(), "demo", &channels.InboundRequest{ChannelID: "demo"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release(channels.VerdictHealthy)

	if len(recorder.events) != 1 {
		t.Fatalf("recorded events = %d, want 1", len(recorder.events))
	}
	event := recorder.events[0]
	if event.ChannelID != "demo" || event.AccountID != account.ID || event.LocalSessionID != lease.SessionID {
		t.Fatalf("recorded event = %+v, lease session %s account %s", event, lease.SessionID, account.ID)
	}
	if event.CreatedAtUnix == 0 || event.ExpiresAtUnix == 0 {
		t.Fatalf("recorded event missing timestamps: %+v", event)
	}
}

func jsonSessionResponse(v map[string]any) *channels.OutboundResponse {
	body, _ := json.Marshal(v)
	return &channels.OutboundResponse{
		Status:      http.StatusOK,
		Headers:     http.Header{"Content-Type": []string{"application/json"}},
		Body:        body,
		BodyPreview: body,
	}
}

func freebuffRequest(model string) *channels.InboundRequest {
	body, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	return &channels.InboundRequest{
		ChannelID: freebuff.ID,
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Headers:   http.Header{},
		Body:      body,
	}
}
