package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

func TestSessionDynamicLimiterRaisesConcurrency(t *testing.T) {
	s := newSession("s1", "demo", "acc1", "key", channels.State{}, time.Hour, 1, false, nil)
	if !s.tryAcquire() {
		t.Fatal("first acquire failed")
	}
	acquired := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		acquired <- s.acquireCtx(ctx)
	}()

	select {
	case err := <-acquired:
		t.Fatalf("second acquire completed before resize: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	s.setMaxConcurrency(2)

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("second acquire after resize: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("second acquire did not unblock: %v", ctx.Err())
	}
	inFlight, max := s.limitSnapshot()
	if inFlight != 2 || max != 2 {
		t.Fatalf("limit snapshot = %d/%d, want 2/2", inFlight, max)
	}
}

func TestSessionDynamicLimiterLowerDoesNotCancelInFlight(t *testing.T) {
	s := newSession("s1", "demo", "acc1", "key", channels.State{}, time.Hour, 2, false, nil)
	if !s.tryAcquire() || !s.tryAcquire() {
		t.Fatal("expected two initial acquires")
	}
	s.setMaxConcurrency(1)
	if s.tryAcquire() {
		t.Fatal("acquired while in_flight is above lowered limit")
	}
	s.releaseSem()
	if s.tryAcquire() {
		t.Fatal("acquired while in_flight equals lowered limit")
	}
	s.releaseSem()
	if !s.tryAcquire() {
		t.Fatal("expected acquire after in_flight dropped below limit")
	}
	inFlight, max := s.limitSnapshot()
	if inFlight != 1 || max != 1 {
		t.Fatalf("limit snapshot = %d/%d, want 1/1", inFlight, max)
	}
}

func TestSessionAcquireCtxCancelsWhileFull(t *testing.T) {
	s := newSession("s1", "demo", "acc1", "key", channels.State{}, time.Hour, 1, true, nil)
	if !s.tryAcquire() {
		t.Fatal("first acquire failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.acquireCtx(ctx); err == nil {
		t.Fatal("expected context cancellation")
	}
}

func TestSessionRefreshingStateBlocksCandidatesAndCanExpire(t *testing.T) {
	s := newSession("s1", "demo", "acc1", "key", channels.State{}, time.Hour, 1, false, nil)
	if !s.isHealthy() {
		t.Fatal("new session should be healthy")
	}
	if !s.beginRefresh() {
		t.Fatal("begin refresh failed")
	}
	if s.isHealthy() {
		t.Fatal("refreshing session should not be healthy")
	}
	if !s.markExpired() {
		t.Fatal("refreshing session should expire")
	}
	if s.restoreHealthyFromRefreshing() {
		t.Fatal("expired session should not restore to healthy")
	}
	if s.markExpired() {
		t.Fatal("expired session should not expire twice")
	}
}

func TestSessionRefreshReservationBlocksStaleCandidateAcquire(t *testing.T) {
	s := newSession("s1", "demo", "acc1", "key", channels.State{}, time.Hour, 1, true, nil)
	if !s.tryAcquire() {
		t.Fatal("initial acquire failed")
	}
	if s.beginRefresh() {
		t.Fatal("refresh should not start while a request is in flight")
	}
	s.releaseSem()
	if !s.beginRefresh() {
		t.Fatal("refresh should reserve an idle session")
	}
	if s.tryAcquire() {
		t.Fatal("tryAcquire should reject a stale refreshing candidate")
	}
	if err := s.acquireCtx(context.Background()); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("acquireCtx err = %v, want ErrNoCapacity", err)
	}
}

func TestPublicDetailsHidesRawSessionJSON(t *testing.T) {
	details := publicDetails(channels.State{
		"freebuff_model":            "deepseek/deepseek-v4-pro",
		"freebuff_raw_session_json": `{"instanceId":"inst-1"}`,
	})
	if details["freebuff_model"] != "deepseek/deepseek-v4-pro" {
		t.Fatalf("model detail = %#v", details["freebuff_model"])
	}
	if _, ok := details["freebuff_raw_session_json"]; ok {
		t.Fatalf("raw session json should not be exposed: %+v", details)
	}
}
