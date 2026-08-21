package session

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCreateGateAllowsBoundedParallelPerModel(t *testing.T) {
	g := newCreateGate(CreateLimitConfig{
		MaxParallelGlobal:   3,
		MaxParallelPerKey:   2,
		MaxParallelPerModel: 2,
		MaxParallelPerGroup: 3,
	})
	labels := createLabels{ChannelID: "freebuff", Key: "freebuff|model-a", Model: "model-a", QuotaGroup: "unlimited"}
	first, err := g.acquire(context.Background(), labels, false)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Release()
	second, err := g.acquire(context.Background(), labels, false)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer second.Release()
	if _, err := g.acquire(context.Background(), labels, false); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("third acquire err = %v, want ErrNoCapacity", err)
	}
}

func TestCreateGateReleasesPermitOnceAndWakesWaiter(t *testing.T) {
	g := newCreateGate(CreateLimitConfig{
		MaxParallelGlobal:   1,
		MaxParallelPerKey:   1,
		MaxParallelPerModel: 1,
		MaxParallelPerGroup: 1,
	})
	labels := createLabels{ChannelID: "freebuff", Key: "freebuff|model-a", Model: "model-a", QuotaGroup: "unlimited"}
	first, err := g.acquire(context.Background(), labels, false)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		permit, err := g.acquire(context.Background(), labels, true)
		if permit != nil {
			permit.Release()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("waiter completed before release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	first.Release()
	first.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not wake after release")
	}
}

func TestCreateGatePendingCandidates(t *testing.T) {
	g := newCreateGate(CreateLimitConfig{MaxParallelGlobal: 2, MaxParallelPerKey: 2, MaxParallelPerModel: 2, MaxParallelPerGroup: 2})
	permit, err := g.acquire(context.Background(), createLabels{ChannelID: "freebuff", Key: "freebuff|model-a", Model: "model-a", QuotaGroup: "unlimited"}, false)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer permit.Release()
	pending := g.pendingCandidates("freebuff")
	if len(pending) != 1 {
		t.Fatalf("pending = %+v, want one candidate", pending)
	}
	if pending[0].Model != "model-a" || pending[0].QuotaGroup != "unlimited" || pending[0].Key != "freebuff|model-a" {
		t.Fatalf("pending candidate = %+v", pending[0])
	}
}

func TestCreateGatePendingCandidatesBeforePermit(t *testing.T) {
	g := newCreateGate(CreateLimitConfig{MaxParallelGlobal: 3, MaxParallelPerKey: 3, MaxParallelPerModel: 3, MaxParallelPerGroup: 3})
	labels := createLabels{ChannelID: "freebuff", Key: "freebuff|model-a", Model: "model-a", QuotaGroup: "unlimited"}
	first, err := g.acquire(context.Background(), labels, false)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Release()
	second, err := g.acquire(context.Background(), labels, false)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer second.Release()

	pending := g.pendingCandidatesBefore("freebuff", second.id)
	if len(pending) != 1 {
		t.Fatalf("pending before second = %+v, want first only", pending)
	}
	if pending[0].Key != labels.Key {
		t.Fatalf("pending before second = %+v, want first key %q", pending[0], labels.Key)
	}
}
