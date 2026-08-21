package main

import (
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"freebuff-reverse/internal/accounts"
	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/freebuffstate"
	"freebuff-reverse/internal/session"
	"freebuff-reverse/internal/storage"
)

type gatewayRestoreAdapter struct {
	policy channels.NoopSessionPolicy
	state  channels.State
	valid  bool

	mu    sync.Mutex
	calls []gatewayRestoreCall
}

type gatewayRestoreCall struct {
	accountID string
	key       string
	state     channels.State
}

func (a *gatewayRestoreAdapter) ID() string { return freebuffstate.ChannelID }

func (a *gatewayRestoreAdapter) InboundPathPrefix() string {
	return "/channels/" + freebuffstate.ChannelID
}

func (a *gatewayRestoreAdapter) SessionPolicy() channels.SessionPolicy { return a.policy }

func (a *gatewayRestoreAdapter) AuthFlow() channels.AuthFlow { return nil }

func (a *gatewayRestoreAdapter) PrepareOutbound(context.Context, *channels.Lease, *channels.InboundRequest) (*channels.OutboundRequest, error) {
	return nil, nil
}

func (a *gatewayRestoreAdapter) ClassifyResponse(int, http.Header, []byte) channels.ResponseClass {
	return channels.ClassOk
}

func (a *gatewayRestoreAdapter) RestoreSession(_ context.Context, acc channels.Account, key string, state channels.State, _ channels.Transport) (channels.State, bool, error) {
	a.mu.Lock()
	a.calls = append(a.calls, gatewayRestoreCall{
		accountID: acc.ID,
		key:       key,
		state:     cloneGatewayRestoreState(state),
	})
	a.mu.Unlock()
	if !a.valid {
		return nil, false, nil
	}
	return cloneGatewayRestoreState(a.state), true, nil
}

func (a *gatewayRestoreAdapter) callSnapshot() []gatewayRestoreCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]gatewayRestoreCall(nil), a.calls...)
}

type gatewayRestoreTransport struct{}

func (gatewayRestoreTransport) Do(context.Context, *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return nil, nil
}

func TestRestoreFreeBuffSessionsRegistersValidatedCandidate(t *testing.T) {
	ctx := context.Background()
	db, err := storage.Open(filepath.Join(t.TempDir(), "gateway-restore.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "freebuff",
		Credential: "secret-token",
		IsActive:   true,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}

	stateRepo := freebuffstate.NewRepo(db)
	expiresAt := time.Now().Add(time.Hour).Unix()
	if err := stateRepo.RecordSessionState(ctx, session.StateEvent{
		ChannelID:      freebuffstate.ChannelID,
		AccountID:      account.ID,
		LocalSessionID: "old-local-session",
		SelectionKey:   "freebuff|deepseek/deepseek-v4-pro",
		State: channels.State{
			"freebuff_instance_id":     "restore-instance",
			"freebuff_model":           "deepseek/deepseek-v4-pro",
			"freebuff_status":          "active",
			"freebuff_expires_at_unix": expiresAt,
		},
		CreatedAtUnix: time.Now().Unix(),
		ExpiresAtUnix: expiresAt,
	}); err != nil {
		t.Fatalf("record candidate: %v", err)
	}

	adapter := &gatewayRestoreAdapter{
		policy: channels.NoopSessionPolicy{TTL: time.Hour, MaxPerAccount: 1, MaxConcurrency: 3},
		valid:  true,
		state: channels.State{
			"freebuff_instance_id":     "restore-instance",
			"freebuff_model":           "deepseek/deepseek-v4-pro",
			"freebuff_status":          "active",
			"freebuff_expires_at_unix": expiresAt,
		},
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	pool := accounts.NewPool(accountRepo)
	manager := session.NewManager(registry, pool, gatewayRestoreTransport{}, session.Config{
		StateRecorder: stateRepo,
	})

	restoreFreeBuffSessions(ctx, freeBuffRestoreDeps{
		registry:    registry,
		accountRepo: accountRepo,
		sessions:    manager,
		transport:   gatewayRestoreTransport{},
		stateRepo:   stateRepo,
	})

	calls := adapter.callSnapshot()
	if len(calls) != 1 {
		t.Fatalf("restore calls = %d, want 1", len(calls))
	}
	if calls[0].accountID != account.ID {
		t.Fatalf("restore account id = %s, want %s", calls[0].accountID, account.ID)
	}
	if calls[0].key != "freebuff|deepseek/deepseek-v4-pro" {
		t.Fatalf("restore key = %q", calls[0].key)
	}
	if got := calls[0].state.String("freebuff_instance_id"); got != "restore-instance" {
		t.Fatalf("restore input instance = %q, want restore-instance", got)
	}

	snapshots := manager.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("session snapshots = %+v, want one restored session", snapshots)
	}
	if snapshots[0].AccountID != account.ID || snapshots[0].Key != "freebuff|deepseek/deepseek-v4-pro" {
		t.Fatalf("snapshot = %+v, want restored account/key", snapshots[0])
	}
	if snapshots[0].MaxConcurrency != 3 {
		t.Fatalf("max concurrency = %d, want 3", snapshots[0].MaxConcurrency)
	}
	if got := snapshots[0].Details["freebuff_model"]; got != "deepseek/deepseek-v4-pro" {
		t.Fatalf("snapshot model = %v, want deepseek/deepseek-v4-pro", got)
	}

	poolSnapshots := pool.Snapshot()
	if len(poolSnapshots) != 1 || poolSnapshots[0].AccountID != account.ID || poolSnapshots[0].SessionCount != 1 {
		t.Fatalf("pool snapshots = %+v, want one reserved restored slot", poolSnapshots)
	}

	upstream, err := stateRepo.GetUpstreamSession(ctx, "restore-instance")
	if err != nil {
		t.Fatalf("get upstream session: %v", err)
	}
	if upstream.LocalSessionID != snapshots[0].ID {
		t.Fatalf("upstream local session id = %q, want %q", upstream.LocalSessionID, snapshots[0].ID)
	}
}

func TestRestoreFreeBuffSessionsSkipsInactiveAccount(t *testing.T) {
	ctx := context.Background()
	db, err := storage.Open(filepath.Join(t.TempDir(), "gateway-restore-inactive.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	accountRepo := accounts.NewRepo(db)
	account := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       "freebuff",
		Credential: "secret-token",
		IsActive:   false,
	}
	if err := accountRepo.Create(account); err != nil {
		t.Fatalf("create account: %v", err)
	}

	stateRepo := freebuffstate.NewRepo(db)
	expiresAt := time.Now().Add(time.Hour).Unix()
	if err := stateRepo.RecordSessionState(ctx, session.StateEvent{
		ChannelID:      freebuffstate.ChannelID,
		AccountID:      account.ID,
		LocalSessionID: "old-local-session",
		SelectionKey:   "freebuff|deepseek/deepseek-v4-pro",
		State: channels.State{
			"freebuff_instance_id":     "restore-instance",
			"freebuff_model":           "deepseek/deepseek-v4-pro",
			"freebuff_status":          "active",
			"freebuff_expires_at_unix": expiresAt,
		},
		CreatedAtUnix: time.Now().Unix(),
		ExpiresAtUnix: expiresAt,
	}); err != nil {
		t.Fatalf("record candidate: %v", err)
	}

	adapter := &gatewayRestoreAdapter{
		policy: channels.NoopSessionPolicy{TTL: time.Hour, MaxPerAccount: 1, MaxConcurrency: 3},
		valid:  true,
	}
	registry := channels.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	pool := accounts.NewPool(accountRepo)
	manager := session.NewManager(registry, pool, gatewayRestoreTransport{}, session.Config{
		StateRecorder: stateRepo,
	})

	restoreFreeBuffSessions(ctx, freeBuffRestoreDeps{
		registry:    registry,
		accountRepo: accountRepo,
		sessions:    manager,
		transport:   gatewayRestoreTransport{},
		stateRepo:   stateRepo,
	})

	if calls := adapter.callSnapshot(); len(calls) != 0 {
		t.Fatalf("restore calls = %d, want 0 for inactive account", len(calls))
	}
	if snapshots := manager.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("session snapshots = %+v, want none", snapshots)
	}
	if snapshots := pool.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("pool snapshots = %+v, want none", snapshots)
	}
}

func cloneGatewayRestoreState(state channels.State) channels.State {
	if len(state) == 0 {
		return channels.State{}
	}
	out := make(channels.State, len(state))
	for key, value := range state {
		out[key] = value
	}
	return out
}
