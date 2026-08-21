package freebuffstate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func TestRecordSessionStatePersistsAccountQuotaAndSession(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuffstate.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	state := channels.State{
		stateInstanceID:     "inst-pro",
		stateModel:          "deepseek/deepseek-v4-pro",
		stateAccessTier:     "limited",
		stateAdmittedAtUnix: int64(1778913012),
		stateExpiresAtUnix:  int64(1778916612),
		stateRemainingMs:    int64(3599623),
		stateRawSessionJSON: `{"status":"active","instanceId":"inst-pro"}`,
		stateRateLimitsByModel: map[string]any{
			"deepseek/deepseek-v4-pro": map[string]any{
				"model":         "deepseek/deepseek-v4-pro",
				"limit":         5,
				"period":        "pacific_day",
				"resetTimeZone": "America/Los_Angeles",
				"resetAt":       "2026-05-16T07:00:00.000Z",
				"windowHours":   24,
				"recentCount":   2.5,
			},
			"moonshotai/kimi-k2.6": map[string]any{
				"model":         "moonshotai/kimi-k2.6",
				"limit":         5,
				"period":        "pacific_day",
				"resetTimeZone": "America/Los_Angeles",
				"resetAt":       "2026-05-16T07:00:00.000Z",
				"windowHours":   24,
				"recentCount":   2.5,
			},
		},
	}
	err = repo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      ChannelID,
		AccountID:      "acc-1",
		LocalSessionID: "sess-1",
		SelectionKey:   "freebuff|deepseek/deepseek-v4-pro",
		State:          state,
	})
	if err != nil {
		t.Fatalf("record session state: %v", err)
	}

	accountState, err := repo.GetAccountState(context.Background(), "acc-1")
	if err != nil {
		t.Fatalf("get account state: %v", err)
	}
	if accountState.AccessTier != "limited" || accountState.RawJSON == "" {
		t.Fatalf("account state = %+v, want access tier and raw json", accountState)
	}

	quotas, err := repo.ListQuotaSnapshots(context.Background(), "acc-1")
	if err != nil {
		t.Fatalf("list quota snapshots: %v", err)
	}
	if len(quotas) != 2 {
		t.Fatalf("quota snapshot count = %d, want 2: %+v", len(quotas), quotas)
	}
	var pro QuotaSnapshot
	for _, quota := range quotas {
		if quota.Model == "deepseek/deepseek-v4-pro" {
			pro = quota
		}
	}
	if pro.QuotaGroup != QuotaGroupPremiumShared || pro.LimitCount != 5 || pro.RecentCount != 2.5 {
		t.Fatalf("pro quota = %+v", pro)
	}
	if pro.ResetAt != parseProviderUnix("2026-05-16T07:00:00.000Z") {
		t.Fatalf("reset unix = %d", pro.ResetAt)
	}

	upstream, err := repo.GetUpstreamSession(context.Background(), "inst-pro")
	if err != nil {
		t.Fatalf("get upstream session: %v", err)
	}
	if upstream.LocalSessionID != "sess-1" || upstream.Model != "deepseek/deepseek-v4-pro" {
		t.Fatalf("upstream session = %+v", upstream)
	}
	if upstream.QuotaGroup != QuotaGroupPremiumShared || upstream.ExpiresAt != 1778916612 {
		t.Fatalf("upstream quota/expiry = %+v", upstream)
	}
}

func TestSchedulerFactsReconstructPremiumWindowHistory(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuffstate-scheduler-facts.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	now := time.Now()
	resetAt := now.Add(time.Hour).UTC().Format(time.RFC3339Nano)
	if err := repo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      ChannelID,
		AccountID:      "acc-1",
		LocalSessionID: "sess-1",
		State: channels.State{
			stateInstanceID:    "inst-pro",
			stateModel:         "deepseek/deepseek-v4-pro",
			stateStatus:        "active",
			stateExpiresAtUnix: now.Add(30 * time.Minute).Unix(),
			stateRateLimitsByModel: map[string]any{
				"deepseek/deepseek-v4-pro": map[string]any{
					"model":       "deepseek/deepseek-v4-pro",
					"limit":       5,
					"resetAt":     resetAt,
					"recentCount": 2.4,
				},
			},
		},
	}); err != nil {
		t.Fatalf("record session state: %v", err)
	}

	facts, err := repo.SchedulerFacts(context.Background(), "acc-1", now)
	if err != nil {
		t.Fatalf("scheduler facts: %v", err)
	}
	if facts[SchedulerFactPremiumWindowTouched] != true || facts[SchedulerFactEverPremiumTouched] != true {
		t.Fatalf("facts = %+v, want premium touched flags", facts)
	}
	if facts[SchedulerFactPremiumRemainingKnown] != true || facts[SchedulerFactPremiumRemaining] != 2 {
		t.Fatalf("facts = %+v, want known remaining quota of 2", facts)
	}
	if resetUnix, ok := facts[SchedulerFactPremiumResetAtUnix].(int64); !ok || resetUnix <= now.Unix() {
		t.Fatalf("facts = %+v, want future reset unix", facts)
	}
	if facts[SchedulerFactPremiumDepleted] == true {
		t.Fatalf("facts = %+v, did not expect depleted premium quota", facts)
	}
}

func TestRecordSessionStateIgnoresOtherChannels(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuffstate-ignore.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	err = repo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      "other",
		AccountID:      "acc-1",
		LocalSessionID: "sess-1",
		State: channels.State{
			stateInstanceID: "inst-1",
			stateModel:      "deepseek/deepseek-v4-pro",
		},
	})
	if err != nil {
		t.Fatalf("record other channel: %v", err)
	}
	if _, err := repo.GetAccountState(context.Background(), "acc-1"); err != ErrNotFound {
		t.Fatalf("get account state err = %v, want ErrNotFound", err)
	}
}

func TestRecordSessionStateMarksEndedSession(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuffstate-ended.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	err = repo.RecordSessionState(context.Background(), session.StateEvent{
		ChannelID:      ChannelID,
		AccountID:      "acc-1",
		LocalSessionID: "sess-1",
		State: channels.State{
			stateInstanceID: "inst-flash",
			stateModel:      "deepseek/deepseek-v4-flash",
			stateStatus:     "ended",
		},
	})
	if err != nil {
		t.Fatalf("record ended session state: %v", err)
	}
	upstream, err := repo.GetUpstreamSession(context.Background(), "inst-flash")
	if err != nil {
		t.Fatalf("get upstream session: %v", err)
	}
	if upstream.Status != "ended" || upstream.EndedAt == 0 {
		t.Fatalf("upstream session = %+v, want ended with ended_at", upstream)
	}
}

func TestListActiveUpstreamSessionsFiltersRestoreCandidates(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "freebuffstate-active.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	now := time.Now()
	events := []session.StateEvent{
		{
			ChannelID:      ChannelID,
			AccountID:      "acc-1",
			LocalSessionID: "sess-active",
			State: channels.State{
				stateInstanceID:     "inst-active",
				stateModel:          "deepseek/deepseek-v4-pro",
				stateStatus:         "active",
				stateExpiresAtUnix:  now.Add(time.Hour).Unix(),
				stateRemainingMs:    int64(time.Hour / time.Millisecond),
				stateRawSessionJSON: `{"status":"active","instanceId":"inst-active"}`,
			},
		},
		{
			ChannelID:      ChannelID,
			AccountID:      "acc-1",
			LocalSessionID: "sess-expired",
			State: channels.State{
				stateInstanceID:    "inst-expired",
				stateModel:         "deepseek/deepseek-v4-pro",
				stateStatus:        "active",
				stateExpiresAtUnix: now.Add(-time.Minute).Unix(),
			},
		},
		{
			ChannelID:      ChannelID,
			AccountID:      "acc-1",
			LocalSessionID: "sess-ended",
			State: channels.State{
				stateInstanceID: "inst-ended",
				stateModel:      "deepseek/deepseek-v4-pro",
				stateStatus:     "ended",
			},
		},
	}
	for _, event := range events {
		if err := repo.RecordSessionState(context.Background(), event); err != nil {
			t.Fatalf("record event %s: %v", event.LocalSessionID, err)
		}
	}

	active, err := repo.ListActiveUpstreamSessions(context.Background(), now)
	if err != nil {
		t.Fatalf("list active upstream sessions: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active sessions = %+v, want one candidate", active)
	}
	if active[0].InstanceID != "inst-active" || active[0].AccountID != "acc-1" || active[0].Model != "deepseek/deepseek-v4-pro" {
		t.Fatalf("active candidate = %+v", active[0])
	}
}
