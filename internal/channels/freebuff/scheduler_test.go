package freebuff

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
)

func TestPremiumSchedulerPrefersCurrentWindowTouchedAccounts(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.5,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	state := channels.State{}
	s.observeSession("acc-1", "deepseek/deepseek-v4-pro", schedulerPremiumSession("deepseek/deepseek-v4-pro", 5, 2, now.Add(time.Hour)), state)

	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-2", 100),
			schedulerCandidate("acc-1", 50),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if len(decision.PreferredAccountIDs) < 2 || decision.PreferredAccountIDs[0] != "acc-1" {
		t.Fatalf("preferred accounts = %+v, want touched account first", decision.PreferredAccountIDs)
	}
	if state[statePremiumTouched] != true || state[statePremiumRemaining] != 3 {
		t.Fatalf("decorated state = %+v, want touched premium remaining", state)
	}
}

func TestPremiumSchedulerMergesProviderFactsIntoOrderingAndSnapshot(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.5,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	touched := schedulerCandidate("acc-touched", 10)
	touched.ProviderFacts = map[string]any{
		freebuffstate.SchedulerFactPremiumWindowTouched:  true,
		freebuffstate.SchedulerFactEverPremiumTouched:    true,
		freebuffstate.SchedulerFactPremiumRemainingKnown: true,
		freebuffstate.SchedulerFactPremiumRemaining:      2,
		freebuffstate.SchedulerFactPremiumResetAtUnix:    now.Add(time.Hour).Unix(),
	}

	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-fresh", 100),
			touched,
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if len(decision.PreferredAccountIDs) < 2 || decision.PreferredAccountIDs[0] != "acc-touched" {
		t.Fatalf("preferred accounts = %+v, want persisted touched account first", decision.PreferredAccountIDs)
	}

	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-fresh", 100),
			touched,
		},
		Now: now,
	})
	var touchedView schedulerAccountView
	for _, account := range snapshot.Accounts {
		if account.AccountID == "acc-touched" {
			touchedView = account
			break
		}
	}
	if touchedView.AccountID == "" {
		t.Fatalf("snapshot accounts = %+v, want touched account", snapshot.Accounts)
	}
	if !touchedView.PremiumWindowTouched || touchedView.PremiumRemaining == nil || *touchedView.PremiumRemaining != 2 {
		t.Fatalf("touched account = %+v, want persisted premium facts", touchedView)
	}
	if touchedView.Score == nil || touchedView.ScoreStatus != "available" || touchedView.ScoreBand != "high" {
		t.Fatalf("touched score = %v status=%q band=%q, want available high score", touchedView.Score, touchedView.ScoreStatus, touchedView.ScoreBand)
	}
	if !containsString(touchedView.ScoreReasons, "current_window_premium_history") ||
		!containsString(touchedView.ScoreReasons, "known_quota_snapshot") ||
		!containsString(touchedView.ScoreReasons, "smallest_positive_remaining_premium_quota") {
		t.Fatalf("score reasons = %+v, missing persisted premium scoring components", touchedView.ScoreReasons)
	}
}

func TestPremiumSchedulerRejectsKnownDepletedPremiumCandidates(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            1,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	depleted := schedulerCandidate("acc-depleted", 100)
	depleted.ProviderFacts = map[string]any{
		freebuffstate.SchedulerFactPremiumWindowTouched:  true,
		freebuffstate.SchedulerFactEverPremiumTouched:    true,
		freebuffstate.SchedulerFactPremiumRemainingKnown: true,
		freebuffstate.SchedulerFactPremiumRemaining:      0,
		freebuffstate.SchedulerFactPremiumDepleted:       true,
		freebuffstate.SchedulerFactPremiumResetAtUnix:    now.Add(time.Hour).Unix(),
	}

	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts:     []channels.AccountCandidate{depleted},
		Now:          now,
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if decision.Action != channels.SessionScheduleReject || decision.Reason != "premium_no_quota_capacity" {
		t.Fatalf("decision = %+v, want reject for known depleted premium account", decision)
	}
}

func TestSchedulerSnapshotDistinguishesResetPremiumHistoryFromFresh(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{PremiumCoreRatio: 1, PremiumMaxRatio: 1, PremiumBurstQueueThreshold: 1})
	now := time.Now()
	reset := schedulerCandidate("acc-reset", 100)
	reset.ProviderFacts = map[string]any{
		freebuffstate.SchedulerFactEverPremiumTouched: true,
	}

	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{
		Accounts: []channels.AccountCandidate{reset},
		Now:      now,
	})
	if len(snapshot.Accounts) != 1 {
		t.Fatalf("snapshot accounts = %+v, want one account", snapshot.Accounts)
	}
	account := snapshot.Accounts[0]
	if account.State != "premium_reset" {
		t.Fatalf("account state = %q, want premium_reset", account.State)
	}
	if containsString(account.ScoreReasons, "fresh_account_not_yet_burned") {
		t.Fatalf("score reasons = %+v, reset history should not be labeled fresh", account.ScoreReasons)
	}
	if !containsString(account.ScoreReasons, "outside_window_premium_history") {
		t.Fatalf("score reasons = %+v, want outside_window_premium_history", account.ScoreReasons)
	}
}

func TestPremiumSchedulerBurstStartsAfterQueuePressure(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.25,
		PremiumMaxRatio:             0.75,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  2,
		PremiumQueueTimeout:         80 * time.Millisecond,
	})
	now := time.Now()
	req := channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
			schedulerCandidate("acc-2", 90),
			schedulerCandidate("acc-3", 80),
			schedulerCandidate("acc-4", 70),
		},
		Sessions: []channels.SessionCandidate{
			schedulerSession("sess-1", "acc-1", "deepseek/deepseek-v4-pro", now.Add(time.Hour)),
		},
		Now: now,
	}

	first, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("first schedule: %v", err)
	}
	if first.Action != channels.SessionScheduleWait || first.Reason != "premium_queue_reuse" {
		t.Fatalf("first decision = %+v, want queue reuse wait", first)
	}

	start := time.Now()
	second, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("second schedule: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("second schedule waited %s, want immediate burst after queue pressure", elapsed)
	}
	if second.Reason != "premium_burst_queue_threshold" {
		t.Fatalf("second reason = %q, want premium_burst_queue_threshold", second.Reason)
	}
	if second.Action != channels.SessionScheduleCreate {
		t.Fatalf("second action = %q, want create", second.Action)
	}
	if first.Finish == nil {
		t.Fatalf("first finish is nil, want queue release callback")
	}
	first.Finish()
}

func TestPremiumSchedulerQueueDepthIsPerModel(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:           0.25,
		PremiumMaxRatio:            0.75,
		PremiumBurstQueueThreshold: 2,
		PremiumQueueTimeout:        80 * time.Millisecond,
	})
	now := time.Now()
	req := channels.SessionScheduleRequest{
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
			schedulerCandidate("acc-2", 90),
			schedulerCandidate("acc-3", 80),
			schedulerCandidate("acc-4", 70),
		},
		Sessions: []channels.SessionCandidate{
			schedulerSession("sess-1", "acc-1", "deepseek/deepseek-v4-pro", now.Add(time.Hour)),
		},
		Now: now,
	}

	req.SelectionKey = "freebuff|deepseek/deepseek-v4-pro"
	firstPro, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("first pro schedule: %v", err)
	}
	if firstPro.Action != channels.SessionScheduleWait {
		t.Fatalf("first pro decision = %+v, want wait", firstPro)
	}

	req.SelectionKey = "freebuff|moonshotai/kimi-k2.6"
	firstKimi, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("first kimi schedule: %v", err)
	}
	if firstKimi.Action != channels.SessionScheduleWait {
		t.Fatalf("first kimi decision = %+v, want independent per-model wait", firstKimi)
	}

	req.SelectionKey = "freebuff|deepseek/deepseek-v4-pro"
	secondPro, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("second pro schedule: %v", err)
	}
	if secondPro.Action != channels.SessionScheduleCreate || secondPro.Reason != "premium_burst_queue_threshold" {
		t.Fatalf("second pro decision = %+v, want burst for pro only", secondPro)
	}

	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{Now: now})
	if snapshot.Queue.PremiumDepth != 2 {
		t.Fatalf("premium queue depth = %d, want 2", snapshot.Queue.PremiumDepth)
	}
	if got := snapshot.Queue.DepthByModel["deepseek/deepseek-v4-pro"]; got != 1 {
		t.Fatalf("pro queue depth = %d, want 1", got)
	}
	if got := snapshot.Queue.DepthByModel["moonshotai/kimi-k2.6"]; got != 1 {
		t.Fatalf("kimi queue depth = %d, want 1", got)
	}

	firstPro.Finish()
	firstKimi.Finish()
}

func TestPremiumSchedulerRejectsAtMaxCapacity(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            1,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
		PremiumQueueTimeout:         time.Millisecond,
	})
	now := time.Now()
	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
		},
		Sessions: []channels.SessionCandidate{
			schedulerSession("sess-1", "acc-1", "deepseek/deepseek-v4-pro", now.Add(time.Hour)),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if decision.Action != channels.SessionScheduleWait || decision.WaitTimeout != time.Millisecond {
		t.Fatalf("decision = %+v, want bounded wait at max capacity", decision)
	}
	if decision.Finish == nil {
		t.Fatalf("finish is nil, want queue release callback")
	}
	decision.Finish()
}

func TestPremiumSchedulerPromotesDepletedAccountAfterReset(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            1,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	s.markPremiumDepleted("acc-1", "deepseek/deepseek-v4-pro", schedulerPremiumSession("deepseek/deepseek-v4-pro", 5, 5, now.Add(-time.Minute)))

	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule after reset: %v", err)
	}
	if len(decision.PreferredAccountIDs) != 1 || decision.PreferredAccountIDs[0] != "acc-1" {
		t.Fatalf("preferred accounts = %+v, want reset account promoted", decision.PreferredAccountIDs)
	}
	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{
		Accounts: []channels.AccountCandidate{schedulerCandidate("acc-1", 100)},
		Now:      now,
	})
	if len(snapshot.Accounts) != 1 || snapshot.Accounts[0].PremiumDepleted {
		t.Fatalf("snapshot account = %+v, want promoted out of depleted state", snapshot.Accounts)
	}
}

func TestUnlimitedSchedulerBorrowsFreshPremiumBeforeTouchedPremium(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.5,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	s.observeSession("acc-1", "deepseek/deepseek-v4-pro", schedulerPremiumSession("deepseek/deepseek-v4-pro", 5, 1, now.Add(time.Hour)), channels.State{})

	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: "freebuff|deepseek/deepseek-v4-flash",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
			schedulerCandidate("acc-2", 50),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule unlimited: %v", err)
	}
	if len(decision.PreferredAccountIDs) < 2 || decision.PreferredAccountIDs[0] != "acc-2" {
		t.Fatalf("preferred accounts = %+v, want fresh premium account first", decision.PreferredAccountIDs)
	}
}

func TestSchedulerSnapshotSmallPoolWarningAndSessionLifetime(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            1,
		PremiumMaxRatio:             1,
		UnlimitedReserveRatio:       0,
		UnlimitedMinReserveAccounts: 0,
		PremiumBurstQueueThreshold:  1,
	})
	now := time.Now()
	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
		},
		Sessions: []channels.SessionCandidate{
			schedulerSession("sess-1", "acc-1", "deepseek/deepseek-v4-pro", now.Add(time.Hour)),
		},
		Now: now,
	})
	if snapshot.Limits.MaxPremiumAccounts != 1 || len(snapshot.Warnings) != 1 || snapshot.Warnings[0] != "no_unlimited_reserve" {
		t.Fatalf("snapshot limits/warnings = %+v / %+v", snapshot.Limits, snapshot.Warnings)
	}
	if len(snapshot.Sessions) != 1 || snapshot.Sessions[0].RemainingLifeMS <= 0 {
		t.Fatalf("snapshot sessions = %+v, want positive remaining life", snapshot.Sessions)
	}
}

func TestSchedulerSnapshotIncludesScoresAndSessionSelectionFields(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:           1,
		PremiumMaxRatio:            1,
		PremiumBurstQueueThreshold: 1,
	})
	now := time.Now()
	state := channels.State{stateModel: "deepseek/deepseek-v4-pro"}
	s.observeSession("acc-1", "deepseek/deepseek-v4-pro", schedulerPremiumSession("deepseek/deepseek-v4-pro", 5, 3, now.Add(time.Hour)), state)
	s.mu.Lock()
	h := s.history["acc-1"]
	h.LastReason = "premium_core_create"
	h.LastPool = "premium_capable"
	s.history["acc-1"] = h
	s.mu.Unlock()
	s.decorateSessionState("acc-1", state)

	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{
		Accounts: []channels.AccountCandidate{schedulerCandidate("acc-1", 100)},
		Sessions: []channels.SessionCandidate{
			{
				ID:             "sess-1",
				ChannelID:      ID,
				AccountID:      "acc-1",
				Key:            "freebuff|deepseek/deepseek-v4-pro",
				State:          state,
				ExpiresAtUnix:  now.Add(time.Hour).Unix(),
				InFlight:       0,
				MaxConcurrency: 1,
				Healthy:        true,
			},
		},
		Now: now,
	})

	if len(snapshot.Accounts) != 1 {
		t.Fatalf("snapshot accounts = %+v, want one", snapshot.Accounts)
	}
	account := snapshot.Accounts[0]
	if account.Score == nil || *account.Score < 80 {
		t.Fatalf("account score = %v, want high premium burn-down score", account.Score)
	}
	if account.ScoreStatus != "available" || account.ScoreBand != "high" {
		t.Fatalf("score status/band = %q/%q, want available/high", account.ScoreStatus, account.ScoreBand)
	}
	if !containsString(account.ScoreReasons, "current_window_premium_history") ||
		!containsString(account.ScoreReasons, "known_quota_snapshot") ||
		!containsString(account.ScoreReasons, "smallest_positive_remaining_premium_quota") {
		t.Fatalf("score reasons = %+v, missing premium scoring components", account.ScoreReasons)
	}

	if len(snapshot.Sessions) != 1 {
		t.Fatalf("snapshot sessions = %+v, want one", snapshot.Sessions)
	}
	session := snapshot.Sessions[0]
	if session.Pool != "premium_capable" || session.AccountState != "premium_ready" {
		t.Fatalf("session pool/state = %q/%q, want premium_capable/premium_ready", session.Pool, session.AccountState)
	}
	if session.SelectionReason != "premium_core_create" {
		t.Fatalf("session selection reason = %q, want premium_core_create", session.SelectionReason)
	}
	if session.SelectionScore == nil || *session.SelectionScore != *account.Score {
		t.Fatalf("session selection score = %v, account score = %v", session.SelectionScore, account.Score)
	}
	if session.Reclaimable {
		t.Fatalf("premium session marked reclaimable: %+v", session)
	}
}

func TestSchedulerSnapshotEmptyWarningsEncodeAsArrays(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.35,
		PremiumMaxRatio:             0.7,
		UnlimitedReserveRatio:       0.25,
		UnlimitedMinReserveAccounts: 1,
		PremiumBurstQueueThreshold:  1,
	})
	snapshot := s.Snapshot(context.Background(), channels.SchedulerSnapshotRequest{Now: time.Now()})
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	text := string(body)
	if strings.Contains(text, `"warnings":null`) {
		t.Fatalf("snapshot encoded null warnings: %s", text)
	}
	if !strings.Contains(text, `"warnings":[]`) {
		t.Fatalf("snapshot did not encode empty warnings arrays: %s", text)
	}
}

func TestPremiumSchedulerCountsPendingCreatesAgainstPremiumLimit(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.35,
		PremiumMaxRatio:             0.70,
		UnlimitedReserveRatio:       0.25,
		UnlimitedMinReserveAccounts: 1,
		PremiumBurstQueueThreshold:  1,
		ModelMaxAccountRatio:        1,
		ModelBurstAccountRatio:      1,
	})
	now := time.Now()
	decision, err := s.Schedule(context.Background(), channels.SessionScheduleRequest{
		SelectionKey: keyPrefix + "deepseek/deepseek-v4-pro",
		Accounts: []channels.AccountCandidate{
			schedulerCandidate("acc-1", 100),
			schedulerCandidate("acc-2", 90),
			schedulerCandidate("acc-3", 80),
		},
		Sessions: []channels.SessionCandidate{
			schedulerSession("s1", "acc-1", "deepseek/deepseek-v4-pro", now.Add(time.Hour)),
		},
		PendingCreates: []channels.SessionCreateCandidate{
			{ChannelID: ID, Key: keyPrefix + "moonshotai/kimi-k2.6", Model: "moonshotai/kimi-k2.6", QuotaGroup: QuotaGroupPremiumShared, StartedAtUnix: now.Unix()},
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if decision.Action != channels.SessionScheduleWait || !strings.Contains(decision.Reason, "premium_capacity_limited") {
		t.Fatalf("decision = %+v, want premium capacity wait", decision)
	}
	if decision.Finish == nil {
		t.Fatal("wait decision missing finish callback")
	}
}

func TestSchedulerCapsOneHotUnlimitedModelButAllowsAnother(t *testing.T) {
	s := newPremiumScheduler(SchedulerConfig{
		PremiumCoreRatio:            0.35,
		PremiumMaxRatio:             0.70,
		UnlimitedReserveRatio:       0.25,
		UnlimitedMinReserveAccounts: 1,
		PremiumBurstQueueThreshold:  1,
		ModelMaxAccountRatio:        0.10,
		ModelBurstAccountRatio:      0.10,
	})
	now := time.Now()
	accounts := make([]channels.AccountCandidate, 0, 10)
	for i := 0; i < 10; i++ {
		accounts = append(accounts, schedulerCandidate("acc-"+string(rune('a'+i)), 100-i))
	}
	req := channels.SessionScheduleRequest{
		SelectionKey: keyPrefix + "deepseek/deepseek-v4-flash",
		Accounts:     accounts,
		Sessions: []channels.SessionCandidate{
			schedulerSession("s1", "acc-a", "deepseek/deepseek-v4-flash", now.Add(time.Hour)),
			schedulerSession("s2", "acc-b", "deepseek/deepseek-v4-flash", now.Add(time.Hour)),
			schedulerSession("s3", "acc-c", "deepseek/deepseek-v4-flash", now.Add(time.Hour)),
			schedulerSession("s4", "acc-d", "deepseek/deepseek-v4-flash", now.Add(time.Hour)),
			schedulerSession("s5", "acc-e", "deepseek/deepseek-v4-flash", now.Add(time.Hour)),
		},
		PendingCreates: []channels.SessionCreateCandidate{
			{ChannelID: ID, Key: keyPrefix + "deepseek/deepseek-v4-flash", Model: "deepseek/deepseek-v4-flash", QuotaGroup: QuotaGroupUnlimited, StartedAtUnix: now.Unix()},
		},
		Now: now,
	}
	decision, err := s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("schedule hot model: %v", err)
	}
	if decision.Action != channels.SessionScheduleWait || !strings.Contains(decision.Reason, "model_burst_capacity_limited") {
		t.Fatalf("hot model decision = %+v, want model cap wait", decision)
	}

	req.SelectionKey = keyPrefix + "minimax/minimax-m2.7"
	decision, err = s.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("schedule other model: %v", err)
	}
	if decision.Action != channels.SessionScheduleCreate {
		t.Fatalf("other model decision = %+v, want create", decision)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func schedulerCandidate(id string, priority int) channels.AccountCandidate {
	return channels.AccountCandidate{
		Account:        channels.Account{ID: id, ChannelID: ID, Name: id},
		Priority:       priority,
		Active:         true,
		Eligible:       true,
		MaxSessions:    1,
		QuotaAvailable: true,
	}
}

func schedulerSession(id, accountID, model string, expiresAt time.Time) channels.SessionCandidate {
	return channels.SessionCandidate{
		ID:             id,
		ChannelID:      ID,
		AccountID:      accountID,
		Key:            keyPrefix + model,
		State:          channels.State{stateModel: model},
		ExpiresAtUnix:  expiresAt.Unix(),
		InFlight:       1,
		MaxConcurrency: 1,
		Healthy:        true,
	}
}

func schedulerPremiumSession(model string, limit int, recent float64, resetAt time.Time) upstreamSession {
	return upstreamSession{
		Status:     "active",
		InstanceID: "inst-" + model,
		Model:      model,
		RateLimitsByModel: map[string]upstreamRateLimit{
			model: {
				Model:       model,
				Limit:       limit,
				RecentCount: recent,
				ResetAt:     resetAt.UTC().Format(time.RFC3339Nano),
			},
		},
	}
}
