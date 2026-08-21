package logs

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"freebuff-reverse/internal/storage"
)

func TestAppendListWithTokens(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "logs.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	repo.Append(Entry{
		ChannelID:       "demo",
		AccountID:       "acc",
		SessionID:       "sess",
		Method:          "POST",
		Path:            "/v1/chat",
		Stream:          true,
		SelectionKey:    "demo|model-a",
		Model:           "model-a",
		Status:          200,
		ResponseClass:   "ok",
		LatencyMS:       12,
		FirstResponseMS: 5,
		TokensIn:        3,
		TokensOut:       9,
		TokensKnown:     true,
		PhaseTimings: map[string]any{
			"session_acquire_ms": 2,
			"freebuff_ads_async": true,
		},
	})
	repo.flushOnce()

	rows, err := repo.List(Query{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].TokensIn != 3 || rows[0].TokensOut != 9 || !rows[0].TokensKnown {
		t.Fatalf("tokens did not round-trip: %+v", rows[0])
	}
	if !rows[0].Stream || rows[0].SelectionKey != "demo|model-a" || rows[0].Model != "model-a" {
		t.Fatalf("metadata did not round-trip: %+v", rows[0])
	}
	if rows[0].FirstResponseMS != 5 {
		t.Fatalf("first response latency did not round-trip: %+v", rows[0])
	}
	if got, ok := phaseInt64(rows[0].PhaseTimings["session_acquire_ms"]); !ok || got != 2 {
		t.Fatalf("phase timings did not round-trip: %+v", rows[0].PhaseTimings)
	}
	if rows[0].PhaseTimings["freebuff_ads_async"] != true {
		t.Fatalf("phase boolean did not round-trip: %+v", rows[0].PhaseTimings)
	}
}

func TestUsageAnalytics(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "analytics.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	for _, entry := range []Entry{
		{
			ChannelID:       "demo",
			AccountID:       "acc-1",
			Method:          "POST",
			Path:            "/v1/chat",
			Model:           "model-a",
			Status:          200,
			ResponseClass:   "ok",
			LatencyMS:       20,
			FirstResponseMS: 8,
			TokensIn:        3,
			TokensOut:       7,
			TokensKnown:     true,
			CreatedAt:       120,
		},
		{
			ChannelID:       "demo",
			AccountID:       "acc-1",
			Method:          "POST",
			Path:            "/v1/chat",
			Model:           "model-a",
			Status:          500,
			ResponseClass:   "retryable",
			LatencyMS:       40,
			FirstResponseMS: 12,
			TokensIn:        1,
			TokensOut:       2,
			TokensKnown:     true,
			CreatedAt:       180,
		},
		{
			ChannelID:       "demo",
			AccountID:       "acc-2",
			Method:          "POST",
			Path:            "/v1/messages",
			Stream:          true,
			Model:           "model-b",
			Status:          200,
			ResponseClass:   "ok",
			LatencyMS:       60,
			FirstResponseMS: 15,
			TokensIn:        5,
			TokensOut:       10,
			TokensKnown:     true,
			PhaseTimings:    map[string]any{"first_content_ms": 16},
			CreatedAt:       220,
		},
		{
			ChannelID:       "demo",
			AccountID:       "outside",
			Method:          "POST",
			Path:            "/v1/chat",
			Model:           "model-c",
			Status:          200,
			ResponseClass:   "ok",
			LatencyMS:       10,
			FirstResponseMS: 4,
			TokensIn:        100,
			TokensOut:       100,
			TokensKnown:     true,
			CreatedAt:       90,
		},
	} {
		repo.Append(entry)
	}
	repo.flushOnce()

	q := UsageQuery{
		Range: TimeRange{Label: "custom", StartAt: 100, EndAt: 300},
		Limit: 10,
	}
	summary, err := repo.UsageSummary(q)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if summary.TotalRequests != 3 || summary.SuccessCount != 2 || summary.FailureCount != 1 || summary.TotalTokens != 28 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	accounts, err := repo.AccountUsage(q)
	if err != nil {
		t.Fatalf("account usage: %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("account count = %d, want 2: %+v", len(accounts), accounts)
	}
	if accounts[0].AccountID != "acc-1" || accounts[0].TotalRequests != 2 || accounts[0].TopModel != "model-a" {
		t.Fatalf("unexpected first account aggregate: %+v", accounts[0])
	}

	events, err := repo.UsageEvents(UsageQuery{
		Range:  q.Range,
		Search: "model-b",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("usage events: %v", err)
	}
	if len(events) != 1 || events[0].AccountID != "acc-2" || !events[0].Stream {
		t.Fatalf("unexpected filtered events: %+v", events)
	}
	if events[0].FirstResponseMS != 15 {
		t.Fatalf("unexpected first response latency: %+v", events[0])
	}
	if got, ok := phaseInt64(events[0].PhaseTimings["first_content_ms"]); !ok || got != 16 {
		t.Fatalf("unexpected phase timings: %+v", events[0].PhaseTimings)
	}
}

func phaseInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
