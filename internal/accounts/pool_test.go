package accounts

import (
	"errors"
	"testing"
	"time"
)

func createQuotaRecord(t *testing.T, repo *Repo, used, total int64, start int64) *Record {
	t.Helper()
	rec := &Record{
		ChannelID:        "demo",
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       total,
		QuotaPeriod:      "day",
		QuotaUsed:        used,
		QuotaPeriodStart: start,
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	return rec
}

func createPoolRecord(t *testing.T, repo *Repo, name string, priority int) *Record {
	t.Helper()
	rec := &Record{
		ChannelID:  "demo",
		Name:       name,
		Credential: "secret",
		Priority:   priority,
		IsActive:   true,
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create pool record: %v", err)
	}
	return rec
}

func TestReserveSlotExcludingSkipsAccount(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	first := createPoolRecord(t, repo, "first", 100)
	second := createPoolRecord(t, repo, "second", 50)

	pool := NewPool(repo)
	acc, release, err := pool.ReserveSlotExcluding("demo", 1, map[string]struct{}{first.ID: {}})
	if err != nil {
		t.Fatalf("reserve excluding: %v", err)
	}
	defer release()
	if acc.ID != second.ID {
		t.Fatalf("reserved account = %s, want %s", acc.ID, second.ID)
	}
}

func TestReserveSlotPreferredOrderOverridesPriority(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	first := createPoolRecord(t, repo, "first", 100)
	second := createPoolRecord(t, repo, "second", 50)

	pool := NewPool(repo)
	acc, release, err := pool.ReserveSlotExcludingPreferred("demo", 1, nil, []string{second.ID, first.ID})
	if err != nil {
		t.Fatalf("reserve preferred: %v", err)
	}
	defer release()
	if acc.ID != second.ID {
		t.Fatalf("reserved account = %s, want preferred second %s", acc.ID, second.ID)
	}
}

func TestReserveAccountSlotUsesExactAccountAndReleases(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	_ = createPoolRecord(t, repo, "first", 100)
	second := createPoolRecord(t, repo, "second", 50)

	pool := NewPool(repo)
	acc, release, err := pool.ReserveAccountSlot("demo", second.ID, 1)
	if err != nil {
		t.Fatalf("reserve account slot: %v", err)
	}
	if acc.ID != second.ID {
		t.Fatalf("reserved account = %s, want exact second %s", acc.ID, second.ID)
	}
	snapshots := pool.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want one runtime row", snapshots)
	}
	counts := map[string]int{}
	for _, snapshot := range snapshots {
		counts[snapshot.AccountID] = snapshot.SessionCount
	}
	if counts[second.ID] != 1 {
		t.Fatalf("session counts = %+v, want second 1", counts)
	}
	release()
	counts = map[string]int{}
	for _, snapshot := range pool.Snapshot() {
		counts[snapshot.AccountID] = snapshot.SessionCount
	}
	if counts[second.ID] != 0 {
		t.Fatalf("second session count after release = %d, want 0", counts[second.ID])
	}
}

func TestReserveAccountSlotRejectsWrongChannelAndFullAccount(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	rec := createPoolRecord(t, repo, "first", 100)

	pool := NewPool(repo)
	if _, _, err := pool.ReserveAccountSlot("other", rec.ID, 1); !errors.Is(err, ErrNoEligibleAccount) {
		t.Fatalf("wrong channel reserve err = %v, want ErrNoEligibleAccount", err)
	}
	_, release, err := pool.ReserveAccountSlot("demo", rec.ID, 1)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	defer release()
	if _, _, err := pool.ReserveAccountSlot("demo", rec.ID, 1); !errors.Is(err, ErrNoEligibleAccount) {
		t.Fatalf("full account reserve err = %v, want ErrNoEligibleAccount", err)
	}
}

func TestSlotCandidatesExposeBlockedReasons(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	blocked := createQuotaRecord(t, repo, 1, 1, QuotaBucketStart(time.Now(), "day"))
	active := createPoolRecord(t, repo, "active", 50)

	pool := NewPool(repo)
	candidates, err := pool.SlotCandidates("demo", 1, nil)
	if err != nil {
		t.Fatalf("slot candidates: %v", err)
	}
	byID := map[string]string{}
	for _, candidate := range candidates {
		byID[candidate.Account.ID] = candidate.BlockedReason
	}
	if byID[blocked.ID] != "quota_exceeded" {
		t.Fatalf("blocked reason for quota account = %q", byID[blocked.ID])
	}
	if byID[active.ID] != "" {
		t.Fatalf("active account blocked reason = %q", byID[active.ID])
	}
}

func TestReserveSlotQuotaBlock(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	createQuotaRecord(t, repo, 5, 5, QuotaBucketStart(time.Now(), "day"))

	pool := NewPool(repo)
	_, _, err := pool.ReserveSlot("demo", 1)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestReserveSlotQuotaRollover(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	rec := createQuotaRecord(t, repo, 5, 5, QuotaBucketStart(time.Now().AddDate(0, 0, -2), "day"))

	pool := NewPool(repo)
	acc, release, err := pool.ReserveSlot("demo", 1)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer release()
	if acc.ID != rec.ID {
		t.Fatalf("expected account %s, got %s", rec.ID, acc.ID)
	}

	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuotaUsed != 0 || got.QuotaPeriodStart == rec.QuotaPeriodStart {
		t.Fatalf("expected rolled quota, got %+v", got)
	}
}

func TestReserveSlotErrQuotaExceeded(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()
	createQuotaRecord(t, repo, 1, 1, QuotaBucketStart(time.Now(), "day"))

	pool := NewPool(repo)
	_, _, err := pool.ReserveSlot("demo", 10)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}
