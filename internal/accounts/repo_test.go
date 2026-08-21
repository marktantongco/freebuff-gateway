package accounts

import (
	"path/filepath"
	"testing"
	"time"

	"freebuff-reverse/internal/storage"
)

func newTestRepo(t *testing.T) (*Repo, func()) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	return NewRepo(db), func() { _ = db.Close() }
}

func TestCreateRoundTripWithQuota(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	start := QuotaBucketStart(time.Now(), "day")
	rec := &Record{
		ChannelID:        "demo",
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       100,
		QuotaPeriod:      "day",
		QuotaPeriodStart: start,
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuotaTotal != 100 || got.QuotaPeriod != "day" || got.QuotaUsed != 0 || got.QuotaPeriodStart != start {
		t.Fatalf("quota fields did not round-trip: %+v", got)
	}
}

func TestUpdateClearsQuota(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	rec := &Record{
		ChannelID:        "demo",
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       100,
		QuotaPeriod:      "day",
		QuotaUsed:        50,
		QuotaPeriodStart: QuotaBucketStart(time.Now(), "day"),
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	rec.QuotaTotal = 0
	rec.QuotaPeriod = ""
	rec.QuotaUsed = 0
	rec.QuotaPeriodStart = 0
	if err := repo.Update(rec); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuotaTotal != 0 || got.QuotaPeriod != "" || got.QuotaUsed != 0 || got.QuotaPeriodStart != 0 {
		t.Fatalf("quota fields not cleared: %+v", got)
	}
}

func TestIncrementQuotaUsed(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	rec := &Record{
		ChannelID:        "demo",
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       100,
		QuotaPeriod:      "day",
		QuotaPeriodStart: QuotaBucketStart(time.Now(), "day"),
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.IncrementQuotaUsed(rec.ID, 7); err != nil {
		t.Fatalf("increment: %v", err)
	}
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuotaUsed != 7 {
		t.Fatalf("expected quota_used=7, got %d", got.QuotaUsed)
	}
}

func TestRollQuota(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	rec := &Record{
		ChannelID:        "demo",
		Name:             "quota",
		Credential:       "secret",
		IsActive:         true,
		QuotaTotal:       100,
		QuotaPeriod:      "day",
		QuotaUsed:        50,
		QuotaPeriodStart: QuotaBucketStart(time.Now().AddDate(0, 0, -1), "day"),
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}

	start := QuotaBucketStart(time.Now(), "day")
	if err := repo.RollQuota(rec.ID, start); err != nil {
		t.Fatalf("roll: %v", err)
	}
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuotaUsed != 0 || got.QuotaPeriodStart != start {
		t.Fatalf("quota did not roll: %+v", got)
	}
}
