package channelconfig

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func newTestRepo(t *testing.T) (*Repo, func()) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "channel-config.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	return NewRepo(db), func() { _ = db.Close() }
}

func TestUpsertRoundTrip(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	wait := true
	rec, err := repo.Upsert("freebuff", "FreeBuff", true, Config{
		MaxConcurrentPerSession: 2,
		WaitOnFull:              &wait,
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if rec.ID != "freebuff" || rec.Name != "FreeBuff" || !rec.IsActive {
		t.Fatalf("record identity = %+v", rec)
	}
	if rec.Config.MaxConcurrentPerSession != 2 || rec.Config.WaitOnFull == nil || !*rec.Config.WaitOnFull {
		t.Fatalf("config did not round-trip: %+v", rec.Config)
	}

	items, err := repo.ListMap()
	if err != nil {
		t.Fatalf("list map: %v", err)
	}
	if got := items["freebuff"].Config.MaxConcurrentPerSession; got != 2 {
		t.Fatalf("list config max = %d, want 2", got)
	}
}

func TestGetMissingReturnsSentinel(t *testing.T) {
	repo, closeDB := newTestRepo(t)
	defer closeDB()

	_, err := repo.Get("missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestValidateMaxConcurrentPerSession(t *testing.T) {
	for _, n := range []int{1, 2, MaxConcurrentPerSessionLimit} {
		if err := ValidateMaxConcurrentPerSession(n); err != nil {
			t.Fatalf("expected %d valid: %v", n, err)
		}
	}
	for _, n := range []int{0, -1, MaxConcurrentPerSessionLimit + 1} {
		if err := ValidateMaxConcurrentPerSession(n); err == nil {
			t.Fatalf("expected %d invalid", n)
		}
	}
}
