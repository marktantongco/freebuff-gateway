package systemlogs

import (
	"path/filepath"
	"testing"

	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func TestAppendListFiltersAndMetadata(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "systemlogs.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	if err := repo.Append(Entry{
		Level:     LevelInfo,
		Component: "freebuff_protocol_login",
		Event:     "job_started",
		Message:   "started",
		JobID:     "job-a",
		Metadata: map[string]any{
			"account_count": 2,
			"skip_refresh":  true,
		},
		CreatedAt: 100,
	}); err != nil {
		t.Fatalf("append info: %v", err)
	}
	if err := repo.Append(Entry{
		Level:     LevelError,
		Component: "proxy_pool",
		Event:     "probe_failed",
		Message:   "failed",
		JobID:     "job-b",
		CreatedAt: 200,
	}); err != nil {
		t.Fatalf("append error: %v", err)
	}

	rows, err := repo.List(Query{Component: "freebuff_protocol_login", JobID: "job-a", Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1: %+v", len(rows), rows)
	}
	row := rows[0]
	if row.Event != "job_started" || row.JobID != "job-a" || row.Metadata["skip_refresh"] != true {
		t.Fatalf("unexpected row: %+v", row)
	}
	if got := row.Metadata["account_count"]; got != float64(2) {
		t.Fatalf("account_count = %#v, want 2", got)
	}
}

func TestListReturnsEmptyArray(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "empty-systemlogs.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	rows, err := NewRepo(db).List(Query{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(rows))
	}
}
