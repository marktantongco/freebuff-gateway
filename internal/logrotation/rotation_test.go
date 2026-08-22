package logrotation

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// Create tables matching the real schema
	db.Exec(`CREATE TABLE IF NOT EXISTS request_logs (
		id TEXT PRIMARY KEY, channel_id TEXT, account_id TEXT, session_id TEXT,
		method TEXT, path TEXT, stream INTEGER, selection_key TEXT, model TEXT,
		status INTEGER, response_class TEXT, latency_ms INTEGER,
		first_response_ms INTEGER, tokens_in INTEGER, tokens_out INTEGER,
		phase_timings_json TEXT, error TEXT, created_at INTEGER
	)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS system_logs (
		id TEXT PRIMARY KEY, level TEXT, component TEXT, event TEXT,
		message TEXT, job_id TEXT, metadata_json TEXT, created_at INTEGER
	)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS alerts (
		id TEXT PRIMARY KEY, name TEXT, severity TEXT, source TEXT,
		status TEXT, message TEXT, fingerprint TEXT,
		created_at INTEGER, resolved_at INTEGER
	)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS sessions (
		id TEXT PRIMARY KEY, channel_id TEXT, account_id TEXT,
		status TEXT, created_at INTEGER, expires_at INTEGER
	)`)
	return db
}

func seedRequestLogs(db *sql.DB, count int, daysAgo int, prefix string) {
	ts := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
	for i := 0; i < count; i++ {
		db.Exec(`INSERT INTO request_logs (id, channel_id, method, path, status, latency_ms, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			prefix+"_"+strconv.Itoa(i), "ch1", "POST", "/v1/chat/completions", 200, 100, ts)
	}
}

func seedSystemLogs(db *sql.DB, count int, daysAgo int, prefix string) {
	ts := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
	for i := 0; i < count; i++ {
		db.Exec(`INSERT INTO system_logs (id, level, component, event, message, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			prefix+"_"+strconv.Itoa(i), "info", "gateway", "request", "test", ts)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RequestLogRetention != 7*24*time.Hour {
		t.Errorf("expected 7 day retention, got %s", cfg.RequestLogRetention)
	}
	if cfg.SystemLogRetention != 30*24*time.Hour {
		t.Errorf("expected 30 day retention, got %s", cfg.SystemLogRetention)
	}
	if cfg.CleanupInterval != 1*time.Hour {
		t.Errorf("expected 1 hour interval, got %s", cfg.CleanupInterval)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", cfg.BatchSize)
	}
}

func TestDeleteExpired(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Seed old logs (60 days ago)
	seedRequestLogs(db, 50, 60, "old")
	seedSystemLogs(db, 30, 60, "old")

	// Seed recent logs (1 day ago)
	seedRequestLogs(db, 20, 1, "new")
	seedSystemLogs(db, 10, 1, "new")

	cfg := DefaultConfig()
	cfg.BatchSize = 10 // Small batch for testing
	r := NewRotator(db, cfg)

	// Force cleanup
	r.runCleanup()

	// Old request logs should be deleted
	var count int64
	db.QueryRow("SELECT COUNT(*) FROM request_logs").Scan(&count)
	if count != 20 {
		t.Errorf("expected 20 recent request logs, got %d", count)
	}

	// Old system logs should be deleted
	db.QueryRow("SELECT COUNT(*) FROM system_logs").Scan(&count)
	if count != 10 {
		t.Errorf("expected 10 recent system logs, got %d", count)
	}
}

func TestDeleteExpiredBatchSize(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Seed 25 old logs
	seedRequestLogs(db, 25, 60, "batch")

	cfg := DefaultConfig()
	cfg.BatchSize = 10 // Small batch
	r := NewRotator(db, cfg)

	// Force cleanup
	r.runCleanup()

	// All should be deleted after multiple batches
	var count int64
	db.QueryRow("SELECT COUNT(*) FROM request_logs").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 request logs after batch cleanup, got %d", count)
	}
}

func TestGetStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	seedRequestLogs(db, 10, 60, "s1")
	seedSystemLogs(db, 5, 60, "s2")
	seedRequestLogs(db, 5, 1, "s3")

	cfg := DefaultConfig()
	r := NewRotator(db, cfg)

	// Force cleanup and check stats
	stats := r.ForceCleanup()
	if stats.LastCleanup.IsZero() {
		t.Error("expected non-zero last cleanup time")
	}
	if stats.RequestLogsDeleted != 10 {
		t.Errorf("expected 10 request logs deleted, got %d", stats.RequestLogsDeleted)
	}
	if stats.SystemLogsDeleted != 5 {
		t.Errorf("expected 5 system logs deleted, got %d", stats.SystemLogsDeleted)
	}
	if stats.TotalDeleted != 15 {
		t.Errorf("expected 15 total deleted, got %d", stats.TotalDeleted)
	}
}

func TestTableSizes(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	seedRequestLogs(db, 100, 1, "t1")
	seedSystemLogs(db, 50, 1, "t2")

	cfg := DefaultConfig()
	r := NewRotator(db, cfg)
	r.updateTableSizes()

	stats := r.GetStats()
	if stats.TableSizes.RequestLogs != 100 {
		t.Errorf("expected 100 request_logs, got %d", stats.TableSizes.RequestLogs)
	}
	if stats.TableSizes.SystemLogs != 50 {
		t.Errorf("expected 50 system_logs, got %d", stats.TableSizes.SystemLogs)
	}
}

func TestStartAndStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.CleanupInterval = 100 * time.Millisecond
	r := NewRotator(db, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		r.Start(ctx)
		close(done)
	}()

	// Let it run for a bit
	time.Sleep(250 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("rotator did not stop in time")
	}
}

func TestNonExistentTable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.RequestLogRetention = 24 * time.Hour
	r := NewRotator(db, cfg)

	// Dropping a table that doesn't exist in our test schema should not panic
	// The alerts and sessions cleanup should be graceful
	r.runCleanup()

	// Should not have panicked
	stats := r.GetStats()
	if stats.LastCleanup.IsZero() {
		t.Error("expected cleanup to run")
	}
}

func TestZeroRetentionDeletesAll(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	seedRequestLogs(db, 20, 1, "zero")

	cfg := DefaultConfig()
	cfg.RequestLogRetention = 0 // Delete everything
	r := NewRotator(db, cfg)

	r.runCleanup()

	var count int64
	db.QueryRow("SELECT COUNT(*) FROM request_logs").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 request logs with zero retention, got %d", count)
	}
}
