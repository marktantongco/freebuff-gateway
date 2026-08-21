package storage

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenAppliesAdditiveMigrationsToExistingDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    name TEXT NOT NULL,
    credential_enc TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50,
    rpm_limit INTEGER,
    is_active INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE request_logs (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    account_id TEXT,
    session_id TEXT,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status INTEGER NOT NULL,
    response_class TEXT NOT NULL,
    latency_ms INTEGER NOT NULL,
    error TEXT,
    created_at INTEGER NOT NULL
);`)
	if err != nil {
		t.Fatalf("seed old schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	migrated, err := Open(path)
	if err != nil {
		t.Fatalf("open migrated db: %v", err)
	}
	defer migrated.Close()

	for _, tc := range []struct {
		table  string
		column string
	}{
		{table: "request_logs", column: "tokens_in"},
		{table: "request_logs", column: "tokens_out"},
		{table: "request_logs", column: "stream"},
		{table: "request_logs", column: "selection_key"},
		{table: "request_logs", column: "model"},
		{table: "request_logs", column: "first_response_ms"},
		{table: "request_logs", column: "phase_timings_json"},
		{table: "accounts", column: "quota_total"},
		{table: "accounts", column: "quota_period"},
		{table: "accounts", column: "quota_used"},
		{table: "accounts", column: "quota_period_start"},
		{table: "proxy_entries", column: "proxy_url_enc"},
		{table: "proxy_entries", column: "is_active"},
		{table: "freebuff_account_state", column: "last_sync_at"},
		{table: "freebuff_quota_snapshots", column: "reset_at"},
		{table: "freebuff_upstream_sessions", column: "expires_at"},
		{table: "auth_keys", column: "key_hash"},
		{table: "auth_keys", column: "key_prefix"},
	} {
		if !columnExists(t, migrated, tc.table, tc.column) {
			t.Fatalf("expected %s.%s to exist after migration", tc.table, tc.column)
		}
	}
}

func columnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query("SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		t.Fatalf("pragma %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("column rows: %v", err)
	}
	return false
}
