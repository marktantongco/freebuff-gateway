package storage

import (
	"database/sql"
	_ "embed"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", sqliteOpenDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: enable WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: set busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: enable fk: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: apply schema: %w", err)
	}
	if err := applyAdditiveMigrations(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: apply migrations: %w", err)
	}
	db.SetMaxOpenConns(0)
	return db, nil
}

func sqliteOpenDSN(dsn string) string {
	if dsn == ":memory:" || strings.Contains(dsn, "_pragma=busy_timeout") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "_pragma=busy_timeout%3d5000"
}

func applyAdditiveMigrations(db *sql.DB) error {
	migrations := []struct {
		table  string
		column string
		typ    string
	}{
		{table: "request_logs", column: "tokens_in", typ: "INTEGER"},
		{table: "request_logs", column: "tokens_out", typ: "INTEGER"},
		{table: "request_logs", column: "stream", typ: "INTEGER NOT NULL DEFAULT 0"},
		{table: "request_logs", column: "selection_key", typ: "TEXT"},
		{table: "request_logs", column: "model", typ: "TEXT"},
		{table: "request_logs", column: "first_response_ms", typ: "INTEGER"},
		{table: "request_logs", column: "phase_timings_json", typ: "TEXT"},
		{table: "accounts", column: "quota_total", typ: "INTEGER"},
		{table: "accounts", column: "quota_period", typ: "TEXT"},
		{table: "accounts", column: "quota_used", typ: "INTEGER NOT NULL DEFAULT 0"},
		{table: "accounts", column: "quota_period_start", typ: "INTEGER"},
		{table: "proxy_entries", column: "proxy_key", typ: "TEXT"},
		{table: "proxy_entries", column: "health_status", typ: "TEXT NOT NULL DEFAULT 'unknown'"},
		{table: "proxy_entries", column: "latency_ms", typ: "INTEGER"},
		{table: "proxy_entries", column: "last_checked_at", typ: "INTEGER"},
		{table: "proxy_entries", column: "last_error", typ: "TEXT"},
		{table: "proxy_entries", column: "failure_count", typ: "INTEGER NOT NULL DEFAULT 0"},
		{table: "proxy_entries", column: "exit_ip", typ: "TEXT"},
		{table: "proxy_entries", column: "country", typ: "TEXT"},
		{table: "proxy_entries", column: "region", typ: "TEXT"},
		{table: "proxy_entries", column: "city", typ: "TEXT"},
	}
	for _, m := range migrations {
		if err := addColumnIfMissing(db, m.table, m.column, m.typ); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_logs_model_time ON request_logs(model, created_at DESC)`); err != nil {
		return fmt.Errorf("request_logs.model index: %w", err)
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_proxy_entries_proxy_key_unique ON proxy_entries(proxy_key) WHERE proxy_key IS NOT NULL`); err != nil {
		return fmt.Errorf("proxy_entries.proxy_key index: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS system_logs (
		id            TEXT PRIMARY KEY,
		level         TEXT NOT NULL,
		component     TEXT NOT NULL,
		event         TEXT NOT NULL,
		message       TEXT NOT NULL,
		job_id        TEXT,
		metadata_json TEXT,
		created_at    INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("system_logs table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_system_logs_created ON system_logs(created_at DESC)`); err != nil {
		return fmt.Errorf("system_logs.created index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_system_logs_component_time ON system_logs(component, created_at DESC)`); err != nil {
		return fmt.Errorf("system_logs.component index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_system_logs_job_time ON system_logs(job_id, created_at DESC)`); err != nil {
		return fmt.Errorf("system_logs.job index: %w", err)
	}
	return nil
}

func addColumnIfMissing(db *sql.DB, table, column, typ string) error {
	rows, err := db.Query("SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		return fmt.Errorf("%s.%s inspect: %w", table, column, err)
	}
	found := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("%s.%s scan: %w", table, column, err)
		}
		if name == column {
			found = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("%s.%s rows: %w", table, column, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%s.%s close: %w", table, column, err)
	}
	if found {
		return nil
	}
	if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, typ)); err != nil {
		return fmt.Errorf("%s.%s add: %w", table, column, err)
	}
	return nil
}
