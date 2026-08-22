package logrotation

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Config controls log retention and cleanup behavior.
type Config struct {
	// RequestLogRetention is how long to keep HTTP request logs.
	RequestLogRetention time.Duration `json:"request_log_retention"`

	// SystemLogRetention is how long to keep system event logs.
	SystemLogRetention time.Duration `json:"system_log_retention"`

	// AlertRetention is how long to keep resolved alerts.
	AlertRetention time.Duration `json:"alert_retention"`

	// SessionRetention is how long to keep expired sessions.
	SessionRetention time.Duration `json:"session_retention"`

	// CleanupInterval is how often to run cleanup.
	CleanupInterval time.Duration `json:"cleanup_interval"`

	// BatchSize is the max rows to delete per DELETE query (prevents long locks).
	BatchSize int `json:"batch_size"`
}

// DefaultConfig returns sensible defaults for log rotation.
func DefaultConfig() Config {
	return Config{
		RequestLogRetention: 7 * 24 * time.Hour,  // 7 days
		SystemLogRetention:  30 * 24 * time.Hour,  // 30 days
		AlertRetention:      90 * 24 * time.Hour,  // 90 days
		SessionRetention:    24 * time.Hour,        // 24 hours
		CleanupInterval:     1 * time.Hour,         // every hour
		BatchSize:           1000,
	}
}

// Stats reports what the last cleanup pass deleted.
type Stats struct {
	RequestLogsDeleted int64      `json:"request_logs_deleted"`
	SystemLogsDeleted  int64      `json:"system_logs_deleted"`
	AlertsDeleted      int64      `json:"alerts_deleted"`
	SessionsDeleted    int64      `json:"sessions_deleted"`
	TotalDeleted       int64      `json:"total_deleted"`
	LastCleanup        time.Time  `json:"last_cleanup"`
	NextCleanup        time.Time  `json:"next_cleanup"`
	TableSizes         TableSizes `json:"table_sizes"`
}

// TableSizes holds row counts for each table.
type TableSizes struct {
	RequestLogs int64 `json:"request_logs"`
	SystemLogs  int64 `json:"system_logs"`
	Alerts      int64 `json:"alerts"`
	Sessions    int64 `json:"sessions"`
}

// Rotator performs periodic log cleanup.
type Rotator struct {
	db     *sql.DB
	config Config
	stats  Stats
}

// NewRotator creates a new log rotator.
func NewRotator(db *sql.DB, config Config) *Rotator {
	return &Rotator{
		db:     db,
		config: config,
	}
}

// Start begins the periodic cleanup loop. Call cancel to stop it.
func (r *Rotator) Start(ctx context.Context) {
	ticker := time.NewTicker(r.config.CleanupInterval)
	defer ticker.Stop()

	log.Printf("logrotation: started (interval=%s, request_retention=%s, system_retention=%s)",
		r.config.CleanupInterval, r.config.RequestLogRetention, r.config.SystemLogRetention)

	// Run once immediately on startup
	r.runCleanup()

	for {
		select {
		case <-ticker.C:
			r.runCleanup()
		case <-ctx.Done():
			log.Printf("logrotation: stopped")
			return
		}
	}
}

// GetStats returns the latest cleanup statistics.
func (r *Rotator) GetStats() Stats {
	return r.stats
}

// ForceCleanup runs an immediate cleanup and returns what was deleted.
func (r *Rotator) ForceCleanup() Stats {
	r.runCleanup()
	return r.stats
}

func (r *Rotator) runCleanup() {
	start := time.Now()
	var totalDeleted int64

	// Clean request logs
	deleted, err := r.deleteExpired("request_logs", r.config.RequestLogRetention)
	if err != nil {
		log.Printf("logrotation: cleanup request_logs: %v", err)
	} else {
		totalDeleted += deleted
		r.stats.RequestLogsDeleted = deleted
	}

	// Clean system logs
	deleted, err = r.deleteExpired("system_logs", r.config.SystemLogRetention)
	if err != nil {
		log.Printf("logrotation: cleanup system_logs: %v", err)
	} else {
		totalDeleted += deleted
		r.stats.SystemLogsDeleted = deleted
	}

	// Clean old alerts
	deleted, err = r.deleteExpired("alerts", r.config.AlertRetention)
	if err != nil {
		// alerts table may not exist — ignore gracefully
	} else {
		totalDeleted += deleted
		r.stats.AlertsDeleted = deleted
	}

	// Clean expired sessions
	deleted, err = r.deleteExpired("sessions", r.config.SessionRetention)
	if err != nil {
		// sessions table may not exist — ignore gracefully
	} else {
		totalDeleted += deleted
		r.stats.SessionsDeleted = deleted
	}

	// Vacuum to reclaim space (only if we deleted something)
	if totalDeleted > 0 {
		if _, err := r.db.Exec("PRAGMA incremental_vacuum"); err != nil {
			log.Printf("logrotation: vacuum: %v", err)
		}
	}

	// Update stats
	r.stats.TotalDeleted = totalDeleted
	r.stats.LastCleanup = time.Now()
	r.stats.NextCleanup = time.Now().Add(r.config.CleanupInterval)

	// Update table sizes
	r.updateTableSizes()

	elapsed := time.Since(start)
	if totalDeleted > 0 {
		log.Printf("logrotation: cleaned %d rows in %s (request=%d system=%d alerts=%d sessions=%d)",
			totalDeleted, elapsed,
			r.stats.RequestLogsDeleted, r.stats.SystemLogsDeleted,
			r.stats.AlertsDeleted, r.stats.SessionsDeleted)
	} else if elapsed > 100*time.Millisecond {
		log.Printf("logrotation: scan completed in %s, no rows deleted", elapsed)
	}
}

func (r *Rotator) deleteExpired(table string, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention).Unix()
	totalDeleted := int64(0)

	for {
		// SQLite doesn't support DELETE ... LIMIT, so use a subquery with rowid
		result, err := r.db.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE created_at < ? LIMIT %d)", table, table, r.config.BatchSize),
			cutoff,
		)
		if err != nil {
			return totalDeleted, fmt.Errorf("delete from %s: %w", table, err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return totalDeleted, fmt.Errorf("rows affected: %w", err)
		}

		totalDeleted += affected

		// If we deleted fewer rows than the batch size, we're done
		if affected < int64(r.config.BatchSize) {
			break
		}

		// Small sleep between batches to avoid locking the DB too long
		time.Sleep(50 * time.Millisecond)
	}

	return totalDeleted, nil
}

func (r *Rotator) updateTableSizes() {
	tables := map[string]*int64{
		"request_logs": &r.stats.TableSizes.RequestLogs,
		"system_logs":  &r.stats.TableSizes.SystemLogs,
		"alerts":       &r.stats.TableSizes.Alerts,
		"sessions":     &r.stats.TableSizes.Sessions,
	}

	for table, target := range tables {
		var count int64
		err := r.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			*target = -1 // table doesn't exist
		} else {
			*target = count
		}
	}
}

// Handler returns an HTTP handler for the cleanup admin API.
func (r *Rotator) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case "GET":
			stats := r.GetStats()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(stats)

		case "POST":
			stats := r.ForceCleanup()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"message": "cleanup completed",
				"stats":   stats,
			})

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
