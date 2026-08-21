package systemlogs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/idgen"
)

const (
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

type Entry struct {
	ID        string         `json:"id"`
	Level     string         `json:"level"`
	Component string         `json:"component"`
	Event     string         `json:"event"`
	Message   string         `json:"message"`
	JobID     string         `json:"job_id,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt int64          `json:"created_at"`
}

type Query struct {
	Component string
	Level     string
	JobID     string
	Limit     int
}

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

func (r *Repo) Append(e Entry) error {
	e = normalizeEntry(e)
	metadata, err := marshalMetadata(e.Metadata)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`INSERT INTO system_logs
		(id, level, component, event, message, job_id, metadata_json, created_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		e.ID, e.Level, e.Component, e.Event, e.Message, nullString(e.JobID), metadata, e.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("systemlogs: append: %w", err)
	}
	return nil
}

func (r *Repo) List(q Query) ([]Entry, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	base := `SELECT id, level, component, event, message, job_id, metadata_json, created_at FROM system_logs WHERE 1=1`
	args := []any{}
	if component := strings.TrimSpace(q.Component); component != "" {
		base += ` AND component = ?`
		args = append(args, component)
	}
	if level := strings.TrimSpace(q.Level); level != "" {
		base += ` AND level = ?`
		args = append(args, level)
	}
	if jobID := strings.TrimSpace(q.JobID); jobID != "" {
		base += ` AND job_id = ?`
		args = append(args, jobID)
	}
	base += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.Query(base, args...)
	if err != nil {
		return nil, fmt.Errorf("systemlogs: list: %w", err)
	}
	defer rows.Close()

	entries := []Entry{}
	for rows.Next() {
		var e Entry
		var jobID sql.NullString
		var metadata sql.NullString
		if err := rows.Scan(&e.ID, &e.Level, &e.Component, &e.Event, &e.Message, &jobID, &metadata, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("systemlogs: scan: %w", err)
		}
		if jobID.Valid {
			e.JobID = jobID.String
		}
		if metadata.Valid && strings.TrimSpace(metadata.String) != "" {
			dec := json.NewDecoder(strings.NewReader(metadata.String))
			if err := dec.Decode(&e.Metadata); err != nil {
				return nil, fmt.Errorf("systemlogs: decode metadata: %w", err)
			}
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("systemlogs: rows: %w", err)
	}
	return entries, nil
}

func normalizeEntry(e Entry) Entry {
	if e.ID == "" {
		e.ID = idgen.New()
	}
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().Unix()
	}
	e.Level = normalizeLevel(e.Level)
	e.Component = defaultText(e.Component, "system")
	e.Event = defaultText(e.Event, "event")
	e.Message = strings.TrimSpace(e.Message)
	return e
}

func normalizeLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case LevelWarn:
		return LevelWarn
	case LevelError:
		return LevelError
	default:
		return LevelInfo
	}
}

func defaultText(v, fallback string) string {
	if trimmed := strings.TrimSpace(v); trimmed != "" {
		return trimmed
	}
	return fallback
}

func marshalMetadata(v map[string]any) (sql.NullString, error) {
	if len(v) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("systemlogs: marshal metadata: %w", err)
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
