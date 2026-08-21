package logs

import (
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/idgen"
)

type Entry struct {
	ID              string         `json:"id"`
	ChannelID       string         `json:"channel_id"`
	AccountID       string         `json:"account_id,omitempty"`
	SessionID       string         `json:"session_id,omitempty"`
	Method          string         `json:"method"`
	Path            string         `json:"path"`
	Stream          bool           `json:"stream"`
	SelectionKey    string         `json:"selection_key,omitempty"`
	Model           string         `json:"model,omitempty"`
	Status          int            `json:"status"`
	ResponseClass   string         `json:"response_class"`
	LatencyMS       int64          `json:"latency_ms"`
	FirstResponseMS int64          `json:"first_response_ms"`
	TokensIn        int            `json:"tokens_in,omitempty"`
	TokensOut       int            `json:"tokens_out,omitempty"`
	PhaseTimings    map[string]any `json:"phase_timings,omitempty"`
	Error           string         `json:"error,omitempty"`
	CreatedAt       int64          `json:"created_at"`
	TokensKnown     bool           `json:"-"`
}

type Repo struct {
	db *sql.DB

	mu     sync.Mutex
	buffer []Entry
	flush  chan struct{}
}

func NewRepo(db *sql.DB) *Repo {
	r := &Repo{
		db:    db,
		flush: make(chan struct{}, 1),
	}
	return r
}

func (r *Repo) Append(e Entry) {
	if e.ID == "" {
		e.ID = idgen.New()
	}
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().Unix()
	}
	r.mu.Lock()
	r.buffer = append(r.buffer, e)
	r.mu.Unlock()
	select {
	case r.flush <- struct{}{}:
	default:
	}
}

func (r *Repo) Run(ctx interface{ Done() <-chan struct{} }) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.flushOnce()
			return
		case <-ticker.C:
			r.flushOnce()
		case <-r.flush:
			r.flushOnce()
		}
	}
}

func (r *Repo) flushOnce() {
	r.mu.Lock()
	if len(r.buffer) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.buffer
	r.buffer = nil
	r.mu.Unlock()

	tx, err := r.db.Begin()
	if err != nil {
		return
	}
	stmt, err := tx.Prepare(`INSERT INTO request_logs
		(id, channel_id, account_id, session_id, method, path, stream, selection_key, model, status, response_class, latency_ms, first_response_ms, tokens_in, tokens_out, phase_timings_json, error, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		return
	}
	for _, e := range batch {
		phaseTimings, err := marshalPhaseTimings(e.PhaseTimings)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return
		}
		_, err = stmt.Exec(
			e.ID, e.ChannelID, nullString(e.AccountID), nullString(e.SessionID),
			e.Method, e.Path, boolInt(e.Stream), nullString(e.SelectionKey), nullString(e.Model),
			e.Status, e.ResponseClass, e.LatencyMS,
			nullableInt64(e.FirstResponseMS),
			nullToken(e.TokensIn, e.TokensKnown), nullToken(e.TokensOut, e.TokensKnown),
			phaseTimings,
			nullString(e.Error), e.CreatedAt,
		)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return
		}
	}
	_ = stmt.Close()
	_ = tx.Commit()
}

type Query struct {
	ChannelID string
	AccountID string
	Limit     int
}

func (r *Repo) List(q Query) ([]Entry, error) {
	limit := q.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	base := `SELECT ` + usageEntryColumns + `
			 FROM request_logs WHERE 1=1`
	args := []any{}
	if q.ChannelID != "" {
		base += ` AND channel_id = ?`
		args = append(args, q.ChannelID)
	}
	if q.AccountID != "" {
		base += ` AND account_id = ?`
		args = append(args, q.AccountID)
	}
	base += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.Query(base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullToken(n int, ok bool) any {
	if !ok {
		return nil
	}
	return n
}

func nullableInt64(n int64) any {
	if n <= 0 {
		return nil
	}
	return n
}

func marshalPhaseTimings(v map[string]any) (sql.NullString, error) {
	if len(v) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
