package channelconfig

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("channelconfig: not found")

const MaxConcurrentPerSessionLimit = 16

type Config struct {
	MaxConcurrentPerSession int   `json:"max_concurrent_per_session,omitempty"`
	WaitOnFull              *bool `json:"wait_on_full,omitempty"`
}

type Record struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsActive  bool   `json:"is_active"`
	Config    Config `json:"config"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

const channelColumns = `id, name, is_active, config_json, created_at, updated_at`

func ValidateMaxConcurrentPerSession(n int) error {
	if n < 1 || n > MaxConcurrentPerSessionLimit {
		return fmt.Errorf("max_concurrent_per_session must be between 1 and %d", MaxConcurrentPerSessionLimit)
	}
	return nil
}

func (c Config) EffectiveMaxConcurrentPerSession(fallback int) int {
	if c.MaxConcurrentPerSession > 0 {
		return c.MaxConcurrentPerSession
	}
	if fallback < 1 {
		return 1
	}
	return fallback
}

func (c Config) EffectiveWaitOnFull(fallback bool) bool {
	if c.WaitOnFull != nil {
		return *c.WaitOnFull
	}
	return fallback
}

func (r *Repo) Get(id string) (*Record, error) {
	row := r.db.QueryRow(`SELECT `+channelColumns+` FROM channels WHERE id = ?`, id)
	rec, err := scanRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rec, err
}

func (r *Repo) ListMap() (map[string]*Record, error) {
	rows, err := r.db.Query(`SELECT ` + channelColumns + ` FROM channels ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*Record{}
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out[rec.ID] = rec
	}
	return out, rows.Err()
}

func (r *Repo) Upsert(id, name string, active bool, cfg Config) (*Record, error) {
	if id == "" {
		return nil, errors.New("channelconfig: id required")
	}
	if name == "" {
		name = id
	}
	now := time.Now().Unix()
	cfgJSON, err := marshalConfig(cfg)
	if err != nil {
		return nil, err
	}
	_, err = r.db.Exec(
		`INSERT INTO channels (`+channelColumns+`) VALUES (?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   name = excluded.name,
		   is_active = excluded.is_active,
		   config_json = excluded.config_json,
		   updated_at = excluded.updated_at`,
		id, name, boolInt(active), cfgJSON, now, now,
	)
	if err != nil {
		return nil, err
	}
	return r.Get(id)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(s rowScanner) (*Record, error) {
	rec := &Record{}
	var active int
	var cfg sql.NullString
	if err := s.Scan(&rec.ID, &rec.Name, &active, &cfg, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
		return nil, err
	}
	rec.IsActive = active != 0
	if cfg.Valid && cfg.String != "" {
		if err := json.Unmarshal([]byte(cfg.String), &rec.Config); err != nil {
			return nil, fmt.Errorf("channelconfig: decode config: %w", err)
		}
	}
	return rec, nil
}

func marshalConfig(cfg Config) (sql.NullString, error) {
	if cfg.MaxConcurrentPerSession == 0 && cfg.WaitOnFull == nil {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("channelconfig: encode config: %w", err)
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
