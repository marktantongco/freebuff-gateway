package authkeys

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"freebuff-reverse/internal/idgen"
)

var ErrNotFound = errors.New("authkeys: not found")

type Record struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	KeyPrefix  string `json:"key_prefix"`
	IsActive   bool   `json:"is_active"`
	LastUsedAt int64  `json:"last_used_at"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

type CreatedRecord struct {
	Record
	Key string `json:"key"`
}

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

const keyColumns = `id, name, key_prefix, is_active, last_used_at, created_at, updated_at`

func GenerateKey() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("authkeys: generate key: %w", err)
	}
	return "sk-" + base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func (r *Repo) Create(name string) (*CreatedRecord, error) {
	key, err := GenerateKey()
	if err != nil {
		return nil, err
	}
	return r.CreateWithKey(name, key)
}

func (r *Repo) CreateWithKey(name, key string) (*CreatedRecord, error) {
	name = strings.TrimSpace(name)
	key = strings.TrimSpace(key)
	if name == "" {
		return nil, errors.New("authkeys: name required")
	}
	if !strings.HasPrefix(key, "sk-") {
		return nil, errors.New("authkeys: key must start with sk-")
	}
	rec := Record{
		ID:        idgen.New(),
		Name:      name,
		KeyPrefix: keyPrefix(key),
		IsActive:  true,
	}
	now := time.Now().Unix()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	_, err := r.db.Exec(
		`INSERT INTO auth_keys (id, name, key_hash, key_prefix, is_active, last_used_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)`,
		rec.ID, rec.Name, keyHash(key), rec.KeyPrefix, boolInt(rec.IsActive), nil, rec.CreatedAt, rec.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("authkeys: create: %w", err)
	}
	return &CreatedRecord{Record: rec, Key: key}, nil
}

func (r *Repo) List() ([]Record, error) {
	rows, err := r.db.Query(`SELECT ` + keyColumns + ` FROM auth_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("authkeys: list: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func (r *Repo) Delete(id string) error {
	res, err := r.db.Exec(`DELETE FROM auth_keys WHERE id = ?`, strings.TrimSpace(id))
	if err != nil {
		return fmt.Errorf("authkeys: delete: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) Authenticate(key string) (*Record, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, ErrNotFound
	}
	row := r.db.QueryRow(`SELECT `+keyColumns+` FROM auth_keys WHERE key_hash = ? AND is_active = 1`, keyHash(key))
	rec, err := scanRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("authkeys: authenticate: %w", err)
	}
	now := time.Now().Unix()
	if _, err := r.db.Exec(`UPDATE auth_keys SET last_used_at = ?, updated_at = ? WHERE id = ?`, now, now, rec.ID); err != nil {
		return nil, fmt.Errorf("authkeys: mark used: %w", err)
	}
	rec.LastUsedAt = now
	rec.UpdatedAt = now
	return rec, nil
}

func scanRows(rows *sql.Rows) ([]Record, error) {
	out := []Record{}
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(s rowScanner) (*Record, error) {
	rec := &Record{}
	var active int
	var lastUsed sql.NullInt64
	if err := s.Scan(
		&rec.ID, &rec.Name, &rec.KeyPrefix, &active, &lastUsed, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		return nil, err
	}
	rec.IsActive = active != 0
	if lastUsed.Valid {
		rec.LastUsedAt = lastUsed.Int64
	}
	return rec, nil
}

func keyHash(key string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return hex.EncodeToString(sum[:])
}

func keyPrefix(key string) string {
	key = strings.TrimSpace(key)
	if len(key) <= 14 {
		return key
	}
	return key[:14]
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
