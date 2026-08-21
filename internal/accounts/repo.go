package accounts

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/idgen"
)

var ErrNotFound = errors.New("accounts: not found")

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

const accountColumns = `id, channel_id, name, credential_enc, priority, rpm_limit, quota_total, quota_period, quota_used, quota_period_start, is_active, metadata_json, created_at, updated_at`

func (r *Repo) ListByChannel(channelID string) ([]*Record, error) {
	rows, err := r.db.Query(`SELECT `+accountColumns+` FROM accounts WHERE channel_id = ? ORDER BY priority DESC, created_at ASC`, channelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (r *Repo) ListAll() ([]*Record, error) {
	rows, err := r.db.Query(`SELECT ` + accountColumns + ` FROM accounts ORDER BY channel_id ASC, priority DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (r *Repo) Get(id string) (*Record, error) {
	row := r.db.QueryRow(`SELECT `+accountColumns+` FROM accounts WHERE id = ?`, id)
	rec, err := scanRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rec, err
}

func (r *Repo) Create(rec *Record) error {
	if rec.ID == "" {
		rec.ID = idgen.New()
	}
	if rec.Priority == 0 {
		rec.Priority = 50
	}
	now := time.Now().Unix()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	metaJSON, err := marshalMetadata(rec.Metadata)
	if err != nil {
		return err
	}
	var rpm sql.NullInt64
	if rec.RPMLimit > 0 {
		rpm = sql.NullInt64{Int64: int64(rec.RPMLimit), Valid: true}
	}
	quotaTotal := nullPositiveInt64(rec.QuotaTotal)
	quotaPeriod := nullString(rec.QuotaPeriod)
	quotaPeriodStart := nullPositiveInt64(rec.QuotaPeriodStart)
	_, err = r.db.Exec(
		`INSERT INTO accounts (`+accountColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ID, rec.ChannelID, rec.Name, rec.Credential, rec.Priority, rpm,
		quotaTotal, quotaPeriod, rec.QuotaUsed, quotaPeriodStart,
		boolInt(rec.IsActive), metaJSON, rec.CreatedAt, rec.UpdatedAt,
	)
	return err
}

func (r *Repo) Update(rec *Record) error {
	if rec.ID == "" {
		return fmt.Errorf("accounts: update without id")
	}
	rec.UpdatedAt = time.Now().Unix()
	metaJSON, err := marshalMetadata(rec.Metadata)
	if err != nil {
		return err
	}
	var rpm sql.NullInt64
	if rec.RPMLimit > 0 {
		rpm = sql.NullInt64{Int64: int64(rec.RPMLimit), Valid: true}
	}
	quotaTotal := nullPositiveInt64(rec.QuotaTotal)
	quotaPeriod := nullString(rec.QuotaPeriod)
	quotaPeriodStart := nullPositiveInt64(rec.QuotaPeriodStart)
	res, err := r.db.Exec(
		`UPDATE accounts SET name = ?, credential_enc = ?, priority = ?, rpm_limit = ?, quota_total = ?, quota_period = ?, quota_used = ?, quota_period_start = ?, is_active = ?, metadata_json = ?, updated_at = ? WHERE id = ?`,
		rec.Name, rec.Credential, rec.Priority, rpm, quotaTotal, quotaPeriod,
		rec.QuotaUsed, quotaPeriodStart, boolInt(rec.IsActive), metaJSON, rec.UpdatedAt, rec.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) IncrementQuotaUsed(id string, by int64) error {
	if id == "" || by <= 0 {
		return nil
	}
	_, err := r.db.Exec(
		`UPDATE accounts SET quota_used = quota_used + ?, updated_at = ? WHERE id = ? AND quota_total IS NOT NULL AND quota_total > 0`,
		by, time.Now().Unix(), id,
	)
	return err
}

func (r *Repo) RollQuota(id string, periodStart int64) error {
	res, err := r.db.Exec(
		`UPDATE accounts SET quota_used = 0, quota_period_start = ?, updated_at = ? WHERE id = ?`,
		periodStart, time.Now().Unix(), id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) Delete(id string) error {
	res, err := r.db.Exec(`DELETE FROM accounts WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanRows(rows *sql.Rows) ([]*Record, error) {
	var out []*Record
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(s rowScanner) (*Record, error) {
	rec := &Record{}
	var meta sql.NullString
	var rpm sql.NullInt64
	var quotaTotal sql.NullInt64
	var quotaPeriod sql.NullString
	var quotaPeriodStart sql.NullInt64
	var active int
	if err := s.Scan(
		&rec.ID, &rec.ChannelID, &rec.Name, &rec.Credential, &rec.Priority,
		&rpm, &quotaTotal, &quotaPeriod, &rec.QuotaUsed, &quotaPeriodStart,
		&active, &meta, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		return nil, err
	}
	rec.RPMLimit = int(rpm.Int64)
	rec.QuotaTotal = quotaTotal.Int64
	rec.QuotaPeriod = quotaPeriod.String
	rec.QuotaPeriodStart = quotaPeriodStart.Int64
	rec.IsActive = active != 0
	if meta.Valid && meta.String != "" {
		_ = json.Unmarshal([]byte(meta.String), &rec.Metadata)
	}
	return rec, nil
}

func marshalMetadata(m map[string]any) (sql.NullString, error) {
	if len(m) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullPositiveInt64(n int64) sql.NullInt64 {
	if n <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
