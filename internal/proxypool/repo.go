package proxypool

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"freebuff-reverse/internal/idgen"
)

var ErrNotFound = errors.New("proxypool: not found")
var ErrDuplicate = errors.New("proxypool: duplicate")

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

const proxyColumns = `id, name, proxy_url_enc, proxy_key, scheme, host, port, username, is_active, notes, health_status, latency_ms, last_checked_at, last_error, failure_count, exit_ip, country, region, city, created_at, updated_at`

func (r *Repo) List() ([]*Record, error) {
	rows, err := r.db.Query(`SELECT ` + proxyColumns + ` FROM proxy_entries ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProxyRows(rows)
}

func (r *Repo) ListActive() ([]*Record, error) {
	rows, err := r.db.Query(`SELECT ` + proxyColumns + ` FROM proxy_entries WHERE is_active = 1 ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProxyRows(rows)
}

func (r *Repo) Get(id string) (*Record, error) {
	row := r.db.QueryRow(`SELECT `+proxyColumns+` FROM proxy_entries WHERE id = ?`, id)
	rec, err := scanProxyRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rec, err
}

func (r *Repo) Create(rec *Record) error {
	if rec == nil {
		return fmt.Errorf("proxypool: nil record")
	}
	if rec.ID == "" {
		rec.ID = idgen.New()
	}
	if err := normalizeRecord(rec); err != nil {
		return err
	}
	if existing, err := r.findDuplicate(rec, ""); err == nil && existing != nil {
		return ErrDuplicate
	} else if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	now := time.Now().Unix()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	_, err := r.db.Exec(
		`INSERT INTO proxy_entries (`+proxyColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ID, rec.Name, rec.ProxyURL, nullString(rec.ProxyKey), rec.Scheme, rec.Host,
		nullPort(rec.Port), nullString(rec.Username), boolInt(rec.IsActive), nullString(rec.Notes),
		rec.HealthStatus, nullLatency(rec.LatencyMS), nullUnix(rec.LastCheckedAt), nullString(rec.LastError),
		rec.FailureCount, nullString(rec.ExitIP), nullString(rec.Country), nullString(rec.Region),
		nullString(rec.City), rec.CreatedAt, rec.UpdatedAt,
	)
	if isUniqueConstraint(err) {
		return ErrDuplicate
	}
	return err
}

func (r *Repo) Update(rec *Record) error {
	if rec == nil || rec.ID == "" {
		return fmt.Errorf("proxypool: update without id")
	}
	current, err := r.Get(rec.ID)
	if err != nil {
		return err
	}
	if err := normalizeRecord(rec); err != nil {
		return err
	}
	if current.ProxyURL != rec.ProxyURL {
		if existing, err := r.findDuplicate(rec, rec.ID); err == nil && existing != nil {
			return ErrDuplicate
		} else if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		resetRuntimeState(rec)
	} else if current.ProxyKey == "" {
		rec.ProxyKey = ""
	}
	rec.UpdatedAt = time.Now().Unix()
	res, err := r.db.Exec(
		`UPDATE proxy_entries SET name = ?, proxy_url_enc = ?, proxy_key = ?, scheme = ?, host = ?, port = ?, username = ?, is_active = ?, notes = ?, health_status = ?, latency_ms = ?, last_checked_at = ?, last_error = ?, failure_count = ?, exit_ip = ?, country = ?, region = ?, city = ?, updated_at = ? WHERE id = ?`,
		rec.Name, rec.ProxyURL, nullString(rec.ProxyKey), rec.Scheme, rec.Host, nullPort(rec.Port),
		nullString(rec.Username), boolInt(rec.IsActive), nullString(rec.Notes), rec.HealthStatus,
		nullLatency(rec.LatencyMS), nullUnix(rec.LastCheckedAt), nullString(rec.LastError),
		rec.FailureCount, nullString(rec.ExitIP), nullString(rec.Country), nullString(rec.Region),
		nullString(rec.City), rec.UpdatedAt, rec.ID,
	)
	if isUniqueConstraint(err) {
		return ErrDuplicate
	}
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) FindDuplicate(proxyURL string) (*Record, error) {
	rec := &Record{ProxyURL: proxyURL}
	if err := normalizeRecord(rec); err != nil {
		return nil, err
	}
	return r.findDuplicate(rec, "")
}

func (r *Repo) MarkHealthy(id string, latencyMS int64, checkedAt int64, meta ProbeMetadata) error {
	res, err := r.db.Exec(
		`UPDATE proxy_entries SET health_status = ?, latency_ms = ?, last_checked_at = ?, last_error = NULL, failure_count = 0, exit_ip = ?, country = ?, region = ?, city = ?, updated_at = ? WHERE id = ?`,
		HealthHealthy, latencyMS, checkedAt, nullString(meta.ExitIP), nullString(meta.Country),
		nullString(meta.Region), nullString(meta.City), time.Now().Unix(), id,
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

func (r *Repo) MarkUnhealthy(id string, checkedAt int64, message string) error {
	res, err := r.db.Exec(
		`UPDATE proxy_entries SET health_status = ?, latency_ms = NULL, last_checked_at = ?, last_error = ?, failure_count = failure_count + 1, updated_at = ? WHERE id = ?`,
		HealthUnhealthy, checkedAt, nullString(message), time.Now().Unix(), id,
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
	res, err := r.db.Exec(`DELETE FROM proxy_entries WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func normalizeRecord(rec *Record) error {
	normalized, err := NormalizeProxyURL(rec.ProxyURL)
	if err != nil {
		return err
	}
	rec.ProxyURL = normalized.URL
	rec.ProxyKey = ProxyKey(normalized.URL)
	rec.Scheme = normalized.Scheme
	rec.Host = normalized.Host
	rec.Port = normalized.Port
	rec.Username = normalized.Username
	if rec.Name == "" {
		rec.Name = DefaultName(rec.ProxyURL)
	}
	if rec.HealthStatus == "" {
		rec.HealthStatus = HealthUnknown
	}
	return nil
}

func resetRuntimeState(rec *Record) {
	rec.HealthStatus = HealthUnknown
	rec.LatencyMS = 0
	rec.LastCheckedAt = 0
	rec.LastError = ""
	rec.FailureCount = 0
	rec.ExitIP = ""
	rec.Country = ""
	rec.Region = ""
	rec.City = ""
}

func (r *Repo) findDuplicate(rec *Record, excludeID string) (*Record, error) {
	if rec.ProxyKey != "" {
		row := r.db.QueryRow(`SELECT `+proxyColumns+` FROM proxy_entries WHERE proxy_key = ? AND id <> ? LIMIT 1`, rec.ProxyKey, excludeID)
		found, err := scanProxyRow(row)
		if err == nil {
			return found, nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}

	rows, err := r.db.Query(
		`SELECT `+proxyColumns+` FROM proxy_entries WHERE scheme = ? AND host = ? AND COALESCE(port, 0) = ? AND COALESCE(username, '') = ? AND id <> ? ORDER BY created_at DESC`,
		rec.Scheme, rec.Host, rec.Port, rec.Username, excludeID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		candidate, err := scanProxyRow(rows)
		if err != nil {
			return nil, err
		}
		if candidate.ProxyURL == rec.ProxyURL {
			return candidate, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotFound
}

func scanProxyRows(rows *sql.Rows) ([]*Record, error) {
	var out []*Record
	for rows.Next() {
		rec, err := scanProxyRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

type proxyRowScanner interface {
	Scan(dest ...any) error
}

func scanProxyRow(s proxyRowScanner) (*Record, error) {
	rec := &Record{}
	var port sql.NullInt64
	var username sql.NullString
	var notes sql.NullString
	var proxyKey sql.NullString
	var healthStatus sql.NullString
	var latency sql.NullInt64
	var lastChecked sql.NullInt64
	var lastError sql.NullString
	var exitIP sql.NullString
	var country sql.NullString
	var region sql.NullString
	var city sql.NullString
	var active int
	if err := s.Scan(
		&rec.ID, &rec.Name, &rec.ProxyURL, &proxyKey, &rec.Scheme, &rec.Host, &port, &username,
		&active, &notes, &healthStatus, &latency, &lastChecked, &lastError, &rec.FailureCount,
		&exitIP, &country, &region, &city, &rec.CreatedAt, &rec.UpdatedAt,
	); err != nil {
		return nil, err
	}
	rec.Port = int(port.Int64)
	rec.Username = username.String
	rec.IsActive = active != 0
	rec.Notes = notes.String
	rec.ProxyKey = proxyKey.String
	rec.HealthStatus = healthStatus.String
	if rec.HealthStatus == "" {
		rec.HealthStatus = HealthUnknown
	}
	rec.LatencyMS = latency.Int64
	rec.LastCheckedAt = lastChecked.Int64
	rec.LastError = lastError.String
	rec.ExitIP = exitIP.String
	rec.Country = country.String
	rec.Region = region.String
	rec.City = city.String
	return rec, nil
}

func nullPort(port int) sql.NullInt64 {
	if port <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(port), Valid: true}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullString(v string) sql.NullString {
	if strings.TrimSpace(v) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func nullLatency(v int64) sql.NullInt64 {
	if v <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

func nullUnix(v int64) sql.NullInt64 {
	if v <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || strings.Contains(msg, "constraint failed")
}
