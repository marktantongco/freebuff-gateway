package freebuffstate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/freebufffacts"
	"freebuff-reverse/internal/session"
)

const (
	ChannelID = "freebuff"

	QuotaGroupUnlimited     = "unlimited"
	QuotaGroupPremiumShared = "premium_shared"

	stateInstanceID        = "freebuff_instance_id"
	stateModel             = "freebuff_model"
	stateAccessTier        = "freebuff_access_tier"
	stateAdmittedAtUnix    = "freebuff_admitted_at_unix"
	stateExpiresAtUnix     = "freebuff_expires_at_unix"
	stateRemainingMs       = "freebuff_remaining_ms"
	stateRateLimit         = "freebuff_rate_limit"
	stateRateLimitsByModel = "freebuff_rate_limits_by_model"
	stateRawSessionJSON    = "freebuff_raw_session_json"
	stateStatus            = "freebuff_status"
)

var ErrNotFound = errors.New("freebuffstate: not found")

const (
	SchedulerFactPremiumWindowTouched  = freebufffacts.SchedulerFactPremiumWindowTouched
	SchedulerFactEverPremiumTouched    = freebufffacts.SchedulerFactEverPremiumTouched
	SchedulerFactPremiumRemaining      = freebufffacts.SchedulerFactPremiumRemaining
	SchedulerFactPremiumRemainingKnown = freebufffacts.SchedulerFactPremiumRemainingKnown
	SchedulerFactPremiumDepleted       = freebufffacts.SchedulerFactPremiumDepleted
	SchedulerFactPremiumResetAtUnix    = freebufffacts.SchedulerFactPremiumResetAtUnix
	SchedulerFactBorrowedUnlimited     = freebufffacts.SchedulerFactBorrowedUnlimited
)

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

type AccountState struct {
	AccountID      string `json:"account_id"`
	UpstreamUserID string `json:"upstream_user_id,omitempty"`
	Email          string `json:"email,omitempty"`
	AccessTier     string `json:"access_tier,omitempty"`
	LastSyncAt     int64  `json:"last_sync_at"`
	RawJSON        string `json:"-"`
}

type QuotaSnapshot struct {
	AccountID     string  `json:"account_id"`
	QuotaGroup    string  `json:"quota_group"`
	Model         string  `json:"model"`
	LimitCount    int     `json:"limit_count"`
	RecentCount   float64 `json:"recent_count"`
	Period        string  `json:"period"`
	ResetTimeZone string  `json:"reset_timezone"`
	ResetAt       int64   `json:"reset_at"`
	WindowHours   int     `json:"window_hours"`
	UpdatedAt     int64   `json:"updated_at"`
	RawJSON       string  `json:"-"`
}

type UpstreamSession struct {
	InstanceID     string `json:"instance_id"`
	AccountID      string `json:"account_id"`
	LocalSessionID string `json:"local_session_id,omitempty"`
	Model          string `json:"model"`
	QuotaGroup     string `json:"quota_group"`
	Status         string `json:"status"`
	AdmittedAt     int64  `json:"admitted_at"`
	ExpiresAt      int64  `json:"expires_at"`
	RemainingMs    int64  `json:"remaining_ms"`
	EndedAt        int64  `json:"ended_at"`
	UpdatedAt      int64  `json:"updated_at"`
	RawJSON        string `json:"-"`
}

func (s UpstreamSession) RestoreState() channels.State {
	state := channels.State{}
	if s.InstanceID != "" {
		state[stateInstanceID] = s.InstanceID
	}
	if s.Model != "" {
		state[stateModel] = s.Model
	}
	if s.Status != "" {
		state[stateStatus] = s.Status
	}
	if s.AdmittedAt > 0 {
		state[stateAdmittedAtUnix] = s.AdmittedAt
	}
	if s.ExpiresAt > 0 {
		state[stateExpiresAtUnix] = s.ExpiresAt
	}
	if s.RemainingMs > 0 {
		state[stateRemainingMs] = s.RemainingMs
	}
	if s.RawJSON != "" {
		state[stateRawSessionJSON] = s.RawJSON
	}
	return state
}

type rateLimitPayload struct {
	Model         string  `json:"model"`
	Limit         int     `json:"limit"`
	Period        string  `json:"period"`
	ResetTimeZone string  `json:"resetTimeZone"`
	ResetAt       string  `json:"resetAt"`
	WindowHours   int     `json:"windowHours"`
	RecentCount   float64 `json:"recentCount"`
}

func (r *Repo) RecordSessionState(ctx context.Context, event session.StateEvent) error {
	if r == nil || r.db == nil || event.ChannelID != ChannelID || event.AccountID == "" {
		return nil
	}
	now := time.Now().Unix()
	rawJSON := event.State.String(stateRawSessionJSON)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("freebuffstate: begin: %w", err)
	}
	if err := r.upsertAccountState(ctx, tx, AccountState{
		AccountID:  event.AccountID,
		AccessTier: event.State.String(stateAccessTier),
		LastSyncAt: now,
		RawJSON:    rawJSON,
	}); err != nil {
		_ = tx.Rollback()
		return err
	}

	quotas := quotaSnapshotsFromState(event.AccountID, now, event.State)
	for _, quota := range quotas {
		if err := r.upsertQuotaSnapshot(ctx, tx, quota); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if session := upstreamSessionFromState(event, now, quotas, rawJSON); session.InstanceID != "" {
		if err := r.upsertUpstreamSession(ctx, tx, session); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("freebuffstate: commit: %w", err)
	}
	return nil
}

func (r *Repo) GetAccountState(ctx context.Context, accountID string) (*AccountState, error) {
	row := r.db.QueryRowContext(ctx, `SELECT account_id, upstream_user_id, email, access_tier, last_sync_at, raw_json FROM freebuff_account_state WHERE account_id = ?`, accountID)
	rec, err := scanAccountState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rec, err
}

func (r *Repo) ListQuotaSnapshots(ctx context.Context, accountID string) ([]QuotaSnapshot, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT account_id, quota_group, model, limit_count, recent_count, period, reset_timezone, reset_at, window_hours, updated_at, raw_json FROM freebuff_quota_snapshots WHERE account_id = ? ORDER BY quota_group ASC, model ASC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []QuotaSnapshot{}
	for rows.Next() {
		rec, err := scanQuotaSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (r *Repo) GetUpstreamSession(ctx context.Context, instanceID string) (*UpstreamSession, error) {
	row := r.db.QueryRowContext(ctx, `SELECT instance_id, account_id, local_session_id, model, quota_group, status, admitted_at, expires_at, remaining_ms, ended_at, updated_at, raw_json FROM freebuff_upstream_sessions WHERE instance_id = ?`, instanceID)
	rec, err := scanUpstreamSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rec, err
}

func (r *Repo) ListUpstreamSessions(ctx context.Context, accountID string) ([]UpstreamSession, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT instance_id, account_id, local_session_id, model, quota_group, status, admitted_at, expires_at, remaining_ms, ended_at, updated_at, raw_json FROM freebuff_upstream_sessions WHERE account_id = ? ORDER BY updated_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UpstreamSession{}
	for rows.Next() {
		rec, err := scanUpstreamSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (r *Repo) ListActiveUpstreamSessions(ctx context.Context, now time.Time) ([]UpstreamSession, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	rows, err := r.db.QueryContext(ctx, `SELECT instance_id, account_id, local_session_id, model, quota_group, status, admitted_at, expires_at, remaining_ms, ended_at, updated_at, raw_json
		FROM freebuff_upstream_sessions
		WHERE status = 'active'
		  AND (ended_at IS NULL OR ended_at = 0)
		  AND (expires_at IS NULL OR expires_at = 0 OR expires_at > ?)
		ORDER BY updated_at DESC`, now.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UpstreamSession{}
	for rows.Next() {
		rec, err := scanUpstreamSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (r *Repo) SchedulerFacts(ctx context.Context, accountID string, now time.Time) (map[string]any, error) {
	if r == nil || r.db == nil || accountID == "" {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	facts := map[string]any{}

	quotas, err := r.ListQuotaSnapshots(ctx, accountID)
	if err != nil {
		return nil, err
	}
	knownRemaining := false
	hasPositiveRemaining := false
	bestPositiveRemaining := 0
	depletedKnown := false
	resetAtUnix := int64(0)
	for _, quota := range quotas {
		if quota.QuotaGroup != QuotaGroupPremiumShared || quota.LimitCount <= 0 {
			continue
		}
		if quota.RecentCount > 0 {
			facts[SchedulerFactEverPremiumTouched] = true
			if quota.ResetAt <= 0 || now.Unix() < quota.ResetAt {
				facts[SchedulerFactPremiumWindowTouched] = true
			}
		}
		if quota.ResetAt > 0 && (resetAtUnix == 0 || quota.ResetAt < resetAtUnix) {
			resetAtUnix = quota.ResetAt
		}
		if quota.ResetAt > 0 && now.Unix() >= quota.ResetAt {
			continue
		}
		remaining := quota.LimitCount - int(math.Ceil(quota.RecentCount))
		if remaining < 0 {
			remaining = 0
		}
		knownRemaining = true
		if remaining > 0 {
			if !hasPositiveRemaining || remaining < bestPositiveRemaining {
				bestPositiveRemaining = remaining
			}
			hasPositiveRemaining = true
		}
	}
	if resetAtUnix > 0 {
		facts[SchedulerFactPremiumResetAtUnix] = resetAtUnix
	}
	if knownRemaining {
		facts[SchedulerFactPremiumRemainingKnown] = true
		if hasPositiveRemaining {
			facts[SchedulerFactPremiumRemaining] = bestPositiveRemaining
		} else {
			facts[SchedulerFactPremiumRemaining] = 0
			depletedKnown = true
		}
		facts[SchedulerFactPremiumDepleted] = depletedKnown
	}

	sessions, err := r.ListUpstreamSessions(ctx, accountID)
	if err != nil {
		return nil, err
	}
	activeUnlimited := false
	for _, session := range sessions {
		active := session.Status == "active" && session.EndedAt == 0 && (session.ExpiresAt == 0 || now.Unix() < session.ExpiresAt)
		if session.QuotaGroup == QuotaGroupPremiumShared {
			facts[SchedulerFactEverPremiumTouched] = true
			if active {
				facts[SchedulerFactPremiumWindowTouched] = true
			}
		}
		if active && session.QuotaGroup == QuotaGroupUnlimited {
			activeUnlimited = true
		}
	}
	if activeUnlimited && !depletedKnown {
		facts[SchedulerFactBorrowedUnlimited] = true
	}
	if len(facts) == 0 {
		return nil, nil
	}
	return facts, nil
}

func (r *Repo) upsertAccountState(ctx context.Context, tx *sql.Tx, rec AccountState) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO freebuff_account_state (account_id, upstream_user_id, email, access_tier, last_sync_at, raw_json)
		 VALUES (?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET
		   upstream_user_id = excluded.upstream_user_id,
		   email = excluded.email,
		   access_tier = excluded.access_tier,
		   last_sync_at = excluded.last_sync_at,
		   raw_json = excluded.raw_json`,
		rec.AccountID, nullString(rec.UpstreamUserID), nullString(rec.Email), nullString(rec.AccessTier), rec.LastSyncAt, nullString(rec.RawJSON),
	)
	if err != nil {
		return fmt.Errorf("freebuffstate: upsert account state: %w", err)
	}
	return nil
}

func (r *Repo) upsertQuotaSnapshot(ctx context.Context, tx *sql.Tx, rec QuotaSnapshot) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO freebuff_quota_snapshots (account_id, quota_group, model, limit_count, recent_count, period, reset_timezone, reset_at, window_hours, updated_at, raw_json)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id, quota_group, model) DO UPDATE SET
		   limit_count = excluded.limit_count,
		   recent_count = excluded.recent_count,
		   period = excluded.period,
		   reset_timezone = excluded.reset_timezone,
		   reset_at = excluded.reset_at,
		   window_hours = excluded.window_hours,
		   updated_at = excluded.updated_at,
		   raw_json = excluded.raw_json`,
		rec.AccountID, rec.QuotaGroup, rec.Model, nullPositiveInt(rec.LimitCount), rec.RecentCount, nullString(rec.Period),
		nullString(rec.ResetTimeZone), nullPositiveInt64(rec.ResetAt), nullPositiveInt(rec.WindowHours), rec.UpdatedAt, nullString(rec.RawJSON),
	)
	if err != nil {
		return fmt.Errorf("freebuffstate: upsert quota snapshot: %w", err)
	}
	return nil
}

func (r *Repo) upsertUpstreamSession(ctx context.Context, tx *sql.Tx, rec UpstreamSession) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO freebuff_upstream_sessions (instance_id, account_id, local_session_id, model, quota_group, status, admitted_at, expires_at, remaining_ms, ended_at, updated_at, raw_json)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(instance_id) DO UPDATE SET
		   account_id = excluded.account_id,
		   local_session_id = excluded.local_session_id,
		   model = excluded.model,
		   quota_group = excluded.quota_group,
		   status = excluded.status,
		   admitted_at = excluded.admitted_at,
		   expires_at = excluded.expires_at,
		   remaining_ms = excluded.remaining_ms,
		   ended_at = excluded.ended_at,
		   updated_at = excluded.updated_at,
		   raw_json = excluded.raw_json`,
		rec.InstanceID, rec.AccountID, nullString(rec.LocalSessionID), rec.Model, rec.QuotaGroup, rec.Status,
		nullPositiveInt64(rec.AdmittedAt), nullPositiveInt64(rec.ExpiresAt), nullPositiveInt64(rec.RemainingMs),
		nullPositiveInt64(rec.EndedAt), rec.UpdatedAt, nullString(rec.RawJSON),
	)
	if err != nil {
		return fmt.Errorf("freebuffstate: upsert upstream session: %w", err)
	}
	return nil
}

func quotaSnapshotsFromState(accountID string, updatedAt int64, state map[string]any) []QuotaSnapshot {
	out := map[string]QuotaSnapshot{}
	if limit, ok := decodeRateLimit(state[stateRateLimit]); ok {
		out[limit.Model] = quotaSnapshotFromPayload(accountID, updatedAt, limit)
	}
	for _, limit := range decodeRateLimitsByModel(state[stateRateLimitsByModel]) {
		out[limit.Model] = quotaSnapshotFromPayload(accountID, updatedAt, limit)
	}
	snapshots := make([]QuotaSnapshot, 0, len(out))
	for _, snapshot := range out {
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func quotaSnapshotFromPayload(accountID string, updatedAt int64, limit rateLimitPayload) QuotaSnapshot {
	raw, _ := json.Marshal(limit)
	return QuotaSnapshot{
		AccountID:     accountID,
		QuotaGroup:    QuotaGroupPremiumShared,
		Model:         limit.Model,
		LimitCount:    limit.Limit,
		RecentCount:   limit.RecentCount,
		Period:        limit.Period,
		ResetTimeZone: limit.ResetTimeZone,
		ResetAt:       parseProviderUnix(limit.ResetAt),
		WindowHours:   limit.WindowHours,
		UpdatedAt:     updatedAt,
		RawJSON:       string(raw),
	}
}

func upstreamSessionFromState(event session.StateEvent, updatedAt int64, quotas []QuotaSnapshot, rawJSON string) UpstreamSession {
	model := event.State.String(stateModel)
	status := firstNonEmpty(event.State.String(stateStatus), "active")
	quotaGroup := QuotaGroupUnlimited
	for _, quota := range quotas {
		if quota.Model == model {
			quotaGroup = quota.QuotaGroup
			break
		}
	}
	endedAt := int64(0)
	if status == "ended" {
		endedAt = updatedAt
	}
	return UpstreamSession{
		InstanceID:     event.State.String(stateInstanceID),
		AccountID:      event.AccountID,
		LocalSessionID: event.LocalSessionID,
		Model:          model,
		QuotaGroup:     quotaGroup,
		Status:         status,
		AdmittedAt:     int64State(event.State[stateAdmittedAtUnix]),
		ExpiresAt:      int64State(event.State[stateExpiresAtUnix]),
		RemainingMs:    int64State(event.State[stateRemainingMs]),
		EndedAt:        endedAt,
		UpdatedAt:      updatedAt,
		RawJSON:        rawJSON,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func decodeRateLimitsByModel(raw any) []rateLimitPayload {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var decoded map[string]rateLimitPayload
	if err := json.Unmarshal(b, &decoded); err != nil {
		return nil
	}
	out := make([]rateLimitPayload, 0, len(decoded))
	for model, limit := range decoded {
		if limit.Model == "" {
			limit.Model = model
		}
		if limit.Model != "" {
			out = append(out, limit)
		}
	}
	return out
}

func decodeRateLimit(raw any) (rateLimitPayload, bool) {
	if raw == nil {
		return rateLimitPayload{}, false
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return rateLimitPayload{}, false
	}
	var decoded rateLimitPayload
	if err := json.Unmarshal(b, &decoded); err != nil || decoded.Model == "" {
		return rateLimitPayload{}, false
	}
	return decoded, true
}

func parseProviderUnix(raw string) int64 {
	if raw == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return 0
	}
	return t.Unix()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanAccountState(s rowScanner) (*AccountState, error) {
	rec := &AccountState{}
	var upstreamUserID, email, accessTier, rawJSON sql.NullString
	if err := s.Scan(&rec.AccountID, &upstreamUserID, &email, &accessTier, &rec.LastSyncAt, &rawJSON); err != nil {
		return nil, err
	}
	rec.UpstreamUserID = upstreamUserID.String
	rec.Email = email.String
	rec.AccessTier = accessTier.String
	rec.RawJSON = rawJSON.String
	return rec, nil
}

func scanQuotaSnapshot(s rowScanner) (*QuotaSnapshot, error) {
	rec := &QuotaSnapshot{}
	var limitCount, resetAt, windowHours sql.NullInt64
	var recentCount sql.NullFloat64
	var period, resetTimezone, rawJSON sql.NullString
	if err := s.Scan(
		&rec.AccountID, &rec.QuotaGroup, &rec.Model, &limitCount, &recentCount, &period,
		&resetTimezone, &resetAt, &windowHours, &rec.UpdatedAt, &rawJSON,
	); err != nil {
		return nil, err
	}
	rec.LimitCount = int(limitCount.Int64)
	rec.RecentCount = recentCount.Float64
	rec.Period = period.String
	rec.ResetTimeZone = resetTimezone.String
	rec.ResetAt = resetAt.Int64
	rec.WindowHours = int(windowHours.Int64)
	rec.RawJSON = rawJSON.String
	return rec, nil
}

func scanUpstreamSession(s rowScanner) (*UpstreamSession, error) {
	rec := &UpstreamSession{}
	var localSessionID, rawJSON sql.NullString
	var admittedAt, expiresAt, remainingMs, endedAt sql.NullInt64
	if err := s.Scan(
		&rec.InstanceID, &rec.AccountID, &localSessionID, &rec.Model, &rec.QuotaGroup, &rec.Status,
		&admittedAt, &expiresAt, &remainingMs, &endedAt, &rec.UpdatedAt, &rawJSON,
	); err != nil {
		return nil, err
	}
	rec.LocalSessionID = localSessionID.String
	rec.AdmittedAt = admittedAt.Int64
	rec.ExpiresAt = expiresAt.Int64
	rec.RemainingMs = remainingMs.Int64
	rec.EndedAt = endedAt.Int64
	rec.RawJSON = rawJSON.String
	return rec, nil
}

func int64State(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case json.Number:
		out, _ := n.Int64()
		return out
	default:
		return 0
	}
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullPositiveInt(n int) sql.NullInt64 {
	if n <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(n), Valid: true}
}

func nullPositiveInt64(n int64) sql.NullInt64 {
	if n <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}
