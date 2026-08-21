package logs

import (
	"database/sql"
	"encoding/json"
	"strings"
)

type TimeRange struct {
	Label   string `json:"range"`
	StartAt int64  `json:"start_at"`
	EndAt   int64  `json:"end_at"`
}

type UsageQuery struct {
	Range     TimeRange
	ChannelID string
	AccountID string
	Search    string
	Limit     int
}

type UsageSummary struct {
	Range         string  `json:"range"`
	StartAt       int64   `json:"start_at"`
	EndAt         int64   `json:"end_at"`
	TotalRequests int64   `json:"total_requests"`
	SuccessCount  int64   `json:"success_count"`
	FailureCount  int64   `json:"failure_count"`
	TokensIn      int64   `json:"tokens_in"`
	TokensOut     int64   `json:"tokens_out"`
	TotalTokens   int64   `json:"total_tokens"`
	AvgLatencyMS  float64 `json:"avg_latency_ms"`
}

type AccountUsage struct {
	ChannelID     string  `json:"channel_id"`
	AccountID     string  `json:"account_id"`
	TotalRequests int64   `json:"total_requests"`
	SuccessCount  int64   `json:"success_count"`
	FailureCount  int64   `json:"failure_count"`
	TokensIn      int64   `json:"tokens_in"`
	TokensOut     int64   `json:"tokens_out"`
	TotalTokens   int64   `json:"total_tokens"`
	AvgLatencyMS  float64 `json:"avg_latency_ms"`
	LastRequestAt int64   `json:"last_request_at"`
	TopModel      string  `json:"top_model"`
}

type ChannelUsage struct {
	ChannelID     string `json:"channel_id"`
	RequestCount  int64  `json:"request_count"`
	TokensIn      int64  `json:"tokens_in"`
	TokensOut     int64  `json:"tokens_out"`
	TotalTokens   int64  `json:"total_tokens"`
	LastRequestAt int64  `json:"last_request_at"`
}

const usageEntryColumns = `id, channel_id, account_id, session_id, method, path, stream, selection_key, model, status, response_class, latency_ms, first_response_ms, tokens_in, tokens_out, phase_timings_json, error, created_at`

func (r *Repo) UsageSummary(q UsageQuery) (UsageSummary, error) {
	where, args := usageWhere(q)
	row := r.db.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN response_class = 'ok' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN response_class != 'ok' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(COALESCE(tokens_in, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_out, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_in, 0) + COALESCE(tokens_out, 0)), 0),
		COALESCE(AVG(latency_ms), 0)
		FROM request_logs`+where, args...)

	summary := UsageSummary{
		Range:   q.Range.Label,
		StartAt: q.Range.StartAt,
		EndAt:   q.Range.EndAt,
	}
	if err := row.Scan(
		&summary.TotalRequests,
		&summary.SuccessCount,
		&summary.FailureCount,
		&summary.TokensIn,
		&summary.TokensOut,
		&summary.TotalTokens,
		&summary.AvgLatencyMS,
	); err != nil {
		return UsageSummary{}, err
	}
	return summary, nil
}

func (r *Repo) AccountUsage(q UsageQuery) ([]AccountUsage, error) {
	where, args := usageWhere(q)
	args = append(args, clampLimit(q.Limit, 500))
	rows, err := r.db.Query(`SELECT
		channel_id,
		COALESCE(account_id, ''),
		COUNT(*),
		COALESCE(SUM(CASE WHEN response_class = 'ok' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN response_class != 'ok' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(COALESCE(tokens_in, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_out, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_in, 0) + COALESCE(tokens_out, 0)), 0),
		COALESCE(AVG(latency_ms), 0),
		COALESCE(MAX(created_at), 0)
		FROM request_logs`+where+`
		GROUP BY channel_id, COALESCE(account_id, '')
		ORDER BY COUNT(*) DESC, COALESCE(MAX(created_at), 0) DESC
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []AccountUsage{}
	for rows.Next() {
		var item AccountUsage
		if err := rows.Scan(
			&item.ChannelID,
			&item.AccountID,
			&item.TotalRequests,
			&item.SuccessCount,
			&item.FailureCount,
			&item.TokensIn,
			&item.TokensOut,
			&item.TotalTokens,
			&item.AvgLatencyMS,
			&item.LastRequestAt,
		); err != nil {
			return nil, err
		}
		item.TopModel = r.topModel(q, item.ChannelID, item.AccountID)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *Repo) ChannelUsage(q UsageQuery) ([]ChannelUsage, error) {
	where, args := usageWhere(q)
	rows, err := r.db.Query(`SELECT
		channel_id,
		COUNT(*),
		COALESCE(SUM(COALESCE(tokens_in, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_out, 0)), 0),
		COALESCE(SUM(COALESCE(tokens_in, 0) + COALESCE(tokens_out, 0)), 0),
		COALESCE(MAX(created_at), 0)
		FROM request_logs`+where+`
		GROUP BY channel_id
		ORDER BY channel_id ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ChannelUsage{}
	for rows.Next() {
		var item ChannelUsage
		if err := rows.Scan(
			&item.ChannelID,
			&item.RequestCount,
			&item.TokensIn,
			&item.TokensOut,
			&item.TotalTokens,
			&item.LastRequestAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *Repo) UsageEvents(q UsageQuery) ([]Entry, error) {
	where, args := usageWhere(q)
	args = append(args, clampLimit(q.Limit, 100))
	rows, err := r.db.Query(`SELECT `+usageEntryColumns+`
		FROM request_logs`+where+`
		ORDER BY created_at DESC
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

func (r *Repo) topModel(q UsageQuery, channelID string, accountID string) string {
	q.ChannelID = channelID
	q.AccountID = accountID
	q.Search = ""
	where, args := usageWhere(q, `model IS NOT NULL`, `model != ''`)
	row := r.db.QueryRow(`SELECT model
		FROM request_logs`+where+`
		GROUP BY model
		ORDER BY COUNT(*) DESC, MAX(created_at) DESC, model ASC
		LIMIT 1`, args...)
	var model string
	if err := row.Scan(&model); err != nil {
		return ""
	}
	return model
}

func usageWhere(q UsageQuery, extra ...string) (string, []any) {
	clauses := []string{"1=1"}
	args := []any{}
	if q.Range.StartAt > 0 {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, q.Range.StartAt)
	}
	if q.Range.EndAt > 0 {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, q.Range.EndAt)
	}
	if q.ChannelID != "" {
		clauses = append(clauses, "channel_id = ?")
		args = append(args, q.ChannelID)
	}
	if q.AccountID != "" {
		clauses = append(clauses, "account_id = ?")
		args = append(args, q.AccountID)
	}
	if search := strings.TrimSpace(strings.ToLower(q.Search)); search != "" {
		clauses = append(clauses, `lower(
			channel_id || ' ' ||
			COALESCE(account_id, '') || ' ' ||
			COALESCE(session_id, '') || ' ' ||
			method || ' ' ||
			path || ' ' ||
			COALESCE(model, '') || ' ' ||
			COALESCE(selection_key, '') || ' ' ||
			response_class
		) LIKE ?`)
		args = append(args, "%"+search+"%")
	}
	clauses = append(clauses, extra...)
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func clampLimit(limit int, fallback int) int {
	if limit <= 0 {
		return fallback
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func scanEntries(rows *sql.Rows) ([]Entry, error) {
	out := []Entry{}
	for rows.Next() {
		var e Entry
		var accID, sessID, selectionKey, model, phaseTimings, errStr sql.NullString
		var stream int
		var firstResponseMS sql.NullInt64
		var tokensIn, tokensOut sql.NullInt64
		if err := rows.Scan(
			&e.ID, &e.ChannelID, &accID, &sessID, &e.Method, &e.Path,
			&stream, &selectionKey, &model, &e.Status, &e.ResponseClass, &e.LatencyMS,
			&firstResponseMS, &tokensIn, &tokensOut, &phaseTimings, &errStr, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		e.AccountID = accID.String
		e.SessionID = sessID.String
		e.Stream = stream != 0
		e.SelectionKey = selectionKey.String
		e.Model = model.String
		e.Error = errStr.String
		if firstResponseMS.Valid {
			e.FirstResponseMS = firstResponseMS.Int64
		}
		if tokensIn.Valid || tokensOut.Valid {
			e.TokensKnown = true
			e.TokensIn = int(tokensIn.Int64)
			e.TokensOut = int(tokensOut.Int64)
		}
		if phaseTimings.Valid && phaseTimings.String != "" {
			_ = json.Unmarshal([]byte(phaseTimings.String), &e.PhaseTimings)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
