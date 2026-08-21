package usage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// RequestRecord represents a single request for analytics.
type RequestRecord struct {
	ID           string `json:"id"`
	Method       string `json:"method"`
	Path         string `json:"path"`
	Status       int    `json:"status"`
	LatencyMS    int64  `json:"latency_ms"`
	TokensIn     int    `json:"tokens_in"`
	TokensOut    int    `json:"tokens_out"`
	Model        string `json:"model,omitempty"`
	AccountID    string `json:"account_id,omitempty"`
	ChannelID    string `json:"channel_id,omitempty"`
	Stream       bool   `json:"stream"`
	ResponseClass string `json:"response_class"`
	Error        string `json:"error,omitempty"`
	CreatedAt    int64  `json:"created_at"`
}

// AnalyticsSnapshot is the top-level analytics view for the dashboard.
type AnalyticsSnapshot struct {
	Summary           RequestSummary     `json:"summary"`
	HourlyTraffic     []HourlyPoint      `json:"hourly_traffic"`
	TopEndpoints      []EndpointStats    `json:"top_endpoints"`
	TopModels         []ModelStats       `json:"top_models"`
	StatusBreakdown   []StatusCount      `json:"status_breakdown"`
	LatencyPercentiles LatencyPercentiles `json:"latency_percentiles"`
	ErrorRate         float64            `json:"error_rate"`
	TokensSummary     TokensSummary      `json:"tokens_summary"`
	Timestamp         time.Time          `json:"timestamp"`
}

// RequestSummary contains overall request statistics.
type RequestSummary struct {
	TotalRequests   int64   `json:"total_requests"`
	SuccessCount    int64   `json:"success_count"`
	ErrorCount      int64   `json:"error_count"`
	AvgLatencyMS    float64 `json:"avg_latency_ms"`
	P50LatencyMS    int64   `json:"p50_latency_ms"`
	P95LatencyMS    int64   `json:"p95_latency_ms"`
	P99LatencyMS    int64   `json:"p99_latency_ms"`
	StreamRequests  int64   `json:"stream_requests"`
	NonStreamRequests int64 `json:"non_stream_requests"`
}

// HourlyPoint is traffic data for one hour.
type HourlyPoint struct {
	Hour     string `json:"hour"`
	Requests int64  `json:"requests"`
	Errors   int64  `json:"errors"`
	Tokens   int64  `json:"tokens"`
}

// EndpointStats is per-endpoint statistics.
type EndpointStats struct {
	Path        string  `json:"path"`
	Requests    int64   `json:"requests"`
	AvgLatency  float64 `json:"avg_latency_ms"`
	ErrorRate   float64 `json:"error_rate"`
	TokensTotal int64   `json:"tokens_total"`
}

// ModelStats is per-model statistics.
type ModelStats struct {
	Model       string  `json:"model"`
	Requests    int64   `json:"requests"`
	AvgLatency  float64 `json:"avg_latency_ms"`
	TokensTotal int64   `json:"tokens_total"`
	SuccessRate float64 `json:"success_rate"`
}

// StatusCount is HTTP status code distribution.
type StatusCount struct {
	Status int   `json:"status"`
	Count  int64 `json:"count"`
	Pct    float64 `json:"pct"`
}

// LatencyPercentiles contains latency distribution.
type LatencyPercentiles struct {
	P50  int64 `json:"p50"`
	P75  int64 `json:"p75"`
	P90  int64 `json:"p90"`
	P95  int64 `json:"p95"`
	P99  int64 `json:"p99"`
	Max  int64 `json:"max"`
}

// TokensSummary contains token usage statistics.
type TokensSummary struct {
	TotalIn    int64 `json:"total_in"`
	TotalOut   int64 `json:"total_out"`
	Total      int64 `json:"total"`
	AvgPerReq  float64 `json:"avg_per_request"`
}

// Analytics tracks real-time usage analytics in memory with periodic DB flush.
type Analytics struct {
	db      *sql.DB
	mu      sync.RWMutex
	recent  []RequestRecord
	maxSize int
	stopCh  chan struct{}
}

// NewAnalytics creates a new analytics tracker.
func NewAnalytics(db *sql.DB) *Analytics {
	a := &Analytics{
		db:      db,
		recent:  make([]RequestRecord, 0, 1000),
		maxSize: 10000,
		stopCh:  make(chan struct{}),
	}
	return a
}

// Record adds a request to the analytics buffer.
func (a *Analytics) Record(req RequestRecord) {
	if req.CreatedAt == 0 {
		req.CreatedAt = time.Now().UnixMilli()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.recent) >= a.maxSize {
		// drop oldest 20%
		cutoff := len(a.recent) / 5
		a.recent = a.recent[cutoff:]
	}
	a.recent = append(a.recent, req)
}

// Snapshot returns the current analytics view from recent in-memory data.
func (a *Analytics) Snapshot() AnalyticsSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	snap := AnalyticsSnapshot{
		Timestamp: time.Now(),
	}

	if len(a.recent) == 0 {
		return snap
	}

	// Copy recent data for safe iteration
	data := make([]RequestRecord, len(a.recent))
	copy(data, a.recent)

	// Summary
	var totalLatency int64
	var totalTokensIn, totalTokensOut int64
	var successCount, errorCount, streamCount int64
	statusCounts := make(map[int]int64)
	latencies := make([]int64, 0, len(data))
	endpointMap := make(map[string]*endpointAccum)
	modelMap := make(map[string]*modelAccum)
	hourMap := make(map[string]*HourlyPoint)

	for _, r := range data {
		latencies = append(latencies, r.LatencyMS)
		totalLatency += r.LatencyMS
		totalTokensIn += int64(r.TokensIn)
		totalTokensOut += int64(r.TokensOut)
		statusCounts[r.Status]++

		if r.Status >= 200 && r.Status < 400 {
			successCount++
		} else {
			errorCount++
		}
		if r.Stream {
			streamCount++
		}

		// Endpoints
		path := normalizePath(r.Path)
		ea, ok := endpointMap[path]
		if !ok {
			ea = &endpointAccum{Path: path}
			endpointMap[path] = ea
		}
		ea.Requests++
		ea.TotalLatency += r.LatencyMS
		ea.TotalTokens += int64(r.TokensIn + r.TokensOut)
		if r.Status >= 400 || (r.Error != "") {
			ea.Errors++
		}

		// Models
		if r.Model != "" {
			ma, ok := modelMap[r.Model]
			if !ok {
				ma = &modelAccum{Model: r.Model}
				modelMap[r.Model] = ma
			}
			ma.Requests++
			ma.TotalLatency += r.LatencyMS
			ma.TotalTokens += int64(r.TokensIn + r.TokensOut)
			if r.Status >= 200 && r.Status < 400 {
				ma.Successes++
			}
		}

		// Hourly
		hourKey := time.UnixMilli(r.CreatedAt).UTC().Format("2006-01-02T15:00")
		hp, ok := hourMap[hourKey]
		if !ok {
			hp = &HourlyPoint{Hour: hourKey}
			hourMap[hourKey] = hp
		}
		hp.Requests++
		hp.Tokens += int64(r.TokensIn + r.TokensOut)
		if r.Status >= 400 || r.Error != "" {
			hp.Errors++
		}
	}

	total := int64(len(data))
	snap.Summary = RequestSummary{
		TotalRequests:   total,
		SuccessCount:    successCount,
		ErrorCount:      errorCount,
		AvgLatencyMS:    float64(totalLatency) / float64(total),
		StreamRequests:  streamCount,
		NonStreamRequests: total - streamCount,
	}

	// Latency percentiles
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	snap.LatencyPercentiles = LatencyPercentiles{
		P50: percentile(latencies, 0.50),
		P75: percentile(latencies, 0.75),
		P90: percentile(latencies, 0.90),
		P95: percentile(latencies, 0.95),
		P99: percentile(latencies, 0.99),
		Max: latencies[len(latencies)-1],
	}
	snap.Summary.P50LatencyMS = snap.LatencyPercentiles.P50
	snap.Summary.P95LatencyMS = snap.LatencyPercentiles.P95
	snap.Summary.P99LatencyMS = snap.LatencyPercentiles.P99

	// Error rate
	if total > 0 {
		snap.ErrorRate = float64(errorCount) / float64(total) * 100
	}

	// Status breakdown
	for status, count := range statusCounts {
		snap.StatusBreakdown = append(snap.StatusBreakdown, StatusCount{
			Status: status,
			Count:  count,
			Pct:    float64(count) / float64(total) * 100,
		})
	}
	sort.Slice(snap.StatusBreakdown, func(i, j int) bool {
		return snap.StatusBreakdown[i].Status < snap.StatusBreakdown[j].Status
	})

	// Top endpoints
	for _, ea := range endpointMap {
		es := EndpointStats{
			Path:        ea.Path,
			Requests:    ea.Requests,
			AvgLatency:  float64(ea.TotalLatency) / float64(ea.Requests),
			TokensTotal: ea.TotalTokens,
		}
		if ea.Requests > 0 {
			es.ErrorRate = float64(ea.Errors) / float64(ea.Requests) * 100
		}
		snap.TopEndpoints = append(snap.TopEndpoints, es)
	}
	sort.Slice(snap.TopEndpoints, func(i, j int) bool {
		return snap.TopEndpoints[i].Requests > snap.TopEndpoints[j].Requests
	})
	if len(snap.TopEndpoints) > 15 {
		snap.TopEndpoints = snap.TopEndpoints[:15]
	}

	// Top models
	for _, ma := range modelMap {
		ms := ModelStats{
			Model:       ma.Model,
			Requests:    ma.Requests,
			AvgLatency:  float64(ma.TotalLatency) / float64(ma.Requests),
			TokensTotal: ma.TotalTokens,
		}
		if ma.Requests > 0 {
			ms.SuccessRate = float64(ma.Successes) / float64(ma.Requests) * 100
		}
		snap.TopModels = append(snap.TopModels, ms)
	}
	sort.Slice(snap.TopModels, func(i, j int) bool {
		return snap.TopModels[i].Requests > snap.TopModels[j].Requests
	})

	// Hourly traffic
	for _, hp := range hourMap {
		snap.HourlyTraffic = append(snap.HourlyTraffic, *hp)
	}
	sort.Slice(snap.HourlyTraffic, func(i, j int) bool {
		return snap.HourlyTraffic[i].Hour < snap.HourlyTraffic[j].Hour
	})

	// Tokens
	snap.TokensSummary = TokensSummary{
		TotalIn:   totalTokensIn,
		TotalOut:  totalTokensOut,
		Total:     totalTokensIn + totalTokensOut,
		AvgPerReq: float64(totalTokensIn+totalTokensOut) / float64(total),
	}

	return snap
}

// Handler returns an HTTP handler that serves the analytics snapshot.
func (a *Analytics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := a.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	}
}

// ─── Helpers ──────────────────────────────────────────────

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func normalizePath(path string) string {
	// Normalize API paths: /v1/chat/completions -> /v1/chat/completions
	// Group similar paths: /v1/models/gpt-4 -> /v1/models/{id}
	if len(path) > 0 && path[0] == '/' {
		parts := splitPath(path)
		// /v1/models/xxx -> /v1/models/{id}
		if len(parts) == 3 && parts[0] == "v1" && parts[1] == "models" {
			return "/v1/models"
		}
		// /api/admin/users/xxx -> /api/admin/users/{id}
		if len(parts) >= 4 && parts[0] == "api" && parts[1] == "admin" && parts[2] == "users" {
			return "/api/admin/users"
		}
	}
	return path
}

func splitPath(path string) []string {
	var parts []string
	start := 1
	for i := 1; i < len(path); i++ {
		if path[i] == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}

type endpointAccum struct {
	Path         string
	Requests     int64
	TotalLatency int64
	TotalTokens  int64
	Errors       int64
}

type modelAccum struct {
	Model       string
	Requests    int64
	TotalLatency int64
	TotalTokens int64
	Successes   int64
}

// LoadFromDB loads recent request logs into the analytics buffer from the database.
func (a *Analytics) LoadFromDB(limit int) error {
	if a.db == nil {
		return nil
	}
	rows, err := a.db.Query(`
		SELECT id, method, path, status, latency_ms, tokens_in, tokens_out,
		       model, account_id, channel_id, stream, response_class, error, created_at
		FROM request_logs
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return fmt.Errorf("load analytics: %w", err)
	}
	defer rows.Close()

	a.mu.Lock()
	defer a.mu.Unlock()

	for rows.Next() {
		var r RequestRecord
		var stream int
		if err := rows.Scan(&r.ID, &r.Method, &r.Path, &r.Status, &r.LatencyMS,
			&r.TokensIn, &r.TokensOut, &r.Model, &r.AccountID, &r.ChannelID,
			&stream, &r.ResponseClass, &r.Error, &r.CreatedAt); err != nil {
			continue
		}
		r.Stream = stream == 1
		a.recent = append(a.recent, r)
	}
	return rows.Err()
}
