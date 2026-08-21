package ratelimit

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// ClientStats tracks rate limit statistics for a single client.
type ClientStats struct {
	Key            string     `json:"key"`
	Requests       int64      `json:"requests"`
	Allowed        int64      `json:"allowed"`
	Rejected       int64      `json:"rejected"`
	LastRequestAt  time.Time  `json:"last_request_at"`
	FirstRequestAt time.Time  `json:"first_request_at"`
	AvgLatencyMS   float64    `json:"avg_latency_ms,omitempty"`
	RejectRate     float64    `json:"reject_rate"`
}

// TrackerConfig configures the rate limit tracker.
type TrackerConfig struct {
	MaxClients     int           `json:"max_clients"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	StaleAfter     time.Duration `json:"stale_after"`
}

// DefaultTrackerConfig returns sensible defaults.
func DefaultTrackerConfig() TrackerConfig {
	return TrackerConfig{
		MaxClients:      10000,
		CleanupInterval: 5 * time.Minute,
		StaleAfter:      30 * time.Minute,
	}
}

// Snapshot is the top-level view of all rate limit statistics.
type Snapshot struct {
	TotalRequests   int64          `json:"total_requests"`
	TotalAllowed    int64          `json:"total_allowed"`
	TotalRejected   int64          `json:"total_rejected"`
	ActiveClients   int            `json:"active_clients"`
	RejectRate      float64        `json:"reject_rate"`
	TopClients      []ClientStats  `json:"top_clients"`
	RecentRejections []ClientStats `json:"recent_rejections"`
	Timestamp       time.Time      `json:"timestamp"`
}

// Tracker records rate limit decisions for dashboard analytics.
type Tracker struct {
	config  TrackerConfig
	clients map[string]*clientEntry
	mu      sync.RWMutex
	stopCh  chan struct{}

	totalRequests int64
	totalAllowed  int64
	totalRejected int64
}

type clientEntry struct {
	stats      ClientStats
	lastAccess time.Time
}

// NewTracker creates a new rate limit tracker.
func NewTracker(config TrackerConfig) *Tracker {
	t := &Tracker{
		config:  config,
		clients: make(map[string]*clientEntry),
		stopCh:  make(chan struct{}),
	}
	if config.CleanupInterval > 0 {
		go t.cleanup()
	}
	return t
}

// RecordAllowed records an allowed request.
func (t *Tracker) RecordAllowed(key string, latencyMS int64) {
	t.record(key, true, latencyMS)
}

// RecordRejected records a rejected request.
func (t *Tracker) RecordRejected(key string) {
	t.record(key, false, 0)
}

func (t *Tracker) record(key string, allowed bool, latencyMS int64) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalRequests++
	if allowed {
		t.totalAllowed++
	} else {
		t.totalRejected++
	}

	entry, ok := t.clients[key]
	if !ok {
		if len(t.clients) >= t.config.MaxClients {
			return // drop rather than OOM
		}
		entry = &clientEntry{
			stats: ClientStats{
				Key:            key,
				FirstRequestAt: now,
			},
		}
		t.clients[key] = entry
	}

	entry.stats.Requests++
	entry.lastAccess = now
	entry.stats.LastRequestAt = now

	if allowed {
		entry.stats.Allowed++
		if latencyMS > 0 {
			// running average
			n := float64(entry.stats.Allowed)
			entry.stats.AvgLatencyMS = (entry.stats.AvgLatencyMS*(n-1) + float64(latencyMS)) / n
		}
	} else {
		entry.stats.Rejected++
	}

	if entry.stats.Requests > 0 {
		entry.stats.RejectRate = float64(entry.stats.Rejected) / float64(entry.stats.Requests) * 100
	}
}

// Snapshot returns the current rate limit statistics.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := Snapshot{
		TotalRequests: t.totalRequests,
		TotalAllowed:  t.totalAllowed,
		TotalRejected: t.totalRejected,
		ActiveClients: len(t.clients),
		Timestamp:     time.Now(),
	}

	if t.totalRequests > 0 {
		snap.RejectRate = float64(t.totalRejected) / float64(t.totalRequests) * 100
	}

	// Collect all clients
	all := make([]ClientStats, 0, len(t.clients))
	for _, entry := range t.clients {
		all = append(all, entry.stats)
	}

	// Top clients by request count
	sort.Slice(all, func(i, j int) bool { return all[i].Requests > all[j].Requests })
	if len(all) > 20 {
		snap.TopClients = all[:20]
	} else {
		snap.TopClients = all
	}

	// Recent rejections (sorted by reject count)
	rejections := make([]ClientStats, 0)
	for _, s := range all {
		if s.Rejected > 0 {
			rejections = append(rejections, s)
		}
	}
	sort.Slice(rejections, func(i, j int) bool { return rejections[i].Rejected > rejections[j].Rejected })
	if len(rejections) > 20 {
		snap.RecentRejections = rejections[:20]
	} else {
		snap.RecentRejections = rejections
	}

	return snap
}

func (t *Tracker) cleanup() {
	ticker := time.NewTicker(t.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.mu.Lock()
			cutoff := time.Now().Add(-t.config.StaleAfter)
			for key, entry := range t.clients {
				if entry.lastAccess.Before(cutoff) {
					delete(t.clients, key)
				}
			}
			t.mu.Unlock()
		case <-t.stopCh:
			return
		}
	}
}

// Stop terminates the cleanup goroutine.
func (t *Tracker) Stop() {
	close(t.stopCh)
}

// Handler returns an HTTP handler that serves the rate limit snapshot as JSON.
func (t *Tracker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := t.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	}
}

// Middleware returns HTTP middleware that records rate limit decisions.
func (t *Tracker) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := extractKey(r)
		start := time.Now()

		// Wrap ResponseWriter to capture status code
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)

		latencyMS := time.Since(start).Milliseconds()
		if rw.statusCode == http.StatusTooManyRequests {
			t.RecordRejected(key)
		} else {
			t.RecordAllowed(key, latencyMS)
		}
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func extractKey(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	addr := r.RemoteAddr
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}
