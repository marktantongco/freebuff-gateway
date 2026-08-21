package middleware

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// RateLimitConfig configures rate limiting behavior.
type RateLimitConfig struct {
	RequestsPerSecond float64       `json:"requests_per_second"`
	BurstSize         int           `json:"burst_size"`
	CleanupInterval   time.Duration `json:"cleanup_interval"`
}

// DefaultRateLimitConfig returns sensible defaults.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         20,
		CleanupInterval:   5 * time.Minute,
	}
}

type tokenBucket struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func (tb *tokenBucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

func (tb *tokenBucket) remaining() int {
	return int(tb.tokens)
}

// RateLimiter tracks per-key rate limiting.
type RateLimiter struct {
	config    RateLimitConfig
	buckets   map[string]*tokenBucket
	mu        sync.Mutex
	stopClean chan struct{}
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(config RateLimitConfig) *RateLimiter {
	rl := &RateLimiter{
		config:    config,
		buckets:   make(map[string]*tokenBucket),
		stopClean: make(chan struct{}),
	}

	// Start cleanup goroutine
	if config.CleanupInterval > 0 {
		go rl.cleanup()
	}

	return rl
}

func (rl *RateLimiter) getBucket(key string) *tokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	tb, ok := rl.buckets[key]
	if !ok {
		tb = &tokenBucket{
			tokens:     float64(rl.config.BurstSize),
			maxTokens:  float64(rl.config.BurstSize),
			refillRate: rl.config.RequestsPerSecond,
			lastRefill: time.Now(),
		}
		rl.buckets[key] = tb
	}
	return tb
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(rl.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			cutoff := time.Now().Add(-rl.config.CleanupInterval)
			for key, tb := range rl.buckets {
				if tb.lastRefill.Before(cutoff) {
					delete(rl.buckets, key)
				}
			}
			rl.mu.Unlock()
		case <-rl.stopClean:
			return
		}
	}
}

// Stop terminates the cleanup goroutine.
func (rl *RateLimiter) Stop() {
	close(rl.stopClean)
}

// Stats returns rate limiter statistics.
func (rl *RateLimiter) Stats() RateLimiterStats {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return RateLimiterStats{
		ActiveKeys: len(rl.buckets),
	}
}

// RateLimiterStats contains rate limiter metrics.
type RateLimiterStats struct {
	ActiveKeys int `json:"active_keys"`
}

// RateLimit returns middleware that rate-limits by client IP.
func RateLimit(config RateLimitConfig) func(http.Handler) http.Handler {
	rl := NewRateLimiter(config)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractClientKey(r)
			bucket := rl.getBucket(key)

			if !bucket.allow() {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.BurstSize))
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limit exceeded","message":"slow down"}`))
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.BurstSize))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(bucket.remaining()))

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimitByKey returns middleware that rate-limits by a custom key extractor.
func RateLimitByKey(config RateLimitConfig, keyFunc func(*http.Request) string) func(http.Handler) http.Handler {
	rl := NewRateLimiter(config)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)
			if key == "" {
				key = extractClientKey(r)
			}

			bucket := rl.getBucket(key)

			if !bucket.allow() {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.BurstSize))
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limit exceeded","message":"slow down"}`))
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.BurstSize))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(bucket.remaining()))

			next.ServeHTTP(w, r)
		})
	}
}

func extractClientKey(r *http.Request) string {
	// Check X-Forwarded-For first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}

	// Check X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	addr := r.RemoteAddr
	if idx := len(addr) - 1; idx > 0 {
		for i := idx; i >= 0; i-- {
			if addr[i] == ':' {
				return addr[:i]
			}
		}
	}
	return addr
}
