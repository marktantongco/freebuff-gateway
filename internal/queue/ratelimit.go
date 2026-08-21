package queue

import (
	"sync"
	"time"
)

// --- Token Bucket Rate Limiter ---

// TokenBucket implements the token bucket rate limiting algorithm.
// Tokens are added at a fixed rate up to a maximum capacity.
// Each request consumes one token; requests wait if no tokens available.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewTokenBucket creates a new token bucket.
// rate = tokens per second, capacity = max burst size.
func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		tokens:     capacity,
		maxTokens:  capacity,
		refillRate: rate,
		lastRefill: time.Now(),
	}
}

// Allow checks if a request can proceed immediately.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN checks if N tokens can be consumed immediately.
func (tb *TokenBucket) AllowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// Wait blocks until a token is available or context is cancelled.
func (tb *TokenBucket) Wait() error {
	return tb.WaitN(1)
}

// WaitN blocks until N tokens are available.
func (tb *TokenBucket) WaitN(n int) error {
	for {
		if tb.AllowN(n) {
			return nil
		}

		// Calculate wait time
		tb.mu.Lock()
		needed := float64(n) - tb.tokens
		waitTime := time.Duration(needed / tb.refillRate * float64(time.Second))
		tb.mu.Unlock()

		if waitTime < time.Millisecond {
			waitTime = time.Millisecond
		}
		time.Sleep(waitTime)
	}
}

// Available returns the current number of available tokens.
func (tb *TokenBucket) Available() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now
}

// --- Sliding Window Rate Limiter ---

// SlidingWindow implements a sliding window rate limiter.
// It counts requests in a sliding time window.
type SlidingWindow struct {
	mu          sync.Mutex
	maxRequests int
	windowSize  time.Duration
	requests    []time.Time
}

// NewSlidingWindow creates a new sliding window rate limiter.
func NewSlidingWindow(maxRequests int, windowSize time.Duration) *SlidingWindow {
	return &SlidingWindow{
		maxRequests: maxRequests,
		windowSize:  windowSize,
		requests:    make([]time.Time, 0, maxRequests),
	}
}

// Allow checks if a request can proceed immediately.
func (sw *SlidingWindow) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.evict(now)

	if len(sw.requests) < sw.maxRequests {
		sw.requests = append(sw.requests, now)
		return true
	}
	return false
}

// Wait blocks until a request slot is available.
func (sw *SlidingWindow) Wait() {
	for {
		if sw.Allow() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Remaining returns the number of available request slots.
func (sw *SlidingWindow) Remaining() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.evict(time.Now())
	return sw.maxRequests - len(sw.requests)
}

func (sw *SlidingWindow) evict(now time.Time) {
	cutoff := now.Add(-sw.windowSize)
	i := 0
	for i < len(sw.requests) && sw.requests[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		sw.requests = sw.requests[i:]
	}
}

// --- Composite Rate Limiter ---

// RateLimiter combines token bucket and sliding window rate limiting.
// A request must pass both limiters to proceed.
type RateLimiter struct {
	bucket   *TokenBucket
	window   *SlidingWindow
	name     string
}

// NewRateLimiter creates a composite rate limiter.
func NewRateLimiter(name string, ratePerSec float64, maxBurst int, windowReqs int, windowSize time.Duration) *RateLimiter {
	return &RateLimiter{
		bucket: NewTokenBucket(ratePerSec, float64(maxBurst)),
		window: NewSlidingWindow(windowReqs, windowSize),
		name:   name,
	}
}

// Allow checks if a request can proceed immediately.
func (rl *RateLimiter) Allow() bool {
	return rl.bucket.Allow() && rl.window.Allow()
}

// Wait blocks until both limiters allow.
func (rl *RateLimiter) Wait() error {
	if err := rl.bucket.Wait(); err != nil {
		return err
	}
	rl.window.Wait()
	return nil
}

// Name returns the limiter name.
func (rl *RateLimiter) Name() string {
	return rl.name
}

// Stats returns current rate limiter stats.
func (rl *RateLimiter) Stats() RateLimiterStats {
	return RateLimiterStats{
		Name:      rl.name,
		Tokens:    rl.bucket.Available(),
		WindowRem: rl.window.Remaining(),
	}
}

// RateLimiterStats contains rate limiter metrics.
type RateLimiterStats struct {
	Name      string  `json:"name"`
	Tokens    float64 `json:"tokens"`
	WindowRem int     `json:"window_remaining"`
}

// --- Per-Provider Rate Limiter Manager ---

// RateLimiterManager manages multiple named rate limiters.
type RateLimiterManager struct {
	mu        sync.RWMutex
	limiters  map[string]*RateLimiter
}

// NewRateLimiterManager creates a new manager.
func NewRateLimiterManager() *RateLimiterManager {
	return &RateLimiterManager{
		limiters: make(map[string]*RateLimiter),
	}
}

// Register adds or replaces a rate limiter for a provider.
func (m *RateLimiterManager) Register(name string, rl *RateLimiter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limiters[name] = rl
}

// Get returns the rate limiter for a provider.
func (m *RateLimiterManager) Get(name string) *RateLimiter {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.limiters[name]
}

// Allow checks if a request for the given provider can proceed.
func (m *RateLimiterManager) Allow(provider string) bool {
	rl := m.Get(provider)
	if rl == nil {
		return true // No limiter = unlimited
	}
	return rl.Allow()
}

// Wait blocks until the provider's rate limiter allows.
func (m *RateLimiterManager) Wait(provider string) error {
	rl := m.Get(provider)
	if rl == nil {
		return nil
	}
	return rl.Wait()
}

// Stats returns stats for all registered limiters.
func (m *RateLimiterManager) Stats() []RateLimiterStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]RateLimiterStats, 0, len(m.limiters))
	for _, rl := range m.limiters {
		stats = append(stats, rl.Stats())
	}
	return stats
}
