package queue

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen    = errors.New("circuit: circuit breaker is open")
	ErrCircuitTimeout = errors.New("circuit: request timed out in half-open")
)

// CircuitState represents the state of the circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // Normal operation
	CircuitOpen                         // Failing, requests rejected
	CircuitHalfOpen                     // Testing if service recovered
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	FailureThreshold int           `json:"failure_threshold"` // failures before opening
	SuccessThreshold int           `json:"success_threshold"` // successes before closing from half-open
	Timeout          time.Duration `json:"timeout"`           // time before half-open
	MaxHalfOpen      int           `json:"max_half_open"`     // max concurrent requests in half-open
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
		MaxHalfOpen:      3,
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	mu              sync.Mutex
	config          CircuitBreakerConfig
	state           CircuitState
	failures        int
	successes       int
	lastFailure     time.Time
	lastStateChange time.Time
	halfOpenCount   int
	name            string

	// Stats
	totalRequests  int64
	totalFailures  int64
	totalSuccesses int64
	totalRejected  int64
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(name string, config CircuitBreakerConfig) *CircuitBreaker {
	now := time.Now()
	return &CircuitBreaker{
		config:          config,
		state:           CircuitClosed,
		lastStateChange: now,
		name:            name,
	}
}

// Execute runs the given function through the circuit breaker.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if err := cb.allowRequest(); err != nil {
		return err
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// ExecuteWithFallback runs the function with a fallback on circuit open.
func (cb *CircuitBreaker) ExecuteWithFallback(fn func() error, fallback func(error) error) error {
	if err := cb.allowRequest(); err != nil {
		if fallback != nil {
			return fallback(err)
		}
		return err
	}

	err := fn()
	cb.recordResult(err)
	return err
}

func (cb *CircuitBreaker) allowRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return nil

	case CircuitOpen:
		// Check if timeout has elapsed → transition to half-open
		if time.Since(cb.lastStateChange) > cb.config.Timeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCount = 0
			cb.successes = 0
			cb.lastStateChange = time.Now()
			return nil
		}
		cb.totalRejected++
		return ErrCircuitOpen

	case CircuitHalfOpen:
		if cb.halfOpenCount >= cb.config.MaxHalfOpen {
			cb.totalRejected++
			return ErrCircuitOpen
		}
		cb.halfOpenCount++
		return nil
	}

	return nil
}

func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalRequests++

	if err != nil {
		cb.totalFailures++
		cb.failures++
		cb.lastFailure = time.Now()

		switch cb.state {
		case CircuitClosed:
			if cb.failures >= cb.config.FailureThreshold {
				cb.state = CircuitOpen
				cb.lastStateChange = time.Now()
			}
		case CircuitHalfOpen:
			// Any failure in half-open → back to open
			cb.state = CircuitOpen
			cb.lastStateChange = time.Now()
			cb.failures = 1
		}
	} else {
		cb.totalSuccesses++
		cb.successes++

		switch cb.state {
		case CircuitClosed:
			cb.failures = 0 // Reset failure count on success
		case CircuitHalfOpen:
			if cb.successes >= cb.config.SuccessThreshold {
				cb.state = CircuitClosed
				cb.failures = 0
				cb.lastStateChange = time.Now()
			}
		}
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check for automatic transition from open to half-open
	if cb.state == CircuitOpen && time.Since(cb.lastStateChange) > cb.config.Timeout {
		cb.state = CircuitHalfOpen
		cb.halfOpenCount = 0
		cb.successes = 0
		cb.lastStateChange = time.Now()
	}

	return cb.state
}

// Reset forces the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenCount = 0
	cb.lastStateChange = time.Now()
}

// Stats returns circuit breaker metrics.
func (cb *CircuitBreaker) Stats() CircuitStats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return CircuitStats{
		Name:           cb.name,
		State:          cb.state.String(),
		Failures:       cb.failures,
		Successes:      cb.successes,
		TotalRequests:  cb.totalRequests,
		TotalFailures:  cb.totalFailures,
		TotalSuccesses: cb.totalSuccesses,
		TotalRejected:  cb.totalRejected,
		LastFailure:    cb.lastFailure,
		LastStateChange: cb.lastStateChange,
	}
}

// CircuitStats contains circuit breaker metrics.
type CircuitStats struct {
	Name            string    `json:"name"`
	State           string    `json:"state"`
	Failures        int       `json:"failures"`
	Successes       int       `json:"successes"`
	TotalRequests   int64     `json:"total_requests"`
	TotalFailures   int64     `json:"total_failures"`
	TotalSuccesses  int64     `json:"total_successes"`
	TotalRejected   int64     `json:"total_rejected"`
	LastFailure     time.Time `json:"last_failure"`
	LastStateChange time.Time `json:"last_state_change"`
}

// --- Circuit Breaker Manager ---

// CircuitBreakerManager manages multiple named circuit breakers.
type CircuitBreakerManager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerManager creates a new manager.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate returns the circuit breaker for a provider, creating one if needed.
func (m *CircuitBreakerManager) GetOrCreate(name string, config ...CircuitBreakerConfig) *CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, ok := m.breakers[name]; ok {
		return cb
	}

	cfg := DefaultCircuitBreakerConfig()
	if len(config) > 0 {
		cfg = config[0]
	}

	cb := NewCircuitBreaker(name, cfg)
	m.breakers[name] = cb
	return cb
}

// Get returns the circuit breaker for a provider.
func (m *CircuitBreakerManager) Get(name string) *CircuitBreaker {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.breakers[name]
}

// Stats returns stats for all circuit breakers.
func (m *CircuitBreakerManager) Stats() []CircuitStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]CircuitStats, 0, len(m.breakers))
	for _, cb := range m.breakers {
		stats = append(stats, cb.Stats())
	}
	return stats
}

// ResetAll resets all circuit breakers to closed.
func (m *CircuitBreakerManager) ResetAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cb := range m.breakers {
		cb.Reset()
	}
}
