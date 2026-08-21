package provider

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"time"
)

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	RetryableCodes []int
}

// DefaultRetryConfig returns sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		RetryableCodes: []int{
			http.StatusTooManyRequests,    // 429
			http.StatusBadGateway,         // 502
			http.StatusServiceUnavailable, // 503
			http.StatusGatewayTimeout,     // 504
		},
	}
}

// IsRetryable checks if the error is retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if err == context.DeadlineExceeded {
		return true
	}
	if netErr, ok := err.(interface {
		Timeout() bool
	}); ok && netErr.Timeout() {
		return true
	}
	return false
}

// IsRetryableStatus checks if the HTTP status code is retryable.
func IsRetryableStatus(code int, retryableCodes []int) bool {
	for _, c := range retryableCodes {
		if code == c {
			return true
		}
	}
	return false
}

// RetryableError wraps an error with retry information.
type RetryableError struct {
	Err        error
	Retryable  bool
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// ExecuteWithRetry runs a function with retry logic.
func ExecuteWithRetry(ctx context.Context, config RetryConfig, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if !IsRetryable(lastErr) {
			return lastErr
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		backoff := calculateBackoff(config, attempt)

		if retryErr, ok := lastErr.(*RetryableError); ok && retryErr.RetryAfter > 0 {
			backoff = retryErr.RetryAfter
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return lastErr
}

// calculateBackoff computes exponential backoff with jitter.
func calculateBackoff(config RetryConfig, attempt int) time.Duration {
	backoff := float64(config.InitialBackoff) * math.Pow(config.Multiplier, float64(attempt))

	jitter := backoff * 0.25 * (rand.Float64()*2 - 1)
	backoff += jitter

	if backoff > float64(config.MaxBackoff) {
		backoff = float64(config.MaxBackoff)
	}

	return time.Duration(backoff)
}

// ProviderWithRetry wraps a provider adapter with retry logic.
type ProviderWithRetry struct {
	adapter     Adapter
	retryConfig RetryConfig
}

// NewProviderWithRetry creates a retry-enabled provider wrapper.
func NewProviderWithRetry(adapter Adapter, retryConfig RetryConfig) *ProviderWithRetry {
	return &ProviderWithRetry{
		adapter:     adapter,
		retryConfig: retryConfig,
	}
}

// ID returns the adapter ID.
func (p *ProviderWithRetry) ID() string {
	return p.adapter.ID()
}

// Type returns the provider type.
func (p *ProviderWithRetry) Type() ProviderType {
	return p.adapter.Type()
}

// Name returns the adapter name.
func (p *ProviderWithRetry) Name() string {
	return p.adapter.Name()
}

// SendRequest delegates with retry.
func (p *ProviderWithRetry) SendRequest(ctx context.Context, req *Request) (*Response, error) {
	var resp *Response
	err := ExecuteWithRetry(ctx, p.retryConfig, func() error {
		var err error
		resp, err = p.adapter.SendRequest(ctx, req)
		return err
	})
	return resp, err
}

// StreamRequest delegates with retry.
func (p *ProviderWithRetry) StreamRequest(ctx context.Context, req *Request) (<-chan *StreamChunk, error) {
	var chunks <-chan *StreamChunk
	err := ExecuteWithRetry(ctx, p.retryConfig, func() error {
		var err error
		chunks, err = p.adapter.StreamRequest(ctx, req)
		return err
	})
	return chunks, err
}

// HealthCheck delegates with retry.
func (p *ProviderWithRetry) HealthCheck(ctx context.Context) error {
	return ExecuteWithRetry(ctx, p.retryConfig, func() error {
		return p.adapter.HealthCheck(ctx)
	})
}

// SupportsModel delegates to the underlying adapter.
func (p *ProviderWithRetry) SupportsModel(model string) bool {
	return p.adapter.SupportsModel(model)
}

// ListModels delegates to the underlying adapter.
func (p *ProviderWithRetry) ListModels() []string {
	return p.adapter.ListModels()
}

// MaxTokens delegates to the underlying adapter.
func (p *ProviderWithRetry) MaxTokens(model string) int {
	return p.adapter.MaxTokens(model)
}
