package middleware

import "net/http"

// Chain composes multiple middleware into a single handler.
// Middleware are applied in order: first listed = outermost (runs first).
func Chain(handler http.Handler, middleware ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

// ChainFunc is like Chain but takes an http.HandlerFunc.
func ChainFunc(handler http.HandlerFunc, middleware ...func(http.Handler) http.Handler) http.Handler {
	return Chain(handler, middleware...)
}

// DefaultChain returns the standard middleware stack for the gateway.
func DefaultChain(handler http.Handler, config ChainConfig) http.Handler {
	mws := []func(http.Handler) http.Handler{
		// Order matters: outermost first
		Recovery,                      // 1. Catch panics
		Logger,                        // 2. Log requests
		RequestID,                     // 3. Generate request ID
		CORS(config.CORS),             // 4. Handle CORS
		Version(config.APIVersion),     // 5. Extract API version
		Validate(config.Validation),   // 6. Validate requests
		RateLimit(config.RateLimit),   // 7. Rate limit
	}

	return Chain(handler, mws...)
}

// ChainConfig holds all middleware configuration.
type ChainConfig struct {
	CORS       CORSConfig       `json:"cors"`
	RateLimit  RateLimitConfig  `json:"rate_limit"`
	Validation ValidationConfig `json:"validation"`
	APIVersion APIVersion       `json:"api_version"`
}

// DefaultChainConfig returns sensible defaults.
func DefaultChainConfig() ChainConfig {
	return ChainConfig{
		CORS:       DefaultCORSConfig(),
		RateLimit:  DefaultRateLimitConfig(),
		Validation: DefaultValidationConfig(),
		APIVersion: DefaultAPIVersion(),
	}
}

// AdminChain returns middleware stack for admin endpoints (no rate limiting).
func AdminChain(handler http.Handler, config ChainConfig) http.Handler {
	mws := []func(http.Handler) http.Handler{
		Recovery,
		Logger,
		RequestID,
		CORS(config.CORS),
		Validate(config.Validation),
	}

	return Chain(handler, mws...)
}

// PublicChain returns a lighter middleware stack for public endpoints.
func PublicChain(handler http.Handler, config ChainConfig) http.Handler {
	mws := []func(http.Handler) http.Handler{
		Recovery,
		Logger,
		RequestID,
		CORS(config.CORS),
		Version(config.APIVersion),
		RateLimit(config.RateLimit),
	}

	return Chain(handler, mws...)
}
