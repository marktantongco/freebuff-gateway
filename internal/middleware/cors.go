package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CORSConfig configures CORS behavior.
type CORSConfig struct {
	AllowedOrigins   []string      `json:"allowed_origins"`
	AllowedMethods   []string      `json:"allowed_methods"`
	AllowedHeaders   []string      `json:"allowed_headers"`
	ExposedHeaders   []string      `json:"exposed_headers"`
	AllowCredentials bool          `json:"allow_credentials"`
	MaxAge           time.Duration `json:"max_age"`
}

// DefaultCORSConfig returns permissive CORS for development.
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "X-Request-ID", "X-API-Key"},
		ExposedHeaders:   []string{"X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining"},
		AllowCredentials: false,
		MaxAge:           12 * time.Hour,
	}
}

// CORS returns a middleware that handles CORS preflight and headers.
func CORS(config CORSConfig) func(http.Handler) http.Handler {
	origins := make(map[string]bool, len(config.AllowedOrigins))
	for _, o := range config.AllowedOrigins {
		origins[strings.ToLower(o)] = true
	}

	methods := strings.Join(config.AllowedMethods, ", ")
	headers := strings.Join(config.AllowedHeaders, ", ")
	exposed := strings.Join(config.ExposedHeaders, ", ")
	maxAge := strconv.Itoa(int(config.MaxAge.Seconds()))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			allowed := false
			if origins["*"] {
				allowed = true
				if config.AllowCredentials {
					// Can't use * with credentials
					allowed = origin != "" && origins[strings.ToLower(origin)]
				}
			} else if origin != "" {
				allowed = origins[strings.ToLower(origin)]
			}

			if allowed {
				if origins["*"] && !config.AllowCredentials {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
			}

			if config.AllowCredentials {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}

			if exposed != "" {
				w.Header().Set("Access-Control-Expose-Headers", exposed)
			}

			// Handle preflight
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", methods)
				w.Header().Set("Access-Control-Allow-Headers", headers)
				w.Header().Set("Access-Control-Max-Age", maxAge)
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
