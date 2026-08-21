package middleware

import (
	"net/http"
	"strconv"
	"strings"
)

// ValidationConfig configures request validation.
type ValidationConfig struct {
	MaxBodySize      int64    `json:"max_body_size"`       // bytes, 0 = unlimited
	AllowedMethods   []string `json:"allowed_methods"`     // nil = all
	RequiredHeaders  []string `json:"required_headers"`    // headers that must be present
	ContentTypeCheck bool     `json:"content_type_check"`  // enforce content-type for POST/PUT
	MaxQueryParams   int      `json:"max_query_params"`    // 0 = unlimited
}

// DefaultValidationConfig returns sensible defaults.
func DefaultValidationConfig() ValidationConfig {
	return ValidationConfig{
		MaxBodySize:      10 * 1024 * 1024, // 10MB
		AllowedMethods:   nil,
		RequiredHeaders:  nil,
		ContentTypeCheck: true,
		MaxQueryParams:   100,
	}
}

// Validate returns a middleware that validates incoming requests.
func Validate(config ValidationConfig) func(http.Handler) http.Handler {
	methodSet := make(map[string]bool, len(config.AllowedMethods))
	for _, m := range config.AllowedMethods {
		methodSet[strings.ToUpper(m)] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Method check
			if len(methodSet) > 0 && !methodSet[r.Method] {
				writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed: "+r.Method)
				return
			}

			// Query param count check
			if config.MaxQueryParams > 0 && len(r.URL.Query()) > config.MaxQueryParams {
				writeJSONError(w, http.StatusBadRequest, "too many query parameters")
				return
			}

			// Required headers check
			for _, h := range config.RequiredHeaders {
				if r.Header.Get(h) == "" {
					writeJSONError(w, http.StatusBadRequest, "missing required header: "+h)
					return
				}
			}

			// Content-Type check for body-bearing methods
			if config.ContentTypeCheck && (r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch) {
				ct := r.Header.Get("Content-Type")
				if ct == "" && r.ContentLength > 0 {
					writeJSONError(w, http.StatusBadRequest, "Content-Type header required for request body")
					return
				}
			}

			// Body size check
			if config.MaxBodySize > 0 && r.ContentLength > config.MaxBodySize {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				w.Write([]byte(`{"error":"payload too large","max_bytes":` + strconv.FormatInt(config.MaxBodySize, 10) + `}`))
				return
			}

			// Wrap body with size limit
			if config.MaxBodySize > 0 {
				r.Body = http.MaxBytesReader(w, r.Body, config.MaxBodySize)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"error":"` + msg + `"}`))
}
