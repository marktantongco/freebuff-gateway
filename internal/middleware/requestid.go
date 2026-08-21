package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestID injects a unique request ID into each request.
// It checks for an existing X-Request-ID header first.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = generateRequestID()
		}

		// Store in context
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		r = r.WithContext(ctx)

		// Set response header
		w.Header().Set("X-Request-ID", id)

		next.ServeHTTP(w, r)
	})
}

// GetRequestID extracts the request ID from context.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

func generateRequestID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// RequestIDFromHeader returns a middleware that reads request ID from a custom header.
func RequestIDFromHeader(header string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSpace(r.Header.Get(header))
			if id == "" {
				id = generateRequestID()
			}

			ctx := context.WithValue(r.Context(), requestIDKey, id)
			r = r.WithContext(ctx)
			w.Header().Set("X-Request-ID", id)

			next.ServeHTTP(w, r)
		})
	}
}
