package middleware

import (
	"log"
	"net/http"
	"runtime/debug"
)

// Recovery catches panics and returns a 500 error.
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				stack := debug.Stack()
				log.Printf("[PANIC] %s %s: %v\n%s", r.Method, r.URL.Path, rec, stack)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error":"internal server error","message":"an unexpected error occurred"}`))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// RecoveryWithLogger returns a recovery middleware with a custom logger.
func RecoveryWithLogger(logger func(format string, args ...interface{})) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					stack := debug.Stack()
					logger("[PANIC] %s %s: %v\n%s", r.Method, r.URL.Path, rec, stack)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(`{"error":"internal server error","message":"an unexpected error occurred"}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
