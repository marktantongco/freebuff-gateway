package middleware

import (
	"context"
	"net/http"
	"regexp"
	"strings"
)

type versionContextKey string

const apiVersionKey versionContextKey = "api_version"

// APIVersion configures version extraction behavior.
type APIVersion struct {
	Header  string `json:"header"`   // header to check, e.g. "X-API-Version"
	Prefix  string `json:"prefix"`   // URL prefix, e.g. "/v"
	Current string `json:"current"`  // current API version
	Valid   []string `json:"valid"`  // valid versions
}

// DefaultAPIVersion returns v1 API versioning.
func DefaultAPIVersion() APIVersion {
	return APIVersion{
		Header:  "X-API-Version",
		Prefix:  "/v",
		Current: "1",
		Valid:   []string{"1"},
	}
}

var versionPattern = regexp.MustCompile(`^/v(\d+)`)

// Version returns middleware that extracts and validates API version.
func Version(api APIVersion) func(http.Handler) http.Handler {
	validSet := make(map[string]bool, len(api.Valid))
	for _, v := range api.Valid {
		validSet[v] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			version := ""

			// Try header first
			if api.Header != "" {
				version = strings.TrimSpace(r.Header.Get(api.Header))
			}

			// Try URL path
			if version == "" {
				if matches := versionPattern.FindStringSubmatch(r.URL.Path); len(matches) > 1 {
					version = matches[1]
				}
			}

			// Default to current version
			if version == "" {
				version = api.Current
			}

			// Validate version
			if len(validSet) > 0 && !validSet[version] {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Accept-API-Version", strings.Join(api.Valid, ","))
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"unsupported API version: ` + version + `","supported":["` + strings.Join(api.Valid, `","`) + `"]}`))
				return
			}

			// Store in context
			ctx := context.WithValue(r.Context(), apiVersionKey, version)
			r = r.WithContext(ctx)

			// Set response header
			w.Header().Set("X-API-Version", version)

			next.ServeHTTP(w, r)
		})
	}
}

// GetAPIVersion extracts the API version from context.
func GetAPIVersion(ctx context.Context) string {
	if v, ok := ctx.Value(apiVersionKey).(string); ok {
		return v
	}
	return ""
}
