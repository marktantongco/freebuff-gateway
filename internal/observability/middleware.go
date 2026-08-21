package observability

import (
	"net/http"
	"time"
)

// responseWriter wraps http.ResponseWriter to capture status code and size
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int
	startTime  time.Time
}

// newResponseWriter creates a new response writer wrapper
func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		startTime:      time.Now(),
	}
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Write captures the bytes written
func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += n
	return n, err
}

// Flush implements http.Flusher
func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// MetricsMiddleware tracks HTTP request metrics
type MetricsMiddleware struct {
	exporter     *PrometheusExporter
	requestTotal *Counter
	requestDuration *Histogram
	requestSize *Histogram
	responseSize *Histogram
	activeRequests *Gauge
}

// NewMetricsMiddleware creates a new metrics middleware
func NewMetricsMiddleware(exporter *PrometheusExporter) *MetricsMiddleware {
	return &MetricsMiddleware{
		exporter: exporter,
		requestTotal: exporter.NewCounter(
			"http_requests_total",
			"Total number of HTTP requests",
		),
		requestDuration: exporter.NewHistogram(
			"http_request_duration_seconds",
			"HTTP request duration in seconds",
			LatencyBuckets(),
		),
		requestSize: exporter.NewHistogram(
			"http_request_size_bytes",
			"HTTP request body size in bytes",
			SizeBuckets(),
		),
		responseSize: exporter.NewHistogram(
			"http_response_size_bytes",
			"HTTP response body size in bytes",
			SizeBuckets(),
		),
		activeRequests: exporter.NewGauge(
			"http_active_requests",
			"Number of active HTTP requests",
		),
	}
}

// Wrap wraps an http.Handler with metrics collection
func (mm *MetricsMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Increment active requests
		mm.activeRequests.Inc()
		defer mm.activeRequests.Dec()

		// Wrap response writer
		rw := newResponseWriter(w)

		// Record request size
		if r.ContentLength > 0 {
			mm.requestSize.Observe(float64(r.ContentLength))
		}

		// Start timer
		start := time.Now()

		// Call next handler
		next.ServeHTTP(rw, r)

		// Calculate duration
		duration := time.Since(start).Seconds()

		// Record metrics
		mm.requestDuration.Observe(duration)
		mm.requestTotal.Inc()
		mm.responseSize.Observe(float64(rw.bytes))
	})
}

// WrapFunc wraps an http.HandlerFunc with metrics collection
func (mm *MetricsMiddleware) WrapFunc(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mm.Wrap(http.HandlerFunc(next)).ServeHTTP(w, r)
	}
}

// RequestMetrics holds metrics for a single request
type RequestMetrics struct {
	Method       string
	Path         string
	StatusCode   int
	Duration     time.Duration
	RequestSize  int64
	ResponseSize int
}

// RequestLogger logs request details
type RequestLogger struct {
	verbose bool
}

// NewRequestLogger creates a new request logger
func NewRequestLogger(verbose bool) *RequestLogger {
	return &RequestLogger{verbose: verbose}
}

// Wrap wraps an http.Handler with request logging
func (rl *RequestLogger) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer
		rw := newResponseWriter(w)

		// Call next handler
		next.ServeHTTP(rw, r)

		// Log request
		if rl.verbose {
			duration := time.Since(start)
			status := rw.statusCode
			if status >= 500 {
				// Log errors
				logError(r.Method, r.URL.Path, status, duration)
			} else if rl.verbose {
				logRequest(r.Method, r.URL.Path, status, duration)
			}
		}
	})
}

// logRequest logs a successful request
func logRequest(method, path string, status int, duration time.Duration) {
	// In production, use structured logging
	// fmt.Printf("[%s] %s %s %d %v\n", time.Now().Format("15:04:05"), method, path, status, duration)
}

// logError logs a failed request
func logError(method, path string, status int, duration time.Duration) {
	// In production, use structured logging
	// fmt.Printf("[%s] ERROR %s %s %d %v\n", time.Now().Format("15:04:05"), method, path, status, duration)
}

// Suppress unused import warnings
var _ = time.Now
