package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrometheusExporter_Counter(t *testing.T) {
	exporter := NewPrometheusExporter()
	counter := exporter.NewCounter("test_counter", "A test counter")

	// Increment counter
	counter.Inc()
	counter.Inc()
	counter.Add(5)

	if counter.value.Load() != 7 {
		t.Errorf("counter value = %d, want 7", counter.value.Load())
	}
}

func TestPrometheusExporter_Gauge(t *testing.T) {
	exporter := NewPrometheusExporter()
	gauge := exporter.NewGauge("test_gauge", "A test gauge")

	gauge.Set(10)
	if gauge.value.Load() != 10 {
		t.Errorf("gauge value = %d, want 10", gauge.value.Load())
	}

	gauge.Inc()
	if gauge.value.Load() != 11 {
		t.Errorf("gauge value after Inc = %d, want 11", gauge.value.Load())
	}

	gauge.Dec()
	if gauge.value.Load() != 10 {
		t.Errorf("gauge value after Dec = %d, want 10", gauge.value.Load())
	}
}

func TestPrometheusExporter_Histogram(t *testing.T) {
	exporter := NewPrometheusExporter()
	histogram := exporter.NewHistogram("test_histogram", "A test histogram", []float64{0.1, 0.5, 1.0})

	histogram.Observe(0.05)
	histogram.Observe(0.3)
	histogram.Observe(0.8)
	histogram.Observe(2.0)

	if histogram.count.Load() != 4 {
		t.Errorf("histogram count = %d, want 4", histogram.count.Load())
	}

	// Check bucket counts
	// 0.05 <= 0.1 -> bucket 0
	// 0.3 <= 0.5 -> bucket 1
	// 0.8 <= 1.0 -> bucket 2
	// 2.0 > 1.0 -> bucket 3 (+Inf)
	if histogram.counts[0].Load() != 1 {
		t.Errorf("bucket 0 count = %d, want 1", histogram.counts[0].Load())
	}
	if histogram.counts[1].Load() != 1 {
		t.Errorf("bucket 1 count = %d, want 1", histogram.counts[1].Load())
	}
	if histogram.counts[2].Load() != 1 {
		t.Errorf("bucket 2 count = %d, want 1", histogram.counts[2].Load())
	}
	if histogram.counts[3].Load() != 1 {
		t.Errorf("bucket 3 count = %d, want 1", histogram.counts[3].Load())
	}
}

func TestPrometheusExporter_Handler(t *testing.T) {
	exporter := NewPrometheusExporter()
	counter := exporter.NewCounter("http_requests_total", "Total HTTP requests")
	counter.Inc()
	counter.Inc()

	handler := exporter.Handler()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handler status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "http_requests_total") {
		t.Error("response should contain http_requests_total")
	}
	if !contains(body, "# TYPE http_requests_total counter") {
		t.Error("response should contain type declaration")
	}
}

func TestPrometheusExporter_HandlerContentType(t *testing.T) {
	exporter := NewPrometheusExporter()
	handler := exporter.Handler()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Content-Type = %q, want Prometheus format", contentType)
	}
}

func TestHealthChecker_CheckHealth(t *testing.T) {
	checker := NewHealthChecker("1.0.0")

	// Register a custom check
	checker.RegisterCheck("custom", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{
			Name:      "custom",
			Status:    HealthStatusHealthy,
			Message:   "Custom component OK",
			LastCheck: time.Now(),
		}
	})

	response := checker.CheckHealth(context.Background())

	if response.Status != HealthStatusHealthy {
		t.Errorf("status = %v, want %v", response.Status, HealthStatusHealthy)
	}

	if response.Version != "1.0.0" {
		t.Errorf("version = %q, want %q", response.Version, "1.0.0")
	}

	if _, ok := response.Components["custom"]; !ok {
		t.Error("response should contain custom component")
	}

	if _, ok := response.Components["memory"]; !ok {
		t.Error("response should contain memory component")
	}

	if _, ok := response.Components["goroutines"]; !ok {
		t.Error("response should contain goroutines component")
	}
}

func TestHealthChecker_Handler(t *testing.T) {
	checker := NewHealthChecker("1.0.0")
	handler := checker.Handler()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handler status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "healthy") {
		t.Error("response should contain healthy status")
	}
}

func TestHealthChecker_ReadyHandler(t *testing.T) {
	checker := NewHealthChecker("1.0.0")
	handler := checker.ReadyHandler()

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handler status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if body != "ready" {
		t.Errorf("body = %q, want %q", body, "ready")
	}
}

func TestHealthChecker_LiveHandler(t *testing.T) {
	checker := NewHealthChecker("1.0.0")
	handler := checker.LiveHandler()

	req := httptest.NewRequest("GET", "/livez", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handler status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if body != "alive" {
		t.Errorf("body = %q, want %q", body, "alive")
	}
}

func TestMetricsMiddleware(t *testing.T) {
	exporter := NewPrometheusExporter()
	middleware := NewMetricsMiddleware(exporter)

	// Track a request
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Check that metrics were recorded
	if exporter.counters["http_requests_total"].value.Load() != 1 {
		t.Error("request count should be 1")
	}
}

func TestMetricsMiddleware_WrapFunc(t *testing.T) {
	exporter := NewPrometheusExporter()
	middleware := NewMetricsMiddleware(exporter)

	handler := middleware.WrapFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})

	req := httptest.NewRequest("POST", "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", w.Code, http.StatusCreated)
	}
}

func TestResponseWriter(t *testing.T) {
	w := httptest.NewRecorder()
	rw := newResponseWriter(w)

	// Write header
	rw.WriteHeader(http.StatusNotFound)

	// Write body
	rw.Write([]byte("not found"))

	if rw.statusCode != http.StatusNotFound {
		t.Errorf("statusCode = %d, want %d", rw.statusCode, http.StatusNotFound)
	}

	if rw.bytes != 9 {
		t.Errorf("bytes = %d, want 9", rw.bytes)
	}
}

func TestFormatLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "empty",
			labels: map[string]string{},
			want:   "",
		},
		{
			name:   "single",
			labels: map[string]string{"method": "GET"},
			want:   "{method=\"GET\"}",
		},
		{
			name:   "multiple",
			labels: map[string]string{"method": "GET", "path": "/test"},
			want:   "", // Order may vary
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatLabels(tt.labels)
			if tt.name == "multiple" {
				// Just check it contains both labels
				if !contains(got, "method=\"GET\"") || !contains(got, "path=\"/test\"") {
					t.Errorf("formatLabels() = %q, should contain both labels", got)
				}
			} else if got != tt.want {
				t.Errorf("formatLabels() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultBuckets(t *testing.T) {
	buckets := DefaultBuckets()
	if len(buckets) == 0 {
		t.Error("DefaultBuckets should not be empty")
	}
}

func TestLatencyBuckets(t *testing.T) {
	buckets := LatencyBuckets()
	if len(buckets) == 0 {
		t.Error("LatencyBuckets should not be empty")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
