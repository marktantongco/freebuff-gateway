package observability

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PrometheusExporter exports metrics in Prometheus format
type PrometheusExporter struct {
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	mu         sync.RWMutex
}

// Counter is a monotonically increasing metric
type Counter struct {
	name   string
	help   string
	value  atomic.Int64
	labels map[string]string
}

// Gauge is a metric that can go up and down
type Gauge struct {
	name   string
	help   string
	value  atomic.Int64
	labels map[string]string
}

// Histogram tracks value distributions
type Histogram struct {
	name    string
	help    string
	buckets []float64
	counts  []atomic.Int64
	sum     atomic.Int64
	count   atomic.Int64
	labels  map[string]string
}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter() *PrometheusExporter {
	return &PrometheusExporter{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

// NewCounter creates or gets a counter
func (pe *PrometheusExporter) NewCounter(name, help string) *Counter {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if c, ok := pe.counters[name]; ok {
		return c
	}
	c := &Counter{name: name, help: help, labels: make(map[string]string)}
	pe.counters[name] = c
	return c
}

// Inc increments a counter
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds a value to a counter
func (c *Counter) Add(n int64) {
	c.value.Add(n)
}

// NewGauge creates or gets a gauge
func (pe *PrometheusExporter) NewGauge(name, help string) *Gauge {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if g, ok := pe.gauges[name]; ok {
		return g
	}
	g := &Gauge{name: name, help: help, labels: make(map[string]string)}
	pe.gauges[name] = g
	return g
}

// Set sets a gauge value
func (g *Gauge) Set(n int64) {
	g.value.Store(n)
}

// Inc increments a gauge
func (g *Gauge) Inc() {
	g.value.Add(1)
}

// Dec decrements a gauge
func (g *Gauge) Dec() {
	g.value.Add(-1)
}

// NewHistogram creates or gets a histogram
func (pe *PrometheusExporter) NewHistogram(name, help string, buckets []float64) *Histogram {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if h, ok := pe.histograms[name]; ok {
		return h
	}
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	h := &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]atomic.Int64, len(buckets)+1),
		labels:  make(map[string]string),
	}
	pe.histograms[name] = h
	return h
}

// Observe adds a value to a histogram
func (h *Histogram) Observe(value float64) {
	h.sum.Add(int64(value * 1000)) // Store as microseconds for precision
	h.count.Add(1)

	for i, bucket := range h.buckets {
		if value <= bucket {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(h.buckets)].Add(1) // +Inf bucket
}

// Handler returns an HTTP handler for Prometheus metrics
func (pe *PrometheusExporter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		pe.mu.RLock()
		defer pe.mu.RUnlock()

		// Write counters
		for _, c := range pe.counters {
			fmt.Fprintf(w, "# HELP %s %s\n", c.name, c.help)
			fmt.Fprintf(w, "# TYPE %s counter\n", c.name)
			labels := formatLabels(c.labels)
			fmt.Fprintf(w, "%s%s %d\n", c.name, labels, c.value.Load())
		}

		// Write gauges
		for _, g := range pe.gauges {
			fmt.Fprintf(w, "# HELP %s %s\n", g.name, g.help)
			fmt.Fprintf(w, "# TYPE %s gauge\n", g.name)
			labels := formatLabels(g.labels)
			fmt.Fprintf(w, "%s%s %d\n", g.name, labels, g.value.Load())
		}

		// Write histograms
		for _, h := range pe.histograms {
			fmt.Fprintf(w, "# HELP %s %s\n", h.name, h.help)
			fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)

			cumCount := int64(0)
			for i, bucket := range h.buckets {
				cumCount += h.counts[i].Load()
				fmt.Fprintf(w, "%s_bucket{le=\"%.4f\"} %d\n", h.name, bucket, cumCount)
			}
			cumCount += h.counts[len(h.buckets)].Load()
			fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, cumCount)
			fmt.Fprintf(w, "%s_sum %d\n", h.name, h.sum.Load())
			fmt.Fprintf(w, "%s_count %d\n", h.name, h.count.Load())
		}
	})
}

// formatLabels formats labels for Prometheus output
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// DefaultBuckets returns default histogram buckets
func DefaultBuckets() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}

// LatencyBuckets returns buckets for latency measurement (in seconds)
func LatencyBuckets() []float64 {
	return []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

// SizeBuckets returns buckets for request/response size (in bytes)
func SizeBuckets() []float64 {
	return []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}
}

// Global default exporter
var defaultExporter = NewPrometheusExporter()

// Default returns the default Prometheus exporter
func Default() *PrometheusExporter {
	return defaultExporter
}

// Suppress unused import warnings
var _ = time.Now
