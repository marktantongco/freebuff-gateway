package alerting

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MetricsCollector exposes alerting metrics in Prometheus format.
type MetricsCollector struct {
	manager  *Manager
	exporter MetricsExporter

	// Metric descriptors
	alertsTotal      *MetricDesc
	alertsBySeverity *MetricDesc
	alertsByState    *MetricDesc
	alertsBySource   *MetricDesc
	notificationsSent *MetricDesc
	notificationsFailed *MetricDesc
	evaluationDuration *MetricDesc
	evaluationTotal    *MetricDesc
	historyEntries     *MetricDesc

	mu sync.RWMutex
}

// MetricsExporter is the interface for exporting metrics.
type MetricsExporter interface {
	NewCounter(name, help string) CounterMetric
	NewGauge(name, help string) GaugeMetric
	NewHistogram(name, help string, buckets []float64) HistogramMetric
}

// CounterMetric is a counter metric interface.
type CounterMetric interface {
	Inc()
	Add(float64)
}

// GaugeMetric is a gauge metric interface.
type GaugeMetric interface {
	Set(float64)
	Add(float64)
	Sub(float64)
}

// HistogramMetric is a histogram metric interface.
type HistogramMetric interface {
	Observe(float64)
}

// MetricDesc holds a metric descriptor.
type MetricDesc struct {
	Name string
	Help string
}

// NewMetricsCollector creates a new metrics collector for the alerting system.
func NewMetricsCollector(manager *Manager, exporter MetricsExporter) *MetricsCollector {
	return &MetricsCollector{
		manager:  manager,
		exporter: exporter,
		alertsTotal: &MetricDesc{
			Name: "freebuff_alerts_total",
			Help: "Total number of alerts created",
		},
		alertsBySeverity: &MetricDesc{
			Name: "freebuff_alerts_by_severity",
			Help: "Number of alerts grouped by severity",
		},
		alertsByState: &MetricDesc{
			Name: "freebuff_alerts_by_state",
			Help: "Number of alerts grouped by state",
		},
		alertsBySource: &MetricDesc{
			Name: "freebuff_alerts_by_source",
			Help: "Number of alerts grouped by source component",
		},
		notificationsSent: &MetricDesc{
			Name: "freebuff_alert_notifications_sent_total",
			Help: "Total number of notifications sent",
		},
		notificationsFailed: &MetricDesc{
			Name: "freebuff_alert_notifications_failed_total",
			Help: "Total number of failed notifications",
		},
		evaluationDuration: &MetricDesc{
			Name: "freebuff_alert_evaluation_duration_seconds",
			Help: "Duration of alert evaluation in seconds",
		},
		evaluationTotal: &MetricDesc{
			Name: "freebuff_alert_evaluations_total",
			Help: "Total number of alert evaluations",
		},
		historyEntries: &MetricDesc{
			Name: "freebuff_alert_history_entries_total",
			Help: "Total number of alert history entries",
		},
	}
}

// Collect gathers all alerting metrics and returns them in Prometheus format.
func (mc *MetricsCollector) Collect() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var sb strings.Builder

	stats := mc.manager.Stats()
	allAlerts := mc.manager.GetAlerts("", "")

	// Alert count by severity
	sevCounts := map[string]float64{
		"info":     0,
		"warning":  0,
		"critical": 0,
	}
	for _, a := range allAlerts {
		sevCounts[string(a.Severity)]++
	}

	// Alert count by state
	stateCounts := map[string]float64{
		"firing":   0,
		"resolved": 0,
		"silenced": 0,
	}
	for _, a := range allAlerts {
		stateCounts[string(a.State)]++
	}

	// Alert count by source
	sourceCounts := make(map[string]float64)
	for _, a := range allAlerts {
		sourceCounts[a.Source]++
	}

	// Write metrics
	mc.writeMetric(&sb, mc.alertsTotal.Name, mc.alertsTotal.Help, "counter", float64(stats["total"]), nil)

	for sev, count := range sevCounts {
		mc.writeMetric(&sb, mc.alertsBySeverity.Name, mc.alertsBySeverity.Help, "gauge", count, map[string]string{"severity": sev})
	}

	for state, count := range stateCounts {
		mc.writeMetric(&sb, mc.alertsByState.Name, mc.alertsByState.Help, "gauge", count, map[string]string{"state": state})
	}

	for source, count := range sourceCounts {
		mc.writeMetric(&sb, mc.alertsBySource.Name, mc.alertsBySource.Help, "gauge", count, map[string]string{"source": source})
	}

	// History entries count
	history := mc.manager.GetHistory(0)
	mc.writeMetric(&sb, mc.historyEntries.Name, mc.historyEntries.Help, "gauge", float64(len(history)), nil)

	// Notification metrics (from manager stats if available)
	if sent, ok := stats["notifications_sent"]; ok {
		mc.writeMetric(&sb, mc.notificationsSent.Name, mc.notificationsSent.Help, "counter", float64(sent), nil)
	}
	if failed, ok := stats["notifications_failed"]; ok {
		mc.writeMetric(&sb, mc.notificationsFailed.Name, mc.notificationsFailed.Help, "counter", float64(failed), nil)
	}

	return sb.String()
}

// CollectWithContext gathers metrics with context support.
func (mc *MetricsCollector) CollectWithContext(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
		return mc.Collect(), nil
	}
}

// StartCollection begins periodic metric collection.
func (mc *MetricsCollector) StartCollection(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mc.Collect()
			}
		}
	}()
}

func (mc *MetricsCollector) writeMetric(sb *strings.Builder, name, help, metricType string, value float64, labels map[string]string) {
	// Write help
	fmt.Fprintf(sb, "# HELP %s %s\n", name, help)
	fmt.Fprintf(sb, "# TYPE %s %s\n", name, metricType)

	// Write metric with labels
	if len(labels) > 0 {
		labelParts := make([]string, 0, len(labels))
		for k, v := range labels {
			labelParts = append(labelParts, fmt.Sprintf("%s=\"%s\"", k, v))
		}
		fmt.Fprintf(sb, "%s{%s} %.0f\n", name, strings.Join(labelParts, ","), value)
	} else {
		fmt.Fprintf(sb, "%s %.0f\n", name, value)
	}
}

// Handler returns an HTTP handler that serves alerting metrics.
func (mc *MetricsCollector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics := mc.Collect()
		w.Write([]byte(metrics))
	})
}

// DefaultBuckets returns the default histogram buckets for alerting metrics.
func DefaultBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}
