package alerting

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Manager orchestrates health check alerting.
type Manager struct {
	config     AlertConfig
	notifiers  []Notifier
	alerts     map[string]*Alert       // fingerprint → alert
	history    []AlertHistory
	mu         sync.RWMutex
	running    bool
	cancel     context.CancelFunc
	onAlert    func(*Alert)            // callback for UI/API
	logger     Logger
}

// Logger is a simple logging interface.
type Logger interface {
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// nopLogger discards all log output.
type nopLogger struct{}

func (nopLogger) Info(msg string, args ...interface{})  {}
func (nopLogger) Warn(msg string, args ...interface{})  {}
func (nopLogger) Error(msg string, args ...interface{}) {}

// NewManager creates a new alert manager.
func NewManager(config AlertConfig, logger Logger) *Manager {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Manager{
		config:    config,
		notifiers: make([]Notifier, 0),
		alerts:    make(map[string]*Alert),
		history:   make([]AlertHistory, 0),
		logger:    logger,
	}
}

// AddNotifier adds a notification channel.
func (m *Manager) AddNotifier(n Notifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifiers = append(m.notifiers, n)
}

// SetOnAlert sets a callback invoked on every state change.
func (m *Manager) SetOnAlert(fn func(*Alert)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onAlert = fn
}

// Start begins the periodic health-check loop.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	ctx, m.cancel = context.WithCancel(ctx)
	m.mu.Unlock()

	go m.run(ctx)
}

// Stop halts the periodic loop.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
	m.running = false
}

// run is the main loop.
func (m *Manager) run(ctx context.Context) {
	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.cleanupStale()
			m.retryNotifications(ctx)
		}
	}
}

// Evaluate runs health check results and fires/resolves alerts.
func (m *Manager) Evaluate(ctx context.Context, components map[string]ComponentHealth) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for name, health := range components {
		fp := fingerprint(name, "")

		existing, exists := m.alerts[fp]

		if health.Status == HealthStatusHealthy {
			// Resolve existing alert
			if exists && existing.State == StateFiring {
				existing.State = StateResolved
				existing.ResolvedAt = &now
				existing.UpdatedAt = now
				m.appendHistory(existing.ID, StateFiring, StateResolved, "component recovered")
				m.logger.Info("alert resolved", "name", name)
				m.notifyAll(ctx, existing)
			}
			continue
		}

		// Component is not healthy — determine severity
		severity := SeverityWarning
		if health.Status == HealthStatusUnhealthy {
			severity = SeverityCritical
		}

		msg := health.Message
		if msg == "" {
			msg = fmt.Sprintf("component %s is %s", name, health.Status)
		}

		if !exists {
			// New alert
			alert := &Alert{
				ID:        fmt.Sprintf("alert-%s-%d", fp, now.UnixNano()),
				Name:      name,
				Severity:  severity,
				State:     StateFiring,
				Source:    "health-check",
				Message:   msg,
				Labels:    map[string]string{"component": name},
				FiredAt:   now,
				UpdatedAt: now,
			}
			m.alerts[fp] = alert
			m.appendHistory(alert.ID, StatePending, StateFiring, msg)
			m.logger.Warn("alert fired", "name", name, "severity", severity)
			m.notifyAll(ctx, alert)
		} else if existing.State == StateResolved {
			// Re-fire
			existing.State = StateFiring
			existing.Severity = severity
			existing.Message = msg
			existing.ResolvedAt = nil
			existing.UpdatedAt = now
			existing.FiredAt = now
			existing.RetryCount = 0
			m.appendHistory(existing.ID, StateResolved, StateFiring, "re-fired: "+msg)
			m.logger.Warn("alert re-fired", "name", name, "severity", severity)
			m.notifyAll(ctx, existing)
		} else if existing.Severity != severity {
			// Severity changed
			m.appendHistory(existing.ID, existing.State, existing.State, "severity changed")
			existing.Severity = severity
			existing.Message = msg
			existing.UpdatedAt = now
			m.notifyAll(ctx, existing)
		}
	}
}

// EvaluateRules evaluates threshold rules against metric values.
func (m *Manager) EvaluateRules(ctx context.Context, metrics map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for _, rule := range m.config.Rules {
		val, ok := metrics[rule.Component+"/"+rule.Metric]
		if !ok {
			continue
		}

		triggered := false
		switch rule.Condition {
		case "gt":
			triggered = val > rule.Threshold
		case "gte":
			triggered = val >= rule.Threshold
		case "lt":
			triggered = val < rule.Threshold
		case "lte":
			triggered = val <= rule.Threshold
		case "eq":
			triggered = math.Abs(val-rule.Threshold) < 0.001
		}

		fp := fingerprint(rule.Name, rule.Component)

		existing, exists := m.alerts[fp]

		if triggered {
			msg := fmt.Sprintf("%s = %.2f (threshold: %s %.2f)", rule.Metric, val, rule.Condition, rule.Threshold)

			if !exists || existing.State == StateResolved {
				alert := &Alert{
					ID:        fmt.Sprintf("rule-%s-%d", fp, now.UnixNano()),
					Name:      rule.Name,
					Severity:  rule.Severity,
					State:     StateFiring,
					Source:    "rule-engine",
					Message:   msg,
					Labels:    map[string]string{"component": rule.Component, "metric": rule.Metric},
					Value:     val,
					Threshold: rule.Threshold,
					FiredAt:   now,
					UpdatedAt: now,
				}
				m.alerts[fp] = alert
				m.appendHistory(alert.ID, StatePending, StateFiring, msg)
				m.logger.Warn("rule alert fired", "name", rule.Name, "value", val)
				m.notifyAll(ctx, alert)
			} else if existing != nil {
				existing.Value = val
				existing.Message = msg
				existing.UpdatedAt = now
			}
		} else if exists && existing.State == StateFiring {
			existing.State = StateResolved
			existing.ResolvedAt = &now
			existing.UpdatedAt = now
			m.appendHistory(existing.ID, StateFiring, StateResolved, "condition cleared")
			m.logger.Info("rule alert resolved", "name", rule.Name)
			m.notifyAll(ctx, existing)
		}
	}
}

// notifyAll sends alert to all matching notifiers.
func (m *Manager) notifyAll(ctx context.Context, alert *Alert) {
	now := time.Now()

	for _, n := range m.notifiers {
		// Check cooldown
		if alert.LastNotified != nil && now.Sub(*alert.LastNotified) < m.config.CooldownPeriod {
			continue
		}

		// Check severity filter from config
		for _, ch := range m.config.Channels {
			if ch.Type == n.Type() && ch.Name == n.Name() {
				if SeverityRank(alert.Severity) < SeverityRank(ch.MinSeverity) {
					continue
				}
				// Check label filter
				if len(ch.Labels) > 0 && alert.Labels != nil {
					match := false
					for k, v := range ch.Labels {
						if alert.Labels[k] == v {
							match = true
							break
						}
					}
					if !match {
						continue
					}
				}
			}
		}

		if err := n.Send(ctx, alert); err != nil {
			alert.RetryCount++
			m.logger.Error("failed to send alert", "notifier", n.Name(), "error", err)
		} else {
			alert.LastNotified = &now
			alert.RetryCount = 0
		}
	}

	// Fire callback
	if m.onAlert != nil {
		m.onAlert(alert)
	}
}

// retryNotifications retries failed notifications.
func (m *Manager) retryNotifications(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, alert := range m.alerts {
		if alert.State != StateFiring {
			continue
		}
		if alert.RetryCount == 0 || alert.RetryCount >= m.config.MaxRetries {
			continue
		}
		if alert.LastNotified != nil && now.Sub(*alert.LastNotified) < m.config.RetryInterval {
			continue
		}
		m.notifyAll(ctx, alert)
	}
}

// cleanupStale removes old resolved alerts.
func (m *Manager) cleanupStale() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for fp, alert := range m.alerts {
		if alert.State == StateResolved && now.Sub(alert.UpdatedAt) > m.config.StaleAfter {
			delete(m.alerts, fp)
		}
	}
}

// appendHistory records a state transition.
func (m *Manager) appendHistory(alertID string, from, to State, message string) {
	m.history = append(m.history, AlertHistory{
		AlertID:   alertID,
		From:      from,
		To:        to,
		Message:   message,
		Timestamp: time.Now(),
	})

	// Keep last 1000 entries
	if len(m.history) > 1000 {
		m.history = m.history[len(m.history)-1000:]
	}
}

// GetAlerts returns all current alerts, optionally filtered.
func (m *Manager) GetAlerts(stateFilter State, severityFilter Severity) []*Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Alert
	for _, a := range m.alerts {
		if stateFilter != "" && a.State != stateFilter {
			continue
		}
		if severityFilter != "" && a.Severity != severityFilter {
			continue
		}
		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].FiredAt.After(result[j].FiredAt)
	})

	return result
}

// GetHistory returns recent alert history.
func (m *Manager) GetHistory(limit int) []AlertHistory {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.history) {
		limit = len(m.history)
	}

	start := len(m.history) - limit
	result := make([]AlertHistory, limit)
	copy(result, m.history[start:])

	// Reverse for newest first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// Acknowledge marks an alert as acknowledged.
func (m *Manager) Acknowledge(alertID, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, a := range m.alerts {
		if a.ID == alertID {
			a.AckedBy = user
			a.AckedAt = &now
			a.UpdatedAt = now
			return nil
		}
	}
	return fmt.Errorf("alert not found: %s", alertID)
}

// Silence suppresses notifications for an alert.
func (m *Manager) Silence(alertID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, a := range m.alerts {
		if a.ID == alertID {
			a.State = StateSilenced
			a.UpdatedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("alert not found: %s", alertID)
}

// Stats returns alert statistics.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]int{
		"total":     len(m.alerts),
		"firing":    0,
		"resolved":  0,
		"silenced":  0,
		"critical":  0,
		"warning":   0,
		"info":      0,
	}

	for _, a := range m.alerts {
		switch a.State {
		case StateFiring:
			stats["firing"]++
		case StateResolved:
			stats["resolved"]++
		case StateSilenced:
			stats["silenced"]++
		}
		switch a.Severity {
		case SeverityCritical:
			stats["critical"]++
		case SeverityWarning:
			stats["warning"]++
		case SeverityInfo:
			stats["info"]++
		}
	}

	return stats
}

// fingerprint generates a stable ID from name + source.
func fingerprint(name, source string) string {
	h := fmt.Sprintf("%s:%s", name, source)
	// Simple hash for deduplication
	var hash uint64
	for _, c := range h {
		hash = hash*31 + uint64(c)
	}
	return fmt.Sprintf("fp-%016x", hash)
}
