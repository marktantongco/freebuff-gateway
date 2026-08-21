package alerting

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// Severity represents the severity level of an alert.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// State represents the current state of an alert.
type State string

const (
	StatePending  State = "pending"
	StateFiring   State = "firing"
	StateResolved State = "resolved"
	StateSilenced State = "silenced"
)

// Alert is a single alert instance.
type Alert struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Severity    Severity          `json:"severity"`
	State       State             `json:"state"`
	Source      string            `json:"source"`
	Message     string            `json:"message"`
	Labels      map[string]string `json:"labels,omitempty"`
	Value       float64           `json:"value,omitempty"`
	Threshold   float64           `json:"threshold,omitempty"`
	FiredAt     time.Time         `json:"fired_at"`
	ResolvedAt  *time.Time        `json:"resolved_at,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
	AckedBy     string            `json:"acked_by,omitempty"`
	AckedAt     *time.Time        `json:"acked_at,omitempty"`
	RetryCount  int               `json:"retry_count"`
	LastNotified *time.Time       `json:"last_notified,omitempty"`
}

// Fingerprint returns a stable hash of alert identity (name + source + labels).
func (a *Alert) Fingerprint() string {
	h := sha256.New()
	h.Write([]byte(a.Name))
	h.Write([]byte(a.Source))
	for k, v := range a.Labels {
		h.Write([]byte(k + "=" + v))
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// IsStale returns true if the alert hasn't been updated recently.
func (a *Alert) IsStale(maxAge time.Duration) bool {
	return time.Since(a.UpdatedAt) > maxAge
}

// AlertHistory records a state transition.
type AlertHistory struct {
	AlertID   string    `json:"alert_id"`
	From      State     `json:"from"`
	To        State     `json:"to"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// AlertConfig holds configuration for alerting.
type AlertConfig struct {
	Enabled         bool          `json:"enabled"`
	CheckInterval   time.Duration `json:"check_interval"`
	CooldownPeriod  time.Duration `json:"cooldown_period"`
	MaxRetries      int           `json:"max_retries"`
	RetryInterval   time.Duration `json:"retry_interval"`
	StaleAfter      time.Duration `json:"stale_after"`
	Channels        []ChannelConfig `json:"channels"`
	Rules           []RuleConfig   `json:"rules"`
}

// DefaultAlertConfig returns sensible defaults.
func DefaultAlertConfig() AlertConfig {
	return AlertConfig{
		Enabled:        true,
		CheckInterval:  30 * time.Second,
		CooldownPeriod: 5 * time.Minute,
		MaxRetries:     3,
		RetryInterval:  1 * time.Minute,
		StaleAfter:     1 * time.Hour,
		Channels:       []ChannelConfig{},
		Rules:          []RuleConfig{},
	}
}

// ChannelConfig configures a notification channel.
type ChannelConfig struct {
	Name     string            `json:"name"`
	Type     ChannelType       `json:"type"`
	Enabled  bool              `json:"enabled"`
	Labels   map[string]string `json:"labels,omitempty"`   // filter by labels
	MinSeverity Severity       `json:"min_severity"`
	Options  map[string]string `json:"options,omitempty"`  // channel-specific
}

// ChannelType identifies the notification channel type.
type ChannelType string

const (
	ChannelWebhook  ChannelType = "webhook"
	ChannelSlack    ChannelType = "slack"
	ChannelTelegram ChannelType = "telegram"
	ChannelDiscord  ChannelType = "discord"
	ChannelEmail    ChannelType = "email"
	ChannelPagerDuty ChannelType = "pagerduty"
)

// RuleConfig defines a threshold rule.
type RuleConfig struct {
	Name       string   `json:"name"`
	Component  string   `json:"component"`
	Metric     string   `json:"metric"`
	Condition  string   `json:"condition"`  // "gt", "lt", "eq", "gte", "lte"
	Threshold  float64  `json:"threshold"`
	Severity   Severity `json:"severity"`
	Duration   string   `json:"duration"`   // how long condition must be true
	Message    string   `json:"message"`
}

// SeverityRank returns a numeric rank for comparison.
func SeverityRank(s Severity) int {
	switch s {
	case SeverityInfo:
		return 0
	case SeverityWarning:
		return 1
	case SeverityCritical:
		return 2
	default:
		return 0
	}
}

// String returns the string representation.
func (s Severity) String() string {
	return string(s)
}

// String returns the string representation.
func (s State) String() string {
	return string(s)
}
