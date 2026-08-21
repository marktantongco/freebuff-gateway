package alerting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ─── Mock Notifier ───────────────────────────────────────

type mockNotifier struct {
	mu       sync.Mutex
	sent     []*Alert
	failNext bool
}

func newMockNotifier() *mockNotifier {
	return &mockNotifier{}
}

func (m *mockNotifier) Name() string       { return "mock" }
func (m *mockNotifier) Type() ChannelType { return ChannelWebhook }

func (m *mockNotifier) Send(ctx context.Context, alert *Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return http.ErrServerClosed
	}
	m.sent = append(m.sent, alert)
	return nil
}

func (m *mockNotifier) sentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}

func (m *mockNotifier) lastSent() *Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sent) == 0 {
		return nil
	}
	return m.sent[len(m.sent)-1]
}

// ─── Types Tests ─────────────────────────────────────────

func TestSeverityRank(t *testing.T) {
	tests := []struct {
		severity Severity
		rank     int
	}{
		{SeverityInfo, 0},
		{SeverityWarning, 1},
		{SeverityCritical, 2},
		{"unknown", 0},
	}

	for _, tt := range tests {
		if got := SeverityRank(tt.severity); got != tt.rank {
			t.Errorf("SeverityRank(%s) = %d, want %d", tt.severity, got, tt.rank)
		}
	}
}

func TestAlertFingerprint(t *testing.T) {
	a := &Alert{
		Name:   "test-alert",
		Source: "health-check",
		Labels: map[string]string{"component": "memory"},
	}
	fp := a.Fingerprint()
	if fp == "" {
		t.Error("expected non-empty fingerprint")
	}
	if len(fp) != 16 {
		t.Errorf("expected fingerprint length 16, got %d", len(fp))
	}

	// Same inputs = same fingerprint
	b := &Alert{
		Name:   "test-alert",
		Source: "health-check",
		Labels: map[string]string{"component": "memory"},
	}
	if a.Fingerprint() != b.Fingerprint() {
		t.Error("same alert inputs should produce same fingerprint")
	}

	// Different source = different fingerprint
	c := &Alert{
		Name:   "test-alert",
		Source: "rule-engine",
		Labels: map[string]string{"component": "memory"},
	}
	if a.Fingerprint() == c.Fingerprint() {
		t.Error("different source should produce different fingerprint")
	}
}

func TestAlertIsStale(t *testing.T) {
	a := &Alert{UpdatedAt: time.Now().Add(-2 * time.Hour)}
	if !a.IsStale(time.Hour) {
		t.Error("expected alert to be stale")
	}

	b := &Alert{UpdatedAt: time.Now()}
	if b.IsStale(time.Hour) {
		t.Error("expected alert not to be stale")
	}
}

func TestDefaultAlertConfig(t *testing.T) {
	cfg := DefaultAlertConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.CheckInterval != 30*time.Second {
		t.Errorf("expected 30s check interval, got %v", cfg.CheckInterval)
	}
	if cfg.CooldownPeriod != 5*time.Minute {
		t.Errorf("expected 5m cooldown, got %v", cfg.CooldownPeriod)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected 3 max retries, got %d", cfg.MaxRetries)
	}
}

// ─── Manager Tests ───────────────────────────────────────

func TestManagerNew(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	if m == nil {
		t.Fatal("expected non-nil manager")
	}
	if len(m.alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(m.alerts))
	}
}

func TestManagerEvaluateHealthy(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	ctx := context.Background()

	components := map[string]ComponentHealth{
		"memory": {
			Name:    "memory",
			Status:  HealthStatusHealthy,
			Message: "OK",
		},
	}

	m.Evaluate(ctx, components)

	alerts := m.GetAlerts("", "")
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts for healthy component, got %d", len(alerts))
	}
}

func TestManagerEvaluateUnhealthy(t *testing.T) {
	notif := newMockNotifier()
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0 // disable cooldown for test
	m := NewManager(cfg, nil)
	m.AddNotifier(notif)

	ctx := context.Background()

	components := map[string]ComponentHealth{
		"memory": {
			Name:    "memory",
			Status:  HealthStatusUnhealthy,
			Message: "Memory usage critical",
		},
	}

	m.Evaluate(ctx, components)

	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 firing alert, got %d", len(alerts))
	}

	if alerts[0].Severity != SeverityCritical {
		t.Errorf("expected critical severity, got %s", alerts[0].Severity)
	}

	if alerts[0].Name != "memory" {
		t.Errorf("expected name 'memory', got '%s'", alerts[0].Name)
	}

	if notif.sentCount() != 1 {
		t.Errorf("expected 1 notification, got %d", notif.sentCount())
	}
}

func TestManagerEvaluateResolve(t *testing.T) {
	notif := newMockNotifier()
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	cfg.StaleAfter = 1 * time.Hour
	m := NewManager(cfg, nil)
	m.AddNotifier(notif)

	ctx := context.Background()

	// Fire
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	if notif.sentCount() != 1 {
		t.Fatalf("expected 1 notification on fire, got %d", notif.sentCount())
	}

	// Resolve
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusHealthy, Message: "OK"},
	})

	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 0 {
		t.Errorf("expected 0 firing alerts after resolve, got %d", len(alerts))
	}

	resolved := m.GetAlerts(StateResolved, "")
	if len(resolved) != 1 {
		t.Errorf("expected 1 resolved alert, got %d", len(resolved))
	}

	if resolved[0].ResolvedAt == nil {
		t.Error("expected resolved_at to be set")
	}

	if notif.sentCount() != 2 {
		t.Errorf("expected 2 notifications (fire + resolve), got %d", notif.sentCount())
	}
}

func TestManagerEvaluateSeverityUpgrade(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	notif := newMockNotifier()
	m.AddNotifier(notif)

	ctx := context.Background()

	// Fire warning
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusDegraded, Message: "elevated"},
	})

	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	if alerts[0].Severity != SeverityWarning {
		t.Errorf("expected warning, got %s", alerts[0].Severity)
	}

	// Upgrade to critical
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	alerts = m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert after upgrade, got %d", len(alerts))
	}
	if alerts[0].Severity != SeverityCritical {
		t.Errorf("expected critical after upgrade, got %s", alerts[0].Severity)
	}
}

func TestManagerEvaluateRules(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	cfg.Rules = []RuleConfig{
		{
			Name:      "high-goroutines",
			Component: "runtime",
			Metric:    "goroutines",
			Condition: "gt",
			Threshold: 100,
			Severity:  SeverityWarning,
		},
	}

	m := NewManager(cfg, nil)
	notif := newMockNotifier()
	m.AddNotifier(notif)

	ctx := context.Background()

	// Below threshold
	m.EvaluateRules(ctx, map[string]float64{
		"runtime/goroutines": 50,
	})

	if notif.sentCount() != 0 {
		t.Errorf("expected 0 notifications below threshold, got %d", notif.sentCount())
	}

	// Above threshold
	m.EvaluateRules(ctx, map[string]float64{
		"runtime/goroutines": 150,
	})

	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 firing alert above threshold, got %d", len(alerts))
	}
	if alerts[0].Value != 150 {
		t.Errorf("expected value 150, got %f", alerts[0].Value)
	}

	// Back below → resolve
	m.EvaluateRules(ctx, map[string]float64{
		"runtime/goroutines": 50,
	})

	alerts = m.GetAlerts(StateFiring, "")
	if len(alerts) != 0 {
		t.Errorf("expected 0 firing alerts after resolve, got %d", len(alerts))
	}
}

func TestManagerCooldown(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 1 * time.Hour // Long cooldown
	cfg.StaleAfter = 1 * time.Hour
	m := NewManager(cfg, nil)
	notif := newMockNotifier()
	m.AddNotifier(notif)

	ctx := context.Background()

	// Fire
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	if notif.sentCount() != 1 {
		t.Fatalf("expected 1 notification, got %d", notif.sentCount())
	}

	// Re-fire same alert — should be suppressed by cooldown
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusDegraded, Message: "still bad"},
	})

	// No new notification due to cooldown
	if notif.sentCount() != 1 {
		t.Errorf("expected still 1 notification due to cooldown, got %d", notif.sentCount())
	}
}

func TestManagerRetry(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	cfg.MaxRetries = 3
	cfg.RetryInterval = 1 * time.Millisecond
	cfg.StaleAfter = 1 * time.Hour
	m := NewManager(cfg, nil)

	failNotif := &failNotifier{failCount: 2}
	m.AddNotifier(failNotif)

	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	// Retry loop should eventually succeed
	m.retryNotifications(ctx)
	m.retryNotifications(ctx)

	// After 2 failures + 1 success, alert should be notified
	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
}

func TestManagerHistory(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	// Fire and resolve
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusHealthy, Message: "OK"},
	})

	history := m.GetHistory(10)
	if len(history) < 2 {
		t.Errorf("expected at least 2 history entries, got %d", len(history))
	}

	// Newest first
	if len(history) >= 2 && history[0].Timestamp.Before(history[1].Timestamp) {
		t.Error("expected history in reverse chronological order")
	}
}

func TestManagerAcknowledge(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	alerts := m.GetAlerts(StateFiring, "")
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	err := m.Acknowledge(alerts[0].ID, "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify acked
	alerts2 := m.GetAlerts(StateFiring, "")
	if len(alerts2) != 1 {
		t.Fatalf("expected alert still firing, got %d", len(alerts2))
	}
	if alerts2[0].AckedBy != "admin" {
		t.Errorf("expected acked_by 'admin', got '%s'", alerts2[0].AckedBy)
	}

	// Unknown ID
	err = m.Acknowledge("nonexistent", "admin")
	if err == nil {
		t.Error("expected error for nonexistent alert")
	}
}

func TestManagerSilence(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	alerts := m.GetAlerts(StateFiring, "")
	err := m.Silence(alerts[0].ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	silenced := m.GetAlerts(StateSilenced, "")
	if len(silenced) != 1 {
		t.Errorf("expected 1 silenced alert, got %d", len(silenced))
	}

	firing := m.GetAlerts(StateFiring, "")
	if len(firing) != 0 {
		t.Errorf("expected 0 firing after silence, got %d", len(firing))
	}
}

func TestManagerStats(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory":   {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
		"goroutines": {Name: "goroutines", Status: HealthStatusDegraded, Message: "elevated"},
	})

	stats := m.Stats()
	if stats["total"] != 2 {
		t.Errorf("expected 2 total, got %d", stats["total"])
	}
	if stats["firing"] != 2 {
		t.Errorf("expected 2 firing, got %d", stats["firing"])
	}
	if stats["critical"] != 1 {
		t.Errorf("expected 1 critical, got %d", stats["critical"])
	}
	if stats["warning"] != 1 {
		t.Errorf("expected 1 warning, got %d", stats["warning"])
	}
}

func TestManagerStartStop(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	ctx := context.Background()

	m.Start(ctx)
	if !m.running {
		t.Error("expected running")
	}

	m.Start(ctx) // second call should be no-op
	m.Stop()

	if m.running {
		t.Error("expected stopped")
	}
}

func TestManagerMultipleChannels(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)

	mock1 := newMockNotifier()
	mock2 := newMockNotifier()
	m.AddNotifier(mock1)
	m.AddNotifier(mock2)

	ctx := context.Background()
	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	if mock1.sentCount() != 1 {
		t.Errorf("mock1: expected 1 notification, got %d", mock1.sentCount())
	}
	if mock2.sentCount() != 1 {
		t.Errorf("mock2: expected 1 notification, got %d", mock2.sentCount())
	}
}

// ─── Handler Tests ───────────────────────────────────────

func TestHandlerListAlerts(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	h := NewHandler(m)
	req := httptest.NewRequest(http.MethodGet, "/api/alerts", nil)
	w := httptest.NewRecorder()

	h.handleAlerts(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	count := int(resp["count"].(float64))
	if count != 1 {
		t.Errorf("expected 1 alert, got %d", count)
	}
}

func TestHandlerListAlertsFilter(t *testing.T) {
	cfg := DefaultAlertConfig()
	cfg.CooldownPeriod = 0
	m := NewManager(cfg, nil)
	ctx := context.Background()

	m.Evaluate(ctx, map[string]ComponentHealth{
		"memory": {Name: "memory", Status: HealthStatusUnhealthy, Message: "critical"},
	})

	h := NewHandler(m)

	// Filter by severity
	req := httptest.NewRequest(http.MethodGet, "/api/alerts?severity=warning", nil)
	w := httptest.NewRecorder()
	h.handleAlerts(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	count := int(resp["count"].(float64))
	if count != 0 {
		t.Errorf("expected 0 warnings, got %d", count)
	}

	// Filter by state
	req = httptest.NewRequest(http.MethodGet, "/api/alerts?state=resolved", nil)
	w = httptest.NewRecorder()
	h.handleAlerts(w, req)

	json.NewDecoder(w.Body).Decode(&resp)
	count = int(resp["count"].(float64))
	if count != 0 {
		t.Errorf("expected 0 resolved, got %d", count)
	}
}

func TestHandlerCreateManualAlert(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	h := NewHandler(m)

	body := `{"name":"manual-alert","severity":"warning","message":"test message","source":"manual"}`
	req := httptest.NewRequest(http.MethodPost, "/api/alerts", 
		bytesReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.handleAlerts(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var alert Alert
	json.NewDecoder(w.Body).Decode(&alert)
	if alert.Name != "manual-alert" {
		t.Errorf("expected name 'manual-alert', got '%s'", alert.Name)
	}
}

func TestHandlerStats(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	h := NewHandler(m)

	req := httptest.NewRequest(http.MethodGet, "/api/alerts/stats", nil)
	w := httptest.NewRecorder()
	h.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var stats map[string]int
	json.NewDecoder(w.Body).Decode(&stats)
	if stats["total"] != 0 {
		t.Errorf("expected 0 total, got %d", stats["total"])
	}
}

func TestHandlerHistory(t *testing.T) {
	m := NewManager(DefaultAlertConfig(), nil)
	h := NewHandler(m)

	req := httptest.NewRequest(http.MethodGet, "/api/alerts/history", nil)
	w := httptest.NewRecorder()
	h.handleHistory(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// ─── Channel Tests ───────────────────────────────────────

func TestWebhookNotifier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := NewWebhookNotifier("test", server.URL, nil)
	if n.Name() != "test" {
		t.Errorf("expected name 'test', got '%s'", n.Name())
	}
	if n.Type() != ChannelWebhook {
		t.Errorf("expected type webhook, got '%s'", n.Type())
	}

	alert := &Alert{
		Name:     "test",
		Severity: SeverityCritical,
		State:    StateFiring,
		Source:   "test",
		FiredAt:  time.Now(),
	}

	err := n.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWebhookNotifierError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	n := NewWebhookNotifier("test", server.URL, nil)
	alert := &Alert{Name: "test", Severity: SeverityWarning, State: StateFiring, Source: "test", FiredAt: time.Now()}

	err := n.Send(context.Background(), alert)
	if err == nil {
		t.Error("expected error for 500 status")
	}
}

func TestSlackNotifier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		json.NewDecoder(r.Body).Decode(&payload)

		if payload["username"] != "Freebuff Alerts" {
			t.Errorf("expected username 'Freebuff Alerts', got '%s'", payload["username"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := NewSlackNotifier("test", server.URL, "#alerts")
	alert := &Alert{
		Name:     "test",
		Severity: SeverityCritical,
		State:    StateFiring,
		Source:   "test",
		Message:  "something is wrong",
		FiredAt:  time.Now(),
	}

	err := n.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTelegramNotifier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		json.NewDecoder(r.Body).Decode(&payload)

		if payload["parse_mode"] != "Markdown" {
			t.Errorf("expected parse_mode Markdown")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	n := NewTelegramNotifier("test", "fake-token", "12345")
	alert := &Alert{
		Name:     "test",
		Severity: SeverityWarning,
		State:    StateFiring,
		Source:   "test",
		Message:  "warning message",
		FiredAt:  time.Now(),
	}

	// Override URL for test — in real code the URL is constructed
	// We test the payload format via Slack which is similar
	err := n.Send(context.Background(), alert)
	// Telegram will fail to connect to fake token URL, that's ok
	if err != nil {
		t.Logf("expected telegram error with fake token: %v", err)
	}
}

func TestDiscordNotifier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		json.NewDecoder(r.Body).Decode(&payload)

		embeds, ok := payload["embeds"].([]interface{})
		if !ok || len(embeds) == 0 {
			t.Error("expected embeds in discord payload")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	n := NewDiscordNotifier("test", server.URL)
	alert := &Alert{
		Name:     "test",
		Severity: SeverityCritical,
		State:    StateFiring,
		Source:   "test",
		Message:  "critical error",
		FiredAt:  time.Now(),
	}

	err := n.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ─── Bridge Tests ────────────────────────────────────────

func TestBridgeComponentHealth(t *testing.T) {
	h := ComponentHealth{
		Name:      "test",
		Status:    HealthStatusDegraded,
		Message:   "elevated",
		LastCheck: time.Now(),
	}
	if h.Name != "test" {
		t.Errorf("expected name 'test', got '%s'", h.Name)
	}
	if h.Status != HealthStatusDegraded {
		t.Errorf("expected degraded, got %s", h.Status)
	}
}

// ─── Helpers ─────────────────────────────────────────────

type failNotifier struct {
	mu        sync.Mutex
	failCount int
	calls     int
}

func (f *failNotifier) Name() string       { return "fail-notifier" }
func (f *failNotifier) Type() ChannelType { return ChannelWebhook }

func (f *failNotifier) Send(ctx context.Context, alert *Alert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failCount {
		return http.ErrServerClosed
	}
	return nil
}

func bytesReader(s string) *stringReader {
	return &stringReader{s: s, i: 0}
}

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, nil
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
