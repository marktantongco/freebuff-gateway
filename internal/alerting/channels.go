package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"strings"
	"time"
)

// Notifier sends alerts through a channel.
type Notifier interface {
	Name() string
	Type() ChannelType
	Send(ctx context.Context, alert *Alert) error
}

// WebhookNotifier sends alerts via HTTP webhook.
type WebhookNotifier struct {
	name    string
	url     string
	headers map[string]string
	client  *http.Client
}

// NewWebhookNotifier creates a webhook notifier.
func NewWebhookNotifier(name, url string, headers map[string]string) *WebhookNotifier {
	return &WebhookNotifier{
		name:    name,
		url:     url,
		headers: headers,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *WebhookNotifier) Name() string  { return n.name }
func (n *WebhookNotifier) Type() ChannelType { return ChannelWebhook }

func (n *WebhookNotifier) Send(ctx context.Context, alert *Alert) error {
	payload := map[string]interface{}{
		"id":       alert.ID,
		"name":     alert.Name,
		"severity": alert.Severity,
		"state":    alert.State,
		"source":   alert.Source,
		"message":  alert.Message,
		"labels":   alert.Labels,
		"value":    alert.Value,
		"fired_at": alert.FiredAt,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range n.headers {
		req.Header.Set(k, v)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// SlackNotifier sends alerts to Slack.
type SlackNotifier struct {
	name    string
	webhookURL string
	channel string
	client  *http.Client
}

// NewSlackNotifier creates a Slack notifier.
func NewSlackNotifier(name, webhookURL, channel string) *SlackNotifier {
	return &SlackNotifier{
		name:       name,
		webhookURL: webhookURL,
		channel:    channel,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *SlackNotifier) Name() string  { return n.name }
func (n *SlackNotifier) Type() ChannelType { return ChannelSlack }

func (n *SlackNotifier) Send(ctx context.Context, alert *Alert) error {
	color := "#36a64f" // green
	switch alert.Severity {
	case SeverityWarning:
		color = "#ff9900"
	case SeverityCritical:
		color = "#ff0000"
	}

	stateEmoji := ":bell:"
	if alert.State == StateResolved {
		stateEmoji = ":white_check_mark:"
		color = "#36a64f"
	}

	text := fmt.Sprintf("%s *[%s]* %s\n%s", stateEmoji, strings.ToUpper(string(alert.Severity)), alert.Name, alert.Message)

	attachment := map[string]interface{}{
		"color":    color,
		"title":    fmt.Sprintf("%s — %s", alert.Name, alert.State),
		"text":     alert.Message,
		"fields": []map[string]interface{}{
			{"title": "Source", "value": alert.Source, "short": true},
			{"title": "Severity", "value": alert.Severity, "short": true},
			{"title": "Fired At", "value": alert.FiredAt.Format(time.RFC3339), "short": true},
		},
	}

	payload := map[string]interface{}{
		"text":       text,
		"channel":    n.channel,
		"username":   "Freebuff Alerts",
		"icon_emoji": ":rotating_light:",
		"attachments": []interface{}{attachment},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", n.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("slack request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// TelegramNotifier sends alerts via Telegram Bot API.
type TelegramNotifier struct {
	name    string
	botToken string
	chatID  string
	client  *http.Client
}

// NewTelegramNotifier creates a Telegram notifier.
func NewTelegramNotifier(name, botToken, chatID string) *TelegramNotifier {
	return &TelegramNotifier{
		name:     name,
		botToken: botToken,
		chatID:   chatID,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *TelegramNotifier) Name() string  { return n.name }
func (n *TelegramNotifier) Type() ChannelType { return ChannelTelegram }

func (n *TelegramNotifier) Send(ctx context.Context, alert *Alert) error {
	emoji := "ℹ️"
	switch alert.Severity {
	case SeverityWarning:
		emoji = "⚠️"
	case SeverityCritical:
		emoji = "🚨"
	}

	text := fmt.Sprintf(
		"%s *%s*\n\n"+
			"*Severity:* %s\n"+
			"*State:* %s\n"+
			"*Source:* %s\n"+
			"*Message:* %s\n"+
			"*Time:* %s",
		emoji,
		alert.Name,
		alert.Severity,
		alert.State,
		alert.Source,
		alert.Message,
		alert.FiredAt.Format(time.RFC3339),
	)

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", n.botToken)
	payload := map[string]interface{}{
		"chat_id":    n.chatID,
		"text":       text,
		"parse_mode": "Markdown",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal telegram payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// DiscordNotifier sends alerts via Discord webhook.
type DiscordNotifier struct {
	name       string
	webhookURL string
	client     *http.Client
}

// NewDiscordNotifier creates a Discord notifier.
func NewDiscordNotifier(name, webhookURL string) *DiscordNotifier {
	return &DiscordNotifier{
		name:       name,
		webhookURL: webhookURL,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *DiscordNotifier) Name() string  { return n.name }
func (n *DiscordNotifier) Type() ChannelType { return ChannelDiscord }

func (n *DiscordNotifier) Send(ctx context.Context, alert *Alert) error {
	color := 0x36a64f // green
	if alert.Severity == SeverityWarning {
		color = 0xff9900
	} else if alert.Severity == SeverityCritical {
		color = 0xff0000
	}

	payload := map[string]interface{}{
		"embeds": []map[string]interface{}{
			{
				"title":       fmt.Sprintf("[%s] %s", strings.ToUpper(string(alert.Severity)), alert.Name),
				"description": alert.Message,
				"color":       color,
				"fields": []map[string]interface{}{
					{"name": "Source", "value": alert.Source, "inline": true},
					{"name": "State", "value": alert.State, "inline": true},
					{"name": "Time", "value": alert.FiredAt.Format(time.RFC3339), "inline": false},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal discord payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", n.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("discord request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("discord returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// EmailNotifier sends alerts via SMTP.
type EmailNotifier struct {
	name     string
	smtpAddr string
	from     string
	to       []string
	auth     smtp.Auth
}

// NewEmailNotifier creates an email notifier.
func NewEmailNotifier(name, smtpAddr, username, password, from string, to []string) *EmailNotifier {
	var auth smtp.Auth
	if username != "" {
		auth = smtp.PlainAuth("", username, password, strings.Split(smtpAddr, ":")[0])
	}
	return &EmailNotifier{
		name:     name,
		smtpAddr: smtpAddr,
		from:     from,
		to:       to,
		auth:     auth,
	}
}

func (n *EmailNotifier) Name() string  { return n.name }
func (n *EmailNotifier) Type() ChannelType { return ChannelEmail }

func (n *EmailNotifier) Send(ctx context.Context, alert *Alert) error {
	subject := fmt.Sprintf("[%s] Freebuff Alert: %s", strings.ToUpper(string(alert.Severity)), alert.Name)
	body := fmt.Sprintf(
		"Alert: %s\nSeverity: %s\nState: %s\nSource: %s\nTime: %s\n\n%s",
		alert.Name, alert.Severity, alert.State, alert.Source,
		alert.FiredAt.Format(time.RFC3339), alert.Message,
	)

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		n.from, strings.Join(n.to, ","), subject, body)

	return smtp.SendMail(n.smtpAddr, n.auth, n.from, n.to, []byte(msg))
}

// PagerDutyNotifier sends alerts via PagerDuty Events API v2.
type PagerDutyNotifier struct {
	name       string
	routingKey string
	client     *http.Client
}

// NewPagerDutyNotifier creates a PagerDuty notifier.
func NewPagerDutyNotifier(name, routingKey string) *PagerDutyNotifier {
	return &PagerDutyNotifier{
		name:       name,
		routingKey: routingKey,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *PagerDutyNotifier) Name() string  { return n.name }
func (n *PagerDutyNotifier) Type() ChannelType { return ChannelPagerDuty }

func (n *PagerDutyNotifier) Send(ctx context.Context, alert *Alert) error {
	sev := "info"
	switch alert.Severity {
	case SeverityWarning:
		sev = "warning"
	case SeverityCritical:
		sev = "critical"
	}

	eventAction := "trigger"
	if alert.State == StateResolved {
		eventAction = "resolve"
	}

	payload := map[string]interface{}{
		"routing_key":  n.routingKey,
		"event_action": eventAction,
		"dedup_key":    alert.Fingerprint(),
		"payload": map[string]interface{}{
			"summary":  fmt.Sprintf("[%s] %s: %s", strings.ToUpper(string(alert.Severity)), alert.Name, alert.Message),
			"source":   alert.Source,
			"severity": sev,
			"timestamp": alert.FiredAt.Format(time.RFC3339),
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pagerduty payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://events.pagerduty.com/v2/enqueue", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("pagerduty request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pagerduty returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}
