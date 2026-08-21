package proxypool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

const defaultProbeURL = "http://ip-api.com/json/?fields=status,message,country,regionName,city,query"
const maxProbeErrorBody = 200

type CheckerConfig struct {
	ProbeURL    string
	Interval    time.Duration
	Timeout     time.Duration
	Concurrency int
}

type Checker struct {
	repo      *Repo
	transport channels.Transport
	cfg       CheckerConfig
}

func DefaultCheckerConfig() CheckerConfig {
	return CheckerConfig{
		ProbeURL:    defaultProbeURL,
		Interval:    time.Minute,
		Timeout:     10 * time.Second,
		Concurrency: 5,
	}
}

func NewChecker(repo *Repo, transport channels.Transport, cfg CheckerConfig) *Checker {
	if cfg.ProbeURL == "" {
		cfg.ProbeURL = defaultProbeURL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	return &Checker{repo: repo, transport: transport, cfg: cfg}
}

func (c *Checker) Run(ctx context.Context) {
	if c == nil || c.repo == nil || c.transport == nil {
		return
	}
	c.CheckOnce(ctx)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.CheckOnce(ctx)
		}
	}
}

func (c *Checker) CheckOnce(ctx context.Context) {
	records, err := c.repo.ListActive()
	if err != nil {
		return
	}
	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, rec := range records {
		if err := ctx.Err(); err != nil {
			break
		}
		if rec == nil || !rec.IsActive {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(rec *Record) {
			defer wg.Done()
			defer func() { <-sem }()
			c.checkOne(ctx, rec)
		}(rec)
	}
	wg.Wait()
}

func (c *Checker) CheckRecord(ctx context.Context, id string) (*Record, error) {
	if c == nil || c.repo == nil || c.transport == nil {
		return nil, fmt.Errorf("proxypool: checker unavailable")
	}
	rec, err := c.repo.Get(id)
	if err != nil {
		return nil, err
	}
	c.checkOne(ctx, rec)
	return c.repo.Get(id)
}

func (c *Checker) checkOne(ctx context.Context, rec *Record) {
	started := time.Now()
	resp, err := c.probe(ctx, rec)
	checkedAt := time.Now().Unix()
	if err != nil {
		_ = c.repo.MarkUnhealthy(rec.ID, checkedAt, sanitizeProbeError(err, rec))
		return
	}
	if resp == nil {
		_ = c.repo.MarkUnhealthy(rec.ID, checkedAt, "probe returned nil response")
		return
	}
	if resp.Status < 200 || resp.Status >= 400 {
		_ = c.repo.MarkUnhealthy(rec.ID, checkedAt, sanitizeProbeMessage(statusProbeError(resp), rec))
		return
	}
	meta, metadataFailure := probeMetadataFromBody(resp.Body)
	if metadataFailure != "" {
		_ = c.repo.MarkUnhealthy(rec.ID, checkedAt, sanitizeProbeMessage(metadataFailure, rec))
		return
	}
	latencyMS := time.Since(started).Milliseconds()
	if latencyMS < 1 {
		latencyMS = 1
	}
	_ = c.repo.MarkHealthy(rec.ID, latencyMS, checkedAt, meta)
}

type probeResult struct {
	resp *channels.OutboundResponse
	err  error
}

func (c *Checker) probe(ctx context.Context, rec *Record) (*channels.OutboundResponse, error) {
	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	headers := http.Header{}
	headers.Set("User-Agent", "github.com/marktantongco/freebuff-gateway/proxy-health")
	headers.Set("Accept", "*/*")
	headers.Set("Accept-Encoding", "identity")
	req := &channels.OutboundRequest{
		Method:  http.MethodGet,
		URL:     c.cfg.ProbeURL,
		Headers: headers,
		Timeout: c.cfg.Timeout,
		TransportProfile: channels.TransportProfile{
			ProxyURL: rec.ProxyURL,
		},
	}

	resultCh := make(chan probeResult, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				resultCh <- probeResult{err: fmt.Errorf("probe panic: %v", recovered)}
			}
		}()
		resp, err := c.transport.Do(probeCtx, req)
		resultCh <- probeResult{resp: resp, err: err}
	}()

	select {
	case result := <-resultCh:
		return result.resp, result.err
	case <-probeCtx.Done():
		if errors.Is(probeCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("probe timed out after %s", c.cfg.Timeout)
		}
		return nil, probeCtx.Err()
	}
}

func sanitizeProbeError(err error, rec *Record) string {
	if err == nil {
		return ""
	}
	return sanitizeProbeMessage(err.Error(), rec)
}

func sanitizeProbeMessage(msg string, rec *Record) string {
	if rec != nil {
		if redacted := rec.RedactedURL(); redacted != "" {
			msg = strings.ReplaceAll(msg, rec.ProxyURL, redacted)
		}
		if parsed, parseErr := url.Parse(rec.ProxyURL); parseErr == nil && parsed.User != nil {
			if password, ok := parsed.User.Password(); ok && password != "" {
				msg = strings.ReplaceAll(msg, password, "***")
			}
		}
	}
	msg = strings.TrimSpace(msg)
	if len(msg) > 500 {
		msg = msg[:500]
	}
	return msg
}

func statusProbeError(resp *channels.OutboundResponse) string {
	if resp == nil {
		return "probe returned nil response"
	}
	msg := fmt.Sprintf("probe returned status %d", resp.Status)
	body := strings.TrimSpace(string(resp.BodyPreview))
	if body == "" && len(resp.Body) > 0 {
		body = strings.TrimSpace(string(resp.Body))
	}
	if body != "" {
		if len(body) > maxProbeErrorBody {
			body = body[:maxProbeErrorBody]
		}
		msg += ": " + body
	}
	return msg
}

func probeMetadataFromBody(body []byte) (ProbeMetadata, string) {
	if len(body) == 0 {
		return ProbeMetadata{}, ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ProbeMetadata{}, ""
	}
	status := strings.ToLower(stringField(payload, "status"))
	if status == "fail" || status == "error" {
		msg := stringField(payload, "message")
		if msg == "" {
			msg = "probe returned failed status"
		}
		return ProbeMetadata{}, msg
	}
	return ProbeMetadata{
		ExitIP:  firstStringField(payload, "ip", "query"),
		Country: firstStringField(payload, "country", "country_name", "countryCode"),
		Region:  firstStringField(payload, "region", "regionName"),
		City:    stringField(payload, "city"),
	}, ""
}

func firstStringField(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringField(payload, key); value != "" {
			return value
		}
	}
	return ""
}

func stringField(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}
