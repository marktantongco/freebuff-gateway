package freebuff

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

type upstreamSession struct {
	Status            string                       `json:"status"`
	AccessTier        string                       `json:"accessTier"`
	Message           string                       `json:"message"`
	InstanceID        string                       `json:"instanceId"`
	Model             string                       `json:"model"`
	AdmittedAt        string                       `json:"admittedAt"`
	ExpiresAt         string                       `json:"expiresAt"`
	RemainingMs       int64                        `json:"remainingMs"`
	RateLimit         *upstreamRateLimit           `json:"rateLimit"`
	RateLimitsByModel map[string]upstreamRateLimit `json:"rateLimitsByModel"`
	QueueDepthByModel map[string]int               `json:"queueDepthByModel"`
	RawJSON           string                       `json:"-"`
}

type upstreamRateLimit struct {
	Model         string  `json:"model"`
	Limit         int     `json:"limit"`
	Period        string  `json:"period"`
	ResetTimeZone string  `json:"resetTimeZone"`
	ResetAt       string  `json:"resetAt"`
	WindowHours   int     `json:"windowHours"`
	RecentCount   float64 `json:"recentCount"`
}

func (a *Adapter) fetchActiveSession(ctx context.Context, tp channels.Transport, baseURL, credential, instanceID string, transportProfile channels.TransportProfile) (upstreamSession, error) {
	headers := sessionHeaders(credential)
	if instanceID != "" {
		headers.Set("x-freebuff-instance-id", instanceID)
	}
	resp, err := a.doSessionRequest(ctx, tp, &channels.OutboundRequest{
		Method:           http.MethodGet,
		URL:              joinURL(baseURL, "/api/v1/freebuff/session"),
		Headers:          headers,
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffSessionTransportProfile(credential), transportProfile),
	})
	if err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: check session: %w", err)
	}
	if resp.Status == http.StatusNotFound || resp.Status == http.StatusNoContent {
		return upstreamSession{}, nil
	}
	if resp.Status == http.StatusUnauthorized || resp.Status == http.StatusForbidden {
		return upstreamSession{}, fmt.Errorf("freebuff: check session auth rejected: status %d", resp.Status)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return upstreamSession{}, fmt.Errorf("freebuff: check session failed: status %d", resp.Status)
	}
	var decoded upstreamSession
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: decode session: %w", err)
	}
	decoded.RawJSON = string(resp.Body)
	decoded.normalize()
	return decoded, nil
}

func (a *Adapter) joinSession(ctx context.Context, tp channels.Transport, baseURL, credential, model string, transportProfile channels.TransportProfile) (upstreamSession, error) {
	headers := sessionHeaders(credential)
	headers.Set("x-freebuff-model", model)
	resp, err := a.doSessionRequest(ctx, tp, &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(baseURL, "/api/v1/freebuff/session"),
		Headers:          headers,
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffSessionTransportProfile(credential), transportProfile),
	})
	if err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: join session: %w", err)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return upstreamSession{}, fmt.Errorf("freebuff: join session failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	var decoded upstreamSession
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: decode joined session: %w", err)
	}
	decoded.RawJSON = string(resp.Body)
	decoded.normalize()
	return decoded, nil
}

func (a *Adapter) releaseSession(ctx context.Context, tp channels.Transport, baseURL, credential string, transportProfile channels.TransportProfile) (upstreamSession, error) {
	headers := sessionHeaders(credential)
	resp, err := a.doSessionRequest(ctx, tp, &channels.OutboundRequest{
		Method:           http.MethodDelete,
		URL:              joinURL(baseURL, "/api/v1/freebuff/session"),
		Headers:          headers,
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffSessionTransportProfile(credential), transportProfile),
	})
	if err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: release session: %w", err)
	}
	if resp.Status == http.StatusNotFound || resp.Status == http.StatusNoContent {
		return upstreamSession{Status: "ended"}, nil
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return upstreamSession{}, fmt.Errorf("freebuff: release session failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	if len(resp.Body) == 0 {
		return upstreamSession{Status: "ended"}, nil
	}
	var decoded upstreamSession
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return upstreamSession{}, fmt.Errorf("freebuff: decode released session: %w", err)
	}
	decoded.RawJSON = string(resp.Body)
	if decoded.Status == "" {
		decoded.Status = "ended"
	}
	decoded.normalize()
	return decoded, nil
}

func (a *Adapter) doSessionRequest(ctx context.Context, tp channels.Transport, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	return tp.Do(ctx, req)
}

func (a *Adapter) createRun(ctx context.Context, tp channels.Transport, baseURL, credential, model string, transportProfile channels.TransportProfile) (string, error) {
	agentID, err := AgentIDForModel(model)
	if err != nil {
		return "", err
	}
	body := map[string]any{
		"action":         "START",
		"agentId":        agentID,
		"ancestorRunIds": []any{},
	}
	resp, err := a.postJSON(ctx, tp, baseURL, credential, "/api/v1/agent-runs", body, transportProfile)
	if err != nil {
		return "", err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return "", fmt.Errorf("freebuff: create run failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	var decoded struct {
		RunID string `json:"runId"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return "", fmt.Errorf("freebuff: decode run: %w", err)
	}
	if decoded.RunID == "" {
		return "", fmt.Errorf("freebuff: run response missing runId")
	}
	return decoded.RunID, nil
}

func (a *Adapter) recordRunStep(ctx context.Context, tp channels.Transport, baseURL, credential, runID, messageID, status string, stepNumber int, start time.Time, transportProfile channels.TransportProfile) error {
	if runID == "" {
		return nil
	}
	body := map[string]any{
		"stepNumber":  stepNumber,
		"credits":     0,
		"childRunIds": []any{},
		"messageId":   nil,
		"status":      status,
		"startTime":   start.UTC().Format(time.RFC3339Nano),
	}
	if messageID != "" {
		body["messageId"] = messageID
	}
	resp, err := a.postJSON(ctx, tp, baseURL, credential, "/api/v1/agent-runs/"+runID+"/steps", body, transportProfile)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("freebuff: record run step failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	return nil
}

func (s *upstreamSession) normalize() {
	if s.Model != "" {
		s.Model = CanonicalModel(s.Model)
	}
	if s.RateLimit != nil {
		if s.RateLimit.Model != "" {
			s.RateLimit.Model = CanonicalModel(s.RateLimit.Model)
		}
	}
	if len(s.RateLimitsByModel) > 0 {
		normalized := make(map[string]upstreamRateLimit, len(s.RateLimitsByModel))
		for key, limit := range s.RateLimitsByModel {
			model := ""
			if limit.Model != "" {
				model = CanonicalModel(limit.Model)
			}
			if model == "" {
				model = CanonicalModel(key)
			}
			limit.Model = model
			normalized[model] = limit
		}
		s.RateLimitsByModel = normalized
	}
}

func (s *upstreamSession) mergeQuotaFallback(fallback upstreamSession) {
	if s.RateLimit == nil && fallback.RateLimit != nil {
		limit := *fallback.RateLimit
		s.RateLimit = &limit
	}
	if len(s.RateLimitsByModel) == 0 && len(fallback.RateLimitsByModel) > 0 {
		s.RateLimitsByModel = make(map[string]upstreamRateLimit, len(fallback.RateLimitsByModel))
		for model, limit := range fallback.RateLimitsByModel {
			s.RateLimitsByModel[model] = limit
		}
	}
	if s.AccessTier == "" {
		s.AccessTier = fallback.AccessTier
	}
}

func (s upstreamSession) rateLimitFor(model string) (upstreamRateLimit, bool) {
	model = CanonicalModel(model)
	if s.RateLimit != nil && CanonicalModel(s.RateLimit.Model) == model {
		return *s.RateLimit, true
	}
	if len(s.RateLimitsByModel) > 0 {
		limit, ok := s.RateLimitsByModel[model]
		return limit, ok
	}
	return upstreamRateLimit{}, false
}

func premiumQuotaExhausted(model string, session upstreamSession, now time.Time) bool {
	profile, ok := ModelProfileFor(model)
	if !ok || profile.QuotaGroup != QuotaGroupPremiumShared {
		return false
	}
	limit, ok := session.rateLimitFor(model)
	if !ok || limit.Limit <= 0 || limit.RecentCount < float64(limit.Limit) {
		return false
	}
	if resetAt := parseExpiresAtUnix(limit.ResetAt); resetAt > 0 && !now.Before(time.Unix(resetAt, 0)) {
		return false
	}
	return true
}

func (a *Adapter) finishRun(ctx context.Context, tp channels.Transport, baseURL, credential, runID, status string, steps int, transportProfile channels.TransportProfile) error {
	body := map[string]any{
		"action":        "FINISH",
		"runId":         runID,
		"status":        status,
		"totalSteps":    steps,
		"directCredits": 0,
		"totalCredits":  0,
	}
	resp, err := a.postJSON(ctx, tp, baseURL, credential, "/api/v1/agent-runs", body, transportProfile)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("freebuff: finish run failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	return nil
}

func (a *Adapter) postJSON(ctx context.Context, tp channels.Transport, baseURL, credential, path string, body any, transportProfile channels.TransportProfile) (*channels.OutboundResponse, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal json: %w", err)
	}
	resp, err := tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(baseURL, path),
		Headers:          codebuffAPIHeaders(credential),
		Body:             payload,
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffAPITransportProfile(credential), transportProfile),
	})
	if err != nil {
		return nil, fmt.Errorf("freebuff: post %s: %w", path, err)
	}
	return resp, nil
}

func codebuffHeaders(credential string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+credential)
	h.Set("User-Agent", "ai-sdk/openai-compatible/0.0.0-test/codebuff ai-sdk/provider-utils/3.0.20 runtime/browser")
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "*/*")
	h.Set("Accept-Encoding", "identity")
	h.Set("Connection", "keep-alive")
	h.Set("Host", "www.codebuff.com")
	return h
}

func sessionHeaders(credential string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+credential)
	h.Set("User-Agent", "Bun/1.3.11")
	h.Set("Accept", "*/*")
	h.Set("Accept-Encoding", "identity")
	h.Set("Connection", "keep-alive")
	h.Set("Host", "www.codebuff.com")
	return h
}

func adsHeaders(credential string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+credential)
	h.Set("User-Agent", "Freebuff-CLI/0.0.91")
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "*/*")
	h.Set("Accept-Encoding", "identity")
	h.Set("Connection", "keep-alive")
	h.Set("Host", "www.codebuff.com")
	return h
}

func codebuffAPIHeaders(credential string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+credential)
	h.Set("User-Agent", "Bun/1.3.11")
	h.Set("Accept", "*/*")
	h.Set("Accept-Encoding", "identity")
	h.Set("Connection", "keep-alive")
	h.Set("Host", "www.codebuff.com")
	return h
}

func freebuffChatTransportProfile() channels.TransportProfile {
	return channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		RandomTLSExtensionOrder: true,
		DisableHTTP3:            true,
		HeaderOrder: []string{
			"authorization",
			"user-agent",
			"content-type",
			"accept",
			"accept-encoding",
			"connection",
			"host",
		},
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}
}

func freebuffAPITransportProfile(credential string) channels.TransportProfile {
	return channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		ReuseKey:                freebuffReuseKey("freebuff_api", credential),
		RandomTLSExtensionOrder: true,
		DisableHTTP3:            true,
		HeaderOrder: []string{
			"authorization",
			"user-agent",
			"accept",
			"accept-encoding",
			"connection",
			"host",
		},
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}
}

func freebuffSessionTransportProfile(credential string) channels.TransportProfile {
	return channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		ReuseKey:                freebuffReuseKey("freebuff_session", credential),
		RandomTLSExtensionOrder: true,
		DisableHTTP3:            true,
		HeaderOrder: []string{
			"authorization",
			"user-agent",
			"accept",
			"accept-encoding",
			"connection",
			"host",
			"x-freebuff-instance-id",
			"x-freebuff-model",
		},
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}
}

func freebuffReuseKey(prefix, credential string) string {
	sum := sha256.Sum256([]byte(credential))
	return prefix + ":" + hex.EncodeToString(sum[:8])
}

func mergeFreeBuffProxyProfile(base, proxy channels.TransportProfile) channels.TransportProfile {
	if proxy.ProxyURL != "" {
		base.ProxyURL = proxy.ProxyURL
	}
	return base
}

func (a *Adapter) fetchMe(ctx context.Context, tp channels.Transport, baseURL, credential string, transportProfile channels.TransportProfile) (meInfo, error) {
	resp, err := tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodGet,
		URL:              joinURL(baseURL, "/api/v1/me?fields=id%2Cemail"),
		Headers:          sessionHeaders(credential),
		Timeout:          15 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffAPITransportProfile(credential), transportProfile),
	})
	if err != nil {
		return meInfo{}, fmt.Errorf("freebuff: fetch me: %w", err)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return meInfo{}, fmt.Errorf("freebuff: fetch me failed: status %d", resp.Status)
	}
	var info meInfo
	if err := json.Unmarshal(resp.Body, &info); err != nil {
		return meInfo{}, fmt.Errorf("freebuff: decode me: %w", err)
	}
	return info, nil
}

func buildAdsBody(provider, adsSessionID string, messages []map[string]any, surface string, dp deviceProfile) map[string]any {
	body := map[string]any{
		"provider":  provider,
		"messages":  messages,
		"sessionId": adsSessionID,
		"device": map[string]any{
			"os":       dp.OS,
			"timezone": dp.Timezone,
			"locale":   dp.Locale,
		},
		"userAgent": dp.BrowserUA,
	}
	if surface != "" {
		body["surface"] = surface
	}
	return body
}

func (a *Adapter) doAdsRequest(ctx context.Context, tp channels.Transport, baseURL, credential string, adBody map[string]any, transportProfile channels.TransportProfile) (*channels.OutboundResponse, error) {
	payload, err := json.Marshal(adBody)
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal ads: %w", err)
	}
	return tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(baseURL, "/api/v1/ads"),
		Headers:          adsHeaders(credential),
		Body:             payload,
		Timeout:          15 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffAPITransportProfile(credential), transportProfile),
	})
}

func (a *Adapter) postAdImpression(ctx context.Context, tp channels.Transport, baseURL, credential, impURL, mode string, transportProfile channels.TransportProfile) error {
	body := map[string]any{
		"impUrl": impURL,
		"mode":   mode,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("freebuff: marshal impression: %w", err)
	}
	resp, err := tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(baseURL, "/api/v1/ads/impression"),
		Headers:          adsHeaders(credential),
		Body:             payload,
		Timeout:          15 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffAPITransportProfile(credential), transportProfile),
	})
	if err != nil {
		return fmt.Errorf("freebuff: post impression: %w", err)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("freebuff: post impression failed: status %d", resp.Status)
	}
	return nil
}

func extractImpressionURL(resp *channels.OutboundResponse) string {
	if resp == nil || len(resp.Body) == 0 {
		return ""
	}
	var decoded struct {
		ImpURL string `json:"impUrl"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return ""
	}
	return decoded.ImpURL
}

func (a *Adapter) sendAdsBeforeSession(ctx context.Context, tp channels.Transport, baseURL, credential, adsSessionID string, dp deviceProfile, transportProfiles ...channels.TransportProfile) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	emptyMsg := []map[string]any{}
	job := adsJob{
		stage:            "before_session",
		transport:        tp,
		baseURL:          baseURL,
		credential:       credential,
		transportProfile: firstTransportProfile(transportProfiles),
		adsSessionID:     adsSessionID,
		device:           dp,
		messages:         emptyMsg,
		surface:          "waiting_room",
	}
	if a.async.enqueueADS(job) {
		return true
	}
	logADSQueueFull(job.stage)
	return false
}

func (a *Adapter) sendAdsWithMessage(ctx context.Context, tp channels.Transport, baseURL, credential, adsSessionID string, dp deviceProfile, userMessage string, transportProfiles ...channels.TransportProfile) bool {
	if userMessage == "" {
		return false
	}
	if err := ctx.Err(); err != nil {
		return false
	}
	msg := []map[string]any{{"role": "user", "content": userMessage}}
	job := adsJob{
		stage:            "with_message",
		transport:        tp,
		baseURL:          baseURL,
		credential:       credential,
		transportProfile: firstTransportProfile(transportProfiles),
		adsSessionID:     adsSessionID,
		device:           dp,
		messages:         msg,
	}
	if a.async.enqueueADS(job) {
		return true
	}
	logADSQueueFull(job.stage)
	return false
}

func firstTransportProfile(profiles []channels.TransportProfile) channels.TransportProfile {
	if len(profiles) == 0 {
		return channels.TransportProfile{}
	}
	return profiles[0]
}

func (a *Adapter) sendHealthz(ctx context.Context, tp channels.Transport, baseURL string, transportProfile channels.TransportProfile) {
	h := http.Header{}
	h.Set("User-Agent", "Bun/1.3.11")
	h.Set("Accept", "*/*")
	h.Set("Accept-Encoding", "identity")
	h.Set("Connection", "keep-alive")
	h.Set("Host", "www.codebuff.com")
	_, _ = tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodGet,
		URL:              joinURL(baseURL, "/api/healthz"),
		Headers:          h,
		Timeout:          10 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffAPITransportProfile("healthz"), transportProfile),
	})
}

func (a *Adapter) createChildRun(ctx context.Context, tp channels.Transport, baseURL, credential, parentRunID string, transportProfile channels.TransportProfile) (string, error) {
	body := map[string]any{
		"action":         "START",
		"agentId":        "context-pruner",
		"ancestorRunIds": []string{parentRunID},
	}
	resp, err := a.postJSON(ctx, tp, baseURL, credential, "/api/v1/agent-runs", body, transportProfile)
	if err != nil {
		return "", err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return "", fmt.Errorf("freebuff: create child run failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	var decoded struct {
		RunID string `json:"runId"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return "", fmt.Errorf("freebuff: decode child run: %w", err)
	}
	if decoded.RunID == "" {
		return "", fmt.Errorf("freebuff: child run response missing runId")
	}
	return decoded.RunID, nil
}

func (a *Adapter) finishChildRun(ctx context.Context, tp channels.Transport, baseURL, credential, childRunID string, transportProfile channels.TransportProfile) error {
	return a.finishRun(ctx, tp, baseURL, credential, childRunID, "completed", 1, transportProfile)
}

func (a *Adapter) recordParentStep1(ctx context.Context, tp channels.Transport, baseURL, credential, parentRunID string, childRunIDs []string, start time.Time, transportProfile channels.TransportProfile) error {
	body := map[string]any{
		"stepNumber":  1,
		"credits":     0,
		"childRunIds": childRunIDs,
		"messageId":   nil,
		"status":      "completed",
		"startTime":   start.UTC().Format(time.RFC3339Nano),
	}
	resp, err := a.postJSON(ctx, tp, baseURL, credential, "/api/v1/agent-runs/"+parentRunID+"/steps", body, transportProfile)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("freebuff: record parent step 1 failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	return nil
}
