package usage

import (
	"testing"
	"time"
)

func TestAnalyticsEmpty(t *testing.T) {
	a := NewAnalytics(nil)
	snap := a.Snapshot()
	if snap.Summary.TotalRequests != 0 {
		t.Errorf("total requests = %d, want 0", snap.Summary.TotalRequests)
	}
	if len(snap.TopEndpoints) != 0 {
		t.Errorf("top endpoints = %d, want 0", len(snap.TopEndpoints))
	}
}

func TestAnalyticsRecordAndSnapshot(t *testing.T) {
	a := NewAnalytics(nil)
	now := time.Now().UnixMilli()

	a.Record(RequestRecord{
		ID: "r1", Method: "POST", Path: "/v1/chat/completions",
		Status: 200, LatencyMS: 100, TokensIn: 50, TokensOut: 100,
		Model: "gpt-4", CreatedAt: now,
	})
	a.Record(RequestRecord{
		ID: "r2", Method: "POST", Path: "/v1/chat/completions",
		Status: 200, LatencyMS: 200, TokensIn: 30, TokensOut: 80,
		Model: "gpt-4", CreatedAt: now,
	})
	a.Record(RequestRecord{
		ID: "r3", Method: "GET", Path: "/v1/models",
		Status: 200, LatencyMS: 10, CreatedAt: now,
	})
	a.Record(RequestRecord{
		ID: "r4", Method: "POST", Path: "/v1/chat/completions",
		Status: 500, LatencyMS: 500, Error: "internal error",
		Model: "gpt-4", CreatedAt: now,
	})

	snap := a.Snapshot()

	// Summary
	if snap.Summary.TotalRequests != 4 {
		t.Errorf("total requests = %d, want 4", snap.Summary.TotalRequests)
	}
	if snap.Summary.SuccessCount != 3 {
		t.Errorf("success count = %d, want 3", snap.Summary.SuccessCount)
	}
	if snap.Summary.ErrorCount != 1 {
		t.Errorf("error count = %d, want 1", snap.Summary.ErrorCount)
	}

	// Error rate
	if snap.ErrorRate < 24 || snap.ErrorRate > 26 {
		t.Errorf("error rate = %.1f, want ~25", snap.ErrorRate)
	}

	// Top endpoints
	if len(snap.TopEndpoints) != 2 {
		t.Fatalf("top endpoints = %d, want 2", len(snap.TopEndpoints))
	}
	if snap.TopEndpoints[0].Path != "/v1/chat/completions" {
		t.Errorf("top endpoint = %s, want /v1/chat/completions", snap.TopEndpoints[0].Path)
	}
	if snap.TopEndpoints[0].Requests != 3 {
		t.Errorf("top endpoint requests = %d, want 3", snap.TopEndpoints[0].Requests)
	}

	// Top models
	if len(snap.TopModels) != 1 {
		t.Fatalf("top models = %d, want 1", len(snap.TopModels))
	}
	if snap.TopModels[0].Model != "gpt-4" {
		t.Errorf("top model = %s, want gpt-4", snap.TopModels[0].Model)
	}
	if snap.TopModels[0].Requests != 3 {
		t.Errorf("model requests = %d, want 3", snap.TopModels[0].Requests)
	}

	// Tokens
	if snap.TokensSummary.TotalIn != 80 {
		t.Errorf("tokens in = %d, want 80", snap.TokensSummary.TotalIn)
	}
	if snap.TokensSummary.TotalOut != 180 {
		t.Errorf("tokens out = %d, want 180", snap.TokensSummary.TotalOut)
	}
}

func TestAnalyticsLatencyPercentiles(t *testing.T) {
	a := NewAnalytics(nil)
	now := time.Now().UnixMilli()

	// Create 100 requests with latencies 1-100
	for i := 1; i <= 100; i++ {
		a.Record(RequestRecord{
			ID: "r", Status: 200, LatencyMS: int64(i),
			CreatedAt: now,
		})
	}

	snap := a.Snapshot()
	p := snap.LatencyPercentiles

	if p.P50 != 50 {
		t.Errorf("p50 = %d, want 50", p.P50)
	}
	if p.P90 != 90 {
		t.Errorf("p90 = %d, want 90", p.P90)
	}
	if p.P95 != 95 {
		t.Errorf("p95 = %d, want 95", p.P95)
	}
	if p.Max != 100 {
		t.Errorf("max = %d, want 100", p.Max)
	}
}

func TestAnalyticsStatusBreakdown(t *testing.T) {
	a := NewAnalytics(nil)
	now := time.Now().UnixMilli()

	for i := 0; i < 80; i++ {
		a.Record(RequestRecord{Status: 200, LatencyMS: 10, CreatedAt: now})
	}
	for i := 0; i < 15; i++ {
		a.Record(RequestRecord{Status: 429, LatencyMS: 5, CreatedAt: now})
	}
	for i := 0; i < 5; i++ {
		a.Record(RequestRecord{Status: 500, LatencyMS: 500, CreatedAt: now})
	}

	snap := a.Snapshot()
	if len(snap.StatusBreakdown) != 3 {
		t.Fatalf("status breakdown = %d, want 3", len(snap.StatusBreakdown))
	}

	// Should be sorted by status code
	if snap.StatusBreakdown[0].Status != 200 {
		t.Errorf("first status = %d, want 200", snap.StatusBreakdown[0].Status)
	}
	if snap.StatusBreakdown[0].Count != 80 {
		t.Errorf("200 count = %d, want 80", snap.StatusBreakdown[0].Count)
	}
}

func TestAnalyticsStreamRequests(t *testing.T) {
	a := NewAnalytics(nil)
	now := time.Now().UnixMilli()

	a.Record(RequestRecord{Stream: true, Status: 200, LatencyMS: 10, CreatedAt: now})
	a.Record(RequestRecord{Stream: true, Status: 200, LatencyMS: 20, CreatedAt: now})
	a.Record(RequestRecord{Stream: false, Status: 200, LatencyMS: 5, CreatedAt: now})

	snap := a.Snapshot()
	if snap.Summary.StreamRequests != 2 {
		t.Errorf("stream requests = %d, want 2", snap.Summary.StreamRequests)
	}
	if snap.Summary.NonStreamRequests != 1 {
		t.Errorf("non-stream requests = %d, want 1", snap.Summary.NonStreamRequests)
	}
}

func TestAnalyticsHourlyTraffic(t *testing.T) {
	a := NewAnalytics(nil)
	hour1 := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC).UnixMilli()
	hour2 := time.Date(2026, 8, 22, 11, 0, 0, 0, time.UTC).UnixMilli()

	a.Record(RequestRecord{Status: 200, LatencyMS: 10, CreatedAt: hour1})
	a.Record(RequestRecord{Status: 200, LatencyMS: 10, CreatedAt: hour1})
	a.Record(RequestRecord{Status: 200, LatencyMS: 10, CreatedAt: hour2})

	snap := a.Snapshot()
	if len(snap.HourlyTraffic) != 2 {
		t.Fatalf("hourly points = %d, want 2", len(snap.HourlyTraffic))
	}
}

func TestAnalyticsNormalizePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/v1/chat/completions", "/v1/chat/completions"},
		{"/v1/models/gpt-4", "/v1/models"},
		{"/v1/models", "/v1/models"},
		{"/api/admin/users/u_123", "/api/admin/users"},
		{"/api/admin/users", "/api/admin/users"},
	}
	for _, tt := range tests {
		got := normalizePath(tt.input)
		if got != tt.want {
			t.Errorf("normalizePath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestAnalyticsEndpointErrorRate(t *testing.T) {
	a := NewAnalytics(nil)
	now := time.Now().UnixMilli()

	// 8 success, 2 errors on same endpoint
	for i := 0; i < 8; i++ {
		a.Record(RequestRecord{Path: "/v1/chat/completions", Status: 200, LatencyMS: 10, CreatedAt: now})
	}
	for i := 0; i < 2; i++ {
		a.Record(RequestRecord{Path: "/v1/chat/completions", Status: 500, LatencyMS: 500, CreatedAt: now})
	}

	snap := a.Snapshot()
	if len(snap.TopEndpoints) != 1 {
		t.Fatalf("endpoints = %d, want 1", len(snap.TopEndpoints))
	}
	if snap.TopEndpoints[0].ErrorRate < 19 || snap.TopEndpoints[0].ErrorRate > 21 {
		t.Errorf("endpoint error rate = %.1f, want ~20", snap.TopEndpoints[0].ErrorRate)
	}
}

func TestAnalyticsHandler(t *testing.T) {
	a := NewAnalytics(nil)
	a.Record(RequestRecord{Status: 200, LatencyMS: 10})

	handler := a.Handler()
	if handler == nil {
		t.Fatal("handler is nil")
	}
}
