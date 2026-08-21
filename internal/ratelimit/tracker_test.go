package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTrackerRecordAllowed(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      100,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	tr.RecordAllowed("192.168.1.1", 50)
	tr.RecordAllowed("192.168.1.1", 100)
	tr.RecordAllowed("192.168.1.2", 30)

	snap := tr.Snapshot()
	if snap.TotalRequests != 3 {
		t.Errorf("total requests = %d, want 3", snap.TotalRequests)
	}
	if snap.TotalAllowed != 3 {
		t.Errorf("total allowed = %d, want 3", snap.TotalAllowed)
	}
	if snap.TotalRejected != 0 {
		t.Errorf("total rejected = %d, want 0", snap.TotalRejected)
	}
	if snap.ActiveClients != 2 {
		t.Errorf("active clients = %d, want 2", snap.ActiveClients)
	}
	if len(snap.TopClients) != 2 {
		t.Errorf("top clients = %d, want 2", len(snap.TopClients))
	}
}

func TestTrackerRecordRejected(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      100,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	tr.RecordAllowed("10.0.0.1", 20)
	tr.RecordRejected("10.0.0.1")
	tr.RecordRejected("10.0.0.1")

	snap := tr.Snapshot()
	if snap.TotalRequests != 3 {
		t.Errorf("total requests = %d, want 3", snap.TotalRequests)
	}
	if snap.TotalRejected != 2 {
		t.Errorf("total rejected = %d, want 2", snap.TotalRejected)
	}
	if snap.RejectRate < 65 || snap.RejectRate > 68 {
		t.Errorf("reject rate = %.1f, want ~66.7", snap.RejectRate)
	}
	if len(snap.RecentRejections) != 1 {
		t.Errorf("recent rejections = %d, want 1", len(snap.RecentRejections))
	}
	if snap.RecentRejections[0].Rejected != 2 {
		t.Errorf("client rejected = %d, want 2", snap.RecentRejections[0].Rejected)
	}
}

func TestTrackerTopClientsSorted(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      100,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	for i := 0; i < 5; i++ {
		tr.RecordAllowed("client-b", 10)
	}
	for i := 0; i < 10; i++ {
		tr.RecordAllowed("client-a", 10)
	}
	for i := 0; i < 3; i++ {
		tr.RecordAllowed("client-c", 10)
	}

	snap := tr.Snapshot()
	if len(snap.TopClients) != 3 {
		t.Fatalf("top clients = %d, want 3", len(snap.TopClients))
	}
	if snap.TopClients[0].Key != "client-a" {
		t.Errorf("top client = %s, want client-a", snap.TopClients[0].Key)
	}
	if snap.TopClients[1].Key != "client-b" {
		t.Errorf("second client = %s, want client-b", snap.TopClients[1].Key)
	}
}

func TestTrackerRejectRate(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      100,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	tr.RecordAllowed("a", 10)
	tr.RecordAllowed("a", 10)
	tr.RecordRejected("a")

	snap := tr.Snapshot()
	if snap.RejectRate < 32 || snap.RejectRate > 34 {
		t.Errorf("reject rate = %.1f, want ~33.3", snap.RejectRate)
	}
}

func TestTrackerMaxClients(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      3,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	tr.RecordAllowed("c1", 10)
	tr.RecordAllowed("c2", 10)
	tr.RecordAllowed("c3", 10)
	tr.RecordAllowed("c4", 10) // should be dropped

	snap := tr.Snapshot()
	if snap.ActiveClients != 3 {
		t.Errorf("active clients = %d, want 3 (max)", snap.ActiveClients)
	}
}

func TestTrackerEmpty(t *testing.T) {
	tr := NewTracker(DefaultTrackerConfig())
	defer tr.Stop()

	snap := tr.Snapshot()
	if snap.TotalRequests != 0 {
		t.Errorf("total requests = %d, want 0", snap.TotalRequests)
	}
	if snap.ActiveClients != 0 {
		t.Errorf("active clients = %d, want 0", snap.ActiveClients)
	}
	if len(snap.TopClients) != 0 {
		t.Errorf("top clients = %d, want 0", len(snap.TopClients))
	}
}

func TestTrackerHandler(t *testing.T) {
	tr := NewTracker(DefaultTrackerConfig())
	defer tr.Stop()

	tr.RecordAllowed("10.0.0.1", 25)
	tr.RecordRejected("10.0.0.2")

	handler := tr.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/admin/rate-limits", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	body := w.Body.String()
	if body == "" {
		t.Error("empty response body")
	}
}

func TestTrackerConcurrent(t *testing.T) {
	tr := NewTracker(TrackerConfig{
		MaxClients:      1000,
		CleanupInterval: 0,
		StaleAfter:      time.Minute,
	})
	defer tr.Stop()

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := "client-" + string(rune('a'+id))
				if j%3 == 0 {
					tr.RecordRejected(key)
				} else {
					tr.RecordAllowed(key, int64(j))
				}
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	snap := tr.Snapshot()
	if snap.TotalRequests != 1000 {
		t.Errorf("total requests = %d, want 1000", snap.TotalRequests)
	}
}

func TestTrackerLatencyAverage(t *testing.T) {
	tr := NewTracker(DefaultTrackerConfig())
	defer tr.Stop()

	tr.RecordAllowed("a", 100)
	tr.RecordAllowed("a", 200)

	snap := tr.Snapshot()
	if len(snap.TopClients) != 1 {
		t.Fatalf("top clients = %d, want 1", len(snap.TopClients))
	}
	avg := snap.TopClients[0].AvgLatencyMS
	if avg < 149 || avg > 151 {
		t.Errorf("avg latency = %.1f, want ~150", avg)
	}
}
