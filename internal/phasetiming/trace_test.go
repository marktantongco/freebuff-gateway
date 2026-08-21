package phasetiming

import (
	"context"
	"testing"
	"time"
)

func TestTraceSnapshotCopiesValues(t *testing.T) {
	trace := New(time.Now().Add(-25 * time.Millisecond))
	trace.Duration("prepare_total_ms", 12*time.Millisecond)
	trace.Bool("freebuff_ads_async", true)
	trace.String("freebuff_run_setup_mode", "sync_parallel")
	if !trace.MarkFirst("first_content_ms") {
		t.Fatalf("expected first mark to set value")
	}
	if trace.MarkFirst("first_content_ms") {
		t.Fatalf("expected second mark to be ignored")
	}

	snapshot := trace.Snapshot()
	if snapshot["prepare_total_ms"] != int64(12) ||
		snapshot["freebuff_ads_async"] != true ||
		snapshot["freebuff_run_setup_mode"] != "sync_parallel" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	firstContent, ok := snapshot["first_content_ms"].(int64)
	if !ok || firstContent < 0 {
		t.Fatalf("first content timing = %#v", snapshot["first_content_ms"])
	}
	snapshot["prepare_total_ms"] = int64(99)
	if trace.Snapshot()["prepare_total_ms"] != int64(12) {
		t.Fatalf("snapshot mutation leaked into trace")
	}
}

func TestTraceContextRoundTrip(t *testing.T) {
	trace := New(time.Now())
	ctx := ContextWithTrace(context.Background(), trace)
	if FromContext(ctx) != trace {
		t.Fatalf("trace did not round-trip through context")
	}
	if FromContext(context.Background()) != nil {
		t.Fatalf("unexpected trace in plain context")
	}
}
