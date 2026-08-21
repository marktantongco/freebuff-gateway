package usage

import (
	"errors"
	"testing"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
)

type captureLogs struct {
	entries []logs.Entry
}

func (c *captureLogs) Append(entry logs.Entry) {
	c.entries = append(c.entries, entry)
}

type captureMetrics struct {
	channelID string
	accountID string
	tokens    int
	calls     int
}

func (c *captureMetrics) Observe(channelID, accountID string, tokens int) {
	c.channelID = channelID
	c.accountID = accountID
	c.tokens = tokens
	c.calls++
}

type captureResults struct {
	accountID string
	class     channels.ResponseClass
	deltas    []int64
	calls     int
}

func (c *captureResults) MarkResult(accountID string, class channels.ResponseClass, quotaDeltas ...int64) {
	c.accountID = accountID
	c.class = class
	c.deltas = append([]int64(nil), quotaDeltas...)
	c.calls++
}

func TestRecorderFansOutSuccessfulTokenUsage(t *testing.T) {
	logSink := &captureLogs{}
	metricSink := &captureMetrics{}
	resultSink := &captureResults{}
	recorder := NewRecorder(logSink, metricSink, resultSink)

	recorder.Record(Event{
		ChannelID:       "demo",
		AccountID:       "acct",
		SessionID:       "sess",
		Method:          "POST",
		Path:            "/v1/chat",
		Status:          200,
		Class:           channels.ClassOk,
		ClassKnown:      true,
		LatencyMS:       12,
		FirstResponseMS: 5,
		PhaseTimings:    map[string]any{"total_ms": int64(12)},
		Tokens:          Tokens{In: 4, Out: 6, Known: true},
	})

	if resultSink.calls != 1 || resultSink.class != channels.ClassOk || len(resultSink.deltas) != 1 || resultSink.deltas[0] != 10 {
		t.Fatalf("unexpected result sink: %+v", resultSink)
	}
	if metricSink.calls != 1 || metricSink.tokens != 10 {
		t.Fatalf("unexpected metric sink: %+v", metricSink)
	}
	if len(logSink.entries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(logSink.entries))
	}
	entry := logSink.entries[0]
	if entry.ResponseClass != "ok" || entry.FirstResponseMS != 5 || entry.TokensIn != 4 || entry.TokensOut != 6 || !entry.TokensKnown {
		t.Fatalf("unexpected log entry: %+v", entry)
	}
	if entry.PhaseTimings["total_ms"] != int64(12) {
		t.Fatalf("unexpected phase timings: %+v", entry.PhaseTimings)
	}
}

func TestRecorderDefaultsQuotaDeltaForUnknownTokens(t *testing.T) {
	resultSink := &captureResults{}
	metricSink := &captureMetrics{}
	recorder := NewRecorder(nil, metricSink, resultSink)

	recorder.Record(Event{
		ChannelID:  "demo",
		AccountID:  "acct",
		Class:      channels.ClassOk,
		ClassKnown: true,
		Tokens:     Tokens{},
	})

	if len(resultSink.deltas) != 1 || resultSink.deltas[0] != 1 {
		t.Fatalf("expected default quota delta 1, got %+v", resultSink.deltas)
	}
	if metricSink.calls != 1 || metricSink.tokens != 0 {
		t.Fatalf("expected zero-token metric observation, got %+v", metricSink)
	}
}

func TestRecorderUsesExplicitClassLabelWithoutMetricObservation(t *testing.T) {
	logSink := &captureLogs{}
	metricSink := &captureMetrics{}
	resultSink := &captureResults{}
	recorder := NewRecorder(logSink, metricSink, resultSink)
	errNetwork := errors.New("network down")

	recorder.Record(Event{
		ChannelID:  "demo",
		AccountID:  "acct",
		Class:      channels.ClassRetryable,
		ClassKnown: true,
		ClassLabel: "transport_error",
		Err:        errNetwork,
	})

	if resultSink.calls != 1 || resultSink.class != channels.ClassRetryable || len(resultSink.deltas) != 0 {
		t.Fatalf("unexpected result sink: %+v", resultSink)
	}
	if metricSink.calls != 0 {
		t.Fatalf("unexpected metric calls: %d", metricSink.calls)
	}
	if len(logSink.entries) != 1 || logSink.entries[0].ResponseClass != "transport_error" || logSink.entries[0].Error != errNetwork.Error() {
		t.Fatalf("unexpected log entries: %+v", logSink.entries)
	}
}

func TestRecorderSkipsResultSinkForUnclassifiedEvents(t *testing.T) {
	logSink := &captureLogs{}
	metricSink := &captureMetrics{}
	resultSink := &captureResults{}
	recorder := NewRecorder(logSink, metricSink, resultSink)

	recorder.Record(Event{
		ChannelID:  "missing",
		ClassLabel: "not_found",
		Err:        errors.New("missing channel"),
	})

	if resultSink.calls != 0 {
		t.Fatalf("unexpected result calls: %d", resultSink.calls)
	}
	if metricSink.calls != 0 {
		t.Fatalf("unexpected metric calls: %d", metricSink.calls)
	}
	if len(logSink.entries) != 1 || logSink.entries[0].ResponseClass != "not_found" {
		t.Fatalf("unexpected log entries: %+v", logSink.entries)
	}
}
