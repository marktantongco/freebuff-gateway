package usage

import (
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/logs"
)

type Tokens struct {
	In    int
	Out   int
	Known bool
}

func (t Tokens) Total() int {
	total := t.In + t.Out
	if total < 0 {
		return 0
	}
	return total
}

func (t Tokens) QuotaDelta() int64 {
	if t.Known {
		if total := t.Total(); total > 0 {
			return int64(total)
		}
	}
	return 1
}

type Event struct {
	ChannelID       string
	AccountID       string
	SessionID       string
	Method          string
	Path            string
	Stream          bool
	SelectionKey    string
	Model           string
	Status          int
	Class           channels.ResponseClass
	ClassKnown      bool
	ClassLabel      string
	LatencyMS       int64
	FirstResponseMS int64
	PhaseTimings    map[string]any
	Tokens          Tokens
	Err             error
}

type LogSink interface {
	Append(logs.Entry)
}

type MetricsSink interface {
	Observe(channelID, accountID string, tokens int)
}

type ResultSink interface {
	MarkResult(accountID string, class channels.ResponseClass, quotaDeltas ...int64)
}

type Recorder struct {
	logs    LogSink
	metrics MetricsSink
	results ResultSink
}

func NewRecorder(logSink LogSink, metricSink MetricsSink, resultSink ResultSink) *Recorder {
	return &Recorder{logs: logSink, metrics: metricSink, results: resultSink}
}

func (r *Recorder) Record(event Event) {
	if r == nil {
		return
	}
	if event.ClassKnown && event.AccountID != "" && r.results != nil {
		if event.Class == channels.ClassOk {
			r.results.MarkResult(event.AccountID, event.Class, event.Tokens.QuotaDelta())
		} else {
			r.results.MarkResult(event.AccountID, event.Class)
		}
	}
	if event.ClassKnown && event.Class == channels.ClassOk && r.metrics != nil {
		r.metrics.Observe(event.ChannelID, event.AccountID, event.Tokens.Total())
	}
	if r.logs != nil {
		r.logs.Append(logEntry(event))
	}
}

func logEntry(event Event) logs.Entry {
	entry := logs.Entry{
		ChannelID:       event.ChannelID,
		AccountID:       event.AccountID,
		SessionID:       event.SessionID,
		Method:          event.Method,
		Path:            event.Path,
		Stream:          event.Stream,
		SelectionKey:    event.SelectionKey,
		Model:           event.Model,
		Status:          event.Status,
		ResponseClass:   responseClassLabel(event),
		LatencyMS:       event.LatencyMS,
		FirstResponseMS: event.FirstResponseMS,
		PhaseTimings:    clonePhaseTimings(event.PhaseTimings),
		TokensIn:        event.Tokens.In,
		TokensOut:       event.Tokens.Out,
		TokensKnown:     event.Tokens.Known,
	}
	if event.Err != nil {
		entry.Error = event.Err.Error()
	}
	return entry
}

func clonePhaseTimings(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func responseClassLabel(event Event) string {
	if event.ClassLabel != "" {
		return event.ClassLabel
	}
	if event.ClassKnown {
		return event.Class.String()
	}
	return ""
}
