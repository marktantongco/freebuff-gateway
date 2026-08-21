package orchestration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/phasetiming"
	"github.com/marktantongco/freebuff-gateway/internal/session"
	"github.com/marktantongco/freebuff-gateway/internal/usage"
)

type UsageRecorder interface {
	Record(usage.Event)
}

type Runner struct {
	registry  *channels.Registry
	sessions  *session.Manager
	transport channels.Transport
	usage     UsageRecorder
}

func NewRunner(
	reg *channels.Registry,
	sm *session.Manager,
	tp channels.Transport,
	recorder UsageRecorder,
) *Runner {
	return &Runner{registry: reg, sessions: sm, transport: tp, usage: recorder}
}

type Outcome struct {
	Response *channels.OutboundResponse
	Class    channels.ResponseClass
	Lease    *channels.Lease
}

type StreamExecution struct {
	Status  int
	Headers http.Header
	Class   channels.ResponseClass

	once    sync.Once
	pump    func(channels.StreamWriter) (*StreamOutcome, error)
	outcome *StreamOutcome
	err     error
}

func (e *StreamExecution) Pump(sink channels.StreamWriter) (*StreamOutcome, error) {
	if e == nil || e.pump == nil {
		return nil, fmt.Errorf("orchestration: nil stream execution")
	}
	e.once.Do(func() {
		e.outcome, e.err = e.pump(sink)
	})
	return e.outcome, e.err
}

type StreamOutcome struct {
	Status int
	Class  channels.ResponseClass
	Lease  *channels.Lease
	Err    error
}

func (r *Runner) Execute(ctx context.Context, in *channels.InboundRequest) (*Outcome, error) {
	started := time.Now()
	trace := phasetiming.New(started)
	ctx = phasetiming.ContextWithTrace(ctx, trace)
	ctx, closeTransportScope := r.requestScopedTransportContext(ctx)
	defer closeTransportScope()

	adapter, ok := r.registry.Get(in.ChannelID)
	if !ok {
		r.recordUsage(in, usage.Event{
			ClassLabel: "not_found",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        fmt.Errorf("channel %q not registered", in.ChannelID),
		}, trace, started)
		return nil, fmt.Errorf("orchestration: channel %q not registered", in.ChannelID)
	}

	acquireStarted := time.Now()
	lease, err := r.sessions.Acquire(ctx, in.ChannelID, in)
	trace.Duration("session_acquire_ms", time.Since(acquireStarted))
	if err != nil {
		r.recordUsage(in, usage.Event{
			ClassLabel: "lease_failed",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        err,
		}, trace, started)
		return nil, fmt.Errorf("orchestration: acquire lease: %w", err)
	}

	prepareStarted := time.Now()
	outbound, err := adapter.PrepareOutbound(ctx, lease, in)
	trace.Duration("prepare_total_ms", time.Since(prepareStarted))
	if err != nil {
		lease.Release(channels.VerdictHealthy)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			ClassLabel:   "prepare_failed",
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          err,
		}, trace, started)
		return nil, fmt.Errorf("orchestration: prepare outbound: %w", err)
	}

	transportStarted := time.Now()
	resp, err := r.transport.Do(ctx, outbound)
	responseReceived := time.Now()
	if err != nil {
		trace.Duration("transport_ttfb_ms", responseReceived.Sub(transportStarted))
		finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
			Request: outbound,
			Status:  0,
			Class:   channels.ClassRetryable,
			Err:     err,
		})
		lease.Release(channels.VerdictHealthy)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			Class:        channels.ClassRetryable,
			ClassKnown:   true,
			ClassLabel:   "transport_error",
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          joinedErr(err, finalizeErr),
		}, trace, started)
		return nil, fmt.Errorf("orchestration: transport: %w", err)
	}

	tokens := tokenUsageFromCounter(adapter, outbound, resp)
	firstResponseMS := firstResponseLatency(started, transportStarted, responseReceived, resp.FirstResponseMS)
	recordTransportTTFB(trace, resp.FirstResponseMS, responseReceived.Sub(transportStarted))
	class := adapter.ClassifyResponse(resp.Status, resp.Headers, resp.BodyPreview)
	if retryOutbound, retryOK, retryErr := r.retryOutbound(ctx, adapter, lease, in, channels.RetryOutcome{
		Request:     outbound,
		Status:      resp.Status,
		BodyPreview: resp.BodyPreview,
	}); retryErr != nil {
		finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
			Request:  outbound,
			Response: resp,
			Status:   resp.Status,
			Class:    class,
			Err:      retryErr,
		})
		lease.Release(channels.VerdictHealthy)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			Status:       resp.Status,
			Class:        class,
			ClassKnown:   true,
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          joinedErr(retryErr, finalizeErr),
		}, trace, started)
		return nil, fmt.Errorf("orchestration: retry outbound: %w", retryErr)
	} else if retryOK {
		outbound = retryOutbound
		transportStarted = time.Now()
		resp, err = r.transport.Do(ctx, outbound)
		responseReceived = time.Now()
		if err != nil {
			trace.Duration("transport_ttfb_ms", responseReceived.Sub(transportStarted))
			finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
				Request: outbound,
				Status:  0,
				Class:   channels.ClassRetryable,
				Err:     err,
			})
			lease.Release(channels.VerdictHealthy)
			r.recordUsage(in, usage.Event{
				AccountID:    lease.AccountID,
				SessionID:    lease.SessionID,
				SelectionKey: lease.Key,
				Model:        modelFromLease(lease),
				Class:        channels.ClassRetryable,
				ClassKnown:   true,
				ClassLabel:   "transport_error",
				LatencyMS:    time.Since(started).Milliseconds(),
				Err:          joinedErr(err, finalizeErr),
			}, trace, started)
			return nil, fmt.Errorf("orchestration: transport retry: %w", err)
		}
		tokens = tokenUsageFromCounter(adapter, outbound, resp)
		firstResponseMS = firstResponseLatency(started, transportStarted, responseReceived, resp.FirstResponseMS)
		recordTransportTTFB(trace, resp.FirstResponseMS, responseReceived.Sub(transportStarted))
		class = adapter.ClassifyResponse(resp.Status, resp.Headers, resp.BodyPreview)
	}
	finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
		Request:  outbound,
		Response: resp,
		Status:   resp.Status,
		Class:    class,
	})
	verdict := adapter.SessionPolicy().ClassifySessionHealth(lease.State, class)
	lease.Release(verdict)
	r.recordUsage(in, usage.Event{
		AccountID:       lease.AccountID,
		SessionID:       lease.SessionID,
		SelectionKey:    lease.Key,
		Model:           modelFromLease(lease),
		Status:          resp.Status,
		Class:           class,
		ClassKnown:      true,
		LatencyMS:       time.Since(started).Milliseconds(),
		FirstResponseMS: firstResponseMS,
		Tokens:          tokens,
		Err:             finalizeErr,
	}, trace, started)

	return &Outcome{Response: resp, Class: class, Lease: lease}, nil
}

func (r *Runner) requestScopedTransportContext(ctx context.Context) (context.Context, func()) {
	scoped, ok := r.transport.(channels.RequestScopedTransport)
	if !ok {
		return ctx, func() {}
	}
	return scoped.WithRequestScope(ctx)
}

func (r *Runner) ExecuteStream(ctx context.Context, in *channels.InboundRequest) (*StreamExecution, error) {
	started := time.Now()
	trace := phasetiming.New(started)
	ctx = phasetiming.ContextWithTrace(ctx, trace)
	ctx, closeTransportScope := r.requestScopedTransportContext(ctx)
	scopeHandedToPump := false
	defer func() {
		if !scopeHandedToPump {
			closeTransportScope()
		}
	}()

	adapter, ok := r.registry.Get(in.ChannelID)
	if !ok {
		err := fmt.Errorf("orchestration: channel %q not registered", in.ChannelID)
		r.recordUsage(in, usage.Event{
			Stream:     true,
			ClassLabel: "not_found",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        err,
		}, trace, started)
		return nil, err
	}
	streamAdapter, ok := adapter.(channels.StreamAdapter)
	if !ok {
		err := fmt.Errorf("orchestration: channel %q does not support streaming", in.ChannelID)
		r.recordUsage(in, usage.Event{
			Stream:     true,
			ClassLabel: "stream_unsupported",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        err,
		}, trace, started)
		return nil, err
	}
	streamTransport, ok := r.transport.(channels.StreamTransport)
	if !ok {
		err := fmt.Errorf("orchestration: transport does not support streaming")
		r.recordUsage(in, usage.Event{
			Stream:     true,
			ClassLabel: "stream_transport_unsupported",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        err,
		}, trace, started)
		return nil, err
	}

	acquireStarted := time.Now()
	lease, err := r.sessions.Acquire(ctx, in.ChannelID, in)
	trace.Duration("session_acquire_ms", time.Since(acquireStarted))
	if err != nil {
		r.recordUsage(in, usage.Event{
			Stream:     true,
			ClassLabel: "lease_failed",
			LatencyMS:  time.Since(started).Milliseconds(),
			Err:        err,
		}, trace, started)
		return nil, fmt.Errorf("orchestration: acquire lease: %w", err)
	}

	prepareStarted := time.Now()
	outbound, err := streamAdapter.PrepareStreamOutbound(ctx, lease, in)
	trace.Duration("prepare_total_ms", time.Since(prepareStarted))
	if err != nil {
		lease.Release(channels.VerdictHealthy)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			Stream:       true,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			ClassLabel:   "prepare_stream_failed",
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          err,
		}, trace, started)
		return nil, fmt.Errorf("orchestration: prepare stream outbound: %w", err)
	}

	transportStarted := time.Now()
	resp, err := streamTransport.DoStream(ctx, outbound)
	responseReceived := time.Now()
	if err != nil {
		trace.Duration("transport_ttfb_ms", responseReceived.Sub(transportStarted))
		finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
			Request: outbound,
			Status:  0,
			Class:   channels.ClassRetryable,
			Err:     err,
		})
		lease.Release(channels.VerdictHealthy)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			Stream:       true,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			Class:        channels.ClassRetryable,
			ClassKnown:   true,
			ClassLabel:   "stream_transport_error",
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          joinedErr(err, finalizeErr),
		}, trace, started)
		return nil, fmt.Errorf("orchestration: stream transport: %w", err)
	}
	class := streamAdapter.ClassifyStreamResponse(resp.Status, resp.Headers, resp.BodyPreview)
	firstResponseMS := firstResponseLatency(started, transportStarted, responseReceived, resp.FirstResponseMS)
	recordTransportTTFB(trace, resp.FirstResponseMS, responseReceived.Sub(transportStarted))
	if retryOutbound, retryOK, retryErr := r.retryOutbound(ctx, adapter, lease, in, channels.RetryOutcome{
		Request:     outbound,
		Status:      resp.Status,
		BodyPreview: resp.BodyPreview,
	}); retryErr != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
			Request: outbound,
			Status:  resp.Status,
			Class:   class,
			Err:     retryErr,
		})
		lease.Release(channels.VerdictHealthy)
		err := fmt.Errorf("orchestration: retry stream outbound: %w", retryErr)
		r.recordUsage(in, usage.Event{
			AccountID:    lease.AccountID,
			SessionID:    lease.SessionID,
			Stream:       true,
			SelectionKey: lease.Key,
			Model:        modelFromLease(lease),
			Status:       resp.Status,
			Class:        class,
			ClassKnown:   true,
			LatencyMS:    time.Since(started).Milliseconds(),
			Err:          joinedErr(err, finalizeErr),
		}, trace, started)
		return nil, err
	} else if retryOK {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		outbound = retryOutbound
		transportStarted = time.Now()
		resp, err = streamTransport.DoStream(ctx, outbound)
		responseReceived = time.Now()
		if err != nil {
			trace.Duration("transport_ttfb_ms", responseReceived.Sub(transportStarted))
			finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
				Request: outbound,
				Status:  0,
				Class:   channels.ClassRetryable,
				Err:     err,
			})
			lease.Release(channels.VerdictHealthy)
			r.recordUsage(in, usage.Event{
				AccountID:    lease.AccountID,
				SessionID:    lease.SessionID,
				Stream:       true,
				SelectionKey: lease.Key,
				Model:        modelFromLease(lease),
				Class:        channels.ClassRetryable,
				ClassKnown:   true,
				ClassLabel:   "stream_transport_error",
				LatencyMS:    time.Since(started).Milliseconds(),
				Err:          joinedErr(err, finalizeErr),
			}, trace, started)
			return nil, fmt.Errorf("orchestration: stream transport retry: %w", err)
		}
		class = streamAdapter.ClassifyStreamResponse(resp.Status, resp.Headers, resp.BodyPreview)
		firstResponseMS = firstResponseLatency(started, transportStarted, responseReceived, resp.FirstResponseMS)
		recordTransportTTFB(trace, resp.FirstResponseMS, responseReceived.Sub(transportStarted))
	}
	execution := &StreamExecution{
		Status:  resp.Status,
		Headers: cloneHeader(resp.Headers),
		Class:   class,
	}
	scopeHandedToPump = true
	execution.pump = func(sink channels.StreamWriter) (*StreamOutcome, error) {
		defer closeTransportScope()
		pumpStarted := time.Now()
		tokens, pumpErr := pumpStreamBody(ctx, adapter, lease, in, class, resp.Body, sink)
		if resp.Body != nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				pumpErr = errors.Join(pumpErr, closeErr)
			}
		}
		trace.Duration("stream_pump_ms", time.Since(pumpStarted))
		if isClientDisconnect(pumpErr) {
			pumpErr = nil
		}
		resultClass := class
		if pumpErr != nil && resultClass == channels.ClassOk {
			resultClass = channels.ClassRetryable
		}
		responseErr := streamResponseError(resp.Status, resultClass, resp.BodyPreview)
		finalizeErr := r.finalize(ctx, adapter, lease, channels.FinalizeOutcome{
			Request: outbound,
			Status:  resp.Status,
			Class:   resultClass,
			Err:     pumpErr,
		})
		outcomeErr := errors.Join(pumpErr, responseErr, finalizeErr)
		verdict := adapter.SessionPolicy().ClassifySessionHealth(lease.State, resultClass)
		lease.Release(verdict)
		r.recordUsage(in, usage.Event{
			AccountID:       lease.AccountID,
			SessionID:       lease.SessionID,
			Status:          resp.Status,
			Stream:          true,
			SelectionKey:    lease.Key,
			Model:           modelFromLease(lease),
			Class:           resultClass,
			ClassKnown:      true,
			LatencyMS:       time.Since(started).Milliseconds(),
			FirstResponseMS: firstResponseMS,
			Tokens:          tokens,
			Err:             outcomeErr,
		}, trace, started)
		outcome := &StreamOutcome{
			Status: resp.Status,
			Class:  resultClass,
			Lease:  lease,
			Err:    outcomeErr,
		}
		return outcome, outcomeErr
	}
	return execution, nil
}

func (r *Runner) finalize(ctx context.Context, adapter channels.ChannelAdapter, lease *channels.Lease, outcome channels.FinalizeOutcome) error {
	finalizer, ok := adapter.(channels.Finalizer)
	if !ok {
		return nil
	}
	started := time.Now()
	defer func() {
		if trace := phasetiming.FromContext(ctx); trace != nil {
			trace.Duration("finalize_ms", time.Since(started))
		}
	}()
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := finalizer.Finalize(finalizeCtx, lease, outcome); err != nil {
		return fmt.Errorf("orchestration: finalize: %w", err)
	}
	return nil
}

func (r *Runner) retryOutbound(ctx context.Context, adapter channels.ChannelAdapter, lease *channels.Lease, in *channels.InboundRequest, outcome channels.RetryOutcome) (*channels.OutboundRequest, bool, error) {
	retrier, ok := adapter.(channels.OutboundRetrier)
	if !ok {
		return nil, false, nil
	}
	started := time.Now()
	defer func() {
		if trace := phasetiming.FromContext(ctx); trace != nil {
			trace.Duration("retry_prepare_ms", time.Since(started))
		}
	}()
	retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return retrier.RetryOutbound(retryCtx, lease, in, outcome)
}

func (r *Runner) recordUsage(in *channels.InboundRequest, event usage.Event, trace *phasetiming.Trace, started time.Time) {
	if r.usage == nil {
		return
	}
	event.ChannelID = in.ChannelID
	event.Method = in.Method
	event.Path = in.Path
	event.PhaseTimings = phaseTimings(trace, started)
	r.usage.Record(event)
}

func modelFromLease(lease *channels.Lease) string {
	if lease == nil {
		return ""
	}
	if model := lease.State.String("model"); model != "" {
		return model
	}
	for key, value := range lease.State {
		model, ok := value.(string)
		if ok && model != "" && strings.HasSuffix(strings.ToLower(key), "_model") {
			return model
		}
	}
	if prefix, model, ok := strings.Cut(lease.Key, "|"); ok && prefix != "" && model != "" {
		return model
	}
	return ""
}

func tokenUsageFromCounter(
	adapter channels.ChannelAdapter,
	req *channels.OutboundRequest,
	resp *channels.OutboundResponse,
) usage.Tokens {
	tc, ok := adapter.(channels.TokenCounter)
	if !ok {
		return usage.Tokens{}
	}
	tokensIn, tokensOut, counted := tc.TokenUsage(req, resp)
	if !counted {
		return usage.Tokens{}
	}
	return usage.Tokens{In: tokensIn, Out: tokensOut, Known: true}
}

func usageTokensFromChannel(tokens channels.TokenUsage) usage.Tokens {
	return usage.Tokens{In: tokens.InputTokens, Out: tokens.OutputTokens, Known: tokens.Known}
}

func firstResponseLatency(started, transportStarted, responseReceived time.Time, upstreamMS int64) int64 {
	if upstreamMS > 0 {
		firstResponseMS := transportStarted.Sub(started).Milliseconds() + upstreamMS
		if firstResponseMS < 0 {
			return 0
		}
		return firstResponseMS
	}
	return responseReceived.Sub(started).Milliseconds()
}

func recordTransportTTFB(trace *phasetiming.Trace, upstreamMS int64, fallback time.Duration) {
	if upstreamMS > 0 {
		trace.Duration("transport_ttfb_ms", time.Duration(upstreamMS)*time.Millisecond)
		return
	}
	trace.Duration("transport_ttfb_ms", fallback)
}

func phaseTimings(trace *phasetiming.Trace, started time.Time) map[string]any {
	if trace == nil {
		return nil
	}
	trace.Since("total_ms", started)
	return trace.Snapshot()
}

func joinedErr(primary, secondary error) error {
	return errors.Join(primary, secondary)
}

func streamResponseError(status int, class channels.ResponseClass, preview []byte) error {
	if status >= 200 && status < 300 && class == channels.ClassOk {
		return nil
	}
	text := strings.TrimSpace(string(preview))
	if text == "" {
		return fmt.Errorf("upstream stream failed: status %d class %s", status, class.String())
	}
	return fmt.Errorf("upstream stream failed: status %d class %s: %s", status, class.String(), text)
}

func isClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "broken pipe") ||
		strings.Contains(text, "connection reset by peer") ||
		strings.Contains(text, "client disconnected") ||
		strings.Contains(text, "context canceled")
}

func pumpStreamBody(ctx context.Context, adapter channels.ChannelAdapter, lease *channels.Lease, in *channels.InboundRequest, class channels.ResponseClass, body io.Reader, sink channels.StreamWriter) (usage.Tokens, error) {
	if sink == nil {
		return usage.Tokens{}, fmt.Errorf("orchestration: nil stream sink")
	}
	if body == nil {
		return usage.Tokens{}, nil
	}
	if class == channels.ClassOk {
		if rewriter, ok := adapter.(channels.StreamUsageRewriter); ok {
			rewriteStarted := time.Now()
			tokens, err := rewriter.RewriteStreamWithUsage(ctx, lease, in, body, sink)
			if trace := phasetiming.FromContext(ctx); trace != nil {
				trace.Duration("stream_rewrite_ms", time.Since(rewriteStarted))
			}
			return usageTokensFromChannel(tokens), err
		}
		if rewriter, ok := adapter.(channels.StreamRewriter); ok {
			rewriteStarted := time.Now()
			err := rewriter.RewriteStream(ctx, lease, in, body, sink)
			if trace := phasetiming.FromContext(ctx); trace != nil {
				trace.Duration("stream_rewrite_ms", time.Since(rewriteStarted))
			}
			return usage.Tokens{}, err
		}
	}
	buf := make([]byte, 32*1024)
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			written, writeErr := sink.Write(buf[:n])
			sink.Flush()
			if writeErr != nil {
				return usage.Tokens{}, fmt.Errorf("orchestration: stream write: %w", writeErr)
			}
			if written != n {
				return usage.Tokens{}, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return usage.Tokens{}, nil
			}
			return usage.Tokens{}, fmt.Errorf("orchestration: stream read: %w", readErr)
		}
	}
}

func cloneHeader(h http.Header) http.Header {
	if h == nil {
		return http.Header{}
	}
	return h.Clone()
}
