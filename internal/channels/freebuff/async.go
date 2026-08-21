package freebuff

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/phasetiming"
)

const (
	defaultADSQueueSize      = 64
	defaultFinalizeQueueSize = 64
	defaultRunTreeQueueSize  = 64
	asyncADSTimeout          = 10 * time.Second
	asyncFinalizeTimeout     = 10 * time.Second
	asyncRunTreeTimeout      = 10 * time.Second
	asyncRunTreeSlowLogAfter = 5 * time.Second
	finalizeInlineTimeout    = 250 * time.Millisecond
)

type asyncSideEffects struct {
	ads      chan adsJob
	finalize chan finalizeJob
	runTree  chan runTreeJob
}

type adsJob struct {
	stage            string
	transport        channels.Transport
	baseURL          string
	credential       string
	transportProfile channels.TransportProfile
	adsSessionID     string
	device           deviceProfile
	messages         []map[string]any
	surface          string
}

type finalizeJob struct {
	transport        channels.Transport
	baseURL          string
	credential       string
	transportProfile channels.TransportProfile
	runID            string
	status           string
	steps            int
	messageID        string
	recordStep       bool
	startedAt        time.Time
}

type runTreeJob struct {
	transport        channels.Transport
	baseURL          string
	credential       string
	transportProfile channels.TransportProfile
	parentRunID      string
}

func newAsyncSideEffects(adsQueueSize, finalizeQueueSize int, runTreeQueueSize ...int) *asyncSideEffects {
	if adsQueueSize < 1 {
		adsQueueSize = defaultADSQueueSize
	}
	if finalizeQueueSize < 1 {
		finalizeQueueSize = defaultFinalizeQueueSize
	}
	runTreeSize := defaultRunTreeQueueSize
	if len(runTreeQueueSize) > 0 {
		runTreeSize = runTreeQueueSize[0]
	}
	if runTreeSize < 1 {
		runTreeSize = defaultRunTreeQueueSize
	}
	return &asyncSideEffects{
		ads:      make(chan adsJob, adsQueueSize),
		finalize: make(chan finalizeJob, finalizeQueueSize),
		runTree:  make(chan runTreeJob, runTreeSize),
	}
}

func (w *asyncSideEffects) Run(ctx context.Context, a *Adapter) {
	if w == nil || a == nil {
		<-ctx.Done()
		return
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		w.runADS(ctx, a)
	}()
	go func() {
		defer wg.Done()
		w.runFinalize(ctx, a)
	}()
	go func() {
		defer wg.Done()
		w.runRunTree(ctx, a)
	}()
	<-ctx.Done()
	wg.Wait()
}

func (w *asyncSideEffects) enqueueADS(job adsJob) bool {
	if w == nil {
		return false
	}
	select {
	case w.ads <- job:
		return true
	default:
		return false
	}
}

func (w *asyncSideEffects) enqueueFinalize(job finalizeJob) bool {
	if w == nil {
		return false
	}
	select {
	case w.finalize <- job:
		return true
	default:
		return false
	}
}

func (w *asyncSideEffects) enqueueRunTree(job runTreeJob) bool {
	if w == nil {
		return false
	}
	select {
	case w.runTree <- job:
		return true
	default:
		return false
	}
}

func logADSQueueFull(stage string) {
	log.Printf("freebuff: async ads dropped stage=%s reason=queue_full", stage)
}

func logFinalizerQueueFull(status string) {
	log.Printf("freebuff: async finalizer queue full status=%s action=inline_fallback", status)
}

func logRunTreeQueueFull() {
	log.Printf("freebuff: async run tree dropped reason=queue_full")
}

func (w *asyncSideEffects) runADS(ctx context.Context, a *Adapter) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.ads:
			jobCtx, cancel := context.WithTimeout(ctx, asyncADSTimeout)
			if err := job.run(jobCtx, a); err != nil {
				log.Printf("freebuff: async ads failed stage=%s: %s", job.stage, sanitizeAsyncError(err))
			}
			cancel()
		}
	}
}

func (w *asyncSideEffects) runFinalize(ctx context.Context, a *Adapter) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.finalize:
			jobCtx, cancel := context.WithTimeout(ctx, asyncFinalizeTimeout)
			if err := job.run(jobCtx, a); err != nil {
				log.Printf("freebuff: async finalizer failed status=%s: %s", job.status, sanitizeAsyncError(err))
			}
			cancel()
		}
	}
}

func (w *asyncSideEffects) runRunTree(ctx context.Context, a *Adapter) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.runTree:
			started := time.Now()
			jobCtx, cancel := context.WithTimeout(ctx, asyncRunTreeTimeout)
			trace := phasetiming.New(started)
			jobCtx = phasetiming.ContextWithTrace(jobCtx, trace)
			if err := job.run(jobCtx, a); err != nil {
				log.Printf(
					"freebuff: async run tree failed total_ms=%d phases=%s: %s",
					time.Since(started).Milliseconds(),
					formatAsyncPhaseTimings(trace.Snapshot()),
					sanitizeAsyncError(err),
				)
			} else if elapsed := time.Since(started); elapsed >= asyncRunTreeSlowLogAfter {
				log.Printf(
					"freebuff: async run tree slow total_ms=%d phases=%s",
					elapsed.Milliseconds(),
					formatAsyncPhaseTimings(trace.Snapshot()),
				)
			}
			cancel()
		}
	}
}

func (j adsJob) run(ctx context.Context, a *Adapter) error {
	if j.transport == nil {
		return nil
	}
	var errs []error
	for _, provider := range []string{"gravity", "zeroclick"} {
		resp, err := a.doAdsRequest(ctx, j.transport, j.baseURL, j.credential, buildAdsBody(provider, j.adsSessionID, j.messages, j.surface, j.device), j.transportProfile)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if provider == "zeroclick" {
			if impURL := extractImpressionURL(resp); impURL != "" {
				if err := a.postAdImpression(ctx, j.transport, j.baseURL, j.credential, impURL, "LITE", j.transportProfile); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func (j finalizeJob) run(ctx context.Context, a *Adapter) error {
	if j.transport == nil || j.runID == "" {
		return nil
	}
	a.sendHealthz(ctx, j.transport, j.baseURL, j.transportProfile)
	if j.recordStep {
		if err := j.recordStep2(ctx, a); err != nil {
			return err
		}
	}
	if err := a.finishRun(ctx, j.transport, j.baseURL, j.credential, j.runID, j.status, j.steps, j.transportProfile); err != nil {
		return err
	}
	return nil
}

func (j runTreeJob) run(ctx context.Context, a *Adapter) error {
	if j.transport == nil || j.parentRunID == "" {
		return nil
	}
	childCreateStarted := time.Now()
	childRunID, err := a.createChildRun(ctx, j.transport, j.baseURL, j.credential, j.parentRunID, j.transportProfile)
	recordFreeBuffPhase(ctx, "freebuff_create_child_run_ms", childCreateStarted)
	if err != nil {
		return fmt.Errorf("freebuff: async run tree create child: %w", err)
	}
	childStart := time.Now()
	parallelStarted := time.Now()
	if err := a.completeFreeBuffRunTree(ctx, j.transport, j.baseURL, j.credential, j.transportProfile, j.parentRunID, childRunID, childStart); err != nil {
		recordFreeBuffPhase(ctx, "freebuff_setup_parallel_wait_ms", parallelStarted)
		return fmt.Errorf("freebuff: async run tree complete: %w", err)
	}
	recordFreeBuffPhase(ctx, "freebuff_setup_parallel_wait_ms", parallelStarted)
	return nil
}

func (j finalizeJob) recordStep2(ctx context.Context, a *Adapter) error {
	body := map[string]any{
		"stepNumber":  2,
		"credits":     0,
		"childRunIds": []any{},
		"messageId":   nil,
		"status":      j.status,
		"startTime":   j.startedAt.UTC().Format(time.RFC3339Nano),
	}
	if j.messageID != "" {
		body["messageId"] = j.messageID
	}
	resp, err := a.postJSON(ctx, j.transport, j.baseURL, j.credential, "/api/v1/agent-runs/"+j.runID+"/steps", body, j.transportProfile)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("freebuff: record run step failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	return nil
}

func sanitizeAsyncError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = redactAfter(msg, "Bearer ")
	msg = redactPathToken(msg, "/api/v1/agent-runs/")
	if len(msg) > 240 {
		return msg[:240] + "..."
	}
	return msg
}

func formatAsyncPhaseTimings(snapshot map[string]any) string {
	if len(snapshot) == 0 {
		return "{}"
	}
	keys := []string{
		"freebuff_create_child_run_ms",
		"freebuff_child_step_ms",
		"freebuff_child_finish_ms",
		"freebuff_parent_step_ms",
		"freebuff_setup_parallel_wait_ms",
	}
	var parts []string
	for _, key := range keys {
		if value, ok := snapshot[key]; ok {
			parts = append(parts, fmt.Sprintf("%s=%v", key, value))
		}
	}
	if len(parts) == 0 {
		return "{}"
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func redactAfter(msg, marker string) string {
	searchFrom := 0
	for {
		idx := strings.Index(msg[searchFrom:], marker)
		if idx < 0 {
			return msg
		}
		idx += searchFrom
		start := idx + len(marker)
		end := start
		for end < len(msg) && !isRedactionBoundary(msg[end]) {
			end++
		}
		msg = msg[:start] + "<redacted>" + msg[end:]
		searchFrom = start + len("<redacted>")
	}
}

func redactPathToken(msg, prefix string) string {
	searchFrom := 0
	for {
		idx := strings.Index(msg[searchFrom:], prefix)
		if idx < 0 {
			return msg
		}
		idx += searchFrom
		start := idx + len(prefix)
		end := start
		for end < len(msg) && !isRedactionBoundary(msg[end]) {
			end++
		}
		msg = msg[:start] + "<redacted>" + msg[end:]
		searchFrom = start + len("<redacted>")
	}
}

func isRedactionBoundary(b byte) bool {
	switch b {
	case '/', ' ', '\t', '\n', '\r', ':', '"', '\'', '?', '&':
		return true
	default:
		return false
	}
}
