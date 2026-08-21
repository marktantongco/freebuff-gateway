package phasetiming

import (
	"context"
	"sync"
	"time"
)

type contextKey struct{}

type Trace struct {
	start time.Time

	mu     sync.Mutex
	values map[string]any
}

func New(start time.Time) *Trace {
	if start.IsZero() {
		start = time.Now()
	}
	return &Trace{
		start:  start,
		values: make(map[string]any),
	}
}

func ContextWithTrace(ctx context.Context, trace *Trace) context.Context {
	if trace == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, trace)
}

func FromContext(ctx context.Context) *Trace {
	if ctx == nil {
		return nil
	}
	trace, _ := ctx.Value(contextKey{}).(*Trace)
	return trace
}

func (t *Trace) Duration(name string, d time.Duration) {
	if t == nil || name == "" {
		return
	}
	if d < 0 {
		d = 0
	}
	t.set(name, d.Milliseconds())
}

func (t *Trace) Since(name string, started time.Time) {
	if started.IsZero() {
		return
	}
	t.Duration(name, time.Since(started))
}

func (t *Trace) MarkFirst(name string) bool {
	if t == nil || name == "" {
		return false
	}
	ms := time.Since(t.start)
	if ms < 0 {
		ms = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.values[name]; exists {
		return false
	}
	t.values[name] = ms.Milliseconds()
	return true
}

func (t *Trace) Bool(name string, value bool) {
	if t == nil || name == "" {
		return
	}
	t.set(name, value)
}

func (t *Trace) String(name, value string) {
	if t == nil || name == "" {
		return
	}
	t.set(name, value)
}

func (t *Trace) Snapshot() map[string]any {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.values) == 0 {
		return nil
	}
	out := make(map[string]any, len(t.values))
	for key, value := range t.values {
		out[key] = value
	}
	return out
}

func (t *Trace) set(name string, value any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.values[name] = value
}
