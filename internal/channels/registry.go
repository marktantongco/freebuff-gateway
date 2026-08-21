package channels

import (
	"fmt"
	"sort"
	"sync"
)

type Builtin func(*Registry) error

var (
	builtinMu sync.Mutex
	builtins  []Builtin
)

type Registry struct {
	mu       sync.RWMutex
	adapters map[string]ChannelAdapter
}

func RegisterBuiltin(fn Builtin) {
	if fn == nil {
		panic("channels: register nil builtin")
	}
	builtinMu.Lock()
	builtins = append(builtins, fn)
	builtinMu.Unlock()
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ChannelAdapter)}
}

func (r *Registry) RegisterBuiltins() error {
	builtinMu.Lock()
	fns := append([]Builtin(nil), builtins...)
	builtinMu.Unlock()
	for _, fn := range fns {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) Register(a ChannelAdapter) error {
	if a == nil {
		return fmt.Errorf("channels: register nil adapter")
	}
	id := a.ID()
	if id == "" {
		return fmt.Errorf("channels: adapter has empty id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.adapters[id]; ok {
		return fmt.Errorf("channels: adapter %q already registered", id)
	}
	r.adapters[id] = a
	return nil
}

func (r *Registry) MustRegister(a ChannelAdapter) {
	if err := r.Register(a); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(id string) (ChannelAdapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[id]
	return a, ok
}

func (r *Registry) List() []ChannelAdapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ChannelAdapter, 0, len(r.adapters))
	for _, a := range r.adapters {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}
