package session

import (
	"context"
	"sync"
	"time"

	"freebuff-reverse/internal/channels"
)

const (
	defaultMaxParallelCreatesGlobal   = 128
	defaultMaxParallelCreatesPerKey   = 32
	defaultMaxParallelCreatesPerModel = 32
	defaultMaxParallelCreatesPerGroup = 96
)

type CreateLimitConfig struct {
	MaxParallelGlobal   int
	MaxParallelPerKey   int
	MaxParallelPerModel int
	MaxParallelPerGroup int
}

type createLabels struct {
	ChannelID  string
	Key        string
	Model      string
	QuotaGroup string
}

type createPermit struct {
	gate *createGate
	id   uint64
	once sync.Once
}

func (p *createPermit) Release() {
	if p == nil || p.gate == nil {
		return
	}
	p.once.Do(func() {
		p.gate.release(p.id)
	})
}

type pendingCreate struct {
	id        uint64
	labels    createLabels
	startedAt time.Time
}

type createGate struct {
	mu      sync.Mutex
	changed chan struct{}
	limits  CreateLimitConfig

	nextID  uint64
	global  int
	byKey   map[string]int
	byModel map[string]int
	byGroup map[string]int
	pending map[uint64]pendingCreate
}

func newCreateGate(cfg CreateLimitConfig) *createGate {
	return &createGate{
		changed: make(chan struct{}),
		limits:  normalizeCreateLimitConfig(cfg),
		byKey:   make(map[string]int),
		byModel: make(map[string]int),
		byGroup: make(map[string]int),
		pending: make(map[uint64]pendingCreate),
	}
}

func normalizeCreateLimitConfig(cfg CreateLimitConfig) CreateLimitConfig {
	if cfg.MaxParallelGlobal <= 0 {
		cfg.MaxParallelGlobal = defaultMaxParallelCreatesGlobal
	}
	if cfg.MaxParallelPerKey <= 0 {
		cfg.MaxParallelPerKey = defaultMaxParallelCreatesPerKey
	}
	if cfg.MaxParallelPerModel <= 0 {
		cfg.MaxParallelPerModel = defaultMaxParallelCreatesPerModel
	}
	if cfg.MaxParallelPerGroup <= 0 {
		cfg.MaxParallelPerGroup = defaultMaxParallelCreatesPerGroup
	}
	if cfg.MaxParallelPerKey > cfg.MaxParallelGlobal {
		cfg.MaxParallelPerKey = cfg.MaxParallelGlobal
	}
	if cfg.MaxParallelPerModel > cfg.MaxParallelGlobal {
		cfg.MaxParallelPerModel = cfg.MaxParallelGlobal
	}
	if cfg.MaxParallelPerGroup > cfg.MaxParallelGlobal {
		cfg.MaxParallelPerGroup = cfg.MaxParallelGlobal
	}
	return cfg
}

func (g *createGate) acquire(ctx context.Context, labels createLabels, wait bool) (*createPermit, error) {
	labels = normalizeCreateLabels(labels)
	for {
		g.mu.Lock()
		if g.canAcquireLocked(labels) {
			g.nextID++
			id := g.nextID
			g.global++
			g.byKey[labels.Key]++
			g.byModel[labels.Model]++
			g.byGroup[labels.QuotaGroup]++
			g.pending[id] = pendingCreate{id: id, labels: labels, startedAt: time.Now()}
			g.mu.Unlock()
			return &createPermit{gate: g, id: id}, nil
		}
		changed := g.changed
		g.mu.Unlock()

		if !wait {
			return nil, ErrNoCapacity
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func normalizeCreateLabels(labels createLabels) createLabels {
	if labels.Key == "" {
		labels.Key = labels.ChannelID
	}
	if labels.Model == "" {
		labels.Model = labels.Key
	}
	if labels.QuotaGroup == "" {
		labels.QuotaGroup = labels.ChannelID
	}
	return labels
}

func (g *createGate) canAcquireLocked(labels createLabels) bool {
	switch {
	case g.global >= g.limits.MaxParallelGlobal:
		return false
	case g.byKey[labels.Key] >= g.limits.MaxParallelPerKey:
		return false
	case g.byModel[labels.Model] >= g.limits.MaxParallelPerModel:
		return false
	case g.byGroup[labels.QuotaGroup] >= g.limits.MaxParallelPerGroup:
		return false
	default:
		return true
	}
}

func (g *createGate) release(id uint64) {
	g.mu.Lock()
	p, ok := g.pending[id]
	if !ok {
		g.mu.Unlock()
		return
	}
	delete(g.pending, id)
	g.global--
	decrementCount(g.byKey, p.labels.Key)
	decrementCount(g.byModel, p.labels.Model)
	decrementCount(g.byGroup, p.labels.QuotaGroup)
	g.notifyLocked()
	g.mu.Unlock()
}

func decrementCount(m map[string]int, key string) {
	if m[key] <= 1 {
		delete(m, key)
		return
	}
	m[key]--
}

func (g *createGate) notifyLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

func (g *createGate) pendingCandidates(channelID string) []channels.SessionCreateCandidate {
	return g.pendingCandidatesBefore(channelID, 0)
}

func (g *createGate) pendingCandidatesBefore(channelID string, beforeID uint64) []channels.SessionCreateCandidate {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]channels.SessionCreateCandidate, 0, len(g.pending))
	for _, p := range g.pending {
		if channelID != "" && p.labels.ChannelID != channelID {
			continue
		}
		if beforeID != 0 && p.id >= beforeID {
			continue
		}
		out = append(out, channels.SessionCreateCandidate{
			ChannelID:     p.labels.ChannelID,
			Key:           p.labels.Key,
			Model:         p.labels.Model,
			QuotaGroup:    p.labels.QuotaGroup,
			StartedAtUnix: p.startedAt.Unix(),
		})
	}
	return out
}
