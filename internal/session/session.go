package session

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

type Health int32

const (
	HealthHealthy Health = iota
	HealthRefreshing
	HealthExpired
)

type Session struct {
	ID        string
	ChannelID string
	AccountID string
	Key       string
	State     channels.State
	CreatedAt time.Time
	ExpiresAt time.Time

	lastUsedAtNS atomic.Int64
	health       atomic.Int32
	waitOnFull   atomic.Bool

	limitMu        sync.Mutex
	limitChanged   chan struct{}
	inFlight       int
	maxConcurrency int

	onExpireOnce sync.Once
	onExpire     func()
}

func newSession(id, channelID, accountID, key string, state channels.State, ttl time.Duration, maxConc int, waitOnFull bool, onExpire func()) *Session {
	if maxConc < 1 {
		maxConc = 1
	}
	now := time.Now()
	s := &Session{
		ID:             id,
		ChannelID:      channelID,
		AccountID:      accountID,
		Key:            key,
		State:          state,
		CreatedAt:      now,
		ExpiresAt:      now.Add(ttl),
		limitChanged:   make(chan struct{}),
		maxConcurrency: maxConc,
		onExpire:       onExpire,
	}
	s.lastUsedAtNS.Store(now.UnixNano())
	s.waitOnFull.Store(waitOnFull)
	return s
}

func (s *Session) isHealthy() bool {
	return Health(s.health.Load()) == HealthHealthy
}

func (s *Session) beginRefresh() bool {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	if Health(s.health.Load()) != HealthHealthy || s.inFlight != 0 {
		return false
	}
	s.health.Store(int32(HealthRefreshing))
	s.notifyLimitChangedLocked()
	return true
}

func (s *Session) restoreHealthyFromRefreshing() bool {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	if Health(s.health.Load()) != HealthRefreshing {
		return false
	}
	s.health.Store(int32(HealthHealthy))
	s.notifyLimitChangedLocked()
	return true
}

func (s *Session) tryAcquire() bool {
	s.limitMu.Lock()
	if Health(s.health.Load()) != HealthHealthy {
		s.limitMu.Unlock()
		return false
	}
	if s.inFlight >= s.maxConcurrency {
		s.limitMu.Unlock()
		return false
	}
	s.inFlight++
	s.limitMu.Unlock()
	s.lastUsedAtNS.Store(time.Now().UnixNano())
	return true
}

func (s *Session) acquireCtx(ctx context.Context) error {
	for {
		s.limitMu.Lock()
		if Health(s.health.Load()) != HealthHealthy {
			s.limitMu.Unlock()
			return ErrNoCapacity
		}
		if s.inFlight < s.maxConcurrency {
			s.inFlight++
			s.limitMu.Unlock()
			s.lastUsedAtNS.Store(time.Now().UnixNano())
			return nil
		}
		changed := s.limitChanged
		s.limitMu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Session) releaseSem() {
	s.limitMu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
		s.notifyLimitChangedLocked()
	}
	s.limitMu.Unlock()
}

func (s *Session) setMaxConcurrency(maxConc int) {
	if maxConc < 1 {
		maxConc = 1
	}
	s.limitMu.Lock()
	if s.maxConcurrency != maxConc {
		s.maxConcurrency = maxConc
		s.notifyLimitChangedLocked()
	}
	s.limitMu.Unlock()
}

func (s *Session) applyRuntimePolicy(p RuntimePolicy) {
	s.setMaxConcurrency(p.MaxConcurrentPerSession)
	s.waitOnFull.Store(p.WaitOnFull)
}

func (s *Session) waitOnFullEnabled() bool {
	return s.waitOnFull.Load()
}

func (s *Session) limitSnapshot() (int, int) {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	return s.inFlight, s.maxConcurrency
}

func (s *Session) inFlightCount() int {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	return s.inFlight
}

func (s *Session) notifyLimitChangedLocked() {
	close(s.limitChanged)
	s.limitChanged = make(chan struct{})
}

func (s *Session) markExpired() bool {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	if Health(s.health.Load()) == HealthExpired {
		return false
	}
	s.health.Store(int32(HealthExpired))
	s.notifyLimitChangedLocked()
	return true
}

func (s *Session) fireOnExpire() {
	s.onExpireOnce.Do(func() {
		if s.onExpire != nil {
			s.onExpire()
		}
	})
}

type Snapshot struct {
	ID             string         `json:"id"`
	ChannelID      string         `json:"channel_id"`
	AccountID      string         `json:"account_id"`
	Key            string         `json:"selection_key"`
	CreatedAtUnix  int64          `json:"created_at_unix"`
	ExpiresAtUnix  int64          `json:"expires_at_unix"`
	LastUsedAtUnix int64          `json:"last_used_at_unix"`
	InFlight       int            `json:"in_flight"`
	MaxConcurrency int            `json:"max_concurrency"`
	Health         string         `json:"health"`
	Details        map[string]any `json:"details,omitempty"`
}

func (s *Session) snapshot() Snapshot {
	h := Health(s.health.Load())
	healthStr := "healthy"
	switch h {
	case HealthRefreshing:
		healthStr = "refreshing"
	case HealthExpired:
		healthStr = "expired"
	}
	inFlight, maxConcurrency := s.limitSnapshot()
	return Snapshot{
		ID:             s.ID,
		ChannelID:      s.ChannelID,
		AccountID:      s.AccountID,
		Key:            s.Key,
		CreatedAtUnix:  s.CreatedAt.Unix(),
		ExpiresAtUnix:  s.ExpiresAt.Unix(),
		LastUsedAtUnix: s.lastUsedAtNS.Load() / int64(time.Second),
		InFlight:       inFlight,
		MaxConcurrency: maxConcurrency,
		Health:         healthStr,
		Details:        publicDetails(s.State),
	}
}

func publicDetails(state channels.State) map[string]any {
	if len(state) == 0 {
		return nil
	}
	out := make(map[string]any)
	for k, v := range state {
		if isSensitiveDetailKey(k) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isSensitiveDetailKey(key string) bool {
	switch key {
	case "":
		return true
	}
	for _, part := range []string{"credential", "token", "secret", "password", "run_id", "raw_session_json"} {
		if strings.Contains(strings.ToLower(key), part) {
			return true
		}
	}
	return false
}
