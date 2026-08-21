package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"freebuff-reverse/internal/accounts"
	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/idgen"
)

var (
	ErrNoCapacity   = errors.New("session: no capacity")
	ErrNoAdapter    = errors.New("session: channel adapter not registered")
	ErrAccountPool  = errors.New("session: account pool unavailable")
	errRetryAcquire = errors.New("session: retry acquire")
)

type Config struct {
	WaitOnFull              bool
	ReapInterval            time.Duration
	Resolver                RuntimePolicyResolver
	StateRecorder           StateRecorder
	AccountMetadataResolver AccountMetadataResolver
	CreateLimits            CreateLimitConfig
}

type RuntimePolicy struct {
	MaxConcurrentPerSession int
	WaitOnFull              bool
}

type RuntimePolicyResolver interface {
	ResolveSessionPolicy(channelID, accountID string, fallback RuntimePolicy) RuntimePolicy
}

type AccountMetadataResolver interface {
	ResolveAccountMetadata(ctx context.Context, account channels.Account) (map[string]any, error)
}

type StateEvent struct {
	ChannelID      string
	AccountID      string
	LocalSessionID string
	SelectionKey   string
	State          channels.State
	CreatedAtUnix  int64
	ExpiresAtUnix  int64
}

type StateRecorder interface {
	RecordSessionState(ctx context.Context, event StateEvent) error
}

type schedulerFactsReader interface {
	SchedulerFacts(ctx context.Context, accountID string, now time.Time) (map[string]any, error)
}

type Manager struct {
	cfg Config

	mu    sync.RWMutex
	byKey map[string][]*Session
	byID  map[string]*Session

	registry   *channels.Registry
	pool       *accounts.Pool
	transport  channels.Transport
	createGate *createGate

	wakeMu sync.Mutex
	wakeCh chan struct{}
}

func NewManager(reg *channels.Registry, pool *accounts.Pool, tp channels.Transport, cfg Config) *Manager {
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 30 * time.Second
	}
	return &Manager{
		cfg:        cfg,
		byKey:      make(map[string][]*Session),
		byID:       make(map[string]*Session),
		registry:   reg,
		pool:       pool,
		transport:  tp,
		createGate: newCreateGate(cfg.CreateLimits),
		wakeCh:     make(chan struct{}),
	}
}

func (m *Manager) Acquire(ctx context.Context, channelID string, in *channels.InboundRequest) (*channels.Lease, error) {
	adapter, ok := m.registry.Get(channelID)
	if !ok {
		return nil, ErrNoAdapter
	}
	policy := adapter.SessionPolicy()
	if policy == nil {
		return nil, fmt.Errorf("session: channel %q has nil policy", channelID)
	}
	key := policy.SelectionKey(in)

	for {
		lease, ok, fallback, err := m.tryLeaseExistingWithCapacity(ctx, channelID, key)
		if err != nil || ok {
			return lease, err
		}

		if err := m.expireReclaimableIdleSessions(ctx, adapter, in); err != nil {
			return nil, err
		}

		decision, err := m.scheduleSession(ctx, adapter, policy, channelID, key, in, 0)
		if err != nil {
			return nil, err
		}
		switch scheduleAction(decision.Action) {
		case channels.SessionScheduleWait:
			lease, err := m.waitForScheduledSlot(ctx, fallback, decision)
			finishScheduleDecision(decision)
			if errors.Is(err, errRetryAcquire) {
				continue
			}
			return lease, err
		case channels.SessionScheduleReject:
			finishScheduleDecision(decision)
			return nil, channels.CapacityLimitedf(capacityReason(decision.Reason))
		}

		permit, err := m.acquireCreatePermit(ctx, adapter, channelID, key, in, fallback)
		if err != nil {
			finishScheduleDecision(decision)
			return nil, err
		}
		finishScheduleDecision(decision)

		lease, ok, fallback, err = m.tryLeaseExistingWithCapacity(ctx, channelID, key)
		if err != nil || ok {
			permit.Release()
			m.notifyCapacityChanged()
			return lease, err
		}
		if err := m.expireReclaimableIdleSessions(ctx, adapter, in); err != nil {
			permit.Release()
			m.notifyCapacityChanged()
			return nil, err
		}

		decision, err = m.scheduleSession(ctx, adapter, policy, channelID, key, in, permit.id)
		if err != nil {
			permit.Release()
			m.notifyCapacityChanged()
			return nil, err
		}
		switch scheduleAction(decision.Action) {
		case channels.SessionScheduleWait:
			permit.Release()
			m.notifyCapacityChanged()
			lease, err := m.waitForScheduledSlot(ctx, fallback, decision)
			finishScheduleDecision(decision)
			if errors.Is(err, errRetryAcquire) {
				continue
			}
			return lease, err
		case channels.SessionScheduleReject:
			permit.Release()
			m.notifyCapacityChanged()
			finishScheduleDecision(decision)
			return nil, channels.CapacityLimitedf(capacityReason(decision.Reason))
		}

		s, err := m.createAndRegister(ctx, adapter, policy, channelID, key, decision.PreferredAccountIDs)
		permit.Release()
		m.notifyCapacityChanged()
		finishScheduleDecision(decision)
		if err != nil {
			if fallback != nil && errors.Is(err, accounts.ErrNoEligibleAccount) {
				if fallback.waitOnFullEnabled() {
					return m.waitForExistingSlot(ctx, fallback)
				}
				return nil, ErrNoCapacity
			}
			return nil, err
		}

		if err := m.ensureQuotaAvailable(s); err != nil {
			return nil, err
		}
		if s.tryAcquire() {
			return m.makeLease(s), nil
		}
		if !s.waitOnFullEnabled() {
			continue
		}
	}
}

func (m *Manager) acquireCreatePermit(ctx context.Context, adapter channels.ChannelAdapter, channelID, key string, in *channels.InboundRequest, fallback *Session) (*createPermit, error) {
	if m.createGate == nil {
		return &createPermit{}, nil
	}
	labels := m.createLabels(adapter, channelID, key, in)
	return m.createGate.acquire(ctx, labels, m.shouldWaitForCapacity(fallback))
}

func (m *Manager) shouldWaitForCapacity(fallback *Session) bool {
	if fallback != nil {
		return fallback.waitOnFullEnabled()
	}
	return m.cfg.WaitOnFull
}

func (m *Manager) createLabels(adapter channels.ChannelAdapter, channelID, key string, in *channels.InboundRequest) createLabels {
	labels := createLabels{
		ChannelID:  channelID,
		Key:        key,
		Model:      key,
		QuotaGroup: channelID,
	}
	if classifier, ok := adapter.(channels.SessionCreateClassifier); ok {
		classified := classifier.ClassifySessionCreate(key, in)
		if classified.Model != "" {
			labels.Model = classified.Model
		}
		if classified.QuotaGroup != "" {
			labels.QuotaGroup = classified.QuotaGroup
		}
	}
	return labels
}

func (m *Manager) ensureQuotaAvailable(s *Session) error {
	if m.pool == nil {
		return ErrAccountPool
	}
	return m.pool.EnsureQuotaAvailable(s.AccountID)
}

func (m *Manager) tryLeaseExisting(channelID, key string) (*Session, bool) {
	for _, s := range m.candidateSessions(channelID, key) {
		return s, true
	}
	return nil, false
}

func (m *Manager) tryLeaseExistingWithCapacity(ctx context.Context, channelID, key string) (*channels.Lease, bool, *Session, error) {
	var fallback *Session
	for _, s := range m.candidateSessions(channelID, key) {
		if err := m.ensureQuotaAvailable(s); err != nil {
			return nil, false, nil, err
		}
		if s.tryAcquire() {
			return m.makeLease(s), true, nil, nil
		}
		if fallback == nil {
			fallback = s
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, false, nil, err
	}
	return nil, false, fallback, nil
}

func (m *Manager) candidateSessions(channelID, key string) []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.byKey[key]
	now := time.Now()
	out := make([]*Session, 0, len(list))
	for _, s := range list {
		if s.ChannelID != channelID {
			continue
		}
		if !s.isHealthy() {
			continue
		}
		if now.After(s.ExpiresAt) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func (m *Manager) takeSlot(ctx context.Context, s *Session) bool {
	if s.waitOnFullEnabled() {
		return s.acquireCtx(ctx) == nil
	}
	return s.tryAcquire()
}

func (m *Manager) waitForExistingSlot(ctx context.Context, s *Session) (*channels.Lease, error) {
	if s == nil {
		return nil, ErrNoCapacity
	}
	if err := m.ensureQuotaAvailable(s); err != nil {
		return nil, err
	}
	if m.takeSlot(ctx, s) {
		return m.makeLease(s), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNoCapacity
}

func (m *Manager) createAndRegister(
	ctx context.Context,
	adapter channels.ChannelAdapter,
	policy channels.SessionPolicy,
	channelID, key string,
	preferredAccountIDs []string,
) (*Session, error) {
	if m.pool == nil {
		return nil, ErrAccountPool
	}
	excluded := map[string]struct{}{}
	var lastUnavailable error
	for {
		acc, releaseSlot, err := m.pool.ReserveSlotExcludingPreferred(channelID, policy.MaxSessionsPerAccount(), excluded, preferredAccountIDs)
		if err != nil {
			if lastUnavailable != nil && errors.Is(err, accounts.ErrNoEligibleAccount) {
				return nil, fmt.Errorf("%w: %w", accounts.ErrNoEligibleAccount, lastUnavailable)
			}
			return nil, err
		}

		if m.cfg.AccountMetadataResolver != nil {
			metadata, err := m.cfg.AccountMetadataResolver.ResolveAccountMetadata(ctx, acc)
			if err != nil {
				releaseSlot()
				err = fmt.Errorf("session: account metadata: %w", err)
				if errors.Is(err, channels.ErrAccountUnavailable) {
					excluded[acc.ID] = struct{}{}
					lastUnavailable = err
					continue
				}
				return nil, err
			}
			acc.Metadata = metadata
		}

		if flow := adapter.AuthFlow(); flow != nil {
			if flow.CredentialExpired(&acc) {
				if err := flow.EnsureAuthenticated(ctx, &acc); err != nil {
					releaseSlot()
					return nil, fmt.Errorf("session: auth flow: %w", err)
				}
			}
		}

		state, err := policy.CreateSession(ctx, acc, key, m.transport)
		if err != nil {
			releaseSlot()
			err = fmt.Errorf("session: create: %w", err)
			if errors.Is(err, channels.ErrAccountUnavailable) {
				excluded[acc.ID] = struct{}{}
				lastUnavailable = err
				continue
			}
			return nil, err
		}

		id := idgen.New()
		runtimePolicy := m.resolveRuntimePolicy(channelID, acc.ID, policy)
		ttl, err := m.effectiveSessionTTL(policy, state)
		if err != nil {
			releaseSlot()
			return nil, err
		}
		s := newSession(id, channelID, acc.ID, key, state, ttl, runtimePolicy.MaxConcurrentPerSession, runtimePolicy.WaitOnFull, releaseSlot)
		if err := m.recordSessionState(ctx, s); err != nil {
			releaseSlot()
			return nil, err
		}

		m.mu.Lock()
		m.byKey[key] = append(m.byKey[key], s)
		m.byID[id] = s
		m.mu.Unlock()
		m.notifyCapacityChanged()
		return s, nil
	}
}

func (m *Manager) RegisterRestoredSession(ctx context.Context, channelID, accountID, key string, state channels.State) (bool, error) {
	adapter, ok := m.registry.Get(channelID)
	if !ok {
		return false, ErrNoAdapter
	}
	policy := adapter.SessionPolicy()
	if policy == nil {
		return false, fmt.Errorf("session: channel %q has nil policy", channelID)
	}
	if m.pool == nil {
		return false, ErrAccountPool
	}
	if accountID == "" || key == "" {
		return false, fmt.Errorf("session: restore requires account id and selection key")
	}

	m.mu.RLock()
	if m.hasHealthySessionLocked(channelID, accountID, key, time.Now()) {
		m.mu.RUnlock()
		return false, nil
	}
	m.mu.RUnlock()

	_, releaseSlot, err := m.pool.ReserveAccountSlot(channelID, accountID, policy.MaxSessionsPerAccount())
	if err != nil {
		return false, err
	}
	runtimePolicy := m.resolveRuntimePolicy(channelID, accountID, policy)
	ttl, err := m.effectiveSessionTTL(policy, state)
	if err != nil {
		releaseSlot()
		return false, err
	}

	m.mu.Lock()
	if m.hasHealthySessionLocked(channelID, accountID, key, time.Now()) {
		m.mu.Unlock()
		releaseSlot()
		return false, nil
	}
	id := idgen.New()
	s := newSession(id, channelID, accountID, key, cloneState(state), ttl, runtimePolicy.MaxConcurrentPerSession, runtimePolicy.WaitOnFull, releaseSlot)
	m.byKey[key] = append(m.byKey[key], s)
	m.byID[id] = s
	m.mu.Unlock()
	m.notifyCapacityChanged()

	if err := m.recordSessionState(ctx, s); err != nil {
		m.expire(s)
		return false, err
	}
	return true, nil
}

func (m *Manager) hasHealthySessionLocked(channelID, accountID, key string, now time.Time) bool {
	for _, s := range m.byKey[key] {
		if s.ChannelID != channelID || s.AccountID != accountID {
			continue
		}
		if !s.isHealthy() || now.After(s.ExpiresAt) {
			continue
		}
		return true
	}
	return false
}

func (m *Manager) scheduleSession(
	ctx context.Context,
	adapter channels.ChannelAdapter,
	policy channels.SessionPolicy,
	channelID, key string,
	in *channels.InboundRequest,
	pendingCreateBeforeID uint64,
) (channels.SessionScheduleDecision, error) {
	scheduler, ok := adapter.(channels.SessionScheduler)
	if !ok {
		return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate}, nil
	}
	if m.pool == nil {
		return channels.SessionScheduleDecision{}, ErrAccountPool
	}
	now := time.Now()
	accounts, err := m.pool.SlotCandidates(channelID, policy.MaxSessionsPerAccount(), nil)
	if err != nil {
		return channels.SessionScheduleDecision{}, err
	}
	if err := m.attachSchedulerFacts(ctx, accounts, now); err != nil {
		return channels.SessionScheduleDecision{}, err
	}
	decision, err := scheduler.ScheduleSession(ctx, channels.SessionScheduleRequest{
		ChannelID:      channelID,
		SelectionKey:   key,
		Inbound:        in,
		Accounts:       accounts,
		Sessions:       m.SchedulingCandidates(channelID),
		PendingCreates: m.pendingCreateCandidatesBefore(channelID, pendingCreateBeforeID),
		Now:            now,
	})
	if err != nil {
		return channels.SessionScheduleDecision{}, err
	}
	if decision.Action == "" {
		decision.Action = channels.SessionScheduleCreate
	}
	return decision, nil
}

func (m *Manager) attachSchedulerFacts(ctx context.Context, accounts []channels.AccountCandidate, now time.Time) error {
	reader, ok := m.cfg.StateRecorder.(schedulerFactsReader)
	if !ok || reader == nil {
		return nil
	}
	for i := range accounts {
		facts, err := reader.SchedulerFacts(ctx, accounts[i].Account.ID, now)
		if err != nil {
			return err
		}
		if len(facts) > 0 {
			accounts[i].ProviderFacts = facts
		}
	}
	return nil
}

func scheduleAction(action channels.SessionScheduleAction) channels.SessionScheduleAction {
	if action == "" {
		return channels.SessionScheduleCreate
	}
	return action
}

func finishScheduleDecision(decision channels.SessionScheduleDecision) {
	if decision.Finish != nil {
		decision.Finish()
	}
}

func (m *Manager) waitForScheduledSlot(ctx context.Context, s *Session, decision channels.SessionScheduleDecision) (*channels.Lease, error) {
	waitCtx := ctx
	cancel := func() {}
	if decision.WaitTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, decision.WaitTimeout)
	}
	defer cancel()

	if s != nil {
		if err := m.ensureQuotaAvailable(s); err != nil {
			return nil, err
		}
		if err := s.acquireCtx(waitCtx); err == nil {
			return m.makeLease(s), nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, channels.CapacityLimitedf(capacityReason(decision.Reason))
	}

	if decision.WaitTimeout > 0 {
		timer := time.NewTimer(decision.WaitTimeout)
		defer timer.Stop()
		changed := m.capacityChanged()
		select {
		case <-changed:
			return nil, errRetryAcquire
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, channels.CapacityLimitedf(capacityReason(decision.Reason))
}

func (m *Manager) capacityChanged() <-chan struct{} {
	m.wakeMu.Lock()
	defer m.wakeMu.Unlock()
	return m.wakeCh
}

func (m *Manager) notifyCapacityChanged() {
	if m == nil {
		return
	}
	m.wakeMu.Lock()
	close(m.wakeCh)
	m.wakeCh = make(chan struct{})
	m.wakeMu.Unlock()
}

func capacityReason(reason string) string {
	if reason == "" {
		return "capacity_limited"
	}
	return reason
}

func (m *Manager) expireReclaimableIdleSessions(ctx context.Context, adapter channels.ChannelAdapter, in *channels.InboundRequest) error {
	reclaimer, ok := adapter.(channels.SessionReclaimPolicy)
	if !ok {
		return nil
	}
	executor, _ := adapter.(channels.SessionReclaimExecutor)
	sessions := m.sessionsBy(func(s *Session) bool {
		return s.ChannelID == adapter.ID()
	})
	now := time.Now()
	for _, s := range sessions {
		if !s.isHealthy() || now.After(s.ExpiresAt) {
			continue
		}
		if s.inFlightCount() != 0 {
			continue
		}
		if reclaimer.CanReclaimSessionForRequest(s.State, in) {
			if !s.beginRefresh() {
				continue
			}
			if s.inFlightCount() != 0 {
				s.restoreHealthyFromRefreshing()
				continue
			}
			state, ok, err := m.reclaimSessionState(ctx, executor, reclaimer, s, in)
			if err != nil {
				s.restoreHealthyFromRefreshing()
				if errors.Is(err, channels.ErrAccountUnavailable) {
					continue
				}
				return err
			}
			if !ok {
				s.restoreHealthyFromRefreshing()
				continue
			}
			if err := m.recordSessionStateWithState(ctx, s, state); err != nil {
				m.expire(s)
				return err
			}
			m.expire(s)
		}
	}
	return nil
}

func (m *Manager) reclaimSessionState(
	ctx context.Context,
	executor channels.SessionReclaimExecutor,
	fallback channels.SessionReclaimPolicy,
	s *Session,
	in *channels.InboundRequest,
) (channels.State, bool, error) {
	if executor == nil {
		state, ok := fallback.ReclaimedSessionState(s.State)
		return state, ok, nil
	}
	acc, err := m.reclaimAccount(ctx, s.AccountID)
	if err != nil {
		return nil, false, err
	}
	return executor.ReclaimSessionForRequest(ctx, acc, s.State, in, m.transport)
}

func (m *Manager) reclaimAccount(ctx context.Context, accountID string) (channels.Account, error) {
	if m.pool == nil || m.pool.Repo() == nil {
		return channels.Account{}, ErrAccountPool
	}
	rec, err := m.pool.Repo().Get(accountID)
	if err != nil {
		return channels.Account{}, err
	}
	acc := rec.ToChannel()
	if m.cfg.AccountMetadataResolver == nil {
		return acc, nil
	}
	metadata, err := m.cfg.AccountMetadataResolver.ResolveAccountMetadata(ctx, acc)
	if err != nil {
		return channels.Account{}, fmt.Errorf("session: account metadata: %w", err)
	}
	acc.Metadata = metadata
	return acc, nil
}

func (m *Manager) recordSessionState(ctx context.Context, s *Session) error {
	if s == nil {
		return nil
	}
	return m.recordSessionStateWithState(ctx, s, s.State)
}

func (m *Manager) recordSessionStateWithState(ctx context.Context, s *Session, state channels.State) error {
	if m.cfg.StateRecorder == nil || s == nil {
		return nil
	}
	return m.cfg.StateRecorder.RecordSessionState(ctx, StateEvent{
		ChannelID:      s.ChannelID,
		AccountID:      s.AccountID,
		LocalSessionID: s.ID,
		SelectionKey:   s.Key,
		State:          state,
		CreatedAtUnix:  s.CreatedAt.Unix(),
		ExpiresAtUnix:  s.ExpiresAt.Unix(),
	})
}

func (m *Manager) makeLease(s *Session) *channels.Lease {
	return channels.NewLease(s.ID, s.AccountID, s.ChannelID, s.Key, s.State, func(v channels.Verdict) {
		m.handleRelease(s, v)
	})
}

func (m *Manager) handleRelease(s *Session, v channels.Verdict) {
	s.releaseSem()
	switch v {
	case channels.VerdictHealthy:
		m.notifyCapacityChanged()
		return
	case channels.VerdictRefresh, channels.VerdictExpire:
		m.expire(s)
	}
}

func (m *Manager) expire(s *Session) {
	if !s.markExpired() {
		return
	}
	m.mu.Lock()
	list := m.byKey[s.Key]
	kept := list[:0]
	for _, e := range list {
		if e.ID != s.ID {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		delete(m.byKey, s.Key)
	} else {
		m.byKey[s.Key] = kept
	}
	delete(m.byID, s.ID)
	m.mu.Unlock()
	m.notifyCapacityChanged()
	s.fireOnExpire()
	m.finalizeExpiredSession(s)
}

func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.reap()
		}
	}
}

func (m *Manager) reap() {
	now := time.Now()
	m.mu.RLock()
	stale := make([]*Session, 0)
	for _, list := range m.byKey {
		for _, s := range list {
			if Health(s.health.Load()) == HealthExpired || now.After(s.ExpiresAt) {
				stale = append(stale, s)
			}
		}
	}
	m.mu.RUnlock()
	for _, s := range stale {
		if s.inFlightCount() != 0 {
			continue
		}
		m.expire(s)
	}
}

func (m *Manager) finalizeExpiredSession(s *Session) {
	if m == nil || s == nil || m.registry == nil {
		return
	}
	adapter, ok := m.registry.Get(s.ChannelID)
	if !ok {
		return
	}
	finalizer, ok := adapter.(channels.SessionFinalizer)
	if !ok {
		return
	}
	finalizer.FinalizeSession(context.Background(), channels.SessionFinalizeEvent{
		ChannelID:      s.ChannelID,
		AccountID:      s.AccountID,
		LocalSessionID: s.ID,
		SelectionKey:   s.Key,
		State:          cloneState(s.State),
		ExpiresAtUnix:  s.ExpiresAt.Unix(),
		Reason:         "expired",
	})
}

func (m *Manager) Snapshot() []Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Snapshot, 0, len(m.byID))
	for _, s := range m.byID {
		out = append(out, s.snapshot())
	}
	return out
}

func (m *Manager) SchedulingCandidates(channelID string) []channels.SessionCandidate {
	if m == nil {
		return nil
	}
	sessions := m.sessionsBy(func(s *Session) bool {
		return channelID == "" || s.ChannelID == channelID
	})
	out := make([]channels.SessionCandidate, 0, len(sessions))
	for _, s := range sessions {
		inFlight, maxConcurrency := s.limitSnapshot()
		out = append(out, channels.SessionCandidate{
			ID:             s.ID,
			ChannelID:      s.ChannelID,
			AccountID:      s.AccountID,
			Key:            s.Key,
			State:          s.State,
			CreatedAtUnix:  s.CreatedAt.Unix(),
			ExpiresAtUnix:  s.ExpiresAt.Unix(),
			LastUsedAtUnix: s.lastUsedAtNS.Load() / int64(time.Second),
			InFlight:       inFlight,
			MaxConcurrency: maxConcurrency,
			Healthy:        s.isHealthy() && time.Now().Before(s.ExpiresAt),
			WaitOnFull:     s.waitOnFullEnabled(),
		})
	}
	return out
}

func (m *Manager) PendingCreateCandidates(channelID string) []channels.SessionCreateCandidate {
	return m.pendingCreateCandidatesBefore(channelID, 0)
}

func (m *Manager) pendingCreateCandidatesBefore(channelID string, beforeID uint64) []channels.SessionCreateCandidate {
	if m == nil || m.createGate == nil {
		return nil
	}
	return m.createGate.pendingCandidatesBefore(channelID, beforeID)
}

func (m *Manager) CountByChannel() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int)
	for _, s := range m.byID {
		out[s.ChannelID]++
	}
	return out
}

func (m *Manager) RefreshChannelPolicy(channelID string) {
	if m == nil || channelID == "" {
		return
	}
	sessions := m.sessionsBy(func(s *Session) bool { return s.ChannelID == channelID })
	m.refreshPolicies(sessions)
}

func (m *Manager) RefreshAccountPolicy(accountID string) {
	if m == nil || accountID == "" {
		return
	}
	sessions := m.sessionsBy(func(s *Session) bool { return s.AccountID == accountID })
	m.refreshPolicies(sessions)
}

func (m *Manager) DefaultWaitOnFull() bool {
	if m == nil {
		return false
	}
	return m.cfg.WaitOnFull
}

func (m *Manager) sessionsBy(match func(*Session) bool) []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Session, 0)
	for _, s := range m.byID {
		if match(s) {
			out = append(out, s)
		}
	}
	return out
}

func (m *Manager) refreshPolicies(sessions []*Session) {
	for _, s := range sessions {
		adapter, ok := m.registry.Get(s.ChannelID)
		if !ok {
			continue
		}
		policy := adapter.SessionPolicy()
		if policy == nil {
			continue
		}
		s.applyRuntimePolicy(m.resolveRuntimePolicy(s.ChannelID, s.AccountID, policy))
	}
}

func (m *Manager) resolveRuntimePolicy(channelID, accountID string, policy channels.SessionPolicy) RuntimePolicy {
	fallback := RuntimePolicy{
		MaxConcurrentPerSession: policy.MaxConcurrentPerSession(),
		WaitOnFull:              m.cfg.WaitOnFull,
	}
	fallback = normalizeRuntimePolicy(fallback)
	if m.cfg.Resolver == nil {
		return fallback
	}
	return normalizeRuntimePolicy(m.cfg.Resolver.ResolveSessionPolicy(channelID, accountID, fallback))
}

func (m *Manager) effectiveSessionTTL(policy channels.SessionPolicy, state channels.State) (time.Duration, error) {
	ttl := policy.SessionTTL()
	if expiryPolicy, ok := policy.(channels.SessionExpiryPolicy); ok {
		if expiresAt, ok := expiryPolicy.SessionExpiresAt(state); ok {
			until := time.Until(expiresAt)
			if until <= 0 {
				return 0, fmt.Errorf("session: provider expiry is in the past")
			}
			if ttl <= 0 || until < ttl {
				ttl = until
			}
		}
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("session: non-positive ttl")
	}
	return ttl, nil
}

func normalizeRuntimePolicy(p RuntimePolicy) RuntimePolicy {
	if p.MaxConcurrentPerSession < 1 {
		p.MaxConcurrentPerSession = 1
	}
	return p
}

func cloneState(state channels.State) channels.State {
	if len(state) == 0 {
		return channels.State{}
	}
	out := make(channels.State, len(state))
	for key, value := range state {
		out[key] = value
	}
	return out
}
