package freebuff

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/freebufffacts"
)

const (
	stateSchedulerPool      = "freebuff_scheduler_pool"
	stateSchedulerReason    = "freebuff_selection_reason"
	statePremiumTouched     = "freebuff_premium_window_touched"
	statePremiumRemaining   = "freebuff_premium_quota_remaining"
	statePremiumResetAtUnix = "freebuff_premium_quota_reset_at_unix"
	stateBorrowedUnlimited  = "freebuff_borrowed_unlimited"
)

type SchedulerConfig struct {
	PremiumCoreRatio            float64       `json:"premium_core_ratio"`
	PremiumMaxRatio             float64       `json:"premium_max_ratio"`
	UnlimitedReserveRatio       float64       `json:"unlimited_reserve_ratio"`
	UnlimitedMinReserveAccounts int           `json:"unlimited_min_reserve_accounts"`
	PremiumBurstQueueThreshold  int           `json:"premium_burst_queue_threshold"`
	PremiumQueueTimeout         time.Duration `json:"premium_queue_timeout"`
	ModelMaxAccountRatio        float64       `json:"model_max_account_ratio"`
	ModelBurstAccountRatio      float64       `json:"model_burst_account_ratio"`
}

type premiumScheduler struct {
	mu                sync.Mutex
	cfg               SchedulerConfig
	history           map[string]premiumAccountHistory
	queueDepthByModel map[string]int
}

type premiumAccountHistory struct {
	CurrentWindowTouched bool
	EverPremiumTouched   bool
	WindowResetUnix      int64
	Remaining            int
	RemainingKnown       bool
	Depleted             bool
	BorrowedUnlimited    bool
	LastReason           string
	LastPool             string
	LastUpdatedUnix      int64
}

type schedulerLimits struct {
	TotalActiveAccounts   int      `json:"total_active_accounts"`
	CorePremiumAccounts   int      `json:"core_premium_accounts"`
	MaxPremiumAccounts    int      `json:"max_premium_accounts"`
	UnlimitedReserve      int      `json:"unlimited_reserve_accounts"`
	Warnings              []string `json:"warnings"`
	PremiumActiveAccounts int      `json:"premium_active_accounts"`
}

type schedulerConfigView struct {
	PremiumCoreRatio            float64 `json:"premium_core_ratio"`
	PremiumMaxRatio             float64 `json:"premium_max_ratio"`
	UnlimitedReserveRatio       float64 `json:"unlimited_reserve_ratio"`
	UnlimitedMinReserveAccounts int     `json:"unlimited_min_reserve_accounts"`
	PremiumBurstQueueThreshold  int     `json:"premium_burst_queue_threshold"`
	PremiumQueueTimeoutMS       int64   `json:"premium_queue_timeout_ms"`
	ModelMaxAccountRatio        float64 `json:"model_max_account_ratio"`
	ModelBurstAccountRatio      float64 `json:"model_burst_account_ratio"`
}

type schedulerQueueView struct {
	PremiumDepth          int            `json:"premium_depth"`
	DepthByModel          map[string]int `json:"depth_by_model,omitempty"`
	PendingCreatesByModel map[string]int `json:"pending_creates_by_model,omitempty"`
	PendingCreatesByGroup map[string]int `json:"pending_creates_by_group,omitempty"`
	TimeoutMS             int64          `json:"timeout_ms"`
}

type schedulerPendingCreateView struct {
	ChannelID     string `json:"channel_id,omitempty"`
	SelectionKey  string `json:"selection_key"`
	Model         string `json:"model,omitempty"`
	QuotaGroup    string `json:"quota_group,omitempty"`
	StartedAtUnix int64  `json:"started_at_unix"`
}

type schedulerModelLimitView struct {
	Model              string `json:"model"`
	QuotaGroup         string `json:"quota_group"`
	ActiveSessions     int    `json:"active_sessions"`
	PendingCreates     int    `json:"pending_creates"`
	ActiveTotal        int    `json:"active_total"`
	SoftCap            int    `json:"soft_cap"`
	BurstCap           int    `json:"burst_cap"`
	State              string `json:"state"`
	OtherModelPressure bool   `json:"other_model_pressure,omitempty"`
}

type schedulerAccountView struct {
	AccountID            string   `json:"account_id"`
	Name                 string   `json:"name,omitempty"`
	Pool                 string   `json:"pool"`
	State                string   `json:"state"`
	Eligible             bool     `json:"eligible"`
	BlockedReason        string   `json:"blocked_reason,omitempty"`
	SessionCount         int      `json:"session_count"`
	LastDecisionReason   string   `json:"last_decision_reason,omitempty"`
	PremiumWindowTouched bool     `json:"premium_window_touched"`
	EverPremiumTouched   bool     `json:"ever_premium_touched"`
	PremiumDepleted      bool     `json:"premium_depleted"`
	PremiumRemaining     *int     `json:"premium_remaining,omitempty"`
	PremiumResetAtUnix   int64    `json:"premium_reset_at_unix,omitempty"`
	BorrowedUnlimited    bool     `json:"borrowed_unlimited"`
	Score                *float64 `json:"score,omitempty"`
	ScoreBand            string   `json:"score_band,omitempty"`
	ScoreStatus          string   `json:"score_status,omitempty"`
	ScoreReasons         []string `json:"score_reasons"`
}

type schedulerSessionView struct {
	ID                 string   `json:"id"`
	AccountID          string   `json:"account_id"`
	AccountDisplayName string   `json:"account_display_name,omitempty"`
	SelectionKey       string   `json:"selection_key"`
	Model              string   `json:"model,omitempty"`
	QuotaGroup         string   `json:"quota_group,omitempty"`
	Pool               string   `json:"pool,omitempty"`
	AccountState       string   `json:"account_state,omitempty"`
	SelectionScore     *float64 `json:"selection_score,omitempty"`
	SelectionReason    string   `json:"selection_reason,omitempty"`
	State              string   `json:"state"`
	InFlight           int      `json:"in_flight"`
	MaxConcurrency     int      `json:"max_concurrency"`
	RemainingLifeMS    int64    `json:"remaining_life_ms"`
	BorrowedUnlimited  bool     `json:"borrowed_unlimited,omitempty"`
	Reclaimable        bool     `json:"reclaimable,omitempty"`
}

type schedulerSnapshot struct {
	Config         schedulerConfigView          `json:"config"`
	Limits         schedulerLimits              `json:"limits"`
	Queue          schedulerQueueView           `json:"queue"`
	Warnings       []string                     `json:"warnings"`
	Accounts       []schedulerAccountView       `json:"accounts"`
	Sessions       []schedulerSessionView       `json:"sessions"`
	PendingCreates []schedulerPendingCreateView `json:"pending_creates,omitempty"`
	ModelLimits    []schedulerModelLimitView    `json:"model_limits,omitempty"`
}

func defaultSchedulerConfig() SchedulerConfig {
	cfg := SchedulerConfig{
		PremiumCoreRatio:            0.35,
		PremiumMaxRatio:             0.70,
		UnlimitedReserveRatio:       0.25,
		UnlimitedMinReserveAccounts: 1,
		PremiumBurstQueueThreshold:  2,
		PremiumQueueTimeout:         1500 * time.Millisecond,
		ModelMaxAccountRatio:        0.35,
		ModelBurstAccountRatio:      0.10,
	}
	cfg.PremiumCoreRatio = envFloat("FREEBUFF_PREMIUM_CORE_RATIO", cfg.PremiumCoreRatio)
	cfg.PremiumMaxRatio = envFloat("FREEBUFF_PREMIUM_MAX_RATIO", cfg.PremiumMaxRatio)
	cfg.UnlimitedReserveRatio = envFloat("FREEBUFF_UNLIMITED_RESERVE_RATIO", cfg.UnlimitedReserveRatio)
	cfg.UnlimitedMinReserveAccounts = envInt("FREEBUFF_UNLIMITED_MIN_RESERVE_ACCOUNTS", cfg.UnlimitedMinReserveAccounts)
	cfg.PremiumBurstQueueThreshold = envInt("FREEBUFF_PREMIUM_BURST_QUEUE_THRESHOLD", cfg.PremiumBurstQueueThreshold)
	cfg.ModelMaxAccountRatio = envFloat("FREEBUFF_MODEL_MAX_ACCOUNT_RATIO", cfg.ModelMaxAccountRatio)
	cfg.ModelBurstAccountRatio = envFloat("FREEBUFF_MODEL_BURST_ACCOUNT_RATIO", cfg.ModelBurstAccountRatio)
	if timeoutMS := envInt("FREEBUFF_PREMIUM_QUEUE_TIMEOUT_MS", 0); timeoutMS > 0 {
		cfg.PremiumQueueTimeout = time.Duration(timeoutMS) * time.Millisecond
	}
	return normalizeSchedulerConfig(cfg)
}

func newPremiumScheduler(cfg SchedulerConfig) *premiumScheduler {
	return &premiumScheduler{
		cfg:               normalizeSchedulerConfig(cfg),
		history:           make(map[string]premiumAccountHistory),
		queueDepthByModel: make(map[string]int),
	}
}

func normalizeSchedulerConfig(cfg SchedulerConfig) SchedulerConfig {
	cfg.PremiumCoreRatio = clampRatio(cfg.PremiumCoreRatio, 0.35)
	cfg.PremiumMaxRatio = clampRatio(cfg.PremiumMaxRatio, 0.70)
	cfg.UnlimitedReserveRatio = clampRatio(cfg.UnlimitedReserveRatio, 0.25)
	cfg.ModelMaxAccountRatio = clampRatio(cfg.ModelMaxAccountRatio, 0.35)
	cfg.ModelBurstAccountRatio = clampRatio(cfg.ModelBurstAccountRatio, 0.10)
	if cfg.PremiumMaxRatio < cfg.PremiumCoreRatio {
		cfg.PremiumMaxRatio = cfg.PremiumCoreRatio
	}
	if cfg.UnlimitedMinReserveAccounts < 0 {
		cfg.UnlimitedMinReserveAccounts = 0
	}
	if cfg.PremiumBurstQueueThreshold < 1 {
		cfg.PremiumBurstQueueThreshold = 1
	}
	if cfg.PremiumQueueTimeout < 0 {
		cfg.PremiumQueueTimeout = 0
	}
	return cfg
}

func clampRatio(v, fallback float64) float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return fallback
	}
	if v > 1 {
		return 1
	}
	return v
}

func envFloat(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func (a *Adapter) ScheduleSession(ctx context.Context, req channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	if a == nil || a.scheduler == nil {
		return channels.SessionScheduleDecision{}, nil
	}
	return a.scheduler.Schedule(ctx, req)
}

func (a *Adapter) SchedulerSnapshot(ctx context.Context, req channels.SchedulerSnapshotRequest) (any, error) {
	if a == nil || a.scheduler == nil {
		return schedulerSnapshot{}, nil
	}
	return a.scheduler.Snapshot(ctx, req), nil
}

func (s *premiumScheduler) Schedule(ctx context.Context, req channels.SessionScheduleRequest) (channels.SessionScheduleDecision, error) {
	model, err := modelFromKey(req.SelectionKey)
	if err != nil {
		return channels.SessionScheduleDecision{}, nil
	}
	group, ok := quotaGroupForModel(model)
	if !ok {
		return channels.SessionScheduleDecision{}, nil
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	if group == QuotaGroupPremiumShared {
		return s.schedulePremium(ctx, req, model, now)
	}
	return s.scheduleUnlimited(req, model, now), nil
}

func (s *premiumScheduler) schedulePremium(_ context.Context, req channels.SessionScheduleRequest, model string, now time.Time) (channels.SessionScheduleDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resetExpiredLocked(now)
	limits := s.limitsLocked(req.Accounts, req.Sessions, req.PendingCreates, now)
	admission := s.modelAdmissionLocked(req, model, QuotaGroupPremiumShared, limits, now)
	if admission.capped {
		s.queueDepthByModel[model]++
		return channels.SessionScheduleDecision{
			Action:      channels.SessionScheduleWait,
			WaitTimeout: s.cfg.PremiumQueueTimeout,
			Reason:      admission.reason,
			Finish:      s.finishQueueWait(model),
		}, nil
	}
	reason := "premium_core_create"
	switch {
	case limits.TotalActiveAccounts == 0:
		return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate}, nil
	case limits.PremiumActiveAccounts < limits.CorePremiumAccounts:
		preferred := s.preferredPremiumAccountsLocked(req.Accounts, now, reason)
		if len(preferred) == 0 {
			return channels.SessionScheduleDecision{Action: channels.SessionScheduleReject, Reason: "premium_no_quota_capacity"}, nil
		}
		return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate, PreferredAccountIDs: preferred, Reason: reason}, nil
	case limits.PremiumActiveAccounts < limits.MaxPremiumAccounts:
		depth := s.queueDepthByModel[model] + 1
		if depth >= s.cfg.PremiumBurstQueueThreshold {
			reason = "premium_burst_queue_threshold"
			preferred := s.preferredPremiumAccountsLocked(req.Accounts, now, reason)
			if len(preferred) == 0 {
				return channels.SessionScheduleDecision{Action: channels.SessionScheduleReject, Reason: "premium_no_quota_capacity"}, nil
			}
			return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate, PreferredAccountIDs: preferred, Reason: reason}, nil
		}
		reason = "premium_queue_reuse"
		s.queueDepthByModel[model]++
		return channels.SessionScheduleDecision{
			Action:      channels.SessionScheduleWait,
			WaitTimeout: s.cfg.PremiumQueueTimeout,
			Reason:      reason,
			Finish:      s.finishQueueWait(model),
		}, nil
	default:
		reason = "premium_capacity_limited"
		s.queueDepthByModel[model]++
		return channels.SessionScheduleDecision{
			Action:      channels.SessionScheduleWait,
			WaitTimeout: s.cfg.PremiumQueueTimeout,
			Reason:      reason + ": model=" + model,
			Finish:      s.finishQueueWait(model),
		}, nil
	}
}

func (s *premiumScheduler) finishQueueWait(model string) func() {
	return func() {
		s.mu.Lock()
		if s.queueDepthByModel[model] > 0 {
			s.queueDepthByModel[model]--
		}
		if s.queueDepthByModel[model] <= 0 {
			delete(s.queueDepthByModel, model)
		}
		s.mu.Unlock()
	}
}

func (s *premiumScheduler) scheduleUnlimited(req channels.SessionScheduleRequest, model string, now time.Time) channels.SessionScheduleDecision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetExpiredLocked(now)
	limits := s.limitsLocked(req.Accounts, req.Sessions, req.PendingCreates, now)
	admission := s.modelAdmissionLocked(req, model, QuotaGroupUnlimited, limits, now)
	if admission.capped {
		s.queueDepthByModel[model]++
		return channels.SessionScheduleDecision{
			Action:      channels.SessionScheduleWait,
			WaitTimeout: s.cfg.PremiumQueueTimeout,
			Reason:      admission.reason,
			Finish:      s.finishQueueWait(model),
		}
	}
	reason := "unlimited_low_tier_first"
	preferred := s.preferredUnlimitedAccountsLocked(req.Accounts, now, reason)
	for _, id := range preferred {
		if _, ok := s.history[id]; !ok {
			continue
		}
		h := s.history[id]
		if strings.HasPrefix(h.LastPool, "premium_") {
			h.BorrowedUnlimited = true
			h.LastUpdatedUnix = now.Unix()
			s.history[id] = h
		}
		break
	}
	_ = model
	return channels.SessionScheduleDecision{Action: channels.SessionScheduleCreate, PreferredAccountIDs: preferred, Reason: reason}
}

func (s *premiumScheduler) observeSession(accountID, model string, session upstreamSession, state channels.State) {
	if accountID == "" {
		return
	}
	group, ok := quotaGroupForModel(model)
	if !ok {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetExpiredLocked(now)
	h := s.history[accountID]
	h.LastUpdatedUnix = now.Unix()
	if group == QuotaGroupPremiumShared {
		h.CurrentWindowTouched = true
		h.EverPremiumTouched = true
		h.BorrowedUnlimited = false
		if limit, ok := session.rateLimitFor(model); ok {
			h.WindowResetUnix = parseExpiresAtUnix(limit.ResetAt)
			h.Remaining, h.RemainingKnown = premiumRemaining(limit)
			h.Depleted = h.RemainingKnown && h.Remaining <= 0 && resetStillCurrent(h.WindowResetUnix, now)
		}
	} else if h.LastPool == "premium_fresh_borrowed" || h.LastPool == "premium_capable_borrowed" {
		h.BorrowedUnlimited = true
	}
	s.history[accountID] = h
	s.decorateStateLocked(accountID, state)
}

func (s *premiumScheduler) markPremiumDepleted(accountID, model string, session upstreamSession) {
	if accountID == "" {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetExpiredLocked(now)
	h := s.history[accountID]
	h.CurrentWindowTouched = true
	h.EverPremiumTouched = true
	h.Depleted = true
	h.RemainingKnown = true
	h.Remaining = 0
	h.LastUpdatedUnix = now.Unix()
	if limit, ok := session.rateLimitFor(model); ok {
		h.WindowResetUnix = parseExpiresAtUnix(limit.ResetAt)
	}
	s.history[accountID] = h
}

func premiumRemaining(limit upstreamRateLimit) (int, bool) {
	if limit.Limit <= 0 {
		return 0, false
	}
	remaining := limit.Limit - int(math.Ceil(limit.RecentCount))
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true
}

func resetStillCurrent(resetUnix int64, now time.Time) bool {
	return resetUnix <= 0 || now.Unix() < resetUnix
}

func (s *premiumScheduler) decorateSessionState(accountID string, state channels.State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decorateStateLocked(accountID, state)
}

func (s *premiumScheduler) decorateStateLocked(accountID string, state channels.State) {
	if state == nil {
		return
	}
	h := s.history[accountID]
	if h.LastPool != "" {
		state[stateSchedulerPool] = h.LastPool
	}
	if h.LastReason != "" {
		state[stateSchedulerReason] = h.LastReason
	}
	state[statePremiumTouched] = h.CurrentWindowTouched
	state[stateBorrowedUnlimited] = h.BorrowedUnlimited
	if h.RemainingKnown {
		state[statePremiumRemaining] = h.Remaining
	}
	if h.WindowResetUnix > 0 {
		state[statePremiumResetAtUnix] = h.WindowResetUnix
	}
}

func (s *premiumScheduler) preferredPremiumAccountsLocked(accounts []channels.AccountCandidate, now time.Time, reason string) []string {
	type ranked struct {
		id        string
		tier      int
		remaining int
		priority  int
		lastUsed  int64
	}
	rankedAccounts := make([]ranked, 0, len(accounts))
	for _, candidate := range accounts {
		if !candidate.Eligible {
			continue
		}
		h := s.historyForCandidateLocked(candidate, now)
		tier := 3
		remaining := math.MaxInt
		switch {
		case h.Depleted:
			continue
		case h.CurrentWindowTouched && h.RemainingKnown:
			tier = 0
			remaining = h.Remaining
		case h.CurrentWindowTouched:
			tier = 1
		default:
			tier = 2
		}
		rankedAccounts = append(rankedAccounts, ranked{
			id:        candidate.Account.ID,
			tier:      tier,
			remaining: remaining,
			priority:  candidate.Priority,
			lastUsed:  candidate.LastUsedAtUnix,
		})
	}
	sort.SliceStable(rankedAccounts, func(i, j int) bool {
		a, b := rankedAccounts[i], rankedAccounts[j]
		if a.tier != b.tier {
			return a.tier < b.tier
		}
		if a.remaining != b.remaining {
			return a.remaining < b.remaining
		}
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		return a.lastUsed < b.lastUsed
	})
	out := make([]string, 0, len(rankedAccounts))
	for _, item := range rankedAccounts {
		h := s.historyForLocked(item.id, now)
		h.LastReason = reason
		h.LastPool = "premium_capable"
		h.LastUpdatedUnix = now.Unix()
		s.history[item.id] = h
		out = append(out, item.id)
	}
	return out
}

func (s *premiumScheduler) preferredUnlimitedAccountsLocked(accounts []channels.AccountCandidate, now time.Time, reason string) []string {
	type ranked struct {
		id        string
		tier      int
		remaining int
		priority  int
		lastUsed  int64
		pool      string
	}
	rankedAccounts := make([]ranked, 0, len(accounts))
	for _, candidate := range accounts {
		if !candidate.Eligible {
			continue
		}
		h := s.historyForCandidateLocked(candidate, now)
		tier := 0
		pool := "low_tier"
		remaining := math.MaxInt
		switch {
		case h.Depleted || h.BorrowedUnlimited:
			tier = 0
			pool = "low_tier"
		case !h.CurrentWindowTouched && !h.EverPremiumTouched:
			tier = 1
			pool = "premium_never_touched_borrowed"
		case !h.CurrentWindowTouched:
			tier = 2
			pool = "premium_outside_window_borrowed"
		case h.RemainingKnown:
			tier = 3
			remaining = -h.Remaining
			pool = "premium_capable_borrowed"
		default:
			tier = 4
			pool = "premium_touched_last_resort"
		}
		rankedAccounts = append(rankedAccounts, ranked{
			id:        candidate.Account.ID,
			tier:      tier,
			remaining: remaining,
			priority:  candidate.Priority,
			lastUsed:  candidate.LastUsedAtUnix,
			pool:      pool,
		})
	}
	sort.SliceStable(rankedAccounts, func(i, j int) bool {
		a, b := rankedAccounts[i], rankedAccounts[j]
		if a.tier != b.tier {
			return a.tier < b.tier
		}
		if a.remaining != b.remaining {
			return a.remaining < b.remaining
		}
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		return a.lastUsed < b.lastUsed
	})
	out := make([]string, 0, len(rankedAccounts))
	for _, item := range rankedAccounts {
		h := s.historyForLocked(item.id, now)
		h.LastReason = reason
		h.LastPool = item.pool
		h.LastUpdatedUnix = now.Unix()
		s.history[item.id] = h
		out = append(out, item.id)
	}
	return out
}

func (s *premiumScheduler) limitsLocked(accounts []channels.AccountCandidate, sessions []channels.SessionCandidate, pending []channels.SessionCreateCandidate, now time.Time) schedulerLimits {
	activeAccounts := 0
	for _, candidate := range accounts {
		if candidate.Active {
			activeAccounts++
		}
	}
	limits := s.accountLimits(activeAccounts)
	limits.PremiumActiveAccounts = activePremiumAccountCount(sessions, pending, now)
	return limits
}

func (s *premiumScheduler) accountLimits(activeAccounts int) schedulerLimits {
	limits := schedulerLimits{TotalActiveAccounts: activeAccounts}
	switch {
	case activeAccounts <= 0:
		return limits
	case activeAccounts == 1:
		limits.CorePremiumAccounts = 1
		limits.MaxPremiumAccounts = 1
		limits.UnlimitedReserve = 0
		limits.Warnings = append(limits.Warnings, "no_unlimited_reserve")
		return limits
	case activeAccounts == 2:
		limits.CorePremiumAccounts = 1
		limits.MaxPremiumAccounts = 1
		limits.UnlimitedReserve = 1
		return limits
	}
	reserve := int(math.Ceil(float64(activeAccounts) * s.cfg.UnlimitedReserveRatio))
	if reserve < s.cfg.UnlimitedMinReserveAccounts {
		reserve = s.cfg.UnlimitedMinReserveAccounts
	}
	if reserve >= activeAccounts {
		reserve = activeAccounts - 1
	}
	maxPremium := int(math.Floor(float64(activeAccounts) * s.cfg.PremiumMaxRatio))
	if maxPremium < 1 {
		maxPremium = 1
	}
	if maxPremium > activeAccounts-reserve {
		maxPremium = activeAccounts - reserve
	}
	core := int(math.Ceil(float64(activeAccounts) * s.cfg.PremiumCoreRatio))
	if core < 1 {
		core = 1
	}
	if core > maxPremium {
		core = maxPremium
	}
	limits.CorePremiumAccounts = core
	limits.MaxPremiumAccounts = maxPremium
	limits.UnlimitedReserve = reserve
	return limits
}

type modelAdmission struct {
	capped bool
	reason string
}

func (s *premiumScheduler) modelAdmissionLocked(req channels.SessionScheduleRequest, model, group string, limits schedulerLimits, now time.Time) modelAdmission {
	activeSessions := activeModelSessionCount(req.Sessions, model, now)
	pendingCreates := pendingModelCreateCount(req.PendingCreates, model)
	activeTotal := activeSessions + pendingCreates
	groupUsable := limits.TotalActiveAccounts
	if group == QuotaGroupPremiumShared && limits.MaxPremiumAccounts > 0 {
		groupUsable = limits.MaxPremiumAccounts
	}
	if groupUsable < 1 {
		groupUsable = 1
	}
	softCap, burstCap := s.modelCaps(limits.TotalActiveAccounts, groupUsable, group)
	otherPressure := s.otherModelPressureLocked(model, group)
	switch {
	case activeTotal >= burstCap:
		return modelAdmission{capped: true, reason: "model_burst_capacity_limited: model=" + model}
	case activeTotal >= softCap && otherPressure:
		return modelAdmission{capped: true, reason: "model_fair_share_limited: model=" + model}
	default:
		return modelAdmission{}
	}
}

func (s *premiumScheduler) modelCaps(totalActiveAccounts, groupUsableAccounts int, group string) (int, int) {
	models := enabledModelCountForGroup(group)
	if models < 1 {
		models = 1
	}
	softCap := int(math.Ceil(float64(groupUsableAccounts) / float64(models)))
	if softCap < 1 {
		softCap = 1
	}
	hardCap := int(math.Ceil(float64(totalActiveAccounts) * s.cfg.ModelMaxAccountRatio))
	if hardCap < softCap {
		hardCap = softCap
	}
	burstExtra := int(math.Ceil(float64(totalActiveAccounts) * s.cfg.ModelBurstAccountRatio))
	burstCap := hardCap + burstExtra
	if burstCap < softCap {
		burstCap = softCap
	}
	if groupUsableAccounts > 0 && burstCap > groupUsableAccounts {
		burstCap = groupUsableAccounts
	}
	if groupUsableAccounts > models && models > 1 && burstCap >= groupUsableAccounts {
		burstCap = groupUsableAccounts - 1
	}
	if burstCap < 1 {
		burstCap = 1
	}
	return softCap, burstCap
}

func enabledModelCountForGroup(group string) int {
	n := 0
	for _, profile := range modelCatalog {
		if profile.Enabled && profile.QuotaGroup == group {
			n++
		}
	}
	return n
}

func (s *premiumScheduler) otherModelPressureLocked(model, group string) bool {
	for queuedModel, depth := range s.queueDepthByModel {
		if queuedModel == model || depth <= 0 {
			continue
		}
		if queuedGroup, ok := quotaGroupForModel(queuedModel); ok && queuedGroup == group {
			return true
		}
	}
	return false
}

func activeModelSessionCount(sessions []channels.SessionCandidate, model string, now time.Time) int {
	count := 0
	for _, session := range sessions {
		if !session.Healthy {
			continue
		}
		if session.ExpiresAtUnix > 0 && now.Unix() >= session.ExpiresAtUnix {
			continue
		}
		if sessionModel(session) == model {
			count++
		}
	}
	return count
}

func pendingModelCreateCount(pending []channels.SessionCreateCandidate, model string) int {
	count := 0
	for _, create := range pending {
		if create.Model == model {
			count++
		}
	}
	return count
}

func sessionModel(session channels.SessionCandidate) string {
	if model := session.State.String(stateModel); model != "" {
		return model
	}
	if model, err := modelFromKey(session.Key); err == nil {
		return model
	}
	return ""
}

func activePremiumAccountCount(sessions []channels.SessionCandidate, pending []channels.SessionCreateCandidate, now time.Time) int {
	seen := map[string]struct{}{}
	countWithoutAccount := 0
	for _, session := range sessions {
		if !session.Healthy || session.AccountID == "" {
			continue
		}
		if session.ExpiresAtUnix > 0 && now.Unix() >= session.ExpiresAtUnix {
			continue
		}
		model := sessionModel(session)
		if group, ok := quotaGroupForModel(model); ok && group == QuotaGroupPremiumShared {
			seen[session.AccountID] = struct{}{}
		}
	}
	for _, create := range pending {
		if create.QuotaGroup != QuotaGroupPremiumShared {
			continue
		}
		if create.AccountID == "" {
			countWithoutAccount++
			continue
		}
		seen[create.AccountID] = struct{}{}
	}
	return len(seen) + countWithoutAccount
}

func (s *premiumScheduler) historyForLocked(accountID string, now time.Time) premiumAccountHistory {
	h := s.history[accountID]
	if h.WindowResetUnix > 0 && now.Unix() >= h.WindowResetUnix {
		h.CurrentWindowTouched = false
		h.Depleted = false
		h.Remaining = 0
		h.RemainingKnown = false
		h.BorrowedUnlimited = false
		h.WindowResetUnix = 0
		s.history[accountID] = h
	}
	return h
}

func (s *premiumScheduler) historyForCandidateLocked(candidate channels.AccountCandidate, now time.Time) premiumAccountHistory {
	h := s.historyForLocked(candidate.Account.ID, now)
	if len(candidate.ProviderFacts) == 0 {
		return h
	}
	if factBool(candidate.ProviderFacts, freebufffacts.SchedulerFactPremiumWindowTouched) {
		h.CurrentWindowTouched = true
		h.EverPremiumTouched = true
	}
	if factBool(candidate.ProviderFacts, freebufffacts.SchedulerFactEverPremiumTouched) {
		h.EverPremiumTouched = true
	}
	if factBool(candidate.ProviderFacts, freebufffacts.SchedulerFactPremiumRemainingKnown) {
		h.RemainingKnown = true
		h.Remaining = factInt(candidate.ProviderFacts, freebufffacts.SchedulerFactPremiumRemaining)
	}
	if resetUnix := factInt64(candidate.ProviderFacts, freebufffacts.SchedulerFactPremiumResetAtUnix); resetUnix > 0 {
		h.WindowResetUnix = resetUnix
	}
	switch {
	case factBool(candidate.ProviderFacts, freebufffacts.SchedulerFactPremiumDepleted) && resetStillCurrent(h.WindowResetUnix, now):
		h.Depleted = true
	case h.RemainingKnown && h.Remaining > 0:
		h.Depleted = false
	}
	if factBool(candidate.ProviderFacts, freebufffacts.SchedulerFactBorrowedUnlimited) {
		h.BorrowedUnlimited = true
	}
	h.LastUpdatedUnix = now.Unix()
	s.history[candidate.Account.ID] = h
	return h
}

func (s *premiumScheduler) resetExpiredLocked(now time.Time) {
	for accountID := range s.history {
		_ = s.historyForLocked(accountID, now)
	}
}

func (s *premiumScheduler) Snapshot(_ context.Context, req channels.SchedulerSnapshotRequest) schedulerSnapshot {
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetExpiredLocked(now)
	limits := s.limitsLocked(req.Accounts, req.Sessions, req.PendingCreates, now)
	if limits.Warnings == nil {
		limits.Warnings = []string{}
	}
	warnings := append([]string{}, limits.Warnings...)
	accounts := make([]schedulerAccountView, 0, len(req.Accounts))
	for _, candidate := range req.Accounts {
		h := s.historyForCandidateLocked(candidate, now)
		remaining := (*int)(nil)
		if h.RemainingKnown {
			value := h.Remaining
			remaining = &value
		}
		pool, state, reasons := s.accountPoolStateLocked(candidate, h)
		score, scoreBand, scoreStatus, scoreReasons := s.accountScoreLocked(candidate, h, pool, state, reasons, now)
		accounts = append(accounts, schedulerAccountView{
			AccountID:            candidate.Account.ID,
			Name:                 candidate.Account.Name,
			Pool:                 pool,
			State:                state,
			Eligible:             candidate.Eligible,
			BlockedReason:        candidate.BlockedReason,
			SessionCount:         candidate.SessionCount,
			LastDecisionReason:   h.LastReason,
			PremiumWindowTouched: h.CurrentWindowTouched,
			EverPremiumTouched:   h.EverPremiumTouched,
			PremiumDepleted:      h.Depleted,
			PremiumRemaining:     remaining,
			PremiumResetAtUnix:   h.WindowResetUnix,
			BorrowedUnlimited:    h.BorrowedUnlimited,
			Score:                score,
			ScoreBand:            scoreBand,
			ScoreStatus:          scoreStatus,
			ScoreReasons:         scoreReasons,
		})
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].AccountID < accounts[j].AccountID })
	accountByID := make(map[string]schedulerAccountView, len(accounts))
	for _, account := range accounts {
		accountByID[account.AccountID] = account
	}
	sessions := make([]schedulerSessionView, 0, len(req.Sessions))
	for _, session := range req.Sessions {
		model := session.State.String(stateModel)
		group, _ := quotaGroupForModel(model)
		remaining := int64(0)
		if session.ExpiresAtUnix > 0 {
			remaining = time.Until(time.Unix(session.ExpiresAtUnix, 0)).Milliseconds()
			if remaining < 0 {
				remaining = 0
			}
		}
		state := "expired"
		if session.Healthy {
			state = "active"
		}
		account := accountByID[session.AccountID]
		pool := firstNonEmptyString(session.State.String(stateSchedulerPool), account.Pool)
		accountState := account.State
		selectionReason := firstNonEmptyString(session.State.String(stateSchedulerReason), account.LastDecisionReason)
		borrowedUnlimited := stateBool(session.State, stateBorrowedUnlimited) || account.BorrowedUnlimited
		reclaimable := group == QuotaGroupUnlimited && session.InFlight == 0 && session.Healthy
		sessions = append(sessions, schedulerSessionView{
			ID:                 session.ID,
			AccountID:          session.AccountID,
			AccountDisplayName: account.Name,
			SelectionKey:       session.Key,
			Model:              model,
			QuotaGroup:         group,
			Pool:               pool,
			AccountState:       accountState,
			SelectionScore:     account.Score,
			SelectionReason:    selectionReason,
			State:              state,
			InFlight:           session.InFlight,
			MaxConcurrency:     session.MaxConcurrency,
			RemainingLifeMS:    remaining,
			BorrowedUnlimited:  borrowedUnlimited,
			Reclaimable:        reclaimable,
		})
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].ID < sessions[j].ID })
	pendingCreates := pendingCreateViews(req.PendingCreates)
	modelLimits := s.modelLimitViewsLocked(req, limits, now)
	return schedulerSnapshot{
		Config: schedulerConfigView{
			PremiumCoreRatio:            s.cfg.PremiumCoreRatio,
			PremiumMaxRatio:             s.cfg.PremiumMaxRatio,
			UnlimitedReserveRatio:       s.cfg.UnlimitedReserveRatio,
			UnlimitedMinReserveAccounts: s.cfg.UnlimitedMinReserveAccounts,
			PremiumBurstQueueThreshold:  s.cfg.PremiumBurstQueueThreshold,
			PremiumQueueTimeoutMS:       s.cfg.PremiumQueueTimeout.Milliseconds(),
			ModelMaxAccountRatio:        s.cfg.ModelMaxAccountRatio,
			ModelBurstAccountRatio:      s.cfg.ModelBurstAccountRatio,
		},
		Limits: limits,
		Queue: schedulerQueueView{
			PremiumDepth:          s.queueDepthTotalLocked(),
			DepthByModel:          s.queueDepthByModelLocked(),
			PendingCreatesByModel: pendingCreatesByModel(req.PendingCreates),
			PendingCreatesByGroup: pendingCreatesByGroup(req.PendingCreates),
			TimeoutMS:             s.cfg.PremiumQueueTimeout.Milliseconds(),
		},
		Warnings:       warnings,
		Accounts:       accounts,
		Sessions:       sessions,
		PendingCreates: pendingCreates,
		ModelLimits:    modelLimits,
	}
}

func pendingCreateViews(pending []channels.SessionCreateCandidate) []schedulerPendingCreateView {
	if len(pending) == 0 {
		return nil
	}
	out := make([]schedulerPendingCreateView, 0, len(pending))
	for _, create := range pending {
		out = append(out, schedulerPendingCreateView{
			ChannelID:     create.ChannelID,
			SelectionKey:  create.Key,
			Model:         create.Model,
			QuotaGroup:    create.QuotaGroup,
			StartedAtUnix: create.StartedAtUnix,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Model != out[j].Model {
			return out[i].Model < out[j].Model
		}
		return out[i].StartedAtUnix < out[j].StartedAtUnix
	})
	return out
}

func pendingCreatesByModel(pending []channels.SessionCreateCandidate) map[string]int {
	out := map[string]int{}
	for _, create := range pending {
		if create.Model == "" {
			continue
		}
		out[create.Model]++
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func pendingCreatesByGroup(pending []channels.SessionCreateCandidate) map[string]int {
	out := map[string]int{}
	for _, create := range pending {
		if create.QuotaGroup == "" {
			continue
		}
		out[create.QuotaGroup]++
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *premiumScheduler) modelLimitViewsLocked(req channels.SchedulerSnapshotRequest, limits schedulerLimits, now time.Time) []schedulerModelLimitView {
	models := map[string]struct{}{}
	for _, session := range req.Sessions {
		if model := sessionModel(session); model != "" {
			models[model] = struct{}{}
		}
	}
	for _, create := range req.PendingCreates {
		if create.Model != "" {
			models[create.Model] = struct{}{}
		}
	}
	out := make([]schedulerModelLimitView, 0, len(models))
	for model := range models {
		group, _ := quotaGroupForModel(model)
		groupUsable := limits.TotalActiveAccounts
		if group == QuotaGroupPremiumShared && limits.MaxPremiumAccounts > 0 {
			groupUsable = limits.MaxPremiumAccounts
		}
		softCap, burstCap := s.modelCaps(limits.TotalActiveAccounts, groupUsable, group)
		activeSessions := activeModelSessionCount(req.Sessions, model, now)
		pendingCreates := pendingModelCreateCount(req.PendingCreates, model)
		activeTotal := activeSessions + pendingCreates
		otherPressure := s.otherModelPressureLocked(model, group)
		state := "available"
		if activeTotal >= burstCap {
			state = "capped"
		} else if activeTotal >= softCap {
			state = "burst"
		}
		out = append(out, schedulerModelLimitView{
			Model:              model,
			QuotaGroup:         group,
			ActiveSessions:     activeSessions,
			PendingCreates:     pendingCreates,
			ActiveTotal:        activeTotal,
			SoftCap:            softCap,
			BurstCap:           burstCap,
			State:              state,
			OtherModelPressure: otherPressure,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

func (s *premiumScheduler) accountPoolStateLocked(candidate channels.AccountCandidate, h premiumAccountHistory) (string, string, []string) {
	if !candidate.Active {
		return "inactive", "inactive", []string{"account_inactive"}
	}
	if candidate.BlockedReason != "" {
		return "blocked", candidate.BlockedReason, []string{candidate.BlockedReason}
	}
	switch {
	case h.BorrowedUnlimited:
		return "low_tier", "borrowed_unlimited", []string{"borrowed_unlimited_reclaimable"}
	case h.Depleted:
		return "low_tier", "premium_depleted", []string{"premium_quota_depleted"}
	case h.CurrentWindowTouched:
		return "premium_capable", "premium_ready", []string{"premium_window_touched"}
	case h.EverPremiumTouched:
		return "premium_capable", "premium_reset", []string{"outside_window_premium_history"}
	default:
		return "premium_capable", "premium_fresh", []string{"fresh_premium_capacity"}
	}
}

func (s *premiumScheduler) accountScoreLocked(candidate channels.AccountCandidate, h premiumAccountHistory, pool, state string, baseReasons []string, now time.Time) (*float64, string, string, []string) {
	reasons := append([]string{}, baseReasons...)
	if !candidate.Active || candidate.BlockedReason != "" || !candidate.Eligible {
		reasons = appendUniqueStrings(reasons, "score_unavailable")
		return nil, "", "unavailable", reasons
	}

	score := 50.0
	reasons = appendUniqueStrings(reasons, "score_context_premium_creation")

	switch {
	case h.BorrowedUnlimited:
		score += 8
		reasons = appendUniqueStrings(reasons, "reclaimable_unlimited_session")
	case h.Depleted:
		score -= 25
		reasons = appendUniqueStrings(reasons, "premium_start_quota_depleted")
	case h.CurrentWindowTouched && h.RemainingKnown:
		score += 28
		reasons = appendUniqueStrings(reasons, "current_window_premium_history", "known_quota_snapshot")
		if h.Remaining > 0 {
			score += math.Max(0, 12-float64(h.Remaining))
			reasons = appendUniqueStrings(reasons, "smallest_positive_remaining_premium_quota")
		}
	case h.CurrentWindowTouched:
		score += 18
		reasons = appendUniqueStrings(reasons, "current_window_premium_history", "unknown_quota_snapshot")
	case !h.EverPremiumTouched:
		score += 7
		reasons = appendUniqueStrings(reasons, "fresh_premium_capacity_preserved")
	default:
		score += 3
		reasons = appendUniqueStrings(reasons, "outside_window_premium_history")
	}

	if pool == "low_tier" {
		reasons = appendUniqueStrings(reasons, "low_tier_unlimited_capacity")
	}
	if h.WindowResetUnix > 0 && resetStillCurrent(h.WindowResetUnix, now) {
		untilReset := time.Unix(h.WindowResetUnix, 0).Sub(now)
		if untilReset <= 2*time.Hour {
			score += 3
			reasons = appendUniqueStrings(reasons, "premium_reset_near")
		}
	}
	if candidate.OnCooldown {
		score -= 30
		reasons = appendUniqueStrings(reasons, "cooldown_pressure")
	}
	if candidate.SessionCount > 0 {
		score -= float64(candidate.SessionCount) * 4
		reasons = appendUniqueStrings(reasons, "active_session_pressure")
	}
	if state == "premium_fresh" {
		reasons = appendUniqueStrings(reasons, "fresh_account_not_yet_burned")
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	score = math.Round(score*10) / 10
	return &score, scoreBand(score), "available", reasons
}

func scoreBand(score float64) string {
	switch {
	case score >= 80:
		return "high"
	case score >= 55:
		return "medium"
	default:
		return "low"
	}
}

func appendUniqueStrings(values []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(additions))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range additions {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values
}

func (s *premiumScheduler) queueDepthTotalLocked() int {
	total := 0
	for _, depth := range s.queueDepthByModel {
		total += depth
	}
	return total
}

func (s *premiumScheduler) queueDepthByModelLocked() map[string]int {
	if len(s.queueDepthByModel) == 0 {
		return nil
	}
	out := make(map[string]int, len(s.queueDepthByModel))
	for model, depth := range s.queueDepthByModel {
		if depth > 0 {
			out[model] = depth
		}
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func stateBool(state channels.State, key string) bool {
	if state == nil {
		return false
	}
	value, ok := state[key]
	if !ok {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1" || v == "yes"
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}

func factBool(facts map[string]any, key string) bool {
	value, ok := facts[key]
	if !ok {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1" || v == "yes"
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}

func factInt(facts map[string]any, key string) int {
	value, ok := facts[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		out, _ := v.Int64()
		return int(out)
	case string:
		out, _ := strconv.Atoi(v)
		return out
	default:
		return 0
	}
}

func factInt64(facts map[string]any, key string) int64 {
	value, ok := facts[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		out, _ := v.Int64()
		return out
	case string:
		out, _ := strconv.ParseInt(v, 10, 64)
		return out
	default:
		return 0
	}
}
