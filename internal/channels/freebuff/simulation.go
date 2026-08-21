package freebuff

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"freebuff-reverse/internal/channels"
)

type SchedulerSimulationInput struct {
	Configs        []SchedulerSimulationConfig  `json:"configs"`
	Accounts       []SchedulerSimulationAccount `json:"accounts"`
	Requests       []SchedulerSimulationRequest `json:"requests"`
	SessionTTLMS   int64                        `json:"session_ttl_ms"`
	MaxConcurrency int                          `json:"max_concurrency"`
}

type SchedulerSimulationConfig struct {
	Name                        string  `json:"name"`
	PremiumCoreRatio            float64 `json:"premium_core_ratio,omitempty"`
	PremiumMaxRatio             float64 `json:"premium_max_ratio,omitempty"`
	UnlimitedReserveRatio       float64 `json:"unlimited_reserve_ratio,omitempty"`
	UnlimitedMinReserveAccounts int     `json:"unlimited_min_reserve_accounts,omitempty"`
	PremiumBurstQueueThreshold  int     `json:"premium_burst_queue_threshold,omitempty"`
	PremiumQueueTimeoutMS       int64   `json:"premium_queue_timeout_ms,omitempty"`
	ModelMaxAccountRatio        float64 `json:"model_max_account_ratio,omitempty"`
	ModelBurstAccountRatio      float64 `json:"model_burst_account_ratio,omitempty"`
}

type SchedulerSimulationAccount struct {
	ID                   string `json:"id"`
	Name                 string `json:"name,omitempty"`
	Priority             int    `json:"priority,omitempty"`
	Active               bool   `json:"active"`
	PremiumLimit         int    `json:"premium_limit,omitempty"`
	PremiumRemaining     *int   `json:"premium_remaining,omitempty"`
	PremiumResetAtUnix   int64  `json:"premium_reset_at_unix,omitempty"`
	PremiumWindowTouched bool   `json:"premium_window_touched,omitempty"`
	EverPremiumTouched   bool   `json:"ever_premium_touched,omitempty"`
	PremiumDepleted      bool   `json:"premium_depleted,omitempty"`
}

type SchedulerSimulationRequest struct {
	AtMS       int64  `json:"at_ms"`
	Model      string `json:"model"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

type SchedulerSimulationReport struct {
	Results []SchedulerSimulationResult `json:"results"`
}

type SchedulerSimulationResult struct {
	Name                          string              `json:"name"`
	Config                        schedulerConfigView `json:"config"`
	PremiumRequests               int                 `json:"premium_requests"`
	UnlimitedRequests             int                 `json:"unlimited_requests"`
	PremiumWaitP50MS              int64               `json:"premium_wait_p50_ms"`
	PremiumWaitP95MS              int64               `json:"premium_wait_p95_ms"`
	Premium429Count               int                 `json:"premium_429_count"`
	UnlimitedStarvationCount      int                 `json:"unlimited_starvation_count"`
	UnlimitedBorrowCount          int                 `json:"unlimited_borrow_count"`
	BorrowedUnlimitedReclaimCount int                 `json:"borrowed_unlimited_reclaim_count"`
	FreshPremiumAccountsPreserved int                 `json:"fresh_premium_accounts_preserved"`
	AccountPremiumQuotaBurn       map[string]int      `json:"account_premium_quota_burn"`
}

type simAccount struct {
	def            SchedulerSimulationAccount
	remaining      int
	remainingKnown bool
	premiumLimit   int
	premiumStarts  int
	initialFresh   bool
}

type simSession struct {
	id             string
	accountID      string
	model          string
	expiresAtMS    int64
	maxConcurrency int
	busyUntilMS    []int64
}

func RunSchedulerSimulation(ctx context.Context, input SchedulerSimulationInput) (SchedulerSimulationReport, error) {
	if len(input.Accounts) == 0 {
		return SchedulerSimulationReport{}, fmt.Errorf("freebuff: simulation requires at least one account")
	}
	if len(input.Requests) == 0 {
		return SchedulerSimulationReport{}, fmt.Errorf("freebuff: simulation requires at least one request")
	}
	configs := input.Configs
	if len(configs) == 0 {
		configs = []SchedulerSimulationConfig{{Name: "default"}}
	}
	results := make([]SchedulerSimulationResult, 0, len(configs))
	for _, cfgSpec := range configs {
		result, err := runSchedulerSimulationConfig(ctx, input, cfgSpec)
		if err != nil {
			return SchedulerSimulationReport{}, err
		}
		results = append(results, result)
	}
	return SchedulerSimulationReport{Results: results}, nil
}

func DefaultSchedulerSimulationInput() SchedulerSimulationInput {
	remainingTwo := 2
	remainingFive := 5
	return SchedulerSimulationInput{
		SessionTTLMS:   int64(time.Hour / time.Millisecond),
		MaxConcurrency: 2,
		Configs: []SchedulerSimulationConfig{
			{
				Name:                        "balanced",
				PremiumCoreRatio:            0.35,
				PremiumMaxRatio:             0.70,
				UnlimitedReserveRatio:       0.25,
				UnlimitedMinReserveAccounts: 1,
				PremiumBurstQueueThreshold:  2,
				PremiumQueueTimeoutMS:       1500,
			},
			{
				Name:                        "aggressive",
				PremiumCoreRatio:            0.50,
				PremiumMaxRatio:             0.80,
				UnlimitedReserveRatio:       0.20,
				UnlimitedMinReserveAccounts: 1,
				PremiumBurstQueueThreshold:  2,
				PremiumQueueTimeoutMS:       1200,
			},
		},
		Accounts: []SchedulerSimulationAccount{
			{ID: "acc-touched", Name: "premium-touched", Active: true, Priority: 100, PremiumLimit: 5, PremiumRemaining: &remainingTwo, PremiumWindowTouched: true, EverPremiumTouched: true, PremiumResetAtUnix: 4102444800},
			{ID: "acc-fresh-1", Name: "premium-fresh-1", Active: true, Priority: 95, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-fresh-2", Name: "premium-fresh-2", Active: true, Priority: 90, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-fresh-3", Name: "premium-fresh-3", Active: true, Priority: 85, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-fresh-4", Name: "premium-fresh-4", Active: true, Priority: 80, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-unlimited", Name: "unlimited-fallback", Active: true, Priority: 70},
		},
		Requests: []SchedulerSimulationRequest{
			{AtMS: 0, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 100, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 180, Model: "deepseek/deepseek-v4-flash", DurationMS: 200},
			{AtMS: 240, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 340, Model: "moonshotai/kimi-k2.6", DurationMS: 900},
			{AtMS: 430, Model: "deepseek/deepseek-v4-flash", DurationMS: 200},
			{AtMS: 520, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 620, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 760, Model: "deepseek/deepseek-v4-flash", DurationMS: 200},
			{AtMS: 860, Model: "moonshotai/kimi-k2.6", DurationMS: 900},
			{AtMS: 980, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
			{AtMS: 1100, Model: "deepseek/deepseek-v4-pro", DurationMS: 900},
		},
	}
}

func runSchedulerSimulationConfig(ctx context.Context, input SchedulerSimulationInput, cfgSpec SchedulerSimulationConfig) (SchedulerSimulationResult, error) {
	cfg := simulationConfig(cfgSpec)
	scheduler := newPremiumScheduler(cfg)
	accounts := simulationAccounts(input.Accounts)
	seedSimulationHistory(scheduler, accounts)
	requests := append([]SchedulerSimulationRequest(nil), input.Requests...)
	sort.SliceStable(requests, func(i, j int) bool { return requests[i].AtMS < requests[j].AtMS })

	ttlMS := input.SessionTTLMS
	if ttlMS <= 0 {
		ttlMS = int64(time.Hour / time.Millisecond)
	}
	maxConcurrency := input.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	sessions := make([]simSession, 0)
	premiumWaits := make([]int64, 0)
	result := SchedulerSimulationResult{
		Name:                    cfgSpec.Name,
		Config:                  schedulerConfigFromConfig(cfg),
		AccountPremiumQuotaBurn: make(map[string]int),
	}
	if result.Name == "" {
		result.Name = "default"
	}

	for idx, req := range requests {
		if err := ctx.Err(); err != nil {
			return SchedulerSimulationResult{}, err
		}
		nowMS := req.AtMS
		model := CanonicalModel(req.Model)
		if model == "" {
			return SchedulerSimulationResult{}, fmt.Errorf("freebuff: simulation request %d missing model", idx)
		}
		durationMS := req.DurationMS
		if durationMS <= 0 {
			durationMS = 1000
		}
		cleanupSimulationSessions(&sessions, nowMS)
		group, _ := quotaGroupForModel(model)
		premium := group == QuotaGroupPremiumShared
		if premium {
			result.PremiumRequests++
			result.BorrowedUnlimitedReclaimCount += reclaimIdleUnlimitedSessions(&sessions, nowMS)
		} else {
			result.UnlimitedRequests++
		}

		if assignSimulationSession(&sessions, model, nowMS, durationMS) {
			continue
		}

		decision, err := scheduler.Schedule(ctx, channels.SessionScheduleRequest{
			ChannelID:    ID,
			SelectionKey: keyPrefix + model,
			Accounts:     simulationCandidates(accounts, sessions),
			Sessions:     simulationSessionCandidates(sessions, nowMS),
			Now:          time.Unix(0, nowMS*int64(time.Millisecond)),
		})
		if err != nil {
			return SchedulerSimulationResult{}, err
		}
		if decision.Finish != nil {
			decision.Finish()
		}
		switch simulationScheduleAction(decision.Action) {
		case channels.SessionScheduleWait:
			waitMS := decision.WaitTimeout.Milliseconds()
			if waitMS < 0 {
				waitMS = 0
			}
			if premium {
				premiumWaits = append(premiumWaits, waitMS)
			}
			if assignAfterWait(&sessions, model, nowMS, waitMS, durationMS) {
				continue
			}
			if premium {
				result.Premium429Count++
			} else {
				result.UnlimitedStarvationCount++
			}
		case channels.SessionScheduleReject:
			if premium {
				result.Premium429Count++
			} else {
				result.UnlimitedStarvationCount++
			}
		default:
			account := chooseSimulationAccount(accounts, sessions, decision.PreferredAccountIDs, premium)
			if account == nil {
				if premium {
					result.Premium429Count++
				} else {
					result.UnlimitedStarvationCount++
				}
				continue
			}
			if premium {
				account.remaining--
				account.premiumStarts++
				result.AccountPremiumQuotaBurn[account.def.ID]++
			} else if account.remainingKnown && account.remaining > 0 {
				result.UnlimitedBorrowCount++
			}
			sessionID := fmt.Sprintf("sim-%d", len(sessions)+1)
			sessions = append(sessions, simSession{
				id:             sessionID,
				accountID:      account.def.ID,
				model:          model,
				expiresAtMS:    nowMS + ttlMS,
				maxConcurrency: maxConcurrency,
				busyUntilMS:    []int64{nowMS + durationMS},
			})
			scheduler.observeSession(account.def.ID, model, simulationUpstreamSession(model, account), channels.State{})
		}
	}

	result.PremiumWaitP50MS = percentileMS(premiumWaits, 0.50)
	result.PremiumWaitP95MS = percentileMS(premiumWaits, 0.95)
	for _, account := range accounts {
		if account.initialFresh && account.premiumStarts == 0 {
			result.FreshPremiumAccountsPreserved++
		}
	}
	return result, nil
}

func simulationConfig(spec SchedulerSimulationConfig) SchedulerConfig {
	cfg := defaultSchedulerConfig()
	if spec.PremiumCoreRatio > 0 {
		cfg.PremiumCoreRatio = spec.PremiumCoreRatio
	}
	if spec.PremiumMaxRatio > 0 {
		cfg.PremiumMaxRatio = spec.PremiumMaxRatio
	}
	if spec.UnlimitedReserveRatio > 0 {
		cfg.UnlimitedReserveRatio = spec.UnlimitedReserveRatio
	}
	if spec.UnlimitedMinReserveAccounts > 0 {
		cfg.UnlimitedMinReserveAccounts = spec.UnlimitedMinReserveAccounts
	}
	if spec.PremiumBurstQueueThreshold > 0 {
		cfg.PremiumBurstQueueThreshold = spec.PremiumBurstQueueThreshold
	}
	if spec.PremiumQueueTimeoutMS > 0 {
		cfg.PremiumQueueTimeout = time.Duration(spec.PremiumQueueTimeoutMS) * time.Millisecond
	}
	if spec.ModelMaxAccountRatio > 0 {
		cfg.ModelMaxAccountRatio = spec.ModelMaxAccountRatio
	}
	if spec.ModelBurstAccountRatio > 0 {
		cfg.ModelBurstAccountRatio = spec.ModelBurstAccountRatio
	}
	return normalizeSchedulerConfig(cfg)
}

func schedulerConfigFromConfig(cfg SchedulerConfig) schedulerConfigView {
	cfg = normalizeSchedulerConfig(cfg)
	return schedulerConfigView{
		PremiumCoreRatio:            cfg.PremiumCoreRatio,
		PremiumMaxRatio:             cfg.PremiumMaxRatio,
		UnlimitedReserveRatio:       cfg.UnlimitedReserveRatio,
		UnlimitedMinReserveAccounts: cfg.UnlimitedMinReserveAccounts,
		PremiumBurstQueueThreshold:  cfg.PremiumBurstQueueThreshold,
		PremiumQueueTimeoutMS:       cfg.PremiumQueueTimeout.Milliseconds(),
		ModelMaxAccountRatio:        cfg.ModelMaxAccountRatio,
		ModelBurstAccountRatio:      cfg.ModelBurstAccountRatio,
	}
}

func simulationScheduleAction(action channels.SessionScheduleAction) channels.SessionScheduleAction {
	if action == "" {
		return channels.SessionScheduleCreate
	}
	return action
}

func simulationAccounts(defs []SchedulerSimulationAccount) []*simAccount {
	out := make([]*simAccount, 0, len(defs))
	for _, def := range defs {
		if def.Priority == 0 {
			def.Priority = 100
		}
		acc := &simAccount{def: def, premiumLimit: def.PremiumLimit}
		if def.PremiumRemaining != nil {
			acc.remainingKnown = true
			acc.remaining = *def.PremiumRemaining
			if acc.premiumLimit < acc.remaining {
				acc.premiumLimit = acc.remaining
			}
			if acc.premiumLimit <= 0 {
				acc.premiumLimit = maxInt(acc.remaining, 5)
			}
		}
		acc.initialFresh = acc.remainingKnown && acc.remaining > 0 && !def.PremiumWindowTouched && !def.EverPremiumTouched
		out = append(out, acc)
	}
	return out
}

func seedSimulationHistory(s *premiumScheduler, accounts []*simAccount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range accounts {
		if !account.remainingKnown && !account.def.PremiumWindowTouched && !account.def.EverPremiumTouched && !account.def.PremiumDepleted {
			continue
		}
		h := premiumAccountHistory{
			CurrentWindowTouched: account.def.PremiumWindowTouched,
			EverPremiumTouched:   account.def.EverPremiumTouched || account.def.PremiumWindowTouched,
			WindowResetUnix:      account.def.PremiumResetAtUnix,
			Remaining:            account.remaining,
			RemainingKnown:       account.remainingKnown,
			Depleted:             account.def.PremiumDepleted || (account.remainingKnown && account.remaining <= 0),
		}
		s.history[account.def.ID] = h
	}
}

func simulationCandidates(accounts []*simAccount, sessions []simSession) []channels.AccountCandidate {
	counts := map[string]int{}
	for _, session := range sessions {
		counts[session.accountID]++
	}
	out := make([]channels.AccountCandidate, 0, len(accounts))
	for _, account := range accounts {
		active := account.def.Active
		out = append(out, channels.AccountCandidate{
			Account: channels.Account{
				ID:        account.def.ID,
				ChannelID: ID,
				Name:      account.def.Name,
			},
			Priority:       account.def.Priority,
			Active:         active,
			Eligible:       active && counts[account.def.ID] == 0,
			BlockedReason:  blockedReason(active, counts[account.def.ID]),
			SessionCount:   counts[account.def.ID],
			MaxSessions:    1,
			QuotaAvailable: true,
		})
	}
	return out
}

func blockedReason(active bool, sessionCount int) string {
	if !active {
		return "account_inactive"
	}
	if sessionCount > 0 {
		return "max_sessions_reached"
	}
	return ""
}

func simulationSessionCandidates(sessions []simSession, nowMS int64) []channels.SessionCandidate {
	out := make([]channels.SessionCandidate, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, channels.SessionCandidate{
			ID:             session.id,
			ChannelID:      ID,
			AccountID:      session.accountID,
			Key:            keyPrefix + session.model,
			State:          channels.State{stateModel: session.model},
			ExpiresAtUnix:  time.Unix(0, session.expiresAtMS*int64(time.Millisecond)).Unix(),
			InFlight:       len(activeBusyUntil(session.busyUntilMS, nowMS)),
			MaxConcurrency: session.maxConcurrency,
			Healthy:        true,
			WaitOnFull:     true,
		})
	}
	return out
}

func cleanupSimulationSessions(sessions *[]simSession, nowMS int64) {
	kept := (*sessions)[:0]
	for _, session := range *sessions {
		if session.expiresAtMS <= nowMS {
			continue
		}
		session.busyUntilMS = activeBusyUntil(session.busyUntilMS, nowMS)
		kept = append(kept, session)
	}
	*sessions = kept
}

func activeBusyUntil(values []int64, nowMS int64) []int64 {
	out := values[:0]
	for _, value := range values {
		if value > nowMS {
			out = append(out, value)
		}
	}
	return out
}

func assignSimulationSession(sessions *[]simSession, model string, nowMS, durationMS int64) bool {
	for i := range *sessions {
		session := &(*sessions)[i]
		if session.model != model || session.expiresAtMS <= nowMS {
			continue
		}
		session.busyUntilMS = activeBusyUntil(session.busyUntilMS, nowMS)
		if len(session.busyUntilMS) >= session.maxConcurrency {
			continue
		}
		session.busyUntilMS = append(session.busyUntilMS, nowMS+durationMS)
		return true
	}
	return false
}

func assignAfterWait(sessions *[]simSession, model string, nowMS, waitMS, durationMS int64) bool {
	deadline := nowMS + waitMS
	next := int64(math.MaxInt64)
	for _, session := range *sessions {
		if session.model != model || len(session.busyUntilMS) == 0 {
			continue
		}
		for _, busyUntil := range session.busyUntilMS {
			if busyUntil > nowMS && busyUntil < next {
				next = busyUntil
			}
		}
	}
	if next == int64(math.MaxInt64) || next > deadline {
		return false
	}
	cleanupSimulationSessions(sessions, next)
	return assignSimulationSession(sessions, model, next, durationMS)
}

func reclaimIdleUnlimitedSessions(sessions *[]simSession, nowMS int64) int {
	reclaimed := 0
	kept := (*sessions)[:0]
	for _, session := range *sessions {
		session.busyUntilMS = activeBusyUntil(session.busyUntilMS, nowMS)
		group, _ := quotaGroupForModel(session.model)
		if group != QuotaGroupPremiumShared && len(session.busyUntilMS) == 0 {
			reclaimed++
			continue
		}
		kept = append(kept, session)
	}
	*sessions = kept
	return reclaimed
}

func chooseSimulationAccount(accounts []*simAccount, sessions []simSession, preferred []string, premium bool) *simAccount {
	byID := make(map[string]*simAccount, len(accounts))
	activeSessions := map[string]struct{}{}
	for _, account := range accounts {
		byID[account.def.ID] = account
	}
	for _, session := range sessions {
		activeSessions[session.accountID] = struct{}{}
	}
	for _, id := range preferred {
		account := byID[id]
		if account == nil || !account.def.Active {
			continue
		}
		if _, busy := activeSessions[id]; busy {
			continue
		}
		if premium && account.remainingKnown && account.remaining <= 0 {
			continue
		}
		return account
	}
	return nil
}

func simulationUpstreamSession(model string, account *simAccount) upstreamSession {
	session := upstreamSession{
		Status:     "active",
		InstanceID: "sim-" + account.def.ID,
		Model:      model,
	}
	if account.remainingKnown && account.premiumLimit > 0 {
		recent := account.premiumLimit - account.remaining
		if recent < 0 {
			recent = 0
		}
		session.RateLimitsByModel = map[string]upstreamRateLimit{
			model: {
				Model:       model,
				Limit:       account.premiumLimit,
				RecentCount: float64(recent),
				ResetAt:     time.Unix(account.def.PremiumResetAtUnix, 0).UTC().Format(time.RFC3339Nano),
			},
		}
	}
	return session
}

func percentileMS(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
