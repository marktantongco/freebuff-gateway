package accounts

import (
	"errors"
	"sort"
	"sync"
	"time"

	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/quota"
)

var (
	ErrNoEligibleAccount = errors.New("accounts: no eligible account")
	ErrQuotaExceeded     = errors.New("accounts: quota exceeded")
)

type runtime struct {
	sessionCount        int
	cooldownUntil       time.Time
	consecutiveFailures int
	lastUsedAt          time.Time
}

type Pool struct {
	mu       sync.Mutex
	repo     *Repo
	runtimes map[string]*runtime
}

func NewPool(repo *Repo) *Pool {
	return &Pool{repo: repo, runtimes: make(map[string]*runtime)}
}

func (p *Pool) Repo() *Repo { return p.repo }

func (p *Pool) ReserveSlot(channelID string, maxPerAccount int) (channels.Account, func(), error) {
	return p.ReserveSlotExcluding(channelID, maxPerAccount, nil)
}

func (p *Pool) ReserveSlotExcluding(channelID string, maxPerAccount int, excluded map[string]struct{}) (channels.Account, func(), error) {
	return p.ReserveSlotExcludingPreferred(channelID, maxPerAccount, excluded, nil)
}

func (p *Pool) ReserveSlotExcludingPreferred(channelID string, maxPerAccount int, excluded map[string]struct{}, preferredAccountIDs []string) (channels.Account, func(), error) {
	records, err := p.repo.ListByChannel(channelID)
	if err != nil {
		return channels.Account{}, nil, err
	}
	if len(records) == 0 {
		return channels.Account{}, nil, ErrNoEligibleAccount
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	eligible := make([]slotCandidate, 0, len(records))
	quotaBlocked := 0
	otherBlocked := 0
	for _, r := range records {
		candidate, blockedReason, err := p.slotCandidateLocked(r, maxPerAccount, excluded, now)
		if err != nil {
			return channels.Account{}, nil, err
		}
		if candidate.view.Eligible {
			eligible = append(eligible, candidate)
			continue
		}
		switch blockedReason {
		case "quota_exceeded":
			quotaBlocked++
		default:
			otherBlocked++
		}
	}
	if len(eligible) == 0 {
		if quotaBlocked > 0 && otherBlocked == 0 {
			return channels.Account{}, nil, ErrQuotaExceeded
		}
		return channels.Account{}, nil, ErrNoEligibleAccount
	}

	sortSlotCandidates(eligible, preferredAccountIDs)

	chosen := eligible[0].record
	rt := p.runtimes[chosen.ID]
	rt.sessionCount++
	rt.lastUsedAt = now

	released := false
	release := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if released {
			return
		}
		released = true
		if r := p.runtimes[chosen.ID]; r != nil && r.sessionCount > 0 {
			r.sessionCount--
		}
	}
	return chosen.ToChannel(), release, nil
}

func (p *Pool) ReserveAccountSlot(channelID, accountID string, maxPerAccount int) (channels.Account, func(), error) {
	if accountID == "" {
		return channels.Account{}, nil, ErrNoEligibleAccount
	}
	rec, err := p.repo.Get(accountID)
	if err != nil {
		return channels.Account{}, nil, err
	}
	if rec.ChannelID != channelID {
		return channels.Account{}, nil, ErrNoEligibleAccount
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	candidate, blockedReason, err := p.slotCandidateLocked(rec, maxPerAccount, nil, time.Now())
	if err != nil {
		return channels.Account{}, nil, err
	}
	if !candidate.view.Eligible {
		if blockedReason == "quota_exceeded" {
			return channels.Account{}, nil, ErrQuotaExceeded
		}
		return channels.Account{}, nil, ErrNoEligibleAccount
	}

	rt := p.runtimes[rec.ID]
	rt.sessionCount++
	rt.lastUsedAt = time.Now()

	released := false
	release := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if released {
			return
		}
		released = true
		if r := p.runtimes[rec.ID]; r != nil && r.sessionCount > 0 {
			r.sessionCount--
		}
	}
	return rec.ToChannel(), release, nil
}

func (p *Pool) SlotCandidates(channelID string, maxPerAccount int, excluded map[string]struct{}) ([]channels.AccountCandidate, error) {
	records, err := p.repo.ListByChannel(channelID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	out := make([]channels.AccountCandidate, 0, len(records))
	for _, r := range records {
		candidate, _, err := p.slotCandidateLocked(r, maxPerAccount, excluded, now)
		if err != nil {
			return nil, err
		}
		out = append(out, candidate.view)
	}
	return out, nil
}

func (p *Pool) EnsureQuotaAvailable(accountID string) error {
	if accountID == "" {
		return nil
	}
	rec, err := p.repo.Get(accountID)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ensureQuotaAvailableLocked(rec, time.Now())
}

func (p *Pool) MarkResult(accountID string, class channels.ResponseClass, quotaDeltas ...int64) {
	if accountID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rt := p.touch(accountID)
	switch class {
	case channels.ClassOk:
		rt.consecutiveFailures = 0
		by := int64(1)
		if len(quotaDeltas) > 0 && quotaDeltas[0] > 0 {
			by = quotaDeltas[0]
		}
		_ = p.repo.IncrementQuotaUsed(accountID, by)
	case channels.ClassRateLimited:
		rt.consecutiveFailures++
		rt.cooldownUntil = time.Now().Add(60 * time.Second)
	case channels.ClassAuthExpired:
		rt.consecutiveFailures++
		rt.cooldownUntil = time.Now().Add(5 * time.Minute)
	case channels.ClassFatal:
		rt.consecutiveFailures++
		if rt.consecutiveFailures >= 3 {
			rt.cooldownUntil = time.Now().Add(5 * time.Minute)
		}
	case channels.ClassRetryable:
		rt.consecutiveFailures++
	}
}

type Snapshot struct {
	AccountID           string `json:"account_id"`
	SessionCount        int    `json:"session_count"`
	CooldownUntilUnix   int64  `json:"cooldown_until_unix"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	LastUsedAtUnix      int64  `json:"last_used_at_unix"`
	OnCooldown          bool   `json:"on_cooldown"`
}

func (p *Pool) Snapshot() []Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	out := make([]Snapshot, 0, len(p.runtimes))
	for id, rt := range p.runtimes {
		out = append(out, Snapshot{
			AccountID:           id,
			SessionCount:        rt.sessionCount,
			CooldownUntilUnix:   unixOrZero(rt.cooldownUntil),
			ConsecutiveFailures: rt.consecutiveFailures,
			LastUsedAtUnix:      unixOrZero(rt.lastUsedAt),
			OnCooldown:          now.Before(rt.cooldownUntil),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccountID < out[j].AccountID })
	return out
}

func (p *Pool) touch(accountID string) *runtime {
	rt, ok := p.runtimes[accountID]
	if !ok {
		rt = &runtime{}
		p.runtimes[accountID] = rt
	}
	return rt
}

func (p *Pool) ensureQuotaAvailableLocked(rec *Record, now time.Time) error {
	if rec.QuotaTotal <= 0 {
		return nil
	}
	if quota.NeedsRollover(rec.QuotaPeriodStart, rec.QuotaPeriod, now) {
		start := quota.BucketStart(now, rec.QuotaPeriod)
		if err := p.repo.RollQuota(rec.ID, start); err != nil {
			return err
		}
		rec.QuotaUsed = 0
		rec.QuotaPeriodStart = start
	}
	if rec.QuotaUsed >= rec.QuotaTotal {
		return ErrQuotaExceeded
	}
	return nil
}

type slotCandidate struct {
	record *Record
	view   channels.AccountCandidate
}

func (p *Pool) slotCandidateLocked(rec *Record, maxPerAccount int, excluded map[string]struct{}, now time.Time) (slotCandidate, string, error) {
	if maxPerAccount < 1 {
		maxPerAccount = 1
	}
	rt := p.touch(rec.ID)
	view := channels.AccountCandidate{
		Account:        rec.ToChannel(),
		Priority:       rec.Priority,
		Active:         rec.IsActive,
		SessionCount:   rt.sessionCount,
		MaxSessions:    maxPerAccount,
		LastUsedAtUnix: unixOrZero(rt.lastUsedAt),
	}
	if _, skip := excluded[rec.ID]; skip {
		view.BlockedReason = "excluded"
		return slotCandidate{record: rec, view: view}, view.BlockedReason, nil
	}
	if !rec.IsActive {
		view.BlockedReason = "inactive"
		return slotCandidate{record: rec, view: view}, view.BlockedReason, nil
	}
	if rt.sessionCount >= maxPerAccount {
		view.BlockedReason = "session_limit"
		return slotCandidate{record: rec, view: view}, view.BlockedReason, nil
	}
	if now.Before(rt.cooldownUntil) {
		view.OnCooldown = true
		view.BlockedReason = "cooldown"
		return slotCandidate{record: rec, view: view}, view.BlockedReason, nil
	}
	if err := p.ensureQuotaAvailableLocked(rec, now); err != nil {
		if errors.Is(err, ErrQuotaExceeded) {
			view.BlockedReason = "quota_exceeded"
			return slotCandidate{record: rec, view: view}, view.BlockedReason, nil
		}
		return slotCandidate{}, "", err
	}
	view.Eligible = true
	view.QuotaAvailable = true
	return slotCandidate{record: rec, view: view}, "", nil
}

func sortSlotCandidates(candidates []slotCandidate, preferredAccountIDs []string) {
	preference := map[string]int{}
	for i, id := range preferredAccountIDs {
		if _, exists := preference[id]; !exists {
			preference[id] = i
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i].record, candidates[j].record
		ai, aPreferred := preference[a.ID]
		bi, bPreferred := preference[b.ID]
		if aPreferred != bPreferred {
			return aPreferred
		}
		if aPreferred && ai != bi {
			return ai < bi
		}
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return candidates[i].view.LastUsedAtUnix < candidates[j].view.LastUsedAtUnix
	})
}

func IsQuotaPeriod(period string) bool {
	return quota.IsPeriod(period)
}

func QuotaBucketStart(now time.Time, period string) int64 {
	return quota.BucketStart(now, period)
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
