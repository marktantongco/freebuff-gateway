package pool

import (
	"kiro-go/config"
	"testing"
	"time"
)

func TestOverageAccountsAreSkippedByDefault(t *testing.T) {
	p := &AccountPool{}
	normal := config.Account{ID: "normal"}
	overLimit := config.Account{ID: "over", UsageCurrent: 10, UsageLimit: 10}

	p.accounts = []config.Account{normal, overLimit}

	for i := 0; i < 5; i++ {
		acc := p.GetNext()
		if acc == nil {
			t.Fatalf("expected an account")
		}
		if acc.ID == "over" {
			t.Fatalf("expected over-limit account to be skipped by default")
		}
	}
}

func TestOverageAccountsCanBeSelectedWhenAllowed(t *testing.T) {
	p := &AccountPool{}
	overLimit := config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		AllowOverage:  true,
		OverageWeight: 1,
	}

	p.accounts = []config.Account{overLimit}

	acc := p.GetNext()
	if acc == nil {
		t.Fatalf("expected allowed overage account")
	}
	if acc.ID != "over" {
		t.Fatalf("expected overage account, got %q", acc.ID)
	}
}

func TestOverageAccountsCanBeSelectedWhenUpstreamEnabled(t *testing.T) {
	p := &AccountPool{}
	overLimit := config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		OverageStatus: "ENABLED",
	}

	p.accounts = []config.Account{overLimit}

	acc := p.GetNext()
	if acc == nil {
		t.Fatalf("expected upstream overage account")
	}
	if acc.ID != "over" {
		t.Fatalf("expected overage account, got %q", acc.ID)
	}
}

func TestOverageWeightIsLowerThanNormalWeight(t *testing.T) {
	normalWeight := effectiveWeight(1) * overageFrequencyScale
	overageWeight := effectiveOverageWeight(1)

	if overageWeight >= normalWeight {
		t.Fatalf("expected overage weight %d to be lower than normal weight %d", overageWeight, normalWeight)
	}
}

func TestGetNextReadyExcludingFallsBackToCooldownAccount(t *testing.T) {
	p := &AccountPool{
		accounts: []config.Account{
			{ID: "first"},
			{ID: "second"},
		},
		cooldowns: map[string]time.Time{
			"first":  time.Now().Add(time.Minute),
			"second": time.Now().Add(2 * time.Minute),
		},
	}

	acc := p.GetNextReadyExcluding(map[string]bool{"first": true})
	if acc == nil {
		t.Fatalf("expected cooldown fallback account")
	}
	if acc.ID != "second" {
		t.Fatalf("expected second account, got %q", acc.ID)
	}
}

func TestExpiredAccountsAreStillSelectableForRefresh(t *testing.T) {
	p := &AccountPool{
		accounts: []config.Account{
			{ID: "expired", ExpiresAt: time.Now().Add(-time.Hour).Unix()},
		},
	}

	acc := p.GetNextReadyExcluding(nil)
	if acc == nil {
		t.Fatalf("expected expired account to be selected for refresh")
	}
	if acc.ID != "expired" {
		t.Fatalf("expected expired account, got %q", acc.ID)
	}
}

func TestGetNextForModelExcludingUsesAccountModelCache(t *testing.T) {
	p := &AccountPool{
		accounts: []config.Account{
			{ID: "sonnet"},
			{ID: "opus"},
		},
	}
	p.SetModelList("sonnet", []string{"claude-sonnet-4.5"})
	p.SetModelList("opus", []string{"claude-opus-4.8"})

	acc := p.GetNextForModelExcluding("kr/claude-opus-4.8", nil)
	if acc == nil {
		t.Fatalf("expected account")
	}
	if acc.ID != "opus" {
		t.Fatalf("expected opus account, got %q", acc.ID)
	}

	acc = p.GetNextForModelExcluding("claude-opus-4.8", map[string]bool{"opus": true})
	if acc != nil {
		t.Fatalf("expected nil when only matching account is excluded, got %q", acc.ID)
	}
}
