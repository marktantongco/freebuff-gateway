package freebuff

import (
	"context"
	"testing"
)

func TestRunSchedulerSimulationComparesConfigurations(t *testing.T) {
	remainingTwo := 2
	remainingFive := 5
	report, err := RunSchedulerSimulation(context.Background(), SchedulerSimulationInput{
		SessionTTLMS:   60000,
		MaxConcurrency: 1,
		Configs: []SchedulerSimulationConfig{
			{
				Name:                       "conservative",
				PremiumCoreRatio:           0.25,
				PremiumMaxRatio:            0.50,
				UnlimitedReserveRatio:      0.25,
				PremiumBurstQueueThreshold: 3,
				PremiumQueueTimeoutMS:      50,
			},
			{
				Name:                       "balanced",
				PremiumCoreRatio:           0.50,
				PremiumMaxRatio:            0.75,
				UnlimitedReserveRatio:      0.25,
				PremiumBurstQueueThreshold: 1,
				PremiumQueueTimeoutMS:      50,
			},
		},
		Accounts: []SchedulerSimulationAccount{
			{ID: "acc-1", Active: true, Priority: 100, PremiumLimit: 5, PremiumRemaining: &remainingTwo, PremiumWindowTouched: true, EverPremiumTouched: true, PremiumResetAtUnix: 4102444800},
			{ID: "acc-2", Active: true, Priority: 90, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-3", Active: true, Priority: 80, PremiumLimit: 5, PremiumRemaining: &remainingFive, PremiumResetAtUnix: 4102444800},
			{ID: "acc-4", Active: true, Priority: 70},
		},
		Requests: []SchedulerSimulationRequest{
			{AtMS: 0, Model: "deepseek/deepseek-v4-pro", DurationMS: 100},
			{AtMS: 1, Model: "deepseek/deepseek-v4-pro", DurationMS: 100},
			{AtMS: 2, Model: "deepseek/deepseek-v4-pro", DurationMS: 100},
			{AtMS: 3, Model: "deepseek/deepseek-v4-flash", DurationMS: 20},
			{AtMS: 4, Model: "deepseek/deepseek-v4-pro", DurationMS: 100},
			{AtMS: 5, Model: "deepseek/deepseek-v4-flash", DurationMS: 20},
		},
	})
	if err != nil {
		t.Fatalf("run simulation: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("results = %+v, want two configurations", report.Results)
	}
	conservative := report.Results[0]
	balanced := report.Results[1]
	if conservative.Name != "conservative" || balanced.Name != "balanced" {
		t.Fatalf("result names = %+v", report.Results)
	}
	if balanced.Premium429Count >= conservative.Premium429Count {
		t.Fatalf("premium 429 balanced=%d conservative=%d, want balanced lower", balanced.Premium429Count, conservative.Premium429Count)
	}
	if conservative.PremiumWaitP95MS == 0 {
		t.Fatalf("conservative wait p95 = 0, want queued premium pressure")
	}
	if len(balanced.AccountPremiumQuotaBurn) == 0 {
		t.Fatalf("balanced quota burn empty")
	}
}

func TestRunSchedulerSimulationRequiresInputs(t *testing.T) {
	if _, err := RunSchedulerSimulation(context.Background(), SchedulerSimulationInput{}); err == nil {
		t.Fatalf("expected input validation error")
	}
}

func TestDefaultSchedulerSimulationInputRuns(t *testing.T) {
	report, err := RunSchedulerSimulation(context.Background(), DefaultSchedulerSimulationInput())
	if err != nil {
		t.Fatalf("run default simulation: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("results = %+v, want two default variants", report.Results)
	}
	if report.Results[0].Name != "balanced" || report.Results[1].Name != "aggressive" {
		t.Fatalf("default variant names = %+v", report.Results)
	}
}
