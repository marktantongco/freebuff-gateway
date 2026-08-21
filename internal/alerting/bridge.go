package alerting

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/observability"
)

// Bridge connects the observability health checker to the alerting system.
type Bridge struct {
	healthChecker *observability.HealthChecker
	manager       *Manager
	interval      time.Duration
	running       bool
	cancel        context.CancelFunc
	mu            sync.Mutex
}

// NewBridge creates a bridge between health checking and alerting.
func NewBridge(hc *observability.HealthChecker, manager *Manager, interval time.Duration) *Bridge {
	return &Bridge{
		healthChecker: hc,
		manager:       manager,
		interval:      interval,
	}
}

// Start begins periodic health check → alerting loop.
func (b *Bridge) Start(ctx context.Context) {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return
	}
	b.running = true
	ctx, b.cancel = context.WithCancel(ctx)
	b.mu.Unlock()

	go b.run(ctx)
}

// Stop halts the loop.
func (b *Bridge) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	b.running = false
}

func (b *Bridge) run(ctx context.Context) {
	// Run immediately
	b.evaluate(ctx)

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.evaluate(ctx)
		}
	}
}

func (b *Bridge) evaluate(ctx context.Context) {
	// Run health checks
	health := b.healthChecker.CheckHealth(ctx)

	// Convert to alerting.ComponentHealth map
	components := make(map[string]ComponentHealth)
	for name, comp := range health.Components {
		components[name] = ComponentHealth{
			Name:      comp.Name,
			Status:    observabilityStatusToAlerting(comp.Status),
			Message:   comp.Message,
			LastCheck: comp.LastCheck,
			Latency:   comp.Latency,
		}
	}

	// Also add system-level checks
	components["runtime"] = checkRuntime()

	// Let the manager evaluate
	b.manager.Evaluate(ctx, components)

	// Also evaluate metric-based rules
	metrics := map[string]float64{
		"runtime/goroutines": float64(runtime.NumGoroutine()),
		"runtime/heap_mb":    heapMB(),
	}
	b.manager.EvaluateRules(ctx, metrics)
}

func checkRuntime() ComponentHealth {
	goroutines := runtime.NumGoroutine()
	status := observability.HealthStatusHealthy
	msg := "Runtime healthy"

	if goroutines > 5000 {
		status = observability.HealthStatusUnhealthy
		msg = "Goroutine count critical"
	} else if goroutines > 1000 {
		status = observability.HealthStatusDegraded
		msg = "Goroutine count elevated"
	}

	return ComponentHealth{
		Name:      "runtime",
		Status:    observabilityStatusToAlerting(status),
		Message:   msg,
		LastCheck: time.Now(),
	}
}

func heapMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

func observabilityStatusToAlerting(status observability.HealthStatus) HealthStatus {
	switch status {
	case observability.HealthStatusUnhealthy:
		return HealthStatusUnhealthy
	case observability.HealthStatusDegraded:
		return HealthStatusDegraded
	default:
		return HealthStatusHealthy
	}
}

// ComponentHealth mirrors observability.ComponentHealth for the alerting package.
type ComponentHealth struct {
	Name      string
	Status    HealthStatus
	Message   string
	LastCheck time.Time
	Latency   int64
}

// HealthStatus mirrors observability.HealthStatus for the alerting package.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)
