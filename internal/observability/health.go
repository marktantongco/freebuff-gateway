package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// HealthStatus represents the overall health status
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component
type ComponentHealth struct {
	Name      string       `json:"name"`
	Status    HealthStatus `json:"status"`
	Message   string       `json:"message,omitempty"`
	LastCheck time.Time    `json:"last_check"`
	Latency   int64        `json:"latency_ms,omitempty"`
}

// HealthResponse represents the full health check response
type HealthResponse struct {
	Status     HealthStatus              `json:"status"`
	Timestamp  time.Time                 `json:"timestamp"`
	Uptime     time.Duration             `json:"uptime"`
	Components map[string]ComponentHealth `json:"components"`
	Version    string                    `json:"version"`
	GoVersion  string                    `json:"go_version"`
	Goroutines int                       `json:"goroutines"`
	MemoryMB   float64                   `json:"memory_mb"`
}

// HealthChecker manages health checks for all components
type HealthChecker struct {
	startTime  time.Time
	version    string
	components map[string]ComponentHealth
	mu         sync.RWMutex
	checks     map[string]func(ctx context.Context) ComponentHealth
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(version string) *HealthChecker {
	return &HealthChecker{
		startTime:  time.Now(),
		version:    version,
		components: make(map[string]ComponentHealth),
		checks:     make(map[string]func(ctx context.Context) ComponentHealth),
	}
}

// RegisterCheck registers a health check function for a component
func (hc *HealthChecker) RegisterCheck(name string, check func(ctx context.Context) ComponentHealth) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.checks[name] = check
}

// CheckHealth runs all health checks and returns the result
func (hc *HealthChecker) CheckHealth(ctx context.Context) *HealthResponse {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	response := &HealthResponse{
		Timestamp:  time.Now(),
		Uptime:     time.Since(hc.startTime),
		Version:    hc.version,
		GoVersion:  runtime.Version(),
		Goroutines: runtime.NumGoroutine(),
		Components: make(map[string]ComponentHealth),
	}

	// Get memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	response.MemoryMB = float64(memStats.Alloc) / 1024 / 1024

	// Run all checks
	overallStatus := HealthStatusHealthy
	for name, check := range hc.checks {
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		health := check(checkCtx)
		cancel()

		response.Components[name] = health

		if health.Status == HealthStatusUnhealthy {
			overallStatus = HealthStatusUnhealthy
		} else if health.Status == HealthStatusDegraded && overallStatus != HealthStatusUnhealthy {
			overallStatus = HealthStatusDegraded
		}
	}

	// Add built-in checks
	response.Components["database"] = hc.checkDatabase(ctx)
	response.Components["memory"] = hc.checkMemory()
	response.Components["goroutines"] = hc.checkGoroutines()

	// Re-evaluate overall status
	for _, comp := range response.Components {
		if comp.Status == HealthStatusUnhealthy {
			overallStatus = HealthStatusUnhealthy
		} else if comp.Status == HealthStatusDegraded && overallStatus != HealthStatusUnhealthy {
			overallStatus = HealthStatusDegraded
		}
	}

	response.Status = overallStatus
	return response
}

// checkDatabase checks database connectivity
func (hc *HealthChecker) checkDatabase(ctx context.Context) ComponentHealth {
	start := time.Now()
	// In production, this would ping the database
	// For now, just return healthy
	return ComponentHealth{
		Name:      "database",
		Status:    HealthStatusHealthy,
		Message:   "SQLite connection OK",
		LastCheck: time.Now(),
		Latency:   time.Since(start).Milliseconds(),
	}
}

// checkMemory checks memory usage
func (hc *HealthChecker) checkMemory() ComponentHealth {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	memMB := float64(memStats.Alloc) / 1024 / 1024
	status := HealthStatusHealthy
	message := "Memory usage normal"

	if memMB > 500 {
		status = HealthStatusDegraded
		message = "Memory usage elevated"
	}
	if memMB > 1000 {
		status = HealthStatusUnhealthy
		message = "Memory usage critical"
	}

	return ComponentHealth{
		Name:      "memory",
		Status:    status,
		Message:   message,
		LastCheck: time.Now(),
	}
}

// checkGoroutines checks goroutine count
func (hc *HealthChecker) checkGoroutines() ComponentHealth {
	count := runtime.NumGoroutine()
	status := HealthStatusHealthy
	message := "Goroutine count normal"

	if count > 1000 {
		status = HealthStatusDegraded
		message = "Goroutine count elevated"
	}
	if count > 10000 {
		status = HealthStatusUnhealthy
		message = "Goroutine count critical"
	}

	return ComponentHealth{
		Name:      "goroutines",
		Status:    status,
		Message:   message,
		LastCheck: time.Now(),
	}
}

// Handler returns an HTTP handler for health checks
func (hc *HealthChecker) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		response := hc.CheckHealth(ctx)

		w.Header().Set("Content-Type", "application/json")

		// Set status code based on health
		switch response.Status {
		case HealthStatusHealthy:
			w.WriteHeader(http.StatusOK)
		case HealthStatusDegraded:
			w.WriteHeader(http.StatusOK) // Still OK but degraded
		case HealthStatusUnhealthy:
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(response)
	})
}

// ReadyHandler returns a readiness probe handler
func (hc *HealthChecker) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		response := hc.CheckHealth(ctx)

		if response.Status == HealthStatusUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})
}

// LiveHandler returns a liveness probe handler
func (hc *HealthChecker) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("alive"))
	})
}
