# Freebuff Gateway — Grafana Monitoring

Production-ready Grafana dashboards for the Freebuff Gateway.

## Quick Start

```bash
cd grafana
./start-monitoring.sh
```

Then open **http://localhost:3000** (login: `admin` / `freebuff`).

## Dashboards

### 1. HTTP Overview
**UID:** `freebuff-http-overview`

| Panel | Description |
|-------|-------------|
| Requests per Second | Total, 2xx, 4xx, 5xx rates |
| Active Requests | Currently in-flight |
| Response Latency | p50, p95, p99 percentiles |
| Average Latency | Mean request duration |
| Error Rate | 5xx percentage |
| Request/Response Size | Body size heatmaps |
| Requests by Method | POST, GET distribution |
| Status Code Breakdown | Per-status bar chart |

### 2. System Health
**UID:** `freebuff-system-health`

| Panel | Description |
|-------|-------------|
| Gateway Status | Up/Down indicator |
| Memory Usage | RSS and virtual memory |
| Goroutines | Active Go goroutines |
| CPU Usage | Process CPU percentage |
| File Descriptors | Open vs max FDs |
| Go GC Duration | Garbage collection timing |
| Heap Allocations | Heap in-use, allocated, idle |
| Network I/O | Receive and transmit rates |
| OS Threads | Go runtime threads |
| GC Cycles | GC frequency |
| Uptime | Time since process start |

### 3. Provider Metrics
**UID:** `freebuff-provider-metrics`

| Panel | Description |
|-------|-------------|
| Provider Request Rate | Requests per provider |
| Provider Latency | p50 and p95 per provider |
| Model Distribution | Donut chart by model |
| Provider Error Rate | 5xx rate per provider |
| Token Usage | Estimated tokens/s |
| Streaming vs Non-Streaming | Request type breakdown |
| Request Queue Depth | Queue backlog |
| Active Sessions | Current sessions |
| Provider Health | Up/Down status |
| Retry Count | Retry frequency |

## Alerts

Prometheus alerting rules are in `alerts/freebuff-alerts.yml`:

| Alert | Severity | Condition |
|-------|----------|-----------|
| GatewayDown | critical | Gateway unreachable >30s |
| HighErrorRate | warning | 5xx rate >5% for 2m |
| CriticalErrorRate | critical | 5xx rate >20% for 1m |
| HighLatencyP95 | warning | p95 >5s for 3m |
| HighMemoryUsage | warning | >500MB for 5m |
| GoroutineLeak | warning | >10K goroutines for 5m |
| FileDescriptorExhaustion | warning | >80% FDs for 2m |
| QueueFull | warning | Queue >100 for 1m |

## Configuration

Set environment variables before starting:

```bash
export FREEBUFF_ADMIN_PASSWORD=your-secure-password
export GRAFANA_USER=admin
export GRAFANA_PASSWORD=your-grafana-password
./start-monitoring.sh
```

## Architecture

```
Freebuff Gateway (:30080)
    │
    ├── /metrics → Prometheus (:9090)
    │                  │
    │                  └── Alert Rules → freebuff-alerts.yml
    │
    └── Dashboard → Grafana (:3000)
                        │
                        ├── HTTP Overview
                        ├── System Health
                        └── Provider Metrics
```

## Files

```
grafana/
├── README.md                              # This file
├── docker-compose.monitoring.yml          # Full stack
├── prometheus.yml                         # Prometheus config
├── start-monitoring.sh                    # Quick start script
├── dashboards/
│   ├── http-overview.json                 # HTTP traffic dashboard
│   ├── system-health.json                 # System resources dashboard
│   └── provider-metrics.json              # AI provider dashboard
├── alerts/
│   └── freebuff-alerts.yml                # Alerting rules
└── provisioning/
    ├── dashboards/
    │   └── dashboards.yml                 # Dashboard provisioning
    └── datasources/
        └── prometheus.yml                 # Datasource provisioning
```

## Adding Custom Metrics

The gateway exposes these metrics at `/metrics`:

```promql
# Request metrics (auto-collected by middleware)
http_requests_total
http_request_duration_seconds
http_request_size_bytes
http_response_size_bytes
http_active_requests

# Go runtime metrics (auto-collected by Prometheus client)
go_goroutines
go_memstats_*
go_gc_duration_seconds
process_*
```

To add custom metrics in Go:

```go
import "github.com/marktantongco/freebuff-gateway/internal/observability"

exporter := observability.Default()
counter := exporter.NewCounter("my_custom_counter", "Description")
counter.Inc()
```
