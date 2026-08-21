# Freebuff Gateway API

## Overview

The Freebuff Gateway exposes a RESTful API for managing AI providers, sessions, alerts, and monitoring. All responses use JSON unless otherwise specified.

## Base URL

```
http://localhost:30080
```

## Authentication

### Admin Endpoints (`/api/admin/*`)

Admin endpoints require a session cookie obtained via login:

```bash
# Login
curl -X POST http://localhost:30080/api/admin/login \
  -H "Content-Type: application/json" \
  -d '{"password": "your-admin-password"}'
```

The response sets a `freebuffreverse_admin` cookie that must be included in subsequent requests.

### Model Endpoints (`/v1/*`)

Model endpoints require an API key via:
- `X-API-Key` header
- `Authorization: Bearer <key>` header

## Endpoints

### Health & Observability

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Full health status with components |
| GET | `/readyz` | Readiness probe |
| GET | `/livez` | Liveness probe |
| GET | `/metrics` | Prometheus metrics |

### Dashboard

| Method | Path | Description |
|--------|------|-------------|
| GET | `/dashboard` | Admin dashboard (requires login) |

### Auth

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/admin/login` | Admin login |
| POST | `/api/admin/logout` | Admin logout |
| GET | `/api/admin/me` | Check auth status |

### Admin Management

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/channels` | List channels |
| PUT | `/api/admin/channels/{id}/config` | Update channel config |
| GET | `/api/admin/freebuff/models` | List Freebuff models |
| GET | `/api/admin/freebuff/proxies` | List Freebuff proxies |
| GET | `/api/admin/freebuff/accounts` | List Freebuff accounts |
| GET | `/api/admin/freebuff/scheduler` | Get scheduler status |
| GET | `/api/admin/auth-keys` | List API keys |
| POST | `/api/admin/auth-keys` | Create API key |
| DELETE | `/api/admin/auth-keys/{id}` | Delete API key |
| GET | `/api/admin/accounts` | List all accounts |
| POST | `/api/admin/accounts` | Create account |
| PUT | `/api/admin/accounts/{id}` | Update account |
| DELETE | `/api/admin/accounts/{id}` | Delete account |
| GET | `/api/admin/sessions` | List sessions |
| GET | `/api/admin/logs` | List logs |
| GET | `/api/admin/system-logs` | List system logs |
| GET | `/api/admin/metrics` | List metrics |
| GET | `/api/admin/usage/summary` | Usage summary |
| GET | `/api/admin/usage/accounts` | Per-account usage |
| GET | `/api/admin/usage/events` | Usage events |

### Models API (OpenAI-compatible)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/models` | List available models |
| GET | `/v1/models/{modelID}` | Get model details |

### Proxy (Chat Completions)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/chat/completions` | Chat completions (OpenAI) |
| POST | `/v1/messages` | Messages (Anthropic) |
| POST | `/channels/{id}` | Channel-specific proxy |

### Alerting

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/alerts` | List alerts |
| POST | `/api/alerts` | Create alert |
| GET | `/api/alerts/{id}` | Get alert |
| POST | `/api/alerts/{id}/ack` | Acknowledge alert |
| POST | `/api/alerts/{id}/silence` | Silence alert |
| GET | `/api/alerts/stats` | Alert statistics |
| GET | `/api/alerts/history` | Alert history |

### WebSocket

| Protocol | Path | Description |
|----------|------|-------------|
| WS | `/ws` | Real-time updates |
| GET | `/ws/status` | Connection count |

## WebSocket Protocol

Connect to `ws://localhost:30080/ws` for real-time updates.

### Subscribe to Topics

```json
{
  "type": "subscribe",
  "data": ["health", "alert", "metrics", "session"]
}
```

### Message Format

```json
{
  "type": "health",
  "data": { "status": "healthy", "uptime": 123456789 },
  "timestamp": 1692681600000
}
```

### Topic Types

| Topic | Description |
|-------|-------------|
| `health` | Health status updates |
| `alert` | New alert notifications |
| `metrics` | Metric updates |
| `session` | Session changes |

## Rate Limiting

All requests are rate-limited per IP. Response headers:

- `X-RateLimit-Limit`: Maximum requests per burst
- `X-RateLimit-Remaining`: Remaining requests in current window

## CORS

CORS is configured via `CORS_ALLOWED_ORIGINS` environment variable. Default allows all origins.

## OpenAPI Specification

The full OpenAPI 3.0 specification is available at [docs/openapi.yaml](openapi.yaml).

You can view it interactively using:
- [Swagger Editor](https://editor.swagger.io/)
- [Redocly](https://redocly.github.io/redoc/)
