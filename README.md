# Freebuff Gateway

> Production-grade Go gateway for AI model proxy management

[![CI](https://github.com/marktantongco/freebuff-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/marktantongco/freebuff-gateway/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/marktantongco/freebuff-gateway)](https://github.com/marktantongco/freebuff-gateway/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/marktantongco/freebuff-gateway)](https://goreportcard.com/report/github.com/marktantongco/freebuff-gateway)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## What is Freebuff Gateway?

A unified, modular Go gateway that intelligently routes AI model requests through multiple providers with automatic failover, session management, and proxy rotation. Built by extracting the strongest capabilities from 27+ repositories in the Freebuff ecosystem.

## Features

- **Multi-Provider Support** — OpenAI, Anthropic, Gemini, NVIDIA, and more
- **Session Management** — Stateful sessions with capacity management and automatic cleanup
- **Proxy Pool** — Rotating proxy support with health checking and failover
- **Stealth Transport** — TLS fingerprinting and header randomization
- **Rate Limiting** — Configurable request throttling
- **Health Monitoring** — Built-in health checks and metrics
- **Embedded Dashboard** — React-based admin UI
- **Docker Ready** — Multi-stage Dockerfile included

## Quick Start

### Install from Binary

```bash
# Linux (amd64)
curl -L https://github.com/marktantongco/freebuff-gateway/releases/latest/download/freebuff-gateway-linux-amd64 -o freebuff-gateway
chmod +x freebuff-gateway
./freebuff-gateway
```

### Install from Source

```bash
# Clone the repository
git clone https://github.com/marktantongco/freebuff-gateway.git
cd freebuff-gateway

# Build
make build

# Run
./bin/freebuff-gateway
```

### Install with Docker

```bash
# Pull the image
docker pull ghcr.io/marktantongco/freebuff-gateway:latest

# Run
docker run -p 30080:30080 \
  -e ADMIN_PASSWORD=your-secret \
  ghcr.io/marktantongco/freebuff-gateway:latest
```

### Install with Docker Compose

```bash
# Clone and start
git clone https://github.com/marktantongco/freebuff-gateway.git
cd freebuff-gateway
docker-compose up -d
```

## Configuration

Configuration is loaded in the following priority order (highest wins):

1. **Environment variables** (with `FREEBUFF_` prefix)
2. **JSON config file** (`data/config.json` or `config.json`)
3. **Compiled defaults**

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `:30080` | Server listen address |
| `ADMIN_PASSWORD` | `admin` | Admin API password |
| `DB_PATH` | `./data/gateway.db` | SQLite database path |
| `LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `SESSION_WAIT_ON_FULL` | `false` | Wait when session pool is full |
| `SESSION_CREATE_MAX_PARALLEL_GLOBAL` | `128` | Max global parallel sessions |
| `TRANSPORT_TIMEOUT` | `60s` | HTTP transport timeout |
| `RATE_LIMIT_ENABLED` | `true` | Enable rate limiting |
| `RATE_LIMIT_RPM` | `60` | Rate limit requests per minute |
| `STEALTH_ENABLED` | `false` | Enable TLS fingerprinting |
| `PROXY_POOL_ENABLED` | `false` | Enable proxy pool |

### Example Config File

```json
{
  "listen_addr": ":30080",
  "admin_password": "your-secret",
  "db_path": "./data/gateway.db",
  "session": {
    "wait_on_full": false,
    "create_limits": {
      "max_parallel_global": 128
    }
  },
  "transport": {
    "timeout": "60s",
    "request_reuse": true
  },
  "rate_limit": {
    "enabled": true,
    "requests_per_minute": 60
  },
  "logging": {
    "level": "info",
    "redact_tokens": true
  }
}
```

## API Endpoints

### Health Check

```bash
curl http://localhost:30080/healthz
```

### OpenAI-Compatible

```bash
curl -X POST http://localhost:30080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d '{
    "model": "gpt-4",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Anthropic-Compatible

```bash
curl -X POST http://localhost:30080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-api-key" \
  -d '{
    "model": "claude-3-opus-20240229",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Model Registry

```bash
curl http://localhost:30080/v1/models
```

## Development

### Prerequisites

- Go 1.25+
- Make
- Docker (optional)

### Build

```bash
# Build all binaries
make build-all

# Run tests
make test

# Run with coverage
make test-coverage

# Lint
make vet
```

### Project Structure

```
freebuff-gateway/
├── cmd/
│   ├── gateway/         # Main server binary
│   ├── doctor/          # Health check CLI
│   └── scheduler-sim/   # Scheduler simulator
├── internal/
│   ├── accounts/        # Account pool management
│   ├── api/             # HTTP handlers
│   ├── auth/            # Authentication (SSO, OIDC)
│   ├── channels/        # Provider adapters
│   ├── config/          # Layered configuration
│   ├── credential/      # Token discovery & decryption
│   ├── model/           # Model registry
│   ├── proxypool/       # Proxy health checking
│   ├── session/         # Session lifecycle
│   ├── stealth/         # TLS fingerprinting
│   └── transport/       # HTTP transport
├── pkg/
│   └── api/
│       ├── openai/      # OpenAI mapper
│       └── anthropic/   # Anthropic mapper
├── web/                 # Embedded dashboard
├── tests/               # Integration tests
├── configs/             # Config examples
├── .github/workflows/   # CI/CD pipelines
├── Dockerfile           # Multi-stage Docker build
└── docker-compose.yml   # Local development
```

## Architecture

```
                    ┌──────────────────────┐
                    │      Clients         │
                    │ OpenCode / Hermes /  │
                    │ CLI / SDK / Web UI   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   API Gateway        │
                    │ Auth / Routing /     │
                    │ Validation / Limits  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Request Orchestrator │
                    │ Queue / Scheduler /  │
                    │ Concurrency Control  │
                    └──────────┬───────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
        ┌────────────┐ ┌────────────┐ ┌────────────┐
        │ Provider A │ │ Provider B │ │ Provider C │
        │ Adapter    │ │ Adapter    │ │ Adapter    │
        └────────────┘ └────────────┘ └────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Session Manager      │
                    │ State / Lifecycle /  │
                    │ Persistence          │
                    └──────────────────────┘
```

## Resource Usage

| Metric | Value |
|--------|-------|
| Idle RAM | ~30 MB |
| 1000 concurrent | ~120 MB |
| Binary size | ~25 MB |
| Database | ~10 MB |

## Testing

```bash
# Run all tests
make test

# Run with race detection
go test -race ./...

# Run specific package
go test ./internal/session/ -v

# Run load tests
go test ./tests/load/ -bench=.
```

## Troubleshooting

### Gateway won't start

```bash
# Check configuration
./bin/freebuff-doctor

# Check logs
tail -f logs/gateway.log
```

### Connection refused

```bash
# Verify gateway is running
curl http://localhost:30080/healthz

# Check port availability
lsof -i :30080
```

### Database errors

```bash
# Check database permissions
ls -la data/

# Reset database
rm data/gateway.db
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built from the best ideas of 27+ repositories in the Freebuff ecosystem:
- [CORE-08](https://github.com/zj669/freebuff-reverse) — Session management foundation
- [CORE-16](https://github.com/marktantongco/Cybx-GateawayQue) — Queue architecture
- [PROXY-02](https://github.com/marktantongco/freebuff-proxy) — Stealth transport
- [CORE-15](https://github.com/notBlubbll/free-buff-lol) — Model registry

---

<p align="center">
  Built with ❤️ by <a href="https://github.com/marktantongco">Mark Tantongco</a>
</p>
