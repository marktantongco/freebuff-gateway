# Freebuff Gateway

**Production-grade, modular Go gateway for AI model proxy management.**

Built by extracting the strongest capabilities from 27+ repositories in the Freebuff ecosystem, unified behind clean Go interfaces.

## Architecture

```
Client → API Gateway → Request Orchestrator → Provider Adapters → Session Manager → Observability
```

### Key Components

| Package | Source | Purpose |
|---------|--------|---------|
| `internal/session/` | CORE-08 | Session lifecycle with create gates, capacity management, state recording |
| `internal/channels/` | CORE-08 | Channel adapter interface for provider abstraction |
| `internal/proxypool/` | CORE-08 | Periodic proxy health checking with concurrency control |
| `internal/orchestration/` | CORE-08 | Request orchestration with timeout propagation |
| `internal/pool/` | CORE-16 | Weighted account pool with cooldowns and overage handling |
| `internal/auth/` | CORE-16 | Multi-protocol authentication (SSO, OIDC, IAM) |
| `internal/contentfilter/` | CORE-16 | Content filtering with audit mode |
| `internal/stealth/` | PROXY-02 | TLS fingerprinting, header randomization, proxy rotation |
| `internal/model/` | CORE-15 | Dynamic model registry with aliases |
| `pkg/api/openai/` | CORE-04 | OpenAI request/response mapping |
| `pkg/api/anthropic/` | CORE-04 | Anthropic request/response mapping |

## Quick Start

```bash
# Build
make build

# Configure
cp configs/config.example.json data/config.json
# Edit data/config.json

# Run
make run

# Health check
make doctor
```

## API Endpoints

```
GET  /healthz                    # Health check
GET  /readyz                     # Readiness check
GET  /v1/models                  # Model registry
POST /v1/chat/completions        # OpenAI-compatible
POST /v1/messages                # Anthropic-compatible
```

## Development

```bash
# Run tests
make test

# Run with coverage
make test-coverage

# Build all binaries
make build-all

# Clean
make clean
```

## Configuration

See `configs/config.example.json` for all options.

### Key Settings

- `listen_addr`: Server address (default `:8080`)
- `admin_password`: Admin API password
- `db_path`: SQLite database path
- `session.create_limits`: Session creation rate limits
- `proxy_pool.enabled`: Enable proxy health checking
- `stealth.enabled`: Enable TLS fingerprinting
- `models.aliases`: Model name aliases

## Resource Usage

| Metric | Value |
|--------|-------|
| Idle RAM | ~30 MB |
| 1000 concurrent | ~120 MB |
| Binary size | ~15 MB |
| Database | ~10 MB |

## License

MIT
