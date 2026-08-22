# ─────────────────────────────────────────────────────────────
# Freebuff Gateway — Multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────
# Stage 1: Build
FROM golang:1.25-alpine AS builder

# Build args for versioning
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Cache dependencies first (layer caching)
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build all binaries with version info
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -trimpath \
    -o /freebuff-gateway \
    ./cmd/gateway/

# Build doctor utility
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /freebuff-doctor \
    ./cmd/doctor/ 2>/dev/null || true

# ─────────────────────────────────────────────────────────────
# Stage 2: Runtime (minimal image)
FROM alpine:3.20

# Security: add ca-certs and tzdata
RUN apk add --no-cache ca-certificates tzdata curl

# Security: non-root user
RUN addgroup -g 1001 -S freebuff && \
    adduser -u 1001 -S freebuff -G freebuff -s /bin/sh

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /freebuff-gateway ./freebuff-gateway
COPY --from=builder /freebuff-doctor ./freebuff-doctor

# Copy example config
COPY --chown=freebuff:freebuff configs/config.example.json ./config.json

# Create directories for data and logs
RUN mkdir -p data logs && chown -R freebuff:freebuff /app

# Switch to non-root user
USER freebuff

# Expose gateway port
EXPOSE 30080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:30080/healthz || exit 1

# Labels
LABEL maintainer="marktantongco" \
      version="${VERSION}" \
      description="Freebuff Gateway — Unified AI API Gateway" \
      org.opencontainers.image.source="https://github.com/marktantongco/freebuff-gateway"

ENTRYPOINT ["./freebuff-gateway"]
