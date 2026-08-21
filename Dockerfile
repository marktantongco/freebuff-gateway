# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /freebuff-gateway \
    ./cmd/gateway/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /freebuff-doctor \
    ./cmd/doctor/

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 -S freebuff && \
    adduser -u 1001 -S freebuff -G freebuff

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /freebuff-gateway .
COPY --from=builder /freebuff-doctor .

# Copy config example
COPY configs/config.example.json ./config.json

# Create data directory
RUN mkdir -p data && chown -R freebuff:freebuff /app

USER freebuff

EXPOSE 30080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:30080/healthz || exit 1

ENTRYPOINT ["./freebuff-gateway"]
