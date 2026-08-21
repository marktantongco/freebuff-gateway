.PHONY: build test install doctor clean lint

# Build variables
BINARY_NAME=freebuff-gateway
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

# Default target
all: build

## build: Build the gateway binary
build:
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/gateway/
	@echo "✅ Built: bin/$(BINARY_NAME) ($(VERSION))"

## build-all: Build all binaries
build-all: build
	go build $(LDFLAGS) -o bin/freebuff-doctor ./cmd/doctor/
	go build $(LDFLAGS) -o bin/freebuff-scheduler-sim ./cmd/scheduler-sim/
	@echo "✅ Built all binaries"

## test: Run all tests
test:
	go test ./... -v -count=1
	@echo "✅ All tests passed"

## test-short: Run short tests only
test-short:
	go test ./... -short -count=1

## test-coverage: Run tests with coverage
test-coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html
	@echo "✅ Coverage report: coverage.html"

## test-contract: Run contract tests
test-contract:
	go test ./tests/contract/ -v -count=1

## test-integration: Run integration tests
test-integration:
	go test ./tests/integration/ -v -count=1 -timeout 60s

## test-e2e: Run end-to-end tests
test-e2e:
	go test ./tests/e2e/ -v -count=1 -timeout 120s

## test-load: Run load tests
test-load:
	go test ./tests/load/ -bench=. -benchmem

## lint: Run linter
lint:
	golangci-lint run ./...

## vet: Run go vet
vet:
	go vet ./...

## install: Install binary to GOPATH
install:
	go install $(LDFLAGS) ./cmd/gateway/
	@echo "✅ Installed to $(shell go env GOPATH)/bin/$(BINARY_NAME)"

## doctor: Run health check
doctor: build
	./bin/freebuff-doctor

## clean: Remove build artifacts
clean:
	rm -rf bin/ coverage.out coverage.html
	@echo "✅ Cleaned"

## help: Show this help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | sed 's/:.*//'
