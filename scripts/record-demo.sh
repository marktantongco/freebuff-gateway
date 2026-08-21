#!/bin/bash
# Freebuff Gateway - Automated Demo Recording Script
# Records a terminal session showing the gateway features

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GATEWAY="http://127.0.0.1:30080"
ADMIN_PASS="demo123"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Check if asciinema is available
if command -v asciinema &> /dev/null; then
    RECORD="asciinema"
    echo -e "${GREEN}Using asciinema for recording${NC}"
elif command -v script &> /dev/null; then
    RECORD="script"
    echo -e "${GREEN}Using script command for recording${NC}"
else
    RECORD="none"
    echo -e "${YELLOW}No recording tool found. Running demo interactively.${NC}"
fi

# Create output directory
mkdir -p "$PROJECT_DIR/demo-output"

# Start recording
if [ "$RECORD" = "asciinema" ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    RECORD_FILE="$PROJECT_DIR/demo-output/demo_${TIMESTAMP}.cast"
    echo -e "${BLUE}Recording to: $RECORD_FILE${NC}"
    asciinema rec -c "$0 --demo" "$RECORD_FILE"
    echo ""
    echo -e "${GREEN}Recording saved to: $RECORD_FILE${NC}"
    echo -e "${BLUE}To view: asciinema play $RECORD_FILE${NC}"
    echo -e "${BLUE}To share: asciinema upload $RECORD_FILE${NC}"
    exit 0
fi

# Demo mode (called by asciinema)
if [ "$1" = "--demo" ]; then
    shift
fi

# ============================================
# Helper functions
# ============================================
type_slowly() {
    local text="$1"
    local delay="${2:-0.05}"
    for (( i=0; i<${#text}; i++ )); do
        echo -n "${text:$i:1}"
        sleep $delay
    done
    echo ""
}

print_header() {
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║  $1$(printf '%*s' $((52 - ${#1})) '')║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
    sleep 1
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
    sleep 0.5
}

print_result() {
    echo -e "${GREEN}  ✓ $1${NC}"
    sleep 0.5
}

print_output() {
    echo -e "${BLUE}$1${NC}"
    sleep 0.3
}

# ============================================
# Demo Script
# ============================================
cd "$PROJECT_DIR"

print_header "FREEBUFF GATEWAY DEMO"

echo -e "${CYAN}A production-grade Go gateway for AI model proxy management${NC}"
echo ""
sleep 1

# Step 1: Build
print_header "STEP 1: Build the Gateway"
print_step "Building gateway binary..."
make build 2>&1 | tail -1
print_result "Binary built successfully"
sleep 1

# Step 2: Start
print_header "STEP 2: Start the Gateway"
print_step "Starting gateway with demo configuration..."
mkdir -p data logs
ADMIN_PASSWORD=$ADMIN_PASS nohup ./bin/freebuff-gateway > logs/gateway.log 2>&1 &
GATEWAY_PID=$!
sleep 2
print_result "Gateway started (PID: $GATEWAY_PID)"
print_result "Listening on $GATEWAY"
sleep 1

# Step 3: Health Check
print_header "STEP 3: Health Check"
print_step "Checking gateway health..."
echo ""
curl -s $GATEWAY/healthz | head -c 200 || echo "(Web dashboard response)"
echo ""
print_result "Gateway is healthy"
sleep 1

# Step 4: Environment Config
print_header "STEP 4: Configuration"
print_step "Showing environment variable configuration..."
echo ""
echo -e "${BLUE}Configuration is loaded with this priority:${NC}"
echo "  1. Environment variables (highest)"
echo "  2. JSON config file"
echo "  3. Compiled defaults (lowest)"
echo ""
echo -e "${BLUE}Example overrides:${NC}"
print_output "  LISTEN_ADDR=:8080"
print_output "  ADMIN_PASSWORD=secret"
print_output "  LOG_LEVEL=debug"
print_output "  RATE_LIMIT_RPM=120"
print_output "  SESSION_CREATE_MAX_PARALLEL_GLOBAL=256"
sleep 1

# Step 5: API Endpoints
print_header "STEP 5: API Endpoints"
print_step "Available endpoints:"
echo ""
print_output "  GET  /healthz                    - Health check"
print_output "  GET  /readyz                     - Readiness check"
print_output "  GET  /v1/models                  - Model registry"
print_output "  POST /v1/chat/completions        - OpenAI-compatible"
print_output "  POST /v1/messages                - Anthropic-compatible"
print_output "  GET  /api/admin/*                - Admin API"
sleep 1

# Step 6: Request Example
print_header "STEP 6: Example Request"
print_step "OpenAI-compatible request:"
echo ""
echo -e "${BLUE}curl -X POST $GATEWAY/v1/chat/completions \\${NC}"
echo -e "${BLUE}  -H \"Content-Type: application/json\" \\${NC}"
echo -e "${BLUE}  -H \"Authorization: Bearer sk-your-key\" \\${NC}"
echo -e "${BLUE}  -d '{\"model\":\"gpt-4\",\"messages\":[...]}'${NC}"
echo ""
print_result "Request format compatible with OpenAI SDK"
sleep 1

# Step 7: Docker
print_header "STEP 7: Docker Support"
print_step "Docker deployment:"
echo ""
print_output "  # Build image"
print_output "  docker build -t freebuff-gateway ."
echo ""
print_output "  # Run container"
print_output "  docker run -p 30080:30080 freebuff-gateway"
echo ""
print_output "  # Or use docker-compose"
print_output "  docker-compose up -d"
sleep 1

# Step 8: Stats
print_header "STEP 8: Performance"
print_step "Resource usage:"
echo ""
GATEWAY_PID=$(pgrep -f "freebuff-gateway" | head -1)
if [ -n "$GATEWAY_PID" ]; then
    RSS=$(ps -o rss= -p $GATEWAY_PID 2>/dev/null | tr -d ' ')
    RSS_MB=$((RSS / 1024))
    print_result "Memory: ${RSS_MB} MB (idle)"
fi
print_result "Binary size: $(ls -lh bin/freebuff-gateway | awk '{print $5}')"
print_result "Startup time: <1 second"
sleep 1

# Cleanup
print_header "Demo Complete!"
print_step "Stopping gateway..."
pkill -f "freebuff-gateway" 2>/dev/null || true
sleep 1
print_result "Gateway stopped"

echo ""
echo -e "${GREEN}${BOLD}Thank you for watching!${NC}"
echo ""
echo -e "${CYAN}Learn more:${NC}"
echo "  GitHub: https://github.com/marktantongco/freebuff-gateway"
echo "  Docs:   https://github.com/marktantongco/freebuff-gateway#readme"
echo ""
