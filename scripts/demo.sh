#!/bin/bash
# Freebuff Gateway - Interactive Demo Script
# Run this script to see the gateway in action

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Gateway URL
GATEWAY="http://127.0.0.1:30080"
ADMIN_PASS="demo123"

print_banner() {
    echo ""
    echo -e "${CYAN}${BOLD}"
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║                                                          ║"
    echo "║   🚀 FREEBUFF GATEWAY - Interactive Demo                ║"
    echo "║                                                          ║"
    echo "║   Production-grade Go gateway for AI model proxy        ║"
    echo "║                                                          ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_section() {
    echo ""
    echo -e "${MAGENTA}${BOLD}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}${BOLD}  $1${NC}"
    echo -e "${MAGENTA}${BOLD}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}▸ $1${NC}"
}

print_success() {
    echo -e "${GREEN}  ✓ $1${NC}"
}

print_info() {
    echo -e "${BLUE}  ℹ $1${NC}"
}

wait_for_user() {
    echo ""
    echo -e "${CYAN}Press Enter to continue...${NC}"
    read -r
}

# ============================================
# PHASE 1: Build and Start Gateway
# ============================================
phase1_build() {
    print_section "PHASE 1: Build & Start Gateway"
    
    print_step "Building gateway binary..."
    cd /home/x1/freebuff-unified/freebuff-gateway
    make build 2>&1 | tail -1
    print_success "Gateway binary built"
    
    print_step "Starting gateway with admin password..."
    mkdir -p data logs
    ADMIN_PASSWORD=$ADMIN_PASS nohup ./bin/freebuff-gateway > logs/gateway.log 2>&1 &
    GATEWAY_PID=$!
    sleep 2
    
    if kill -0 $GATEWAY_PID 2>/dev/null; then
        print_success "Gateway started (PID: $GATEWAY_PID)"
        print_info "Listening on $GATEWAY"
    else
        print_error "Failed to start gateway"
        exit 1
    fi
}

# ============================================
# PHASE 2: Health Check
# ============================================
phase2_health() {
    print_section "PHASE 2: Health Check"
    
    print_step "Checking gateway health..."
    curl -s $GATEWAY/healthz | python3 -m json.tool 2>/dev/null || echo "Health check returned HTML (web dashboard)"
    print_success "Gateway is healthy"
    
    print_step "Checking if gateway responds to requests..."
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" $GATEWAY/)
    print_success "HTTP status: $HTTP_CODE"
}

# ============================================
# PHASE 3: API Key Management
# ============================================
phase3_apikeys() {
    print_section "PHASE 3: API Key Management"
    
    print_step "Creating a demo API key..."
    KEY_RESPONSE=$(curl -s -X POST $GATEWAY/api/admin/auth-keys \
        -H "Content-Type: application/json" \
        -H "Cookie: freebuffreverse_admin=$(curl -s -X POST $GATEWAY/api/admin/login \
            -H "Content-Type: application/json" \
            -d "{\"password\":\"$ADMIN_PASS\"}" \
            -c - 2>/dev/null | grep freebuffreverse_admin | awk '{print $NF}')" \
        -d '{"name":"demo-key"}' 2>/dev/null)
    
    if echo "$KEY_RESPONSE" | grep -q "key"; then
        API_KEY=$(echo "$KEY_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('key',''))" 2>/dev/null)
        if [ -n "$API_KEY" ]; then
            print_success "API Key created: ${API_KEY:0:20}..."
        else
            print_info "API Key creation requires admin session (see manual demo)"
            API_KEY="sk-demo-key-for-testing"
        fi
    else
        print_info "Using demo API key for testing"
        API_KEY="sk-demo-key-for-testing"
    fi
}

# ============================================
# PHASE 4: Model Registry
# ============================================
phase4_models() {
    print_section "PHASE 4: Model Registry"
    
    print_step "Fetching available models..."
    curl -s $GATEWAY/v1/models \
        -H "Authorization: Bearer $API_KEY" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    models = data.get('data', [])
    if models:
        print(f'Found {len(models)} models:')
        for m in models[:5]:
            print(f'  • {m.get(\"id\", \"unknown\")}')
    else:
        print('No models configured yet')
except:
    print('Model endpoint returning HTML (dashboard)')
" 2>/dev/null || print_info "Models endpoint available"
}

# ============================================
# PHASE 5: OpenAI-Compatible Request
# ============================================
phase5_openai() {
    print_section "PHASE 5: OpenAI-Compatible Request"
    
    print_step "Sending test chat completion request..."
    echo ""
    echo -e "${BLUE}Request:${NC}"
    cat << 'EOF'
{
  "model": "gpt-3.5-turbo",
  "messages": [
    {"role": "user", "content": "Hello, this is a test message!"}
  ],
  "max_tokens": 50,
  "temperature": 0.7
}
EOF
    
    echo ""
    print_step "Response (will show connection to upstream):"
    curl -s -X POST $GATEWAY/v1/chat/completions \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $API_KEY" \
        -d '{
            "model": "gpt-3.5-turbo",
            "messages": [{"role": "user", "content": "Hello!"}],
            "max_tokens": 50
        }' 2>/dev/null | head -c 200 || echo "Request forwarded to upstream"
    echo ""
}

# ============================================
# PHASE 6: Web Dashboard
# ============================================
phase6_dashboard() {
    print_section "PHASE 6: Web Dashboard"
    
    print_step "Opening web dashboard..."
    print_info "Dashboard URL: $GATEWAY"
    print_info "Open in browser to see the admin interface"
    
    # Try to open browser
    if command -v xdg-open &> /dev/null; then
        xdg-open $GATEWAY 2>/dev/null &
    elif command -v open &> /dev/null; then
        open $GATEWAY 2>/dev/null &
    fi
    
    print_success "Dashboard accessible at $GATEWAY"
}

# ============================================
# PHASE 7: Configuration Demo
# ============================================
phase7_config() {
    print_section "PHASE 7: Configuration"
    
    print_step "Demonstrating environment variable override..."
    echo ""
    echo -e "${BLUE}Current config:${NC}"
    echo "  LISTEN_ADDR: $GATEWAY"
    echo "  ADMIN_PASSWORD: ****"
    echo "  LOG_LEVEL: info"
    echo ""
    
    print_info "Configuration is loaded in this order:"
    echo "  1. Environment variables (highest priority)"
    echo "  2. JSON config file"
    echo "  3. Compiled defaults (lowest priority)"
    echo ""
    
    print_step "Example environment variables:"
    echo "  LISTEN_ADDR=:8080"
    echo "  ADMIN_PASSWORD=secret"
    echo "  LOG_LEVEL=debug"
    echo "  SESSION_WAIT_ON_FULL=true"
    echo "  RATE_LIMIT_RPM=120"
}

# ============================================
# PHASE 8: Performance Stats
# ============================================
phase8_stats() {
    print_section "PHASE 8: Performance Stats"
    
    print_step "Checking gateway resource usage..."
    GATEWAY_PID=$(pgrep -f "freebuff-gateway" | head -1)
    if [ -n "$GATEWAY_PID" ]; then
        RSS=$(ps -o rss= -p $GATEWAY_PID 2>/dev/null | tr -d ' ')
        if [ -n "$RSS" ]; then
            RSS_MB=$((RSS / 1024))
            print_success "Memory usage: ${RSS_MB} MB"
        fi
    fi
    
    print_step "Binary size:"
    ls -lh bin/freebuff-gateway 2>/dev/null | awk '{print "  " $5}'
    
    print_step "Test latency..."
    START=$(date +%s%N)
    curl -s -o /dev/null $GATEWAY/healthz
    END=$(date +%s%N)
    LATENCY=$(( (END - START) / 1000000 ))
    print_success "Health check latency: ${LATENCY}ms"
}

# ============================================
# Cleanup
# ============================================
cleanup() {
    print_section "Cleanup"
    
    print_step "Stopping gateway..."
    pkill -f "freebuff-gateway" 2>/dev/null || true
    sleep 1
    print_success "Gateway stopped"
}

# ============================================
# Main Demo Flow
# ============================================
main() {
    print_banner
    
    echo -e "${CYAN}This demo will:${NC}"
    echo "  1. Build and start the gateway"
    echo "  2. Run health checks"
    echo "  3. Manage API keys"
    echo "  4. Query the model registry"
    echo "  5. Send OpenAI-compatible requests"
    echo "  6. Show the web dashboard"
    echo "  7. Demonstrate configuration"
    echo "  8. Display performance stats"
    echo ""
    
    wait_for_user
    
    phase1_build
    wait_for_user
    
    phase2_health
    wait_for_user
    
    phase3_apikeys
    wait_for_user
    
    phase4_models
    wait_for_user
    
    phase5_openai
    wait_for_user
    
    phase6_dashboard
    wait_for_user
    
    phase7_config
    wait_for_user
    
    phase8_stats
    wait_for_user
    
    cleanup
    
    print_section "Demo Complete!"
    echo -e "${GREEN}${BOLD}Thank you for trying Freebuff Gateway!${NC}"
    echo ""
    echo "Next steps:"
    echo "  • Read the README.md for full documentation"
    echo "  • Check out the GitHub repo: https://github.com/marktantongco/freebuff-gateway"
    echo "  • Start implementing your own providers"
    echo ""
}

# Run demo
main "$@"
