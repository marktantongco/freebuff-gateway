#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Freebuff Gateway — Monitoring Stack${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
echo ""

# Check Docker
if ! command -v docker &>/dev/null; then
    echo -e "${YELLOW}Docker not found. Install with:${NC}"
    echo "  curl -fsSL https://get.docker.com | sh"
    exit 1
fi

if ! docker info &>/dev/null; then
    echo -e "${YELLOW}Docker daemon not running. Start with:${NC}"
    echo "  sudo systemctl start docker"
    exit 1
fi

# Check docker-compose
if ! command -v docker-compose &>/dev/null && ! docker compose version &>/dev/null 2>&1; then
    echo -e "${YELLOW}docker-compose not found. Install with:${NC}"
    echo "  sudo apt install docker-compose-plugin"
    exit 1
fi

# Set defaults
export FREEBUFF_ADMIN_PASSWORD="${FREEBUFF_ADMIN_PASSWORD:-admin}"
export GRAFANA_USER="${GRAFANA_USER:-admin}"
export GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-freebuff}"

echo -e "${GREEN}Starting monitoring stack...${NC}"
echo ""

# Detect compose command
if docker compose version &>/dev/null 2>&1; then
    COMPOSE="docker compose"
else
    COMPOSE="docker-compose"
fi

$COMPOSE -f docker-compose.monitoring.yml up -d

echo ""
echo -e "${GREEN}✅ Monitoring stack started!${NC}"
echo ""
echo -e "${BLUE}Endpoints:${NC}"
echo -e "  Gateway:     ${GREEN}http://localhost:30080${NC}"
echo -e "  Prometheus:  ${GREEN}http://localhost:9090${NC}"
echo -e "  Grafana:     ${GREEN}http://localhost:3000${NC}"
echo ""
echo -e "${BLUE}Grafana Login:${NC}"
echo -e "  User:     ${GREEN}${GRAFANA_USER}${NC}"
echo -e "  Password: ${GREEN}${GRAFANA_PASSWORD}${NC}"
echo ""
echo -e "${BLUE}Dashboards:${NC}"
echo -e "  HTTP Overview:   ${GREEN}http://localhost:3000/d/freebuff-http-overview${NC}"
echo -e "  System Health:   ${GREEN}http://localhost:3000/d/freebuff-system-health${NC}"
echo -e "  Provider Metrics: ${GREEN}http://localhost:3000/d/freebuff-provider-metrics${NC}"
echo ""
echo -e "${YELLOW}Tip: Set GRAFANA_PASSWORD env var before running to change the default password.${NC}"
