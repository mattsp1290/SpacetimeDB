#!/bin/bash
# Verify SpacetimeDB Telemetry Stack

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SpacetimeDB Telemetry Verification ===${NC}"
echo ""

# Function to check endpoint
check_endpoint() {
    local name=$1
    local url=$2
    local expected=$3
    
    echo -n "Checking $name... "
    
    response=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    
    if [ "$response" = "$expected" ]; then
        echo -e "${GREEN}✓ OK (HTTP $response)${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed (HTTP $response)${NC}"
        return 1
    fi
}

# Check all endpoints
echo -e "${YELLOW}Checking service endpoints:${NC}"
check_endpoint "SpacetimeDB Health" "http://localhost:3000/ping" "200"
check_endpoint "SpacetimeDB Metrics" "http://localhost:9000/metrics" "200"
check_endpoint "Prometheus" "http://localhost:9090/-/healthy" "200"
check_endpoint "Grafana" "http://localhost:3001/api/health" "200"
check_endpoint "Jaeger UI" "http://localhost:16686/" "200"
check_endpoint "OTEL Collector Health" "http://localhost:13133/" "200"

echo ""
echo -e "${YELLOW}Checking Prometheus targets:${NC}"
# Check if SpacetimeDB is being scraped
targets=$(curl -s http://localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | select(.labels.job=="spacetimedb") | .health' 2>/dev/null || echo "error")

if [ "$targets" = "up" ]; then
    echo -e "${GREEN}✓ SpacetimeDB metrics are being scraped${NC}"
else
    echo -e "${RED}✗ SpacetimeDB metrics scraping issue${NC}"
fi

echo ""
echo -e "${YELLOW}Checking for metrics data:${NC}"
# Query for SpacetimeDB metrics
metric_count=$(curl -s "http://localhost:9090/api/v1/query?query=up{job='spacetimedb'}" | jq -r '.data.result | length' 2>/dev/null || echo "0")

if [ "$metric_count" -gt 0 ]; then
    echo -e "${GREEN}✓ Found SpacetimeDB metrics in Prometheus${NC}"
else
    echo -e "${RED}✗ No SpacetimeDB metrics found${NC}"
fi

echo ""
echo -e "${YELLOW}Checking telemetry files:${NC}"
# Check for telemetry data files
for file in metrics/metrics.jsonl traces/traces.jsonl logs/logs.jsonl; do
    if [ -f "telemetry/data/$file" ] && [ -s "telemetry/data/$file" ]; then
        echo -e "${GREEN}✓ Found data in $file${NC}"
    else
        echo -e "${YELLOW}○ No data yet in $file${NC}"
    fi
done

echo ""
echo -e "${BLUE}=== Quick Links ===${NC}"
echo "Grafana Dashboard: http://localhost:3001/d/spacetimedb-overview/spacetimedb-overview"
echo "Prometheus Query: http://localhost:9090/graph?g0.expr=up{job%3D\"spacetimedb\"}"
echo "Jaeger Search: http://localhost:16686/search?service=spacetimedb"

echo ""
echo -e "${GREEN}Verification complete!${NC}"
