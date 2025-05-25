#!/bin/bash
# Start SpacetimeDB with Telemetry Stack

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SpacetimeDB Telemetry Stack Startup ===${NC}"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Create necessary directories
echo -e "${YELLOW}Creating telemetry directories...${NC}"
mkdir -p telemetry/data/{logs,metrics,traces,processed}
mkdir -p modules

# Set environment variables
export SPACETIMEDB_VERSION=${SPACETIMEDB_VERSION:-latest}
export SPACETIMEDB_PORT=${SPACETIMEDB_PORT:-3000}
export SPACETIMEDB_LOG_LEVEL=${SPACETIMEDB_LOG_LEVEL:-info}

# Build SpacetimeDB image if needed
if [[ "$(docker images -q spacetimedb:${SPACETIMEDB_VERSION} 2> /dev/null)" == "" ]]; then
    echo -e "${YELLOW}Building SpacetimeDB Docker image...${NC}"
    docker build -t spacetimedb:${SPACETIMEDB_VERSION} -f Dockerfile.prod .
fi

# Start the telemetry stack
echo -e "${YELLOW}Starting telemetry stack...${NC}"
docker-compose -f docker-compose.telemetry.yml up -d

# Wait for services to be healthy
echo -e "${YELLOW}Waiting for services to start...${NC}"
sleep 10

# Check service health
echo -e "${BLUE}Checking service health...${NC}"
echo ""

# Function to check service health
check_service() {
    local service=$1
    local url=$2
    local name=$3
    
    if curl -f -s "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ $name is running${NC}"
        return 0
    else
        echo -e "${RED}✗ $name is not responding${NC}"
        return 1
    fi
}

# Check each service
check_service "spacetimedb" "http://localhost:3000/ping" "SpacetimeDB"
check_service "prometheus" "http://localhost:9090/-/healthy" "Prometheus"
check_service "grafana" "http://localhost:3001/api/health" "Grafana"
check_service "jaeger" "http://localhost:16686/" "Jaeger"
check_service "otel-collector" "http://localhost:13133/" "OpenTelemetry Collector"

echo ""
echo -e "${GREEN}=== Telemetry Stack Started ===${NC}"
echo ""
echo "Access the following services:"
echo -e "  ${BLUE}SpacetimeDB:${NC}    http://localhost:3000"
echo -e "  ${BLUE}Grafana:${NC}        http://localhost:3001 (admin/admin)"
echo -e "  ${BLUE}Prometheus:${NC}     http://localhost:9090"
echo -e "  ${BLUE}Jaeger:${NC}         http://localhost:16686"
echo ""
echo "Metrics endpoint: http://localhost:9000/metrics"
echo "OTLP endpoint:    localhost:4317 (gRPC) / localhost:4318 (HTTP)"
echo ""
echo "To view logs:"
echo "  docker-compose -f docker-compose.telemetry.yml logs -f"
echo ""
echo "To stop the stack:"
echo "  ./telemetry/scripts/stop-telemetry-stack.sh"
