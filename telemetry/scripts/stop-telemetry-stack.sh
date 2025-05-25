#!/bin/bash
# Stop SpacetimeDB Telemetry Stack

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Stopping SpacetimeDB Telemetry Stack ===${NC}"
echo ""

# Stop the telemetry stack
echo -e "${YELLOW}Stopping services...${NC}"
docker-compose -f docker-compose.telemetry.yml down

# Optional: Remove volumes (uncomment if you want to clean data)
# echo -e "${YELLOW}Removing data volumes...${NC}"
# docker-compose -f docker-compose.telemetry.yml down -v

echo ""
echo -e "${GREEN}=== Telemetry Stack Stopped ===${NC}"
echo ""
echo "To restart the stack:"
echo "  ./telemetry/scripts/start-telemetry-stack.sh"
echo ""
echo "To remove all data volumes:"
echo "  docker-compose -f docker-compose.telemetry.yml down -v"
