# SpacetimeDB Telemetry Integration

This setup integrates SpacetimeDB with a complete OpenTelemetry observability stack, providing metrics collection, visualization, and trace readiness for future OpenTelemetry instrumentation.

## 🚀 Quick Start

```bash
# Start the telemetry stack
./telemetry/scripts/start-telemetry-stack.sh

# Verify everything is working
./telemetry/scripts/verify-telemetry.sh

# Stop the stack
./telemetry/scripts/stop-telemetry-stack.sh
```

## 📊 Architecture

The telemetry stack includes:

- **SpacetimeDB**: Configured to expose Prometheus metrics on port 9000
- **OpenTelemetry Collector**: Central telemetry hub for metrics, traces, and logs
- **Prometheus**: Time-series database for metrics storage
- **Grafana**: Visualization and dashboards (port 3001)
- **Jaeger**: Distributed tracing UI (port 16686)

```
SpacetimeDB (metrics:9000) → OTEL Collector → Prometheus → Grafana
                                    ↓
                                  Jaeger (traces)
                                    ↓
                              File exporters (testing)
```

## 🔗 Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| SpacetimeDB | http://localhost:3000 | - |
| Grafana | http://localhost:3001 | admin/admin |
| Prometheus | http://localhost:9090 | - |
| Jaeger | http://localhost:16686 | - |
| OTEL Collector Health | http://localhost:13133 | - |

## 📈 Available Metrics

Currently, SpacetimeDB exposes standard Prometheus metrics:

- `up` - Service availability
- `process_cpu_seconds_total` - CPU usage
- `process_resident_memory_bytes` - Memory usage
- Additional metrics as configured in SpacetimeDB

## 🔧 Configuration

### Environment Variables

Set these in `.env` or export before starting:

```bash
SPACETIMEDB_VERSION=latest
SPACETIMEDB_PORT=3000
SPACETIMEDB_LOG_LEVEL=info
RUST_LOG=info,spacetimedb=debug
```

### OTEL Collector Configuration

The collector is configured to:
- Receive OTLP data on ports 4317 (gRPC) and 4318 (HTTP)
- Scrape Prometheus metrics from SpacetimeDB
- Export to Prometheus, Jaeger, and file storage
- Apply SpacetimeDB-specific attributes and filtering

See `telemetry/configs/otel/otel-collector-config.yaml` for details.

## 📁 Directory Structure

```
telemetry/
├── configs/
│   ├── otel/                 # OpenTelemetry Collector config
│   ├── prometheus/           # Prometheus scrape config
│   └── grafana/             # Grafana provisioning
│       ├── provisioning/
│       └── dashboards/      # Pre-built dashboards
├── data/                    # Telemetry data storage
│   ├── logs/               # Log files
│   ├── metrics/            # Metric exports
│   └── traces/             # Trace exports
└── scripts/                # Helper scripts
```

## 🎯 Current Capabilities

### ✅ What's Working Now

1. **Metrics Collection**: SpacetimeDB's built-in Prometheus metrics are collected
2. **Visualization**: Basic Grafana dashboard shows service health, CPU, and memory
3. **Infrastructure**: Full OTEL pipeline ready for traces and logs
4. **Persistence**: Metrics stored in Prometheus and file exports

### 🚧 Future Enhancements

Based on the telemetry tasks plan:

1. **OpenTelemetry SDK Integration** (Task 1-2)
   - Add OTLP trace export from SpacetimeDB
   - Integrate with existing tracing infrastructure

2. **Core Instrumentation** (Task 3-4)
   - Database operation spans
   - WASM module execution tracing
   - Query performance metrics

3. **Advanced Features** (Task 5-8)
   - API endpoint tracing
   - Custom SpacetimeDB metrics
   - Distributed tracing support

## 🧪 Testing Telemetry

### Verify Metrics Collection

```bash
# Check if metrics are being scraped
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job=="spacetimedb")'

# Query SpacetimeDB metrics
curl "http://localhost:9090/api/v1/query?query=up{job='spacetimedb'}"
```

### Generate Test Load

```bash
# Create a test module and publish it
docker exec spacetimedb-telemetry spacetime new mymodule
docker exec spacetimedb-telemetry spacetime publish mymodule
```

### View in Grafana

1. Open http://localhost:3001 (admin/admin)
2. Navigate to Dashboards → SpacetimeDB → SpacetimeDB Overview
3. See real-time metrics visualization

## 🛠️ Troubleshooting

### Services Not Starting

```bash
# Check service logs
docker-compose -f docker-compose.telemetry.yml logs [service-name]

# Verify network exists
docker network ls | grep spacetimedb-telemetry-network

# Check port conflicts
lsof -i :3000,3001,9090,16686,4317,4318
```

### No Metrics Showing

1. Verify SpacetimeDB is exposing metrics:
   ```bash
   curl http://localhost:9000/metrics
   ```

2. Check Prometheus targets:
   - Open http://localhost:9090/targets
   - Ensure spacetimedb target is "UP"

3. Check OTEL Collector logs:
   ```bash
   docker logs spacetimedb-otel-collector
   ```

### Data Persistence

Telemetry data is stored in Docker volumes and local directories:
- Prometheus data: `spacetimedb-prometheus-data` volume
- Grafana config: `spacetimedb-grafana-data` volume
- File exports: `telemetry/data/` directory

To reset all data:
```bash
docker-compose -f docker-compose.telemetry.yml down -v
rm -rf telemetry/data/*
```

## 📚 Next Steps

1. **For Development**: Use this stack to monitor SpacetimeDB during development
2. **For Testing**: File exporters in `telemetry/data/` can be used for integration tests
3. **For Production**: Adapt configuration for production requirements (retention, security, etc.)

## 🔗 References

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
