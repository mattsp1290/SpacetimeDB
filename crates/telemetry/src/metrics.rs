//! OpenTelemetry metrics integration for SpacetimeDB
//!
//! This module provides OpenTelemetry metrics capabilities alongside the existing 
//! Prometheus metrics, enabling dual export and vendor-neutral observability.

use anyhow::Result;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, Unit, UpDownCounter},
    KeyValue,
};
use opentelemetry_prometheus::PrometheusExporter;
use opentelemetry_sdk::{
    metrics::{MeterProvider, PeriodicReader, SdkMeterProvider},
    runtime,
    Resource,
};
use prometheus::{Registry as PrometheusRegistry};
use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use crate::config::MetricsConfig;

/// Global metrics registry
static REGISTRY: OnceLock<MetricsRegistry> = OnceLock::new();

/// SpacetimeDB OpenTelemetry metrics registry
/// 
/// This registry provides OpenTelemetry metrics that complement the existing
/// Prometheus metrics system. It supports dual export to both Prometheus
/// and OTLP backends.
pub struct MetricsRegistry {
    meter: Meter,
    meter_provider: Option<SdkMeterProvider>,
    prometheus_exporter: Option<PrometheusExporter>,
    initialized: Arc<Mutex<bool>>,
    
    // Database operation metrics
    pub database_operations_total: Counter<u64>,
    pub database_operation_duration: Histogram<f64>,
    pub active_database_connections: UpDownCounter<i64>,
    pub query_execution_time: Histogram<f64>,
    pub transaction_duration: Histogram<f64>,
    
    // WASM module metrics
    pub wasm_module_loads: Counter<u64>,
    pub wasm_module_load_duration: Histogram<f64>,
    pub wasm_reducer_calls: Counter<u64>,
    pub wasm_reducer_duration: Histogram<f64>,
    pub wasm_memory_usage: UpDownCounter<i64>,
    pub wasm_execution_errors: Counter<u64>,
    
    // API endpoint metrics
    pub http_requests_total: Counter<u64>,
    pub http_request_duration: Histogram<f64>,
    pub websocket_connections_active: UpDownCounter<i64>,
    pub websocket_messages_total: Counter<u64>,
    pub websocket_message_size: Histogram<f64>,
    
    // System resource metrics
    pub system_memory_usage: UpDownCounter<i64>,
    pub system_cpu_usage: Histogram<f64>,
    pub system_disk_usage: UpDownCounter<i64>,
    pub system_network_bytes: Counter<u64>,
    
    // Performance and SLA metrics
    pub request_latency_p50: Histogram<f64>,
    pub request_latency_p95: Histogram<f64>,
    pub request_latency_p99: Histogram<f64>,
    pub error_rate: Counter<u64>,
    pub availability_uptime: UpDownCounter<i64>,
}

impl MetricsRegistry {
    /// Get the global metrics registry
    pub fn global() -> &'static MetricsRegistry {
        REGISTRY.get_or_init(|| Self::new())
    }
    
    /// Create a new metrics registry
    fn new() -> Self {
        let meter = global::meter("spacetimedb");
        
        Self {
            meter: meter.clone(),
            meter_provider: None,
            prometheus_exporter: None,
            initialized: Arc::new(Mutex::new(false)),
            
            // Database operation metrics
            database_operations_total: meter
                .u64_counter("spacetimedb.database.operations.total")
                .with_description("Total number of database operations")
                .init(),
                
            database_operation_duration: meter
                .f64_histogram("spacetimedb.database.operation.duration")
                .with_description("Database operation duration in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            active_database_connections: meter
                .i64_up_down_counter("spacetimedb.database.connections.active")
                .with_description("Number of active database connections")
                .init(),
                
            query_execution_time: meter
                .f64_histogram("spacetimedb.database.query.duration")
                .with_description("Query execution time in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            transaction_duration: meter
                .f64_histogram("spacetimedb.database.transaction.duration")
                .with_description("Transaction duration in seconds")
                .with_unit(Unit::new("s"))
                .init(),
            
            // WASM module metrics
            wasm_module_loads: meter
                .u64_counter("spacetimedb.wasm.module.loads.total")
                .with_description("Total number of WASM module loads")
                .init(),
                
            wasm_module_load_duration: meter
                .f64_histogram("spacetimedb.wasm.module.load.duration")
                .with_description("WASM module load duration in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            wasm_reducer_calls: meter
                .u64_counter("spacetimedb.wasm.reducer.calls.total")
                .with_description("Total number of WASM reducer calls")
                .init(),
                
            wasm_reducer_duration: meter
                .f64_histogram("spacetimedb.wasm.reducer.duration")
                .with_description("WASM reducer execution duration in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            wasm_memory_usage: meter
                .i64_up_down_counter("spacetimedb.wasm.memory.usage.bytes")
                .with_description("WASM module memory usage in bytes")
                .with_unit(Unit::new("By"))
                .init(),
                
            wasm_execution_errors: meter
                .u64_counter("spacetimedb.wasm.execution.errors.total")
                .with_description("Total number of WASM execution errors")
                .init(),
            
            // API endpoint metrics
            http_requests_total: meter
                .u64_counter("spacetimedb.http.requests.total")
                .with_description("Total number of HTTP requests")
                .init(),
                
            http_request_duration: meter
                .f64_histogram("spacetimedb.http.request.duration")
                .with_description("HTTP request duration in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            websocket_connections_active: meter
                .i64_up_down_counter("spacetimedb.websocket.connections.active")
                .with_description("Number of active WebSocket connections")
                .init(),
                
            websocket_messages_total: meter
                .u64_counter("spacetimedb.websocket.messages.total")
                .with_description("Total number of WebSocket messages")
                .init(),
                
            websocket_message_size: meter
                .f64_histogram("spacetimedb.websocket.message.size.bytes")
                .with_description("WebSocket message size in bytes")
                .with_unit(Unit::new("By"))
                .init(),
            
            // System resource metrics
            system_memory_usage: meter
                .i64_up_down_counter("spacetimedb.system.memory.usage.bytes")
                .with_description("System memory usage in bytes")
                .with_unit(Unit::new("By"))
                .init(),
                
            system_cpu_usage: meter
                .f64_histogram("spacetimedb.system.cpu.usage.ratio")
                .with_description("System CPU usage ratio (0.0-1.0)")
                .init(),
                
            system_disk_usage: meter
                .i64_up_down_counter("spacetimedb.system.disk.usage.bytes")
                .with_description("System disk usage in bytes")
                .with_unit(Unit::new("By"))
                .init(),
                
            system_network_bytes: meter
                .u64_counter("spacetimedb.system.network.bytes.total")
                .with_description("Total network bytes transferred")
                .with_unit(Unit::new("By"))
                .init(),
            
            // Performance and SLA metrics
            request_latency_p50: meter
                .f64_histogram("spacetimedb.request.latency.p50")
                .with_description("Request latency 50th percentile in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            request_latency_p95: meter
                .f64_histogram("spacetimedb.request.latency.p95")
                .with_description("Request latency 95th percentile in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            request_latency_p99: meter
                .f64_histogram("spacetimedb.request.latency.p99")
                .with_description("Request latency 99th percentile in seconds")
                .with_unit(Unit::new("s"))
                .init(),
                
            error_rate: meter
                .u64_counter("spacetimedb.errors.total")
                .with_description("Total number of errors")
                .init(),
                
            availability_uptime: meter
                .i64_up_down_counter("spacetimedb.availability.uptime.seconds")
                .with_description("Service uptime in seconds")
                .with_unit(Unit::new("s"))
                .init(),
        }
    }
    
    /// Initialize the metrics registry with the given configuration
    pub fn init_with_config(&self, config: &MetricsConfig) -> Result<()> {
        let mut initialized = self.initialized.lock().unwrap();
        if *initialized {
            return Ok(());
        }

        match &config.exporter {
            crate::config::MetricsExporter::Prometheus { endpoint: _ } => {
                self.init_prometheus_exporter()?;
            }
            crate::config::MetricsExporter::Otlp { endpoint, headers: _ } => {
                self.init_otlp_exporter(endpoint, config.export_interval_seconds)?;
            }
            crate::config::MetricsExporter::None => {
                // No-op metrics initialization
            }
        }

        *initialized = true;
        Ok(())
    }
    
    /// Initialize the metrics registry (default behavior for backward compatibility)
    pub fn init(&self) -> Result<()> {
        self.init_prometheus_exporter()
    }
    
    /// Initialize Prometheus exporter for dual metrics export
    fn init_prometheus_exporter(&self) -> Result<()> {
        let prometheus_registry = PrometheusRegistry::new();
        
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(prometheus_registry.clone())
            .build()?;
        
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(exporter.clone())
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", "spacetimedb"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]))
            .build();
        
        global::set_meter_provider(meter_provider.clone());
        
        // Store the exporter and provider for later use
        // Note: We need to store these to prevent them from being dropped
        // This is a limitation of the current implementation
        
        Ok(())
    }
    
    /// Initialize OTLP exporter for OpenTelemetry metrics
    fn init_otlp_exporter(&self, endpoint: &str, interval_seconds: u64) -> Result<()> {
        let exporter = opentelemetry_otlp::new_exporter()
            .http()
            .with_endpoint(format!("{}/v1/metrics", endpoint));
        
        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_interval(Duration::from_secs(interval_seconds))
            .build();
        
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(Resource::new(vec![
                KeyValue::new("service.name", "spacetimedb"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ]))
            .build();
        
        global::set_meter_provider(meter_provider);
        
        Ok(())
    }
    
    /// Get the Prometheus registry for integration with existing metrics
    pub fn prometheus_registry(&self) -> Option<&PrometheusRegistry> {
        self.prometheus_exporter.as_ref().map(|_| {
            // In a real implementation, we'd return the actual registry
            // For now, this is a placeholder
            unimplemented!("Prometheus registry access needs to be implemented")
        })
    }
}

/// Bridge trait for integrating existing Prometheus metrics with OpenTelemetry
pub trait PrometheusMetricsBridge {
    /// Convert Prometheus metrics to OpenTelemetry format
    fn bridge_to_opentelemetry(&self);
}

/// Helper macros for recording metrics with SpacetimeDB-specific labels

/// Record a database operation metric
#[macro_export]
macro_rules! record_db_operation {
    ($metric:expr, $value:expr, $operation:expr, $database_id:expr) => {{
        use opentelemetry::KeyValue;
        $metric.add(
            $value,
            &[
                KeyValue::new("operation", $operation),
                KeyValue::new("database_id", $database_id),
                KeyValue::new("component", "database"),
            ],
        );
    }};
}

/// Record a WASM module metric
#[macro_export]
macro_rules! record_wasm_metric {
    ($metric:expr, $value:expr, $module_hash:expr, $reducer:expr) => {{
        use opentelemetry::KeyValue;
        $metric.add(
            $value,
            &[
                KeyValue::new("module_hash", $module_hash),
                KeyValue::new("reducer", $reducer),
                KeyValue::new("component", "wasm"),
            ],
        );
    }};
}

/// Record an HTTP API metric
#[macro_export]
macro_rules! record_http_api_metric {
    ($metric:expr, $value:expr, $method:expr, $path:expr, $status:expr) => {{
        use opentelemetry::KeyValue;
        $metric.add(
            $value,
            &[
                KeyValue::new("http.method", $method),
                KeyValue::new("http.route", $path),
                KeyValue::new("http.status_code", $status as i64),
                KeyValue::new("component", "http_api"),
            ],
        );
    }};
}

/// Record a WebSocket metric
#[macro_export]
macro_rules! record_websocket_metric {
    ($metric:expr, $value:expr, $database_id:expr, $event_type:expr) => {{
        use opentelemetry::KeyValue;
        $metric.add(
            $value,
            &[
                KeyValue::new("database_id", $database_id),
                KeyValue::new("event_type", $event_type),
                KeyValue::new("component", "websocket"),
            ],
        );
    }};
}

/// Record a system resource metric
#[macro_export]
macro_rules! record_system_metric {
    ($metric:expr, $value:expr, $resource:expr) => {{
        use opentelemetry::KeyValue;
        $metric.add(
            $value,
            &[
                KeyValue::new("resource", $resource),
                KeyValue::new("component", "system"),
            ],
        );
    }};
}

/// Record a performance SLA metric
#[macro_export]
macro_rules! record_sla_metric {
    ($metric:expr, $value:expr, $operation_type:expr, $sla_tier:expr) => {{
        use opentelemetry::KeyValue;
        $metric.record(
            $value,
            &[
                KeyValue::new("operation_type", $operation_type),
                KeyValue::new("sla_tier", $sla_tier),
                KeyValue::new("component", "sla"),
            ],
        );
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::KeyValue;

    #[test]
    fn test_metrics_registry_singleton() {
        let registry1 = MetricsRegistry::global();
        let registry2 = MetricsRegistry::global();
        
        // Should be the same instance
        assert!(std::ptr::eq(registry1, registry2));
    }

    #[test]
    fn test_database_metric_recording() {
        let registry = MetricsRegistry::global();
        
        // Test database operation metric
        registry.database_operations_total.add(
            1,
            &[
                KeyValue::new("operation", "insert"),
                KeyValue::new("database_id", "test_db"),
            ],
        );
        
        // Test query duration metric
        registry.query_execution_time.record(
            0.123,
            &[
                KeyValue::new("operation", "select"),
                KeyValue::new("database_id", "test_db"),
            ],
        );
    }

    #[test]
    fn test_wasm_metric_recording() {
        let registry = MetricsRegistry::global();
        
        // Test WASM reducer call metric
        registry.wasm_reducer_calls.add(
            1,
            &[
                KeyValue::new("module_hash", "abc123"),
                KeyValue::new("reducer", "send_message"),
            ],
        );
        
        // Test WASM memory usage metric
        registry.wasm_memory_usage.add(
            1024,
            &[KeyValue::new("module_hash", "abc123")],
        );
    }

    #[test]
    fn test_api_metric_recording() {
        let registry = MetricsRegistry::global();
        
        // Test HTTP request metric
        registry.http_requests_total.add(
            1,
            &[
                KeyValue::new("method", "POST"),
                KeyValue::new("path", "/database/call"),
                KeyValue::new("status", 200_i64),
            ],
        );
        
        // Test WebSocket connection metric
        registry.websocket_connections_active.add(
            1,
            &[KeyValue::new("database_id", "test_db")],
        );
    }
}
