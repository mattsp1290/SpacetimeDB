//! Metrics bridge for integrating OpenTelemetry metrics with existing SpacetimeDB Prometheus metrics
//!
//! This module provides utilities to instrument existing SpacetimeDB operations with
//! OpenTelemetry metrics while maintaining backward compatibility with Prometheus.

use crate::metrics::MetricsRegistry;
use opentelemetry::KeyValue;
use std::time::{Duration, Instant};

/// SpacetimeDB metrics instrumentation utilities
pub struct SpacetimeDBMetrics;

impl SpacetimeDBMetrics {
    /// Record a database operation
    pub fn record_database_operation(
        operation: &str,
        database_id: &str,
        duration: Duration,
        success: bool,
    ) {
        let registry = MetricsRegistry::global();
        
        // Record operation count
        registry.database_operations_total.add(
            1,
            &[
                KeyValue::new("operation", operation),
                KeyValue::new("database_id", database_id),
                KeyValue::new("success", success),
                KeyValue::new("component", "database"),
            ],
        );
        
        // Record operation duration
        registry.database_operation_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("operation", operation),
                KeyValue::new("database_id", database_id),
                KeyValue::new("component", "database"),
            ],
        );
        
        if !success {
            registry.error_rate.add(
                1,
                &[
                    KeyValue::new("operation", operation),
                    KeyValue::new("component", "database"),
                ],
            );
        }
    }
    
    /// Record a query execution
    pub fn record_query_execution(
        database_id: &str,
        query_type: &str,
        duration: Duration,
        rows_affected: Option<u64>,
    ) {
        let registry = MetricsRegistry::global();
        
        let mut labels = vec![
            KeyValue::new("database_id", database_id),
            KeyValue::new("query_type", query_type),
            KeyValue::new("component", "database"),
        ];
        
        if let Some(rows) = rows_affected {
            labels.push(KeyValue::new("rows_affected", rows as i64));
        }
        
        registry.query_execution_time.record(duration.as_secs_f64(), &labels);
    }
    
    /// Record a transaction lifecycle event
    pub fn record_transaction(
        database_id: &str,
        transaction_type: &str,
        duration: Duration,
        outcome: &str,
    ) {
        let registry = MetricsRegistry::global();
        
        registry.transaction_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("database_id", database_id),
                KeyValue::new("transaction_type", transaction_type),
                KeyValue::new("outcome", outcome),
                KeyValue::new("component", "database"),
            ],
        );
    }
    
    /// Record a WASM module load
    pub fn record_wasm_module_load(
        module_hash: &str,
        module_size_bytes: Option<usize>,
        duration: Duration,
        success: bool,
    ) {
        let registry = MetricsRegistry::global();
        
        let mut labels = vec![
            KeyValue::new("module_hash", module_hash),
            KeyValue::new("success", success),
            KeyValue::new("component", "wasm"),
        ];
        
        if let Some(size) = module_size_bytes {
            labels.push(KeyValue::new("module_size_bytes", size as i64));
        }
        
        registry.wasm_module_loads.add(1, &labels);
        registry.wasm_module_load_duration.record(duration.as_secs_f64(), &labels);
        
        if !success {
            registry.wasm_execution_errors.add(
                1,
                &[
                    KeyValue::new("module_hash", module_hash),
                    KeyValue::new("error_type", "module_load_failure"),
                    KeyValue::new("component", "wasm"),
                ],
            );
        }
    }
    
    /// Record a WASM reducer call
    pub fn record_wasm_reducer_call(
        module_hash: &str,
        reducer_name: &str,
        duration: Duration,
        outcome: &str,
        energy_used: Option<u64>,
    ) {
        let registry = MetricsRegistry::global();
        
        let mut labels = vec![
            KeyValue::new("module_hash", module_hash),
            KeyValue::new("reducer_name", reducer_name),
            KeyValue::new("outcome", outcome),
            KeyValue::new("component", "wasm"),
        ];
        
        if let Some(energy) = energy_used {
            labels.push(KeyValue::new("energy_used", energy as i64));
        }
        
        registry.wasm_reducer_calls.add(1, &labels);
        registry.wasm_reducer_duration.record(duration.as_secs_f64(), &labels);
        
        if outcome == "failed" || outcome == "budget_exceeded" {
            registry.wasm_execution_errors.add(
                1,
                &[
                    KeyValue::new("module_hash", module_hash),
                    KeyValue::new("reducer_name", reducer_name),
                    KeyValue::new("error_type", outcome),
                    KeyValue::new("component", "wasm"),
                ],
            );
        }
    }
    
    /// Record WASM memory usage
    pub fn record_wasm_memory_usage(module_hash: &str, memory_bytes: i64) {
        let registry = MetricsRegistry::global();
        
        registry.wasm_memory_usage.add(
            memory_bytes,
            &[
                KeyValue::new("module_hash", module_hash),
                KeyValue::new("component", "wasm"),
            ],
        );
    }
    
    /// Record an HTTP API request
    pub fn record_http_request(
        method: &str,
        path: &str,
        status_code: u16,
        duration: Duration,
        request_size: Option<usize>,
    ) {
        let registry = MetricsRegistry::global();
        
        let mut labels = vec![
            KeyValue::new("method", method),
            KeyValue::new("path", path),
            KeyValue::new("status_code", status_code as i64),
            KeyValue::new("component", "http_api"),
        ];
        
        if let Some(size) = request_size {
            labels.push(KeyValue::new("request_size_bytes", size as i64));
        }
        
        registry.http_requests_total.add(1, &labels);
        registry.http_request_duration.record(duration.as_secs_f64(), &labels);
        
        if status_code >= 400 {
            registry.error_rate.add(
                1,
                &[
                    KeyValue::new("method", method),
                    KeyValue::new("path", path),
                    KeyValue::new("status_code", status_code as i64),
                    KeyValue::new("component", "http_api"),
                ],
            );
        }
    }
    
    /// Track WebSocket connection state
    pub fn track_websocket_connection(database_id: &str, connected: bool) {
        let registry = MetricsRegistry::global();
        
        let delta = if connected { 1 } else { -1 };
        
        registry.websocket_connections_active.add(
            delta,
            &[
                KeyValue::new("database_id", database_id),
                KeyValue::new("component", "websocket"),
            ],
        );
    }
    
    /// Record a WebSocket message
    pub fn record_websocket_message(
        database_id: &str,
        message_type: &str,
        message_size_bytes: usize,
        direction: &str, // "inbound" or "outbound"
    ) {
        let registry = MetricsRegistry::global();
        
        let labels = [
            KeyValue::new("database_id", database_id),
            KeyValue::new("message_type", message_type),
            KeyValue::new("direction", direction),
            KeyValue::new("component", "websocket"),
        ];
        
        registry.websocket_messages_total.add(1, &labels);
        registry.websocket_message_size.record(message_size_bytes as f64, &labels);
    }
    
    /// Record system resource usage
    pub fn record_system_memory_usage(memory_bytes: i64, memory_type: &str) {
        let registry = MetricsRegistry::global();
        
        registry.system_memory_usage.add(
            memory_bytes,
            &[
                KeyValue::new("memory_type", memory_type),
                KeyValue::new("component", "system"),
            ],
        );
    }
    
    /// Record system CPU usage
    pub fn record_system_cpu_usage(cpu_ratio: f64, cpu_type: &str) {
        let registry = MetricsRegistry::global();
        
        registry.system_cpu_usage.record(
            cpu_ratio,
            &[
                KeyValue::new("cpu_type", cpu_type),
                KeyValue::new("component", "system"),
            ],
        );
    }
    
    /// Record request latency for SLA monitoring
    pub fn record_request_latency(
        operation_type: &str,
        latency: Duration,
        percentile: &str, // "p50", "p95", "p99"
    ) {
        let registry = MetricsRegistry::global();
        let latency_seconds = latency.as_secs_f64();
        
        let labels = [
            KeyValue::new("operation_type", operation_type),
            KeyValue::new("component", "sla"),
        ];
        
        match percentile {
            "p50" => registry.request_latency_p50.record(latency_seconds, &labels),
            "p95" => registry.request_latency_p95.record(latency_seconds, &labels),
            "p99" => registry.request_latency_p99.record(latency_seconds, &labels),
            _ => {} // Unknown percentile
        }
    }
    
    /// Record service uptime
    pub fn record_service_uptime(uptime_seconds: i64) {
        let registry = MetricsRegistry::global();
        
        registry.availability_uptime.add(
            uptime_seconds,
            &[KeyValue::new("component", "service")],
        );
    }
}

/// Timing utility for measuring operation duration
pub struct MetricsTimer {
    start: Instant,
    operation: String,
    labels: Vec<KeyValue>,
}

impl MetricsTimer {
    /// Start a new metrics timer
    pub fn new(operation: &str) -> Self {
        Self {
            start: Instant::now(),
            operation: operation.to_string(),
            labels: Vec::new(),
        }
    }
    
    /// Add a label to the timer
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.push(KeyValue::new(key, value));
        self
    }
    
    /// Finish the timer and record the duration
    pub fn finish_with_outcome(self, outcome: &str) -> Duration {
        let duration = self.start.elapsed();
        
        let mut labels = self.labels;
        labels.push(KeyValue::new("outcome", outcome));
        
        // Record to appropriate metric based on operation type
        let registry = MetricsRegistry::global();
        
        if self.operation.starts_with("database") {
            registry.database_operation_duration.record(duration.as_secs_f64(), &labels);
        } else if self.operation.starts_with("wasm") {
            registry.wasm_reducer_duration.record(duration.as_secs_f64(), &labels);
        } else if self.operation.starts_with("http") {
            registry.http_request_duration.record(duration.as_secs_f64(), &labels);
        }
        
        duration
    }
}

/// Macro for easy metrics timing
#[macro_export]
macro_rules! time_operation {
    ($operation:expr, $labels:expr, $code:block) => {{
        let _timer = $crate::metrics_bridge::MetricsTimer::new($operation);
        let result = $code;
        let _duration = _timer.finish_with_outcome("success");
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_database_operation_recording() {
        SpacetimeDBMetrics::record_database_operation(
            "insert",
            "test_db",
            Duration::from_millis(100),
            true,
        );
        
        SpacetimeDBMetrics::record_query_execution(
            "test_db",
            "select",
            Duration::from_millis(50),
            Some(10),
        );
    }

    #[test]
    fn test_wasm_metrics_recording() {
        SpacetimeDBMetrics::record_wasm_module_load(
            "abc123",
            Some(1024),
            Duration::from_millis(200),
            true,
        );
        
        SpacetimeDBMetrics::record_wasm_reducer_call(
            "abc123",
            "send_message",
            Duration::from_millis(10),
            "committed",
            Some(100),
        );
    }

    #[test]
    fn test_api_metrics_recording() {
        SpacetimeDBMetrics::record_http_request(
            "POST",
            "/database/call",
            200,
            Duration::from_millis(150),
            Some(512),
        );
        
        SpacetimeDBMetrics::track_websocket_connection("test_db", true);
        SpacetimeDBMetrics::record_websocket_message("test_db", "subscribe", 256, "inbound");
    }

    #[test]
    fn test_metrics_timer() {
        let timer = MetricsTimer::new("test_operation")
            .with_label("component", "test");
        
        std::thread::sleep(Duration::from_millis(1));
        let duration = timer.finish_with_outcome("success");
        
        assert!(duration.as_millis() >= 1);
    }
}
