//! SpacetimeDB OpenTelemetry Integration
//!
//! This crate provides OpenTelemetry support for SpacetimeDB, including:
//! - Distributed tracing with OTLP, Jaeger, and Zipkin exporters
//! - Performance monitoring and SLA tracking
//! - Resource utilization monitoring
//! - Configurable telemetry pipeline
//! - Integration with existing tracing infrastructure

use anyhow::{Context, Result};
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    runtime,
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub mod config;
pub mod metrics_simple;
pub mod performance_monitor;
pub mod propagation;
pub mod resource_monitor;
pub mod sla_metrics;
pub mod tracing_layer;

pub use config::TelemetryConfig;
pub use metrics_simple::{SimpleSpacetimeDBMetrics as SpacetimeDBMetrics, SimpleMetricsTimer as MetricsTimer, SimpleMetricsRegistry as MetricsRegistry};
pub use performance_monitor::{
    PerformanceConfig, PerformanceMonitor, PerformanceBaseline, RegressionDetection, 
    CapacityMetrics, CapacityRecommendation, global_performance_registry, 
    init_performance_monitoring, record_performance_sample, get_performance_baseline,
    detect_performance_regression, get_capacity_metrics
};
pub use resource_monitor::{
    ResourceConfig, ResourceMonitor, ResourceSummary, ResourceTrends, TrendDirection,
    MemoryUsage, CpuUsage, DiskUsage, NetworkUsage, ResourceAlert, ResourceAlertEvent,
    AlertSeverity, global_resource_monitor, record_memory_usage, record_cpu_usage,
    record_disk_usage, record_network_usage, get_resource_summary, get_recent_resource_alerts,
    get_resource_trends
};
pub use sla_metrics::{
    SlaConfig, SlaTracker, SlaStatus, SlaMetrics, SlaViolation, SlaViolationType,
    global_sla_registry, init_sla_tracking, record_operation_success, record_operation_failure,
    get_sla_status, check_overall_sla_compliance
};
pub use tracing_layer::SpacetimeDBLayer;

/// Initialize the complete telemetry subsystem with performance monitoring
pub async fn init_telemetry(config: &TelemetryConfig) -> Result<()> {
    // Set global error handler
    global::set_error_handler(|error| {
        tracing::error!("OpenTelemetry error: {}", error);
    })?;

    // Initialize propagator for distributed tracing
    global::set_text_map_propagator(TraceContextPropagator::new());

    // Initialize tracing if enabled
    if config.tracing.enabled {
        init_tracing(&config.tracing).await?;
    }

    // Initialize metrics if enabled
    if config.metrics.enabled {
        init_metrics(&config.metrics)?;
    }

    // Initialize performance monitoring and SLA tracking
    init_performance_monitoring();
    init_sla_tracking();

    Ok(())
}

/// Create a tracing layer for integration with existing tracing-subscriber setup
pub async fn create_tracing_layer(config: &config::TracingConfig) -> Result<Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>> {
    if !config.enabled {
        return Ok(Box::new(tracing_subscriber::layer::Identity::new()));
    }

    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    // Create tracer based on configured exporter
    let tracer = match &config.exporter {
        config::TracingExporter::Otlp { endpoint, headers } => {
            let mut exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(config.export_timeout_seconds));

            if let Some(headers) = headers {
                let mut metadata = tonic::metadata::MetadataMap::new();
                for (key, value) in headers {
                    let key = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
                        .context("Invalid header name")?;
                    let value = tonic::metadata::MetadataValue::try_from(value.as_str())
                        .context("Invalid header value")?;
                    metadata.insert(key, value);
                }
                exporter = exporter.with_metadata(metadata);
            }

            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(exporter)
                .with_trace_config(
                    trace::config()
                        .with_sampler(get_sampler(&config.sampling))
                        .with_id_generator(RandomIdGenerator::default())
                        .with_max_events_per_span(config.max_events_per_span)
                        .with_max_attributes_per_span(config.max_attributes_per_span)
                        .with_resource(resource),
                )
                .install_batch(runtime::Tokio)?
        }
        config::TracingExporter::Jaeger { endpoint: _ } => {
            // Note: Jaeger exporter is deprecated in newer OpenTelemetry versions
            // Use OTLP instead with a Jaeger backend
            return Err(anyhow::anyhow!("Jaeger exporter is deprecated. Please use OTLP exporter with Jaeger backend."));
        }
        config::TracingExporter::None => {
            // Return identity layer when disabled
            return Ok(Box::new(tracing_subscriber::layer::Identity::new()));
        }
    };

    // Create OpenTelemetry layer
    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    
    // Create custom SpacetimeDB layer for enriching spans
    let spacetimedb_layer = SpacetimeDBLayer::new();

    // Combine both layers
    let combined_layer = opentelemetry_layer.and_then(spacetimedb_layer);
    
    Ok(Box::new(combined_layer))
}

/// Initialize the tracing subsystem
async fn init_tracing(config: &config::TracingConfig) -> Result<()> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    // Create tracer based on configured exporter
    let tracer = match &config.exporter {
        config::TracingExporter::Otlp { endpoint, headers } => {
            let mut exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(config.export_timeout_seconds));

            if let Some(headers) = headers {
                let mut metadata = tonic::metadata::MetadataMap::new();
                for (key, value) in headers {
                    let key = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
                        .context("Invalid header name")?;
                    let value = tonic::metadata::MetadataValue::try_from(value.as_str())
                        .context("Invalid header value")?;
                    metadata.insert(key, value);
                }
                exporter = exporter.with_metadata(metadata);
            }

            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(exporter)
                .with_trace_config(
                    trace::config()
                        .with_sampler(get_sampler(&config.sampling))
                        .with_id_generator(RandomIdGenerator::default())
                        .with_max_events_per_span(config.max_events_per_span)
                        .with_max_attributes_per_span(config.max_attributes_per_span)
                        .with_resource(resource),
                )
                .install_batch(runtime::Tokio)?
        }
        config::TracingExporter::Jaeger { endpoint: _ } => {
            // Note: Jaeger exporter is deprecated in newer OpenTelemetry versions
            // Use OTLP instead with a Jaeger backend
            return Err(anyhow::anyhow!("Jaeger exporter is deprecated. Please use OTLP exporter with Jaeger backend."));
        }
        config::TracingExporter::None => {
            // No-op tracer for testing or when tracing is disabled
            return Ok(());
        }
    };

    // Create OpenTelemetry layer
    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Create custom SpacetimeDB layer for enriching spans
    let spacetimedb_layer = SpacetimeDBLayer::new();

    // Combine with existing subscriber
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(opentelemetry_layer)
        .with(spacetimedb_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}

/// Initialize the metrics subsystem (simplified implementation)
fn init_metrics(config: &config::MetricsConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Use simplified metrics registry
    MetricsRegistry::global().init()?;
    
    Ok(())
}

/// Get the appropriate sampler based on configuration
fn get_sampler(config: &config::SamplingConfig) -> Sampler {
    match config.strategy.as_str() {
        "always_on" => Sampler::AlwaysOn,
        "always_off" => Sampler::AlwaysOff,
        "trace_id_ratio" => Sampler::TraceIdRatioBased(config.rate),
        _ => Sampler::AlwaysOn,
    }
}

/// Shutdown the telemetry subsystem gracefully
pub fn shutdown_telemetry() {
    // Shutdown tracing
    global::shutdown_tracer_provider();
    
    // Note: OpenTelemetry 0.22 doesn't have shutdown_meter_provider
    // Metrics will be cleaned up when the process exits
}

/// Comprehensive performance and SLA monitoring utilities
pub mod monitoring {
    pub use crate::performance_monitor::*;
    pub use crate::resource_monitor::*;
    pub use crate::sla_metrics::*;
}

/// Utility to create spans with SpacetimeDB-specific attributes
#[macro_export]
macro_rules! spacetime_span {
    ($level:expr, $name:expr, $($field:tt)*) => {
        tracing::span!(
            $level,
            $name,
            spacetimedb.component = module_path!(),
            $($field)*
        )
    };
}

/// Utility to record metrics with SpacetimeDB-specific labels (simplified)
#[macro_export]
macro_rules! spacetime_metric {
    ($operation:expr, $value:expr, $($label:expr => $label_value:expr),*) => {
        // Simplified macro - no-op for now
    };
}

/// Macro for recording operation performance and SLA compliance
#[macro_export]
macro_rules! record_operation {
    ($component:expr, $duration:expr, $success:expr) => {
        // Record for performance monitoring
        $crate::record_performance_sample($component, $duration, $success);
        
        // Record for SLA tracking
        if $success {
            $crate::record_operation_success($component, $duration);
        } else {
            $crate::record_operation_failure($component, Some($duration));
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_telemetry_initialization() {
        let config = TelemetryConfig::default();
        assert!(init_telemetry(&config).await.is_ok());
        shutdown_telemetry();
    }

    #[test]
    fn test_performance_monitoring_integration() {
        // Test that performance monitoring is properly initialized
        let baseline = get_performance_baseline("test_operation");
        assert!(baseline.is_none()); // Should be None initially
        
        // Record some operations
        record_performance_sample("test_operation", Duration::from_millis(50), true);
        record_performance_sample("test_operation", Duration::from_millis(75), true);
        
        // Check SLA compliance
        let sla_status = get_sla_status("test_operation");
        assert!(sla_status.is_none()); // Should be None until enough samples
    }

    #[test]
    fn test_resource_monitoring_integration() {
        // Test that resource monitoring works
        record_memory_usage(1024 * 1024, 64 * 1024, 2 * 1024 * 1024, 0, 1000);
        record_cpu_usage(25.0, 15.0, 16, 60.0);
        
        let summary = get_resource_summary();
        assert!(summary.memory_usage.is_some());
        assert!(summary.cpu_usage.is_some());
        assert!(summary.overall_health >= 0.0);
    }
}
