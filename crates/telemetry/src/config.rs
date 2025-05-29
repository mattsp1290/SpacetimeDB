//! Telemetry configuration structures and utilities

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main telemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Tracing configuration
    pub tracing: TracingConfig,
    /// Metrics configuration
    pub metrics: MetricsConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            tracing: TracingConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Whether tracing is enabled
    pub enabled: bool,
    /// Service name for traces
    pub service_name: String,
    /// Log level filter
    pub log_level: String,
    /// Tracing exporter configuration
    pub exporter: TracingExporter,
    /// Sampling configuration
    pub sampling: SamplingConfig,
    /// Maximum number of events per span
    pub max_events_per_span: u32,
    /// Maximum number of attributes per span
    pub max_attributes_per_span: u32,
    /// Export timeout in seconds
    pub export_timeout_seconds: u64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "spacetimedb".to_string(),
            log_level: "info".to_string(),
            exporter: TracingExporter::default(),
            sampling: SamplingConfig::default(),
            max_events_per_span: 128,
            max_attributes_per_span: 128,
            export_timeout_seconds: 30,
        }
    }
}

/// Tracing exporter types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TracingExporter {
    /// OpenTelemetry Protocol (OTLP) exporter
    Otlp {
        /// OTLP endpoint
        endpoint: String,
        /// Optional headers for authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<HashMap<String, String>>,
    },
    /// Jaeger exporter
    Jaeger {
        /// Jaeger agent endpoint
        endpoint: String,
    },
    /// No exporter (useful for testing)
    None,
}

impl Default for TracingExporter {
    fn default() -> Self {
        TracingExporter::None
    }
}

/// Sampling configuration for tracing
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SamplingConfig {
    /// Sampling strategy: "always_on", "always_off", "trace_id_ratio"
    pub strategy: String,
    /// Sampling rate (0.0 to 1.0) for trace_id_ratio strategy
    pub rate: f64,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            strategy: "always_on".to_string(),
            rate: 1.0,
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether metrics are enabled
    pub enabled: bool,
    /// Metrics exporter configuration
    pub exporter: MetricsExporter,
    /// Export interval in seconds
    pub export_interval_seconds: u64,
    /// Export timeout in seconds
    pub export_timeout_seconds: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: MetricsExporter::default(),
            export_interval_seconds: 60,
            export_timeout_seconds: 30,
        }
    }
}

/// Metrics exporter types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetricsExporter {
    /// Prometheus exporter (pull-based)
    Prometheus {
        /// HTTP endpoint for Prometheus scraping
        endpoint: String,
    },
    /// OpenTelemetry Protocol (OTLP) exporter (push-based)
    Otlp {
        /// OTLP endpoint
        endpoint: String,
        /// Optional headers for authentication
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<HashMap<String, String>>,
    },
    /// No exporter
    None,
}

impl Default for MetricsExporter {
    fn default() -> Self {
        MetricsExporter::None
    }
}

/// Load telemetry configuration from environment variables
impl TelemetryConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Tracing configuration
        if let Ok(enabled) = std::env::var("SPACETIMEDB_TELEMETRY_TRACING_ENABLED") {
            config.tracing.enabled = enabled.parse().unwrap_or(false);
        }

        if let Ok(service_name) = std::env::var("SPACETIMEDB_TELEMETRY_SERVICE_NAME") {
            config.tracing.service_name = service_name;
        }

        if let Ok(log_level) = std::env::var("SPACETIMEDB_TELEMETRY_LOG_LEVEL") {
            config.tracing.log_level = log_level;
        }

        // Tracing exporter
        if let Ok(exporter_type) = std::env::var("SPACETIMEDB_TELEMETRY_TRACING_EXPORTER") {
            match exporter_type.as_str() {
                "otlp" => {
                    let endpoint = std::env::var("SPACETIMEDB_TELEMETRY_OTLP_ENDPOINT")
                        .unwrap_or_else(|_| "http://localhost:4317".to_string());
                    
                    let headers = Self::parse_headers_env("SPACETIMEDB_TELEMETRY_OTLP_HEADERS");
                    
                    config.tracing.exporter = TracingExporter::Otlp { endpoint, headers };
                }
                "jaeger" => {
                    let endpoint = std::env::var("SPACETIMEDB_TELEMETRY_JAEGER_ENDPOINT")
                        .unwrap_or_else(|_| "localhost:6831".to_string());
                    
                    config.tracing.exporter = TracingExporter::Jaeger { endpoint };
                }
                _ => {}
            }
        }

        // Sampling configuration
        if let Ok(strategy) = std::env::var("SPACETIMEDB_TELEMETRY_SAMPLING_STRATEGY") {
            config.tracing.sampling.strategy = strategy;
        }

        if let Ok(rate) = std::env::var("SPACETIMEDB_TELEMETRY_SAMPLING_RATE") {
            if let Ok(rate) = rate.parse() {
                config.tracing.sampling.rate = rate;
            }
        }

        // Metrics configuration
        if let Ok(enabled) = std::env::var("SPACETIMEDB_TELEMETRY_METRICS_ENABLED") {
            config.metrics.enabled = enabled.parse().unwrap_or(false);
        }

        // Metrics exporter
        if let Ok(exporter_type) = std::env::var("SPACETIMEDB_TELEMETRY_METRICS_EXPORTER") {
            match exporter_type.as_str() {
                "prometheus" => {
                    let endpoint = std::env::var("SPACETIMEDB_TELEMETRY_PROMETHEUS_ENDPOINT")
                        .unwrap_or_else(|_| "/metrics".to_string());
                    
                    config.metrics.exporter = MetricsExporter::Prometheus { endpoint };
                }
                "otlp" => {
                    let endpoint = std::env::var("SPACETIMEDB_TELEMETRY_METRICS_OTLP_ENDPOINT")
                        .unwrap_or_else(|_| "http://localhost:4317".to_string());
                    
                    let headers = Self::parse_headers_env("SPACETIMEDB_TELEMETRY_METRICS_OTLP_HEADERS");
                    
                    config.metrics.exporter = MetricsExporter::Otlp { endpoint, headers };
                }
                _ => {}
            }
        }

        config
    }

    /// Parse headers from environment variable (format: "key1=value1,key2=value2")
    fn parse_headers_env(env_var: &str) -> Option<HashMap<String, String>> {
        std::env::var(env_var).ok().map(|headers_str| {
            headers_str
                .split(',')
                .filter_map(|header| {
                    let parts: Vec<&str> = header.split('=').collect();
                    if parts.len() == 2 {
                        Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                    } else {
                        None
                    }
                })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TelemetryConfig::default();
        assert!(!config.tracing.enabled);
        assert!(!config.metrics.enabled);
        assert_eq!(config.tracing.service_name, "spacetimedb");
    }

    #[test]
    fn test_config_serialization() {
        let config = TelemetryConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: TelemetryConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.tracing.service_name, deserialized.tracing.service_name);
    }
}
