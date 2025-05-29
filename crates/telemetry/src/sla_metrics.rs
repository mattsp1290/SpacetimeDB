//! SLA (Service Level Agreement) metrics for SpacetimeDB
//!
//! This module provides comprehensive SLA tracking including:
//! - Response time percentiles (P50, P95, P99)
//! - Availability and uptime monitoring
//! - Error rate tracking
//! - SLA compliance reporting

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// SLA configuration for different service components
#[derive(Debug, Clone)]
pub struct SlaConfig {
    /// Target response time thresholds in milliseconds
    pub response_time_targets: SlaThresholds,
    /// Target availability percentage (0.0 to 1.0)
    pub availability_target: f64,
    /// Target error rate percentage (0.0 to 1.0)
    pub error_rate_target: f64,
    /// Window size for SLA calculations in seconds
    pub measurement_window_seconds: u64,
}

impl Default for SlaConfig {
    fn default() -> Self {
        Self {
            response_time_targets: SlaThresholds {
                p50_ms: 50.0,
                p95_ms: 200.0,
                p99_ms: 500.0,
            },
            availability_target: 0.999, // 99.9%
            error_rate_target: 0.01,    // 1%
            measurement_window_seconds: 300, // 5 minutes
        }
    }
}

/// Response time SLA thresholds
#[derive(Debug, Clone)]
pub struct SlaThresholds {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

/// SLA compliance status for a service component
#[derive(Debug, Clone)]
pub struct SlaStatus {
    pub component: String,
    pub is_compliant: bool,
    pub current_metrics: SlaMetrics,
    pub violations: Vec<SlaViolation>,
}

/// Current SLA metrics for a component
#[derive(Debug, Clone)]
pub struct SlaMetrics {
    pub response_time_p50_ms: f64,
    pub response_time_p95_ms: f64,
    pub response_time_p99_ms: f64,
    pub availability_percentage: f64,
    pub error_rate_percentage: f64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub measurement_period_seconds: u64,
}

/// SLA violation record
#[derive(Debug, Clone)]
pub struct SlaViolation {
    pub timestamp: SystemTime,
    pub component: String,
    pub violation_type: SlaViolationType,
    pub threshold: f64,
    pub actual_value: f64,
    pub duration_seconds: u64,
}

/// Types of SLA violations
#[derive(Debug, Clone, PartialEq)]
pub enum SlaViolationType {
    ResponseTimeP50,
    ResponseTimeP95,
    ResponseTimeP99,
    Availability,
    ErrorRate,
}

/// Thread-safe SLA tracker for a service component
pub struct SlaTracker {
    component_name: String,
    config: SlaConfig,
    start_time: Instant,
    
    // Atomic counters for basic metrics
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    
    // Response time tracking (simplified histogram)
    response_times: Arc<Mutex<Vec<f64>>>,
    
    // Violation tracking
    violations: Arc<Mutex<Vec<SlaViolation>>>,
    
    // Last calculation time
    last_calculation: AtomicU64,
}

impl SlaTracker {
    /// Create a new SLA tracker for a component
    pub fn new(component_name: String, config: SlaConfig) -> Self {
        Self {
            component_name,
            config,
            start_time: Instant::now(),
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            response_times: Arc::new(Mutex::new(Vec::new())),
            violations: Arc::new(Mutex::new(Vec::new())),
            last_calculation: AtomicU64::new(0),
        }
    }

    /// Record a successful request with response time
    pub fn record_success(&self, response_time: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        
        let response_time_ms = response_time.as_secs_f64() * 1000.0;
        
        // Store response time (with limit to prevent memory growth)
        if let Ok(mut times) = self.response_times.lock() {
            times.push(response_time_ms);
            
            // Keep only recent measurements (sliding window)
            if times.len() > 10000 {
                times.drain(0..5000); // Remove oldest half
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&self, response_time: Option<Duration>) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
        
        if let Some(duration) = response_time {
            let response_time_ms = duration.as_secs_f64() * 1000.0;
            if let Ok(mut times) = self.response_times.lock() {
                times.push(response_time_ms);
                
                if times.len() > 10000 {
                    times.drain(0..5000);
                }
            }
        }
    }

    /// Calculate current SLA metrics
    pub fn calculate_metrics(&self) -> SlaMetrics {
        let total = self.total_requests.load(Ordering::Relaxed);
        let successful = self.successful_requests.load(Ordering::Relaxed);
        let failed = self.failed_requests.load(Ordering::Relaxed);
        
        let availability = if total > 0 {
            successful as f64 / total as f64
        } else {
            1.0
        };
        
        let error_rate = if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        };
        
        // Calculate response time percentiles
        let (p50, p95, p99) = self.calculate_percentiles();
        
        let elapsed = self.start_time.elapsed().as_secs();
        
        SlaMetrics {
            response_time_p50_ms: p50,
            response_time_p95_ms: p95,
            response_time_p99_ms: p99,
            availability_percentage: availability * 100.0,
            error_rate_percentage: error_rate * 100.0,
            total_requests: total,
            successful_requests: successful,
            failed_requests: failed,
            measurement_period_seconds: elapsed,
        }
    }

    /// Check SLA compliance and record violations
    pub fn check_compliance(&self) -> SlaStatus {
        let metrics = self.calculate_metrics();
        let mut is_compliant = true;
        let mut current_violations = Vec::new();
        let now = SystemTime::now();
        
        // Check response time SLA
        if metrics.response_time_p50_ms > self.config.response_time_targets.p50_ms {
            is_compliant = false;
            current_violations.push(SlaViolation {
                timestamp: now,
                component: self.component_name.clone(),
                violation_type: SlaViolationType::ResponseTimeP50,
                threshold: self.config.response_time_targets.p50_ms,
                actual_value: metrics.response_time_p50_ms,
                duration_seconds: 0, // Will be calculated by violation tracker
            });
        }
        
        if metrics.response_time_p95_ms > self.config.response_time_targets.p95_ms {
            is_compliant = false;
            current_violations.push(SlaViolation {
                timestamp: now,
                component: self.component_name.clone(),
                violation_type: SlaViolationType::ResponseTimeP95,
                threshold: self.config.response_time_targets.p95_ms,
                actual_value: metrics.response_time_p95_ms,
                duration_seconds: 0,
            });
        }
        
        if metrics.response_time_p99_ms > self.config.response_time_targets.p99_ms {
            is_compliant = false;
            current_violations.push(SlaViolation {
                timestamp: now,
                component: self.component_name.clone(),
                violation_type: SlaViolationType::ResponseTimeP99,
                threshold: self.config.response_time_targets.p99_ms,
                actual_value: metrics.response_time_p99_ms,
                duration_seconds: 0,
            });
        }
        
        // Check availability SLA
        let availability_ratio = metrics.availability_percentage / 100.0;
        if availability_ratio < self.config.availability_target {
            is_compliant = false;
            current_violations.push(SlaViolation {
                timestamp: now,
                component: self.component_name.clone(),
                violation_type: SlaViolationType::Availability,
                threshold: self.config.availability_target * 100.0,
                actual_value: metrics.availability_percentage,
                duration_seconds: 0,
            });
        }
        
        // Check error rate SLA
        let error_rate_ratio = metrics.error_rate_percentage / 100.0;
        if error_rate_ratio > self.config.error_rate_target {
            is_compliant = false;
            current_violations.push(SlaViolation {
                timestamp: now,
                component: self.component_name.clone(),
                violation_type: SlaViolationType::ErrorRate,
                threshold: self.config.error_rate_target * 100.0,
                actual_value: metrics.error_rate_percentage,
                duration_seconds: 0,
            });
        }
        
        // Store violations
        if !current_violations.is_empty() {
            if let Ok(mut violations) = self.violations.lock() {
                violations.extend(current_violations.clone());
                
                // Keep only recent violations (last 24 hours)
                let cutoff = now.duration_since(UNIX_EPOCH).unwrap().as_secs() - 86400;
                violations.retain(|v| {
                    v.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs() > cutoff
                });
            }
        }
        
        // Get all violations for status report
        let all_violations = self.violations.lock()
            .map(|v| v.clone())
            .unwrap_or_default();
        
        SlaStatus {
            component: self.component_name.clone(),
            is_compliant,
            current_metrics: metrics,
            violations: all_violations,
        }
    }

    /// Calculate response time percentiles from stored data
    fn calculate_percentiles(&self) -> (f64, f64, f64) {
        let times = match self.response_times.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => return (0.0, 0.0, 0.0),
        };
        
        if times.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        
        let mut sorted_times = times;
        sorted_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let len = sorted_times.len();
        
        // Calculate percentiles using interpolation for better accuracy
        let p50 = self.calculate_percentile(&sorted_times, 0.5);
        let p95 = self.calculate_percentile(&sorted_times, 0.95);
        let p99 = self.calculate_percentile(&sorted_times, 0.99);
        
        (p50, p95, p99)
    }

    /// Calculate a specific percentile using linear interpolation
    fn calculate_percentile(&self, sorted_values: &[f64], percentile: f64) -> f64 {
        if sorted_values.is_empty() {
            return 0.0;
        }
        
        if sorted_values.len() == 1 {
            return sorted_values[0];
        }
        
        // Calculate the exact position for this percentile
        let pos = percentile * (sorted_values.len() - 1) as f64;
        let lower_index = pos.floor() as usize;
        let upper_index = pos.ceil() as usize;
        
        if lower_index == upper_index {
            // Exact match, no interpolation needed
            sorted_values[lower_index]
        } else {
            // Linear interpolation between the two nearest values
            let lower_value = sorted_values[lower_index];
            let upper_value = sorted_values[upper_index];
            let fraction = pos - lower_index as f64;
            
            lower_value + fraction * (upper_value - lower_value)
        }
    }

    /// Get recent violations (last N hours)
    pub fn get_recent_violations(&self, hours: u64) -> Vec<SlaViolation> {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (hours * 3600);
        
        self.violations.lock()
            .map(|violations| {
                violations.iter()
                    .filter(|v| {
                        v.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs() > cutoff
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Reset metrics (useful for testing)
    pub fn reset(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.successful_requests.store(0, Ordering::Relaxed);
        self.failed_requests.store(0, Ordering::Relaxed);
        
        if let Ok(mut times) = self.response_times.lock() {
            times.clear();
        }
        
        if let Ok(mut violations) = self.violations.lock() {
            violations.clear();
        }
    }
}

/// Global SLA tracking registry
pub struct SlaRegistry {
    trackers: Arc<Mutex<HashMap<String, Arc<SlaTracker>>>>,
    default_config: SlaConfig,
}

impl SlaRegistry {
    /// Create a new SLA registry with default configuration
    pub fn new(default_config: SlaConfig) -> Self {
        Self {
            trackers: Arc::new(Mutex::new(HashMap::new())),
            default_config,
        }
    }

    /// Get or create an SLA tracker for a component
    pub fn get_tracker(&self, component: &str) -> Option<Arc<SlaTracker>> {
        let trackers = self.trackers.lock().ok()?;
        trackers.get(component).cloned()
    }

    /// Register a new SLA tracker for a component
    pub fn register_tracker(&self, component: String, config: Option<SlaConfig>) {
        let config = config.unwrap_or_else(|| self.default_config.clone());
        let tracker = Arc::new(SlaTracker::new(component.clone(), config));
        
        if let Ok(mut trackers) = self.trackers.lock() {
            trackers.insert(component, tracker);
        }
    }

    /// Get SLA status for all registered components
    pub fn get_all_statuses(&self) -> Vec<SlaStatus> {
        let trackers = match self.trackers.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => return Vec::new(),
        };
        
        trackers.values()
            .map(|tracker| tracker.check_compliance())
            .collect()
    }

    /// Get overall SLA compliance (true if all components are compliant)
    pub fn is_overall_compliant(&self) -> bool {
        self.get_all_statuses()
            .iter()
            .all(|status| status.is_compliant)
    }
}

/// Global static SLA registry instance
static SLA_REGISTRY: once_cell::sync::Lazy<SlaRegistry> = once_cell::sync::Lazy::new(|| {
    SlaRegistry::new(SlaConfig::default())
});

/// Get the global SLA registry
pub fn global_sla_registry() -> &'static SlaRegistry {
    &SLA_REGISTRY
}

/// Initialize SLA tracking for SpacetimeDB components
pub fn init_sla_tracking() {
    let registry = global_sla_registry();
    
    // Register standard SpacetimeDB components
    registry.register_tracker("reducer_calls".to_string(), None);
    registry.register_tracker("database_operations".to_string(), None);
    registry.register_tracker("http_api".to_string(), None);
    registry.register_tracker("websocket_api".to_string(), None);
    registry.register_tracker("wasm_execution".to_string(), None);
    
    // Register with custom thresholds for critical components
    let critical_config = SlaConfig {
        response_time_targets: SlaThresholds {
            p50_ms: 25.0,   // Stricter requirements
            p95_ms: 100.0,
            p99_ms: 250.0,
        },
        availability_target: 0.9999, // 99.99%
        error_rate_target: 0.001,    // 0.1%
        measurement_window_seconds: 300,
    };
    
    registry.register_tracker("critical_operations".to_string(), Some(critical_config));
}

/// Convenience function to record a successful operation
pub fn record_operation_success(component: &str, duration: Duration) {
    if let Some(tracker) = global_sla_registry().get_tracker(component) {
        tracker.record_success(duration);
    }
}

/// Convenience function to record a failed operation
pub fn record_operation_failure(component: &str, duration: Option<Duration>) {
    if let Some(tracker) = global_sla_registry().get_tracker(component) {
        tracker.record_failure(duration);
    }
}

/// Get SLA status for a specific component
pub fn get_sla_status(component: &str) -> Option<SlaStatus> {
    global_sla_registry()
        .get_tracker(component)
        .map(|tracker| tracker.check_compliance())
}

/// Get overall SLA compliance status
pub fn check_overall_sla_compliance() -> bool {
    global_sla_registry().is_overall_compliant()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_sla_tracker_creation() {
        let config = SlaConfig::default();
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        let metrics = tracker.calculate_metrics();
        assert_eq!(metrics.total_requests, 0);
        assert_eq!(metrics.availability_percentage, 100.0);
        assert_eq!(metrics.error_rate_percentage, 0.0);
    }

    #[test]
    fn test_successful_request_recording() {
        let config = SlaConfig::default();
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        tracker.record_success(Duration::from_millis(50));
        tracker.record_success(Duration::from_millis(100));
        
        let metrics = tracker.calculate_metrics();
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.successful_requests, 2);
        assert_eq!(metrics.failed_requests, 0);
        assert_eq!(metrics.availability_percentage, 100.0);
        assert_eq!(metrics.error_rate_percentage, 0.0);
    }

    #[test]
    fn test_failed_request_recording() {
        let config = SlaConfig::default();
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        tracker.record_success(Duration::from_millis(50));
        tracker.record_failure(Some(Duration::from_millis(1000)));
        
        let metrics = tracker.calculate_metrics();
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.successful_requests, 1);
        assert_eq!(metrics.failed_requests, 1);
        assert_eq!(metrics.availability_percentage, 50.0);
        assert_eq!(metrics.error_rate_percentage, 50.0);
    }

    #[test]
    fn test_sla_compliance_check() {
        let config = SlaConfig {
            response_time_targets: SlaThresholds {
                p50_ms: 50.0,
                p95_ms: 200.0,
                p99_ms: 500.0,
            },
            availability_target: 0.95,
            error_rate_target: 0.05,
            measurement_window_seconds: 300,
        };
        
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        // Record requests within SLA
        for _ in 0..100 {
            tracker.record_success(Duration::from_millis(30));
        }
        
        let status = tracker.check_compliance();
        assert!(status.is_compliant);
        assert!(status.violations.is_empty());
    }

    #[test]
    fn test_sla_violation_detection() {
        let config = SlaConfig {
            response_time_targets: SlaThresholds {
                p50_ms: 50.0,
                p95_ms: 200.0,
                p99_ms: 500.0,
            },
            availability_target: 0.95,
            error_rate_target: 0.05,
            measurement_window_seconds: 300,
        };
        
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        // Record slow requests that violate SLA
        for _ in 0..100 {
            tracker.record_success(Duration::from_millis(600)); // Above P99 threshold
        }
        
        let status = tracker.check_compliance();
        assert!(!status.is_compliant);
        assert!(!status.violations.is_empty());
        
        // Should have violations for P50, P95, and P99
        let violation_types: Vec<_> = status.violations.iter()
            .map(|v| &v.violation_type)
            .collect();
        
        assert!(violation_types.contains(&&SlaViolationType::ResponseTimeP50));
        assert!(violation_types.contains(&&SlaViolationType::ResponseTimeP95));
        assert!(violation_types.contains(&&SlaViolationType::ResponseTimeP99));
    }

    #[test]
    fn test_sla_registry() {
        let registry = SlaRegistry::new(SlaConfig::default());
        
        registry.register_tracker("test_component".to_string(), None);
        
        let tracker = registry.get_tracker("test_component");
        assert!(tracker.is_some());
        
        let tracker = tracker.unwrap();
        tracker.record_success(Duration::from_millis(50));
        
        let statuses = registry.get_all_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].component, "test_component");
    }

    #[test]
    fn test_percentile_calculation() {
        let config = SlaConfig::default();
        let tracker = SlaTracker::new("test_component".to_string(), config);
        
        // Record a range of response times: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        let times = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        for time in times.iter() {
            tracker.record_success(Duration::from_millis(*time));
        }
        
        let (p50, p95, p99) = tracker.calculate_percentiles();
        
        // With linear interpolation on 10 values [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        // P50 position = 0.5 * (10-1) = 4.5, interpolate between index 4 (50) and 5 (60) = 55
        // P95 position = 0.95 * (10-1) = 8.55, interpolate between index 8 (90) and 9 (100) = 95.5
        // P99 position = 0.99 * (10-1) = 8.91, interpolate between index 8 (90) and 9 (100) = 99.1
        assert!((p50 - 55.0).abs() < 1.0, "P50 should be around 55, got {}", p50);
        assert!(p95 >= 95.0 && p95 <= 96.0, "P95 should be around 95.5, got {}", p95);
        assert!(p99 >= 99.0 && p99 <= 100.0, "P99 should be around 99.1, got {}", p99);
    }
}
