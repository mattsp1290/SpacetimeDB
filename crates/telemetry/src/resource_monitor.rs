//! Resource utilization monitoring for SpacetimeDB
//!
//! This module provides:
//! - Memory usage patterns and leak detection
//! - CPU utilization and thread pool metrics
//! - Disk I/O performance tracking
//! - Network bandwidth monitoring

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

/// Configuration for resource monitoring
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Sample interval for resource collection (seconds)
    pub sample_interval_seconds: u64,
    /// Number of samples to keep for trend analysis
    pub max_samples: usize,
    /// Memory leak detection threshold (percentage growth)
    pub memory_leak_threshold_percent: f64,
    /// CPU usage alert threshold (percentage)
    pub cpu_alert_threshold_percent: f64,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            sample_interval_seconds: 30, // Sample every 30 seconds
            max_samples: 120, // Keep 1 hour of data (120 * 30s = 3600s)
            memory_leak_threshold_percent: 50.0, // 50% growth indicates potential leak
            cpu_alert_threshold_percent: 85.0, // Alert when CPU > 85%
        }
    }
}

/// Memory usage information
#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub timestamp: SystemTime,
    pub heap_bytes: u64,
    pub stack_bytes: u64,
    pub total_allocated_bytes: u64,
    pub total_deallocated_bytes: u64,
    pub active_allocations: u64,
    pub rss_bytes: Option<u64>, // Resident Set Size from OS
    pub virtual_bytes: Option<u64>, // Virtual memory from OS
}

/// CPU usage information
#[derive(Debug, Clone)]
pub struct CpuUsage {
    pub timestamp: SystemTime,
    pub user_percent: f64,
    pub system_percent: f64,
    pub total_percent: f64,
    pub load_average_1min: Option<f64>,
    pub load_average_5min: Option<f64>,
    pub load_average_15min: Option<f64>,
    pub active_threads: u32,
    pub thread_pool_utilization: f64,
}

/// Disk I/O metrics
#[derive(Debug, Clone)]
pub struct DiskUsage {
    pub timestamp: SystemTime,
    pub read_bytes_per_sec: u64,
    pub write_bytes_per_sec: u64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub avg_read_latency_ms: f64,
    pub avg_write_latency_ms: f64,
    pub disk_usage_percent: f64,
    pub available_space_bytes: u64,
}

/// Network usage metrics
#[derive(Debug, Clone)]
pub struct NetworkUsage {
    pub timestamp: SystemTime,
    pub bytes_received_per_sec: u64,
    pub bytes_sent_per_sec: u64,
    pub packets_received_per_sec: u64,
    pub packets_sent_per_sec: u64,
    pub active_connections: u32,
    pub connection_errors_per_sec: u32,
}

/// Resource alert types
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceAlert {
    MemoryLeak {
        growth_rate_percent: f64,
        current_usage_bytes: u64,
    },
    HighCpuUsage {
        current_percent: f64,
        duration_seconds: u64,
    },
    LowDiskSpace {
        available_percent: f64,
        available_bytes: u64,
    },
    HighDiskLatency {
        avg_latency_ms: f64,
    },
    NetworkCongestion {
        bandwidth_utilization_percent: f64,
    },
    ThreadPoolExhaustion {
        utilization_percent: f64,
        active_threads: u32,
    },
}

/// Resource monitoring alert
#[derive(Debug, Clone)]
pub struct ResourceAlertEvent {
    pub timestamp: SystemTime,
    pub severity: AlertSeverity,
    pub alert: ResourceAlert,
    pub description: String,
    pub recommended_action: String,
}

/// Alert severity levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Comprehensive resource monitor
pub struct ResourceMonitor {
    config: ResourceConfig,
    
    // Resource usage history
    memory_samples: Arc<Mutex<VecDeque<MemoryUsage>>>,
    cpu_samples: Arc<Mutex<VecDeque<CpuUsage>>>,
    disk_samples: Arc<Mutex<VecDeque<DiskUsage>>>,
    network_samples: Arc<Mutex<VecDeque<NetworkUsage>>>,
    
    // Alert tracking
    alerts: Arc<Mutex<VecDeque<ResourceAlertEvent>>>,
    
    // Tracking state
    last_sample_time: Arc<Mutex<Instant>>,
    baseline_memory: Arc<Mutex<Option<u64>>>,
}

impl ResourceMonitor {
    /// Create a new resource monitor
    pub fn new(config: ResourceConfig) -> Self {
        Self {
            config,
            memory_samples: Arc::new(Mutex::new(VecDeque::new())),
            cpu_samples: Arc::new(Mutex::new(VecDeque::new())),
            disk_samples: Arc::new(Mutex::new(VecDeque::new())),
            network_samples: Arc::new(Mutex::new(VecDeque::new())),
            alerts: Arc::new(Mutex::new(VecDeque::new())),
            last_sample_time: Arc::new(Mutex::new(Instant::now())),
            baseline_memory: Arc::new(Mutex::new(None)),
        }
    }

    /// Record a memory usage sample
    pub fn record_memory_usage(&self, usage: MemoryUsage) {
        // Set baseline if not already set
        if let Ok(mut baseline) = self.baseline_memory.lock() {
            if baseline.is_none() {
                *baseline = Some(usage.total_allocated_bytes);
            }
        }

        // Store sample
        if let Ok(mut samples) = self.memory_samples.lock() {
            samples.push_back(usage.clone());
            
            // Keep only max_samples
            while samples.len() > self.config.max_samples {
                samples.pop_front();
            }
        }

        // Check for memory leak
        self.check_memory_leak(&usage);
    }

    /// Record a CPU usage sample
    pub fn record_cpu_usage(&self, usage: CpuUsage) {
        // Store sample
        if let Ok(mut samples) = self.cpu_samples.lock() {
            samples.push_back(usage.clone());
            
            while samples.len() > self.config.max_samples {
                samples.pop_front();
            }
        }

        // Check for high CPU usage
        self.check_high_cpu_usage(&usage);
        
        // Check for thread pool exhaustion
        self.check_thread_pool_exhaustion(&usage);
    }

    /// Record a disk usage sample
    pub fn record_disk_usage(&self, usage: DiskUsage) {
        // Store sample
        if let Ok(mut samples) = self.disk_samples.lock() {
            samples.push_back(usage.clone());
            
            while samples.len() > self.config.max_samples {
                samples.pop_front();
            }
        }

        // Check for low disk space
        self.check_low_disk_space(&usage);
        
        // Check for high disk latency
        self.check_high_disk_latency(&usage);
    }

    /// Record a network usage sample
    pub fn record_network_usage(&self, usage: NetworkUsage) {
        // Store sample
        if let Ok(mut samples) = self.network_samples.lock() {
            samples.push_back(usage.clone());
            
            while samples.len() > self.config.max_samples {
                samples.pop_front();
            }
        }

        // Check for network congestion
        self.check_network_congestion(&usage);
    }

    /// Check for memory leak patterns
    fn check_memory_leak(&self, current: &MemoryUsage) {
        if let Ok(baseline) = self.baseline_memory.lock() {
            if let Some(baseline_memory) = *baseline {
                let growth_percent = ((current.total_allocated_bytes as f64 - baseline_memory as f64) 
                    / baseline_memory as f64) * 100.0;
                
                if growth_percent > self.config.memory_leak_threshold_percent {
                    let alert = ResourceAlertEvent {
                        timestamp: SystemTime::now(),
                        severity: AlertSeverity::Warning,
                        alert: ResourceAlert::MemoryLeak {
                            growth_rate_percent: growth_percent,
                            current_usage_bytes: current.total_allocated_bytes,
                        },
                        description: format!(
                            "Memory usage has grown {:.1}% from baseline ({} bytes)",
                            growth_percent, current.total_allocated_bytes
                        ),
                        recommended_action: "Check for memory leaks, review allocation patterns, consider garbage collection tuning".to_string(),
                    };
                    
                    self.add_alert(alert);
                }
            }
        }
    }

    /// Check for sustained high CPU usage
    fn check_high_cpu_usage(&self, current: &CpuUsage) {
        if current.total_percent > self.config.cpu_alert_threshold_percent {
            // Check if this is sustained high usage
            let sustained_duration = self.get_sustained_cpu_duration();
            
            let severity = if current.total_percent > 95.0 {
                AlertSeverity::Critical
            } else {
                AlertSeverity::Warning
            };
            
            let alert = ResourceAlertEvent {
                timestamp: SystemTime::now(),
                severity,
                alert: ResourceAlert::HighCpuUsage {
                    current_percent: current.total_percent,
                    duration_seconds: sustained_duration,
                },
                description: format!(
                    "CPU usage is {:.1}% for {} seconds",
                    current.total_percent, sustained_duration
                ),
                recommended_action: "Check for CPU-intensive operations, consider scaling, review algorithm efficiency".to_string(),
            };
            
            self.add_alert(alert);
        }
    }

    /// Check for thread pool exhaustion
    fn check_thread_pool_exhaustion(&self, current: &CpuUsage) {
        if current.thread_pool_utilization > 90.0 {
            let alert = ResourceAlertEvent {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                alert: ResourceAlert::ThreadPoolExhaustion {
                    utilization_percent: current.thread_pool_utilization,
                    active_threads: current.active_threads,
                },
                description: format!(
                    "Thread pool utilization is {:.1}% with {} active threads",
                    current.thread_pool_utilization, current.active_threads
                ),
                recommended_action: "Consider increasing thread pool size or optimizing async operations".to_string(),
            };
            
            self.add_alert(alert);
        }
    }

    /// Check for low disk space
    fn check_low_disk_space(&self, current: &DiskUsage) {
        let available_percent = 100.0 - current.disk_usage_percent;
        
        if available_percent < 10.0 {
            let severity = if available_percent < 5.0 {
                AlertSeverity::Critical
            } else {
                AlertSeverity::Warning
            };
            
            let alert = ResourceAlertEvent {
                timestamp: SystemTime::now(),
                severity,
                alert: ResourceAlert::LowDiskSpace {
                    available_percent,
                    available_bytes: current.available_space_bytes,
                },
                description: format!(
                    "Disk space is {:.1}% full, {} bytes available",
                    current.disk_usage_percent, current.available_space_bytes
                ),
                recommended_action: "Free up disk space, archive old data, or add storage capacity".to_string(),
            };
            
            self.add_alert(alert);
        }
    }

    /// Check for high disk latency
    fn check_high_disk_latency(&self, current: &DiskUsage) {
        let avg_latency = (current.avg_read_latency_ms + current.avg_write_latency_ms) / 2.0;
        
        if avg_latency > 100.0 { // 100ms is high for disk operations
            let alert = ResourceAlertEvent {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                alert: ResourceAlert::HighDiskLatency {
                    avg_latency_ms: avg_latency,
                },
                description: format!(
                    "Average disk latency is {:.1}ms (read: {:.1}ms, write: {:.1}ms)",
                    avg_latency, current.avg_read_latency_ms, current.avg_write_latency_ms
                ),
                recommended_action: "Check disk health, consider SSD upgrade, or optimize I/O patterns".to_string(),
            };
            
            self.add_alert(alert);
        }
    }

    /// Check for network congestion
    fn check_network_congestion(&self, current: &NetworkUsage) {
        // Assume 1Gbps network capacity (adjust based on actual infrastructure)
        let network_capacity_bytes_per_sec = 125_000_000_u64; // 1Gbps in bytes/sec
        let total_bandwidth = current.bytes_received_per_sec + current.bytes_sent_per_sec;
        let utilization_percent = (total_bandwidth as f64 / network_capacity_bytes_per_sec as f64) * 100.0;
        
        if utilization_percent > 80.0 {
            let alert = ResourceAlertEvent {
                timestamp: SystemTime::now(),
                severity: AlertSeverity::Warning,
                alert: ResourceAlert::NetworkCongestion {
                    bandwidth_utilization_percent: utilization_percent,
                },
                description: format!(
                    "Network bandwidth utilization is {:.1}% ({} bytes/sec total)",
                    utilization_percent, total_bandwidth
                ),
                recommended_action: "Monitor network traffic, consider load balancing, or upgrade network capacity".to_string(),
            };
            
            self.add_alert(alert);
        }
    }

    /// Get duration of sustained high CPU usage
    fn get_sustained_cpu_duration(&self) -> u64 {
        let samples = match self.cpu_samples.lock() {
            Ok(guard) => guard.clone().into_iter().collect::<Vec<_>>(),
            Err(_) => return 0,
        };
        
        if samples.is_empty() {
            return 0;
        }
        
        // Count consecutive samples above threshold from the end
        let mut duration_samples = 0;
        for sample in samples.iter().rev() {
            if sample.total_percent > self.config.cpu_alert_threshold_percent {
                duration_samples += 1;
            } else {
                break;
            }
        }
        
        duration_samples * self.config.sample_interval_seconds
    }

    /// Add an alert to the alert queue
    fn add_alert(&self, alert: ResourceAlertEvent) {
        if let Ok(mut alerts) = self.alerts.lock() {
            alerts.push_back(alert);
            
            // Keep only recent alerts (last 24 hours)
            while alerts.len() > 2880 { // 24 hours * 60 minutes * 2 (assuming 30s intervals)
                alerts.pop_front();
            }
        }
    }

    /// Get recent alerts
    pub fn get_recent_alerts(&self, duration: Duration) -> Vec<ResourceAlertEvent> {
        let cutoff = SystemTime::now() - duration;
        
        self.alerts.lock().map(|alerts| {
            alerts.iter()
                .filter(|alert| alert.timestamp > cutoff)
                .cloned()
                .collect()
        }).unwrap_or_default()
    }

    /// Get current resource summary
    pub fn get_resource_summary(&self) -> ResourceSummary {
        let memory = self.memory_samples.lock().ok()
            .and_then(|samples| samples.back().cloned());
        
        let cpu = self.cpu_samples.lock().ok()
            .and_then(|samples| samples.back().cloned());
        
        let disk = self.disk_samples.lock().ok()
            .and_then(|samples| samples.back().cloned());
        
        let network = self.network_samples.lock().ok()
            .and_then(|samples| samples.back().cloned());
        
        let active_alerts = self.get_recent_alerts(Duration::from_secs(300)); // Last 5 minutes
        
        ResourceSummary {
            timestamp: SystemTime::now(),
            memory_usage: memory,
            cpu_usage: cpu,
            disk_usage: disk,
            network_usage: network,
            active_alerts,
            overall_health: self.calculate_overall_health(),
        }
    }

    /// Calculate overall system health score (0-100)
    fn calculate_overall_health(&self) -> f64 {
        let mut health_score = 100.0;
        let recent_alerts = self.get_recent_alerts(Duration::from_secs(300));
        
        // Deduct points for alerts
        for alert in recent_alerts {
            match alert.severity {
                AlertSeverity::Info => health_score -= 1.0,
                AlertSeverity::Warning => health_score -= 5.0,
                AlertSeverity::Critical => health_score -= 15.0,
            }
        }
        
        // Deduct points for resource utilization
        if let Some(cpu) = self.cpu_samples.lock().ok().and_then(|s| s.back().cloned()) {
            if cpu.total_percent > 80.0 {
                health_score -= (cpu.total_percent - 80.0) / 4.0; // Up to 5 points
            }
        }
        
        if let Some(disk) = self.disk_samples.lock().ok().and_then(|s| s.back().cloned()) {
            if disk.disk_usage_percent > 80.0 {
                health_score -= (disk.disk_usage_percent - 80.0) / 4.0; // Up to 5 points
            }
        }
        
        health_score.max(0.0)
    }

    /// Get resource trends (simplified trend analysis)
    pub fn get_resource_trends(&self, duration: Duration) -> ResourceTrends {
        ResourceTrends {
            memory_trend: self.calculate_memory_trend(duration),
            cpu_trend: self.calculate_cpu_trend(duration),
            disk_trend: self.calculate_disk_trend(duration),
            network_trend: self.calculate_network_trend(duration),
        }
    }

    fn calculate_memory_trend(&self, duration: Duration) -> TrendDirection {
        let samples = match self.memory_samples.lock() {
            Ok(guard) => guard.clone().into_iter().collect::<Vec<_>>(),
            Err(_) => return TrendDirection::Stable,
        };
        
        if samples.len() < 2 {
            return TrendDirection::Stable;
        }
        
        let cutoff = SystemTime::now() - duration;
        let recent_samples: Vec<_> = samples.iter()
            .filter(|s| s.timestamp > cutoff)
            .collect();
        
        if recent_samples.len() < 2 {
            return TrendDirection::Stable;
        }
        
        let first_usage = recent_samples.first().unwrap().total_allocated_bytes as f64;
        let last_usage = recent_samples.last().unwrap().total_allocated_bytes as f64;
        let change_percent = ((last_usage - first_usage) / first_usage) * 100.0;
        
        if change_percent > 5.0 {
            TrendDirection::Increasing
        } else if change_percent < -5.0 {
            TrendDirection::Decreasing
        } else {
            TrendDirection::Stable
        }
    }

    fn calculate_cpu_trend(&self, duration: Duration) -> TrendDirection {
        let samples = match self.cpu_samples.lock() {
            Ok(guard) => guard.clone().into_iter().collect::<Vec<_>>(),
            Err(_) => return TrendDirection::Stable,
        };
        
        if samples.len() < 2 {
            return TrendDirection::Stable;
        }
        
        let cutoff = SystemTime::now() - duration;
        let recent_samples: Vec<_> = samples.iter()
            .filter(|s| s.timestamp > cutoff)
            .collect();
        
        if recent_samples.len() < 2 {
            return TrendDirection::Stable;
        }
        
        let first_cpu = recent_samples.first().unwrap().total_percent;
        let last_cpu = recent_samples.last().unwrap().total_percent;
        let change = last_cpu - first_cpu;
        
        if change > 10.0 {
            TrendDirection::Increasing
        } else if change < -10.0 {
            TrendDirection::Decreasing
        } else {
            TrendDirection::Stable
        }
    }

    fn calculate_disk_trend(&self, _duration: Duration) -> TrendDirection {
        // Simplified: disk usage typically increases slowly
        TrendDirection::Stable
    }

    fn calculate_network_trend(&self, _duration: Duration) -> TrendDirection {
        // Simplified: network usage varies widely
        TrendDirection::Stable
    }
}

/// Resource summary snapshot
#[derive(Debug, Clone)]
pub struct ResourceSummary {
    pub timestamp: SystemTime,
    pub memory_usage: Option<MemoryUsage>,
    pub cpu_usage: Option<CpuUsage>,
    pub disk_usage: Option<DiskUsage>,
    pub network_usage: Option<NetworkUsage>,
    pub active_alerts: Vec<ResourceAlertEvent>,
    pub overall_health: f64,
}

/// Resource trend analysis
#[derive(Debug, Clone)]
pub struct ResourceTrends {
    pub memory_trend: TrendDirection,
    pub cpu_trend: TrendDirection,
    pub disk_trend: TrendDirection,
    pub network_trend: TrendDirection,
}

/// Trend direction
#[derive(Debug, Clone, PartialEq)]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
}

/// Global resource monitor instance
static RESOURCE_MONITOR: std::sync::LazyLock<ResourceMonitor> = std::sync::LazyLock::new(|| {
    ResourceMonitor::new(ResourceConfig::default())
});

/// Get the global resource monitor
pub fn global_resource_monitor() -> &'static ResourceMonitor {
    &RESOURCE_MONITOR
}

/// Convenience functions for recording resource usage
pub fn record_memory_usage(
    heap_bytes: u64,
    stack_bytes: u64,
    total_allocated_bytes: u64,
    total_deallocated_bytes: u64,
    active_allocations: u64,
) {
    let usage = MemoryUsage {
        timestamp: SystemTime::now(),
        heap_bytes,
        stack_bytes,
        total_allocated_bytes,
        total_deallocated_bytes,
        active_allocations,
        rss_bytes: None, // Could be filled by OS-specific code
        virtual_bytes: None,
    };
    
    global_resource_monitor().record_memory_usage(usage);
}

pub fn record_cpu_usage(
    user_percent: f64,
    system_percent: f64,
    active_threads: u32,
    thread_pool_utilization: f64,
) {
    let usage = CpuUsage {
        timestamp: SystemTime::now(),
        user_percent,
        system_percent,
        total_percent: user_percent + system_percent,
        load_average_1min: None, // Could be filled by OS-specific code
        load_average_5min: None,
        load_average_15min: None,
        active_threads,
        thread_pool_utilization,
    };
    
    global_resource_monitor().record_cpu_usage(usage);
}

pub fn record_disk_usage(
    read_bytes_per_sec: u64,
    write_bytes_per_sec: u64,
    read_ops_per_sec: u64,
    write_ops_per_sec: u64,
    avg_read_latency_ms: f64,
    avg_write_latency_ms: f64,
    disk_usage_percent: f64,
    available_space_bytes: u64,
) {
    let usage = DiskUsage {
        timestamp: SystemTime::now(),
        read_bytes_per_sec,
        write_bytes_per_sec,
        read_ops_per_sec,
        write_ops_per_sec,
        avg_read_latency_ms,
        avg_write_latency_ms,
        disk_usage_percent,
        available_space_bytes,
    };
    
    global_resource_monitor().record_disk_usage(usage);
}

pub fn record_network_usage(
    bytes_received_per_sec: u64,
    bytes_sent_per_sec: u64,
    packets_received_per_sec: u64,
    packets_sent_per_sec: u64,
    active_connections: u32,
    connection_errors_per_sec: u32,
) {
    let usage = NetworkUsage {
        timestamp: SystemTime::now(),
        bytes_received_per_sec,
        bytes_sent_per_sec,
        packets_received_per_sec,
        packets_sent_per_sec,
        active_connections,
        connection_errors_per_sec,
    };
    
    global_resource_monitor().record_network_usage(usage);
}

/// Get current resource summary
pub fn get_resource_summary() -> ResourceSummary {
    global_resource_monitor().get_resource_summary()
}

/// Get recent resource alerts
pub fn get_recent_resource_alerts(duration: Duration) -> Vec<ResourceAlertEvent> {
    global_resource_monitor().get_recent_alerts(duration)
}

/// Get resource trends
pub fn get_resource_trends(duration: Duration) -> ResourceTrends {
    global_resource_monitor().get_resource_trends(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_monitor_creation() {
        let config = ResourceConfig::default();
        let monitor = ResourceMonitor::new(config);
        
        let summary = monitor.get_resource_summary();
        assert!(summary.memory_usage.is_none());
        assert!(summary.cpu_usage.is_none());
        assert!(summary.overall_health >= 0.0);
    }

    #[test]
    fn test_memory_usage_recording() {
        let monitor = ResourceMonitor::new(ResourceConfig::default());
        
        let usage = MemoryUsage {
            timestamp: SystemTime::now(),
            heap_bytes: 1024 * 1024,
            stack_bytes: 64 * 1024,
            total_allocated_bytes: 2 * 1024 * 1024,
            total_deallocated_bytes: 512 * 1024,
            active_allocations: 1000,
            rss_bytes: None,
            virtual_bytes: None,
        };
        
        monitor.record_memory_usage(usage);
        
        let summary = monitor.get_resource_summary();
        assert!(summary.memory_usage.is_some());
        assert_eq!(summary.memory_usage.unwrap().heap_bytes, 1024 * 1024);
    }

    #[test]
    fn test_cpu_usage_recording() {
        let monitor = ResourceMonitor::new(ResourceConfig::default());
        
        let usage = CpuUsage {
            timestamp: SystemTime::now(),
            user_percent: 25.0,
            system_percent: 15.0,
            total_percent: 40.0,
            load_average_1min: None,
            load_average_5min: None,
            load_average_15min: None,
            active_threads: 16,
            thread_pool_utilization: 60.0,
        };
        
        monitor.record_cpu_usage(usage);
        
        let summary = monitor.get_resource_summary();
        assert!(summary.cpu_usage.is_some());
        assert_eq!(summary.cpu_usage.unwrap().total_percent, 40.0);
    }

    #[test]
    fn test_high_cpu_alert() {
        let config = ResourceConfig {
            cpu_alert_threshold_percent: 50.0,
            ..Default::default()
        };
        let monitor = ResourceMonitor::new(config);
        
        let usage = CpuUsage {
            timestamp: SystemTime::now(),
            user_percent: 60.0,
            system_percent: 20.0,
            total_percent: 80.0,
            load_average_1min: None,
            load_average_5min: None,
            load_average_15min: None,
            active_threads: 16,
            thread_pool_utilization: 80.0,
        };
        
        monitor.record_cpu_usage(usage);
        
        let alerts = monitor.get_recent_alerts(Duration::from_secs(60));
        assert!(!alerts.is_empty());
        
        let cpu_alert = alerts.iter().find(|a| matches!(a.alert, ResourceAlert::HighCpuUsage { .. }));
        assert!(cpu_alert.is_some());
    }
}
