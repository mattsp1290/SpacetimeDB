package security

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// SecurityMonitoringManager handles security monitoring and threat detection
type SecurityMonitoringManager struct {
	config *SecurityConfig

	// Event storage
	events    []SecurityEvent
	alerts    []SecurityAlert
	threats   []ThreatEvent
	auditLogs []AuditLog

	// Real-time monitoring
	eventCallbacks []SecurityEventCallback
	alertHandlers  []AlertHandler

	// Threat detection
	threatDetector *ThreatDetector

	// Statistics
	eventsLogged    atomic.Uint64
	alertsGenerated atomic.Uint64
	threatsDetected atomic.Uint64
	threatsBlocked  atomic.Uint64

	// Background operations
	monitorTicker *time.Ticker
	stopMonitor   chan bool

	// Thread safety
	mu sync.RWMutex

	healthy atomic.Bool
}

// SecurityAlert represents a security alert
type SecurityAlert struct {
	ID          string
	Type        AlertType
	Severity    SecuritySeverity
	Title       string
	Description string
	Source      string
	Timestamp   time.Time
	EventIDs    []string
	Metadata    map[string]interface{}
	Resolved    bool
	ResolvedAt  *time.Time
	ResolvedBy  string
	Actions     []string
}

// AlertType defines different types of security alerts
type AlertType int

const (
	AlertTypeAuthFailure AlertType = iota
	AlertTypeSuspiciousActivity
	AlertTypeThreatDetection
	AlertTypeSystemAnomaly
	AlertTypeDataBreach
	AlertTypeAccessViolation
	AlertTypeIntrusionAttempt
	AlertTypeRateLimitExceeded
)

func (at AlertType) String() string {
	switch at {
	case AlertTypeAuthFailure:
		return "AUTH_FAILURE"
	case AlertTypeSuspiciousActivity:
		return "SUSPICIOUS_ACTIVITY"
	case AlertTypeThreatDetection:
		return "THREAT_DETECTION"
	case AlertTypeSystemAnomaly:
		return "SYSTEM_ANOMALY"
	case AlertTypeDataBreach:
		return "DATA_BREACH"
	case AlertTypeAccessViolation:
		return "ACCESS_VIOLATION"
	case AlertTypeIntrusionAttempt:
		return "INTRUSION_ATTEMPT"
	case AlertTypeRateLimitExceeded:
		return "RATE_LIMIT_EXCEEDED"
	default:
		return "UNKNOWN"
	}
}

// ThreatEvent represents a detected security threat
type ThreatEvent struct {
	ID          string
	Type        ThreatType
	Severity    SecuritySeverity
	Description string
	Source      string
	Target      string
	Method      string
	Timestamp   time.Time
	Blocked     bool
	Mitigated   bool
	EventIDs    []string
	Indicators  []ThreatIndicator
	Metadata    map[string]interface{}
}

// ThreatType defines different types of security threats
type ThreatType int

const (
	ThreatTypeBruteForce ThreatType = iota
	ThreatTypeSuspiciousActivity
	ThreatTypeSQLInjection
	ThreatTypeXSS
	ThreatTypeCSRF
	ThreatTypeDOS
	ThreatTypeDDOS
	ThreatTypeMalware
	ThreatTypePhishing
	ThreatTypePrivilegeEscalation
	ThreatTypeDataExfiltration
)

func (tt ThreatType) String() string {
	switch tt {
	case ThreatTypeBruteForce:
		return "BRUTE_FORCE"
	case ThreatTypeSuspiciousActivity:
		return "SUSPICIOUS_ACTIVITY"
	case ThreatTypeSQLInjection:
		return "SQL_INJECTION"
	case ThreatTypeXSS:
		return "XSS"
	case ThreatTypeCSRF:
		return "CSRF"
	case ThreatTypeDOS:
		return "DOS"
	case ThreatTypeDDOS:
		return "DDOS"
	case ThreatTypeMalware:
		return "MALWARE"
	case ThreatTypePhishing:
		return "PHISHING"
	case ThreatTypePrivilegeEscalation:
		return "PRIVILEGE_ESCALATION"
	case ThreatTypeDataExfiltration:
		return "DATA_EXFILTRATION"
	default:
		return "UNKNOWN"
	}
}

// ThreatIndicator represents an indicator of compromise
type ThreatIndicator struct {
	Type        string
	Value       string
	Confidence  float64
	Source      string
	FirstSeen   time.Time
	LastSeen    time.Time
	Description string
}

// AuditLog represents an audit log entry
type AuditLog struct {
	ID        string
	Action    string
	Resource  string
	UserID    string
	IPAddress string
	UserAgent string
	Timestamp time.Time
	Result    string
	Details   map[string]interface{}
	Risk      RiskLevel
}

// RiskLevel defines risk levels for audit events
type RiskLevel int

const (
	RiskLow RiskLevel = iota
	RiskMedium
	RiskHigh
	RiskCritical
)

func (rl RiskLevel) String() string {
	switch rl {
	case RiskLow:
		return "LOW"
	case RiskMedium:
		return "MEDIUM"
	case RiskHigh:
		return "HIGH"
	case RiskCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// ThreatDetector handles threat detection logic
type ThreatDetector struct {
	rules          []DetectionRule
	patterns       map[string]*ThreatPattern
	rateLimiters   map[string]*RateLimiter
	anomalyTracker *AnomalyTracker

	mu sync.RWMutex
}

// DetectionRule represents a threat detection rule
type DetectionRule struct {
	ID          string
	Name        string
	Description string
	Type        ThreatType
	Pattern     string
	Threshold   int
	TimeWindow  time.Duration
	Enabled     bool
	Priority    int
}

// ThreatPattern represents a threat pattern
type ThreatPattern struct {
	Pattern     string
	ThreatType  ThreatType
	Confidence  float64
	Description string
}

// RateLimiter tracks rate limits for threat detection
type RateLimiter struct {
	Limit    int
	Window   time.Duration
	Requests []time.Time
	mu       sync.Mutex
}

// AnomalyTracker tracks system anomalies
type AnomalyTracker struct {
	baselines map[string]float64
	current   map[string]float64
	threshold float64

	mu sync.RWMutex
}

// EventFilters holds event filtering criteria
type EventFilters struct {
	StartTime *time.Time
	EndTime   *time.Time
	EventType string
	Severity  *SecuritySeverity
	Source    string
	UserID    string
	Limit     int
}

// ThreatAnalysis holds threat analysis results
type ThreatAnalysis struct {
	TotalThreats    int
	ActiveThreats   int
	BlockedThreats  int
	TopThreatTypes  map[string]int
	ThreatTrends    map[string][]int
	RiskScore       float64
	Recommendations []string
	LastUpdated     time.Time
}

// SecurityEventCallback defines callback function for security events
type SecurityEventCallback func(*SecurityEvent)

// AlertHandler defines handler function for security alerts
type AlertHandler func(*SecurityAlert)

// NewSecurityMonitoringManager creates a new security monitoring manager
func NewSecurityMonitoringManager(config *SecurityConfig) *SecurityMonitoringManager {
	manager := &SecurityMonitoringManager{
		config:         config,
		events:         make([]SecurityEvent, 0),
		alerts:         make([]SecurityAlert, 0),
		threats:        make([]ThreatEvent, 0),
		auditLogs:      make([]AuditLog, 0),
		eventCallbacks: make([]SecurityEventCallback, 0),
		alertHandlers:  make([]AlertHandler, 0),
		threatDetector: NewThreatDetector(),
	}

	manager.healthy.Store(true)
	return manager
}

// Initialize initializes the security monitoring manager
func (smm *SecurityMonitoringManager) Initialize(config *SecurityConfig) error {
	smm.mu.Lock()
	defer smm.mu.Unlock()

	if config != nil {
		smm.config = config
	}

	// Initialize threat detector
	if err := smm.threatDetector.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize threat detector: %w", err)
	}

	// Start background monitoring if enabled
	if smm.config.RealTimeMonitoring {
		smm.monitorTicker = time.NewTicker(1 * time.Second)
		smm.stopMonitor = make(chan bool)
		go smm.backgroundMonitoring()
	}

	return nil
}

// IsHealthy returns the health status of the monitoring manager
func (smm *SecurityMonitoringManager) IsHealthy() bool {
	return smm.healthy.Load()
}

// LogSecurityEvent logs a security event
func (smm *SecurityMonitoringManager) LogSecurityEvent(event *SecurityEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Generate ID if not provided
	if event.ID == "" {
		event.ID = generateEventID()
	}

	smm.mu.Lock()
	smm.events = append(smm.events, *event)
	smm.eventsLogged.Add(1)
	smm.mu.Unlock()

	// Analyze for threats
	if smm.config.ThreatDetection {
		smm.analyzeEventForThreats(event)
	}

	// Check for alert conditions
	smm.checkAlertConditions(event)

	// Call event callbacks
	for _, callback := range smm.eventCallbacks {
		go callback(event)
	}

	// Cleanup old events
	smm.cleanupOldEvents()

	return nil
}

// GetSecurityEvents retrieves security events based on filters
func (smm *SecurityMonitoringManager) GetSecurityEvents(filters *EventFilters) ([]SecurityEvent, error) {
	smm.mu.RLock()
	defer smm.mu.RUnlock()

	var filtered []SecurityEvent

	for _, event := range smm.events {
		if smm.eventMatchesFilters(event, filters) {
			filtered = append(filtered, event)
		}

		// Apply limit
		if filters != nil && filters.Limit > 0 && len(filtered) >= filters.Limit {
			break
		}
	}

	return filtered, nil
}

// GetThreatAnalysis returns current threat analysis
func (smm *SecurityMonitoringManager) GetThreatAnalysis() (*ThreatAnalysis, error) {
	smm.mu.RLock()
	defer smm.mu.RUnlock()

	analysis := &ThreatAnalysis{
		TotalThreats:    len(smm.threats),
		TopThreatTypes:  make(map[string]int),
		ThreatTrends:    make(map[string][]int),
		Recommendations: make([]string, 0),
		LastUpdated:     time.Now(),
	}

	// Count active and blocked threats
	for _, threat := range smm.threats {
		if !threat.Mitigated {
			analysis.ActiveThreats++
		}
		if threat.Blocked {
			analysis.BlockedThreats++
		}

		// Count by type
		threatType := threat.Type.String()
		analysis.TopThreatTypes[threatType]++
	}

	// Calculate risk score (simplified)
	analysis.RiskScore = float64(analysis.ActiveThreats) / float64(max(analysis.TotalThreats, 1)) * 100

	// Generate recommendations
	if analysis.ActiveThreats > 0 {
		analysis.Recommendations = append(analysis.Recommendations, "Active threats detected - investigate immediately")
	}
	if analysis.RiskScore > 50 {
		analysis.Recommendations = append(analysis.Recommendations, "High risk score - review security policies")
	}

	return analysis, nil
}

// EnableRealTimeMonitoring enables real-time monitoring with callback
func (smm *SecurityMonitoringManager) EnableRealTimeMonitoring(callback SecurityEventCallback) error {
	smm.mu.Lock()
	defer smm.mu.Unlock()

	smm.eventCallbacks = append(smm.eventCallbacks, callback)
	return nil
}

// analyzeEventForThreats analyzes an event for potential threats
func (smm *SecurityMonitoringManager) analyzeEventForThreats(event *SecurityEvent) {
	threats := smm.threatDetector.AnalyzeEvent(event)

	for _, threat := range threats {
		smm.mu.Lock()
		smm.threats = append(smm.threats, threat)
		smm.threatsDetected.Add(1)
		smm.mu.Unlock()

		// Generate alert for threat
		alert := &SecurityAlert{
			ID:          generateAlertID(),
			Type:        AlertTypeThreatDetection,
			Severity:    threat.Severity,
			Title:       fmt.Sprintf("Threat Detected: %s", threat.Type),
			Description: threat.Description,
			Source:      "threat_detector",
			Timestamp:   time.Now(),
			EventIDs:    []string{event.ID},
			Metadata: map[string]interface{}{
				"threat_id":   threat.ID,
				"threat_type": threat.Type.String(),
				"blocked":     threat.Blocked,
			},
		}

		smm.generateAlert(alert)
	}
}

// checkAlertConditions checks if an event triggers any alert conditions
func (smm *SecurityMonitoringManager) checkAlertConditions(event *SecurityEvent) {
	// Check rate limits
	if threshold, exists := smm.config.AlertThresholds[event.Type]; exists {
		recentEvents := smm.countRecentEvents(event.Type, 1*time.Hour)
		if recentEvents >= threshold {
			alert := &SecurityAlert{
				ID:          generateAlertID(),
				Type:        AlertTypeSystemAnomaly,
				Severity:    SeverityWarning,
				Title:       fmt.Sprintf("High frequency of %s events", event.Type),
				Description: fmt.Sprintf("Detected %d %s events in the last hour", recentEvents, event.Type),
				Source:      "rate_monitor",
				Timestamp:   time.Now(),
				EventIDs:    []string{event.ID},
			}

			smm.generateAlert(alert)
		}
	}

	// Check for authentication failures
	if event.Type == "authentication_failure" {
		if event.UserID != "" {
			recentFailures := smm.countRecentEventsForUser(event.UserID, "authentication_failure", 5*time.Minute)
			if recentFailures >= 3 {
				alert := &SecurityAlert{
					ID:          generateAlertID(),
					Type:        AlertTypeAuthFailure,
					Severity:    SeverityError,
					Title:       "Multiple authentication failures detected",
					Description: fmt.Sprintf("User %s has %d failed authentication attempts", event.UserID, recentFailures),
					Source:      "auth_monitor",
					Timestamp:   time.Now(),
					EventIDs:    []string{event.ID},
				}

				smm.generateAlert(alert)
			}
		}
	}
}

// generateAlert generates and processes a security alert
func (smm *SecurityMonitoringManager) generateAlert(alert *SecurityAlert) {
	smm.mu.Lock()
	smm.alerts = append(smm.alerts, *alert)
	smm.alertsGenerated.Add(1)
	smm.mu.Unlock()

	// Call alert handlers
	for _, handler := range smm.alertHandlers {
		go handler(alert)
	}
}

// countRecentEvents counts recent events of a specific type
func (smm *SecurityMonitoringManager) countRecentEvents(eventType string, duration time.Duration) int {
	cutoff := time.Now().Add(-duration)
	count := 0

	for _, event := range smm.events {
		if event.Type == eventType && event.Timestamp.After(cutoff) {
			count++
		}
	}

	return count
}

// countRecentEventsForUser counts recent events for a specific user
func (smm *SecurityMonitoringManager) countRecentEventsForUser(userID, eventType string, duration time.Duration) int {
	cutoff := time.Now().Add(-duration)
	count := 0

	for _, event := range smm.events {
		if event.UserID == userID && event.Type == eventType && event.Timestamp.After(cutoff) {
			count++
		}
	}

	return count
}

// eventMatchesFilters checks if an event matches the given filters
func (smm *SecurityMonitoringManager) eventMatchesFilters(event SecurityEvent, filters *EventFilters) bool {
	if filters == nil {
		return true
	}

	if filters.StartTime != nil && event.Timestamp.Before(*filters.StartTime) {
		return false
	}

	if filters.EndTime != nil && event.Timestamp.After(*filters.EndTime) {
		return false
	}

	if filters.EventType != "" && event.Type != filters.EventType {
		return false
	}

	if filters.Severity != nil && event.Severity != *filters.Severity {
		return false
	}

	if filters.Source != "" && event.Source != filters.Source {
		return false
	}

	if filters.UserID != "" && event.UserID != filters.UserID {
		return false
	}

	return true
}

// backgroundMonitoring performs continuous background monitoring
func (smm *SecurityMonitoringManager) backgroundMonitoring() {
	for {
		select {
		case <-smm.monitorTicker.C:
			smm.performMonitoringTasks()
		case <-smm.stopMonitor:
			return
		}
	}
}

// performMonitoringTasks performs regular monitoring tasks
func (smm *SecurityMonitoringManager) performMonitoringTasks() {
	// Update anomaly baselines
	smm.threatDetector.UpdateAnomalyBaselines()

	// Check for system anomalies
	anomalies := smm.threatDetector.DetectAnomalies()
	for _, anomaly := range anomalies {
		event := &SecurityEvent{
			ID:        generateEventID(),
			Type:      "system_anomaly",
			Severity:  SeverityWarning,
			Message:   anomaly,
			Source:    "anomaly_detector",
			Timestamp: time.Now(),
		}
		smm.LogSecurityEvent(event)
	}
}

// cleanupOldEvents removes old events to prevent memory growth
func (smm *SecurityMonitoringManager) cleanupOldEvents() {
	if smm.config.RetentionPeriod == 0 {
		return
	}

	cutoff := time.Now().Add(-smm.config.RetentionPeriod)

	// Filter events
	var filtered []SecurityEvent
	for _, event := range smm.events {
		if event.Timestamp.After(cutoff) {
			filtered = append(filtered, event)
		}
	}
	smm.events = filtered

	// Filter alerts
	var filteredAlerts []SecurityAlert
	for _, alert := range smm.alerts {
		if alert.Timestamp.After(cutoff) {
			filteredAlerts = append(filteredAlerts, alert)
		}
	}
	smm.alerts = filteredAlerts

	// Filter threats
	var filteredThreats []ThreatEvent
	for _, threat := range smm.threats {
		if threat.Timestamp.After(cutoff) {
			filteredThreats = append(filteredThreats, threat)
		}
	}
	smm.threats = filteredThreats
}

// Shutdown gracefully shuts down the monitoring manager
func (smm *SecurityMonitoringManager) Shutdown() error {
	if smm.monitorTicker != nil {
		smm.monitorTicker.Stop()
	}

	if smm.stopMonitor != nil {
		close(smm.stopMonitor)
	}

	smm.healthy.Store(false)
	return nil
}

// NewThreatDetector creates a new threat detector
func NewThreatDetector() *ThreatDetector {
	return &ThreatDetector{
		rules:          make([]DetectionRule, 0),
		patterns:       make(map[string]*ThreatPattern),
		rateLimiters:   make(map[string]*RateLimiter),
		anomalyTracker: NewAnomalyTracker(),
	}
}

// Initialize initializes the threat detector
func (td *ThreatDetector) Initialize() error {
	// Add default detection rules
	td.addDefaultRules()
	return nil
}

// AnalyzeEvent analyzes an event for threats
func (td *ThreatDetector) AnalyzeEvent(event *SecurityEvent) []ThreatEvent {
	var threats []ThreatEvent

	// Check against detection rules
	for _, rule := range td.rules {
		if rule.Enabled && td.eventMatchesRule(event, rule) {
			threat := ThreatEvent{
				ID:          generateThreatID(),
				Type:        rule.Type,
				Severity:    event.Severity,
				Description: rule.Description,
				Source:      event.Source,
				Timestamp:   time.Now(),
				EventIDs:    []string{event.ID},
				Blocked:     false,
				Mitigated:   false,
			}
			threats = append(threats, threat)
		}
	}

	return threats
}

// UpdateAnomalyBaselines updates anomaly detection baselines
func (td *ThreatDetector) UpdateAnomalyBaselines() {
	// Simplified implementation
	td.anomalyTracker.UpdateBaselines()
}

// DetectAnomalies detects system anomalies
func (td *ThreatDetector) DetectAnomalies() []string {
	return td.anomalyTracker.DetectAnomalies()
}

// addDefaultRules adds default detection rules
func (td *ThreatDetector) addDefaultRules() {
	rules := []DetectionRule{
		{
			ID:          "rule_001",
			Name:        "Brute Force Detection",
			Description: "Detects brute force authentication attempts",
			Type:        ThreatTypeBruteForce,
			Pattern:     "authentication_failure",
			Threshold:   5,
			TimeWindow:  5 * time.Minute,
			Enabled:     true,
			Priority:    1,
		},
		{
			ID:          "rule_002",
			Name:        "Suspicious Access Pattern",
			Description: "Detects unusual access patterns",
			Type:        ThreatTypeSuspiciousActivity,
			Pattern:     "access_violation",
			Threshold:   3,
			TimeWindow:  1 * time.Minute,
			Enabled:     true,
			Priority:    2,
		},
	}

	td.rules = append(td.rules, rules...)
}

// eventMatchesRule checks if an event matches a detection rule
func (td *ThreatDetector) eventMatchesRule(event *SecurityEvent, rule DetectionRule) bool {
	// Simplified pattern matching
	return event.Type == rule.Pattern
}

// NewAnomalyTracker creates a new anomaly tracker
func NewAnomalyTracker() *AnomalyTracker {
	return &AnomalyTracker{
		baselines: make(map[string]float64),
		current:   make(map[string]float64),
		threshold: 2.0, // 2 standard deviations
	}
}

// UpdateBaselines updates anomaly detection baselines
func (at *AnomalyTracker) UpdateBaselines() {
	at.mu.Lock()
	defer at.mu.Unlock()

	// Simplified baseline update
	for metric, value := range at.current {
		if baseline, exists := at.baselines[metric]; exists {
			at.baselines[metric] = (baseline * 0.9) + (value * 0.1) // Exponential moving average
		} else {
			at.baselines[metric] = value
		}
	}
}

// DetectAnomalies detects anomalies in current metrics
func (at *AnomalyTracker) DetectAnomalies() []string {
	at.mu.RLock()
	defer at.mu.RUnlock()

	anomalies := make([]string, 0) // Initialize as empty slice instead of nil slice

	for metric, current := range at.current {
		if baseline, exists := at.baselines[metric]; exists {
			deviation := (current - baseline) / baseline
			if deviation > at.threshold {
				anomalies = append(anomalies, fmt.Sprintf("Anomaly detected in %s: %.2f%% above baseline", metric, deviation*100))
			}
		}
	}

	return anomalies
}

// Helper functions
func generateAlertID() string {
	return "alert_" + generateRandomID(16)
}

func generateThreatID() string {
	return "threat_" + generateRandomID(16)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
