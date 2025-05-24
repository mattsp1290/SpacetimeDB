// Package security provides comprehensive security features for SpacetimeDB Go bindings
package security

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// SecurityLevel defines the security enforcement level
type SecurityLevel int

const (
	// SecurityDisabled disables all security features (development only)
	SecurityDisabled SecurityLevel = iota
	// SecurityBasic enables basic security features (authentication only)
	SecurityBasic
	// SecurityStandard enables standard security features (auth + encryption)
	SecurityStandard
	// SecurityStrict enables strict security features (standard + monitoring)
	SecurityStrict
	// SecurityMaximum enables maximum security features (all features)
	SecurityMaximum
)

// SecurityError represents security-related errors
type SecurityError struct {
	Type      string
	Message   string
	Code      uint32
	Timestamp int64
	Context   map[string]interface{}
	Severity  SecuritySeverity
}

func (e *SecurityError) Error() string {
	return fmt.Sprintf("security error [%s]: %s (code: %d, severity: %s)",
		e.Type, e.Message, e.Code, e.Severity)
}

// SecuritySeverity defines the severity of security events
type SecuritySeverity int

const (
	SeverityInfo SecuritySeverity = iota
	SeverityWarning
	SeverityError
	SeverityCritical
	SeverityFatal
)

func (s SecuritySeverity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	case SeverityCritical:
		return "CRITICAL"
	case SeverityFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// SecurityManager provides comprehensive security management
type SecurityManager struct {
	level SecurityLevel

	// Core security modules
	authManager    *AuthenticationManager
	cryptoManager  *CryptographyManager
	commManager    *SecureCommunicationManager
	monitorManager *SecurityMonitoringManager

	// Configuration
	config *SecurityConfig

	// Security state
	securityEvents []SecurityEvent
	eventCount     atomic.Uint64
	violations     atomic.Uint64

	// Thread safety
	mu sync.RWMutex

	// Statistics
	operationsPerformed atomic.Uint64
	threatsBlocked      atomic.Uint64
	authAttempts        atomic.Uint64
	authSuccesses       atomic.Uint64
}

// SecurityConfig holds security configuration
type SecurityConfig struct {
	// Authentication settings
	TokenTTL               time.Duration
	MaxLoginAttempts       int
	LockoutDuration        time.Duration
	RequireStrongPasswords bool

	// Encryption settings
	EncryptionAlgorithm string
	KeySize             int
	RotationInterval    time.Duration

	// Communication settings
	RequireTLS     bool
	TLSMinVersion  uint16
	AllowedCiphers []uint16

	// Monitoring settings
	AuditLogging       bool
	ThreatDetection    bool
	RealTimeMonitoring bool
	AlertThresholds    map[string]int
	RetentionPeriod    time.Duration

	// General settings
	EnableSecureHeaders  bool
	EnableCSRFProtection bool
	EnableRateLimiting   bool
	MaxRequestsPerMinute int
}

// SecurityEvent represents a security-related event
type SecurityEvent struct {
	ID         string
	Type       string
	Severity   SecuritySeverity
	Message    string
	Source     string
	UserID     string
	IPAddress  string
	UserAgent  string
	Timestamp  time.Time
	Details    map[string]interface{}
	Resolved   bool
	ResolvedAt *time.Time
	ResolvedBy string
}

// AuthenticationResult represents the result of an authentication attempt
type AuthenticationResult struct {
	Success     bool
	UserID      string
	Token       string
	ExpiresAt   time.Time
	Permissions []string
	Reason      string
	Metadata    map[string]interface{}
}

// CryptographyResult represents the result of a cryptographic operation
type CryptographyResult struct {
	Success   bool
	Data      []byte
	Metadata  map[string]interface{}
	Algorithm string
	KeyID     string
}

// SecurityInterface defines the main security operations
type SecurityInterface interface {
	// Core operations
	Initialize(config *SecurityConfig) error
	SetSecurityLevel(level SecurityLevel) error
	GetSecurityStatus() *SecurityStatus
	Shutdown() error

	// Authentication operations
	Authenticate(credentials *Credentials) (*AuthenticationResult, error)
	ValidateToken(token string) (*TokenValidationResult, error)
	RevokeToken(token string) error
	RefreshToken(token string) (*AuthenticationResult, error)

	// Encryption operations
	Encrypt(data []byte, options *EncryptionOptions) (*CryptographyResult, error)
	Decrypt(encryptedData []byte, options *DecryptionOptions) (*CryptographyResult, error)
	Hash(data []byte, algorithm string) ([]byte, error)
	VerifyHash(data []byte, hash []byte, algorithm string) (bool, error)

	// Secure communication operations
	EstablishSecureConnection(endpoint string) (*SecureConnection, error)
	ValidateConnection(conn *SecureConnection) error
	SecureTransfer(conn *SecureConnection, data []byte) error

	// Monitoring operations
	LogSecurityEvent(event *SecurityEvent) error
	GetSecurityEvents(filters *EventFilters) ([]SecurityEvent, error)
	GetThreatAnalysis() (*ThreatAnalysis, error)
	EnableRealTimeMonitoring(callback SecurityEventCallback) error
}

// SecurityStatus represents the current security status
type SecurityStatus struct {
	Level           SecurityLevel
	Healthy         bool
	ActiveThreats   int
	RecentEvents    int
	AuthSessions    int
	EncryptedConns  int
	LastUpdated     time.Time
	ComponentStatus map[string]bool
	Metrics         *SecurityMetrics
}

// SecurityMetrics holds security-related metrics
type SecurityMetrics struct {
	TotalOperations     uint64
	AuthAttempts        uint64
	AuthSuccesses       uint64
	AuthFailures        uint64
	EncryptionOps       uint64
	DecryptionOps       uint64
	SecureConnections   uint64
	ThreatsDetected     uint64
	ThreatsBlocked      uint64
	ViolationsFound     uint64
	EventsLogged        uint64
	AverageResponseTime time.Duration
	UptimePercentage    float64
}

// NewSecurityManager creates a new security manager
func NewSecurityManager(level SecurityLevel) *SecurityManager {
	manager := &SecurityManager{
		level:          level,
		config:         DefaultSecurityConfig(),
		securityEvents: make([]SecurityEvent, 0),
	}

	// Initialize subsystems based on security level
	if level >= SecurityBasic {
		manager.authManager = NewAuthenticationManager(manager.config)
	}

	if level >= SecurityStandard {
		manager.cryptoManager = NewCryptographyManager(manager.config)
	}

	if level >= SecurityStrict {
		manager.commManager = NewSecureCommunicationManager(manager.config)
		manager.monitorManager = NewSecurityMonitoringManager(manager.config)
	}

	return manager
}

// Initialize initializes the security manager with configuration
func (sm *SecurityManager) Initialize(config *SecurityConfig) error {
	if config != nil {
		sm.mu.Lock()
		sm.config = config
		sm.mu.Unlock()
	}

	// Initialize subsystems without holding the main lock
	if sm.authManager != nil {
		if err := sm.authManager.Initialize(sm.config); err != nil {
			return fmt.Errorf("failed to initialize authentication manager: %w", err)
		}
	}

	if sm.cryptoManager != nil {
		if err := sm.cryptoManager.Initialize(sm.config); err != nil {
			return fmt.Errorf("failed to initialize crypto manager: %w", err)
		}
	}

	if sm.commManager != nil {
		if err := sm.commManager.Initialize(sm.config); err != nil {
			return fmt.Errorf("failed to initialize communication manager: %w", err)
		}
	}

	if sm.monitorManager != nil {
		if err := sm.monitorManager.Initialize(sm.config); err != nil {
			return fmt.Errorf("failed to initialize monitoring manager: %w", err)
		}
	}

	// Log initialization (with proper locking)
	sm.mu.Lock()
	event := &SecurityEvent{
		ID:        generateEventID(),
		Type:      "system_initialization",
		Severity:  SeverityInfo,
		Message:   fmt.Sprintf("Security manager initialized with level %s", sm.level),
		Source:    "security_manager",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"level":  sm.level.String(),
			"config": sm.config,
		},
	}
	sm.logSecurityEvent(event)
	sm.mu.Unlock()

	return nil
}

// SetSecurityLevel changes the security level
func (sm *SecurityManager) SetSecurityLevel(level SecurityLevel) error {
	// Don't hold the lock while reinitializing to avoid deadlock
	sm.mu.Lock()
	oldLevel := sm.level
	sm.level = level
	sm.mu.Unlock()

	// Shutdown existing subsystems first to avoid conflicts
	if sm.monitorManager != nil {
		sm.monitorManager.Shutdown()
	}
	if sm.commManager != nil {
		sm.commManager.Shutdown()
	}
	if sm.cryptoManager != nil {
		sm.cryptoManager.Shutdown()
	}
	if sm.authManager != nil {
		sm.authManager.Shutdown()
	}

	// Reinitialize subsystems with new level
	if level >= SecurityBasic {
		sm.authManager = NewAuthenticationManager(sm.config)
	} else {
		sm.authManager = nil
	}

	if level >= SecurityStandard {
		sm.cryptoManager = NewCryptographyManager(sm.config)
	} else {
		sm.cryptoManager = nil
	}

	if level >= SecurityStrict {
		sm.commManager = NewSecureCommunicationManager(sm.config)
		sm.monitorManager = NewSecurityMonitoringManager(sm.config)
	} else {
		sm.commManager = nil
		sm.monitorManager = nil
	}

	// Initialize the new subsystems
	if err := sm.Initialize(sm.config); err != nil {
		// Rollback on error
		sm.mu.Lock()
		sm.level = oldLevel
		sm.mu.Unlock()
		return fmt.Errorf("failed to change security level: %w", err)
	}

	// Log level change
	sm.mu.Lock()
	event := &SecurityEvent{
		ID:        generateEventID(),
		Type:      "security_level_change",
		Severity:  SeverityWarning,
		Message:   fmt.Sprintf("Security level changed from %s to %s", oldLevel, level),
		Source:    "security_manager",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"old_level": oldLevel.String(),
			"new_level": level.String(),
		},
	}
	sm.logSecurityEvent(event)
	sm.mu.Unlock()

	return nil
}

// GetSecurityStatus returns current security status
func (sm *SecurityManager) GetSecurityStatus() *SecurityStatus {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	componentStatus := make(map[string]bool)
	componentStatus["authentication"] = sm.authManager == nil || sm.authManager.IsHealthy()
	componentStatus["cryptography"] = sm.cryptoManager == nil || sm.cryptoManager.IsHealthy()
	componentStatus["communication"] = sm.commManager == nil || sm.commManager.IsHealthy()
	componentStatus["monitoring"] = sm.monitorManager == nil || sm.monitorManager.IsHealthy()

	// Calculate overall health - true if all active components are healthy
	healthy := true
	for _, status := range componentStatus {
		if !status {
			healthy = false
			break
		}
	}

	metrics := &SecurityMetrics{
		TotalOperations:  sm.operationsPerformed.Load(),
		AuthAttempts:     sm.authAttempts.Load(),
		AuthSuccesses:    sm.authSuccesses.Load(),
		AuthFailures:     sm.authAttempts.Load() - sm.authSuccesses.Load(),
		ThreatsBlocked:   sm.threatsBlocked.Load(),
		ViolationsFound:  sm.violations.Load(),
		EventsLogged:     sm.eventCount.Load(),
		UptimePercentage: 99.9, // Would be calculated based on actual uptime
	}

	return &SecurityStatus{
		Level:           sm.level,
		Healthy:         healthy,
		ActiveThreats:   0, // Would be calculated from monitoring
		RecentEvents:    len(sm.securityEvents),
		AuthSessions:    0, // Would be from auth manager
		EncryptedConns:  0, // Would be from comm manager
		LastUpdated:     time.Now(),
		ComponentStatus: componentStatus,
		Metrics:         metrics,
	}
}

// logSecurityEvent logs a security event internally
func (sm *SecurityManager) logSecurityEvent(event *SecurityEvent) {
	sm.securityEvents = append(sm.securityEvents, *event)
	sm.eventCount.Add(1)

	// Cleanup old events to prevent memory growth
	if len(sm.securityEvents) > 10000 {
		sm.securityEvents = sm.securityEvents[1000:] // Keep last 9000 events
	}
}

// DefaultSecurityConfig returns default security configuration
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		// Authentication
		TokenTTL:               24 * time.Hour,
		MaxLoginAttempts:       5,
		LockoutDuration:        15 * time.Minute,
		RequireStrongPasswords: true,

		// Encryption
		EncryptionAlgorithm: "AES-256-GCM",
		KeySize:             256,
		RotationInterval:    30 * 24 * time.Hour, // 30 days

		// Communication
		RequireTLS:    true,
		TLSMinVersion: 0x0303, // TLS 1.2

		// Monitoring
		AuditLogging:       true,
		ThreatDetection:    true,
		RealTimeMonitoring: true,
		AlertThresholds: map[string]int{
			"failed_logins":     10,
			"suspicious_access": 5,
			"crypto_failures":   3,
			"connection_errors": 20,
		},
		RetentionPeriod: 90 * 24 * time.Hour, // 90 days

		// General
		EnableSecureHeaders:  true,
		EnableCSRFProtection: true,
		EnableRateLimiting:   true,
		MaxRequestsPerMinute: 100,
	}
}

// generateEventID generates a unique event ID
func generateEventID() string {
	timestamp := time.Now().UnixNano()
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)

	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%d", timestamp)))
	hasher.Write(randomBytes)

	return hex.EncodeToString(hasher.Sum(nil))[:16]
}

// String returns string representation of SecurityLevel
func (sl SecurityLevel) String() string {
	switch sl {
	case SecurityDisabled:
		return "DISABLED"
	case SecurityBasic:
		return "BASIC"
	case SecurityStandard:
		return "STANDARD"
	case SecurityStrict:
		return "STRICT"
	case SecurityMaximum:
		return "MAXIMUM"
	default:
		return "UNKNOWN"
	}
}

// Shutdown gracefully shuts down the security manager
func (sm *SecurityManager) Shutdown() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Shutdown subsystems
	var errors []error

	if sm.monitorManager != nil {
		if err := sm.monitorManager.Shutdown(); err != nil {
			errors = append(errors, fmt.Errorf("monitoring manager shutdown: %w", err))
		}
	}

	if sm.commManager != nil {
		if err := sm.commManager.Shutdown(); err != nil {
			errors = append(errors, fmt.Errorf("communication manager shutdown: %w", err))
		}
	}

	if sm.cryptoManager != nil {
		if err := sm.cryptoManager.Shutdown(); err != nil {
			errors = append(errors, fmt.Errorf("crypto manager shutdown: %w", err))
		}
	}

	if sm.authManager != nil {
		if err := sm.authManager.Shutdown(); err != nil {
			errors = append(errors, fmt.Errorf("auth manager shutdown: %w", err))
		}
	}

	// Log shutdown
	event := &SecurityEvent{
		ID:        generateEventID(),
		Type:      "system_shutdown",
		Severity:  SeverityInfo,
		Message:   "Security manager shutting down",
		Source:    "security_manager",
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"errors_count": len(errors),
		},
	}
	sm.logSecurityEvent(event)

	if len(errors) > 0 {
		return fmt.Errorf("shutdown completed with %d errors: %v", len(errors), errors)
	}

	return nil
}
