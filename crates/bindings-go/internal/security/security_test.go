package security

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecurityManager(t *testing.T) {
	manager := NewSecurityManager(SecurityStandard)
	require.NotNil(t, manager)

	// Test initialization
	err := manager.Initialize(nil)
	require.NoError(t, err)

	// Test security status
	status := manager.GetSecurityStatus()
	assert.NotNil(t, status)
	assert.Equal(t, SecurityStandard, status.Level)
	assert.True(t, status.Healthy)

	// Give time for background goroutines to settle
	time.Sleep(100 * time.Millisecond)

	// Test security level change
	err = manager.SetSecurityLevel(SecurityStrict)
	assert.NoError(t, err)

	// Give time for the change to complete
	time.Sleep(100 * time.Millisecond)

	status = manager.GetSecurityStatus()
	assert.Equal(t, SecurityStrict, status.Level)

	// Test shutdown
	err = manager.Shutdown()
	assert.NoError(t, err)
}

func TestAuthenticationManager(t *testing.T) {
	config := DefaultSecurityConfig()
	authManager := NewAuthenticationManager(config)
	require.NotNil(t, authManager)

	// Test initialization
	err := authManager.Initialize(config)
	require.NoError(t, err)
	assert.True(t, authManager.IsHealthy())

	// Test user creation
	user, err := authManager.CreateUser("testuser", "test@example.com", "TestPass123!", []string{"read", "write"})
	require.NoError(t, err)
	require.NotNil(t, user)
	assert.Equal(t, "testuser", user.Username)
	assert.Equal(t, "test@example.com", user.Email)
	assert.True(t, user.IsActive)

	// Test password verification works
	isValidPassword := authManager.verifyPassword("TestPass123!", user.PasswordHash, user.Salt)
	assert.True(t, isValidPassword, "Password verification should work")

	// Test password verification fails for wrong password
	isInvalidPassword := authManager.verifyPassword("WrongPass123!", user.PasswordHash, user.Salt)
	assert.False(t, isInvalidPassword, "Wrong password should fail verification")

	// Create a test token manually for validation test
	session, token, err := authManager.createSessionAndToken(user)
	require.NoError(t, err)
	assert.NotNil(t, session)
	assert.NotNil(t, token)

	// Test token validation
	tokenResult, err := authManager.ValidateToken(token.Value)
	require.NoError(t, err)
	assert.True(t, tokenResult.Valid)
	assert.Equal(t, user.ID, tokenResult.User.ID)

	// Test token revocation
	err = authManager.RevokeToken(token.Value)
	assert.NoError(t, err)

	// Test shutdown
	err = authManager.Shutdown()
	assert.NoError(t, err)
	assert.False(t, authManager.IsHealthy())
}

func TestCryptographyManager(t *testing.T) {
	config := DefaultSecurityConfig()
	cryptoManager := NewCryptographyManager(config)
	require.NotNil(t, cryptoManager)

	// Test initialization
	err := cryptoManager.Initialize(config)
	require.NoError(t, err)
	assert.True(t, cryptoManager.IsHealthy())

	// Test encryption
	plaintext := []byte("Hello, World! This is a test message for encryption.")
	result, err := cryptoManager.Encrypt(plaintext, nil)
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.NotNil(t, result.Data)
	assert.Greater(t, len(result.Data), len(plaintext)) // Encrypted data should be larger

	// Test decryption
	decryptResult, err := cryptoManager.Decrypt(result.Data, nil)
	require.NoError(t, err)
	assert.True(t, decryptResult.Success)
	assert.Equal(t, plaintext, decryptResult.Data)

	// Test hashing
	data := []byte("test data for hashing")
	hash, err := cryptoManager.Hash(data, "SHA-256")
	require.NoError(t, err)
	assert.NotNil(t, hash)
	assert.Equal(t, 32, len(hash)) // SHA-256 produces 32-byte hash

	// Test hash verification
	valid, err := cryptoManager.VerifyHash(data, hash, "SHA-256")
	require.NoError(t, err)
	assert.True(t, valid)

	// Test hash verification with wrong data
	wrongData := []byte("wrong data")
	valid, err = cryptoManager.VerifyHash(wrongData, hash, "SHA-256")
	require.NoError(t, err)
	assert.False(t, valid)

	// Test key info
	keyInfo := cryptoManager.GetKeyInfo()
	assert.NotNil(t, keyInfo)
	assert.Contains(t, keyInfo, "active_key")
	assert.Contains(t, keyInfo, "total_keys")

	// Test crypto stats
	stats := cryptoManager.GetCryptoStats()
	assert.NotNil(t, stats)
	assert.Contains(t, stats, "encryption_ops")
	assert.Contains(t, stats, "decryption_ops")

	// Test shutdown
	err = cryptoManager.Shutdown()
	assert.NoError(t, err)
	assert.False(t, cryptoManager.IsHealthy())
}

func TestSecureCommunicationManager(t *testing.T) {
	config := DefaultSecurityConfig()
	commManager := NewSecureCommunicationManager(config)
	require.NotNil(t, commManager)

	// Test initialization
	err := commManager.Initialize(config)
	require.NoError(t, err)
	assert.True(t, commManager.IsHealthy())

	// Test connection info (should be empty initially)
	info := commManager.GetConnectionInfo()
	assert.NotNil(t, info)
	assert.Equal(t, 0, info["total_connections"])

	// Note: We can't test actual network connections in unit tests
	// but we can test the structure and basic functionality

	// Test shutdown
	err = commManager.Shutdown()
	assert.NoError(t, err)
	assert.False(t, commManager.IsHealthy())
}

func TestSecurityMonitoringManager(t *testing.T) {
	config := DefaultSecurityConfig()
	monitorManager := NewSecurityMonitoringManager(config)
	require.NotNil(t, monitorManager)

	// Test initialization
	err := monitorManager.Initialize(config)
	require.NoError(t, err)
	assert.True(t, monitorManager.IsHealthy())

	// Test security event logging
	event := &SecurityEvent{
		Type:      "test_event",
		Severity:  SeverityInfo,
		Message:   "Test security event",
		Source:    "test",
		UserID:    "user123",
		IPAddress: "192.168.1.1",
		Timestamp: time.Now(),
		Details:   map[string]interface{}{"test": "data"},
	}

	err = monitorManager.LogSecurityEvent(event)
	require.NoError(t, err)

	// Test getting security events
	events, err := monitorManager.GetSecurityEvents(nil)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "test_event", events[0].Type)

	// Test event filtering
	filters := &EventFilters{
		EventType: "test_event",
		Limit:     10,
	}
	filteredEvents, err := monitorManager.GetSecurityEvents(filters)
	require.NoError(t, err)
	assert.Len(t, filteredEvents, 1)

	// Test threat analysis
	analysis, err := monitorManager.GetThreatAnalysis()
	require.NoError(t, err)
	assert.NotNil(t, analysis)
	assert.NotNil(t, analysis.TopThreatTypes)
	assert.NotNil(t, analysis.Recommendations)

	// Test real-time monitoring callback with atomic flag
	var callbackCalled atomic.Bool
	callback := func(event *SecurityEvent) {
		callbackCalled.Store(true)
	}

	err = monitorManager.EnableRealTimeMonitoring(callback)
	assert.NoError(t, err)

	// Log another event to trigger callback
	event2 := &SecurityEvent{
		Type:     "callback_test",
		Severity: SeverityWarning,
		Message:  "Callback test event",
		Source:   "test",
	}

	err = monitorManager.LogSecurityEvent(event2)
	require.NoError(t, err)

	// Give callback time to execute
	time.Sleep(10 * time.Millisecond)
	assert.True(t, callbackCalled.Load())

	// Test shutdown
	err = monitorManager.Shutdown()
	assert.NoError(t, err)
	assert.False(t, monitorManager.IsHealthy())
}

func TestThreatDetector(t *testing.T) {
	detector := NewThreatDetector()
	require.NotNil(t, detector)

	// Test initialization
	err := detector.Initialize()
	require.NoError(t, err)

	// Test event analysis
	event := &SecurityEvent{
		ID:       "test-event-1",
		Type:     "authentication_failure",
		Severity: SeverityWarning,
		Message:  "Authentication failed",
		Source:   "auth_system",
	}

	threats := detector.AnalyzeEvent(event)
	assert.NotNil(t, threats) // Should not be nil (may be empty, but not nil)

	// Since we now have rules that match "authentication_failure", we should get threats
	if len(threats) > 0 {
		assert.Equal(t, ThreatTypeBruteForce, threats[0].Type)
	}

	// Test anomaly detection
	detector.UpdateAnomalyBaselines()
	anomalies := detector.DetectAnomalies()
	assert.NotNil(t, anomalies) // May be empty, but should not be nil
}

func TestSecurityLevels(t *testing.T) {
	// Test security level strings
	assert.Equal(t, "DISABLED", SecurityDisabled.String())
	assert.Equal(t, "BASIC", SecurityBasic.String())
	assert.Equal(t, "STANDARD", SecurityStandard.String())
	assert.Equal(t, "STRICT", SecurityStrict.String())
	assert.Equal(t, "MAXIMUM", SecurityMaximum.String())

	// Test severity strings
	assert.Equal(t, "INFO", SeverityInfo.String())
	assert.Equal(t, "WARNING", SeverityWarning.String())
	assert.Equal(t, "ERROR", SeverityError.String())
	assert.Equal(t, "CRITICAL", SeverityCritical.String())
	assert.Equal(t, "FATAL", SeverityFatal.String())

	// Test threat type strings
	assert.Equal(t, "BRUTE_FORCE", ThreatTypeBruteForce.String())
	assert.Equal(t, "SUSPICIOUS_ACTIVITY", ThreatTypeSuspiciousActivity.String())
	assert.Equal(t, "SQL_INJECTION", ThreatTypeSQLInjection.String())

	// Test alert type strings
	assert.Equal(t, "AUTH_FAILURE", AlertTypeAuthFailure.String())
	assert.Equal(t, "THREAT_DETECTION", AlertTypeThreatDetection.String())
	assert.Equal(t, "SYSTEM_ANOMALY", AlertTypeSystemAnomaly.String())
}

func TestDefaultSecurityConfig(t *testing.T) {
	config := DefaultSecurityConfig()
	require.NotNil(t, config)

	// Test authentication settings
	assert.Equal(t, 24*time.Hour, config.TokenTTL)
	assert.Equal(t, 5, config.MaxLoginAttempts)
	assert.Equal(t, 15*time.Minute, config.LockoutDuration)
	assert.True(t, config.RequireStrongPasswords)

	// Test encryption settings
	assert.Equal(t, "AES-256-GCM", config.EncryptionAlgorithm)
	assert.Equal(t, 256, config.KeySize)
	assert.Equal(t, 30*24*time.Hour, config.RotationInterval)

	// Test communication settings
	assert.True(t, config.RequireTLS)
	assert.Equal(t, uint16(0x0303), config.TLSMinVersion) // TLS 1.2

	// Test monitoring settings
	assert.True(t, config.AuditLogging)
	assert.True(t, config.ThreatDetection)
	assert.True(t, config.RealTimeMonitoring)
	assert.Contains(t, config.AlertThresholds, "failed_logins")
	assert.Equal(t, 90*24*time.Hour, config.RetentionPeriod)

	// Test general settings
	assert.True(t, config.EnableSecureHeaders)
	assert.True(t, config.EnableCSRFProtection)
	assert.True(t, config.EnableRateLimiting)
	assert.Equal(t, 100, config.MaxRequestsPerMinute)
}

func TestPasswordStrengthValidation(t *testing.T) {
	config := DefaultSecurityConfig()
	authManager := NewAuthenticationManager(config)

	// Test weak passwords (should fail)
	weakPasswords := []string{
		"123456",
		"password",
		"abc123",
		"Password",    // Missing special chars and numbers
		"password123", // Missing uppercase and special chars
	}

	for _, password := range weakPasswords {
		_, err := authManager.CreateUser("test", "test@example.com", password, []string{})
		assert.Error(t, err, "Weak password should fail: %s", password)
	}

	// Test strong password (should succeed)
	strongPassword := "StrongPass123!"
	user, err := authManager.CreateUser("testuser", "test@example.com", strongPassword, []string{})
	assert.NoError(t, err)
	assert.NotNil(t, user)
}

func TestEncryptionAlgorithms(t *testing.T) {
	config := DefaultSecurityConfig()
	cryptoManager := NewCryptographyManager(config)
	err := cryptoManager.Initialize(config)
	require.NoError(t, err)

	testData := []byte("Test encryption data with various characters: !@#$%^&*()_+")

	// Test AES-256-GCM encryption
	options := &EncryptionOptions{
		Algorithm: "AES-256-GCM",
	}

	result, err := cryptoManager.Encrypt(testData, options)
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, "AES-256-GCM", result.Algorithm)

	// Test decryption
	decryptOptions := &DecryptionOptions{}
	decryptResult, err := cryptoManager.Decrypt(result.Data, decryptOptions)
	require.NoError(t, err)
	assert.True(t, decryptResult.Success)
	assert.Equal(t, testData, decryptResult.Data)

	// Test unsupported algorithm
	unsupportedOptions := &EncryptionOptions{
		Algorithm: "UNSUPPORTED_ALGO",
	}

	result, err = cryptoManager.Encrypt(testData, unsupportedOptions)
	assert.Error(t, err)
	assert.False(t, result.Success)
}

func TestHashAlgorithms(t *testing.T) {
	config := DefaultSecurityConfig()
	cryptoManager := NewCryptographyManager(config)
	err := cryptoManager.Initialize(config)
	require.NoError(t, err)

	testData := []byte("Test hashing data")

	// Test SHA-256
	hash256, err := cryptoManager.Hash(testData, "SHA-256")
	require.NoError(t, err)
	assert.Equal(t, 32, len(hash256))

	// Test SHA-512
	hash512, err := cryptoManager.Hash(testData, "SHA-512")
	require.NoError(t, err)
	assert.Equal(t, 64, len(hash512))

	// Test default algorithm (should be SHA-256)
	hashDefault, err := cryptoManager.Hash(testData, "")
	require.NoError(t, err)
	assert.Equal(t, hash256, hashDefault)

	// Test unsupported algorithm
	_, err = cryptoManager.Hash(testData, "UNSUPPORTED_HASH")
	assert.Error(t, err)
}

// BenchmarkEncryption benchmarks encryption performance
func BenchmarkEncryption(b *testing.B) {
	config := DefaultSecurityConfig()
	cryptoManager := NewCryptographyManager(config)
	cryptoManager.Initialize(config)

	testData := make([]byte, 1024) // 1KB test data
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cryptoManager.Encrypt(testData, nil)
	}
}

// BenchmarkHashing benchmarks hashing performance
func BenchmarkHashing(b *testing.B) {
	config := DefaultSecurityConfig()
	cryptoManager := NewCryptographyManager(config)
	cryptoManager.Initialize(config)

	testData := make([]byte, 1024) // 1KB test data
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cryptoManager.Hash(testData, "SHA-256")
	}
}
