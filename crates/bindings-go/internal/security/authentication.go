package security

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

// AuthenticationManager handles user authentication and token management
type AuthenticationManager struct {
	config *SecurityConfig

	// User and session storage
	users     map[string]*User // keyed by username
	usersByID map[string]*User // keyed by userID
	sessions  map[string]*Session
	tokens    map[string]*Token
	lockouts  map[string]*Lockout

	// Security state
	authAttempts   atomic.Uint64
	authSuccesses  atomic.Uint64
	authFailures   atomic.Uint64
	activeSessions atomic.Uint32
	lockedAccounts atomic.Uint32

	// Thread safety
	mu sync.RWMutex

	// Background operations
	cleanupTicker *time.Ticker
	stopCleanup   chan bool

	healthy atomic.Bool
}

// User represents a user account
type User struct {
	ID           string
	Username     string
	Email        string
	PasswordHash []byte
	Salt         []byte
	Permissions  []string
	Roles        []string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastLoginAt  *time.Time
	IsActive     bool
	IsLocked     bool
	MFAEnabled   bool
	MFASecret    string
	Metadata     map[string]interface{}
}

// Session represents an active user session
type Session struct {
	ID          string
	UserID      string
	Token       string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	LastSeen    time.Time
	IPAddress   string
	UserAgent   string
	Permissions []string
	Metadata    map[string]interface{}
	IsActive    bool
}

// Token represents an authentication token
type Token struct {
	ID          string
	UserID      string
	Type        TokenType
	Value       string
	HashedValue []byte
	CreatedAt   time.Time
	ExpiresAt   time.Time
	IsActive    bool
	Scopes      []string
	Metadata    map[string]interface{}
}

// TokenType defines different types of tokens
type TokenType int

const (
	TokenTypeAccess TokenType = iota
	TokenTypeRefresh
	TokenTypeReset
	TokenTypeActivation
	TokenTypeAPI
)

func (tt TokenType) String() string {
	switch tt {
	case TokenTypeAccess:
		return "ACCESS"
	case TokenTypeRefresh:
		return "REFRESH"
	case TokenTypeReset:
		return "RESET"
	case TokenTypeActivation:
		return "ACTIVATION"
	case TokenTypeAPI:
		return "API"
	default:
		return "UNKNOWN"
	}
}

// Lockout represents an account lockout
type Lockout struct {
	UserID       string
	Attempts     int
	LastAttempt  time.Time
	LockoutUntil time.Time
	IPAddress    string
	Reason       string
}

// Credentials represents user credentials for authentication
type Credentials struct {
	Username string
	Password string
	Email    string
	MFACode  string
	Metadata map[string]interface{}
}

// TokenValidationResult represents token validation result
type TokenValidationResult struct {
	Valid       bool
	Token       *Token
	User        *User
	Session     *Session
	Permissions []string
	ExpiresAt   time.Time
	Reason      string
	Metadata    map[string]interface{}
}

// AuthenticationOptions holds authentication configuration
type AuthenticationOptions struct {
	RequireMFA         bool
	AllowPasswordReset bool
	SessionTimeout     time.Duration
	TokenTimeout       time.Duration
	MaxSessions        int
	IPWhitelist        []string
	IPBlacklist        []string
}

// NewAuthenticationManager creates a new authentication manager
func NewAuthenticationManager(config *SecurityConfig) *AuthenticationManager {
	manager := &AuthenticationManager{
		config:    config,
		users:     make(map[string]*User),
		usersByID: make(map[string]*User),
		sessions:  make(map[string]*Session),
		tokens:    make(map[string]*Token),
		lockouts:  make(map[string]*Lockout),
	}

	manager.healthy.Store(true)
	return manager
}

// Initialize initializes the authentication manager
func (am *AuthenticationManager) Initialize(config *SecurityConfig) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if config != nil {
		am.config = config
	}

	// Start background cleanup
	am.cleanupTicker = time.NewTicker(5 * time.Minute)
	am.stopCleanup = make(chan bool)

	go am.backgroundCleanup()

	return nil
}

// IsHealthy returns the health status of the authentication manager
func (am *AuthenticationManager) IsHealthy() bool {
	return am.healthy.Load()
}

// Authenticate authenticates user credentials
func (am *AuthenticationManager) Authenticate(credentials *Credentials) (*AuthenticationResult, error) {
	am.authAttempts.Add(1)

	// Validate input
	if credentials == nil || credentials.Username == "" || credentials.Password == "" {
		am.authFailures.Add(1)
		return &AuthenticationResult{
			Success: false,
			Reason:  "invalid credentials",
		}, fmt.Errorf("invalid credentials provided")
	}

	am.mu.RLock()
	user, exists := am.users[credentials.Username]
	lockout, isLocked := am.lockouts[credentials.Username]
	am.mu.RUnlock()

	// Check if account is locked
	if isLocked && time.Now().Before(lockout.LockoutUntil) {
		am.authFailures.Add(1)
		return &AuthenticationResult{
			Success: false,
			Reason:  "account locked",
		}, fmt.Errorf("account is locked until %v", lockout.LockoutUntil)
	}

	// Check if user exists
	if !exists || !user.IsActive {
		am.authFailures.Add(1)
		am.recordFailedAttempt(credentials.Username, "user not found")
		return &AuthenticationResult{
			Success: false,
			Reason:  "invalid username or password",
		}, fmt.Errorf("authentication failed")
	}

	// Verify password
	if !am.verifyPassword(credentials.Password, user.PasswordHash, user.Salt) {
		am.authFailures.Add(1)
		am.recordFailedAttempt(credentials.Username, "invalid password")
		return &AuthenticationResult{
			Success: false,
			Reason:  "invalid username or password",
		}, fmt.Errorf("authentication failed")
	}

	// Check MFA if enabled
	if user.MFAEnabled && credentials.MFACode != "" {
		if !am.verifyMFA(user.MFASecret, credentials.MFACode) {
			am.authFailures.Add(1)
			am.recordFailedAttempt(credentials.Username, "invalid MFA code")
			return &AuthenticationResult{
				Success: false,
				Reason:  "invalid MFA code",
			}, fmt.Errorf("MFA verification failed")
		}
	}

	// Authentication successful
	am.authSuccesses.Add(1)
	am.clearFailedAttempts(credentials.Username)

	// Create session and token
	session, token, err := am.createSessionAndToken(user)
	if err != nil {
		return &AuthenticationResult{
			Success: false,
			Reason:  "session creation failed",
		}, fmt.Errorf("failed to create session: %w", err)
	}

	// Update user last login
	am.mu.Lock()
	now := time.Now()
	user.LastLoginAt = &now
	user.UpdatedAt = now
	am.mu.Unlock()

	return &AuthenticationResult{
		Success:     true,
		UserID:      user.ID,
		Token:       token.Value,
		ExpiresAt:   token.ExpiresAt,
		Permissions: user.Permissions,
		Reason:      "authentication successful",
		Metadata: map[string]interface{}{
			"session_id": session.ID,
			"token_type": token.Type.String(),
			"user_roles": user.Roles,
			"mfa_used":   user.MFAEnabled && credentials.MFACode != "",
		},
	}, nil
}

// ValidateToken validates an authentication token
func (am *AuthenticationManager) ValidateToken(tokenValue string) (*TokenValidationResult, error) {
	if tokenValue == "" {
		return &TokenValidationResult{
			Valid:  false,
			Reason: "empty token",
		}, fmt.Errorf("token is empty")
	}

	am.mu.RLock()
	defer am.mu.RUnlock()

	// Find token
	var token *Token
	for _, t := range am.tokens {
		if subtle.ConstantTimeCompare([]byte(t.Value), []byte(tokenValue)) == 1 {
			token = t
			break
		}
	}

	if token == nil {
		return &TokenValidationResult{
			Valid:  false,
			Reason: "token not found",
		}, fmt.Errorf("token not found")
	}

	// Check if token is active and not expired
	if !token.IsActive {
		return &TokenValidationResult{
			Valid:  false,
			Reason: "token inactive",
		}, fmt.Errorf("token is inactive")
	}

	if time.Now().After(token.ExpiresAt) {
		return &TokenValidationResult{
			Valid:  false,
			Reason: "token expired",
		}, fmt.Errorf("token has expired")
	}

	// Get user
	user, exists := am.usersByID[token.UserID]
	if !exists || !user.IsActive {
		return &TokenValidationResult{
			Valid:  false,
			Reason: "user not found or inactive",
		}, fmt.Errorf("user not found or inactive")
	}

	// Get session if exists
	var session *Session
	for _, s := range am.sessions {
		if s.UserID == user.ID && s.Token == token.Value {
			session = s
			break
		}
	}

	return &TokenValidationResult{
		Valid:       true,
		Token:       token,
		User:        user,
		Session:     session,
		Permissions: user.Permissions,
		ExpiresAt:   token.ExpiresAt,
		Reason:      "token valid",
		Metadata: map[string]interface{}{
			"token_type": token.Type.String(),
			"user_roles": user.Roles,
			"session_id": session.ID,
			"last_seen":  session.LastSeen,
		},
	}, nil
}

// RevokeToken revokes an authentication token
func (am *AuthenticationManager) RevokeToken(tokenValue string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Find and revoke token
	for _, token := range am.tokens {
		if subtle.ConstantTimeCompare([]byte(token.Value), []byte(tokenValue)) == 1 {
			token.IsActive = false

			// Also revoke associated session
			for _, session := range am.sessions {
				if session.Token == tokenValue {
					session.IsActive = false
					am.activeSessions.Add(^uint32(0)) // Decrement
					break
				}
			}

			return nil
		}
	}

	return fmt.Errorf("token not found")
}

// RefreshToken refreshes an authentication token
func (am *AuthenticationManager) RefreshToken(tokenValue string) (*AuthenticationResult, error) {
	// Validate current token
	validation, err := am.ValidateToken(tokenValue)
	if err != nil || !validation.Valid {
		return &AuthenticationResult{
			Success: false,
			Reason:  "invalid token for refresh",
		}, fmt.Errorf("token validation failed: %w", err)
	}

	// Revoke old token
	if err := am.RevokeToken(tokenValue); err != nil {
		return &AuthenticationResult{
			Success: false,
			Reason:  "failed to revoke old token",
		}, fmt.Errorf("failed to revoke old token: %w", err)
	}

	// Create new session and token
	session, token, err := am.createSessionAndToken(validation.User)
	if err != nil {
		return &AuthenticationResult{
			Success: false,
			Reason:  "session creation failed",
		}, fmt.Errorf("failed to create new session: %w", err)
	}

	return &AuthenticationResult{
		Success:     true,
		UserID:      validation.User.ID,
		Token:       token.Value,
		ExpiresAt:   token.ExpiresAt,
		Permissions: validation.User.Permissions,
		Reason:      "token refreshed",
		Metadata: map[string]interface{}{
			"session_id":     session.ID,
			"token_type":     token.Type.String(),
			"refreshed_from": validation.Token.ID,
		},
	}, nil
}

// CreateUser creates a new user account
func (am *AuthenticationManager) CreateUser(username, email, password string, permissions []string) (*User, error) {
	// Validate password strength
	if am.config.RequireStrongPasswords && !am.isStrongPassword(password) {
		return nil, fmt.Errorf("password does not meet strength requirements")
	}

	// Generate salt and hash password
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	passwordHash, err := am.hashPassword(password, salt)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	user := &User{
		ID:           generateUserID(),
		Username:     username,
		Email:        email,
		PasswordHash: passwordHash,
		Salt:         salt,
		Permissions:  permissions,
		Roles:        []string{"user"},
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		IsActive:     true,
		IsLocked:     false,
		MFAEnabled:   false,
		Metadata:     make(map[string]interface{}),
	}

	am.mu.Lock()
	am.users[username] = user
	am.usersByID[user.ID] = user
	am.mu.Unlock()

	return user, nil
}

// verifyPassword verifies a password against its hash
func (am *AuthenticationManager) verifyPassword(password string, hash, salt []byte) bool {
	expectedHash, err := am.hashPassword(password, salt)
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare(hash, expectedHash) == 1
}

// hashPassword hashes a password with salt using SHA-256
func (am *AuthenticationManager) hashPassword(password string, salt []byte) ([]byte, error) {
	hasher := sha256.New()
	hasher.Write([]byte(password))
	hasher.Write(salt)

	// Additional rounds for security
	hash := hasher.Sum(nil)
	for i := 0; i < 100000; i++ {
		hasher.Reset()
		hasher.Write(hash)
		hasher.Write(salt)
		hash = hasher.Sum(nil)
	}

	return hash, nil
}

// isStrongPassword checks if password meets strength requirements
func (am *AuthenticationManager) isStrongPassword(password string) bool {
	if len(password) < 8 {
		return false
	}

	hasUpper := regexp.MustCompile(`[A-Z]`).MatchString(password)
	hasLower := regexp.MustCompile(`[a-z]`).MatchString(password)
	hasDigit := regexp.MustCompile(`\d`).MatchString(password)
	hasSpecial := regexp.MustCompile(`[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]`).MatchString(password)

	return hasUpper && hasLower && hasDigit && hasSpecial
}

// verifyMFA verifies MFA code (simplified TOTP implementation)
func (am *AuthenticationManager) verifyMFA(secret, code string) bool {
	// This is a simplified implementation
	// In production, use a proper TOTP library
	return code != "" && len(code) == 6
}

// createSessionAndToken creates a new session and token for user
func (am *AuthenticationManager) createSessionAndToken(user *User) (*Session, *Token, error) {
	sessionID := generateSessionID()
	tokenValue := generateTokenValue()

	now := time.Now()
	expiresAt := now.Add(am.config.TokenTTL)

	session := &Session{
		ID:          sessionID,
		UserID:      user.ID,
		Token:       tokenValue,
		CreatedAt:   now,
		ExpiresAt:   expiresAt,
		LastSeen:    now,
		Permissions: user.Permissions,
		IsActive:    true,
		Metadata:    make(map[string]interface{}),
	}

	tokenHash := sha256.Sum256([]byte(tokenValue))
	token := &Token{
		ID:          generateTokenID(),
		UserID:      user.ID,
		Type:        TokenTypeAccess,
		Value:       tokenValue,
		HashedValue: tokenHash[:],
		CreatedAt:   now,
		ExpiresAt:   expiresAt,
		IsActive:    true,
		Scopes:      user.Permissions,
		Metadata:    make(map[string]interface{}),
	}

	am.mu.Lock()
	am.sessions[sessionID] = session
	am.tokens[token.ID] = token
	am.activeSessions.Add(1)
	am.mu.Unlock()

	return session, token, nil
}

// recordFailedAttempt records a failed authentication attempt
func (am *AuthenticationManager) recordFailedAttempt(username, reason string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	lockout, exists := am.lockouts[username]
	if !exists {
		lockout = &Lockout{
			UserID:      username,
			Attempts:    0,
			LastAttempt: time.Now(),
			Reason:      reason,
		}
		am.lockouts[username] = lockout
	}

	lockout.Attempts++
	lockout.LastAttempt = time.Now()
	lockout.Reason = reason

	// Lock account if max attempts reached
	if lockout.Attempts >= am.config.MaxLoginAttempts {
		lockout.LockoutUntil = time.Now().Add(am.config.LockoutDuration)
		am.lockedAccounts.Add(1)
	}
}

// clearFailedAttempts clears failed authentication attempts for user
func (am *AuthenticationManager) clearFailedAttempts(username string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if lockout, exists := am.lockouts[username]; exists {
		if !lockout.LockoutUntil.IsZero() {
			am.lockedAccounts.Add(^uint32(0)) // Decrement
		}
		delete(am.lockouts, username)
	}
}

// backgroundCleanup performs background cleanup of expired sessions and tokens
func (am *AuthenticationManager) backgroundCleanup() {
	for {
		select {
		case <-am.cleanupTicker.C:
			am.cleanupExpired()
		case <-am.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes expired sessions, tokens, and lockouts
func (am *AuthenticationManager) cleanupExpired() {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()

	// Cleanup expired sessions
	for id, session := range am.sessions {
		if now.After(session.ExpiresAt) || !session.IsActive {
			delete(am.sessions, id)
			if session.IsActive {
				am.activeSessions.Add(^uint32(0)) // Decrement
			}
		}
	}

	// Cleanup expired tokens
	for id, token := range am.tokens {
		if now.After(token.ExpiresAt) || !token.IsActive {
			delete(am.tokens, id)
		}
	}

	// Cleanup expired lockouts
	for username, lockout := range am.lockouts {
		if !lockout.LockoutUntil.IsZero() && now.After(lockout.LockoutUntil) {
			delete(am.lockouts, username)
			am.lockedAccounts.Add(^uint32(0)) // Decrement
		}
	}
}

// Shutdown gracefully shuts down the authentication manager
func (am *AuthenticationManager) Shutdown() error {
	if am.cleanupTicker != nil {
		am.cleanupTicker.Stop()
	}

	if am.stopCleanup != nil {
		close(am.stopCleanup)
	}

	am.healthy.Store(false)
	return nil
}

// Helper functions for generating IDs
func generateUserID() string {
	return "user_" + generateRandomID(16)
}

func generateSessionID() string {
	return "sess_" + generateRandomID(24)
}

func generateTokenID() string {
	return "tok_" + generateRandomID(20)
}

func generateTokenValue() string {
	return generateRandomID(64)
}

func generateRandomID(length int) string {
	bytes := make([]byte, length)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)[:length]
}
