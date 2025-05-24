package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// CryptographyManager handles encryption, decryption, and cryptographic operations
type CryptographyManager struct {
	config *SecurityConfig

	// Key management
	keys         map[string]*EncryptionKey
	activeKey    string
	keyRotator   *time.Ticker
	stopRotation chan bool

	// Statistics
	encryptionOps atomic.Uint64
	decryptionOps atomic.Uint64
	hashOps       atomic.Uint64
	keyRotations  atomic.Uint64

	// Thread safety
	mu sync.RWMutex

	healthy atomic.Bool
}

// EncryptionKey represents an encryption key
type EncryptionKey struct {
	ID        string
	Algorithm string
	Key       []byte
	CreatedAt time.Time
	ExpiresAt time.Time
	IsActive  bool
	Usage     KeyUsage
	Metadata  map[string]interface{}
}

// KeyUsage defines how a key can be used
type KeyUsage int

const (
	KeyUsageEncryption KeyUsage = iota
	KeyUsageDecryption
	KeyUsageSigning
	KeyUsageVerification
	KeyUsageAll
)

func (ku KeyUsage) String() string {
	switch ku {
	case KeyUsageEncryption:
		return "ENCRYPTION"
	case KeyUsageDecryption:
		return "DECRYPTION"
	case KeyUsageSigning:
		return "SIGNING"
	case KeyUsageVerification:
		return "VERIFICATION"
	case KeyUsageAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// EncryptionOptions holds encryption configuration
type EncryptionOptions struct {
	Algorithm   string
	KeyID       string
	AAD         []byte // Additional Authenticated Data
	Compression bool
	Metadata    map[string]interface{}
}

// DecryptionOptions holds decryption configuration
type DecryptionOptions struct {
	KeyID       string
	AAD         []byte // Additional Authenticated Data
	Compression bool
	Metadata    map[string]interface{}
}

// EncryptedData represents encrypted data with metadata
type EncryptedData struct {
	Data      []byte
	KeyID     string
	Algorithm string
	IV        []byte
	AuthTag   []byte
	Metadata  map[string]interface{}
}

// HashOptions holds hashing configuration
type HashOptions struct {
	Algorithm string
	Salt      []byte
	Rounds    int
	Metadata  map[string]interface{}
}

// NewCryptographyManager creates a new cryptography manager
func NewCryptographyManager(config *SecurityConfig) *CryptographyManager {
	manager := &CryptographyManager{
		config: config,
		keys:   make(map[string]*EncryptionKey),
	}

	manager.healthy.Store(true)
	return manager
}

// Initialize initializes the cryptography manager
func (cm *CryptographyManager) Initialize(config *SecurityConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if config != nil {
		cm.config = config
	}

	// Generate initial key
	if err := cm.generateInitialKey(); err != nil {
		return fmt.Errorf("failed to generate initial key: %w", err)
	}

	// Start key rotation if configured
	if cm.config.RotationInterval > 0 {
		cm.keyRotator = time.NewTicker(cm.config.RotationInterval)
		cm.stopRotation = make(chan bool)
		go cm.backgroundKeyRotation()
	}

	return nil
}

// IsHealthy returns the health status of the cryptography manager
func (cm *CryptographyManager) IsHealthy() bool {
	return cm.healthy.Load()
}

// Encrypt encrypts data using the specified options
func (cm *CryptographyManager) Encrypt(data []byte, options *EncryptionOptions) (*CryptographyResult, error) {
	cm.encryptionOps.Add(1)

	if len(data) == 0 {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("data is empty")
	}

	// Use default options if not provided
	if options == nil {
		options = &EncryptionOptions{
			Algorithm: cm.config.EncryptionAlgorithm,
			KeyID:     cm.activeKey,
		}
	}

	// Get encryption key
	cm.mu.RLock()
	key, exists := cm.keys[options.KeyID]
	if !exists {
		key = cm.keys[cm.activeKey] // Fallback to active key
	}
	cm.mu.RUnlock()

	if key == nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("encryption key not found")
	}

	// Encrypt based on algorithm
	switch options.Algorithm {
	case "AES-256-GCM", "":
		return cm.encryptAESGCM(data, key, options)
	default:
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("unsupported encryption algorithm: %s", options.Algorithm)
	}
}

// Decrypt decrypts data using the specified options
func (cm *CryptographyManager) Decrypt(encryptedData []byte, options *DecryptionOptions) (*CryptographyResult, error) {
	cm.decryptionOps.Add(1)

	if len(encryptedData) == 0 {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("encrypted data is empty")
	}

	// Parse encrypted data format (simplified for this example)
	// In practice, you'd have a proper format for storing metadata

	// Use default options if not provided
	if options == nil {
		options = &DecryptionOptions{
			KeyID: cm.activeKey,
		}
	}

	// Get decryption key
	cm.mu.RLock()
	key, exists := cm.keys[options.KeyID]
	if !exists {
		key = cm.keys[cm.activeKey] // Fallback to active key
	}
	cm.mu.RUnlock()

	if key == nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("decryption key not found")
	}

	// Decrypt based on algorithm (assuming AES-GCM for this example)
	return cm.decryptAESGCM(encryptedData, key, options)
}

// Hash hashes data using the specified algorithm
func (cm *CryptographyManager) Hash(data []byte, algorithm string) ([]byte, error) {
	cm.hashOps.Add(1)

	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	switch algorithm {
	case "SHA-256", "":
		hasher := sha256.New()
		hasher.Write(data)
		return hasher.Sum(nil), nil

	case "SHA-512":
		hasher := sha512.New()
		hasher.Write(data)
		return hasher.Sum(nil), nil

	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}
}

// VerifyHash verifies data against its hash
func (cm *CryptographyManager) VerifyHash(data []byte, hash []byte, algorithm string) (bool, error) {
	computedHash, err := cm.Hash(data, algorithm)
	if err != nil {
		return false, fmt.Errorf("failed to compute hash: %w", err)
	}

	// Use constant-time comparison to prevent timing attacks
	return len(hash) == len(computedHash) &&
		subtle.ConstantTimeCompare(hash, computedHash) == 1, nil
}

// encryptAESGCM encrypts data using AES-GCM
func (cm *CryptographyManager) encryptAESGCM(data []byte, key *EncryptionKey, options *EncryptionOptions) (*CryptographyResult, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt data
	ciphertext := gcm.Seal(nil, nonce, data, options.AAD)

	// Combine nonce and ciphertext (simplified format)
	result := append(nonce, ciphertext...)

	return &CryptographyResult{
		Success:   true,
		Data:      result,
		Algorithm: "AES-256-GCM",
		KeyID:     key.ID,
		Metadata: map[string]interface{}{
			"nonce_size":    len(nonce),
			"original_size": len(data),
			"encrypted_at":  time.Now(),
		},
	}, nil
}

// decryptAESGCM decrypts data using AES-GCM
func (cm *CryptographyManager) decryptAESGCM(encryptedData []byte, key *EncryptionKey, options *DecryptionOptions) (*CryptographyResult, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedData) < nonceSize {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("encrypted data too short")
	}

	// Extract nonce and ciphertext
	nonce := encryptedData[:nonceSize]
	ciphertext := encryptedData[nonceSize:]

	// Decrypt data
	plaintext, err := gcm.Open(nil, nonce, ciphertext, options.AAD)
	if err != nil {
		return &CryptographyResult{
			Success: false,
		}, fmt.Errorf("failed to decrypt: %w", err)
	}

	return &CryptographyResult{
		Success:   true,
		Data:      plaintext,
		Algorithm: "AES-256-GCM",
		KeyID:     key.ID,
		Metadata: map[string]interface{}{
			"decrypted_size": len(plaintext),
			"decrypted_at":   time.Now(),
		},
	}, nil
}

// generateInitialKey generates the initial encryption key
func (cm *CryptographyManager) generateInitialKey() error {
	key, err := cm.generateKey("AES-256", 32)
	if err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
	}

	keyID := generateKeyID()
	cm.keys[keyID] = key
	cm.activeKey = keyID

	return nil
}

// generateKey generates a new encryption key
func (cm *CryptographyManager) generateKey(algorithm string, size int) (*EncryptionKey, error) {
	keyBytes := make([]byte, size)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}

	return &EncryptionKey{
		ID:        generateKeyID(),
		Algorithm: algorithm,
		Key:       keyBytes,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(cm.config.RotationInterval),
		IsActive:  true,
		Usage:     KeyUsageAll,
		Metadata:  make(map[string]interface{}),
	}, nil
}

// backgroundKeyRotation performs automatic key rotation
func (cm *CryptographyManager) backgroundKeyRotation() {
	for {
		select {
		case <-cm.keyRotator.C:
			cm.rotateKeys()
		case <-cm.stopRotation:
			return
		}
	}
}

// rotateKeys rotates encryption keys
func (cm *CryptographyManager) rotateKeys() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Generate new key
	newKey, err := cm.generateKey(cm.config.EncryptionAlgorithm, cm.config.KeySize/8)
	if err != nil {
		// Log error in production
		return
	}

	// Mark old key as inactive
	if oldKey, exists := cm.keys[cm.activeKey]; exists {
		oldKey.IsActive = false
	}

	// Set new key as active
	cm.keys[newKey.ID] = newKey
	cm.activeKey = newKey.ID
	cm.keyRotations.Add(1)

	// Clean up old keys (keep last 5 for decryption)
	if len(cm.keys) > 5 {
		var oldest string
		var oldestTime time.Time
		for id, key := range cm.keys {
			if !key.IsActive && (oldest == "" || key.CreatedAt.Before(oldestTime)) {
				oldest = id
				oldestTime = key.CreatedAt
			}
		}
		if oldest != "" {
			delete(cm.keys, oldest)
		}
	}
}

// GetKeyInfo returns information about encryption keys
func (cm *CryptographyManager) GetKeyInfo() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	keyInfo := make(map[string]interface{})
	keyInfo["active_key"] = cm.activeKey
	keyInfo["total_keys"] = len(cm.keys)
	keyInfo["key_rotations"] = cm.keyRotations.Load()

	var activeKeys, inactiveKeys int
	for _, key := range cm.keys {
		if key.IsActive {
			activeKeys++
		} else {
			inactiveKeys++
		}
	}

	keyInfo["active_keys"] = activeKeys
	keyInfo["inactive_keys"] = inactiveKeys

	return keyInfo
}

// GetCryptoStats returns cryptography statistics
func (cm *CryptographyManager) GetCryptoStats() map[string]interface{} {
	return map[string]interface{}{
		"encryption_ops": cm.encryptionOps.Load(),
		"decryption_ops": cm.decryptionOps.Load(),
		"hash_ops":       cm.hashOps.Load(),
		"key_rotations":  cm.keyRotations.Load(),
		"healthy":        cm.healthy.Load(),
	}
}

// Shutdown gracefully shuts down the cryptography manager
func (cm *CryptographyManager) Shutdown() error {
	if cm.keyRotator != nil {
		cm.keyRotator.Stop()
	}

	if cm.stopRotation != nil {
		close(cm.stopRotation)
	}

	// Clear sensitive data
	cm.mu.Lock()
	for id, key := range cm.keys {
		// Zero out key material
		for i := range key.Key {
			key.Key[i] = 0
		}
		delete(cm.keys, id)
	}
	cm.mu.Unlock()

	cm.healthy.Store(false)
	return nil
}

// generateKeyID generates a unique key ID
func generateKeyID() string {
	return "key_" + generateRandomID(16)
}
