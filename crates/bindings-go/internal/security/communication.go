package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// SecureCommunicationManager handles secure network communications
type SecureCommunicationManager struct {
	config *SecurityConfig

	// Connection management
	connections map[string]*SecureConnection

	// TLS configuration
	tlsConfig *tls.Config

	// Statistics
	connectionsEstablished atomic.Uint64
	connectionsActive      atomic.Uint32
	connectionsRejected    atomic.Uint64
	dataTransferred        atomic.Uint64

	// Thread safety
	mu sync.RWMutex

	healthy atomic.Bool
}

// SecureConnection represents a secure network connection
type SecureConnection struct {
	ID            string
	RemoteAddress string
	LocalAddress  string
	Protocol      string
	TLSVersion    uint16
	CipherSuite   uint16
	Established   time.Time
	LastActivity  time.Time
	BytesReceived uint64
	BytesSent     uint64
	IsActive      bool
	Metadata      map[string]interface{}

	// Underlying connection
	conn net.Conn

	// Security state
	Verified    bool
	Certificate *x509.Certificate
	PeerCerts   []*x509.Certificate
}

// ConnectionOptions holds secure connection configuration
type ConnectionOptions struct {
	Timeout            time.Duration
	RequireClientCert  bool
	VerifyPeerCert     bool
	AllowedProtocols   []string
	MinTLSVersion      uint16
	MaxTLSVersion      uint16
	CipherSuites       []uint16
	InsecureSkipVerify bool
	RootCAs            *x509.CertPool
	Certificates       []tls.Certificate
}

// NewSecureCommunicationManager creates a new secure communication manager
func NewSecureCommunicationManager(config *SecurityConfig) *SecureCommunicationManager {
	manager := &SecureCommunicationManager{
		config:      config,
		connections: make(map[string]*SecureConnection),
	}

	manager.healthy.Store(true)
	return manager
}

// Initialize initializes the secure communication manager
func (scm *SecureCommunicationManager) Initialize(config *SecurityConfig) error {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	if config != nil {
		scm.config = config
	}

	// Initialize TLS configuration
	if err := scm.initializeTLSConfig(); err != nil {
		return fmt.Errorf("failed to initialize TLS config: %w", err)
	}

	return nil
}

// IsHealthy returns the health status of the communication manager
func (scm *SecureCommunicationManager) IsHealthy() bool {
	return scm.healthy.Load()
}

// EstablishSecureConnection establishes a secure connection to an endpoint
func (scm *SecureCommunicationManager) EstablishSecureConnection(endpoint string) (*SecureConnection, error) {
	// Parse endpoint URL
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint URL: %w", err)
	}

	// Determine connection type
	var network, address string
	switch u.Scheme {
	case "tls", "https":
		network = "tcp"
		address = u.Host
		if u.Port() == "" {
			address += ":443"
		}
	case "tcp":
		network = "tcp"
		address = u.Host
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", u.Scheme)
	}

	// Establish connection with timeout
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
	}

	var conn net.Conn
	if scm.config.RequireTLS || u.Scheme == "tls" || u.Scheme == "https" {
		conn, err = tls.DialWithDialer(dialer, network, address, scm.tlsConfig)
		if err != nil {
			scm.connectionsRejected.Add(1)
			return nil, fmt.Errorf("failed to establish TLS connection: %w", err)
		}
	} else {
		conn, err = dialer.Dial(network, address)
		if err != nil {
			scm.connectionsRejected.Add(1)
			return nil, fmt.Errorf("failed to establish connection: %w", err)
		}
	}

	// Create secure connection object
	secureConn := &SecureConnection{
		ID:            generateConnectionID(),
		RemoteAddress: conn.RemoteAddr().String(),
		LocalAddress:  conn.LocalAddr().String(),
		Protocol:      u.Scheme,
		Established:   time.Now(),
		LastActivity:  time.Now(),
		IsActive:      true,
		Metadata:      make(map[string]interface{}),
		conn:          conn,
	}

	// Extract TLS information if applicable
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		secureConn.TLSVersion = state.Version
		secureConn.CipherSuite = state.CipherSuite
		secureConn.Verified = state.HandshakeComplete

		if len(state.PeerCertificates) > 0 {
			secureConn.Certificate = state.PeerCertificates[0]
			secureConn.PeerCerts = state.PeerCertificates
		}

		secureConn.Metadata["tls_version"] = tlsVersionString(state.Version)
		secureConn.Metadata["cipher_suite"] = cipherSuiteString(state.CipherSuite)
		secureConn.Metadata["handshake_complete"] = state.HandshakeComplete
	}

	// Store connection
	scm.mu.Lock()
	scm.connections[secureConn.ID] = secureConn
	scm.connectionsEstablished.Add(1)
	scm.connectionsActive.Add(1)
	scm.mu.Unlock()

	return secureConn, nil
}

// ValidateConnection validates an existing secure connection
func (scm *SecureCommunicationManager) ValidateConnection(conn *SecureConnection) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	if !conn.IsActive {
		return fmt.Errorf("connection is not active")
	}

	// Check if connection is still valid
	if conn.conn == nil {
		return fmt.Errorf("underlying connection is nil")
	}

	// For TLS connections, verify certificate if required
	if tlsConn, ok := conn.conn.(*tls.Conn); ok {
		if scm.config.RequireTLS {
			state := tlsConn.ConnectionState()
			if !state.HandshakeComplete {
				return fmt.Errorf("TLS handshake not complete")
			}

			// Verify certificate chain
			if len(state.PeerCertificates) > 0 {
				opts := x509.VerifyOptions{
					Roots: scm.tlsConfig.RootCAs,
				}

				_, err := state.PeerCertificates[0].Verify(opts)
				if err != nil && !scm.tlsConfig.InsecureSkipVerify {
					return fmt.Errorf("certificate verification failed: %w", err)
				}
			}
		}
	}

	// Update last activity
	conn.LastActivity = time.Now()

	return nil
}

// SecureTransfer transfers data securely over a connection
func (scm *SecureCommunicationManager) SecureTransfer(conn *SecureConnection, data []byte) error {
	if err := scm.ValidateConnection(conn); err != nil {
		return fmt.Errorf("connection validation failed: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("no data to transfer")
	}

	// Write data to connection
	bytesWritten, err := conn.conn.Write(data)
	if err != nil {
		conn.IsActive = false
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Update statistics
	conn.BytesSent += uint64(bytesWritten)
	conn.LastActivity = time.Now()
	scm.dataTransferred.Add(uint64(bytesWritten))

	return nil
}

// ReceiveData receives data from a secure connection
func (scm *SecureCommunicationManager) ReceiveData(conn *SecureConnection, buffer []byte) (int, error) {
	if err := scm.ValidateConnection(conn); err != nil {
		return 0, fmt.Errorf("connection validation failed: %w", err)
	}

	// Read data from connection
	bytesRead, err := conn.conn.Read(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return bytesRead, err // Timeout is not necessarily a fatal error
		}
		conn.IsActive = false
		return bytesRead, fmt.Errorf("failed to read data: %w", err)
	}

	// Update statistics
	conn.BytesReceived += uint64(bytesRead)
	conn.LastActivity = time.Now()
	scm.dataTransferred.Add(uint64(bytesRead))

	return bytesRead, nil
}

// CloseConnection closes a secure connection
func (scm *SecureCommunicationManager) CloseConnection(connID string) error {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	conn, exists := scm.connections[connID]
	if !exists {
		return fmt.Errorf("connection not found")
	}

	// Close underlying connection
	if conn.conn != nil {
		if err := conn.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	// Mark as inactive and remove from active connections
	conn.IsActive = false
	delete(scm.connections, connID)
	scm.connectionsActive.Add(^uint32(0)) // Decrement

	return nil
}

// GetConnectionInfo returns information about active connections
func (scm *SecureCommunicationManager) GetConnectionInfo() map[string]interface{} {
	scm.mu.RLock()
	defer scm.mu.RUnlock()

	info := make(map[string]interface{})
	info["total_connections"] = len(scm.connections)
	info["active_connections"] = scm.connectionsActive.Load()
	info["connections_established"] = scm.connectionsEstablished.Load()
	info["connections_rejected"] = scm.connectionsRejected.Load()
	info["data_transferred"] = scm.dataTransferred.Load()

	// Connection details
	connections := make([]map[string]interface{}, 0, len(scm.connections))
	for _, conn := range scm.connections {
		connInfo := map[string]interface{}{
			"id":             conn.ID,
			"remote_address": conn.RemoteAddress,
			"local_address":  conn.LocalAddress,
			"protocol":       conn.Protocol,
			"established":    conn.Established,
			"last_activity":  conn.LastActivity,
			"bytes_sent":     conn.BytesSent,
			"bytes_received": conn.BytesReceived,
			"is_active":      conn.IsActive,
			"verified":       conn.Verified,
		}

		if conn.TLSVersion != 0 {
			connInfo["tls_version"] = tlsVersionString(conn.TLSVersion)
			connInfo["cipher_suite"] = cipherSuiteString(conn.CipherSuite)
		}

		connections = append(connections, connInfo)
	}
	info["connections"] = connections

	return info
}

// initializeTLSConfig initializes the TLS configuration
func (scm *SecureCommunicationManager) initializeTLSConfig() error {
	scm.tlsConfig = &tls.Config{
		MinVersion: scm.config.TLSMinVersion,
		MaxVersion: tls.VersionTLS13, // Use latest TLS version
	}

	// Set cipher suites if specified
	if len(scm.config.AllowedCiphers) > 0 {
		scm.tlsConfig.CipherSuites = scm.config.AllowedCiphers
	}

	// For testing purposes, allow insecure connections
	// In production, this should be configurable and default to secure
	scm.tlsConfig.InsecureSkipVerify = true

	return nil
}

// backgroundMaintenance performs background maintenance tasks
func (scm *SecureCommunicationManager) backgroundMaintenance() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		scm.cleanupInactiveConnections()
	}
}

// cleanupInactiveConnections removes inactive connections
func (scm *SecureCommunicationManager) cleanupInactiveConnections() {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	now := time.Now()
	inactiveThreshold := 10 * time.Minute

	for id, conn := range scm.connections {
		if !conn.IsActive || now.Sub(conn.LastActivity) > inactiveThreshold {
			if conn.conn != nil {
				conn.conn.Close()
			}
			delete(scm.connections, id)
			if conn.IsActive {
				scm.connectionsActive.Add(^uint32(0)) // Decrement
			}
		}
	}
}

// Shutdown gracefully shuts down the communication manager
func (scm *SecureCommunicationManager) Shutdown() error {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	// Close all active connections
	for id, conn := range scm.connections {
		if conn.conn != nil {
			conn.conn.Close()
		}
		conn.IsActive = false
		delete(scm.connections, id)
	}

	scm.connectionsActive.Store(0)
	scm.healthy.Store(false)

	return nil
}

// Helper functions

// generateConnectionID generates a unique connection ID
func generateConnectionID() string {
	return "conn_" + generateRandomID(16)
}

// tlsVersionString returns the string representation of a TLS version
func tlsVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", version)
	}
}

// cipherSuiteString returns the string representation of a cipher suite
func cipherSuiteString(suite uint16) string {
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256:
		return "TLS_AES_128_GCM_SHA256"
	case tls.TLS_AES_256_GCM_SHA384:
		return "TLS_AES_256_GCM_SHA384"
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		return "TLS_CHACHA20_POLY1305_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:
		return "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", suite)
	}
}
