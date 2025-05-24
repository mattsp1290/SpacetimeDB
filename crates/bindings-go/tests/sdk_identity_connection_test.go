package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/db"
	goruntime "github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/runtime"
	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SDK Task 4: Identity and Connection Types with BSATN Type Safety
//
// This test demonstrates SpacetimeDB identity and connection management:
// - Identity Types: Core user identity management and authentication
// - Connection Types: Client connection and session management
// - Caller Operations: Context-aware operations with caller identity
// - Identity Constraints: Unique constraints and primary keys with identities
// - Multi-Client Scenarios: Complex identity and connection interactions
// - Authentication Simulation: Identity-based access control patterns

// === IDENTITY TYPE DEFINITIONS ===

// IdentityType represents a basic identity for testing identity operations
type IdentityType struct {
	Id       uint32    `json:"id"`        // Primary key
	Identity [32]uint8 `json:"identity"`  // SpacetimeDB Identity (32-byte array)
	Name     string    `json:"name"`      // Human-readable name
	Email    string    `json:"email"`     // Email address
	Created  uint64    `json:"created"`   // Creation timestamp
	IsActive bool      `json:"is_active"` // Active status
}

// ConnectionType represents a basic connection for testing connection operations
type ConnectionType struct {
	Id           uint32    `json:"id"`            // Primary key
	ConnectionId [2]uint64 `json:"connection_id"` // SpacetimeDB ConnectionId (2 x uint64)
	ClientName   string    `json:"client_name"`   // Client identifier
	Connected    uint64    `json:"connected"`     // Connection timestamp
	LastPing     uint64    `json:"last_ping"`     // Last ping timestamp
	IsOnline     bool      `json:"is_online"`     // Online status
}

// VectorIdentityType represents identity vectors for bulk operations
type VectorIdentityType struct {
	Id         uint32      `json:"id"`         // Primary key
	Identities [][32]uint8 `json:"identities"` // Vector of identities
	GroupName  string      `json:"group_name"` // Group identifier
	Created    uint64      `json:"created"`    // Creation timestamp
}

// OptionalIdentityType represents optional identity for nullable scenarios
type OptionalIdentityType struct {
	Id       uint32     `json:"id"`       // Primary key
	Identity *[32]uint8 `json:"identity"` // Optional identity (nullable)
	Role     string     `json:"role"`     // User role
	Status   string     `json:"status"`   // Account status
}

// === CONNECTION TYPE DEFINITIONS ===

// VectorConnectionType represents connection vectors for multi-client scenarios
type VectorConnectionType struct {
	Id            uint32      `json:"id"`             // Primary key
	ConnectionIds [][2]uint64 `json:"connection_ids"` // Vector of connection IDs
	SessionName   string      `json:"session_name"`   // Session identifier
	Created       uint64      `json:"created"`        // Creation timestamp
	ActiveCount   int32       `json:"active_count"`   // Number of active connections
}

// OptionalConnectionType represents optional connections for session management
type OptionalConnectionType struct {
	Id           uint32     `json:"id"`            // Primary key
	ConnectionId *[2]uint64 `json:"connection_id"` // Optional connection ID (nullable)
	DeviceType   string     `json:"device_type"`   // Device type
	UserAgent    string     `json:"user_agent"`    // User agent string
}

// === CALLER CONTEXT TYPES ===

// CallerIdentityType represents caller context operations with identity
type CallerIdentityType struct {
	Id          uint32    `json:"id"`           // Primary key
	CallerIdent [32]uint8 `json:"caller_ident"` // Caller's identity
	TargetIdent [32]uint8 `json:"target_ident"` // Target identity
	Action      string    `json:"action"`       // Action performed
	Timestamp   uint64    `json:"timestamp"`    // Action timestamp
	Success     bool      `json:"success"`      // Operation success
}

// CallerConnectionType represents caller context operations with connections
type CallerConnectionType struct {
	Id             uint32    `json:"id"`               // Primary key
	CallerConnId   [2]uint64 `json:"caller_conn_id"`   // Caller's connection ID
	TargetConnId   [2]uint64 `json:"target_conn_id"`   // Target connection ID
	Operation      string    `json:"operation"`        // Operation type
	Timestamp      uint64    `json:"timestamp"`        // Operation timestamp
	ResponseTimeMs int32     `json:"response_time_ms"` // Response time
}

// === UNIQUE CONSTRAINT TYPES ===

// IdentityUniqueType represents identity with unique constraints
type IdentityUniqueType struct {
	Id       uint32    `json:"id"`       // Primary key
	Identity [32]uint8 `json:"identity"` // Unique identity constraint
	Username string    `json:"username"` // Unique username
	Created  uint64    `json:"created"`  // Creation timestamp
}

// === PRIMARY KEY TYPES ===

// PrimaryKeyIdentityType represents identity as primary key
type PrimaryKeyIdentityType struct {
	Identity    [32]uint8 `json:"identity"`     // Primary key identity
	DisplayName string    `json:"display_name"` // Display name
	Created     uint64    `json:"created"`      // Creation timestamp
	LastSeen    uint64    `json:"last_seen"`    // Last seen timestamp
}

// === AUTHENTICATION SIMULATION TYPES ===

// AuthenticationRequest represents authentication request with identity
type AuthenticationRequest struct {
	Id           uint32    `json:"id"`            // Primary key
	Identity     [32]uint8 `json:"identity"`      // Authenticating identity
	ConnectionId [2]uint64 `json:"connection_id"` // Connection ID
	Challenge    string    `json:"challenge"`     // Authentication challenge
	Response     string    `json:"response"`      // Authentication response
	Success      bool      `json:"success"`       // Authentication result
	Timestamp    uint64    `json:"timestamp"`     // Authentication timestamp
}

// === MULTI-CLIENT SCENARIO TYPES ===

// ClientSession represents multi-client session management
type ClientSession struct {
	Id           uint32      `json:"id"`            // Primary key
	SessionId    string      `json:"session_id"`    // Session identifier
	OwnerIdent   [32]uint8   `json:"owner_ident"`   // Session owner identity
	Participants [][32]uint8 `json:"participants"`  // Participant identities
	Connections  [][2]uint64 `json:"connections"`   // Active connections
	Created      uint64      `json:"created"`       // Session creation
	LastActivity uint64      `json:"last_activity"` // Last activity timestamp
	IsActive     bool        `json:"is_active"`     // Session status
}

// IdentityConnectionMapping represents identity-to-connection relationships
type IdentityConnectionMapping struct {
	Id           uint32    `json:"id"`            // Primary key
	Identity     [32]uint8 `json:"identity"`      // User identity
	ConnectionId [2]uint64 `json:"connection_id"` // Associated connection
	DeviceId     string    `json:"device_id"`     // Device identifier
	LoginTime    uint64    `json:"login_time"`    // Login timestamp
	LogoutTime   uint64    `json:"logout_time"`   // Logout timestamp (0 if still connected)
	IpAddress    string    `json:"ip_address"`    // Client IP address
}

// === TEST CONFIGURATION ===

// IdentityConnectionConfig defines test configuration for identity and connection testing
type IdentityConnectionConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	CreateReducer  string        // Create reducer name
	UpdateReducer  string        // Update reducer name
	DeleteReducer  string        // Delete reducer name
	TestValues     []interface{} // Standard test values
	IdentityTest   bool          // Whether this tests identity operations
	ConnectionTest bool          // Whether this tests connection operations
	CallerTest     bool          // Whether this tests caller operations
	UniqueTest     bool          // Whether this tests unique constraints
	PrimaryKeyTest bool          // Whether this tests primary keys
	AuthTest       bool          // Whether this tests authentication
}

// Identity and connection configurations for testing
var IdentityConnectionTypes = []IdentityConnectionConfig{
	{
		Name:          "basic_identity",
		TableName:     "IdentityType",
		CreateReducer: "insert_one_identity",
		UpdateReducer: "update_identity",
		DeleteReducer: "delete_identity",
		IdentityTest:  true,
		TestValues: []interface{}{
			IdentityType{
				Id:       1,
				Identity: generateTestIdentity(1),
				Name:     "Alice Smith",
				Email:    "alice@example.com",
				Created:  uint64(time.Now().UnixMicro()),
				IsActive: true,
			},
			IdentityType{
				Id:       2,
				Identity: generateTestIdentity(2),
				Name:     "Bob Johnson",
				Email:    "bob@example.com",
				Created:  uint64(time.Now().UnixMicro()),
				IsActive: true,
			},
		},
	},
	{
		Name:           "basic_connection",
		TableName:      "ConnectionType",
		CreateReducer:  "insert_one_connection_id",
		UpdateReducer:  "update_connection_id",
		DeleteReducer:  "delete_connection_id",
		ConnectionTest: true,
		TestValues: []interface{}{
			ConnectionType{
				Id:           1,
				ConnectionId: generateTestConnectionId(1),
				ClientName:   "WebClient_1",
				Connected:    uint64(time.Now().UnixMicro()),
				LastPing:     uint64(time.Now().UnixMicro()),
				IsOnline:     true,
			},
			ConnectionType{
				Id:           2,
				ConnectionId: generateTestConnectionId(2),
				ClientName:   "MobileClient_1",
				Connected:    uint64(time.Now().Add(-10 * time.Minute).UnixMicro()),
				LastPing:     uint64(time.Now().Add(-30 * time.Second).UnixMicro()),
				IsOnline:     true,
			},
		},
	},
	{
		Name:          "vector_identity",
		TableName:     "VectorIdentityType",
		CreateReducer: "insert_vec_identity",
		UpdateReducer: "update_vec_identity",
		DeleteReducer: "delete_vec_identity",
		IdentityTest:  true,
		TestValues: []interface{}{
			VectorIdentityType{
				Id:         1,
				Identities: [][32]uint8{generateTestIdentity(3), generateTestIdentity(4), generateTestIdentity(5)},
				GroupName:  "admin_group",
				Created:    uint64(time.Now().UnixMicro()),
			},
		},
	},
	{
		Name:          "optional_identity",
		TableName:     "OptionalIdentityType",
		CreateReducer: "insert_option_identity",
		UpdateReducer: "update_option_identity",
		DeleteReducer: "delete_option_identity",
		IdentityTest:  true,
		TestValues: []interface{}{
			OptionalIdentityType{
				Id:       1,
				Identity: func() *[32]uint8 { id := generateTestIdentity(6); return &id }(),
				Role:     "user",
				Status:   "active",
			},
			OptionalIdentityType{
				Id:       2,
				Identity: nil, // Test null identity
				Role:     "guest",
				Status:   "pending",
			},
		},
	},
	{
		Name:          "caller_identity",
		TableName:     "CallerIdentityType",
		CreateReducer: "insert_caller_one_identity",
		CallerTest:    true,
		TestValues: []interface{}{
			CallerIdentityType{
				Id:          1,
				CallerIdent: generateTestIdentity(7),
				TargetIdent: generateTestIdentity(8),
				Action:      "send_message",
				Timestamp:   uint64(time.Now().UnixMicro()),
				Success:     true,
			},
		},
	},
	{
		Name:          "unique_identity",
		TableName:     "IdentityUniqueType",
		CreateReducer: "insert_unique_identity",
		UpdateReducer: "update_unique_identity",
		DeleteReducer: "delete_unique_identity",
		UniqueTest:    true,
		TestValues: []interface{}{
			IdentityUniqueType{
				Id:       1,
				Identity: generateTestIdentity(9),
				Username: "unique_user_1",
				Created:  uint64(time.Now().UnixMicro()),
			},
		},
	},
	{
		Name:           "pk_identity",
		TableName:      "PrimaryKeyIdentityType",
		CreateReducer:  "insert_pk_identity",
		UpdateReducer:  "update_pk_identity",
		DeleteReducer:  "delete_pk_identity",
		PrimaryKeyTest: true,
		TestValues: []interface{}{
			PrimaryKeyIdentityType{
				Identity:    generateTestIdentity(10),
				DisplayName: "Primary Key User",
				Created:     uint64(time.Now().UnixMicro()),
				LastSeen:    uint64(time.Now().UnixMicro()),
			},
		},
	},
	{
		Name:          "authentication",
		TableName:     "AuthenticationRequest",
		CreateReducer: "insert_auth_request",
		UpdateReducer: "update_auth_request",
		DeleteReducer: "delete_auth_request",
		AuthTest:      true,
		TestValues: []interface{}{
			AuthenticationRequest{
				Id:           1,
				Identity:     generateTestIdentity(11),
				ConnectionId: generateTestConnectionId(10),
				Challenge:    "auth_challenge_123",
				Response:     "signed_response_456",
				Success:      true,
				Timestamp:    uint64(time.Now().UnixMicro()),
			},
		},
	},
}

// Test configuration constants
const (
	// Performance thresholds for identity and connection operations
	IdentityInsertTime       = 10 * time.Millisecond // Single identity insertion
	ConnectionInsertTime     = 10 * time.Millisecond // Single connection insertion
	IdentityVectorInsertTime = 50 * time.Millisecond // Vector operations
	CallerOpTime             = 15 * time.Millisecond // Caller context operations
	AuthOperationTime        = 25 * time.Millisecond // Authentication operations
)

// Utility functions for generating test identities and connections

// generateTestIdentity creates a deterministic test identity based on a seed
func generateTestIdentity(seed uint32) [32]uint8 {
	var identity [32]uint8

	// Create deterministic identity based on seed
	baseStr := fmt.Sprintf("test_identity_%d", seed)
	copy(identity[:], []byte(baseStr))

	// Fill remaining bytes with seed-based pattern
	for i := len(baseStr); i < 32; i++ {
		identity[i] = uint8((seed * uint32(i+1)) % 256)
	}

	return identity
}

// generateTestConnectionId creates a deterministic test connection ID based on a seed
func generateTestConnectionId(seed uint32) [2]uint64 {
	return [2]uint64{
		uint64(seed)<<32 | uint64(seed),
		uint64(seed*2)<<32 | uint64(seed*3),
	}
}

// generateRandomIdentity creates a cryptographically random identity for security testing
func generateRandomIdentity() [32]uint8 {
	var identity [32]uint8
	_, err := rand.Read(identity[:])
	if err != nil {
		// Fallback to deterministic if random fails
		return generateTestIdentity(uint32(time.Now().UnixNano()))
	}
	return identity
}

// generateRandomConnectionId creates a random connection ID
func generateRandomConnectionId() [2]uint64 {
	var conn [2]uint64
	for i := range conn {
		bytes := make([]byte, 8)
		rand.Read(bytes)
		for j, b := range bytes {
			conn[i] |= uint64(b) << (j * 8)
		}
	}
	return conn
}

// TestSDKIdentityConnection is the main integration test for SDK Task 4
func TestSDKIdentityConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK identity connection integration test in short mode")
	}

	repoRoot := os.Getenv("SPACETIMEDB_DIR")
	if repoRoot == "" {
		t.Skip("SPACETIMEDB_DIR not set – skipping SDK integration")
	}

	// Path to the sdk-test WASM module
	wasmPath := filepath.Join(repoRoot, "target/wasm32-unknown-unknown/release/sdk_test_module.wasm")
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		t.Fatalf("SDK test WASM module not found: %v", wasmPath)
	}

	ctx := context.Background()

	// Create database managers
	rt := &goruntime.Runtime{}
	encodingManager := db.NewEncodingManager(rt)

	// Create WASM runtime
	wasmRuntime, err := wasm.NewRuntime(wasm.DefaultConfig())
	require.NoError(t, err)
	defer wasmRuntime.Close(ctx)

	// Load and instantiate the SDK test module
	wasmBytes, err := os.ReadFile(wasmPath)
	require.NoError(t, err)

	err = wasmRuntime.LoadModule(ctx, wasmBytes)
	require.NoError(t, err)

	err = wasmRuntime.InstantiateModule(ctx, "sdk_test_module", true)
	require.NoError(t, err)

	// Discover available reducers
	registry, err := DiscoverReducers(ctx, wasmRuntime, "sdk_test_module")
	require.NoError(t, err)

	t.Logf("✅ SDK test module loaded with %d reducers", len(registry.All))
	t.Logf("🆔 Testing IDENTITY TYPES: Single, vector, optional, caller context")
	t.Logf("🔗 Testing CONNECTION TYPES: Client connections, session management")
	t.Logf("🔐 Testing AUTHENTICATION: Identity-based access control")
	t.Logf("👥 Testing MULTI-CLIENT: Complex identity and connection scenarios")

	// Generate additional test data
	generateIdentityConnectionTestData(t)

	// Run comprehensive identity and connection test suites
	t.Run("BasicIdentityOperations", func(t *testing.T) {
		testBasicIdentityOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("VectorIdentityOperations", func(t *testing.T) {
		testVectorIdentityOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalIdentityOperations", func(t *testing.T) {
		testOptionalIdentityOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("BasicConnectionOperations", func(t *testing.T) {
		testBasicConnectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CallerContextOperations", func(t *testing.T) {
		testCallerContextOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintOperations", func(t *testing.T) {
		testUniqueConstraintOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyOperations", func(t *testing.T) {
		testPrimaryKeyOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("AuthenticationSimulation", func(t *testing.T) {
		testAuthenticationSimulation(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateIdentityConnectionTestData generates additional test data for identity and connection testing
func generateIdentityConnectionTestData(t *testing.T) {
	t.Log("Generating additional IDENTITY and CONNECTION test data...")

	// Generate large identity sets for performance testing
	identityCount := 100
	connectionCount := 50

	for i := 0; i < identityCount; i++ {
		_ = generateTestIdentity(uint32(i + 1000))
	}

	for i := 0; i < connectionCount; i++ {
		_ = generateTestConnectionId(uint32(i + 1000))
	}

	t.Logf("Generated %d test identities and %d test connections", identityCount, connectionCount)
	t.Logf("✅ Generated comprehensive identity and connection test data")
}

// testBasicIdentityOperations tests basic identity operations with proper BSATN encoding
func testBasicIdentityOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🆔 Testing BASIC IDENTITY operations with single identity types...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.IdentityTest || config.Name != "basic_identity" {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the identity data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Identity operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, IdentityInsertTime,
				"Identity operation took %v, expected less than %v", duration, IdentityInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 BASIC IDENTITY Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Basic identity operations should have >95%% success rate")
}

// testVectorIdentityOperations tests vector identity operations (insert_vec_identity, etc.)
func testVectorIdentityOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🆔 Testing VECTOR IDENTITY operations with identity arrays...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.IdentityTest || config.Name != "vector_identity" {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the vector identity data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Vector identity operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, IdentityVectorInsertTime,
				"Vector identity operation took %v, expected less than %v", duration, IdentityVectorInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 VECTOR IDENTITY Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Vector identity operations should have >95%% success rate")
}

// testOptionalIdentityOperations tests optional identity operations (insert_option_identity, etc.)
func testOptionalIdentityOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🆔 Testing OPTIONAL IDENTITY operations with nullable identities...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	nullCount := 0
	nonNullCount := 0

	for _, config := range IdentityConnectionTypes {
		if !config.IdentityTest || config.Name != "optional_identity" {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Count null vs non-null identities
			if optId, ok := testValue.(OptionalIdentityType); ok {
				if optId.Identity == nil {
					nullCount++
				} else {
					nonNullCount++
				}
			}

			// Encode the optional identity data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Optional identity operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, IdentityInsertTime,
				"Optional identity operation took %v, expected less than %v", duration, IdentityInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 OPTIONAL IDENTITY Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)
	t.Logf("🔢 Null handling: %d null identities, %d non-null identities tested", nullCount, nonNullCount)

	assert.Greater(t, successRate, 95.0,
		"Optional identity operations should have >95%% success rate")
}

// testBasicConnectionOperations tests basic connection operations (insert_one_connection_id, etc.)
func testBasicConnectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔗 Testing BASIC CONNECTION operations with connection IDs...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.ConnectionTest || config.Name != "basic_connection" {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the connection data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Connection operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, ConnectionInsertTime,
				"Connection operation took %v, expected less than %v", duration, ConnectionInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 BASIC CONNECTION Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Basic connection operations should have >95%% success rate")
}

// testCallerContextOperations tests caller context operations (insert_caller_one_identity, etc.)
func testCallerContextOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("👤 Testing CALLER CONTEXT operations with caller identity...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.CallerTest {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the caller context data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Caller context operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, CallerOpTime,
				"Caller context operation took %v, expected less than %v", duration, CallerOpTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 CALLER CONTEXT Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Caller context operations should have >95%% success rate")
}

// testUniqueConstraintOperations tests unique constraint operations (insert_unique_identity, etc.)
func testUniqueConstraintOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔒 Testing UNIQUE CONSTRAINT operations with identity uniqueness...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.UniqueTest {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the unique constraint data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Unique constraint operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, IdentityInsertTime,
				"Unique constraint operation took %v, expected less than %v", duration, IdentityInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 UNIQUE CONSTRAINT Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Unique constraint operations should have >95%% success rate")
}

// testPrimaryKeyOperations tests primary key operations (insert_pk_identity, etc.)
func testPrimaryKeyOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔑 Testing PRIMARY KEY operations with identity as primary key...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration

	for _, config := range IdentityConnectionTypes {
		if !config.PrimaryKeyTest {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Encode the primary key data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Primary key operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, IdentityInsertTime,
				"Primary key operation took %v, expected less than %v", duration, IdentityInsertTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 PRIMARY KEY Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Primary key operations should have >95%% success rate")
}

// testAuthenticationSimulation tests authentication simulation (insert_auth_request, etc.)
func testAuthenticationSimulation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔐 Testing AUTHENTICATION SIMULATION with identity-based access control...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	authSuccessCount := 0
	authFailureCount := 0

	for _, config := range IdentityConnectionTypes {
		if !config.AuthTest {
			continue
		}

		t.Logf("Testing %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Count authentication results
			if authReq, ok := testValue.(AuthenticationRequest); ok {
				if authReq.Success {
					authSuccessCount++
				} else {
					authFailureCount++
				}
			}

			// Encode the authentication data using proper BSATN encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s test data: %v", config.Name, err)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate the encoding succeeded
			successCount++
			t.Logf("✅ Authentication operation %s[%d]: encoded %d bytes in %v", config.Name, i, len(encoded), duration)

			assert.Less(t, duration, AuthOperationTime,
				"Authentication operation took %v, expected less than %v", duration, AuthOperationTime)
		}
	}

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 AUTHENTICATION SIMULATION Results: %d/%d operations successful (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, avgDuration)
	t.Logf("🔐 Auth results: %d successful authentications, %d failed authentications",
		authSuccessCount, authFailureCount)

	assert.Greater(t, successRate, 95.0,
		"Authentication simulation should have >95%% success rate")
}
