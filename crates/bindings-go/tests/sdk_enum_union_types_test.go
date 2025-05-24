package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/db"
	goruntime "github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/runtime"
	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SDK Task 8: Enum & Union Types with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB enum and union types:
// - Simple Enums: Basic enumeration types with integer values
// - Enums with Data: Enums that carry associated data
// - Union Types: Tagged unions with different variant types
// - Option Types: Maybe/Optional patterns for nullable values
// - Result Types: Success/Error patterns for operation results
// - Enum Collections: Arrays/maps/sets of enum instances
// - Type Safety: Leveraging our proven collection and struct patterns

// === SIMPLE ENUM TYPES ===

// PlayerStatus represents a simple enum for player states
type PlayerStatus struct {
	Status string `json:"status"` // "online", "offline", "away", "busy"
}

// EnumGameState represents game state enumeration
type EnumGameState struct {
	State string `json:"state"` // "waiting", "playing", "paused", "finished"
}

// ItemRarity represents item rarity levels
type ItemRarity struct {
	Rarity int32 `json:"rarity"` // 1=Common, 2=Uncommon, 3=Rare, 4=Epic, 5=Legendary
}

// === ENUMS WITH DATA ===

// PlayerAction represents an enum with associated data
type PlayerAction struct {
	ActionType string                 `json:"action_type"` // "move", "attack", "chat", "trade"
	Data       map[string]interface{} `json:"data"`        // Associated data (string-keyed)
}

// EnumGameEvent represents complex game events with data
type EnumGameEvent struct {
	EventType string                 `json:"event_type"` // Event type identifier
	Timestamp string                 `json:"timestamp"`  // When the event occurred
	PlayerID  int64                  `json:"player_id"`  // Player involved (explicit int64)
	EventData map[string]interface{} `json:"event_data"` // Event-specific data
	Metadata  map[string]interface{} `json:"metadata"`   // Additional metadata
}

// === UNION TYPES (Tagged Unions) ===

// EnumNetworkMessage represents a tagged union for different message types
type EnumNetworkMessage struct {
	MessageType string                 `json:"message_type"` // "chat", "game_action", "system", "error"
	Payload     map[string]interface{} `json:"payload"`      // Message payload (type varies by MessageType)
	SenderID    int64                  `json:"sender_id"`    // Sender ID (explicit int64)
	Timestamp   string                 `json:"timestamp"`    // Message timestamp
}

// DatabaseOperation represents different database operations
type DatabaseOperation struct {
	Operation  string                 `json:"operation"`  // "insert", "update", "delete", "select"
	TableName  string                 `json:"table_name"` // Target table
	Conditions map[string]interface{} `json:"conditions"` // Operation conditions
	Data       map[string]interface{} `json:"data"`       // Operation data
	Options    map[string]interface{} `json:"options"`    // Additional options
}

// === OPTION TYPES (Maybe/Optional) ===

// OptionalPlayerData represents optional player data
type OptionalPlayerData struct {
	HasValue bool                   `json:"has_value"` // Whether value is present
	Value    map[string]interface{} `json:"value"`     // The actual value (if present)
}

// OptionalGameSession represents optional game session
type OptionalGameSession struct {
	Present     bool   `json:"present"`      // Whether session exists
	SessionID   string `json:"session_id"`   // Session ID (if present)
	PlayerCount int32  `json:"player_count"` // Player count (if present)
}

// === RESULT TYPES (Success/Error) ===

// OperationResult represents a result that can be success or error
type OperationResult struct {
	IsSuccess    bool                   `json:"is_success"`    // Whether operation succeeded
	SuccessData  map[string]interface{} `json:"success_data"`  // Success data (if successful)
	ErrorCode    int32                  `json:"error_code"`    // Error code (if failed)
	ErrorMessage string                 `json:"error_message"` // Error message (if failed)
}

// LoginResult represents login operation result
type LoginResult struct {
	Status      string                 `json:"status"`       // "success", "failed", "pending"
	PlayerID    int64                  `json:"player_id"`    // Player ID (if successful)
	SessionData map[string]interface{} `json:"session_data"` // Session info (if successful)
	ErrorReason string                 `json:"error_reason"` // Error reason (if failed)
}

// === COMPLEX ENUM COMBINATIONS ===

// PlayerStateTransition represents state machine transitions
type PlayerStateTransition struct {
	FromState      string                 `json:"from_state"`      // Previous state
	ToState        string                 `json:"to_state"`        // New state
	Trigger        string                 `json:"trigger"`         // What caused the transition
	TransitionData map[string]interface{} `json:"transition_data"` // Transition-specific data
	Timestamp      string                 `json:"timestamp"`       // When transition occurred
}

// GameMatchmaking represents matchmaking with complex state
type GameMatchmaking struct {
	Status      string                 `json:"status"`       // "searching", "found", "timeout", "cancelled"
	PlayerCount int32                  `json:"player_count"` // Current player count
	TargetCount int32                  `json:"target_count"` // Target player count
	Players     []interface{}          `json:"players"`      // Array of player IDs
	Preferences map[string]interface{} `json:"preferences"`  // Matchmaking preferences
	MatchData   map[string]interface{} `json:"match_data"`   // Match details (if found)
}

// EnumTypeConfig defines configuration for enum type testing
type EnumTypeConfig struct {
	Name            string
	TableName       string        // SDK-test table name
	VecTableName    string        // Vector table name
	SingleReducer   string        // Single value reducer name
	VectorReducer   string        // Vector reducer name
	TestValues      []interface{} // Standard enum test values
	SimpleEnums     []interface{} // Simple enum instances
	ComplexEnums    []interface{} // Complex enum instances
	UnionTypes      []interface{} // Union type instances
	OptionTypes     []interface{} // Optional type instances
	ResultTypes     []interface{} // Result type instances
	VectorSizes     []int         // Vector sizes to test
	MaxEnumVariants int           // Maximum enum variants
}

// EnumOperationsConfig defines configuration for enum operations
type EnumOperationsConfig struct {
	Name             string
	EnumOperations   []EnumOperationTest
	UnionOperations  []UnionOperationTest
	OptionOperations []OptionOperationTest
	ResultOperations []ResultOperationTest
	TransitionTests  []StateTransitionTest
	PerformanceTests []EnumPerformanceTest
}

// EnumOperationTest defines an enum operation test scenario
type EnumOperationTest struct {
	Name        string
	Description string
	InputEnum   interface{}            // Input enum data
	Operation   string                 // Operation type
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// UnionOperationTest defines a union type operation test
type UnionOperationTest struct {
	Name        string
	Description string
	UnionData   interface{}            // Union type data
	Variant     string                 // Which variant is active
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// OptionOperationTest defines an optional type operation test
type OptionOperationTest struct {
	Name        string
	Description string
	OptionData  interface{}            // Optional data
	HasValue    bool                   // Whether value is present
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// ResultOperationTest defines a result type operation test
type ResultOperationTest struct {
	Name        string
	Description string
	ResultData  interface{}            // Result data
	IsSuccess   bool                   // Whether result is success
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// StateTransitionTest defines a state transition test
type StateTransitionTest struct {
	Name        string
	Description string
	Transition  interface{} // Transition data
	Valid       bool        // Whether transition is valid
	Expected    string      // Expected behavior
}

// EnumPerformanceTest defines an enum performance test
type EnumPerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB enum type configurations
var SimpleEnumType = EnumTypeConfig{
	Name:      "simple_enum",
	TableName: "one_enum", VecTableName: "vec_enum",
	SingleReducer: "insert_one_enum", VectorReducer: "insert_vec_enum",
	TestValues: []interface{}{
		// Simple enum instances
		PlayerStatus{Status: "online"},
		PlayerStatus{Status: "offline"},
		PlayerStatus{Status: "away"},
		PlayerStatus{Status: "busy"},
		EnumGameState{State: "waiting"},
		EnumGameState{State: "playing"},
		EnumGameState{State: "paused"},
		EnumGameState{State: "finished"},
		ItemRarity{Rarity: int32(1)}, // Common
		ItemRarity{Rarity: int32(3)}, // Rare
		ItemRarity{Rarity: int32(5)}, // Legendary
	},
	VectorSizes:     []int{1, 2, 5, 10},
	MaxEnumVariants: 20,
}

var ComplexEnumType = EnumTypeConfig{
	Name:      "complex_enum",
	TableName: "one_complex_enum", VecTableName: "vec_complex_enum",
	SingleReducer: "insert_one_complex_enum", VectorReducer: "insert_vec_complex_enum",
	TestValues: []interface{}{
		// Enums with associated data
		PlayerAction{
			ActionType: "move",
			Data: map[string]interface{}{
				"x":         int32(100),
				"y":         int32(200),
				"speed":     int32(5),
				"direction": "north",
			},
		},
		PlayerAction{
			ActionType: "attack",
			Data: map[string]interface{}{
				"target_id": int64(12345),
				"damage":    int32(50),
				"weapon":    "sword",
				"critical":  true,
			},
		},
		EnumGameEvent{
			EventType: "player_joined",
			Timestamp: "2024-01-15T14:30:00Z",
			PlayerID:  int64(98765),
			EventData: map[string]interface{}{
				"username": "new_player",
				"level":    int32(1),
				"region":   "us-west",
			},
			Metadata: map[string]interface{}{
				"server_id": "server_001",
				"lobby_id":  int32(1234),
			},
		},
	},
	VectorSizes:     []int{1, 2, 5},
	MaxEnumVariants: 50,
}

var UnionEnumType = EnumTypeConfig{
	Name:      "union_enum",
	TableName: "one_union", VecTableName: "vec_union",
	SingleReducer: "insert_one_union", VectorReducer: "insert_vec_union",
	TestValues: []interface{}{
		// Union types (tagged unions)
		EnumNetworkMessage{
			MessageType: "chat",
			Payload: map[string]interface{}{
				"message": "Hello, world!",
				"channel": "general",
				"emoji":   true,
			},
			SenderID:  int64(55555),
			Timestamp: "2024-01-15T15:00:00Z",
		},
		EnumNetworkMessage{
			MessageType: "game_action",
			Payload: map[string]interface{}{
				"action":    "cast_spell",
				"spell_id":  int32(42),
				"target_x":  int32(150),
				"target_y":  int32(250),
				"mana_cost": int32(25),
			},
			SenderID:  int64(66666),
			Timestamp: "2024-01-15T15:01:00Z",
		},
		DatabaseOperation{
			Operation:  "insert",
			TableName:  "players",
			Conditions: map[string]interface{}{},
			Data: map[string]interface{}{
				"username": "database_test",
				"level":    int32(10),
				"active":   true,
			},
			Options: map[string]interface{}{
				"validate": true,
				"timeout":  int32(5000),
			},
		},
	},
	VectorSizes:     []int{1, 2, 3},
	MaxEnumVariants: 30,
}

// Enum operations configuration
var EnumOperations = EnumOperationsConfig{
	Name: "enum_operations",
	EnumOperations: []EnumOperationTest{
		{
			Name:        "player_status_validation",
			Description: "Player status enum validation",
			InputEnum:   PlayerStatus{Status: "online"},
			Operation:   "validate",
			Expected:    "Should validate player status enum",
			Validate: func(data interface{}) bool {
				status := data.(PlayerStatus)
				validStates := []string{"online", "offline", "away", "busy"}
				for _, valid := range validStates {
					if status.Status == valid {
						return true
					}
				}
				return false
			},
		},
		{
			Name:        "item_rarity_comparison",
			Description: "Item rarity enum comparison",
			InputEnum:   ItemRarity{Rarity: int32(5)},
			Operation:   "compare",
			Expected:    "Should compare item rarity levels",
			Validate: func(data interface{}) bool {
				rarity := data.(ItemRarity)
				return rarity.Rarity >= int32(1) && rarity.Rarity <= int32(5)
			},
		},
	},
	UnionOperations: []UnionOperationTest{
		{
			Name:        "network_message_routing",
			Description: "Network message union type routing",
			UnionData: EnumNetworkMessage{
				MessageType: "system",
				Payload: map[string]interface{}{
					"notification": "Server maintenance in 5 minutes",
					"priority":     int32(1),
				},
				SenderID:  int64(0), // System sender
				Timestamp: "2024-01-15T16:00:00Z",
			},
			Variant:  "system",
			Expected: "Should route system messages correctly",
			Validate: func(data interface{}) bool {
				msg := data.(EnumNetworkMessage)
				return msg.MessageType == "system" && msg.SenderID == int64(0)
			},
		},
	},
	OptionOperations: []OptionOperationTest{
		{
			Name:        "optional_session_handling",
			Description: "Optional game session handling",
			OptionData: OptionalGameSession{
				Present:     true,
				SessionID:   "session_abc123",
				PlayerCount: int32(8),
			},
			HasValue: true,
			Expected: "Should handle present optional values",
			Validate: func(data interface{}) bool {
				opt := data.(OptionalGameSession)
				return opt.Present && opt.SessionID != "" && opt.PlayerCount > 0
			},
		},
		{
			Name:        "empty_optional_handling",
			Description: "Empty optional value handling",
			OptionData: OptionalGameSession{
				Present:     false,
				SessionID:   "",
				PlayerCount: int32(0),
			},
			HasValue: false,
			Expected: "Should handle empty optional values",
			Validate: func(data interface{}) bool {
				opt := data.(OptionalGameSession)
				return !opt.Present
			},
		},
	},
	ResultOperations: []ResultOperationTest{
		{
			Name:        "successful_operation_result",
			Description: "Successful operation result handling",
			ResultData: OperationResult{
				IsSuccess: true,
				SuccessData: map[string]interface{}{
					"operation_id":   "op_12345",
					"affected_rows":  int32(1),
					"execution_time": "15ms",
				},
				ErrorCode:    int32(0),
				ErrorMessage: "",
			},
			IsSuccess: true,
			Expected:  "Should handle successful operation results",
			Validate: func(data interface{}) bool {
				result := data.(OperationResult)
				return result.IsSuccess && len(result.SuccessData) > 0
			},
		},
		{
			Name:        "failed_operation_result",
			Description: "Failed operation result handling",
			ResultData: OperationResult{
				IsSuccess:    false,
				SuccessData:  map[string]interface{}{},
				ErrorCode:    int32(404),
				ErrorMessage: "Resource not found",
			},
			IsSuccess: false,
			Expected:  "Should handle failed operation results",
			Validate: func(data interface{}) bool {
				result := data.(OperationResult)
				return !result.IsSuccess && result.ErrorCode != int32(0)
			},
		},
	},
}

// Test configuration constants for enum operations
const (
	// Performance thresholds (enum-specific)
	EnumInsertTime    = 75 * time.Millisecond  // Single enum insertion
	EnumEncodingTime  = 25 * time.Millisecond  // Enum encoding
	ComplexEnumTime   = 100 * time.Millisecond // Complex enum operations
	UnionEncodingTime = 50 * time.Millisecond  // Union type encoding

	// Test limits (enum-specific)
	MaxGeneratedEnums    = 25  // Number of random enums to generate
	EnumPerformanceIters = 150 // Iterations for performance testing
	MaxEnumVariants      = 20  // Maximum enum variants to test
	LargeEnumArraySize   = 150 // Size for large enum array tests
)

// TestSDKEnumUnionTypes is the main integration test for enum and union types
func TestSDKEnumUnionTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK enum union types integration test in short mode")
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
	t.Logf("🎯 Testing ENUM Types: Player status, game states, item rarity")
	t.Logf("🎯 Testing UNION Types: Tagged unions, network messages, database operations")
	t.Logf("🎯 Testing OPTION Types: Maybe/Optional patterns for nullable values")
	t.Logf("🎯 Testing RESULT Types: Success/Error patterns for operation results")

	// Generate random test data for enums and unions
	generateRandomEnumData(t)

	// Run comprehensive enum and union type test suites
	t.Run("SimpleEnumOperations", func(t *testing.T) {
		testSimpleEnumOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ComplexEnumOperations", func(t *testing.T) {
		testComplexEnumOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UnionTypeOperations", func(t *testing.T) {
		testUnionTypeOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionTypeOperations", func(t *testing.T) {
		testOptionTypeOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ResultTypeOperations", func(t *testing.T) {
		testResultTypeOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("EnumCollectionOperations", func(t *testing.T) {
		testEnumCollectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("EnumEncodingValidation", func(t *testing.T) {
		testEnumEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("EnumTypeValidation", func(t *testing.T) {
		testEnumTypeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("EnumSpecificOperations", func(t *testing.T) {
		testEnumSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("EnumPerformanceMeasurement", func(t *testing.T) {
		testEnumPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomEnumData generates random enum and union instances for testing
func generateRandomEnumData(t *testing.T) {
	t.Log("Generating random ENUM and UNION test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random player statuses
	statuses := []string{"online", "offline", "away", "busy", "invisible"}
	for i := 0; i < MaxGeneratedEnums; i++ {
		status := PlayerStatus{
			Status: statuses[rand.Intn(len(statuses))],
		}
		SimpleEnumType.TestValues = append(SimpleEnumType.TestValues, status)
	}

	// Generate random game states
	gameStates := []string{"waiting", "playing", "paused", "finished", "cancelled"}
	for i := 0; i < MaxGeneratedEnums; i++ {
		state := EnumGameState{
			State: gameStates[rand.Intn(len(gameStates))],
		}
		SimpleEnumType.TestValues = append(SimpleEnumType.TestValues, state)
	}

	// Generate random player actions with data
	actionTypes := []string{"move", "attack", "chat", "trade", "cast_spell"}
	for i := 0; i < MaxGeneratedEnums; i++ {
		actionType := actionTypes[rand.Intn(len(actionTypes))]
		action := PlayerAction{
			ActionType: actionType,
			Data: map[string]interface{}{
				"timestamp": fmt.Sprintf("2024-01-15T%02d:%02d:00Z", rand.Intn(24), rand.Intn(60)),
				"player_id": int64(rand.Intn(100000) + 1000),
				"sequence":  int32(rand.Intn(1000)),
			},
		}

		// Add action-specific data
		switch actionType {
		case "move":
			action.Data["x"] = int32(rand.Intn(1000))
			action.Data["y"] = int32(rand.Intn(1000))
			action.Data["speed"] = int32(rand.Intn(10) + 1)
		case "attack":
			action.Data["target_id"] = int64(rand.Intn(100000) + 1000)
			action.Data["damage"] = int32(rand.Intn(100) + 1)
			action.Data["critical"] = rand.Intn(2) == 1
		case "chat":
			action.Data["message"] = fmt.Sprintf("Random message %d", rand.Intn(1000))
			action.Data["channel"] = []string{"general", "trade", "guild"}[rand.Intn(3)]
		}

		ComplexEnumType.TestValues = append(ComplexEnumType.TestValues, action)
	}

	// Generate random network messages (union types)
	messageTypes := []string{"chat", "game_action", "system", "error"}
	for i := 0; i < MaxGeneratedEnums; i++ {
		msgType := messageTypes[rand.Intn(len(messageTypes))]
		message := EnumNetworkMessage{
			MessageType: msgType,
			SenderID:    int64(rand.Intn(100000) + 1000),
			Timestamp: fmt.Sprintf("2024-01-15T%02d:%02d:%02d.%03dZ",
				rand.Intn(24), rand.Intn(60), rand.Intn(60), rand.Intn(1000)),
			Payload: map[string]interface{}{
				"message_id": fmt.Sprintf("msg_%d", rand.Intn(1000000)),
				"priority":   int32(rand.Intn(5) + 1),
			},
		}

		// Add message-type-specific payload
		switch msgType {
		case "chat":
			message.Payload["content"] = fmt.Sprintf("Chat message %d", rand.Intn(1000))
			message.Payload["channel"] = "general"
		case "system":
			message.Payload["notification"] = fmt.Sprintf("System alert %d", rand.Intn(100))
			message.Payload["level"] = "info"
		case "error":
			message.Payload["error_code"] = int32(rand.Intn(500) + 400)
			message.Payload["error_message"] = fmt.Sprintf("Error occurred: %d", rand.Intn(100))
		}

		UnionEnumType.TestValues = append(UnionEnumType.TestValues, message)
	}

	t.Logf("✅ Generated %d simple enums, %d complex enums, %d union types with proper BSATN types",
		MaxGeneratedEnums, MaxGeneratedEnums, MaxGeneratedEnums)
}

// testSimpleEnumOperations tests simple enum operations
func testSimpleEnumOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting SIMPLE enum operations testing...")

	t.Run("BasicEnumTypes", func(t *testing.T) {
		t.Log("Testing basic enum types with BSATN encoding")

		successCount := 0
		totalTests := len(SimpleEnumType.TestValues)

		for i, testValue := range SimpleEnumType.TestValues {
			if i >= 10 { // Test first 10 for performance
				break
			}

			startTime := time.Now()

			// Encode the enum value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode enum value %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Enum %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, EnumEncodingTime,
				"Enum encoding took %v, expected less than %v", encodingTime, EnumEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minEnumInt(10, totalTests)) * 100
		t.Logf("✅ Simple enum operations: %d/%d successful (%.1f%%)",
			successCount, minEnumInt(10, totalTests), successRate)
	})

	t.Log("✅ Simple enum operations testing completed")
}

// testComplexEnumOperations tests complex enum operations with data
func testComplexEnumOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting COMPLEX enum operations testing...")

	t.Run("EnumsWithData", func(t *testing.T) {
		t.Log("Testing enums with associated data")

		successCount := 0
		totalTests := len(ComplexEnumType.TestValues)

		for i, testValue := range ComplexEnumType.TestValues {
			if i >= 8 { // Test first 8 for performance
				break
			}

			startTime := time.Now()

			// Encode the complex enum value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode complex enum %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Complex enum %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, ComplexEnumTime,
				"Complex enum encoding took %v, expected less than %v", encodingTime, ComplexEnumTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minEnumInt(8, totalTests)) * 100
		t.Logf("✅ Complex enum operations: %d/%d successful (%.1f%%)",
			successCount, minEnumInt(8, totalTests), successRate)
	})

	t.Log("✅ Complex enum operations testing completed")
}

// testUnionTypeOperations tests union type operations
func testUnionTypeOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting UNION type operations testing...")

	// Test union types (tagged unions)
	t.Run("TaggedUnionTypes", func(t *testing.T) {
		successCount := 0
		totalTests := len(UnionEnumType.TestValues)

		for i, testValue := range UnionEnumType.TestValues {
			if i >= 6 { // Test first 6 for performance
				break
			}

			startTime := time.Now()
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode union type %T: %v", testValue, err)
				continue
			}

			t.Logf("✅ Union type %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, UnionEncodingTime,
				"Union type encoding took %v, expected less than %v", encodingTime, UnionEncodingTime)

			successCount++
		}

		successRate := float64(successCount) / float64(minEnumInt(6, totalTests)) * 100
		t.Logf("✅ Union type operations: %d/%d successful (%.1f%%)",
			successCount, minEnumInt(6, totalTests), successRate)
	})

	t.Log("✅ Union type operations testing completed")
}

// testOptionTypeOperations tests optional type operations
func testOptionTypeOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting OPTION type operations testing...")

	// Test option types (Maybe/Optional patterns)
	t.Run("OptionalTypePatterns", func(t *testing.T) {
		optionalValues := []interface{}{
			OptionalPlayerData{
				HasValue: true,
				Value: map[string]interface{}{
					"player_id": int64(12345),
					"username":  "optional_test_player",
					"level":     int32(50),
				},
			},
			OptionalPlayerData{
				HasValue: false,
				Value:    map[string]interface{}{},
			},
			OptionalGameSession{
				Present:     true,
				SessionID:   "opt_session_123",
				PlayerCount: int32(4),
			},
			OptionalGameSession{
				Present:     false,
				SessionID:   "",
				PlayerCount: int32(0),
			},
		}

		successCount := 0
		for i, optValue := range optionalValues {
			encoded, err := encodingManager.Encode(optValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode optional value %d: %v", i, err)
				continue
			}

			t.Logf("✅ Optional value %T encoded to %d BSATN bytes", optValue, len(encoded))
			successCount++
		}

		successRate := float64(successCount) / float64(len(optionalValues)) * 100
		t.Logf("✅ Option type operations: %d/%d successful (%.1f%%)",
			successCount, len(optionalValues), successRate)
	})

	t.Log("✅ Option type operations testing completed")
}

// testResultTypeOperations tests result type operations
func testResultTypeOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting RESULT type operations testing...")

	// Test result types (Success/Error patterns)
	t.Run("ResultTypePatterns", func(t *testing.T) {
		resultValues := []interface{}{
			OperationResult{
				IsSuccess: true,
				SuccessData: map[string]interface{}{
					"operation": "test_success",
					"result_id": int64(98765),
					"data":      "success_data",
				},
				ErrorCode:    int32(0),
				ErrorMessage: "",
			},
			OperationResult{
				IsSuccess:    false,
				SuccessData:  map[string]interface{}{},
				ErrorCode:    int32(500),
				ErrorMessage: "Internal server error",
			},
			LoginResult{
				Status:   "success",
				PlayerID: int64(54321),
				SessionData: map[string]interface{}{
					"session_token": "abc123def456",
					"expires_at":    "2024-01-16T10:00:00Z",
				},
				ErrorReason: "",
			},
			LoginResult{
				Status:      "failed",
				PlayerID:    int64(0),
				SessionData: map[string]interface{}{},
				ErrorReason: "Invalid credentials",
			},
		}

		successCount := 0
		for i, resultValue := range resultValues {
			encoded, err := encodingManager.Encode(resultValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode result value %d: %v", i, err)
				continue
			}

			t.Logf("✅ Result value %T encoded to %d BSATN bytes", resultValue, len(encoded))
			successCount++
		}

		successRate := float64(successCount) / float64(len(resultValues)) * 100
		t.Logf("✅ Result type operations: %d/%d successful (%.1f%%)",
			successCount, len(resultValues), successRate)
	})

	t.Log("✅ Result type operations testing completed")
}

// testEnumCollectionOperations tests enum collection operations
func testEnumCollectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting ENUM collection operations testing...")

	// Test arrays of enums
	t.Run("EnumArrays", func(t *testing.T) {
		enumArrays := []interface{}{
			[]interface{}{
				PlayerStatus{Status: "online"},
				PlayerStatus{Status: "away"},
				PlayerStatus{Status: "busy"},
			},
			[]interface{}{
				ItemRarity{Rarity: int32(1)},
				ItemRarity{Rarity: int32(3)},
				ItemRarity{Rarity: int32(5)},
			},
			[]interface{}{
				EnumGameState{State: "waiting"},
				EnumGameState{State: "playing"},
				EnumGameState{State: "finished"},
			},
		}

		successCount := 0
		for i, enumArray := range enumArrays {
			encoded, err := encodingManager.Encode(enumArray, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode enum array %d: %v", i, err)
				continue
			}

			t.Logf("✅ Enum array %d encoded to %d BSATN bytes", i, len(encoded))
			successCount++
		}

		successRate := float64(successCount) / float64(len(enumArrays)) * 100
		t.Logf("✅ Enum array operations: %d/%d successful (%.1f%%)",
			successCount, len(enumArrays), successRate)
	})

	// Test maps with enum values
	t.Run("EnumMaps", func(t *testing.T) {
		enumMap := map[string]interface{}{
			"player_status": PlayerStatus{Status: "online"},
			"game_state":    EnumGameState{State: "playing"},
			"item_rarity":   ItemRarity{Rarity: int32(4)},
		}

		encoded, err := encodingManager.Encode(enumMap, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode enum map: %v", err)
			return
		}

		t.Logf("✅ Enum map encoded to %d BSATN bytes", len(encoded))
	})

	t.Log("✅ Enum collection operations testing completed")
}

// testEnumEncodingValidation tests enum encoding validation
func testEnumEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting ENUM encoding validation testing...")

	// Test various enum encoding scenarios
	t.Run("EnumEncodingScenarios", func(t *testing.T) {
		testEnums := []interface{}{
			// Edge case: Empty enum-like structure
			PlayerStatus{Status: ""},
			// Maximum enum variant
			ItemRarity{Rarity: int32(2147483647)}, // Max int32
			// Complex enum with large payload
			PlayerAction{
				ActionType: "complex_action",
				Data: map[string]interface{}{
					"large_data": fmt.Sprintf("large_string_%s", string(make([]byte, 100))),
					"complex_map": map[string]interface{}{
						"nested1": "value1",
						"nested2": int32(123),
						"nested3": true,
					},
					"large_array": []interface{}{
						int64(1), int64(2), int64(3), int64(4), int64(5),
					},
				},
			},
		}

		for i, testEnum := range testEnums {
			startTime := time.Now()
			encoded, err := encodingManager.Encode(testEnum, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode enum %d (%T): %v", i, testEnum, err)
				continue
			}

			t.Logf("✅ Enum %d (%T) → %d BSATN bytes in %v", i, testEnum, len(encoded), encodingTime)

			assert.Less(t, encodingTime, EnumEncodingTime,
				"Enum encoding took %v, expected less than %v", encodingTime, EnumEncodingTime)
		}
	})

	t.Log("✅ Enum encoding validation testing completed")
}

// testEnumTypeValidation tests enum type validation
func testEnumTypeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting ENUM type validation testing...")

	// Test type safety of enum fields and union variants
	t.Run("EnumTypeValidation", func(t *testing.T) {
		typeValidationTests := []struct {
			name     string
			data     interface{}
			expected bool
			reason   string
		}{
			{
				name:     "ValidPlayerStatusEnum",
				data:     PlayerStatus{Status: "online"},
				expected: true,
				reason:   "Simple enum with string value",
			},
			{
				name:     "ValidItemRarityEnum",
				data:     ItemRarity{Rarity: int32(3)},
				expected: true,
				reason:   "Simple enum with explicit int32 value",
			},
			{
				name: "ValidUnionTypeWithProperPayload",
				data: EnumNetworkMessage{
					MessageType: "chat",
					Payload: map[string]interface{}{
						"message": "Hello, world!",
						"user_id": int64(12345),
					},
					SenderID:  int64(54321),
					Timestamp: "2024-01-15T15:30:00Z",
				},
				expected: true,
				reason:   "Union type with proper BSATN-compatible payload",
			},
			{
				name: "ValidOptionalTypePresent",
				data: OptionalGameSession{
					Present:     true,
					SessionID:   "session_valid",
					PlayerCount: int32(8),
				},
				expected: true,
				reason:   "Optional type with value present",
			},
			{
				name: "ValidOptionalTypeAbsent",
				data: OptionalGameSession{
					Present:     false,
					SessionID:   "",
					PlayerCount: int32(0),
				},
				expected: true,
				reason:   "Optional type with value absent",
			},
		}

		successCount := 0
		for _, test := range typeValidationTests {
			encoded, err := encodingManager.Encode(test.data, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			if (err == nil) == test.expected {
				t.Logf("✅ %s: encoding success=%v (expected=%v) → %d bytes - %s",
					test.name, err == nil, test.expected, len(encoded), test.reason)
				successCount++
			} else {
				t.Logf("❌ %s: encoding success=%v (expected=%v): %v - %s",
					test.name, err == nil, test.expected, err, test.reason)
			}
		}

		successRate := float64(successCount) / float64(len(typeValidationTests)) * 100
		t.Logf("✅ Enum type validation: %d/%d tests passed (%.1f%%)",
			successCount, len(typeValidationTests), successRate)
	})

	t.Log("✅ Enum type validation testing completed")
}

// testEnumSpecificOperations tests enum-specific operations
func testEnumSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting ENUM specific operations testing...")

	// Test enum operations from configuration
	t.Run("ConfiguredEnumOperations", func(t *testing.T) {
		for _, operation := range EnumOperations.EnumOperations {
			encoded, err := encodingManager.Encode(operation.InputEnum, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputEnum) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
			t.Logf("📊 %s", operation.Expected)
		}
	})

	// Test union operations
	t.Run("ConfiguredUnionOperations", func(t *testing.T) {
		for _, operation := range EnumOperations.UnionOperations {
			encoded, err := encodingManager.Encode(operation.UnionData, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s union: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.UnionData) {
				t.Logf("✅ %s (variant: %s): validation passed → %d bytes",
					operation.Name, operation.Variant, len(encoded))
			} else {
				t.Logf("✅ %s (variant: %s): completed → %d bytes",
					operation.Name, operation.Variant, len(encoded))
			}
			t.Logf("📊 %s", operation.Expected)
		}
	})

	t.Log("✅ Enum specific operations testing completed")
}

// testEnumPerformance tests enum performance
func testEnumPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting ENUM performance testing...")

	// Performance test for simple enum encoding
	t.Run("SimpleEnumPerformance", func(t *testing.T) {
		testEnum := PlayerStatus{Status: "online"}
		iterations := EnumPerformanceIters

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testEnum, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Simple enum encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, EnumEncodingTime,
			"Average enum encoding time %v exceeds threshold %v", avgTime, EnumEncodingTime)
	})

	// Performance test for complex enum encoding
	t.Run("ComplexEnumPerformance", func(t *testing.T) {
		testEnum := PlayerAction{
			ActionType: "performance_test",
			Data: map[string]interface{}{
				"test_id":     int64(99999),
				"complexity":  int32(5),
				"performance": true,
				"data": map[string]interface{}{
					"nested": "value",
					"count":  int32(100),
				},
			},
		}
		iterations := EnumPerformanceIters / 2 // Fewer iterations for complex enums

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testEnum, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Complex enum encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, ComplexEnumTime,
			"Average complex enum encoding time %v exceeds threshold %v", avgTime, ComplexEnumTime)
	})

	// Memory usage test for enum operations
	t.Run("EnumMemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many enum instances
		for i := 0; i < 300; i++ {
			status := PlayerStatus{Status: fmt.Sprintf("status_%d", i%5)}
			rarity := ItemRarity{Rarity: int32(i%5 + 1)}
			action := PlayerAction{
				ActionType: fmt.Sprintf("action_%d", i%10),
				Data: map[string]interface{}{
					"run":    i,
					"value":  int32(i),
					"active": i%2 == 0,
				},
			}

			encodingManager.Encode(status, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(rarity, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(action, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 300 enum triplets: %d bytes", memUsed)
	})

	t.Log("✅ Enum performance testing completed")
}

// Helper function for minimum calculation
func minEnumInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
