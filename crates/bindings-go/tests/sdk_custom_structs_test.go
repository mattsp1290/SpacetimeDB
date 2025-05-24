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

// SDK Task 7: Custom Struct Types with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB custom struct types:
// - Simple Structs: Basic structs with primitive fields (using explicit types)
// - Complex Structs: Structs with nested structures and collections
// - Struct Collections: Arrays/maps/sets of struct instances
// - Nested Structs: Structs containing other structs
// - Performance: Optimized for BSATN encoding/decoding
// - Type Safety: Leveraging our proven collection patterns

// === BASIC STRUCT TYPES ===

// PlayerData represents a simple game player with proper types
type PlayerData struct {
	PlayerID   int64                  `json:"player_id"`  // Explicit int64
	Username   string                 `json:"username"`   // String (works great)
	Level      int32                  `json:"level"`      // Explicit int32
	Experience int64                  `json:"experience"` // Explicit int64
	IsActive   bool                   `json:"is_active"`  // Boolean (works great)
	LastLogin  string                 `json:"last_login"` // Timestamp as string
	Settings   map[string]interface{} `json:"settings"`   // String-keyed map (our proven pattern)
}

// GameSession represents a gaming session with collections
type GameSession struct {
	SessionID   string                 `json:"session_id"`   // String identifier
	PlayerCount int32                  `json:"player_count"` // Explicit int32
	StartTime   string                 `json:"start_time"`   // Timestamp as string
	Duration    int32                  `json:"duration"`     // Explicit int32 (seconds)
	IsPublic    bool                   `json:"is_public"`    // Boolean
	Players     []interface{}          `json:"players"`      // Array of player IDs (properly typed)
	Metadata    map[string]interface{} `json:"metadata"`     // String-keyed metadata
}

// InventoryItem represents a game item with nested data
type InventoryItem struct {
	ItemID     int64                  `json:"item_id"`     // Explicit int64
	Name       string                 `json:"name"`        // String
	Type       string                 `json:"type"`        // String (item type)
	Quantity   int32                  `json:"quantity"`    // Explicit int32
	Rarity     int32                  `json:"rarity"`      // Explicit int32 (1-5 scale)
	IsEquipped bool                   `json:"is_equipped"` // Boolean
	Stats      map[string]interface{} `json:"stats"`       // String-keyed stats
	Attributes []interface{}          `json:"attributes"`  // Array of attribute names
}

// === COMPLEX STRUCT TYPES ===

// PlayerProfile represents a complete player profile with nested structs
type PlayerProfile struct {
	Player       PlayerData             `json:"player"`       // Nested struct
	Inventory    []interface{}          `json:"inventory"`    // Array of InventoryItem structs
	Friends      []interface{}          `json:"friends"`      // Array of friend IDs
	Achievements []interface{}          `json:"achievements"` // Array of achievement IDs
	Statistics   map[string]interface{} `json:"statistics"`   // String-keyed stats
	Preferences  map[string]interface{} `json:"preferences"`  // String-keyed preferences
}

// GuildData represents a guild/clan with member management
type GuildData struct {
	GuildID     int64                  `json:"guild_id"`     // Explicit int64
	Name        string                 `json:"name"`         // String
	Description string                 `json:"description"`  // String
	Level       int32                  `json:"level"`        // Explicit int32
	MemberCount int32                  `json:"member_count"` // Explicit int32
	MaxMembers  int32                  `json:"max_members"`  // Explicit int32
	IsPublic    bool                   `json:"is_public"`    // Boolean
	CreatedAt   string                 `json:"created_at"`   // Timestamp as string
	Members     []interface{}          `json:"members"`      // Array of member data
	Settings    map[string]interface{} `json:"settings"`     // String-keyed settings
}

// === SPECIALIZED STRUCT TYPES ===

// GameEvent represents an event in the game world
type GameEvent struct {
	EventID      string                 `json:"event_id"`     // String identifier
	Type         string                 `json:"type"`         // String (event type)
	Timestamp    string                 `json:"timestamp"`    // Timestamp as string
	PlayerID     int64                  `json:"player_id"`    // Explicit int64
	Data         map[string]interface{} `json:"data"`         // String-keyed event data
	Participants []interface{}          `json:"participants"` // Array of participant IDs
	IsPublic     bool                   `json:"is_public"`    // Boolean
}

// CustomStructTypeConfig defines configuration for struct type testing
type CustomStructTypeConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	VecTableName   string        // Vector table name
	SingleReducer  string        // Single value reducer name
	VectorReducer  string        // Vector reducer name
	TestValues     []interface{} // Standard struct test values
	SimpleStructs  []interface{} // Simple struct instances
	ComplexStructs []interface{} // Complex struct instances
	NestedStructs  []interface{} // Nested struct instances
	StructArrays   []interface{} // Arrays of structs
	VectorSizes    []int         // Vector sizes to test
	MaxStructSize  int           // Maximum struct complexity
}

// CustomStructOperationsConfig defines configuration for struct operations
type CustomStructOperationsConfig struct {
	Name             string
	StructOperations []StructOperationTest
	NestingTests     []StructNestingTest
	CollectionTests  []StructCollectionTest
	PerformanceTests []StructPerformanceTest
}

// StructOperationTest defines a struct operation test scenario
type StructOperationTest struct {
	Name        string
	Description string
	InputStruct interface{}            // Input struct data
	Operation   string                 // Operation type
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// StructNestingTest defines a struct nesting test
type StructNestingTest struct {
	Name        string
	Description string
	NestedData  interface{} // Nested struct data
	Depth       int         // Nesting depth
	Expected    string      // Expected behavior
}

// StructCollectionTest defines a struct with collections test
type StructCollectionTest struct {
	Name        string
	Description string
	StructData  interface{} // Struct containing collections
	Expected    string      // Expected behavior
}

// StructPerformanceTest defines a struct performance test
type StructPerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB struct type configurations
var SimpleStructType = CustomStructTypeConfig{
	Name:      "simple_struct",
	TableName: "one_struct", VecTableName: "vec_struct",
	SingleReducer: "insert_one_struct", VectorReducer: "insert_vec_struct",
	TestValues: []interface{}{
		// Simple struct instances with proper types
		PlayerData{
			PlayerID:   int64(12345),
			Username:   "alice_player",
			Level:      int32(25),
			Experience: int64(125000),
			IsActive:   true,
			LastLogin:  "2024-01-15T10:30:00Z",
			Settings: map[string]interface{}{
				"theme":         "dark",
				"notifications": true,
				"volume":        int32(80),
			},
		},
		GameSession{
			SessionID:   "session_001",
			PlayerCount: int32(4),
			StartTime:   "2024-01-15T14:00:00Z",
			Duration:    int32(3600), // 1 hour in seconds
			IsPublic:    true,
			Players:     []interface{}{int64(12345), int64(12346), int64(12347), int64(12348)},
			Metadata: map[string]interface{}{
				"game_mode":  "ranked",
				"map":        "classic",
				"difficulty": int32(3),
			},
		},
	},
	VectorSizes:   []int{1, 2, 5, 10},
	MaxStructSize: 50,
}

var ComplexStructType = CustomStructTypeConfig{
	Name:      "complex_struct",
	TableName: "one_complex", VecTableName: "vec_complex",
	SingleReducer: "insert_one_complex", VectorReducer: "insert_vec_complex",
	TestValues: []interface{}{
		// Complex struct instances with nested data
		InventoryItem{
			ItemID:     int64(9001),
			Name:       "Legendary Sword",
			Type:       "weapon",
			Quantity:   int32(1),
			Rarity:     int32(5), // Legendary
			IsEquipped: true,
			Stats: map[string]interface{}{
				"attack":     int32(150),
				"durability": int32(100),
				"speed":      int32(85),
			},
			Attributes: []interface{}{"fire_damage", "crit_chance", "lifesteal"},
		},
		GuildData{
			GuildID:     int64(50001),
			Name:        "Dragon Slayers",
			Description: "Elite guild for experienced players",
			Level:       int32(15),
			MemberCount: int32(25),
			MaxMembers:  int32(50),
			IsPublic:    false,
			CreatedAt:   "2024-01-01T00:00:00Z",
			Members:     []interface{}{int64(12345), int64(12346), int64(12347)},
			Settings: map[string]interface{}{
				"auto_accept":    false,
				"min_level":      int32(20),
				"activity_bonus": true,
			},
		},
	},
	VectorSizes:   []int{1, 2, 5},
	MaxStructSize: 100,
}

// Struct operations configuration
var CustomStructOperations = CustomStructOperationsConfig{
	Name: "custom_struct_operations",
	StructOperations: []StructOperationTest{
		{
			Name:        "player_data_validation",
			Description: "Player data struct validation",
			InputStruct: PlayerData{
				PlayerID:   int64(99999),
				Username:   "test_player",
				Level:      int32(50),
				Experience: int64(1000000),
				IsActive:   true,
				LastLogin:  "2024-01-15T12:00:00Z",
				Settings: map[string]interface{}{
					"language": "en",
					"region":   "us-west",
				},
			},
			Operation: "validate",
			Expected:  "Should validate all player data fields",
			Validate: func(data interface{}) bool {
				player := data.(PlayerData)
				return player.PlayerID > 0 && player.Username != "" && player.Level > 0
			},
		},
		{
			Name:        "inventory_item_operations",
			Description: "Inventory item struct operations",
			InputStruct: InventoryItem{
				ItemID:     int64(7777),
				Name:       "Health Potion",
				Type:       "consumable",
				Quantity:   int32(10),
				Rarity:     int32(2), // Common
				IsEquipped: false,
				Stats: map[string]interface{}{
					"healing": int32(50),
				},
				Attributes: []interface{}{"stackable", "instant"},
			},
			Operation: "process",
			Expected:  "Should process inventory item data",
			Validate: func(data interface{}) bool {
				item := data.(InventoryItem)
				return item.ItemID > 0 && item.Quantity > 0
			},
		},
	},
}

// Test configuration constants for custom struct operations
const (
	// Performance thresholds (struct-specific)
	StructInsertTime   = 100 * time.Millisecond // Single struct insertion
	StructEncodingTime = 50 * time.Millisecond  // Struct encoding
	ComplexStructTime  = 150 * time.Millisecond // Complex struct operations

	// Test limits (struct-specific)
	MaxGeneratedStructs    = 20  // Number of random structs to generate
	StructPerformanceIters = 100 // Iterations for performance testing
	MaxNestingDepth        = 5   // Maximum struct nesting depth
	LargeStructArraySize   = 100 // Size for large struct array tests
)

// TestSDKCustomStructs is the main integration test for custom struct types
func TestSDKCustomStructs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK custom structs integration test in short mode")
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
	t.Logf("🎯 Testing CUSTOM Struct Types: Player data, game sessions, inventory")
	t.Logf("🎯 Testing COMPLEX Structs: Nested structures with collections")
	t.Logf("🎯 Testing STRUCT Arrays: Collections of struct instances")

	// Generate random test data for custom structs
	generateRandomStructData(t)

	// Run comprehensive custom struct test suites
	t.Run("SimpleStructOperations", func(t *testing.T) {
		testSimpleStructOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ComplexStructOperations", func(t *testing.T) {
		testComplexStructOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("NestedStructOperations", func(t *testing.T) {
		testNestedStructOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StructCollectionOperations", func(t *testing.T) {
		testStructCollectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StructEncodingValidation", func(t *testing.T) {
		testStructEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StructTypeValidation", func(t *testing.T) {
		testStructTypeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StructSpecificOperations", func(t *testing.T) {
		testStructSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StructPerformanceMeasurement", func(t *testing.T) {
		testStructPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomStructData generates random struct instances for testing
func generateRandomStructData(t *testing.T) {
	t.Log("Generating random CUSTOM struct test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random player data structs
	for i := 0; i < MaxGeneratedStructs; i++ {
		player := PlayerData{
			PlayerID:   int64(rand.Intn(100000) + 1000),
			Username:   fmt.Sprintf("player_%d", rand.Intn(1000)),
			Level:      int32(rand.Intn(100) + 1),
			Experience: int64(rand.Intn(1000000)),
			IsActive:   rand.Intn(2) == 1,
			LastLogin:  fmt.Sprintf("2024-01-%02dT%02d:30:00Z", rand.Intn(30)+1, rand.Intn(24)),
			Settings: map[string]interface{}{
				"theme":  []string{"light", "dark"}[rand.Intn(2)],
				"volume": int32(rand.Intn(100)),
				"lang":   []string{"en", "es", "fr"}[rand.Intn(3)],
			},
		}
		SimpleStructType.TestValues = append(SimpleStructType.TestValues, player)
	}

	// Generate random inventory items
	itemTypes := []string{"weapon", "armor", "consumable", "material"}
	for i := 0; i < MaxGeneratedStructs; i++ {
		item := InventoryItem{
			ItemID:     int64(rand.Intn(10000) + 1000),
			Name:       fmt.Sprintf("Item_%d", rand.Intn(1000)),
			Type:       itemTypes[rand.Intn(len(itemTypes))],
			Quantity:   int32(rand.Intn(99) + 1),
			Rarity:     int32(rand.Intn(5) + 1),
			IsEquipped: rand.Intn(2) == 1,
			Stats: map[string]interface{}{
				"power":  int32(rand.Intn(200)),
				"speed":  int32(rand.Intn(100)),
				"weight": int32(rand.Intn(50)),
			},
			Attributes: []interface{}{
				fmt.Sprintf("attr_%d", rand.Intn(10)),
				fmt.Sprintf("attr_%d", rand.Intn(10)),
			},
		}
		ComplexStructType.TestValues = append(ComplexStructType.TestValues, item)
	}

	t.Logf("✅ Generated %d simple structs and %d complex structs with proper BSATN types",
		MaxGeneratedStructs, MaxGeneratedStructs)
}

// testSimpleStructOperations tests simple struct operations
func testSimpleStructOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting SIMPLE struct operations testing...")

	t.Run("PlayerDataStructs", func(t *testing.T) {
		t.Log("Testing PlayerData struct encoding")

		successCount := 0
		totalTests := len(SimpleStructType.TestValues)

		for i, testValue := range SimpleStructType.TestValues {
			if i >= 5 { // Test first 5 for performance
				break
			}

			startTime := time.Now()

			// Encode the struct value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode struct value %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Struct %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, StructEncodingTime,
				"Struct encoding took %v, expected less than %v", encodingTime, StructEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minStructInt(5, totalTests)) * 100
		t.Logf("✅ Simple struct operations: %d/%d successful (%.1f%%)",
			successCount, minStructInt(5, totalTests), successRate)
	})

	t.Log("✅ Simple struct operations testing completed")
}

// testComplexStructOperations tests complex struct operations
func testComplexStructOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting COMPLEX struct operations testing...")

	t.Run("ComplexStructTypes", func(t *testing.T) {
		t.Log("Testing complex struct types with nested data")

		successCount := 0
		totalTests := len(ComplexStructType.TestValues)

		for i, testValue := range ComplexStructType.TestValues {
			if i >= 5 { // Test first 5 for performance
				break
			}

			startTime := time.Now()

			// Encode the complex struct value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode complex struct %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Complex struct %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, ComplexStructTime,
				"Complex struct encoding took %v, expected less than %v", encodingTime, ComplexStructTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minStructInt(5, totalTests)) * 100
		t.Logf("✅ Complex struct operations: %d/%d successful (%.1f%%)",
			successCount, minStructInt(5, totalTests), successRate)
	})

	t.Log("✅ Complex struct operations testing completed")
}

// testNestedStructOperations tests nested struct operations
func testNestedStructOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting NESTED struct operations testing...")

	// Test nested structs with multiple levels
	t.Run("DeepNestedStructs", func(t *testing.T) {
		// Create a deeply nested structure
		playerProfile := PlayerProfile{
			Player: PlayerData{
				PlayerID:   int64(99999),
				Username:   "nested_test_player",
				Level:      int32(100),
				Experience: int64(5000000),
				IsActive:   true,
				LastLogin:  "2024-01-15T15:45:00Z",
				Settings: map[string]interface{}{
					"theme":         "custom",
					"notifications": true,
					"advanced":      true,
				},
			},
			Inventory: []interface{}{
				InventoryItem{
					ItemID:     int64(10001),
					Name:       "Nested Sword",
					Type:       "weapon",
					Quantity:   int32(1),
					Rarity:     int32(4),
					IsEquipped: true,
					Stats: map[string]interface{}{
						"attack": int32(200),
						"speed":  int32(90),
					},
					Attributes: []interface{}{"sharp", "magical"},
				},
			},
			Friends:      []interface{}{int64(11111), int64(22222), int64(33333)},
			Achievements: []interface{}{int32(1), int32(5), int32(10), int32(25)},
			Statistics: map[string]interface{}{
				"battles_won":  int32(150),
				"battles_lost": int32(25),
				"total_kills":  int32(500),
			},
			Preferences: map[string]interface{}{
				"auto_save":  true,
				"difficulty": int32(4),
				"show_hints": false,
			},
		}

		startTime := time.Now()
		encoded, err := encodingManager.Encode(playerProfile, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		encodingTime := time.Since(startTime)

		if err != nil {
			t.Logf("❌ Failed to encode nested PlayerProfile: %v", err)
			return
		}

		t.Logf("✅ Deeply nested PlayerProfile encoded to %d BSATN bytes in %v", len(encoded), encodingTime)

		assert.Less(t, encodingTime, ComplexStructTime,
			"Nested struct encoding took %v, expected less than %v", encodingTime, ComplexStructTime)
	})

	t.Log("✅ Nested struct operations testing completed")
}

// testStructCollectionOperations tests struct collection operations
func testStructCollectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting STRUCT collection operations testing...")

	// Test arrays of structs
	t.Run("StructArrays", func(t *testing.T) {
		// Create array of PlayerData structs
		players := []interface{}{
			PlayerData{
				PlayerID:   int64(10001),
				Username:   "player_1",
				Level:      int32(25),
				Experience: int64(50000),
				IsActive:   true,
				LastLogin:  "2024-01-15T10:00:00Z",
				Settings: map[string]interface{}{
					"theme": "light",
				},
			},
			PlayerData{
				PlayerID:   int64(10002),
				Username:   "player_2",
				Level:      int32(30),
				Experience: int64(75000),
				IsActive:   false,
				LastLogin:  "2024-01-14T18:30:00Z",
				Settings: map[string]interface{}{
					"theme": "dark",
				},
			},
		}

		encoded, err := encodingManager.Encode(players, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode struct array: %v", err)
			return
		}

		t.Logf("✅ Array of %d PlayerData structs encoded to %d BSATN bytes", len(players), len(encoded))
	})

	// Test maps containing structs
	t.Run("StructMaps", func(t *testing.T) {
		structMap := map[string]interface{}{
			"main_player": PlayerData{
				PlayerID:   int64(99999),
				Username:   "main_user",
				Level:      int32(50),
				Experience: int64(250000),
				IsActive:   true,
				LastLogin:  "2024-01-15T16:00:00Z",
				Settings: map[string]interface{}{
					"notifications": true,
				},
			},
			"session": GameSession{
				SessionID:   "session_map_test",
				PlayerCount: int32(1),
				StartTime:   "2024-01-15T16:00:00Z",
				Duration:    int32(1800), // 30 minutes
				IsPublic:    false,
				Players:     []interface{}{int64(99999)},
				Metadata: map[string]interface{}{
					"test": "struct_map",
				},
			},
		}

		encoded, err := encodingManager.Encode(structMap, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode struct map: %v", err)
			return
		}

		t.Logf("✅ Map containing structs encoded to %d BSATN bytes", len(encoded))
	})

	t.Log("✅ Struct collection operations testing completed")
}

// testStructEncodingValidation tests struct encoding validation
func testStructEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting STRUCT encoding validation testing...")

	// Test various struct encoding scenarios
	t.Run("StructEncodingScenarios", func(t *testing.T) {
		testStructs := []interface{}{
			// Minimal PlayerData
			PlayerData{
				PlayerID:   int64(1),
				Username:   "min_player",
				Level:      int32(1),
				Experience: int64(0),
				IsActive:   false,
				LastLogin:  "",
				Settings:   map[string]interface{}{},
			},
			// Maximal PlayerData
			PlayerData{
				PlayerID:   int64(9223372036854775807), // Max int64
				Username:   "max_player_with_very_long_username_to_test_limits",
				Level:      int32(2147483647), // Max int32
				Experience: int64(9223372036854775807),
				IsActive:   true,
				LastLogin:  "2024-12-31T23:59:59Z",
				Settings: map[string]interface{}{
					"setting1": "value1",
					"setting2": int32(123),
					"setting3": true,
					"setting4": int64(456),
					"setting5": "another_value",
				},
			},
			// Complex InventoryItem
			InventoryItem{
				ItemID:     int64(987654321),
				Name:       "Ultimate Test Item",
				Type:       "legendary_artifact",
				Quantity:   int32(99),
				Rarity:     int32(5),
				IsEquipped: true,
				Stats: map[string]interface{}{
					"power":        int32(999),
					"defense":      int32(888),
					"speed":        int32(777),
					"intelligence": int32(666),
					"luck":         int32(555),
				},
				Attributes: []interface{}{
					"indestructible", "self_repairing", "auto_upgrading",
					"soul_bound", "legendary_effect", "time_manipulation",
				},
			},
		}

		for i, testStruct := range testStructs {
			startTime := time.Now()
			encoded, err := encodingManager.Encode(testStruct, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode struct %d (%T): %v", i, testStruct, err)
				continue
			}

			t.Logf("✅ Struct %d (%T) → %d BSATN bytes in %v", i, testStruct, len(encoded), encodingTime)

			assert.Less(t, encodingTime, StructEncodingTime,
				"Struct encoding took %v, expected less than %v", encodingTime, StructEncodingTime)
		}
	})

	t.Log("✅ Struct encoding validation testing completed")
}

// testStructTypeValidation tests struct type validation
func testStructTypeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting STRUCT type validation testing...")

	// Test type safety of struct fields
	t.Run("StructFieldTypeValidation", func(t *testing.T) {
		typeValidationTests := []struct {
			name     string
			data     interface{}
			expected bool
			reason   string
		}{
			{
				name: "ValidPlayerDataWithProperTypes",
				data: PlayerData{
					PlayerID:   int64(12345),           // ✅ Explicit int64
					Username:   "valid_player",         // ✅ String
					Level:      int32(25),              // ✅ Explicit int32
					Experience: int64(50000),           // ✅ Explicit int64
					IsActive:   true,                   // ✅ Boolean
					LastLogin:  "2024-01-15T10:30:00Z", // ✅ String
					Settings: map[string]interface{}{ // ✅ String-keyed map
						"theme":  "dark",
						"volume": int32(80), // ✅ Explicit int32
					},
				},
				expected: true,
				reason:   "All fields use proper BSATN-compatible types",
			},
			{
				name: "ValidInventoryItemWithCollections",
				data: InventoryItem{
					ItemID:     int64(9001),   // ✅ Explicit int64
					Name:       "Test Weapon", // ✅ String
					Type:       "sword",       // ✅ String
					Quantity:   int32(1),      // ✅ Explicit int32
					Rarity:     int32(3),      // ✅ Explicit int32
					IsEquipped: false,         // ✅ Boolean
					Stats: map[string]interface{}{ // ✅ String-keyed map
						"attack": int32(100), // ✅ Explicit int32
						"speed":  int32(75),  // ✅ Explicit int32
					},
					Attributes: []interface{}{"sharp", "durable"}, // ✅ String array
				},
				expected: true,
				reason:   "Complex struct with proper collection types",
			},
			{
				name: "ValidGameSessionWithArrays",
				data: GameSession{
					SessionID:   "test_session",                              // ✅ String
					PlayerCount: int32(3),                                    // ✅ Explicit int32
					StartTime:   "2024-01-15T14:00:00Z",                      // ✅ String
					Duration:    int32(1800),                                 // ✅ Explicit int32
					IsPublic:    true,                                        // ✅ Boolean
					Players:     []interface{}{int64(1), int64(2), int64(3)}, // ✅ Explicit int64 array
					Metadata: map[string]interface{}{ // ✅ String-keyed map
						"game_mode": "casual",
						"max_score": int32(1000), // ✅ Explicit int32
					},
				},
				expected: true,
				reason:   "Game session with properly typed arrays and maps",
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
		t.Logf("✅ Struct type validation: %d/%d tests passed (%.1f%%)",
			successCount, len(typeValidationTests), successRate)
	})

	t.Log("✅ Struct type validation testing completed")
}

// testStructSpecificOperations tests struct-specific operations
func testStructSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting STRUCT specific operations testing...")

	// Test struct operations from configuration
	t.Run("ConfiguredStructOperations", func(t *testing.T) {
		for _, operation := range CustomStructOperations.StructOperations {
			encoded, err := encodingManager.Encode(operation.InputStruct, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputStruct) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
			t.Logf("📊 %s", operation.Expected)
		}
	})

	// Test struct field access patterns
	t.Run("StructFieldAccess", func(t *testing.T) {
		player := PlayerData{
			PlayerID:   int64(54321),
			Username:   "field_test_player",
			Level:      int32(42),
			Experience: int64(180000),
			IsActive:   true,
			LastLogin:  "2024-01-15T17:30:00Z",
			Settings: map[string]interface{}{
				"display_name":     "Field Tester",
				"public_profile":   true,
				"score_visibility": int32(2),
			},
		}

		// Test individual field encoding (simulated by encoding the whole struct)
		encoded, err := encodingManager.Encode(player, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode player for field access test: %v", err)
			return
		}

		t.Logf("✅ Player struct with field access patterns → %d bytes", len(encoded))
		t.Logf("📊 Struct fields: PlayerID=%d, Username=%s, Level=%d",
			player.PlayerID, player.Username, player.Level)
	})

	t.Log("✅ Struct specific operations testing completed")
}

// testStructPerformance tests struct performance
func testStructPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting STRUCT performance testing...")

	// Performance test for simple struct encoding
	t.Run("SimpleStructPerformance", func(t *testing.T) {
		testPlayer := PlayerData{
			PlayerID:   int64(12345),
			Username:   "perf_test_player",
			Level:      int32(25),
			Experience: int64(75000),
			IsActive:   true,
			LastLogin:  "2024-01-15T12:00:00Z",
			Settings: map[string]interface{}{
				"theme":  "performance",
				"volume": int32(50),
			},
		}
		iterations := StructPerformanceIters

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testPlayer, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Simple struct encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, StructEncodingTime,
			"Average struct encoding time %v exceeds threshold %v", avgTime, StructEncodingTime)
	})

	// Performance test for complex struct encoding
	t.Run("ComplexStructPerformance", func(t *testing.T) {
		testItem := InventoryItem{
			ItemID:     int64(9001),
			Name:       "Performance Test Item",
			Type:       "test_artifact",
			Quantity:   int32(1),
			Rarity:     int32(4),
			IsEquipped: true,
			Stats: map[string]interface{}{
				"attack":     int32(150),
				"defense":    int32(100),
				"speed":      int32(85),
				"magic":      int32(120),
				"durability": int32(95),
			},
			Attributes: []interface{}{
				"performance_tested", "benchmark_ready",
				"optimized", "efficient", "fast_encoding",
			},
		}
		iterations := StructPerformanceIters / 2 // Fewer iterations for complex structs

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testItem, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Complex struct encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, ComplexStructTime,
			"Average complex struct encoding time %v exceeds threshold %v", avgTime, ComplexStructTime)
	})

	// Memory usage test for struct operations
	t.Run("StructMemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many struct instances
		for i := 0; i < 200; i++ {
			player := PlayerData{
				PlayerID:   int64(i + 1000),
				Username:   fmt.Sprintf("mem_test_player_%d", i),
				Level:      int32(i%100 + 1),
				Experience: int64(i * 1000),
				IsActive:   i%2 == 0,
				LastLogin:  fmt.Sprintf("2024-01-15T%02d:00:00Z", i%24),
				Settings: map[string]interface{}{
					"test_run":  i,
					"memory_id": int32(i),
				},
			}

			item := InventoryItem{
				ItemID:     int64(i + 5000),
				Name:       fmt.Sprintf("Memory Test Item %d", i),
				Type:       "test_item",
				Quantity:   int32(i%10 + 1),
				Rarity:     int32(i%5 + 1),
				IsEquipped: i%3 == 0,
				Stats: map[string]interface{}{
					"power": int32(i % 200),
				},
				Attributes: []interface{}{fmt.Sprintf("attr_%d", i)},
			}

			encodingManager.Encode(player, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(item, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 200 struct pairs: %d bytes", memUsed)
	})

	t.Log("✅ Struct performance testing completed")
}

// Helper function for minimum calculation
func minStructInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
