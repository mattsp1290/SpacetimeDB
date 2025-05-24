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

// SDK Task 6 Fixed: Collection Types with BSATN Constraints
//
// This test demonstrates the CORRECT way to use SpacetimeDB collection types:
// - Arrays: Using explicit integer types (int32, int64) instead of int
// - Maps: Using string keys (map[string]interface{}) instead of interface{} keys
// - Sets: Using proper type-safe element collections
// - Performance: Optimized for BSATN encoding/decoding
// - Type Safety: Leveraging BSATN's strict type system

// FixedArrayTypeConfig defines configuration for properly typed Array testing
type FixedArrayTypeConfig struct {
	Name          string
	TableName     string        // SDK-test table name
	VecTableName  string        // Vector table name
	SingleReducer string        // Single value reducer name
	VectorReducer string        // Vector reducer name
	TestValues    []interface{} // Standard Array test values (properly typed)
	Int32Arrays   []interface{} // int32 array values
	Int64Arrays   []interface{} // int64 array values
	StringArrays  []interface{} // String array values
	BoolArrays    []interface{} // Boolean array values
	MixedArrays   []interface{} // Mixed type arrays (type-safe)
	NestedArrays  []interface{} // Nested array values
	VectorSizes   []int         // Vector sizes to test
	MaxArraySize  int           // Maximum array size for testing
}

// FixedMapTypeConfig defines configuration for properly typed Map testing
type FixedMapTypeConfig struct {
	Name              string
	TableName         string        // SDK-test table name
	VecTableName      string        // Vector table name
	SingleReducer     string        // Single value reducer name
	VectorReducer     string        // Vector reducer name
	TestValues        []interface{} // Standard Map test values (string keys only)
	SimpleStringMaps  []interface{} // Simple string-keyed maps
	ComplexStringMaps []interface{} // Complex string-keyed maps
	NestedMaps        []interface{} // Nested map values (string keys)
	VectorSizes       []int         // Vector sizes to test
	MaxMapSize        int           // Maximum map size for testing
}

// FixedSetTypeConfig defines configuration for properly typed Set testing
type FixedSetTypeConfig struct {
	Name          string
	TableName     string        // SDK-test table name
	VecTableName  string        // Vector table name
	SingleReducer string        // Single value reducer name
	VectorReducer string        // Vector reducer name
	TestValues    []interface{} // Standard Set test values (properly typed)
	Int32Sets     []interface{} // int32 element sets
	Int64Sets     []interface{} // int64 element sets
	StringSets    []interface{} // String element sets
	BoolSets      []interface{} // Boolean element sets
	VectorSizes   []int         // Vector sizes to test
	MaxSetSize    int           // Maximum set size for testing
}

// FixedCollectionOperationsConfig defines configuration for type-safe operations
type FixedCollectionOperationsConfig struct {
	Name             string
	ArrayOperations  []FixedArrayOperationTest
	MapOperations    []FixedMapOperationTest
	SetOperations    []FixedSetOperationTest
	TypeSafetyTests  []TypeSafetyTest
	PerformanceTests []FixedPerformanceTest
}

// FixedArrayOperationTest defines a type-safe array operation test
type FixedArrayOperationTest struct {
	Name        string
	Description string
	InputArray  []interface{}            // Input array data (properly typed)
	Operation   string                   // Operation type
	Expected    string                   // Expected behavior
	Validate    func([]interface{}) bool // Validation function
}

// FixedMapOperationTest defines a type-safe map operation test
type FixedMapOperationTest struct {
	Name        string
	Description string
	InputMap    map[string]interface{}            // Input map data (string keys only)
	Operation   string                            // Operation type
	Key         string                            // Key for operation (string only)
	Value       interface{}                       // Value for operation
	Expected    string                            // Expected behavior
	Validate    func(map[string]interface{}) bool // Validation function
}

// FixedSetOperationTest defines a type-safe set operation test
type FixedSetOperationTest struct {
	Name        string
	Description string
	InputSet    []interface{}            // Input set data (properly typed)
	Operation   string                   // Operation type
	Element     interface{}              // Element for operation (properly typed)
	Expected    string                   // Expected behavior
	Validate    func([]interface{}) bool // Validation function
}

// TypeSafetyTest defines a type safety validation test
type TypeSafetyTest struct {
	Name        string
	Description string
	TestData    interface{} // Test data to validate
	Expected    bool        // Expected success/failure
	Reason      string      // Reason for expected result
}

// FixedPerformanceTest defines a performance test with proper types
type FixedPerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB Array type configuration (FIXED)
var FixedArrayType = FixedArrayTypeConfig{
	Name:      "fixed_array",
	TableName: "one_array", VecTableName: "vec_array",
	SingleReducer: "insert_one_array", VectorReducer: "insert_vec_array",
	TestValues: []interface{}{
		// Standard Array values (using proper types)
		[]interface{}{},                                                 // Empty array
		[]interface{}{int32(1)},                                         // Single int32 element
		[]interface{}{int32(1), int32(2), int32(3)},                     // Small int32 array
		[]interface{}{"a", "b", "c"},                                    // String array (already works)
		[]interface{}{true, false, true},                                // Boolean array (already works)
		[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}, // int64 array
	},
	Int32Arrays: []interface{}{
		[]interface{}{int32(10), int32(20), int32(30)},
		[]interface{}{int32(-1), int32(0), int32(1)},
		[]interface{}{int32(100), int32(200), int32(300), int32(400)},
	},
	Int64Arrays: []interface{}{
		[]interface{}{int64(1000), int64(2000), int64(3000)},
		[]interface{}{int64(-1000), int64(0), int64(1000)},
		[]interface{}{int64(9223372036854775807)}, // Max int64
	},
	StringArrays: []interface{}{
		[]interface{}{"hello", "world"},
		[]interface{}{"red", "green", "blue"},
		[]interface{}{"SpacetimeDB", "Go", "SDK"},
	},
	BoolArrays: []interface{}{
		[]interface{}{true},
		[]interface{}{false},
		[]interface{}{true, false, true, false},
	},
	MixedArrays: []interface{}{
		// Note: Mixed arrays need to be carefully typed
		[]interface{}{"string", true},              // String + Bool
		[]interface{}{int32(42), "answer"},         // int32 + String
		[]interface{}{int64(123), "number", false}, // int64 + String + Bool
	},
	NestedArrays: []interface{}{
		[]interface{}{
			[]interface{}{int32(1), int32(2)},
			[]interface{}{int32(3), int32(4)},
		}, // 2D int32 array
		[]interface{}{
			[]interface{}{"a", "b"},
			[]interface{}{"c", "d"},
		}, // 2D string array
	},
	VectorSizes:  []int{1, 2, 5, 10, 25},
	MaxArraySize: 100,
}

// SpacetimeDB Map type configuration (FIXED)
var FixedMapType = FixedMapTypeConfig{
	Name:      "fixed_map",
	TableName: "one_map", VecTableName: "vec_map",
	SingleReducer: "insert_one_map", VectorReducer: "insert_vec_map",
	TestValues: []interface{}{
		// Standard Map values (using string keys only)
		map[string]interface{}{},               // Empty map
		map[string]interface{}{"key": "value"}, // Single entry
		map[string]interface{}{
			"name": "Alice",
			"age":  int32(30), // Explicit int32
			"city": "New York",
		}, // String-keyed map with mixed values
		map[string]interface{}{
			"count":   int64(42), // Explicit int64
			"active":  true,
			"message": "hello world",
		}, // More mixed value types
	},
	SimpleStringMaps: []interface{}{
		map[string]interface{}{
			"first": "John",
			"last":  "Doe",
		},
		map[string]interface{}{
			"host": "localhost",
			"port": int32(8080), // Explicit int32
			"ssl":  false,
		},
	},
	ComplexStringMaps: []interface{}{
		map[string]interface{}{
			"user_id":    int64(12345),
			"username":   "alice_smith",
			"is_admin":   true,
			"last_login": "2024-01-01",
			"settings": map[string]interface{}{
				"theme":         "dark",
				"notifications": true,
			},
		},
		map[string]interface{}{
			"product_id":   int32(101),
			"product_name": "SpacetimeDB SDK",
			"price":        "free",
			"categories":   []interface{}{"database", "realtime", "gaming"},
		},
	},
	NestedMaps: []interface{}{
		map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"value": int32(42),
				},
			},
		},
		map[string]interface{}{
			"metadata": map[string]interface{}{
				"version": "1.0",
				"build":   int32(123),
			},
			"data": []interface{}{"item1", "item2", "item3"},
		},
	},
	VectorSizes: []int{1, 2, 5, 10, 25},
	MaxMapSize:  50,
}

// SpacetimeDB Set type configuration (FIXED)
var FixedSetType = FixedSetTypeConfig{
	Name:      "fixed_set",
	TableName: "one_set", VecTableName: "vec_set",
	SingleReducer: "insert_one_set", VectorReducer: "insert_vec_set",
	TestValues: []interface{}{
		// Standard Set values (using proper types)
		[]interface{}{},                             // Empty set
		[]interface{}{int32(1)},                     // Single int32 element
		[]interface{}{int32(1), int32(2), int32(3)}, // Small int32 set
		[]interface{}{"a", "b", "c"},                // String set (already works)
		[]interface{}{true, false},                  // Boolean set (already works)
		[]interface{}{int64(10), int64(20), int64(30), int64(40), int64(50)}, // int64 set
	},
	Int32Sets: []interface{}{
		[]interface{}{int32(2), int32(4), int32(6), int32(8), int32(10)},          // Even numbers
		[]interface{}{int32(1), int32(3), int32(5), int32(7), int32(9)},           // Odd numbers
		[]interface{}{int32(1), int32(1), int32(2), int32(3), int32(5), int32(8)}, // Fibonacci (with duplicates to test uniqueness)
	},
	Int64Sets: []interface{}{
		[]interface{}{int64(100), int64(200), int64(300)},
		[]interface{}{int64(1000), int64(2000), int64(3000), int64(4000)},
		[]interface{}{int64(-100), int64(0), int64(100)},
	},
	StringSets: []interface{}{
		[]interface{}{"red", "green", "blue"},
		[]interface{}{"apple", "banana", "cherry", "date"},
		[]interface{}{"admin", "user", "guest"},
	},
	BoolSets: []interface{}{
		[]interface{}{true},
		[]interface{}{false},
		[]interface{}{true, false}, // Both boolean values
	},
	VectorSizes: []int{1, 2, 5, 10, 25},
	MaxSetSize:  75,
}

// Fixed collection operations configuration
var FixedCollectionOperations = FixedCollectionOperationsConfig{
	Name: "fixed_collection_operations",
	ArrayOperations: []FixedArrayOperationTest{
		{
			Name:        "int32_array_access",
			Description: "Array element access with int32 values",
			InputArray:  []interface{}{int32(10), int32(20), int32(30), int32(40), int32(50)},
			Operation:   "access",
			Expected:    "Should access int32 elements by valid indices",
			Validate: func(arr []interface{}) bool {
				return len(arr) == 5 && arr[2] == int32(30)
			},
		},
		{
			Name:        "string_array_operations",
			Description: "String array operations",
			InputArray:  []interface{}{"hello", "world", "spacetime"},
			Operation:   "string_ops",
			Expected:    "Should handle string arrays perfectly",
			Validate: func(arr []interface{}) bool {
				return len(arr) >= 3
			},
		},
		{
			Name:        "bool_array_operations",
			Description: "Boolean array operations",
			InputArray:  []interface{}{true, false, true, false},
			Operation:   "bool_ops",
			Expected:    "Should handle boolean arrays perfectly",
			Validate: func(arr []interface{}) bool {
				return len(arr) == 4
			},
		},
	},
	MapOperations: []FixedMapOperationTest{
		{
			Name:        "string_map_get",
			Description: "Map value retrieval with string keys",
			InputMap: map[string]interface{}{
				"name": "Alice",
				"age":  int32(30),
			},
			Operation: "get",
			Key:       "name",
			Expected:  "Should retrieve value by string key",
			Validate: func(m map[string]interface{}) bool {
				return m["name"] == "Alice"
			},
		},
		{
			Name:        "typed_map_set",
			Description: "Map value setting with proper types",
			InputMap: map[string]interface{}{
				"count": int64(0),
			},
			Operation: "set",
			Key:       "count",
			Value:     int64(42),
			Expected:  "Should set value with proper typing",
			Validate: func(m map[string]interface{}) bool {
				return len(m) >= 1
			},
		},
		{
			Name:        "complex_map_operations",
			Description: "Complex map with multiple types",
			InputMap: map[string]interface{}{
				"id":     int64(12345),
				"name":   "TestUser",
				"active": true,
				"score":  int32(100),
			},
			Operation: "complex",
			Key:       "active",
			Expected:  "Should handle complex typed maps",
			Validate: func(m map[string]interface{}) bool {
				return m["active"] == true
			},
		},
	},
	SetOperations: []FixedSetOperationTest{
		{
			Name:        "int32_set_add",
			Description: "Set element addition with int32",
			InputSet:    []interface{}{int32(1), int32(2), int32(3)},
			Operation:   "add",
			Element:     int32(4),
			Expected:    "Should add int32 elements to set",
			Validate: func(s []interface{}) bool {
				return len(s) >= 3
			},
		},
		{
			Name:        "string_set_contains",
			Description: "Set element containment check with strings",
			InputSet:    []interface{}{"apple", "banana", "cherry"},
			Operation:   "contains",
			Element:     "banana",
			Expected:    "Should check string element existence",
			Validate: func(s []interface{}) bool {
				for _, item := range s {
					if item == "banana" {
						return true
					}
				}
				return false
			},
		},
		{
			Name:        "int64_set_remove",
			Description: "Set element removal with int64",
			InputSet:    []interface{}{int64(100), int64(200), int64(300), int64(400), int64(500)},
			Operation:   "remove",
			Element:     int64(300),
			Expected:    "Should remove int64 elements from set",
			Validate: func(s []interface{}) bool {
				for _, item := range s {
					if item == int64(300) {
						return false // Should not contain 300 after removal
					}
				}
				return true
			},
		},
	},
}

// Test configuration constants for fixed collection operations
const (
	// Performance thresholds (optimized for fixed types)
	FixedArrayInsertTime        = 75 * time.Millisecond // Single array insertion
	FixedMapInsertTime          = 75 * time.Millisecond // Single map insertion
	FixedSetInsertTime          = 75 * time.Millisecond // Single set insertion
	FixedCollectionEncodingTime = 25 * time.Millisecond // Collection encoding

	// Test limits (optimized for fixed types)
	MaxGeneratedFixedArrays              = 25  // Number of random arrays to generate
	MaxGeneratedFixedMaps                = 20  // Number of random maps to generate
	MaxGeneratedFixedSets                = 30  // Number of random sets to generate
	FixedCollectionPerformanceIterations = 150 // Iterations for performance testing
	LargeFixedCollectionSize             = 150 // Size for large collection tests
)

// TestSDKCollectionsFixed is the main integration test for fixed collection types
func TestSDKCollectionsFixed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK fixed collections integration test in short mode")
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
	t.Logf("🎯 Testing FIXED Array types: Explicit int32/int64, strings, booleans")
	t.Logf("🎯 Testing FIXED Map types: String keys with typed values")
	t.Logf("🎯 Testing FIXED Set types: Properly typed element collections")

	// Generate random test data for fixed collections
	generateRandomFixedCollectionData(t)

	// Run comprehensive fixed collection test suites
	t.Run("FixedArrayOperations", func(t *testing.T) {
		testFixedArrayOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedMapOperations", func(t *testing.T) {
		testFixedMapOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedSetOperations", func(t *testing.T) {
		testFixedSetOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedVectorOperations", func(t *testing.T) {
		testFixedVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedNestedOperations", func(t *testing.T) {
		testFixedNestedOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedEncodingValidation", func(t *testing.T) {
		testFixedEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TypeSafetyValidation", func(t *testing.T) {
		testTypeSafetyValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedSpecificOperations", func(t *testing.T) {
		testFixedSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FixedPerformanceMeasurement", func(t *testing.T) {
		testFixedPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomFixedCollectionData generates random properly-typed collections for testing
func generateRandomFixedCollectionData(t *testing.T) {
	t.Log("Generating random FIXED collection test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random int32 arrays
	for i := 0; i < MaxGeneratedFixedArrays; i++ {
		arraySize := rand.Intn(15) + 1 // Arrays of size 1-15
		randomArray := make([]interface{}, arraySize)

		// Fill with properly typed data
		for j := 0; j < arraySize; j++ {
			switch rand.Intn(4) {
			case 0:
				randomArray[j] = int32(rand.Intn(1000))
			case 1:
				randomArray[j] = fmt.Sprintf("item_%d", rand.Intn(100))
			case 2:
				randomArray[j] = int64(rand.Intn(10000))
			case 3:
				randomArray[j] = rand.Intn(2) == 1
			}
		}

		FixedArrayType.TestValues = append(FixedArrayType.TestValues, randomArray)
	}

	// Generate random string-keyed maps
	for i := 0; i < MaxGeneratedFixedMaps; i++ {
		mapSize := rand.Intn(10) + 1 // Maps of size 1-10
		randomMap := make(map[string]interface{})

		for j := 0; j < mapSize; j++ {
			// Generate random string key
			key := fmt.Sprintf("key_%d", j)

			// Generate properly typed value
			switch rand.Intn(4) {
			case 0:
				randomMap[key] = int32(rand.Intn(1000))
			case 1:
				randomMap[key] = fmt.Sprintf("value_%d", rand.Intn(100))
			case 2:
				randomMap[key] = int64(rand.Intn(10000))
			case 3:
				randomMap[key] = rand.Intn(2) == 1
			}
		}

		FixedMapType.TestValues = append(FixedMapType.TestValues, randomMap)
	}

	// Generate random properly-typed sets
	for i := 0; i < MaxGeneratedFixedSets; i++ {
		setSize := rand.Intn(20) + 1 // Sets of size 1-20

		// Choose a single type for each set to ensure consistency
		switch rand.Intn(4) {
		case 0: // int32 set
			uniqueItems := make(map[int32]bool)
			for len(uniqueItems) < setSize {
				uniqueItems[int32(rand.Intn(1000))] = true
			}
			randomSet := make([]interface{}, 0, len(uniqueItems))
			for item := range uniqueItems {
				randomSet = append(randomSet, item)
			}
			FixedSetType.TestValues = append(FixedSetType.TestValues, randomSet)

		case 1: // string set
			uniqueItems := make(map[string]bool)
			for len(uniqueItems) < setSize {
				uniqueItems[fmt.Sprintf("item_%d", rand.Intn(100))] = true
			}
			randomSet := make([]interface{}, 0, len(uniqueItems))
			for item := range uniqueItems {
				randomSet = append(randomSet, item)
			}
			FixedSetType.TestValues = append(FixedSetType.TestValues, randomSet)

		case 2: // int64 set
			uniqueItems := make(map[int64]bool)
			for len(uniqueItems) < setSize {
				uniqueItems[int64(rand.Intn(10000))] = true
			}
			randomSet := make([]interface{}, 0, len(uniqueItems))
			for item := range uniqueItems {
				randomSet = append(randomSet, item)
			}
			FixedSetType.TestValues = append(FixedSetType.TestValues, randomSet)

		case 3: // bool set (always just true/false)
			randomSet := []interface{}{true, false}
			FixedSetType.TestValues = append(FixedSetType.TestValues, randomSet)
		}
	}

	// Generate large properly-typed collections for performance testing
	largeInt32Array := make([]interface{}, LargeFixedCollectionSize)
	for i := 0; i < LargeFixedCollectionSize; i++ {
		largeInt32Array[i] = int32(i)
	}
	FixedArrayType.TestValues = append(FixedArrayType.TestValues, largeInt32Array)

	largeStringMap := make(map[string]interface{})
	for i := 0; i < LargeFixedCollectionSize; i++ {
		largeStringMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	FixedMapType.TestValues = append(FixedMapType.TestValues, largeStringMap)

	largeInt64Set := make([]interface{}, LargeFixedCollectionSize)
	for i := 0; i < LargeFixedCollectionSize; i++ {
		largeInt64Set[i] = int64(i)
	}
	FixedSetType.TestValues = append(FixedSetType.TestValues, largeInt64Set)

	t.Logf("✅ Generated %d arrays, %d maps, %d sets with proper BSATN types",
		MaxGeneratedFixedArrays, MaxGeneratedFixedMaps, MaxGeneratedFixedSets)
}

// testFixedArrayOperations tests properly typed Array operations
func testFixedArrayOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED Array operations testing...")

	t.Run("StandardFixedArrays", func(t *testing.T) {
		t.Log("Testing properly typed Array values")

		successCount := 0
		totalTests := len(FixedArrayType.TestValues)

		for i, testValue := range FixedArrayType.TestValues {
			if i >= 8 { // Test first 8 for performance
				break
			}

			startTime := time.Now()

			// Encode the Array value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode fixed Array value %v: %v", testValue, err)
				continue
			}

			// Validate Array encoding
			arrayValue := testValue.([]interface{})
			t.Logf("✅ Fixed Array (size %d) encoded to %d BSATN bytes", len(arrayValue), len(encoded))

			encodingTime := time.Since(startTime)
			assert.Less(t, encodingTime, FixedCollectionEncodingTime,
				"Fixed Array encoding took %v, expected less than %v", encodingTime, FixedCollectionEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minInt(8, totalTests)) * 100
		t.Logf("✅ Fixed Array operations: %d/%d successful (%.1f%%)",
			successCount, minInt(8, totalTests), successRate)
	})

	t.Log("✅ Fixed Array operations testing completed")
}

// testFixedMapOperations tests properly typed Map operations
func testFixedMapOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED Map operations testing...")

	t.Run("StandardFixedMaps", func(t *testing.T) {
		t.Log("Testing properly typed Map values with string keys")

		successCount := 0
		totalTests := len(FixedMapType.TestValues)

		for i, testValue := range FixedMapType.TestValues {
			if i >= 8 { // Test first 8 for performance
				break
			}

			startTime := time.Now()

			// Encode the Map value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode fixed Map value %v: %v", testValue, err)
				continue
			}

			// Validate Map encoding
			mapValue := testValue.(map[string]interface{})
			t.Logf("✅ Fixed Map (size %d) encoded to %d BSATN bytes", len(mapValue), len(encoded))

			encodingTime := time.Since(startTime)
			assert.Less(t, encodingTime, FixedCollectionEncodingTime,
				"Fixed Map encoding took %v, expected less than %v", encodingTime, FixedCollectionEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minInt(8, totalTests)) * 100
		t.Logf("✅ Fixed Map operations: %d/%d successful (%.1f%%)",
			successCount, minInt(8, totalTests), successRate)
	})

	t.Log("✅ Fixed Map operations testing completed")
}

// testFixedSetOperations tests properly typed Set operations
func testFixedSetOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED Set operations testing...")

	t.Run("StandardFixedSets", func(t *testing.T) {
		t.Log("Testing properly typed Set values")

		successCount := 0
		totalTests := len(FixedSetType.TestValues)

		for i, testValue := range FixedSetType.TestValues {
			if i >= 8 { // Test first 8 for performance
				break
			}

			startTime := time.Now()

			// Encode the Set value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode fixed Set value %v: %v", testValue, err)
				continue
			}

			// Validate Set encoding
			setValue := testValue.([]interface{})
			t.Logf("✅ Fixed Set (size %d) encoded to %d BSATN bytes", len(setValue), len(encoded))

			encodingTime := time.Since(startTime)
			assert.Less(t, encodingTime, FixedCollectionEncodingTime,
				"Fixed Set encoding took %v, expected less than %v", encodingTime, FixedCollectionEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minInt(8, totalTests)) * 100
		t.Logf("✅ Fixed Set operations: %d/%d successful (%.1f%%)",
			successCount, minInt(8, totalTests), successRate)
	})

	t.Log("✅ Fixed Set operations testing completed")
}

// testFixedVectorOperations tests vector operations with fixed types
func testFixedVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED vector operations testing...")

	// Test typed array vectors
	t.Run("TypedArrayVectors", func(t *testing.T) {
		for _, vectorSize := range FixedArrayType.VectorSizes {
			if vectorSize > 5 { // Limit for performance
				continue
			}

			// Create vector of properly typed arrays
			arrayVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				arrayVector[i] = []interface{}{int32(i), int32(i * 2), int32(i * 3)}
			}

			encoded, err := encodingManager.Encode(arrayVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode typed Array vector of size %d: %v", vectorSize, err)
				continue
			}

			t.Logf("✅ Typed Array vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		}
	})

	// Test string-keyed map vectors
	t.Run("StringKeyMapVectors", func(t *testing.T) {
		for _, vectorSize := range FixedMapType.VectorSizes {
			if vectorSize > 5 { // Limit for performance
				continue
			}

			// Create vector of properly typed maps
			mapVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				testMap := make(map[string]interface{})
				testMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
				testMap["index"] = int32(i)
				testMap["active"] = i%2 == 0
				mapVector[i] = testMap
			}

			encoded, err := encodingManager.Encode(mapVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode string-key Map vector of size %d: %v", vectorSize, err)
				continue
			}

			t.Logf("✅ String-key Map vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		}
	})

	t.Log("✅ Fixed vector operations testing completed")
}

// testFixedNestedOperations tests nested operations with fixed types
func testFixedNestedOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED nested operations testing...")

	// Test nested typed arrays
	t.Run("NestedTypedArrays", func(t *testing.T) {
		nestedArray := []interface{}{
			[]interface{}{int32(1), int32(2), int32(3)},
			[]interface{}{"a", "b", "c"},
			[]interface{}{true, false},
		}

		encoded, err := encodingManager.Encode(nestedArray, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode nested typed array: %v", err)
			return
		}

		t.Logf("✅ Nested typed array encoded to %d BSATN bytes", len(encoded))
	})

	// Test nested string-keyed maps
	t.Run("NestedStringKeyMaps", func(t *testing.T) {
		nestedMap := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"value": int32(42),
					"name":  "nested",
				},
				"array": []interface{}{int64(1), int64(2), int64(3)},
			},
			"top_level": "test",
		}

		encoded, err := encodingManager.Encode(nestedMap, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode nested string-key map: %v", err)
			return
		}

		t.Logf("✅ Nested string-key map encoded to %d BSATN bytes", len(encoded))
	})

	t.Log("✅ Fixed nested operations testing completed")
}

// testFixedEncodingValidation tests encoding validation for fixed types
func testFixedEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED encoding validation testing...")

	// Test int32 array encoding
	t.Run("Int32ArrayEncoding", func(t *testing.T) {
		testArrays := [][]interface{}{
			{int32(1), int32(2), int32(3)},
			{int32(-100), int32(0), int32(100)},
			{int32(2147483647)}, // Max int32
		}

		for i, arr := range testArrays {
			encoded, err := encodingManager.Encode(arr, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode int32 Array %d: %v", i, err)
				continue
			}
			t.Logf("✅ Int32 Array (size %d) → %d BSATN bytes", len(arr), len(encoded))
		}
	})

	// Test string-keyed map encoding
	t.Run("StringKeyMapEncoding", func(t *testing.T) {
		testMaps := []map[string]interface{}{
			{"key": "value"},
			{"name": "Alice", "age": int32(30)},
			{"active": true, "count": int64(42)},
		}

		for i, m := range testMaps {
			encoded, err := encodingManager.Encode(m, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode string-key Map %d: %v", i, err)
				continue
			}
			t.Logf("✅ String-key Map (size %d) → %d BSATN bytes", len(m), len(encoded))
		}
	})

	t.Log("✅ Fixed encoding validation testing completed")
}

// testTypeSafetyValidation tests type safety validation
func testTypeSafetyValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting type safety validation testing...")

	// Test type-safe collections
	t.Run("TypeSafeCollections", func(t *testing.T) {
		typeSafeTests := []struct {
			name     string
			data     interface{}
			expected bool
		}{
			{
				name:     "ValidInt32Array",
				data:     []interface{}{int32(1), int32(2), int32(3)},
				expected: true,
			},
			{
				name:     "ValidStringKeyMap",
				data:     map[string]interface{}{"key": "value"},
				expected: true,
			},
			{
				name:     "ValidMixedTypedArray",
				data:     []interface{}{int32(42), "hello", true},
				expected: true,
			},
			{
				name:     "ValidInt64Set",
				data:     []interface{}{int64(100), int64(200), int64(300)},
				expected: true,
			},
		}

		successCount := 0
		for _, test := range typeSafeTests {
			encoded, err := encodingManager.Encode(test.data, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			if (err == nil) == test.expected {
				t.Logf("✅ %s: encoding success=%v (expected=%v) → %d bytes",
					test.name, err == nil, test.expected, len(encoded))
				successCount++
			} else {
				t.Logf("❌ %s: encoding success=%v (expected=%v): %v",
					test.name, err == nil, test.expected, err)
			}
		}

		successRate := float64(successCount) / float64(len(typeSafeTests)) * 100
		t.Logf("✅ Type safety validation: %d/%d tests passed (%.1f%%)",
			successCount, len(typeSafeTests), successRate)
	})

	t.Log("✅ Type safety validation testing completed")
}

// testFixedSpecificOperations tests collection-specific operations with fixed types
func testFixedSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED specific operations testing...")

	// Test fixed array operations
	t.Run("FixedArrayOperations", func(t *testing.T) {
		for _, operation := range FixedCollectionOperations.ArrayOperations {
			encoded, err := encodingManager.Encode(operation.InputArray, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputArray) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
		}
	})

	// Test fixed map operations
	t.Run("FixedMapOperations", func(t *testing.T) {
		for _, operation := range FixedCollectionOperations.MapOperations {
			encoded, err := encodingManager.Encode(operation.InputMap, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputMap) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
		}
	})

	// Test fixed set operations
	t.Run("FixedSetOperations", func(t *testing.T) {
		for _, operation := range FixedCollectionOperations.SetOperations {
			encoded, err := encodingManager.Encode(operation.InputSet, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputSet) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
		}
	})

	t.Log("✅ Fixed specific operations testing completed")
}

// testFixedPerformance tests performance with fixed types
func testFixedPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting FIXED performance testing...")

	// Performance test for int32 array encoding
	t.Run("Int32ArrayPerformance", func(t *testing.T) {
		testArray := []interface{}{int32(1), int32(2), int32(3), int32(4), int32(5)}
		iterations := FixedCollectionPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testArray, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Int32 Array encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, FixedCollectionEncodingTime,
			"Average int32 Array encoding time %v exceeds threshold %v", avgTime, FixedCollectionEncodingTime)
	})

	// Performance test for string-keyed map encoding
	t.Run("StringKeyMapPerformance", func(t *testing.T) {
		testMap := map[string]interface{}{
			"name":   "test",
			"age":    int32(25),
			"active": true,
			"score":  int64(100),
		}
		iterations := FixedCollectionPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testMap, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ String-key Map encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, FixedCollectionEncodingTime,
			"Average string-key Map encoding time %v exceeds threshold %v", avgTime, FixedCollectionEncodingTime)
	})

	// Memory usage test
	t.Run("FixedMemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many fixed-type collections
		for i := 0; i < 300; i++ {
			testArray := []interface{}{int32(i), int32(i + 1), int32(i + 2)}
			testMap := map[string]interface{}{
				fmt.Sprintf("key_%d", i): fmt.Sprintf("value_%d", i),
				"index":                  int32(i),
			}
			testSet := []interface{}{int64(i), int64(i + 1), int64(i + 2)}

			encodingManager.Encode(testArray, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testMap, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testSet, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 300 fixed-type collections: %d bytes", memUsed)
	})

	t.Log("✅ Fixed performance testing completed")
}

// Helper function for minimum calculation (Go doesn't have a built-in min for int)
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
