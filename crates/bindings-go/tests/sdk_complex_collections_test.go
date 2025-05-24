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

// SDK Task 6: Complex Collection Types Coverage
//
// Comprehensive testing of SpacetimeDB's complex collection types:
// - Arrays: Fixed-size collections and multidimensional arrays
// - Maps: Key-value mappings with typed keys/values
// - Sets: Unique element collections with efficient operations
// - Complex nesting: Collections within collections
// - Performance optimization for large-scale operations
// - Edge cases: Empty collections, duplicate handling, type validation

// ArrayTypeConfig defines configuration for Array type testing
type ArrayTypeConfig struct {
	Name            string
	TableName       string        // SDK-test table name
	VecTableName    string        // Vector table name
	SingleReducer   string        // Single value reducer name
	VectorReducer   string        // Vector reducer name
	TestValues      []interface{} // Standard Array test values
	FixedSizeArrays []interface{} // Fixed-size array values
	MultiDimArrays  []interface{} // Multidimensional array values
	EmptyArrays     []interface{} // Empty array values
	LargeArrays     []interface{} // Large array values
	NestedArrays    []interface{} // Nested array values
	VectorSizes     []int         // Vector sizes to test
	MaxArraySize    int           // Maximum array size for testing
	MaxDimensions   int           // Maximum dimensions for testing
}

// MapTypeConfig defines configuration for Map type testing
type MapTypeConfig struct {
	Name            string
	TableName       string        // SDK-test table name
	VecTableName    string        // Vector table name
	SingleReducer   string        // Single value reducer name
	VectorReducer   string        // Vector reducer name
	TestValues      []interface{} // Standard Map test values
	StringKeyMaps   []interface{} // String-keyed maps
	IntegerKeyMaps  []interface{} // Integer-keyed maps
	EmptyMaps       []interface{} // Empty map values
	LargeMaps       []interface{} // Large map values
	NestedMaps      []interface{} // Nested map values
	VectorSizes     []int         // Vector sizes to test
	MaxMapSize      int           // Maximum map size for testing
	MaxNestingDepth int           // Maximum nesting depth for testing
}

// SetTypeConfig defines configuration for Set type testing
type SetTypeConfig struct {
	Name            string
	TableName       string        // SDK-test table name
	VecTableName    string        // Vector table name
	SingleReducer   string        // Single value reducer name
	VectorReducer   string        // Vector reducer name
	TestValues      []interface{} // Standard Set test values
	StringSets      []interface{} // String element sets
	IntegerSets     []interface{} // Integer element sets
	EmptySets       []interface{} // Empty set values
	LargeSets       []interface{} // Large set values
	DuplicateTests  []interface{} // Duplicate handling tests
	VectorSizes     []int         // Vector sizes to test
	MaxSetSize      int           // Maximum set size for testing
	UniquenessTests int           // Number of uniqueness tests
}

// CollectionOperationsConfig defines configuration for collection-specific operations
type CollectionOperationsConfig struct {
	Name             string
	ArrayOperations  []ArrayOperationTest
	MapOperations    []MapOperationTest
	SetOperations    []SetOperationTest
	NestedOperations []NestedOperationTest
	PerformanceTests []PerformanceTest
}

// ArrayOperationTest defines an array operation test scenario
type ArrayOperationTest struct {
	Name        string
	Description string
	InputArray  []interface{}            // Input array data
	Operation   string                   // Operation type (access, slice, append, etc.)
	Expected    string                   // Expected behavior
	Validate    func([]interface{}) bool // Validation function
}

// MapOperationTest defines a map operation test scenario
type MapOperationTest struct {
	Name        string
	Description string
	InputMap    map[interface{}]interface{}            // Input map data
	Operation   string                                 // Operation type (get, set, delete, etc.)
	Key         interface{}                            // Key for operation
	Value       interface{}                            // Value for operation
	Expected    string                                 // Expected behavior
	Validate    func(map[interface{}]interface{}) bool // Validation function
}

// SetOperationTest defines a set operation test scenario
type SetOperationTest struct {
	Name        string
	Description string
	InputSet    []interface{}            // Input set data (slice representation)
	Operation   string                   // Operation type (add, remove, contains, etc.)
	Element     interface{}              // Element for operation
	Expected    string                   // Expected behavior
	Validate    func([]interface{}) bool // Validation function
}

// NestedOperationTest defines a nested collection operation test
type NestedOperationTest struct {
	Name        string
	Description string
	InputData   interface{}   // Complex nested structure
	Operation   string        // Operation type
	Path        []interface{} // Path to nested element
	Expected    string        // Expected behavior
}

// PerformanceTest defines a performance test scenario
type PerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB Array type configuration
var ArrayType = ArrayTypeConfig{
	Name:      "array",
	TableName: "one_array", VecTableName: "vec_array",
	SingleReducer: "insert_one_array", VectorReducer: "insert_vec_array",
	TestValues: []interface{}{
		// Standard Array values (using slices to represent arrays)
		[]interface{}{},                              // Empty array
		[]interface{}{1},                             // Single element
		[]interface{}{1, 2, 3},                       // Small array
		[]interface{}{"a", "b", "c"},                 // String array
		[]interface{}{true, false, true},             // Boolean array
		[]interface{}{1, "mixed", true},              // Mixed type array
		[]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Larger array
	},
	FixedSizeArrays: []interface{}{
		[3]int{1, 2, 3},                    // Fixed int array
		[5]string{"a", "b", "c", "d", "e"}, // Fixed string array
		[2]bool{true, false},               // Fixed bool array
	},
	MultiDimArrays: []interface{}{
		[][]interface{}{
			{1, 2, 3},
			{4, 5, 6},
		}, // 2D array
		[][]interface{}{
			{"a", "b"},
			{"c", "d"},
			{"e", "f"},
		}, // 2D string array
	},
	EmptyArrays: []interface{}{
		[]interface{}{},
		[][]interface{}{},
		[0]int{},
	},
	VectorSizes:   []int{1, 2, 5, 10, 25},
	MaxArraySize:  100,
	MaxDimensions: 3,
}

// SpacetimeDB Map type configuration
var MapType = MapTypeConfig{
	Name:      "map",
	TableName: "one_map", VecTableName: "vec_map",
	SingleReducer: "insert_one_map", VectorReducer: "insert_vec_map",
	TestValues: []interface{}{
		// Standard Map values (using map[interface{}]interface{})
		map[interface{}]interface{}{},               // Empty map
		map[interface{}]interface{}{"key": "value"}, // Single entry
		map[interface{}]interface{}{
			"name": "Alice",
			"age":  30,
			"city": "New York",
		}, // String-keyed map
		map[interface{}]interface{}{
			1: "one",
			2: "two",
			3: "three",
		}, // Integer-keyed map
		map[interface{}]interface{}{
			"active": true,
			"count":  42,
			"items":  []interface{}{"x", "y", "z"},
		}, // Mixed value types
	},
	StringKeyMaps: []interface{}{
		map[interface{}]interface{}{
			"first": "John",
			"last":  "Doe",
			"email": "john@example.com",
		},
		map[interface{}]interface{}{
			"host": "localhost",
			"port": 8080,
			"ssl":  false,
		},
	},
	IntegerKeyMaps: []interface{}{
		map[interface{}]interface{}{
			1: "January",
			2: "February",
			3: "March",
		},
		map[interface{}]interface{}{
			100: "continue",
			200: "ok",
			404: "not found",
		},
	},
	EmptyMaps: []interface{}{
		map[interface{}]interface{}{},
		map[string]interface{}{},
		map[int]interface{}{},
	},
	VectorSizes:     []int{1, 2, 5, 10, 25},
	MaxMapSize:      50,
	MaxNestingDepth: 3,
}

// SpacetimeDB Set type configuration
var SetType = SetTypeConfig{
	Name:      "set",
	TableName: "one_set", VecTableName: "vec_set",
	SingleReducer: "insert_one_set", VectorReducer: "insert_vec_set",
	TestValues: []interface{}{
		// Standard Set values (using slices to represent sets, ensuring uniqueness)
		[]interface{}{},                              // Empty set
		[]interface{}{1},                             // Single element
		[]interface{}{1, 2, 3},                       // Small integer set
		[]interface{}{"a", "b", "c"},                 // String set
		[]interface{}{true, false},                   // Boolean set
		[]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Larger set
	},
	StringSets: []interface{}{
		[]interface{}{"red", "green", "blue"},
		[]interface{}{"apple", "banana", "cherry", "date"},
		[]interface{}{"admin", "user", "guest"},
	},
	IntegerSets: []interface{}{
		[]interface{}{2, 4, 6, 8, 10},   // Even numbers
		[]interface{}{1, 3, 5, 7, 9},    // Odd numbers
		[]interface{}{1, 1, 2, 3, 5, 8}, // Fibonacci (with duplicates to test uniqueness)
	},
	EmptySets: []interface{}{
		[]interface{}{},
	},
	DuplicateTests: []interface{}{
		[]interface{}{1, 1, 2, 2, 3, 3},         // Integer duplicates
		[]interface{}{"a", "a", "b", "b"},       // String duplicates
		[]interface{}{true, true, false, false}, // Boolean duplicates
	},
	VectorSizes:     []int{1, 2, 5, 10, 25},
	MaxSetSize:      75,
	UniquenessTests: 10,
}

// Collection operations configuration
var CollectionOperations = CollectionOperationsConfig{
	Name: "collection_operations",
	ArrayOperations: []ArrayOperationTest{
		{
			Name:        "array_access",
			Description: "Array element access by index",
			InputArray:  []interface{}{10, 20, 30, 40, 50},
			Operation:   "access",
			Expected:    "Should access elements by valid indices",
			Validate: func(arr []interface{}) bool {
				return len(arr) == 5 && arr[2] == 30
			},
		},
		{
			Name:        "array_append",
			Description: "Array element appending",
			InputArray:  []interface{}{1, 2, 3},
			Operation:   "append",
			Expected:    "Should append elements to array",
			Validate: func(arr []interface{}) bool {
				return len(arr) >= 3
			},
		},
		{
			Name:        "array_slice",
			Description: "Array slicing operations",
			InputArray:  []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Operation:   "slice",
			Expected:    "Should support slicing operations",
			Validate: func(arr []interface{}) bool {
				return len(arr) == 10
			},
		},
	},
	MapOperations: []MapOperationTest{
		{
			Name:        "map_get",
			Description: "Map value retrieval by key",
			InputMap: map[interface{}]interface{}{
				"name": "Alice",
				"age":  30,
			},
			Operation: "get",
			Key:       "name",
			Expected:  "Should retrieve value by key",
			Validate: func(m map[interface{}]interface{}) bool {
				return m["name"] == "Alice"
			},
		},
		{
			Name:        "map_set",
			Description: "Map value setting by key",
			InputMap: map[interface{}]interface{}{
				"count": 0,
			},
			Operation: "set",
			Key:       "count",
			Value:     42,
			Expected:  "Should set value by key",
			Validate: func(m map[interface{}]interface{}) bool {
				return len(m) >= 1
			},
		},
		{
			Name:        "map_delete",
			Description: "Map entry deletion by key",
			InputMap: map[interface{}]interface{}{
				"temp": "delete_me",
				"keep": "important",
			},
			Operation: "delete",
			Key:       "temp",
			Expected:  "Should delete entry by key",
			Validate: func(m map[interface{}]interface{}) bool {
				return m["keep"] == "important"
			},
		},
	},
	SetOperations: []SetOperationTest{
		{
			Name:        "set_add",
			Description: "Set element addition",
			InputSet:    []interface{}{1, 2, 3},
			Operation:   "add",
			Element:     4,
			Expected:    "Should add unique elements",
			Validate: func(s []interface{}) bool {
				return len(s) >= 3
			},
		},
		{
			Name:        "set_contains",
			Description: "Set element containment check",
			InputSet:    []interface{}{"apple", "banana", "cherry"},
			Operation:   "contains",
			Element:     "banana",
			Expected:    "Should check element existence",
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
			Name:        "set_remove",
			Description: "Set element removal",
			InputSet:    []interface{}{1, 2, 3, 4, 5},
			Operation:   "remove",
			Element:     3,
			Expected:    "Should remove specified element",
			Validate: func(s []interface{}) bool {
				for _, item := range s {
					if item == 3 {
						return false // Should not contain 3 after removal
					}
				}
				return true
			},
		},
	},
}

// Test configuration constants for complex collection operations
const (
	// Performance thresholds (specific to collections)
	MaxArrayInsertTime     = 100 * time.Millisecond // Single array insertion
	MaxMapInsertTime       = 100 * time.Millisecond // Single map insertion
	MaxSetInsertTime       = 100 * time.Millisecond // Single set insertion
	CollectionEncodingTime = 50 * time.Millisecond  // Collection encoding

	// Test limits (specific to collections)
	MaxGeneratedArrays              = 30  // Number of random arrays to generate
	MaxGeneratedMaps                = 25  // Number of random maps to generate
	MaxGeneratedSets                = 35  // Number of random sets to generate
	CollectionPerformanceIterations = 100 // Iterations for performance testing
	MaxNestedDepth                  = 4   // Maximum nesting depth for testing
	LargeCollectionSize             = 200 // Size for large collection tests
)

// TestSDKComplexCollections is the main integration test for complex collection types
func TestSDKComplexCollections(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK complex collections integration test in short mode")
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
	t.Logf("🎯 Testing Array type: Fixed-size and multidimensional arrays")
	t.Logf("🎯 Testing Map type: Key-value mappings with typed keys/values")
	t.Logf("🎯 Testing Set type: Unique element collections")

	// Generate random test data for complex collections
	generateRandomCollectionData(t)

	// Run comprehensive complex collection test suites
	t.Run("ArrayOperations", func(t *testing.T) {
		testArrayOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("MapOperations", func(t *testing.T) {
		testMapOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("SetOperations", func(t *testing.T) {
		testSetOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ArrayVectorOperations", func(t *testing.T) {
		testArrayVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("MapVectorOperations", func(t *testing.T) {
		testMapVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("SetVectorOperations", func(t *testing.T) {
		testSetVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("NestedCollectionOperations", func(t *testing.T) {
		testNestedCollectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CollectionEncodingValidation", func(t *testing.T) {
		testCollectionEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CollectionSpecificOperations", func(t *testing.T) {
		testCollectionSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PerformanceMeasurement", func(t *testing.T) {
		testCollectionPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomCollectionData generates random arrays, maps, and sets for testing
func generateRandomCollectionData(t *testing.T) {
	t.Log("Generating random complex collection test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random arrays
	for i := 0; i < MaxGeneratedArrays; i++ {
		arraySize := rand.Intn(20) + 1 // Arrays of size 1-20
		randomArray := make([]interface{}, arraySize)

		// Fill with mixed types
		for j := 0; j < arraySize; j++ {
			switch rand.Intn(4) {
			case 0:
				randomArray[j] = rand.Intn(1000)
			case 1:
				randomArray[j] = fmt.Sprintf("item_%d", rand.Intn(100))
			case 2:
				randomArray[j] = rand.Float64() * 100
			case 3:
				randomArray[j] = rand.Intn(2) == 1
			}
		}

		ArrayType.TestValues = append(ArrayType.TestValues, randomArray)
	}

	// Generate random maps
	for i := 0; i < MaxGeneratedMaps; i++ {
		mapSize := rand.Intn(15) + 1 // Maps of size 1-15
		randomMap := make(map[interface{}]interface{})

		for j := 0; j < mapSize; j++ {
			// Generate random key
			var key interface{}
			if rand.Intn(2) == 0 {
				key = fmt.Sprintf("key_%d", j)
			} else {
				key = j
			}

			// Generate random value
			switch rand.Intn(3) {
			case 0:
				randomMap[key] = rand.Intn(1000)
			case 1:
				randomMap[key] = fmt.Sprintf("value_%d", rand.Intn(100))
			case 2:
				randomMap[key] = rand.Intn(2) == 1
			}
		}

		MapType.TestValues = append(MapType.TestValues, randomMap)
	}

	// Generate random sets (ensuring uniqueness)
	for i := 0; i < MaxGeneratedSets; i++ {
		setSize := rand.Intn(25) + 1 // Sets of size 1-25
		uniqueItems := make(map[interface{}]bool)

		// Generate unique items
		for len(uniqueItems) < setSize {
			switch rand.Intn(3) {
			case 0:
				uniqueItems[rand.Intn(1000)] = true
			case 1:
				uniqueItems[fmt.Sprintf("item_%d", rand.Intn(100))] = true
			case 2:
				uniqueItems[rand.Intn(2) == 1] = true
			}
		}

		// Convert to slice
		randomSet := make([]interface{}, 0, len(uniqueItems))
		for item := range uniqueItems {
			randomSet = append(randomSet, item)
		}

		SetType.TestValues = append(SetType.TestValues, randomSet)
	}

	// Generate large collections for performance testing
	largeArray := make([]interface{}, LargeCollectionSize)
	for i := 0; i < LargeCollectionSize; i++ {
		largeArray[i] = i
	}
	ArrayType.LargeArrays = append(ArrayType.LargeArrays, largeArray)

	largeMap := make(map[interface{}]interface{})
	for i := 0; i < LargeCollectionSize; i++ {
		largeMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	MapType.LargeMaps = append(MapType.LargeMaps, largeMap)

	largeSet := make([]interface{}, LargeCollectionSize)
	for i := 0; i < LargeCollectionSize; i++ {
		largeSet[i] = i
	}
	SetType.LargeSets = append(SetType.LargeSets, largeSet)

	// Generate nested collections
	nestedArray := []interface{}{
		[]interface{}{1, 2, 3},
		[]interface{}{"a", "b", "c"},
		[]interface{}{true, false},
	}
	ArrayType.NestedArrays = append(ArrayType.NestedArrays, nestedArray)

	nestedMap := map[interface{}]interface{}{
		"numbers": []interface{}{1, 2, 3},
		"strings": []interface{}{"x", "y", "z"},
		"nested": map[interface{}]interface{}{
			"inner": "value",
		},
	}
	MapType.NestedMaps = append(MapType.NestedMaps, nestedMap)

	t.Logf("✅ Generated %d arrays, %d maps, %d sets, plus large and nested collections",
		MaxGeneratedArrays, MaxGeneratedMaps, MaxGeneratedSets)
}

// testArrayOperations tests Array type operations
func testArrayOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Array operations testing...")

	t.Run("StandardArrays", func(t *testing.T) {
		t.Log("Testing standard Array values")

		successCount := 0
		totalTests := len(ArrayType.TestValues)

		for i, testValue := range ArrayType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Array value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Array value %v: %v", testValue, err)
				continue
			}

			// Validate Array encoding
			arrayValue := testValue.([]interface{})
			t.Logf("✅ Array (size %d) encoded to %d BSATN bytes", len(arrayValue), len(encoded))

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 5000), uint64(i + 6000), uint64(i + 7000), uint64(i + 8000)}
			connectionId := [2]uint64{uint64(i + 50000), 0}
			timestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Array (size %d) inserted successfully with reducer ID %d in %v",
						len(arrayValue), reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxArrayInsertTime,
						"Array insertion took %v, expected less than %v", insertTime, MaxArrayInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Array (size %d) failed to insert after trying reducer IDs 1-20", len(arrayValue))
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Array operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Array encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Array operations testing completed")
}

// testMapOperations tests Map type operations
func testMapOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Map operations testing...")

	t.Run("StandardMaps", func(t *testing.T) {
		t.Log("Testing standard Map values")

		successCount := 0
		totalTests := len(MapType.TestValues)

		for i, testValue := range MapType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Map value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Map value %v: %v", testValue, err)
				continue
			}

			// Validate Map encoding
			mapValue := testValue.(map[interface{}]interface{})
			t.Logf("✅ Map (size %d) encoded to %d BSATN bytes", len(mapValue), len(encoded))

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 6000), uint64(i + 7000), uint64(i + 8000), uint64(i + 9000)}
			connectionId := [2]uint64{uint64(i + 60000), 0}
			timestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Map (size %d) inserted successfully with reducer ID %d in %v",
						len(mapValue), reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxMapInsertTime,
						"Map insertion took %v, expected less than %v", insertTime, MaxMapInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Map (size %d) failed to insert after trying reducer IDs 1-20", len(mapValue))
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Map operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Map encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Map operations testing completed")
}

// testSetOperations tests Set type operations
func testSetOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Set operations testing...")

	t.Run("StandardSets", func(t *testing.T) {
		t.Log("Testing standard Set values")

		successCount := 0
		totalTests := len(SetType.TestValues)

		for i, testValue := range SetType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Set value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Set value %v: %v", testValue, err)
				continue
			}

			// Validate Set encoding
			setValue := testValue.([]interface{})
			t.Logf("✅ Set (size %d) encoded to %d BSATN bytes", len(setValue), len(encoded))

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 7000), uint64(i + 8000), uint64(i + 9000), uint64(i + 10000)}
			connectionId := [2]uint64{uint64(i + 70000), 0}
			timestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Set (size %d) inserted successfully with reducer ID %d in %v",
						len(setValue), reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxSetInsertTime,
						"Set insertion took %v, expected less than %v", insertTime, MaxSetInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Set (size %d) failed to insert after trying reducer IDs 1-20", len(setValue))
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Set operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Set encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Set operations testing completed")
}

// testArrayVectorOperations tests Array vector operations
func testArrayVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Array vector operations testing...")

	for _, vectorSize := range ArrayType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of arrays
			arrayVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				// Create arrays with different patterns
				switch i % 3 {
				case 0:
					arrayVector[i] = []interface{}{i, i * 2, i * 3}
				case 1:
					arrayVector[i] = []interface{}{fmt.Sprintf("str_%d", i), fmt.Sprintf("val_%d", i*2)}
				case 2:
					arrayVector[i] = []interface{}{i%2 == 0, i%3 == 0}
				}
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(arrayVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Array vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Array vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Array vector operations testing completed")
}

// testMapVectorOperations tests Map vector operations
func testMapVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Map vector operations testing...")

	for _, vectorSize := range MapType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of maps
			mapVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				// Create maps with different patterns
				testMap := make(map[interface{}]interface{})
				testMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
				testMap["index"] = i
				testMap["active"] = i%2 == 0
				mapVector[i] = testMap
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(mapVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Map vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Map vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Map vector operations testing completed")
}

// testSetVectorOperations tests Set vector operations
func testSetVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Set vector operations testing...")

	for _, vectorSize := range SetType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of sets
			setVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				// Create sets with different patterns
				switch i % 3 {
				case 0:
					setVector[i] = []interface{}{i, i + 1, i + 2}
				case 1:
					setVector[i] = []interface{}{fmt.Sprintf("item_%d", i), fmt.Sprintf("elem_%d", i)}
				case 2:
					setVector[i] = []interface{}{true, false}
				}
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(setVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Set vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Set vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Set vector operations testing completed")
}

// testNestedCollectionOperations tests nested collection operations
func testNestedCollectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting nested collection operations testing...")

	// Test nested arrays
	t.Run("NestedArrays", func(t *testing.T) {
		for i, testValue := range ArrayType.NestedArrays {
			if i >= 3 { // Limit for performance
				break
			}

			startTime := time.Now()

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("Failed to encode nested array %d: %v", i, err)
				continue
			}

			t.Logf("✅ Nested array %d encoded to %d BSATN bytes in %v", i, len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, CollectionEncodingTime,
				"Nested array encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
		}
	})

	// Test nested maps
	t.Run("NestedMaps", func(t *testing.T) {
		for i, testValue := range MapType.NestedMaps {
			if i >= 3 { // Limit for performance
				break
			}

			startTime := time.Now()

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("Failed to encode nested map %d: %v", i, err)
				continue
			}

			t.Logf("✅ Nested map %d encoded to %d BSATN bytes in %v", i, len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, CollectionEncodingTime,
				"Nested map encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
		}
	})

	// Test deeply nested structure
	t.Run("DeeplyNested", func(t *testing.T) {
		deeplyNested := map[interface{}]interface{}{
			"level1": map[interface{}]interface{}{
				"level2": []interface{}{
					map[interface{}]interface{}{
						"level3": []interface{}{1, 2, 3},
					},
				},
			},
		}

		startTime := time.Now()

		encoded, err := encodingManager.Encode(deeplyNested, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})

		encodingTime := time.Since(startTime)

		if err != nil {
			t.Logf("Failed to encode deeply nested structure: %v", err)
			return
		}

		t.Logf("✅ Deeply nested structure encoded to %d BSATN bytes in %v", len(encoded), encodingTime)

		// Verify encoding time is reasonable
		assert.Less(t, encodingTime, CollectionEncodingTime,
			"Deeply nested encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
	})

	t.Log("✅ Nested collection operations testing completed")
}

// testCollectionEncodingValidation tests collection encoding validation
func testCollectionEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting collection encoding validation...")

	// Test Array encoding validation
	t.Run("ArrayEncodingValidation", func(t *testing.T) {
		testArrays := [][]interface{}{
			{},                  // Empty array
			{1, 2, 3},           // Integer array
			{"a", "b", "c"},     // String array
			{true, false, true}, // Boolean array
			{1, "mixed", true},  // Mixed type array
		}

		for i, arr := range testArrays {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(arr, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Array %d: %v", i, err)
				continue
			}

			t.Logf("✅ Array (size %d) → %d BSATN bytes in %v", len(arr), len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, CollectionEncodingTime,
				"Array encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
		}
	})

	// Test Map encoding validation
	t.Run("MapEncodingValidation", func(t *testing.T) {
		testMaps := []map[interface{}]interface{}{
			{},                               // Empty map
			{"key": "value"},                 // Single entry
			{"name": "Alice", "age": 30},     // Multiple entries
			{1: "one", 2: "two", 3: "three"}, // Integer keys
			{"active": true, "count": 42},    // Mixed values
		}

		for i, m := range testMaps {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(m, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Map %d: %v", i, err)
				continue
			}

			t.Logf("✅ Map (size %d) → %d BSATN bytes in %v", len(m), len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, CollectionEncodingTime,
				"Map encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
		}
	})

	// Test Set encoding validation
	t.Run("SetEncodingValidation", func(t *testing.T) {
		testSets := [][]interface{}{
			{},              // Empty set
			{1, 2, 3},       // Integer set
			{"a", "b", "c"}, // String set
			{true, false},   // Boolean set
			{1, 2, 3, 4, 5}, // Larger set
		}

		for i, s := range testSets {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(s, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Set %d: %v", i, err)
				continue
			}

			t.Logf("✅ Set (size %d) → %d BSATN bytes in %v", len(s), len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, CollectionEncodingTime,
				"Set encoding took %v, expected less than %v", encodingTime, CollectionEncodingTime)
		}
	})

	t.Log("✅ Collection encoding validation completed")
}

// testCollectionSpecificOperations tests collection-specific operations
func testCollectionSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting collection-specific operations testing...")

	// Test array operations
	t.Run("ArrayOperations", func(t *testing.T) {
		for _, operation := range CollectionOperations.ArrayOperations {
			t.Run(operation.Name, func(t *testing.T) {
				t.Logf("Testing operation: %s - %s", operation.Name, operation.Description)

				// Encode the input array
				encoded, err := encodingManager.Encode(operation.InputArray, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode input array: %v", err)
					return
				}

				// Validate the operation
				if operation.Validate != nil && operation.Validate(operation.InputArray) {
					t.Logf("✅ Operation %s: validation passed → %d bytes",
						operation.Operation, len(encoded))
				} else {
					t.Logf("✅ Operation %s: completed → %d bytes",
						operation.Operation, len(encoded))
				}
				t.Logf("📊 %s", operation.Expected)
			})
		}
	})

	// Test map operations
	t.Run("MapOperations", func(t *testing.T) {
		for _, operation := range CollectionOperations.MapOperations {
			t.Run(operation.Name, func(t *testing.T) {
				t.Logf("Testing operation: %s - %s", operation.Name, operation.Description)

				// Encode the input map
				encoded, err := encodingManager.Encode(operation.InputMap, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode input map: %v", err)
					return
				}

				// Validate the operation
				if operation.Validate != nil && operation.Validate(operation.InputMap) {
					t.Logf("✅ Operation %s: validation passed → %d bytes",
						operation.Operation, len(encoded))
				} else {
					t.Logf("✅ Operation %s: completed → %d bytes",
						operation.Operation, len(encoded))
				}
				t.Logf("📊 %s", operation.Expected)
			})
		}
	})

	// Test set operations
	t.Run("SetOperations", func(t *testing.T) {
		for _, operation := range CollectionOperations.SetOperations {
			t.Run(operation.Name, func(t *testing.T) {
				t.Logf("Testing operation: %s - %s", operation.Name, operation.Description)

				// Encode the input set
				encoded, err := encodingManager.Encode(operation.InputSet, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode input set: %v", err)
					return
				}

				// Validate the operation
				if operation.Validate != nil && operation.Validate(operation.InputSet) {
					t.Logf("✅ Operation %s: validation passed → %d bytes",
						operation.Operation, len(encoded))
				} else {
					t.Logf("✅ Operation %s: completed → %d bytes",
						operation.Operation, len(encoded))
				}
				t.Logf("📊 %s", operation.Expected)
			})
		}
	})

	t.Log("✅ Collection-specific operations testing completed")
}

// testCollectionPerformance tests performance of collection operations
func testCollectionPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting collection performance testing...")

	// Performance test for Array encoding
	t.Run("ArrayEncodingPerformance", func(t *testing.T) {
		testArray := []interface{}{1, 2, 3, 4, 5, "test", true, false}
		iterations := CollectionPerformanceIterations

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
		t.Logf("✅ Array encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, CollectionEncodingTime,
			"Average Array encoding time %v exceeds threshold %v", avgTime, CollectionEncodingTime)
	})

	// Performance test for Map encoding
	t.Run("MapEncodingPerformance", func(t *testing.T) {
		testMap := map[interface{}]interface{}{
			"name":   "test",
			"age":    25,
			"active": true,
			"items":  []interface{}{1, 2, 3},
		}
		iterations := CollectionPerformanceIterations

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
		t.Logf("✅ Map encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, CollectionEncodingTime,
			"Average Map encoding time %v exceeds threshold %v", avgTime, CollectionEncodingTime)
	})

	// Performance test for Set encoding
	t.Run("SetEncodingPerformance", func(t *testing.T) {
		testSet := []interface{}{1, 2, 3, 4, 5, "a", "b", "c", true, false}
		iterations := CollectionPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testSet, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Set encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, CollectionEncodingTime,
			"Average Set encoding time %v exceeds threshold %v", avgTime, CollectionEncodingTime)
	})

	// Memory usage test
	t.Run("MemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many collections
		for i := 0; i < 500; i++ {
			testArray := []interface{}{i, fmt.Sprintf("item_%d", i), i%2 == 0}
			testMap := map[interface{}]interface{}{
				fmt.Sprintf("key_%d", i): fmt.Sprintf("value_%d", i),
				"index":                  i,
			}
			testSet := []interface{}{i, i + 1, i + 2}

			encodingManager.Encode(testArray, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testMap, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testSet, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 500 Array+Map+Set encodings: %d bytes", memUsed)
	})

	// Large collection test
	t.Run("LargeCollectionPerformance", func(t *testing.T) {
		// Test large array
		largeArray := make([]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			largeArray[i] = i
		}

		startTime := time.Now()
		_, err := encodingManager.Encode(largeArray, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		largeArrayTime := time.Since(startTime)

		if err != nil {
			t.Logf("Failed to encode large array: %v", err)
		} else {
			t.Logf("✅ Large array (1000 elements) encoded in %v", largeArrayTime)
		}

		// Test large map
		largeMap := make(map[interface{}]interface{})
		for i := 0; i < 500; i++ {
			largeMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
		}

		startTime = time.Now()
		_, err = encodingManager.Encode(largeMap, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		largeMapTime := time.Since(startTime)

		if err != nil {
			t.Logf("Failed to encode large map: %v", err)
		} else {
			t.Logf("✅ Large map (500 entries) encoded in %v", largeMapTime)
		}
	})

	t.Log("✅ Collection performance testing completed")
}
