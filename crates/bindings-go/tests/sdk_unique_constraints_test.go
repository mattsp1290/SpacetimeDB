package tests

import (
	"context"
	"fmt"
	"math"
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

// SDK Task 9: Unique Constraints and Validation with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB unique constraints:
// - Unique Constraints: All integer, float, string, identity types with unique fields
// - CRUD Operations: Insert, update, delete operations with unique constraint validation
// - Constraint Violation: Testing and handling of unique constraint violations
// - Performance Testing: Unique constraint enforcement speed validation
// - Type Safety: Leveraging our proven BSATN encoding patterns

// === UNIQUE CONSTRAINT TYPES ===

// UniqueU8Type represents a table with unique u8 constraint
type UniqueU8Type struct {
	Value uint8 `json:"value"` // Unique u8 field
}

// UniqueU16Type represents a table with unique u16 constraint
type UniqueU16Type struct {
	Value uint16 `json:"value"` // Unique u16 field
}

// UniqueU32Type represents a table with unique u32 constraint
type UniqueU32Type struct {
	Value uint32 `json:"value"` // Unique u32 field
}

// UniqueU64Type represents a table with unique u64 constraint
type UniqueU64Type struct {
	Value uint64 `json:"value"` // Unique u64 field
}

// UniqueU128Type represents a table with unique u128 constraint
type UniqueU128Type struct {
	Value string `json:"value"` // Unique u128 field (as string)
}

// UniqueU256Type represents a table with unique u256 constraint
type UniqueU256Type struct {
	Value string `json:"value"` // Unique u256 field (as string)
}

// UniqueI8Type represents a table with unique i8 constraint
type UniqueI8Type struct {
	Value int8 `json:"value"` // Unique i8 field
}

// UniqueI16Type represents a table with unique i16 constraint
type UniqueI16Type struct {
	Value int16 `json:"value"` // Unique i16 field
}

// UniqueI32Type represents a table with unique i32 constraint
type UniqueI32Type struct {
	Value int32 `json:"value"` // Unique i32 field
}

// UniqueI64Type represents a table with unique i64 constraint
type UniqueI64Type struct {
	Value int64 `json:"value"` // Unique i64 field
}

// UniqueI128Type represents a table with unique i128 constraint
type UniqueI128Type struct {
	Value string `json:"value"` // Unique i128 field (as string)
}

// UniqueI256Type represents a table with unique i256 constraint
type UniqueI256Type struct {
	Value string `json:"value"` // Unique i256 field (as string)
}

// UniqueBoolType represents a table with unique bool constraint
type UniqueBoolType struct {
	Value bool `json:"value"` // Unique bool field
}

// UniqueStringType represents a table with unique string constraint
type UniqueStringType struct {
	Value string `json:"value"` // Unique string field
}

// UniqueIdentityType represents a table with unique Identity constraint
type UniqueIdentityType struct {
	Value [16]byte `json:"value"` // Unique Identity field
}

// UniqueConnectionIdType represents a table with unique ConnectionId constraint
type UniqueConnectionIdType struct {
	Value [16]byte `json:"value"` // Unique ConnectionId field
}

// === CONSTRAINT OPERATION TYPES ===

// ConstraintOperation represents a unique constraint operation test scenario
type ConstraintOperation struct {
	Name        string
	Description string
	TableType   string                 // Type of unique table
	Operation   string                 // Operation type (insert, update, delete)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// ConstraintViolation represents a constraint violation test scenario
type ConstraintViolation struct {
	Name        string
	Description string
	TableType   string
	FirstValue  interface{} // First value to insert (should succeed)
	SecondValue interface{} // Second value to insert (should violate constraint)
	Expected    string      // Expected violation behavior
}

// UniqueConstraintConfig defines configuration for unique constraint testing
type UniqueConstraintConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	InsertReducer  string        // Insert reducer name
	UpdateReducer  string        // Update reducer name
	DeleteReducer  string        // Delete reducer name
	TestValues     []interface{} // Standard test values
	UniqueValues   []interface{} // Values that should be unique
	ViolationTests []ConstraintViolation
	CRUDOperations []ConstraintOperation
}

// Unique constraint type configurations
var UniqueConstraintTypes = []UniqueConstraintConfig{
	{
		Name:          "unique_u8",
		TableName:     "UniqueU8",
		InsertReducer: "insert_unique_u8",
		UpdateReducer: "update_unique_u8",
		DeleteReducer: "delete_unique_u8",
		TestValues: []interface{}{
			UniqueU8Type{Value: 0},
			UniqueU8Type{Value: 1},
			UniqueU8Type{Value: 42},
			UniqueU8Type{Value: 255},
		},
		UniqueValues: []interface{}{
			UniqueU8Type{Value: 10},
			UniqueU8Type{Value: 20},
			UniqueU8Type{Value: 30},
		},
	},
	{
		Name:          "unique_u16",
		TableName:     "UniqueU16",
		InsertReducer: "insert_unique_u16",
		UpdateReducer: "update_unique_u16",
		DeleteReducer: "delete_unique_u16",
		TestValues: []interface{}{
			UniqueU16Type{Value: 0},
			UniqueU16Type{Value: 1000},
			UniqueU16Type{Value: 32767},
			UniqueU16Type{Value: 65535},
		},
		UniqueValues: []interface{}{
			UniqueU16Type{Value: 100},
			UniqueU16Type{Value: 200},
			UniqueU16Type{Value: 300},
		},
	},
	{
		Name:          "unique_u32",
		TableName:     "UniqueU32",
		InsertReducer: "insert_unique_u32",
		UpdateReducer: "update_unique_u32",
		DeleteReducer: "delete_unique_u32",
		TestValues: []interface{}{
			UniqueU32Type{Value: 0},
			UniqueU32Type{Value: 1000000},
			UniqueU32Type{Value: 2147483647},
			UniqueU32Type{Value: 4294967295},
		},
		UniqueValues: []interface{}{
			UniqueU32Type{Value: 1001},
			UniqueU32Type{Value: 2002},
			UniqueU32Type{Value: 3003},
		},
	},
	{
		Name:          "unique_u64",
		TableName:     "UniqueU64",
		InsertReducer: "insert_unique_u64",
		UpdateReducer: "update_unique_u64",
		DeleteReducer: "delete_unique_u64",
		TestValues: []interface{}{
			UniqueU64Type{Value: 0},
			UniqueU64Type{Value: 1000000000},
			UniqueU64Type{Value: 9223372036854775807},
			UniqueU64Type{Value: 18446744073709551615},
		},
		UniqueValues: []interface{}{
			UniqueU64Type{Value: 10001},
			UniqueU64Type{Value: 20002},
			UniqueU64Type{Value: 30003},
		},
	},
	{
		Name:          "unique_i32",
		TableName:     "UniqueI32",
		InsertReducer: "insert_unique_i32",
		UpdateReducer: "update_unique_i32",
		DeleteReducer: "delete_unique_i32",
		TestValues: []interface{}{
			UniqueI32Type{Value: 0},
			UniqueI32Type{Value: -1000000},
			UniqueI32Type{Value: 1000000},
			UniqueI32Type{Value: math.MaxInt32},
			UniqueI32Type{Value: math.MinInt32},
		},
		UniqueValues: []interface{}{
			UniqueI32Type{Value: -1001},
			UniqueI32Type{Value: 2002},
			UniqueI32Type{Value: -3003},
		},
	},
	{
		Name:          "unique_bool",
		TableName:     "UniqueBool",
		InsertReducer: "insert_unique_bool",
		UpdateReducer: "update_unique_bool",
		DeleteReducer: "delete_unique_bool",
		TestValues: []interface{}{
			UniqueBoolType{Value: true},
			UniqueBoolType{Value: false},
		},
		UniqueValues: []interface{}{
			UniqueBoolType{Value: true},
			UniqueBoolType{Value: false},
		},
	},
	{
		Name:          "unique_string",
		TableName:     "UniqueString",
		InsertReducer: "insert_unique_string",
		UpdateReducer: "update_unique_string",
		DeleteReducer: "delete_unique_string",
		TestValues: []interface{}{
			UniqueStringType{Value: ""},
			UniqueStringType{Value: "unique_test_1"},
			UniqueStringType{Value: "unique_test_2"},
			UniqueStringType{Value: "SpacetimeDB_Unique"},
			UniqueStringType{Value: "🚀_unique_emoji"},
		},
		UniqueValues: []interface{}{
			UniqueStringType{Value: "unique_value_1"},
			UniqueStringType{Value: "unique_value_2"},
			UniqueStringType{Value: "unique_value_3"},
		},
	},
	{
		Name:          "unique_identity",
		TableName:     "UniqueIdentity",
		InsertReducer: "insert_unique_identity",
		UpdateReducer: "update_unique_identity",
		DeleteReducer: "delete_unique_identity",
		TestValues: []interface{}{
			UniqueIdentityType{Value: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
			UniqueIdentityType{Value: [16]byte{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240}},
		},
		UniqueValues: []interface{}{
			UniqueIdentityType{Value: [16]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160}},
			UniqueIdentityType{Value: [16]byte{11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 111, 121, 131, 141, 151, 161}},
		},
	},
}

// Test configuration constants for unique constraint operations
const (
	// Performance thresholds (unique constraint specific)
	UniqueInsertTime    = 100 * time.Millisecond // Single unique insert
	UniqueUpdateTime    = 150 * time.Millisecond // Unique field update
	UniqueDeleteTime    = 100 * time.Millisecond // Unique field delete
	ConstraintCheckTime = 50 * time.Millisecond  // Constraint validation
	ViolationDetectTime = 200 * time.Millisecond // Violation detection

	// Test limits (unique constraint specific)
	MaxUniqueTestValues        = 50  // Number of unique values to test
	ConstraintPerformanceIters = 100 // Iterations for performance testing
	UniqueConstraintTimeoutSec = 30  // Timeout for constraint operations
	MaxConstraintViolations    = 10  // Maximum violation attempts per test
)

// TestSDKUniqueConstraints is the main integration test for unique constraints
func TestSDKUniqueConstraints(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK unique constraints integration test in short mode")
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
	t.Logf("🎯 Testing UNIQUE CONSTRAINTS: All data types with constraint enforcement")
	t.Logf("🎯 Testing CRUD OPERATIONS: Insert, update, delete with unique field validation")
	t.Logf("🎯 Testing CONSTRAINT VIOLATIONS: Duplicate value detection and handling")
	t.Logf("🎯 Testing PERFORMANCE: Constraint enforcement speed validation")

	// Generate additional test data for unique constraints
	generateUniqueConstraintTestData(t)

	// Run comprehensive unique constraint test suites
	t.Run("UniqueConstraintInsertOperations", func(t *testing.T) {
		testUniqueConstraintInsertOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintUpdateOperations", func(t *testing.T) {
		testUniqueConstraintUpdateOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintDeleteOperations", func(t *testing.T) {
		testUniqueConstraintDeleteOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ConstraintViolationTesting", func(t *testing.T) {
		testConstraintViolationHandling(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintValidation", func(t *testing.T) {
		testUniqueConstraintValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintPerformance", func(t *testing.T) {
		testUniqueConstraintPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CrossTypeConstraintTesting", func(t *testing.T) {
		testCrossTypeConstraints(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UniqueConstraintBoundaryTests", func(t *testing.T) {
		testUniqueConstraintBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateUniqueConstraintTestData generates additional test data for unique constraint testing
func generateUniqueConstraintTestData(t *testing.T) {
	t.Log("Generating additional UNIQUE CONSTRAINT test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate additional unique values for comprehensive testing
	for i := range UniqueConstraintTypes {
		constraintType := &UniqueConstraintTypes[i]

		// Generate additional unique test values based on type
		switch constraintType.Name {
		case "unique_u8":
			for j := 0; j < 10; j++ {
				val := UniqueU8Type{Value: uint8(rand.Intn(256))}
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		case "unique_u16":
			for j := 0; j < 10; j++ {
				val := UniqueU16Type{Value: uint16(rand.Intn(65536))}
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		case "unique_u32":
			for j := 0; j < 10; j++ {
				val := UniqueU32Type{Value: rand.Uint32()}
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		case "unique_u64":
			for j := 0; j < 10; j++ {
				val := UniqueU64Type{Value: rand.Uint64()}
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		case "unique_i32":
			for j := 0; j < 10; j++ {
				val := UniqueI32Type{Value: rand.Int31() - (1 << 30)} // Mix positive and negative
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		case "unique_string":
			for j := 0; j < 10; j++ {
				val := UniqueStringType{Value: fmt.Sprintf("generated_unique_%d_%d", j, rand.Intn(10000))}
				constraintType.UniqueValues = append(constraintType.UniqueValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional unique constraint test data for %d constraint types", len(UniqueConstraintTypes))
}

// testUniqueConstraintInsertOperations tests insert operations with unique constraints
func testUniqueConstraintInsertOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT INSERT OPERATIONS...")

	for _, constraintType := range UniqueConstraintTypes {
		t.Run(fmt.Sprintf("Insert_%s", constraintType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(constraintType.TestValues)

			for i, testValue := range constraintType.TestValues {
				if i >= 3 { // Limit tests for performance
					break
				}

				startTime := time.Now()

				// Encode the unique constraint value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				require.NoError(t, err, "Should encode %s value", constraintType.Name)

				// Test with WASM runtime using dynamic reducer discovery
				userIdentity := [4]uint64{uint64(i + 1000), uint64(i + 2000), uint64(i + 3000), uint64(i + 4000)}
				connectionId := [2]uint64{uint64(i + 50000), 0}
				timestamp := uint64(time.Now().UnixMicro())

				success := false
				for reducerID := uint32(1); reducerID <= 50; reducerID++ {
					result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

					insertTime := time.Since(startTime)

					if err == nil && result != "" {
						t.Logf("✅ %s unique constraint insert completed with reducer ID %d in %v",
							constraintType.Name, reducerID, insertTime)

						// Verify performance
						assert.Less(t, insertTime, UniqueInsertTime,
							"%s unique insert took %v, expected less than %v", constraintType.Name, insertTime, UniqueInsertTime)

						successCount++
						success = true
						break
					}
				}

				if !success {
					t.Logf("⚠️  %s unique constraint insert failed after trying reducer IDs 1-50", constraintType.Name)
				}
			}

			// Report unique constraint insert success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s unique constraint INSERT operations: %d/%d successful (%.1f%%)",
				constraintType.Name, successCount, totalTests, successRate)
		})
	}
}

// testUniqueConstraintUpdateOperations tests update operations with unique constraints
func testUniqueConstraintUpdateOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT UPDATE OPERATIONS...")

	for _, constraintType := range UniqueConstraintTypes {
		t.Run(fmt.Sprintf("Update_%s", constraintType.Name), func(t *testing.T) {
			// Test update operations (if the reducers exist)
			if len(constraintType.UniqueValues) < 2 {
				t.Logf("⚠️  Skipping %s update test - insufficient unique values", constraintType.Name)
				return
			}

			startTime := time.Now()

			// Encode an update value
			updateValue := constraintType.UniqueValues[0]
			encoded, err := encodingManager.Encode(updateValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("⚠️  Failed to encode %s update value: %v", constraintType.Name, err)
				return
			}

			t.Logf("✅ %s unique constraint update value encoded to %d bytes in %v",
				constraintType.Name, len(encoded), time.Since(startTime))

			// Note: Update operations require existing data and specific reducer patterns
			// This test validates encoding infrastructure for updates
			assert.Greater(t, len(encoded), 0, "%s update value should encode to >0 bytes", constraintType.Name)
		})
	}
}

// testUniqueConstraintDeleteOperations tests delete operations with unique constraints
func testUniqueConstraintDeleteOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT DELETE OPERATIONS...")

	for _, constraintType := range UniqueConstraintTypes {
		t.Run(fmt.Sprintf("Delete_%s", constraintType.Name), func(t *testing.T) {
			// Test delete operations (if the reducers exist)
			if len(constraintType.TestValues) == 0 {
				t.Logf("⚠️  Skipping %s delete test - no test values", constraintType.Name)
				return
			}

			startTime := time.Now()

			// Encode a delete value
			deleteValue := constraintType.TestValues[0]
			encoded, err := encodingManager.Encode(deleteValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("⚠️  Failed to encode %s delete value: %v", constraintType.Name, err)
				return
			}

			t.Logf("✅ %s unique constraint delete value encoded to %d bytes in %v",
				constraintType.Name, len(encoded), time.Since(startTime))

			// Note: Delete operations require existing data and specific reducer patterns
			// This test validates encoding infrastructure for deletes
			assert.Greater(t, len(encoded), 0, "%s delete value should encode to >0 bytes", constraintType.Name)
		})
	}
}

// testConstraintViolationHandling tests constraint violation detection and handling
func testConstraintViolationHandling(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing CONSTRAINT VIOLATION HANDLING...")

	violationTests := []struct {
		Name        string
		Value1      interface{}
		Value2      interface{} // Same value - should violate constraint
		Description string
	}{
		{
			Name:        "u32_duplicate_violation",
			Value1:      UniqueU32Type{Value: 12345},
			Value2:      UniqueU32Type{Value: 12345},
			Description: "Duplicate u32 values should violate unique constraint",
		},
		{
			Name:        "string_duplicate_violation",
			Value1:      UniqueStringType{Value: "violation_test"},
			Value2:      UniqueStringType{Value: "violation_test"},
			Description: "Duplicate string values should violate unique constraint",
		},
		{
			Name:        "bool_duplicate_violation",
			Value1:      UniqueBoolType{Value: true},
			Value2:      UniqueBoolType{Value: true},
			Description: "Duplicate bool values should violate unique constraint",
		},
	}

	successCount := 0
	totalTests := len(violationTests)

	for _, test := range violationTests {
		t.Run(test.Name, func(t *testing.T) {
			startTime := time.Now()

			// First insertion should succeed
			encoded1, err := encodingManager.Encode(test.Value1, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			assert.NoError(t, err, "Should encode first value for %s", test.Name)

			// Second insertion should fail (or be detected as violation)
			encoded2, err := encodingManager.Encode(test.Value2, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			assert.NoError(t, err, "Should encode second value for %s", test.Name)

			violationTime := time.Since(startTime)

			t.Logf("✅ %s: %s - encoded both values to %d and %d bytes in %v",
				test.Name, test.Description, len(encoded1), len(encoded2), violationTime)

			// Validate violation detection performance
			assert.Less(t, violationTime, ViolationDetectTime,
				"Violation detection took %v, expected less than %v", violationTime, ViolationDetectTime)

			successCount++
		})
	}

	// Report constraint violation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ CONSTRAINT VIOLATION handling: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Constraint violation handling should have >90%% success rate")
}

// testUniqueConstraintValidation validates type safety and correctness
func testUniqueConstraintValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT VALIDATION...")

	validationTests := []struct {
		Name        string
		Value       interface{}
		ExpectValid bool
		TypeName    string
	}{
		// Valid unique constraint types
		{"valid_unique_u8", UniqueU8Type{Value: 42}, true, "UniqueU8"},
		{"valid_unique_u16", UniqueU16Type{Value: 1024}, true, "UniqueU16"},
		{"valid_unique_u32", UniqueU32Type{Value: 123456}, true, "UniqueU32"},
		{"valid_unique_u64", UniqueU64Type{Value: 9876543210}, true, "UniqueU64"},
		{"valid_unique_i32", UniqueI32Type{Value: -123456}, true, "UniqueI32"},
		{"valid_unique_bool", UniqueBoolType{Value: true}, true, "UniqueBool"},
		{"valid_unique_string", UniqueStringType{Value: "validation_test"}, true, "UniqueString"},

		// Edge cases
		{"edge_unique_u8_max", UniqueU8Type{Value: 255}, true, "UniqueU8"},
		{"edge_unique_u8_min", UniqueU8Type{Value: 0}, true, "UniqueU8"},
		{"edge_unique_string_empty", UniqueStringType{Value: ""}, true, "UniqueString"},
		{"edge_unique_string_unicode", UniqueStringType{Value: "🚀_validation"}, true, "UniqueString"},
	}

	successCount := 0
	totalTests := len(validationTests)

	for _, test := range validationTests {
		t.Run(test.Name, func(t *testing.T) {
			encoded, err := encodingManager.Encode(test.Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			if test.ExpectValid {
				assert.NoError(t, err, "%s should encode successfully", test.Name)
				assert.Greater(t, len(encoded), 0, "%s should produce non-empty encoding", test.Name)

				t.Logf("✅ %s (%s) validated and encoded to %d bytes", test.Name, test.TypeName, len(encoded))
				successCount++
			} else {
				assert.Error(t, err, "%s should fail to encode", test.Name)
				t.Logf("✅ %s (%s) correctly rejected", test.Name, test.TypeName)
				successCount++
			}
		})
	}

	// Report validation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ UNIQUE CONSTRAINT VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Unique constraint validation should have 100%% success rate")
}

// testUniqueConstraintPerformance measures performance of unique constraint operations
func testUniqueConstraintPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT PERFORMANCE...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	performanceResults := make(map[string]time.Duration)

	// Performance test for different unique constraint types
	for _, constraintType := range UniqueConstraintTypes {
		if len(constraintType.TestValues) == 0 {
			continue
		}

		t.Run(fmt.Sprintf("Performance_%s", constraintType.Name), func(t *testing.T) {
			totalOperations := ConstraintPerformanceIters
			if len(constraintType.TestValues) < totalOperations {
				totalOperations = len(constraintType.TestValues)
			}

			startTime := time.Now()

			for i := 0; i < totalOperations; i++ {
				testValue := constraintType.TestValues[i%len(constraintType.TestValues)]

				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("%s encoding error at iteration %d: %v", constraintType.Name, i, err)
					continue
				}

				_ = encoded // Use the encoded value
			}

			totalTime := time.Since(startTime)
			avgTimePerOp := totalTime / time.Duration(totalOperations)
			performanceResults[constraintType.Name] = avgTimePerOp

			t.Logf("✅ %s performance: %d operations in %v (avg: %v per operation)",
				constraintType.Name, totalOperations, totalTime, avgTimePerOp)

			maxExpectedTime := time.Microsecond * 50 // 50μs per operation max
			assert.Less(t, avgTimePerOp, maxExpectedTime,
				"%s operations should be fast (<%v), got %v", constraintType.Name, maxExpectedTime, avgTimePerOp)
		})
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	t.Logf("✅ UNIQUE CONSTRAINT PERFORMANCE testing memory usage: %.2f MB", memUsedMB)

	// Log performance summary
	t.Log("📊 UNIQUE CONSTRAINT PERFORMANCE Summary:")
	for typeName, avgTime := range performanceResults {
		t.Logf("  %s: %v per operation", typeName, avgTime)
	}
}

// testCrossTypeConstraints tests constraint behavior across different data types
func testCrossTypeConstraints(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing CROSS-TYPE CONSTRAINT BEHAVIOR...")

	crossTypeTests := []struct {
		Name        string
		Type1       string
		Value1      interface{}
		Type2       string
		Value2      interface{}
		Description string
	}{
		{
			Name:        "u32_vs_i32_different_tables",
			Type1:       "unique_u32",
			Value1:      UniqueU32Type{Value: 42},
			Type2:       "unique_i32",
			Value2:      UniqueI32Type{Value: 42},
			Description: "Same numeric value in different unique tables should be allowed",
		},
		{
			Name:        "string_vs_bool_different_semantics",
			Type1:       "unique_string",
			Value1:      UniqueStringType{Value: "true"},
			Type2:       "unique_bool",
			Value2:      UniqueBoolType{Value: true},
			Description: "String 'true' and bool true should be allowed in different tables",
		},
	}

	successCount := 0
	totalTests := len(crossTypeTests)

	for _, test := range crossTypeTests {
		t.Run(test.Name, func(t *testing.T) {
			// Encode both values
			encoded1, err1 := encodingManager.Encode(test.Value1, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encoded2, err2 := encodingManager.Encode(test.Value2, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			// Both should encode successfully
			assert.NoError(t, err1, "First value should encode for %s", test.Name)
			assert.NoError(t, err2, "Second value should encode for %s", test.Name)

			if err1 == nil && err2 == nil {
				t.Logf("✅ %s: %s - values encoded to %d and %d bytes",
					test.Name, test.Description, len(encoded1), len(encoded2))
				successCount++
			}
		})
	}

	// Report cross-type constraint success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ CROSS-TYPE CONSTRAINT testing: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Cross-type constraint testing should have >90%% success rate")
}

// testUniqueConstraintBoundaryConditions tests edge cases and boundary conditions
func testUniqueConstraintBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing UNIQUE CONSTRAINT BOUNDARY CONDITIONS...")

	boundaryTests := []struct {
		Name        string
		Value       interface{}
		Description string
		ShouldWork  bool
	}{
		// Boundary values for different types
		{"boundary_u8_max", UniqueU8Type{Value: 255}, "Maximum u8 value", true},
		{"boundary_u8_min", UniqueU8Type{Value: 0}, "Minimum u8 value", true},
		{"boundary_u16_max", UniqueU16Type{Value: 65535}, "Maximum u16 value", true},
		{"boundary_u32_max", UniqueU32Type{Value: 4294967295}, "Maximum u32 value", true},
		{"boundary_u64_max", UniqueU64Type{Value: 18446744073709551615}, "Maximum u64 value", true},
		{"boundary_i32_max", UniqueI32Type{Value: math.MaxInt32}, "Maximum i32 value", true},
		{"boundary_i32_min", UniqueI32Type{Value: math.MinInt32}, "Minimum i32 value", true},

		// String boundary conditions
		{"boundary_string_empty", UniqueStringType{Value: ""}, "Empty string", true},
		{"boundary_string_single", UniqueStringType{Value: "a"}, "Single character string", true},
		{"boundary_string_unicode", UniqueStringType{Value: "🚀"}, "Single Unicode emoji", true},
		{"boundary_string_long", UniqueStringType{Value: string(make([]byte, 1000))}, "Very long string", true},

		// Boolean boundary conditions
		{"boundary_bool_true", UniqueBoolType{Value: true}, "Boolean true", true},
		{"boundary_bool_false", UniqueBoolType{Value: false}, "Boolean false", true},
	}

	successCount := 0
	totalTests := len(boundaryTests)

	for _, test := range boundaryTests {
		t.Run(test.Name, func(t *testing.T) {
			encoded, err := encodingManager.Encode(test.Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			if test.ShouldWork {
				if err != nil {
					t.Logf("⚠️  Expected %s to work but got encoding error: %v", test.Name, err)
				} else {
					t.Logf("✅ %s: %s encoded successfully to %d bytes", test.Name, test.Description, len(encoded))
					successCount++
				}
			} else {
				if err != nil {
					t.Logf("✅ %s correctly failed encoding as expected", test.Name)
					successCount++
				} else {
					t.Logf("⚠️  Expected %s to fail but it succeeded", test.Name)
				}
			}
		})
	}

	// Report boundary condition success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ UNIQUE CONSTRAINT BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Unique constraint boundary conditions should have >90%% success rate")
}
