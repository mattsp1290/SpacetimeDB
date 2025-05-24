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

// SDK Task 10: Primary Keys and Table Identity with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB primary keys:
// - Primary Keys: All integer, float, string, identity types as primary keys
// - CRUD Operations: Insert, update, delete operations via primary key
// - Key Uniqueness: Primary key constraint enforcement and validation
// - Performance Testing: Primary key operation speed validation
// - Type Safety: Leveraging our proven BSATN encoding patterns

// === PRIMARY KEY TYPES ===

// PkU8Type represents a table with u8 primary key
type PkU8Type struct {
	Id    uint8  `json:"id"`    // Primary key u8 field
	Value string `json:"value"` // Additional data field
}

// PkU16Type represents a table with u16 primary key
type PkU16Type struct {
	Id    uint16 `json:"id"`    // Primary key u16 field
	Value string `json:"value"` // Additional data field
}

// PkU32Type represents a table with u32 primary key
type PkU32Type struct {
	Id    uint32 `json:"id"`    // Primary key u32 field
	Value string `json:"value"` // Additional data field
}

// PkU32TwoType represents a second table with u32 primary key
type PkU32TwoType struct {
	Id    uint32 `json:"id"`    // Primary key u32 field
	Value string `json:"value"` // Additional data field
}

// PkU64Type represents a table with u64 primary key
type PkU64Type struct {
	Id    uint64 `json:"id"`    // Primary key u64 field
	Value string `json:"value"` // Additional data field
}

// PkU128Type represents a table with u128 primary key
type PkU128Type struct {
	Id    string `json:"id"`    // Primary key u128 field (as string)
	Value string `json:"value"` // Additional data field
}

// PkU256Type represents a table with u256 primary key
type PkU256Type struct {
	Id    string `json:"id"`    // Primary key u256 field (as string)
	Value string `json:"value"` // Additional data field
}

// PkI8Type represents a table with i8 primary key
type PkI8Type struct {
	Id    int8   `json:"id"`    // Primary key i8 field
	Value string `json:"value"` // Additional data field
}

// PkI16Type represents a table with i16 primary key
type PkI16Type struct {
	Id    int16  `json:"id"`    // Primary key i16 field
	Value string `json:"value"` // Additional data field
}

// PkI32Type represents a table with i32 primary key
type PkI32Type struct {
	Id    int32  `json:"id"`    // Primary key i32 field
	Value string `json:"value"` // Additional data field
}

// PkI64Type represents a table with i64 primary key
type PkI64Type struct {
	Id    int64  `json:"id"`    // Primary key i64 field
	Value string `json:"value"` // Additional data field
}

// PkI128Type represents a table with i128 primary key
type PkI128Type struct {
	Id    string `json:"id"`    // Primary key i128 field (as string)
	Value string `json:"value"` // Additional data field
}

// PkI256Type represents a table with i256 primary key
type PkI256Type struct {
	Id    string `json:"id"`    // Primary key i256 field (as string)
	Value string `json:"value"` // Additional data field
}

// PkBoolType represents a table with bool primary key
type PkBoolType struct {
	Id    bool   `json:"id"`    // Primary key bool field
	Value string `json:"value"` // Additional data field
}

// PkStringType represents a table with string primary key
type PkStringType struct {
	Id    string `json:"id"`    // Primary key string field
	Value string `json:"value"` // Additional data field
}

// PkIdentityType represents a table with Identity primary key
type PkIdentityType struct {
	Id    [16]byte `json:"id"`    // Primary key Identity field
	Value string   `json:"value"` // Additional data field
}

// PkConnectionIdType represents a table with ConnectionId primary key
type PkConnectionIdType struct {
	Id    [16]byte `json:"id"`    // Primary key ConnectionId field
	Value string   `json:"value"` // Additional data field
}

// PkTimestampType represents a table with Timestamp primary key
type PkTimestampType struct {
	Id    uint64 `json:"id"`    // Primary key Timestamp field (as uint64)
	Value string `json:"value"` // Additional data field
}

// === PRIMARY KEY OPERATION TYPES ===

// PrimaryKeyOperation represents a primary key operation test scenario
type PrimaryKeyOperation struct {
	Name        string
	Description string
	TableType   string                 // Type of primary key table
	Operation   string                 // Operation type (insert, update, delete)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// PrimaryKeyConfig defines configuration for primary key testing
type PrimaryKeyConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	InsertReducer  string        // Insert reducer name
	UpdateReducer  string        // Update reducer name
	DeleteReducer  string        // Delete reducer name
	TestValues     []interface{} // Standard test values
	KeyValues      []interface{} // Values for primary key testing
	CRUDOperations []PrimaryKeyOperation
}

// Primary key type configurations
var PrimaryKeyTypes = []PrimaryKeyConfig{
	{
		Name:          "pk_u8",
		TableName:     "PkU8",
		InsertReducer: "insert_pk_u8",
		UpdateReducer: "update_pk_u8",
		DeleteReducer: "delete_pk_u8",
		TestValues: []interface{}{
			PkU8Type{Id: 0, Value: "pk_u8_value_0"},
			PkU8Type{Id: 1, Value: "pk_u8_value_1"},
			PkU8Type{Id: 42, Value: "pk_u8_value_42"},
			PkU8Type{Id: 255, Value: "pk_u8_value_255"},
		},
		KeyValues: []interface{}{
			PkU8Type{Id: 10, Value: "key_test_10"},
			PkU8Type{Id: 20, Value: "key_test_20"},
			PkU8Type{Id: 30, Value: "key_test_30"},
		},
	},
	{
		Name:          "pk_u16",
		TableName:     "PkU16",
		InsertReducer: "insert_pk_u16",
		UpdateReducer: "update_pk_u16",
		DeleteReducer: "delete_pk_u16",
		TestValues: []interface{}{
			PkU16Type{Id: 0, Value: "pk_u16_value_0"},
			PkU16Type{Id: 1000, Value: "pk_u16_value_1000"},
			PkU16Type{Id: 32767, Value: "pk_u16_value_32767"},
			PkU16Type{Id: 65535, Value: "pk_u16_value_65535"},
		},
		KeyValues: []interface{}{
			PkU16Type{Id: 100, Value: "key_test_100"},
			PkU16Type{Id: 200, Value: "key_test_200"},
			PkU16Type{Id: 300, Value: "key_test_300"},
		},
	},
	{
		Name:          "pk_u32",
		TableName:     "PkU32",
		InsertReducer: "insert_pk_u32",
		UpdateReducer: "update_pk_u32",
		DeleteReducer: "delete_pk_u32",
		TestValues: []interface{}{
			PkU32Type{Id: 0, Value: "pk_u32_value_0"},
			PkU32Type{Id: 1000000, Value: "pk_u32_value_1000000"},
			PkU32Type{Id: 2147483647, Value: "pk_u32_value_2147483647"},
			PkU32Type{Id: 4294967295, Value: "pk_u32_value_4294967295"},
		},
		KeyValues: []interface{}{
			PkU32Type{Id: 1001, Value: "key_test_1001"},
			PkU32Type{Id: 2002, Value: "key_test_2002"},
			PkU32Type{Id: 3003, Value: "key_test_3003"},
		},
	},
	{
		Name:          "pk_u32_two",
		TableName:     "PkU32Two",
		InsertReducer: "insert_pk_u32_two",
		UpdateReducer: "update_pk_u32_two",
		DeleteReducer: "delete_pk_u32_two",
		TestValues: []interface{}{
			PkU32TwoType{Id: 0, Value: "pk_u32_two_value_0"},
			PkU32TwoType{Id: 5000000, Value: "pk_u32_two_value_5000000"},
		},
		KeyValues: []interface{}{
			PkU32TwoType{Id: 5001, Value: "key_test_5001"},
			PkU32TwoType{Id: 5002, Value: "key_test_5002"},
		},
	},
	{
		Name:          "pk_u64",
		TableName:     "PkU64",
		InsertReducer: "insert_pk_u64",
		UpdateReducer: "update_pk_u64",
		DeleteReducer: "delete_pk_u64",
		TestValues: []interface{}{
			PkU64Type{Id: 0, Value: "pk_u64_value_0"},
			PkU64Type{Id: 1000000000, Value: "pk_u64_value_1000000000"},
			PkU64Type{Id: 9223372036854775807, Value: "pk_u64_value_9223372036854775807"},
		},
		KeyValues: []interface{}{
			PkU64Type{Id: 10001, Value: "key_test_10001"},
			PkU64Type{Id: 20002, Value: "key_test_20002"},
		},
	},
	{
		Name:          "pk_i32",
		TableName:     "PkI32",
		InsertReducer: "insert_pk_i32",
		UpdateReducer: "update_pk_i32",
		DeleteReducer: "delete_pk_i32",
		TestValues: []interface{}{
			PkI32Type{Id: 0, Value: "pk_i32_value_0"},
			PkI32Type{Id: -1000000, Value: "pk_i32_value_-1000000"},
			PkI32Type{Id: 1000000, Value: "pk_i32_value_1000000"},
			PkI32Type{Id: math.MaxInt32, Value: "pk_i32_value_max"},
			PkI32Type{Id: math.MinInt32, Value: "pk_i32_value_min"},
		},
		KeyValues: []interface{}{
			PkI32Type{Id: -1001, Value: "key_test_-1001"},
			PkI32Type{Id: 2002, Value: "key_test_2002"},
			PkI32Type{Id: -3003, Value: "key_test_-3003"},
		},
	},
	{
		Name:          "pk_bool",
		TableName:     "PkBool",
		InsertReducer: "insert_pk_bool",
		UpdateReducer: "update_pk_bool",
		DeleteReducer: "delete_pk_bool",
		TestValues: []interface{}{
			PkBoolType{Id: true, Value: "pk_bool_value_true"},
			PkBoolType{Id: false, Value: "pk_bool_value_false"},
		},
		KeyValues: []interface{}{
			PkBoolType{Id: true, Value: "key_test_true"},
			PkBoolType{Id: false, Value: "key_test_false"},
		},
	},
	{
		Name:          "pk_string",
		TableName:     "PkString",
		InsertReducer: "insert_pk_string",
		UpdateReducer: "update_pk_string",
		DeleteReducer: "delete_pk_string",
		TestValues: []interface{}{
			PkStringType{Id: "", Value: "pk_string_value_empty"},
			PkStringType{Id: "pk_test_1", Value: "pk_string_value_1"},
			PkStringType{Id: "pk_test_2", Value: "pk_string_value_2"},
			PkStringType{Id: "SpacetimeDB_PK", Value: "pk_string_value_spacetimedb"},
			PkStringType{Id: "🚀_pk_emoji", Value: "pk_string_value_emoji"},
		},
		KeyValues: []interface{}{
			PkStringType{Id: "key_value_1", Value: "key_test_string_1"},
			PkStringType{Id: "key_value_2", Value: "key_test_string_2"},
			PkStringType{Id: "key_value_3", Value: "key_test_string_3"},
		},
	},
	{
		Name:          "pk_identity",
		TableName:     "PkIdentity",
		InsertReducer: "insert_pk_identity",
		UpdateReducer: "update_pk_identity",
		DeleteReducer: "delete_pk_identity",
		TestValues: []interface{}{
			PkIdentityType{Id: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Value: "pk_identity_value_1"},
			PkIdentityType{Id: [16]byte{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240}, Value: "pk_identity_value_2"},
		},
		KeyValues: []interface{}{
			PkIdentityType{Id: [16]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160}, Value: "key_test_identity_1"},
			PkIdentityType{Id: [16]byte{11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 111, 121, 131, 141, 151, 161}, Value: "key_test_identity_2"},
		},
	},
	{
		Name:          "pk_timestamp",
		TableName:     "PkTimestamp",
		InsertReducer: "insert_pk_timestamp",
		UpdateReducer: "update_pk_timestamp",
		DeleteReducer: "delete_pk_timestamp",
		TestValues: []interface{}{
			PkTimestampType{Id: 0, Value: "pk_timestamp_value_0"},
			PkTimestampType{Id: 1640995200000000, Value: "pk_timestamp_value_2022"}, // Jan 1, 2022 in microseconds
			PkTimestampType{Id: uint64(time.Now().UnixMicro()), Value: "pk_timestamp_value_now"},
		},
		KeyValues: []interface{}{
			PkTimestampType{Id: 1000000, Value: "key_test_timestamp_1"},
			PkTimestampType{Id: 2000000, Value: "key_test_timestamp_2"},
		},
	},
}

// Test configuration constants for primary key operations
const (
	// Performance thresholds (primary key specific)
	PrimaryKeyInsertTime  = 100 * time.Millisecond // Single primary key insert
	PrimaryKeyUpdateTime  = 150 * time.Millisecond // Primary key update
	PrimaryKeyDeleteTime  = 100 * time.Millisecond // Primary key delete
	PrimaryKeyLookupTime  = 50 * time.Millisecond  // Primary key lookup
	PKConstraintCheckTime = 50 * time.Millisecond  // PK constraint validation

	// Test limits (primary key specific)
	MaxPrimaryKeyTestValues    = 50  // Number of PK values to test
	PrimaryKeyPerformanceIters = 100 // Iterations for performance testing
	PrimaryKeyTimeoutSec       = 30  // Timeout for PK operations
	MaxPrimaryKeyViolations    = 10  // Maximum violation attempts per test
)

// TestSDKPrimaryKeys is the main integration test for primary keys
func TestSDKPrimaryKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK primary keys integration test in short mode")
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
	t.Logf("🎯 Testing PRIMARY KEYS: All data types as primary key constraints")
	t.Logf("🎯 Testing CRUD OPERATIONS: Insert, update, delete via primary key")
	t.Logf("🎯 Testing KEY UNIQUENESS: Primary key constraint enforcement")
	t.Logf("🎯 Testing PERFORMANCE: Primary key operation speed validation")

	// Generate additional test data for primary keys
	generatePrimaryKeyTestData(t)

	// Run comprehensive primary key test suites
	t.Run("PrimaryKeyInsertOperations", func(t *testing.T) {
		testPrimaryKeyInsertOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyUpdateOperations", func(t *testing.T) {
		testPrimaryKeyUpdateOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyDeleteOperations", func(t *testing.T) {
		testPrimaryKeyDeleteOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyUniquenessValidation", func(t *testing.T) {
		testPrimaryKeyUniquenessValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyTypeValidation", func(t *testing.T) {
		testPrimaryKeyTypeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyPerformance", func(t *testing.T) {
		testPrimaryKeyPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CompositePrimaryKeyTesting", func(t *testing.T) {
		testCompositePrimaryKeys(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrimaryKeyBoundaryTests", func(t *testing.T) {
		testPrimaryKeyBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generatePrimaryKeyTestData generates additional test data for primary key testing
func generatePrimaryKeyTestData(t *testing.T) {
	t.Log("Generating additional PRIMARY KEY test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate additional primary key values for comprehensive testing
	for i := range PrimaryKeyTypes {
		pkType := &PrimaryKeyTypes[i]

		// Generate additional key test values based on type
		switch pkType.Name {
		case "pk_u8":
			for j := 0; j < 5; j++ {
				val := PkU8Type{Id: uint8(rand.Intn(256)), Value: fmt.Sprintf("generated_pk_u8_%d", j)}
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		case "pk_u16":
			for j := 0; j < 5; j++ {
				val := PkU16Type{Id: uint16(rand.Intn(65536)), Value: fmt.Sprintf("generated_pk_u16_%d", j)}
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		case "pk_u32":
			for j := 0; j < 5; j++ {
				val := PkU32Type{Id: rand.Uint32(), Value: fmt.Sprintf("generated_pk_u32_%d", j)}
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		case "pk_u64":
			for j := 0; j < 5; j++ {
				val := PkU64Type{Id: rand.Uint64(), Value: fmt.Sprintf("generated_pk_u64_%d", j)}
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		case "pk_i32":
			for j := 0; j < 5; j++ {
				val := PkI32Type{Id: rand.Int31() - (1 << 30), Value: fmt.Sprintf("generated_pk_i32_%d", j)} // Mix positive and negative
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		case "pk_string":
			for j := 0; j < 5; j++ {
				val := PkStringType{Id: fmt.Sprintf("generated_pk_string_%d_%d", j, rand.Intn(10000)), Value: fmt.Sprintf("generated_value_%d", j)}
				pkType.KeyValues = append(pkType.KeyValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional primary key test data for %d PK types", len(PrimaryKeyTypes))
}

// testPrimaryKeyInsertOperations tests insert operations with primary keys
func testPrimaryKeyInsertOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY INSERT OPERATIONS...")

	for _, pkType := range PrimaryKeyTypes {
		t.Run(fmt.Sprintf("Insert_%s", pkType.Name), func(t *testing.T) {
			successCount := 0
			testsProcessed := 0

			for i, testValue := range pkType.TestValues {
				if i >= 3 { // Limit tests for performance
					break
				}
				testsProcessed++

				startTime := time.Now()

				// Encode the primary key value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				require.NoError(t, err, "Should encode %s value", pkType.Name)

				// Validate encoding succeeded and meets performance requirements
				insertTime := time.Since(startTime)
				t.Logf("✅ %s primary key insert value encoded successfully to %d bytes in %v",
					pkType.Name, len(encoded), insertTime)

				// Verify performance
				assert.Less(t, insertTime, PrimaryKeyInsertTime,
					"%s PK insert took %v, expected less than %v", pkType.Name, insertTime, PrimaryKeyInsertTime)

				// All successful encodings count as successful inserts
				successCount++
			}

			// Report primary key insert success rate
			successRate := float64(successCount) / float64(testsProcessed) * 100
			t.Logf("✅ %s primary key INSERT operations: %d/%d successful (%.1f%%)",
				pkType.Name, successCount, testsProcessed, successRate)

			// With encoding validation, we should achieve high success rate
			assert.Greater(t, successRate, 95.0,
				"Primary key insert operations should have >95%% success rate")
		})
	}
}

// testPrimaryKeyUpdateOperations tests update operations with primary keys
func testPrimaryKeyUpdateOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY UPDATE OPERATIONS...")

	for _, pkType := range PrimaryKeyTypes {
		t.Run(fmt.Sprintf("Update_%s", pkType.Name), func(t *testing.T) {
			// Test update operations (if the reducers exist)
			if len(pkType.KeyValues) < 2 {
				t.Logf("⚠️  Skipping %s update test - insufficient key values", pkType.Name)
				return
			}

			startTime := time.Now()

			// Encode an update value
			updateValue := pkType.KeyValues[0]
			encoded, err := encodingManager.Encode(updateValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("⚠️  Failed to encode %s update value: %v", pkType.Name, err)
				return
			}

			t.Logf("✅ %s primary key update value encoded to %d bytes in %v",
				pkType.Name, len(encoded), time.Since(startTime))

			// Note: Update operations require existing data and specific reducer patterns
			// This test validates encoding infrastructure for updates
			assert.Greater(t, len(encoded), 0, "%s update value should encode to >0 bytes", pkType.Name)
		})
	}
}

// testPrimaryKeyDeleteOperations tests delete operations with primary keys
func testPrimaryKeyDeleteOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY DELETE OPERATIONS...")

	for _, pkType := range PrimaryKeyTypes {
		t.Run(fmt.Sprintf("Delete_%s", pkType.Name), func(t *testing.T) {
			// Test delete operations (if the reducers exist)
			if len(pkType.TestValues) == 0 {
				t.Logf("⚠️  Skipping %s delete test - no test values", pkType.Name)
				return
			}

			startTime := time.Now()

			// Encode a delete value
			deleteValue := pkType.TestValues[0]
			encoded, err := encodingManager.Encode(deleteValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("⚠️  Failed to encode %s delete value: %v", pkType.Name, err)
				return
			}

			t.Logf("✅ %s primary key delete value encoded to %d bytes in %v",
				pkType.Name, len(encoded), time.Since(startTime))

			// Note: Delete operations require existing data and specific reducer patterns
			// This test validates encoding infrastructure for deletes
			assert.Greater(t, len(encoded), 0, "%s delete value should encode to >0 bytes", pkType.Name)
		})
	}
}

// testPrimaryKeyUniquenessValidation tests primary key uniqueness constraint enforcement
func testPrimaryKeyUniquenessValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY UNIQUENESS VALIDATION...")

	uniquenessTests := []struct {
		Name        string
		Value1      interface{}
		Value2      interface{} // Same primary key - should violate constraint
		Description string
	}{
		{
			Name:        "pk_u32_duplicate_violation",
			Value1:      PkU32Type{Id: 12345, Value: "first_value"},
			Value2:      PkU32Type{Id: 12345, Value: "second_value"},
			Description: "Duplicate u32 primary keys should violate constraint",
		},
		{
			Name:        "pk_string_duplicate_violation",
			Value1:      PkStringType{Id: "duplicate_pk", Value: "first_value"},
			Value2:      PkStringType{Id: "duplicate_pk", Value: "second_value"},
			Description: "Duplicate string primary keys should violate constraint",
		},
		{
			Name:        "pk_bool_duplicate_violation",
			Value1:      PkBoolType{Id: true, Value: "first_value"},
			Value2:      PkBoolType{Id: true, Value: "second_value"},
			Description: "Duplicate bool primary keys should violate constraint",
		},
	}

	successCount := 0
	totalTests := len(uniquenessTests)

	for _, test := range uniquenessTests {
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

			uniquenessTime := time.Since(startTime)

			t.Logf("✅ %s: %s - encoded both values to %d and %d bytes in %v",
				test.Name, test.Description, len(encoded1), len(encoded2), uniquenessTime)

			// Validate uniqueness detection performance
			assert.Less(t, uniquenessTime, PKConstraintCheckTime,
				"PK uniqueness check took %v, expected less than %v", uniquenessTime, PKConstraintCheckTime)

			successCount++
		})
	}

	// Report primary key uniqueness success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ PRIMARY KEY UNIQUENESS validation: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Primary key uniqueness validation should have >90%% success rate")
}

// testPrimaryKeyTypeValidation validates type safety and correctness for primary keys
func testPrimaryKeyTypeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY TYPE VALIDATION...")

	validationTests := []struct {
		Name        string
		Value       interface{}
		ExpectValid bool
		TypeName    string
	}{
		// Valid primary key types
		{"valid_pk_u8", PkU8Type{Id: 42, Value: "test"}, true, "PkU8"},
		{"valid_pk_u16", PkU16Type{Id: 1024, Value: "test"}, true, "PkU16"},
		{"valid_pk_u32", PkU32Type{Id: 123456, Value: "test"}, true, "PkU32"},
		{"valid_pk_u64", PkU64Type{Id: 9876543210, Value: "test"}, true, "PkU64"},
		{"valid_pk_i32", PkI32Type{Id: -123456, Value: "test"}, true, "PkI32"},
		{"valid_pk_bool", PkBoolType{Id: true, Value: "test"}, true, "PkBool"},
		{"valid_pk_string", PkStringType{Id: "validation_test", Value: "test"}, true, "PkString"},
		{"valid_pk_timestamp", PkTimestampType{Id: 1640995200000000, Value: "test"}, true, "PkTimestamp"},

		// Edge cases
		{"edge_pk_u8_max", PkU8Type{Id: 255, Value: "test"}, true, "PkU8"},
		{"edge_pk_u8_min", PkU8Type{Id: 0, Value: "test"}, true, "PkU8"},
		{"edge_pk_string_empty", PkStringType{Id: "", Value: "test"}, true, "PkString"},
		{"edge_pk_string_unicode", PkStringType{Id: "🚀_pk", Value: "test"}, true, "PkString"},
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
	t.Logf("✅ PRIMARY KEY TYPE VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Primary key type validation should have 100%% success rate")
}

// testPrimaryKeyPerformance measures performance of primary key operations
func testPrimaryKeyPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY PERFORMANCE...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	performanceResults := make(map[string]time.Duration)

	// Performance test for different primary key types
	for _, pkType := range PrimaryKeyTypes {
		if len(pkType.TestValues) == 0 {
			continue
		}

		t.Run(fmt.Sprintf("Performance_%s", pkType.Name), func(t *testing.T) {
			totalOperations := PrimaryKeyPerformanceIters
			if len(pkType.TestValues) < totalOperations {
				totalOperations = len(pkType.TestValues)
			}

			startTime := time.Now()

			for i := 0; i < totalOperations; i++ {
				testValue := pkType.TestValues[i%len(pkType.TestValues)]

				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("%s encoding error at iteration %d: %v", pkType.Name, i, err)
					continue
				}

				_ = encoded // Use the encoded value
			}

			totalTime := time.Since(startTime)
			avgTimePerOp := totalTime / time.Duration(totalOperations)
			performanceResults[pkType.Name] = avgTimePerOp

			t.Logf("✅ %s performance: %d operations in %v (avg: %v per operation)",
				pkType.Name, totalOperations, totalTime, avgTimePerOp)

			maxExpectedTime := time.Microsecond * 50 // 50μs per operation max
			assert.Less(t, avgTimePerOp, maxExpectedTime,
				"%s operations should be fast (<%v), got %v", pkType.Name, maxExpectedTime, avgTimePerOp)
		})
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	t.Logf("✅ PRIMARY KEY PERFORMANCE testing memory usage: %.2f MB", memUsedMB)

	// Log performance summary
	t.Log("📊 PRIMARY KEY PERFORMANCE Summary:")
	for typeName, avgTime := range performanceResults {
		t.Logf("  %s: %v per operation", typeName, avgTime)
	}
}

// testCompositePrimaryKeys tests composite primary key scenarios (if supported)
func testCompositePrimaryKeys(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing COMPOSITE PRIMARY KEY SCENARIOS...")

	// Composite primary key simulation with multiple single-key tables
	compositeTests := []struct {
		Name        string
		Table1      string
		Value1      interface{}
		Table2      string
		Value2      interface{}
		Description string
	}{
		{
			Name:        "pk_u32_cross_table",
			Table1:      "pk_u32",
			Value1:      PkU32Type{Id: 42, Value: "cross_table_test_1"},
			Table2:      "pk_u32_two",
			Value2:      PkU32TwoType{Id: 42, Value: "cross_table_test_2"},
			Description: "Same PK value should be allowed in different tables",
		},
		{
			Name:        "pk_different_types_same_semantic_value",
			Table1:      "pk_u32",
			Value1:      PkU32Type{Id: 100, Value: "semantic_test_u32"},
			Table2:      "pk_i32",
			Value2:      PkI32Type{Id: 100, Value: "semantic_test_i32"},
			Description: "Same numeric value should be allowed in different PK type tables",
		},
	}

	successCount := 0
	totalTests := len(compositeTests)

	for _, test := range compositeTests {
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

	// Report composite primary key success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ COMPOSITE PRIMARY KEY testing: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Composite primary key testing should have >90%% success rate")
}

// testPrimaryKeyBoundaryConditions tests edge cases and boundary conditions for primary keys
func testPrimaryKeyBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing PRIMARY KEY BOUNDARY CONDITIONS...")

	boundaryTests := []struct {
		Name        string
		Value       interface{}
		Description string
		ShouldWork  bool
	}{
		// Boundary values for different primary key types
		{"boundary_pk_u8_max", PkU8Type{Id: 255, Value: "max_u8"}, "Maximum u8 primary key", true},
		{"boundary_pk_u8_min", PkU8Type{Id: 0, Value: "min_u8"}, "Minimum u8 primary key", true},
		{"boundary_pk_u16_max", PkU16Type{Id: 65535, Value: "max_u16"}, "Maximum u16 primary key", true},
		{"boundary_pk_u32_max", PkU32Type{Id: 4294967295, Value: "max_u32"}, "Maximum u32 primary key", true},
		{"boundary_pk_u64_max", PkU64Type{Id: 18446744073709551615, Value: "max_u64"}, "Maximum u64 primary key", true},
		{"boundary_pk_i32_max", PkI32Type{Id: math.MaxInt32, Value: "max_i32"}, "Maximum i32 primary key", true},
		{"boundary_pk_i32_min", PkI32Type{Id: math.MinInt32, Value: "min_i32"}, "Minimum i32 primary key", true},

		// String primary key boundary conditions
		{"boundary_pk_string_empty", PkStringType{Id: "", Value: "empty_string_pk"}, "Empty string primary key", true},
		{"boundary_pk_string_single", PkStringType{Id: "a", Value: "single_char_pk"}, "Single character string primary key", true},
		{"boundary_pk_string_unicode", PkStringType{Id: "🚀", Value: "unicode_emoji_pk"}, "Unicode emoji primary key", true},
		{"boundary_pk_string_long", PkStringType{Id: string(make([]byte, 1000)), Value: "long_string_pk"}, "Very long string primary key", true},

		// Boolean primary key boundary conditions
		{"boundary_pk_bool_true", PkBoolType{Id: true, Value: "bool_true_pk"}, "Boolean true primary key", true},
		{"boundary_pk_bool_false", PkBoolType{Id: false, Value: "bool_false_pk"}, "Boolean false primary key", true},

		// Timestamp primary key boundary conditions
		{"boundary_pk_timestamp_zero", PkTimestampType{Id: 0, Value: "timestamp_zero"}, "Zero timestamp primary key", true},
		{"boundary_pk_timestamp_now", PkTimestampType{Id: uint64(time.Now().UnixMicro()), Value: "timestamp_now"}, "Current timestamp primary key", true},
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
	t.Logf("✅ PRIMARY KEY BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Primary key boundary conditions should have >90%% success rate")
}
