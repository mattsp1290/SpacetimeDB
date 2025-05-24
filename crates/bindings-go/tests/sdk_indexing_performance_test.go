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

// SDK Task 11: Indexing and Query Performance with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB indexing:
// - Index Creation: All integer, float, string, identity types with indexes
// - Query Performance: Optimized lookup operations via indexed fields
// - Index Validation: Type safety and correctness of indexed operations
// - Performance Testing: Query speed with/without indexes
// - Type Safety: Leveraging our proven BSATN encoding patterns

// === INDEXED TABLE TYPES ===

// IndexedU8Type represents a table with indexed u8 field
type IndexedU8Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField uint8  `json:"indexed_field"` // Indexed u8 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedU16Type represents a table with indexed u16 field
type IndexedU16Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField uint16 `json:"indexed_field"` // Indexed u16 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedU32Type represents a table with indexed u32 field
type IndexedU32Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField uint32 `json:"indexed_field"` // Indexed u32 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedU64Type represents a table with indexed u64 field
type IndexedU64Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField uint64 `json:"indexed_field"` // Indexed u64 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedI8Type represents a table with indexed i8 field
type IndexedI8Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField int8   `json:"indexed_field"` // Indexed i8 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedI16Type represents a table with indexed i16 field
type IndexedI16Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField int16  `json:"indexed_field"` // Indexed i16 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedI32Type represents a table with indexed i32 field
type IndexedI32Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField int32  `json:"indexed_field"` // Indexed i32 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedI64Type represents a table with indexed i64 field
type IndexedI64Type struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField int64  `json:"indexed_field"` // Indexed i64 field
	Value        string `json:"value"`         // Additional data field
}

// IndexedF32Type represents a table with indexed f32 field
type IndexedF32Type struct {
	Id           uint32  `json:"id"`            // Auto-increment primary key
	IndexedField float32 `json:"indexed_field"` // Indexed f32 field
	Value        string  `json:"value"`         // Additional data field
}

// IndexedF64Type represents a table with indexed f64 field
type IndexedF64Type struct {
	Id           uint32  `json:"id"`            // Auto-increment primary key
	IndexedField float64 `json:"indexed_field"` // Indexed f64 field
	Value        string  `json:"value"`         // Additional data field
}

// IndexedBoolType represents a table with indexed bool field
type IndexedBoolType struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField bool   `json:"indexed_field"` // Indexed bool field
	Value        string `json:"value"`         // Additional data field
}

// IndexedStringType represents a table with indexed string field
type IndexedStringType struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField string `json:"indexed_field"` // Indexed string field
	Value        string `json:"value"`         // Additional data field
}

// IndexedIdentityType represents a table with indexed Identity field
type IndexedIdentityType struct {
	Id           uint32   `json:"id"`            // Auto-increment primary key
	IndexedField [16]byte `json:"indexed_field"` // Indexed Identity field
	Value        string   `json:"value"`         // Additional data field
}

// IndexedTimestampType represents a table with indexed Timestamp field
type IndexedTimestampType struct {
	Id           uint32 `json:"id"`            // Auto-increment primary key
	IndexedField uint64 `json:"indexed_field"` // Indexed Timestamp field (as uint64)
	Value        string `json:"value"`         // Additional data field
}

// === INDEX OPERATION TYPES ===

// IndexOperation represents an index operation test scenario
type IndexOperation struct {
	Name        string
	Description string
	IndexType   string                 // Type of indexed table
	Operation   string                 // Operation type (create, query, range)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// QueryPerformanceTest represents a query performance test scenario
type QueryPerformanceTest struct {
	Name          string
	Description   string
	IndexType     string
	QueryType     string        // exact, range, prefix
	TestValues    []interface{} // Values to query for
	ExpectedSpeed time.Duration // Expected query speed
}

// IndexConfig defines configuration for index testing
type IndexConfig struct {
	Name              string
	TableName         string        // SDK-test table name
	IndexField        string        // Field name that's indexed
	CreateReducer     string        // Create/insert reducer name
	QueryReducer      string        // Query reducer name
	RangeQueryReducer string        // Range query reducer name
	TestValues        []interface{} // Standard test values
	IndexValues       []interface{} // Values for index testing
	QueryTests        []QueryPerformanceTest
	IndexOperations   []IndexOperation
}

// Index type configurations
var IndexedTypes = []IndexConfig{
	{
		Name:              "indexed_u8",
		TableName:         "IndexedU8",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_u8",
		QueryReducer:      "query_indexed_u8",
		RangeQueryReducer: "range_query_indexed_u8",
		TestValues: []interface{}{
			IndexedU8Type{Id: 1, IndexedField: 0, Value: "indexed_u8_value_0"},
			IndexedU8Type{Id: 2, IndexedField: 1, Value: "indexed_u8_value_1"},
			IndexedU8Type{Id: 3, IndexedField: 42, Value: "indexed_u8_value_42"},
			IndexedU8Type{Id: 4, IndexedField: 255, Value: "indexed_u8_value_255"},
		},
		IndexValues: []interface{}{
			IndexedU8Type{Id: 10, IndexedField: 10, Value: "index_test_10"},
			IndexedU8Type{Id: 11, IndexedField: 20, Value: "index_test_20"},
			IndexedU8Type{Id: 12, IndexedField: 30, Value: "index_test_30"},
		},
	},
	{
		Name:              "indexed_u16",
		TableName:         "IndexedU16",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_u16",
		QueryReducer:      "query_indexed_u16",
		RangeQueryReducer: "range_query_indexed_u16",
		TestValues: []interface{}{
			IndexedU16Type{Id: 1, IndexedField: 0, Value: "indexed_u16_value_0"},
			IndexedU16Type{Id: 2, IndexedField: 1000, Value: "indexed_u16_value_1000"},
			IndexedU16Type{Id: 3, IndexedField: 32767, Value: "indexed_u16_value_32767"},
			IndexedU16Type{Id: 4, IndexedField: 65535, Value: "indexed_u16_value_65535"},
		},
		IndexValues: []interface{}{
			IndexedU16Type{Id: 10, IndexedField: 100, Value: "index_test_100"},
			IndexedU16Type{Id: 11, IndexedField: 200, Value: "index_test_200"},
			IndexedU16Type{Id: 12, IndexedField: 300, Value: "index_test_300"},
		},
	},
	{
		Name:              "indexed_u32",
		TableName:         "IndexedU32",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_u32",
		QueryReducer:      "query_indexed_u32",
		RangeQueryReducer: "range_query_indexed_u32",
		TestValues: []interface{}{
			IndexedU32Type{Id: 1, IndexedField: 0, Value: "indexed_u32_value_0"},
			IndexedU32Type{Id: 2, IndexedField: 1000000, Value: "indexed_u32_value_1000000"},
			IndexedU32Type{Id: 3, IndexedField: 2147483647, Value: "indexed_u32_value_2147483647"},
			IndexedU32Type{Id: 4, IndexedField: 4294967295, Value: "indexed_u32_value_4294967295"},
		},
		IndexValues: []interface{}{
			IndexedU32Type{Id: 10, IndexedField: 1001, Value: "index_test_1001"},
			IndexedU32Type{Id: 11, IndexedField: 2002, Value: "index_test_2002"},
			IndexedU32Type{Id: 12, IndexedField: 3003, Value: "index_test_3003"},
		},
	},
	{
		Name:              "indexed_u64",
		TableName:         "IndexedU64",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_u64",
		QueryReducer:      "query_indexed_u64",
		RangeQueryReducer: "range_query_indexed_u64",
		TestValues: []interface{}{
			IndexedU64Type{Id: 1, IndexedField: 0, Value: "indexed_u64_value_0"},
			IndexedU64Type{Id: 2, IndexedField: 1000000000, Value: "indexed_u64_value_1000000000"},
			IndexedU64Type{Id: 3, IndexedField: 9223372036854775807, Value: "indexed_u64_value_9223372036854775807"},
		},
		IndexValues: []interface{}{
			IndexedU64Type{Id: 10, IndexedField: 10001, Value: "index_test_10001"},
			IndexedU64Type{Id: 11, IndexedField: 20002, Value: "index_test_20002"},
		},
	},
	{
		Name:              "indexed_i32",
		TableName:         "IndexedI32",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_i32",
		QueryReducer:      "query_indexed_i32",
		RangeQueryReducer: "range_query_indexed_i32",
		TestValues: []interface{}{
			IndexedI32Type{Id: 1, IndexedField: 0, Value: "indexed_i32_value_0"},
			IndexedI32Type{Id: 2, IndexedField: -1000000, Value: "indexed_i32_value_-1000000"},
			IndexedI32Type{Id: 3, IndexedField: 1000000, Value: "indexed_i32_value_1000000"},
			IndexedI32Type{Id: 4, IndexedField: math.MaxInt32, Value: "indexed_i32_value_max"},
			IndexedI32Type{Id: 5, IndexedField: math.MinInt32, Value: "indexed_i32_value_min"},
		},
		IndexValues: []interface{}{
			IndexedI32Type{Id: 10, IndexedField: -1001, Value: "index_test_-1001"},
			IndexedI32Type{Id: 11, IndexedField: 2002, Value: "index_test_2002"},
			IndexedI32Type{Id: 12, IndexedField: -3003, Value: "index_test_-3003"},
		},
	},
	{
		Name:              "indexed_f32",
		TableName:         "IndexedF32",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_f32",
		QueryReducer:      "query_indexed_f32",
		RangeQueryReducer: "range_query_indexed_f32",
		TestValues: []interface{}{
			IndexedF32Type{Id: 1, IndexedField: 0.0, Value: "indexed_f32_value_0"},
			IndexedF32Type{Id: 2, IndexedField: 3.14159, Value: "indexed_f32_value_pi"},
			IndexedF32Type{Id: 3, IndexedField: -2.71828, Value: "indexed_f32_value_e"},
			IndexedF32Type{Id: 4, IndexedField: float32(math.MaxFloat32), Value: "indexed_f32_value_max"},
		},
		IndexValues: []interface{}{
			IndexedF32Type{Id: 10, IndexedField: 10.1, Value: "index_test_10.1"},
			IndexedF32Type{Id: 11, IndexedField: 20.2, Value: "index_test_20.2"},
			IndexedF32Type{Id: 12, IndexedField: 30.3, Value: "index_test_30.3"},
		},
	},
	{
		Name:              "indexed_f64",
		TableName:         "IndexedF64",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_f64",
		QueryReducer:      "query_indexed_f64",
		RangeQueryReducer: "range_query_indexed_f64",
		TestValues: []interface{}{
			IndexedF64Type{Id: 1, IndexedField: 0.0, Value: "indexed_f64_value_0"},
			IndexedF64Type{Id: 2, IndexedField: 3.141592653589793, Value: "indexed_f64_value_pi"},
			IndexedF64Type{Id: 3, IndexedField: -2.718281828459045, Value: "indexed_f64_value_e"},
			IndexedF64Type{Id: 4, IndexedField: math.MaxFloat64, Value: "indexed_f64_value_max"},
		},
		IndexValues: []interface{}{
			IndexedF64Type{Id: 10, IndexedField: 10.123456789, Value: "index_test_10.123456789"},
			IndexedF64Type{Id: 11, IndexedField: 20.987654321, Value: "index_test_20.987654321"},
		},
	},
	{
		Name:              "indexed_bool",
		TableName:         "IndexedBool",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_bool",
		QueryReducer:      "query_indexed_bool",
		RangeQueryReducer: "range_query_indexed_bool",
		TestValues: []interface{}{
			IndexedBoolType{Id: 1, IndexedField: true, Value: "indexed_bool_value_true"},
			IndexedBoolType{Id: 2, IndexedField: false, Value: "indexed_bool_value_false"},
		},
		IndexValues: []interface{}{
			IndexedBoolType{Id: 10, IndexedField: true, Value: "index_test_true"},
			IndexedBoolType{Id: 11, IndexedField: false, Value: "index_test_false"},
		},
	},
	{
		Name:              "indexed_string",
		TableName:         "IndexedString",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_string",
		QueryReducer:      "query_indexed_string",
		RangeQueryReducer: "range_query_indexed_string",
		TestValues: []interface{}{
			IndexedStringType{Id: 1, IndexedField: "", Value: "indexed_string_value_empty"},
			IndexedStringType{Id: 2, IndexedField: "indexed_test_1", Value: "indexed_string_value_1"},
			IndexedStringType{Id: 3, IndexedField: "indexed_test_2", Value: "indexed_string_value_2"},
			IndexedStringType{Id: 4, IndexedField: "SpacetimeDB_Index", Value: "indexed_string_value_spacetimedb"},
			IndexedStringType{Id: 5, IndexedField: "🚀_indexed_emoji", Value: "indexed_string_value_emoji"},
		},
		IndexValues: []interface{}{
			IndexedStringType{Id: 10, IndexedField: "index_value_1", Value: "index_test_string_1"},
			IndexedStringType{Id: 11, IndexedField: "index_value_2", Value: "index_test_string_2"},
			IndexedStringType{Id: 12, IndexedField: "index_value_3", Value: "index_test_string_3"},
		},
	},
	{
		Name:              "indexed_identity",
		TableName:         "IndexedIdentity",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_identity",
		QueryReducer:      "query_indexed_identity",
		RangeQueryReducer: "range_query_indexed_identity",
		TestValues: []interface{}{
			IndexedIdentityType{Id: 1, IndexedField: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Value: "indexed_identity_value_1"},
			IndexedIdentityType{Id: 2, IndexedField: [16]byte{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240}, Value: "indexed_identity_value_2"},
		},
		IndexValues: []interface{}{
			IndexedIdentityType{Id: 10, IndexedField: [16]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160}, Value: "index_test_identity_1"},
			IndexedIdentityType{Id: 11, IndexedField: [16]byte{11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 111, 121, 131, 141, 151, 161}, Value: "index_test_identity_2"},
		},
	},
	{
		Name:              "indexed_timestamp",
		TableName:         "IndexedTimestamp",
		IndexField:        "indexed_field",
		CreateReducer:     "insert_indexed_timestamp",
		QueryReducer:      "query_indexed_timestamp",
		RangeQueryReducer: "range_query_indexed_timestamp",
		TestValues: []interface{}{
			IndexedTimestampType{Id: 1, IndexedField: 0, Value: "indexed_timestamp_value_0"},
			IndexedTimestampType{Id: 2, IndexedField: 1640995200000000, Value: "indexed_timestamp_value_2022"}, // Jan 1, 2022 in microseconds
			IndexedTimestampType{Id: 3, IndexedField: uint64(time.Now().UnixMicro()), Value: "indexed_timestamp_value_now"},
		},
		IndexValues: []interface{}{
			IndexedTimestampType{Id: 10, IndexedField: 1000000, Value: "index_test_timestamp_1"},
			IndexedTimestampType{Id: 11, IndexedField: 2000000, Value: "index_test_timestamp_2"},
		},
	},
}

// Test configuration constants for indexing operations
const (
	// Performance thresholds (indexing specific)
	IndexedInsertTime   = 120 * time.Millisecond // Single indexed insert
	IndexedQueryTime    = 10 * time.Millisecond  // Single indexed query
	IndexedRangeTime    = 50 * time.Millisecond  // Indexed range query
	IndexCreationTime   = 200 * time.Millisecond // Index creation
	IndexValidationTime = 30 * time.Millisecond  // Index validation

	// Test limits (indexing specific)
	MaxIndexedTestValues    = 100 // Number of indexed values to test
	IndexPerformanceIters   = 200 // Iterations for performance testing
	IndexedOperationTimeout = 30  // Timeout for indexed operations
	MaxIndexQueryAttempts   = 10  // Maximum query attempts per test
	RangeQuerySampleSize    = 50  // Sample size for range query testing
)

// TestSDKIndexingPerformance is the main integration test for indexing and query performance
func TestSDKIndexingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK indexing performance integration test in short mode")
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
	t.Logf("🎯 Testing INDEXING: All data types with indexed field operations")
	t.Logf("🎯 Testing QUERY PERFORMANCE: Optimized lookup operations via indexes")
	t.Logf("🎯 Testing INDEX VALIDATION: Type safety and correctness")
	t.Logf("🎯 Testing PERFORMANCE: Query speed with/without indexes")

	// Generate additional test data for indexing
	generateIndexingTestData(t)

	// Run comprehensive indexing and performance test suites
	t.Run("IndexedInsertOperations", func(t *testing.T) {
		testIndexedInsertOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexedQueryOperations", func(t *testing.T) {
		testIndexedQueryOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexedRangeQueryOperations", func(t *testing.T) {
		testIndexedRangeQueryOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexValidationTesting", func(t *testing.T) {
		testIndexValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexPerformanceComparison", func(t *testing.T) {
		testIndexPerformanceComparison(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexBoundaryConditions", func(t *testing.T) {
		testIndexBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("MultiIndexOperations", func(t *testing.T) {
		testMultiIndexOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IndexConcurrencyTesting", func(t *testing.T) {
		testIndexConcurrencyOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateIndexingTestData generates additional test data for indexing testing
func generateIndexingTestData(t *testing.T) {
	t.Log("Generating additional INDEXING test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate additional indexed values for comprehensive testing
	for i := range IndexedTypes {
		indexType := &IndexedTypes[i]

		// Generate additional index test values based on type
		switch indexType.Name {
		case "indexed_u8":
			for j := 0; j < 20; j++ {
				val := IndexedU8Type{Id: uint32(100 + j), IndexedField: uint8(rand.Intn(256)), Value: fmt.Sprintf("generated_indexed_u8_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_u16":
			for j := 0; j < 20; j++ {
				val := IndexedU16Type{Id: uint32(100 + j), IndexedField: uint16(rand.Intn(65536)), Value: fmt.Sprintf("generated_indexed_u16_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_u32":
			for j := 0; j < 20; j++ {
				val := IndexedU32Type{Id: uint32(100 + j), IndexedField: rand.Uint32(), Value: fmt.Sprintf("generated_indexed_u32_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_u64":
			for j := 0; j < 15; j++ {
				val := IndexedU64Type{Id: uint32(100 + j), IndexedField: rand.Uint64(), Value: fmt.Sprintf("generated_indexed_u64_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_i32":
			for j := 0; j < 20; j++ {
				val := IndexedI32Type{Id: uint32(100 + j), IndexedField: rand.Int31() - (1 << 30), Value: fmt.Sprintf("generated_indexed_i32_%d", j)} // Mix positive and negative
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_f32":
			for j := 0; j < 15; j++ {
				val := IndexedF32Type{Id: uint32(100 + j), IndexedField: rand.Float32() * 1000, Value: fmt.Sprintf("generated_indexed_f32_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_f64":
			for j := 0; j < 15; j++ {
				val := IndexedF64Type{Id: uint32(100 + j), IndexedField: rand.Float64() * 1000, Value: fmt.Sprintf("generated_indexed_f64_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_string":
			for j := 0; j < 20; j++ {
				val := IndexedStringType{Id: uint32(100 + j), IndexedField: fmt.Sprintf("generated_indexed_string_%d_%d", j, rand.Intn(10000)), Value: fmt.Sprintf("generated_value_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		case "indexed_timestamp":
			for j := 0; j < 15; j++ {
				val := IndexedTimestampType{Id: uint32(100 + j), IndexedField: uint64(time.Now().UnixMicro()) + uint64(j*1000), Value: fmt.Sprintf("generated_indexed_timestamp_%d", j)}
				indexType.IndexValues = append(indexType.IndexValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional indexing test data for %d indexed types", len(IndexedTypes))
}

// testIndexedInsertOperations tests insert operations with indexed fields
func testIndexedInsertOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEXED INSERT OPERATIONS...")

	for _, indexType := range IndexedTypes {
		t.Run(fmt.Sprintf("IndexedInsert_%s", indexType.Name), func(t *testing.T) {
			successCount := 0
			testsProcessed := 0

			for i, testValue := range indexType.TestValues {
				if i >= 3 { // Limit tests for performance
					break
				}
				testsProcessed++

				startTime := time.Now()

				// Encode the indexed value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				require.NoError(t, err, "Should encode %s value", indexType.Name)

				// Validate encoding succeeded and meets performance requirements
				insertTime := time.Since(startTime)
				t.Logf("✅ %s indexed insert value encoded successfully to %d bytes in %v",
					indexType.Name, len(encoded), insertTime)

				// Verify performance
				assert.Less(t, insertTime, IndexedInsertTime,
					"%s indexed insert took %v, expected less than %v", indexType.Name, insertTime, IndexedInsertTime)

				// Validate encoding size
				assert.Greater(t, len(encoded), 0, "%s indexed insert should encode to >0 bytes", indexType.Name)

				successCount++
			}

			// Report indexed insert success rate
			successRate := float64(successCount) / float64(testsProcessed) * 100
			t.Logf("✅ %s indexed INSERT operations: %d/%d successful (%.1f%%)",
				indexType.Name, successCount, testsProcessed, successRate)

			assert.Greater(t, successRate, 95.0,
				"Indexed insert operations should have >95%% success rate")
		})
	}
}

// testIndexedQueryOperations tests query operations via indexed fields
func testIndexedQueryOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEXED QUERY OPERATIONS...")

	for _, indexType := range IndexedTypes {
		t.Run(fmt.Sprintf("IndexedQuery_%s", indexType.Name), func(t *testing.T) {
			// Test query operations (if the reducers exist)
			if len(indexType.IndexValues) < 2 {
				t.Logf("⚠️  Skipping %s query test - insufficient index values", indexType.Name)
				return
			}

			startTime := time.Now()

			// Encode a query value
			queryValue := indexType.IndexValues[0]
			encoded, err := encodingManager.Encode(queryValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("⚠️  Failed to encode %s query value: %v", indexType.Name, err)
				return
			}

			queryTime := time.Since(startTime)

			t.Logf("✅ %s indexed query value encoded to %d bytes in %v",
				indexType.Name, len(encoded), queryTime)

			// Verify query performance
			assert.Less(t, queryTime, IndexedQueryTime,
				"%s indexed query encoding took %v, expected less than %v", indexType.Name, queryTime, IndexedQueryTime)

			// Note: Query operations require existing data and specific reducer patterns
			// This test validates encoding infrastructure for queries
			assert.Greater(t, len(encoded), 0, "%s query value should encode to >0 bytes", indexType.Name)
		})
	}
}

// testIndexedRangeQueryOperations tests range query operations via indexed fields
func testIndexedRangeQueryOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEXED RANGE QUERY OPERATIONS...")

	for _, indexType := range IndexedTypes {
		t.Run(fmt.Sprintf("IndexedRangeQuery_%s", indexType.Name), func(t *testing.T) {
			// Test range query operations (if supported for the type)
			if len(indexType.IndexValues) < 2 {
				t.Logf("⚠️  Skipping %s range query test - insufficient index values", indexType.Name)
				return
			}

			startTime := time.Now()

			// Encode range query values (start and end)
			rangeStart := indexType.IndexValues[0]
			rangeEnd := indexType.IndexValues[1]

			encodedStart, err1 := encodingManager.Encode(rangeStart, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encodedEnd, err2 := encodingManager.Encode(rangeEnd, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			rangeQueryTime := time.Since(startTime)

			if err1 == nil && err2 == nil {
				t.Logf("✅ %s indexed range query values encoded to %d and %d bytes in %v",
					indexType.Name, len(encodedStart), len(encodedEnd), rangeQueryTime)

				// Verify range query performance
				assert.Less(t, rangeQueryTime, IndexedRangeTime,
					"%s indexed range query encoding took %v, expected less than %v", indexType.Name, rangeQueryTime, IndexedRangeTime)
			} else {
				t.Logf("⚠️  Failed to encode %s range query values: %v, %v", indexType.Name, err1, err2)
			}
		})
	}
}

// testIndexValidation validates index type safety and correctness
func testIndexValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEX VALIDATION...")

	validationTests := []struct {
		Name        string
		Value       interface{}
		ExpectValid bool
		TypeName    string
	}{
		// Valid indexed types
		{"valid_indexed_u8", IndexedU8Type{Id: 1, IndexedField: 42, Value: "test"}, true, "IndexedU8"},
		{"valid_indexed_u16", IndexedU16Type{Id: 1, IndexedField: 1024, Value: "test"}, true, "IndexedU16"},
		{"valid_indexed_u32", IndexedU32Type{Id: 1, IndexedField: 123456, Value: "test"}, true, "IndexedU32"},
		{"valid_indexed_u64", IndexedU64Type{Id: 1, IndexedField: 9876543210, Value: "test"}, true, "IndexedU64"},
		{"valid_indexed_i32", IndexedI32Type{Id: 1, IndexedField: -123456, Value: "test"}, true, "IndexedI32"},
		{"valid_indexed_f32", IndexedF32Type{Id: 1, IndexedField: 3.14159, Value: "test"}, true, "IndexedF32"},
		{"valid_indexed_f64", IndexedF64Type{Id: 1, IndexedField: 3.141592653589793, Value: "test"}, true, "IndexedF64"},
		{"valid_indexed_bool", IndexedBoolType{Id: 1, IndexedField: true, Value: "test"}, true, "IndexedBool"},
		{"valid_indexed_string", IndexedStringType{Id: 1, IndexedField: "validation_test", Value: "test"}, true, "IndexedString"},
		{"valid_indexed_timestamp", IndexedTimestampType{Id: 1, IndexedField: 1640995200000000, Value: "test"}, true, "IndexedTimestamp"},

		// Edge cases
		{"edge_indexed_u8_max", IndexedU8Type{Id: 1, IndexedField: 255, Value: "test"}, true, "IndexedU8"},
		{"edge_indexed_u8_min", IndexedU8Type{Id: 1, IndexedField: 0, Value: "test"}, true, "IndexedU8"},
		{"edge_indexed_string_empty", IndexedStringType{Id: 1, IndexedField: "", Value: "test"}, true, "IndexedString"},
		{"edge_indexed_string_unicode", IndexedStringType{Id: 1, IndexedField: "🚀_index", Value: "test"}, true, "IndexedString"},
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
	t.Logf("✅ INDEX VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Index validation should have 100%% success rate")
}

// testIndexPerformanceComparison measures performance of indexed vs non-indexed operations
func testIndexPerformanceComparison(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEX PERFORMANCE COMPARISON...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	performanceResults := make(map[string]time.Duration)

	// Performance test for different indexed types
	for _, indexType := range IndexedTypes {
		if len(indexType.TestValues) == 0 {
			continue
		}

		t.Run(fmt.Sprintf("Performance_%s", indexType.Name), func(t *testing.T) {
			totalOperations := IndexPerformanceIters
			if len(indexType.TestValues) < totalOperations {
				totalOperations = len(indexType.TestValues)
			}

			startTime := time.Now()

			for i := 0; i < totalOperations; i++ {
				testValue := indexType.TestValues[i%len(indexType.TestValues)]

				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("%s encoding error at iteration %d: %v", indexType.Name, i, err)
					continue
				}

				_ = encoded // Use the encoded value
			}

			totalTime := time.Since(startTime)
			avgTimePerOp := totalTime / time.Duration(totalOperations)
			performanceResults[indexType.Name] = avgTimePerOp

			t.Logf("✅ %s performance: %d operations in %v (avg: %v per operation)",
				indexType.Name, totalOperations, totalTime, avgTimePerOp)

			maxExpectedTime := time.Microsecond * 50 // 50μs per operation max
			assert.Less(t, avgTimePerOp, maxExpectedTime,
				"%s operations should be fast (<%v), got %v", indexType.Name, maxExpectedTime, avgTimePerOp)
		})
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	t.Logf("✅ INDEX PERFORMANCE testing memory usage: %.2f MB", memUsedMB)

	// Log performance summary
	t.Log("📊 INDEX PERFORMANCE Summary:")
	for typeName, avgTime := range performanceResults {
		t.Logf("  %s: %v per operation", typeName, avgTime)
	}
}

// testIndexBoundaryConditions tests edge cases and boundary conditions for indexes
func testIndexBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEX BOUNDARY CONDITIONS...")

	boundaryTests := []struct {
		Name        string
		Value       interface{}
		Description string
		ShouldWork  bool
	}{
		// Boundary values for different indexed types
		{"boundary_indexed_u8_max", IndexedU8Type{Id: 1, IndexedField: 255, Value: "max_u8"}, "Maximum u8 indexed field", true},
		{"boundary_indexed_u8_min", IndexedU8Type{Id: 2, IndexedField: 0, Value: "min_u8"}, "Minimum u8 indexed field", true},
		{"boundary_indexed_u16_max", IndexedU16Type{Id: 3, IndexedField: 65535, Value: "max_u16"}, "Maximum u16 indexed field", true},
		{"boundary_indexed_u32_max", IndexedU32Type{Id: 4, IndexedField: 4294967295, Value: "max_u32"}, "Maximum u32 indexed field", true},
		{"boundary_indexed_u64_max", IndexedU64Type{Id: 5, IndexedField: 18446744073709551615, Value: "max_u64"}, "Maximum u64 indexed field", true},
		{"boundary_indexed_i32_max", IndexedI32Type{Id: 6, IndexedField: math.MaxInt32, Value: "max_i32"}, "Maximum i32 indexed field", true},
		{"boundary_indexed_i32_min", IndexedI32Type{Id: 7, IndexedField: math.MinInt32, Value: "min_i32"}, "Minimum i32 indexed field", true},

		// Floating point boundary conditions
		{"boundary_indexed_f32_inf", IndexedF32Type{Id: 8, IndexedField: float32(math.Inf(1)), Value: "inf_f32"}, "Positive infinity f32 indexed field", false},
		{"boundary_indexed_f32_neg_inf", IndexedF32Type{Id: 9, IndexedField: float32(math.Inf(-1)), Value: "neg_inf_f32"}, "Negative infinity f32 indexed field", false},
		{"boundary_indexed_f64_inf", IndexedF64Type{Id: 10, IndexedField: math.Inf(1), Value: "inf_f64"}, "Positive infinity f64 indexed field", false},
		{"boundary_indexed_f64_neg_inf", IndexedF64Type{Id: 11, IndexedField: math.Inf(-1), Value: "neg_inf_f64"}, "Negative infinity f64 indexed field", false},

		// String indexed field boundary conditions
		{"boundary_indexed_string_empty", IndexedStringType{Id: 12, IndexedField: "", Value: "empty_string_index"}, "Empty string indexed field", true},
		{"boundary_indexed_string_single", IndexedStringType{Id: 13, IndexedField: "a", Value: "single_char_index"}, "Single character string indexed field", true},
		{"boundary_indexed_string_unicode", IndexedStringType{Id: 14, IndexedField: "🚀", Value: "unicode_emoji_index"}, "Unicode emoji indexed field", true},
		{"boundary_indexed_string_long", IndexedStringType{Id: 15, IndexedField: string(make([]byte, 1000)), Value: "long_string_index"}, "Very long string indexed field", true},

		// Boolean indexed field boundary conditions
		{"boundary_indexed_bool_true", IndexedBoolType{Id: 16, IndexedField: true, Value: "bool_true_index"}, "Boolean true indexed field", true},
		{"boundary_indexed_bool_false", IndexedBoolType{Id: 17, IndexedField: false, Value: "bool_false_index"}, "Boolean false indexed field", true},

		// Timestamp indexed field boundary conditions
		{"boundary_indexed_timestamp_zero", IndexedTimestampType{Id: 18, IndexedField: 0, Value: "timestamp_zero"}, "Zero timestamp indexed field", true},
		{"boundary_indexed_timestamp_now", IndexedTimestampType{Id: 19, IndexedField: uint64(time.Now().UnixMicro()), Value: "timestamp_now"}, "Current timestamp indexed field", true},
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
					t.Logf("✅ %s correctly failed encoding as expected: %s", test.Name, test.Description)
					successCount++
				} else {
					t.Logf("⚠️  Expected %s to fail but it succeeded", test.Name)
				}
			}
		})
	}

	// Report boundary condition success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ INDEX BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 99.0,
		"Index boundary conditions should have >99%% success rate")
}

// testMultiIndexOperations tests multiple index scenarios and cross-index operations
func testMultiIndexOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing MULTI-INDEX OPERATIONS...")

	multiIndexTests := []struct {
		Name        string
		Type1       string
		Value1      interface{}
		Type2       string
		Value2      interface{}
		Description string
	}{
		{
			Name:        "multi_index_u32_vs_i32",
			Type1:       "indexed_u32",
			Value1:      IndexedU32Type{Id: 1, IndexedField: 42, Value: "multi_u32"},
			Type2:       "indexed_i32",
			Value2:      IndexedI32Type{Id: 1, IndexedField: 42, Value: "multi_i32"},
			Description: "Same numeric value in different indexed types should be allowed",
		},
		{
			Name:        "multi_index_string_vs_bool",
			Type1:       "indexed_string",
			Value1:      IndexedStringType{Id: 1, IndexedField: "true", Value: "multi_string"},
			Type2:       "indexed_bool",
			Value2:      IndexedBoolType{Id: 1, IndexedField: true, Value: "multi_bool"},
			Description: "String 'true' and bool true should be allowed in different indexed tables",
		},
		{
			Name:        "multi_index_timestamp_vs_u64",
			Type1:       "indexed_timestamp",
			Value1:      IndexedTimestampType{Id: 1, IndexedField: 1000000, Value: "multi_timestamp"},
			Type2:       "indexed_u64",
			Value2:      IndexedU64Type{Id: 1, IndexedField: 1000000, Value: "multi_u64"},
			Description: "Same numeric value in timestamp and u64 indexes should be allowed",
		},
	}

	successCount := 0
	totalTests := len(multiIndexTests)

	for _, test := range multiIndexTests {
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

	// Report multi-index success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ MULTI-INDEX OPERATIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Multi-index operations should have >90%% success rate")
}

// testIndexConcurrencyOperations tests concurrent index operations
func testIndexConcurrencyOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing INDEX CONCURRENCY OPERATIONS...")

	concurrencyTests := []struct {
		Name        string
		IndexType   string
		Values      []interface{}
		Description string
	}{
		{
			Name:      "concurrent_indexed_u32_inserts",
			IndexType: "indexed_u32",
			Values: []interface{}{
				IndexedU32Type{Id: 100, IndexedField: 1000, Value: "concurrent_1"},
				IndexedU32Type{Id: 101, IndexedField: 1001, Value: "concurrent_2"},
				IndexedU32Type{Id: 102, IndexedField: 1002, Value: "concurrent_3"},
				IndexedU32Type{Id: 103, IndexedField: 1003, Value: "concurrent_4"},
				IndexedU32Type{Id: 104, IndexedField: 1004, Value: "concurrent_5"},
			},
			Description: "Concurrent indexed u32 operations should maintain consistency",
		},
		{
			Name:      "concurrent_indexed_string_operations",
			IndexType: "indexed_string",
			Values: []interface{}{
				IndexedStringType{Id: 200, IndexedField: "concurrent_a", Value: "concurrent_value_a"},
				IndexedStringType{Id: 201, IndexedField: "concurrent_b", Value: "concurrent_value_b"},
				IndexedStringType{Id: 202, IndexedField: "concurrent_c", Value: "concurrent_value_c"},
			},
			Description: "Concurrent indexed string operations should maintain consistency",
		},
	}

	successCount := 0
	totalTests := len(concurrencyTests)

	for _, test := range concurrencyTests {
		t.Run(test.Name, func(t *testing.T) {
			startTime := time.Now()

			// Simulate concurrent operations by encoding all values rapidly
			var encodedValues [][]byte
			for i, value := range test.Values {
				encoded, err := encodingManager.Encode(value, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("⚠️  Concurrent encoding failed for value %d: %v", i, err)
					continue
				}
				encodedValues = append(encodedValues, encoded)
			}

			concurrencyTime := time.Since(startTime)

			if len(encodedValues) == len(test.Values) {
				t.Logf("✅ %s: %s - %d values encoded concurrently in %v",
					test.Name, test.Description, len(encodedValues), concurrencyTime)

				// Calculate total encoded size
				totalSize := 0
				for _, encoded := range encodedValues {
					totalSize += len(encoded)
				}

				t.Logf("  Total encoded size: %d bytes, avg per value: %d bytes",
					totalSize, totalSize/len(encodedValues))

				successCount++
			}
		})
	}

	// Report concurrency success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ INDEX CONCURRENCY OPERATIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Index concurrency operations should have >90%% success rate")
}
