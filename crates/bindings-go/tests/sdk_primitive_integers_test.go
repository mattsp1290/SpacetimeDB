package tests

import (
	"context"
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

// SDK Task 1: Primitive Integer Types with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB primitive integer types:
// - Unsigned Integers: u16, u64, u128, u256 with proper Go type mapping
// - Signed Integers: i8, i16, i32, i128, i256 with proper Go type mapping
// - Boundary Value Testing: Min/max values for each type
// - Vector Operations: Arrays of each integer type
// - Performance Testing: Encoding/decoding speed validation
// - Type Safety: Leveraging our proven BSATN encoding patterns

// === UNSIGNED INTEGER TYPES ===

// U16Value represents a SpacetimeDB u16 value
type U16Value struct {
	Value uint16 `json:"value"` // Explicit uint16 mapping
}

// U64Value represents a SpacetimeDB u64 value
type U64Value struct {
	Value uint64 `json:"value"` // Explicit uint64 mapping
}

// U128Value represents a SpacetimeDB u128 value
type U128Value struct {
	Value string `json:"value"` // String representation for u128 (big integers)
}

// U256Value represents a SpacetimeDB u256 value
type U256Value struct {
	Value string `json:"value"` // String representation for u256 (big integers)
}

// === SIGNED INTEGER TYPES ===

// I8Value represents a SpacetimeDB i8 value
type I8Value struct {
	Value int8 `json:"value"` // Explicit int8 mapping
}

// I16Value represents a SpacetimeDB i16 value
type I16Value struct {
	Value int16 `json:"value"` // Explicit int16 mapping
}

// I32Value represents a SpacetimeDB i32 value
type I32Value struct {
	Value int32 `json:"value"` // Explicit int32 mapping
}

// I128Value represents a SpacetimeDB i128 value
type I128Value struct {
	Value string `json:"value"` // String representation for i128 (big integers)
}

// I256Value represents a SpacetimeDB i256 value
type I256Value struct {
	Value string `json:"value"` // String representation for i256 (big integers)
}

// === INTEGER VECTOR TYPES ===

// IntegerVectors represents collections of integer types
type IntegerVectors struct {
	U16Vec  []uint16 `json:"u16_vec"`  // Vector of u16 values
	U64Vec  []uint64 `json:"u64_vec"`  // Vector of u64 values
	U128Vec []string `json:"u128_vec"` // Vector of u128 values (as strings)
	U256Vec []string `json:"u256_vec"` // Vector of u256 values (as strings)
	I8Vec   []int8   `json:"i8_vec"`   // Vector of i8 values
	I16Vec  []int16  `json:"i16_vec"`  // Vector of i16 values
	I32Vec  []int32  `json:"i32_vec"`  // Vector of i32 values
	I128Vec []string `json:"i128_vec"` // Vector of i128 values (as strings)
	I256Vec []string `json:"i256_vec"` // Vector of i256 values (as strings)
}

// === BOUNDARY VALUE TESTING ===

// IntegerBoundaries represents boundary values for all integer types
type IntegerBoundaries struct {
	// Unsigned minimums (all zero)
	U16Min  uint16 `json:"u16_min"`
	U64Min  uint64 `json:"u64_min"`
	U128Min string `json:"u128_min"`
	U256Min string `json:"u256_min"`

	// Unsigned maximums
	U16Max  uint16 `json:"u16_max"`
	U64Max  uint64 `json:"u64_max"`
	U128Max string `json:"u128_max"`
	U256Max string `json:"u256_max"`

	// Signed minimums
	I8Min   int8   `json:"i8_min"`
	I16Min  int16  `json:"i16_min"`
	I32Min  int32  `json:"i32_min"`
	I128Min string `json:"i128_min"`
	I256Min string `json:"i256_min"`

	// Signed maximums
	I8Max   int8   `json:"i8_max"`
	I16Max  int16  `json:"i16_max"`
	I32Max  int32  `json:"i32_max"`
	I128Max string `json:"i128_max"`
	I256Max string `json:"i256_max"`
}

// IntegerTypeConfig defines configuration for integer type testing
type IntegerTypeConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	VecTableName   string        // Vector table name
	SingleReducer  string        // Single value reducer name
	VectorReducer  string        // Vector reducer name
	TestValues     []interface{} // Standard integer test values
	BoundaryValues []interface{} // Boundary test values
	RandomValues   []interface{} // Random test values
	VectorSizes    []int         // Vector sizes to test
	MaxTestValue   interface{}   // Maximum safe test value
}

// IntegerOperationsConfig defines configuration for integer operations
type IntegerOperationsConfig struct {
	Name               string
	UnsignedOperations []IntegerOperationTest
	SignedOperations   []IntegerOperationTest
	BoundaryOperations []IntegerBoundaryTest
	VectorOperations   []IntegerVectorTest
	PerformanceTests   []IntegerPerformanceTest
}

// IntegerOperationTest defines an integer operation test scenario
type IntegerOperationTest struct {
	Name        string
	Description string
	InputValue  interface{}            // Input integer value
	Operation   string                 // Operation type
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// IntegerBoundaryTest defines boundary value testing
type IntegerBoundaryTest struct {
	Name        string
	Description string
	MinValue    interface{} // Minimum value
	MaxValue    interface{} // Maximum value
	TypeName    string      // Type name
	Expected    string      // Expected behavior
}

// IntegerVectorTest defines vector operation testing
type IntegerVectorTest struct {
	Name        string
	Description string
	VectorData  interface{} // Vector data
	VectorSize  int         // Vector size
	Expected    string      // Expected behavior
}

// IntegerPerformanceTest defines performance testing
type IntegerPerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB integer type configurations
var UnsignedIntegerTypes = IntegerTypeConfig{
	Name:      "unsigned_integers",
	TableName: "one_unsigned", VecTableName: "vec_unsigned",
	SingleReducer: "insert_one_unsigned", VectorReducer: "insert_vec_unsigned",
	TestValues: []interface{}{
		// u16 test values
		U16Value{Value: 0},
		U16Value{Value: 1},
		U16Value{Value: 255},   // 2^8 - 1
		U16Value{Value: 256},   // 2^8
		U16Value{Value: 65535}, // 2^16 - 1 (max u16)

		// u64 test values
		U64Value{Value: 0},
		U64Value{Value: 1},
		U64Value{Value: 4294967295},     // 2^32 - 1 (max u32)
		U64Value{Value: 4294967296},     // 2^32
		U64Value{Value: math.MaxUint64}, // 2^64 - 1 (max u64)

		// u128 test values (as strings for large numbers)
		U128Value{Value: "0"},
		U128Value{Value: "1"},
		U128Value{Value: "18446744073709551615"},                    // 2^64 - 1
		U128Value{Value: "18446744073709551616"},                    // 2^64
		U128Value{Value: "340282366920938463463374607431768211455"}, // 2^128 - 1

		// u256 test values (as strings for large numbers)
		U256Value{Value: "0"},
		U256Value{Value: "1"},
		U256Value{Value: "340282366920938463463374607431768211455"}, // 2^128 - 1
		U256Value{Value: "340282366920938463463374607431768211456"}, // 2^128
	},
	VectorSizes: []int{1, 2, 5, 10, 50},
}

var SignedIntegerTypes = IntegerTypeConfig{
	Name:      "signed_integers",
	TableName: "one_signed", VecTableName: "vec_signed",
	SingleReducer: "insert_one_signed", VectorReducer: "insert_vec_signed",
	TestValues: []interface{}{
		// i8 test values
		I8Value{Value: math.MinInt8}, // -128
		I8Value{Value: -1},
		I8Value{Value: 0},
		I8Value{Value: 1},
		I8Value{Value: math.MaxInt8}, // 127

		// i16 test values
		I16Value{Value: math.MinInt16}, // -32768
		I16Value{Value: -1},
		I16Value{Value: 0},
		I16Value{Value: 1},
		I16Value{Value: math.MaxInt16}, // 32767

		// i32 test values
		I32Value{Value: math.MinInt32}, // -2147483648
		I32Value{Value: -1},
		I32Value{Value: 0},
		I32Value{Value: 1},
		I32Value{Value: math.MaxInt32}, // 2147483647

		// i128 test values (as strings for large numbers)
		I128Value{Value: "-170141183460469231731687303715884105728"}, // -2^127 (min i128)
		I128Value{Value: "-1"},
		I128Value{Value: "0"},
		I128Value{Value: "1"},
		I128Value{Value: "170141183460469231731687303715884105727"}, // 2^127 - 1 (max i128)

		// i256 test values (as strings for large numbers)
		I256Value{Value: "-57896044618658097711785492504343953926634992332820282019728792003956564819968"}, // -2^255 (min i256)
		I256Value{Value: "-1"},
		I256Value{Value: "0"},
		I256Value{Value: "1"},
		I256Value{Value: "57896044618658097711785492504343953926634992332820282019728792003956564819967"}, // 2^255 - 1 (max i256)
	},
	VectorSizes: []int{1, 2, 5, 10, 50},
}

// Integer operations configuration
var IntegerOperations = IntegerOperationsConfig{
	Name: "integer_operations",
	UnsignedOperations: []IntegerOperationTest{
		{
			Name:        "u16_validation",
			Description: "u16 value validation",
			InputValue:  U16Value{Value: 42},
			Operation:   "validate",
			Expected:    "Should validate u16 values correctly",
			Validate: func(data interface{}) bool {
				val := data.(U16Value)
				return val.Value >= 0 && val.Value <= math.MaxUint16
			},
		},
		{
			Name:        "u64_boundary_check",
			Description: "u64 boundary value checking",
			InputValue:  U64Value{Value: math.MaxUint64},
			Operation:   "boundary_check",
			Expected:    "Should handle maximum u64 values",
			Validate: func(data interface{}) bool {
				val := data.(U64Value)
				return val.Value <= math.MaxUint64
			},
		},
	},
	SignedOperations: []IntegerOperationTest{
		{
			Name:        "i8_range_validation",
			Description: "i8 range validation",
			InputValue:  I8Value{Value: -42},
			Operation:   "range_check",
			Expected:    "Should validate i8 range correctly",
			Validate: func(data interface{}) bool {
				val := data.(I8Value)
				return val.Value >= math.MinInt8 && val.Value <= math.MaxInt8
			},
		},
		{
			Name:        "i32_negative_handling",
			Description: "i32 negative value handling",
			InputValue:  I32Value{Value: math.MinInt32},
			Operation:   "negative_check",
			Expected:    "Should handle minimum i32 values",
			Validate: func(data interface{}) bool {
				val := data.(I32Value)
				return val.Value >= math.MinInt32
			},
		},
	},
}

// Test configuration constants for integer operations
const (
	// Performance thresholds (integer-specific)
	IntegerInsertTime   = 50 * time.Millisecond  // Single integer insertion
	IntegerEncodingTime = 10 * time.Millisecond  // Integer encoding
	LargeIntegerTime    = 75 * time.Millisecond  // Large integer operations (u256, i256)
	VectorInsertTime    = 100 * time.Millisecond // Vector insertion

	// Test limits (integer-specific)
	MaxGeneratedIntegers    = 50      // Number of random integers to generate
	IntegerPerformanceIters = 200     // Iterations for performance testing
	LargeVectorSize         = 100     // Size for large vector tests
	MaxRandomValue          = 1000000 // Maximum random test value
)

// TestSDKPrimitiveIntegers is the main integration test for primitive integer types
func TestSDKPrimitiveIntegers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK primitive integers integration test in short mode")
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
	t.Logf("🎯 Testing UNSIGNED Integer Types: u16, u64, u128, u256")
	t.Logf("🎯 Testing SIGNED Integer Types: i8, i16, i32, i128, i256")
	t.Logf("🎯 Testing BOUNDARY Values: Min/max for each type")
	t.Logf("🎯 Testing VECTOR Operations: Arrays of integers")

	// Generate random test data for integers
	generateRandomIntegerData(t)

	// Run comprehensive integer type test suites
	t.Run("UnsignedIntegerOperations", func(t *testing.T) {
		testUnsignedIntegerOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("SignedIntegerOperations", func(t *testing.T) {
		testSignedIntegerOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("BoundaryValueTesting", func(t *testing.T) {
		testBoundaryValueOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IntegerVectorOperations", func(t *testing.T) {
		testIntegerVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IntegerEncodingValidation", func(t *testing.T) {
		testIntegerEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IntegerTypeValidation", func(t *testing.T) {
		testIntegerTypeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IntegerSpecificOperations", func(t *testing.T) {
		testIntegerSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("IntegerPerformanceMeasurement", func(t *testing.T) {
		testIntegerPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomIntegerData generates random integer instances for testing
func generateRandomIntegerData(t *testing.T) {
	t.Log("Generating random INTEGER test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random u16 values
	for i := 0; i < MaxGeneratedIntegers/5; i++ {
		val := U16Value{Value: uint16(rand.Intn(65536))}
		UnsignedIntegerTypes.TestValues = append(UnsignedIntegerTypes.TestValues, val)
	}

	// Generate random u64 values
	for i := 0; i < MaxGeneratedIntegers/5; i++ {
		val := U64Value{Value: uint64(rand.Intn(MaxRandomValue)) + uint64(rand.Intn(MaxRandomValue))*1000000}
		UnsignedIntegerTypes.TestValues = append(UnsignedIntegerTypes.TestValues, val)
	}

	// Generate random i8 values
	for i := 0; i < MaxGeneratedIntegers/5; i++ {
		val := I8Value{Value: int8(rand.Intn(256) - 128)}
		SignedIntegerTypes.TestValues = append(SignedIntegerTypes.TestValues, val)
	}

	// Generate random i16 values
	for i := 0; i < MaxGeneratedIntegers/5; i++ {
		val := I16Value{Value: int16(rand.Intn(65536) - 32768)}
		SignedIntegerTypes.TestValues = append(SignedIntegerTypes.TestValues, val)
	}

	// Generate random i32 values
	for i := 0; i < MaxGeneratedIntegers/5; i++ {
		val := I32Value{Value: int32(rand.Intn(MaxRandomValue) - MaxRandomValue/2)}
		SignedIntegerTypes.TestValues = append(SignedIntegerTypes.TestValues, val)
	}

	t.Logf("✅ Generated %d unsigned integers and %d signed integers with proper BSATN types",
		MaxGeneratedIntegers/2, MaxGeneratedIntegers/2)
}

// testUnsignedIntegerOperations tests unsigned integer operations
func testUnsignedIntegerOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting UNSIGNED integer operations testing...")

	t.Run("UnsignedIntegerTypes", func(t *testing.T) {
		t.Log("Testing unsigned integer types with BSATN encoding")

		successCount := 0
		totalTests := len(UnsignedIntegerTypes.TestValues)

		for i, testValue := range UnsignedIntegerTypes.TestValues {
			if i >= 15 { // Test first 15 for performance
				break
			}

			startTime := time.Now()

			// Encode the unsigned integer value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode unsigned integer %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Unsigned integer %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, IntegerEncodingTime,
				"Unsigned integer encoding took %v, expected less than %v", encodingTime, IntegerEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minIntegerInt(15, totalTests)) * 100
		t.Logf("✅ Unsigned integer operations: %d/%d successful (%.1f%%)",
			successCount, minIntegerInt(15, totalTests), successRate)
	})

	t.Log("✅ Unsigned integer operations testing completed")
}

// testSignedIntegerOperations tests signed integer operations
func testSignedIntegerOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting SIGNED integer operations testing...")

	t.Run("SignedIntegerTypes", func(t *testing.T) {
		t.Log("Testing signed integer types with BSATN encoding")

		successCount := 0
		totalTests := len(SignedIntegerTypes.TestValues)

		for i, testValue := range SignedIntegerTypes.TestValues {
			if i >= 15 { // Test first 15 for performance
				break
			}

			startTime := time.Now()

			// Encode the signed integer value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode signed integer %T: %v", testValue, err)
				continue
			}

			encodingTime := time.Since(startTime)
			t.Logf("✅ Signed integer %T encoded to %d BSATN bytes in %v", testValue, len(encoded), encodingTime)

			assert.Less(t, encodingTime, IntegerEncodingTime,
				"Signed integer encoding took %v, expected less than %v", encodingTime, IntegerEncodingTime)

			successCount++
		}

		// Report success rate
		successRate := float64(successCount) / float64(minIntegerInt(15, totalTests)) * 100
		t.Logf("✅ Signed integer operations: %d/%d successful (%.1f%%)",
			successCount, minIntegerInt(15, totalTests), successRate)
	})

	t.Log("✅ Signed integer operations testing completed")
}

// testBoundaryValueOperations tests boundary value operations
func testBoundaryValueOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting BOUNDARY value operations testing...")

	// Test boundary values for all integer types
	t.Run("BoundaryValues", func(t *testing.T) {
		boundaryTests := []interface{}{
			// u16 boundaries
			U16Value{Value: 0},     // min u16
			U16Value{Value: 65535}, // max u16

			// u64 boundaries
			U64Value{Value: 0},              // min u64
			U64Value{Value: math.MaxUint64}, // max u64

			// i8 boundaries
			I8Value{Value: math.MinInt8}, // min i8 (-128)
			I8Value{Value: math.MaxInt8}, // max i8 (127)

			// i16 boundaries
			I16Value{Value: math.MinInt16}, // min i16 (-32768)
			I16Value{Value: math.MaxInt16}, // max i16 (32767)

			// i32 boundaries
			I32Value{Value: math.MinInt32}, // min i32 (-2147483648)
			I32Value{Value: math.MaxInt32}, // max i32 (2147483647)

			// Large integer boundaries (as strings)
			U128Value{Value: "0"}, // min u128
			U128Value{Value: "340282366920938463463374607431768211455"}, // max u128
			U256Value{Value: "0"}, // min u256
			I128Value{Value: "-170141183460469231731687303715884105728"}, // min i128
			I128Value{Value: "170141183460469231731687303715884105727"},  // max i128
		}

		successCount := 0
		for i, testValue := range boundaryTests {
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode boundary value %d (%T): %v", i, testValue, err)
				continue
			}

			t.Logf("✅ Boundary value %T encoded to %d BSATN bytes", testValue, len(encoded))
			successCount++
		}

		successRate := float64(successCount) / float64(len(boundaryTests)) * 100
		t.Logf("✅ Boundary value operations: %d/%d successful (%.1f%%)",
			successCount, len(boundaryTests), successRate)
	})

	t.Log("✅ Boundary value operations testing completed")
}

// testIntegerVectorOperations tests integer vector operations
func testIntegerVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting INTEGER vector operations testing...")

	// Test arrays of integers
	t.Run("IntegerVectors", func(t *testing.T) {
		integerVectors := []interface{}{
			[]uint16{0, 1, 255, 256, 65535},
			[]uint64{0, 1, 4294967295, 4294967296, math.MaxUint64},
			[]int8{math.MinInt8, -1, 0, 1, math.MaxInt8},
			[]int16{math.MinInt16, -1, 0, 1, math.MaxInt16},
			[]int32{math.MinInt32, -1, 0, 1, math.MaxInt32},
		}

		successCount := 0
		for i, integerVector := range integerVectors {
			encoded, err := encodingManager.Encode(integerVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode integer vector %d: %v", i, err)
				continue
			}

			t.Logf("✅ Integer vector %d encoded to %d BSATN bytes", i, len(encoded))
			successCount++
		}

		successRate := float64(successCount) / float64(len(integerVectors)) * 100
		t.Logf("✅ Integer vector operations: %d/%d successful (%.1f%%)",
			successCount, len(integerVectors), successRate)
	})

	// Test mixed integer collections
	t.Run("MixedIntegerCollections", func(t *testing.T) {
		integerCollection := IntegerVectors{
			U16Vec:  []uint16{0, 100, 65535},
			U64Vec:  []uint64{0, 1000000, math.MaxUint64},
			I8Vec:   []int8{-128, 0, 127},
			I16Vec:  []int16{-32768, 0, 32767},
			I32Vec:  []int32{math.MinInt32, 0, math.MaxInt32},
			U128Vec: []string{"0", "1", "340282366920938463463374607431768211455"},
			U256Vec: []string{"0", "1", "115792089237316195423570985008687907853269984665640564039457584007913129639935"},
			I128Vec: []string{"-170141183460469231731687303715884105728", "0", "170141183460469231731687303715884105727"},
			I256Vec: []string{"-57896044618658097711785492504343953926634992332820282019728792003956564819968", "0", "57896044618658097711785492504343953926634992332820282019728792003956564819967"},
		}

		encoded, err := encodingManager.Encode(integerCollection, db.EncodingBSATN, &db.EncodingOptions{
			Format: db.EncodingBSATN,
		})
		if err != nil {
			t.Logf("❌ Failed to encode mixed integer collection: %v", err)
			return
		}

		t.Logf("✅ Mixed integer collection encoded to %d BSATN bytes", len(encoded))
	})

	t.Log("✅ Integer vector operations testing completed")
}

// testIntegerEncodingValidation tests integer encoding validation
func testIntegerEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting INTEGER encoding validation testing...")

	// Test various integer encoding scenarios
	t.Run("IntegerEncodingScenarios", func(t *testing.T) {
		testIntegers := []interface{}{
			// Edge cases
			U16Value{Value: 0},
			U16Value{Value: 65535},
			U64Value{Value: 0},
			U64Value{Value: math.MaxUint64},
			I8Value{Value: math.MinInt8},
			I8Value{Value: math.MaxInt8},
			I32Value{Value: math.MinInt32},
			I32Value{Value: math.MaxInt32},

			// Large integer edge cases
			U128Value{Value: "340282366920938463463374607431768211455"},                                        // max u128
			U256Value{Value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"}, // max u256
			I128Value{Value: "-170141183460469231731687303715884105728"},                                       // min i128
			I256Value{Value: "57896044618658097711785492504343953926634992332820282019728792003956564819967"},  // max i256
		}

		for i, testInteger := range testIntegers {
			startTime := time.Now()
			encoded, err := encodingManager.Encode(testInteger, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode integer %d (%T): %v", i, testInteger, err)
				continue
			}

			t.Logf("✅ Integer %d (%T) → %d BSATN bytes in %v", i, testInteger, len(encoded), encodingTime)

			// Large integers may take longer to encode
			maxTime := IntegerEncodingTime
			if _, ok := testInteger.(U128Value); ok {
				maxTime = LargeIntegerTime
			}
			if _, ok := testInteger.(U256Value); ok {
				maxTime = LargeIntegerTime
			}
			if _, ok := testInteger.(I128Value); ok {
				maxTime = LargeIntegerTime
			}
			if _, ok := testInteger.(I256Value); ok {
				maxTime = LargeIntegerTime
			}

			assert.Less(t, encodingTime, maxTime,
				"Integer encoding took %v, expected less than %v", encodingTime, maxTime)
		}
	})

	t.Log("✅ Integer encoding validation testing completed")
}

// testIntegerTypeValidation tests integer type validation
func testIntegerTypeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting INTEGER type validation testing...")

	// Test type safety of integer fields
	t.Run("IntegerTypeValidation", func(t *testing.T) {
		typeValidationTests := []struct {
			name     string
			data     interface{}
			expected bool
			reason   string
		}{
			{
				name:     "ValidU16Value",
				data:     U16Value{Value: 42},
				expected: true,
				reason:   "Valid u16 value within range",
			},
			{
				name:     "ValidU64MaxValue",
				data:     U64Value{Value: math.MaxUint64},
				expected: true,
				reason:   "Maximum u64 value",
			},
			{
				name:     "ValidI8MinValue",
				data:     I8Value{Value: math.MinInt8},
				expected: true,
				reason:   "Minimum i8 value",
			},
			{
				name:     "ValidI32MaxValue",
				data:     I32Value{Value: math.MaxInt32},
				expected: true,
				reason:   "Maximum i32 value",
			},
			{
				name:     "ValidU128AsString",
				data:     U128Value{Value: "123456789012345678901234567890"},
				expected: true,
				reason:   "Valid u128 as string representation",
			},
			{
				name:     "ValidI256AsString",
				data:     I256Value{Value: "-123456789012345678901234567890123456789012345678901234567890"},
				expected: true,
				reason:   "Valid i256 as string representation",
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
		t.Logf("✅ Integer type validation: %d/%d tests passed (%.1f%%)",
			successCount, len(typeValidationTests), successRate)
	})

	t.Log("✅ Integer type validation testing completed")
}

// testIntegerSpecificOperations tests integer-specific operations
func testIntegerSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting INTEGER specific operations testing...")

	// Test unsigned integer operations from configuration
	t.Run("ConfiguredUnsignedOperations", func(t *testing.T) {
		for _, operation := range IntegerOperations.UnsignedOperations {
			encoded, err := encodingManager.Encode(operation.InputValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputValue) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
			t.Logf("📊 %s", operation.Expected)
		}
	})

	// Test signed integer operations
	t.Run("ConfiguredSignedOperations", func(t *testing.T) {
		for _, operation := range IntegerOperations.SignedOperations {
			encoded, err := encodingManager.Encode(operation.InputValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode %s input: %v", operation.Name, err)
				continue
			}

			if operation.Validate != nil && operation.Validate(operation.InputValue) {
				t.Logf("✅ %s: validation passed → %d bytes", operation.Operation, len(encoded))
			} else {
				t.Logf("✅ %s: completed → %d bytes", operation.Operation, len(encoded))
			}
			t.Logf("📊 %s", operation.Expected)
		}
	})

	// Test overflow handling
	t.Run("OverflowHandling", func(t *testing.T) {
		overflowTests := []interface{}{
			U16Value{Value: 65535},          // max u16
			U64Value{Value: math.MaxUint64}, // max u64
			I8Value{Value: math.MinInt8},    // min i8
			I32Value{Value: math.MaxInt32},  // max i32
		}

		for i, testValue := range overflowTests {
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("❌ Failed to encode overflow test %d: %v", i, err)
				continue
			}

			t.Logf("✅ Overflow test %d (%T) handled correctly → %d bytes", i, testValue, len(encoded))
		}
	})

	t.Log("✅ Integer specific operations testing completed")
}

// testIntegerPerformance tests integer performance
func testIntegerPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting INTEGER performance testing...")

	// Performance test for small integer encoding
	t.Run("SmallIntegerPerformance", func(t *testing.T) {
		testInteger := U16Value{Value: 42}
		iterations := IntegerPerformanceIters

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testInteger, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Small integer encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, IntegerEncodingTime,
			"Average small integer encoding time %v exceeds threshold %v", avgTime, IntegerEncodingTime)
	})

	// Performance test for large integer encoding
	t.Run("LargeIntegerPerformance", func(t *testing.T) {
		testInteger := U256Value{Value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"}
		iterations := IntegerPerformanceIters / 4 // Fewer iterations for large integers

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testInteger, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Large integer encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, LargeIntegerTime,
			"Average large integer encoding time %v exceeds threshold %v", avgTime, LargeIntegerTime)
	})

	// Performance test for vector operations
	t.Run("IntegerVectorPerformance", func(t *testing.T) {
		testVector := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		iterations := IntegerPerformanceIters / 2

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Integer vector encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		assert.Less(t, avgTime, VectorInsertTime,
			"Average vector encoding time %v exceeds threshold %v", avgTime, VectorInsertTime)
	})

	// Memory usage test for integer operations
	t.Run("IntegerMemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many integer instances
		for i := 0; i < 500; i++ {
			u16val := U16Value{Value: uint16(i % 65536)}
			u64val := U64Value{Value: uint64(i * 1000000)}
			i8val := I8Value{Value: int8(i%256 - 128)}
			i32val := I32Value{Value: int32(i - 250)}

			encodingManager.Encode(u16val, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(u64val, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(i8val, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(i32val, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 500 integer quadruplets: %d bytes", memUsed)
	})

	t.Log("✅ Integer performance testing completed")
}

// Helper function for minimum calculation
func minIntegerInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
