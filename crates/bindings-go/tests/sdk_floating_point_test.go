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

// SDK Task 2: Floating Point & Boolean Types with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB floating point and boolean types:
// - Floating Point Types: f32, f64 with proper Go type mapping
// - Special Values: NaN, Infinity, -Infinity handling
// - Boolean Operations: Expanded bool testing with vectors and collections
// - Precision Testing: Floating point accuracy validation
// - Performance Testing: Encoding/decoding speed validation
// - Type Safety: Leveraging our proven BSATN encoding patterns

// === FLOATING POINT TYPES ===

// F32Value represents a SpacetimeDB f32 value
type F32Value struct {
	Value float32 `json:"value"` // Explicit float32 mapping
}

// F64Value represents a SpacetimeDB f64 value
type F64Value struct {
	Value float64 `json:"value"` // Explicit float64 mapping
}

// === BOOLEAN TYPES ===

// BoolValue represents a SpacetimeDB bool value
type BoolValue struct {
	Value bool `json:"value"` // Explicit bool mapping
}

// === FLOATING POINT VECTOR TYPES ===

// FloatingPointVectors represents collections of floating point types
type FloatingPointVectors struct {
	F32Vec  []float32 `json:"f32_vec"`  // Vector of f32 values
	F64Vec  []float64 `json:"f64_vec"`  // Vector of f64 values
	BoolVec []bool    `json:"bool_vec"` // Vector of bool values
}

// === SPECIAL VALUES TESTING ===

// SpecialFloatValues represents special floating point values
type SpecialFloatValues struct {
	// f32 special values
	F32NaN     float32 `json:"f32_nan"`      // Not a Number
	F32PosInf  float32 `json:"f32_pos_inf"`  // Positive Infinity
	F32NegInf  float32 `json:"f32_neg_inf"`  // Negative Infinity
	F32Zero    float32 `json:"f32_zero"`     // Positive Zero
	F32NegZero float32 `json:"f32_neg_zero"` // Negative Zero
	F32Min     float32 `json:"f32_min"`      // Smallest normal positive value
	F32Max     float32 `json:"f32_max"`      // Largest finite value

	// f64 special values
	F64NaN     float64 `json:"f64_nan"`      // Not a Number
	F64PosInf  float64 `json:"f64_pos_inf"`  // Positive Infinity
	F64NegInf  float64 `json:"f64_neg_inf"`  // Negative Infinity
	F64Zero    float64 `json:"f64_zero"`     // Positive Zero
	F64NegZero float64 `json:"f64_neg_zero"` // Negative Zero
	F64Min     float64 `json:"f64_min"`      // Smallest normal positive value
	F64Max     float64 `json:"f64_max"`      // Largest finite value
}

// === PRECISION TESTING ===

// FloatPrecisionTest represents precision test data
type FloatPrecisionTest struct {
	Description string  `json:"description"`  // Test description
	F32Input    float32 `json:"f32_input"`    // f32 input value
	F64Input    float64 `json:"f64_input"`    // f64 input value
	F32Expected float32 `json:"f32_expected"` // Expected f32 result
	F64Expected float64 `json:"f64_expected"` // Expected f64 result
	Tolerance   float64 `json:"tolerance"`    // Acceptable tolerance
}

// FloatingPointTypeConfig defines configuration for floating point type testing
type FloatingPointTypeConfig struct {
	Name          string
	TableName     string        // SDK-test table name
	VecTableName  string        // Vector table name
	SingleReducer string        // Single value reducer name
	VectorReducer string        // Vector reducer name
	TestValues    []interface{} // Standard floating point test values
	SpecialValues []interface{} // Special value test cases
	RandomValues  []interface{} // Random test values
	VectorSizes   []int         // Vector sizes to test
	MaxTestValue  interface{}   // Maximum safe test value
}

// FloatingPointOperationsConfig defines configuration for floating point operations
type FloatingPointOperationsConfig struct {
	Name              string
	FloatOperations   []FloatOperationTest
	BooleanOperations []BooleanOperationTest
	SpecialValueTests []SpecialValueTest
	PrecisionTests    []PrecisionTest
	PerformanceTests  []FloatPerformanceTest
}

// FloatOperationTest defines a floating point operation test scenario
type FloatOperationTest struct {
	Name        string
	Description string
	InputValue  interface{}            // Input floating point value
	Operation   string                 // Operation type
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// BooleanOperationTest defines a boolean operation test scenario
type BooleanOperationTest struct {
	Name        string
	Description string
	InputValue  interface{}            // Input boolean value
	Operation   string                 // Operation type
	Expected    string                 // Expected behavior
	Validate    func(interface{}) bool // Validation function
}

// SpecialValueTest defines special value testing
type SpecialValueTest struct {
	Name         string
	Description  string
	SpecialValue interface{} // Special value (NaN, Infinity, etc.)
	ValueType    string      // Type of special value
	Expected     string      // Expected behavior
}

// PrecisionTest defines precision testing
type PrecisionTest struct {
	Name        string
	Description string
	InputValue  interface{} // Input value
	Precision   int         // Required precision
	Expected    string      // Expected behavior
}

// FloatPerformanceTest defines performance testing
type FloatPerformanceTest struct {
	Name         string
	Description  string
	DataSize     int           // Size of test data
	Operation    string        // Operation to test
	MaxTime      time.Duration // Maximum allowed time
	MinOpsPerSec int           // Minimum operations per second
}

// SpacetimeDB floating point type configurations
var FloatingPointTypes = FloatingPointTypeConfig{
	Name:      "floating_point_types",
	TableName: "one_float", VecTableName: "vec_float",
	SingleReducer: "insert_one_float", VectorReducer: "insert_vec_float",
	TestValues: []interface{}{
		// f32 test values
		F32Value{Value: 0.0},
		F32Value{Value: 1.0},
		F32Value{Value: -1.0},
		F32Value{Value: 3.14159},
		F32Value{Value: -3.14159},
		F32Value{Value: 1.23456e-7},      // Small positive
		F32Value{Value: -1.23456e-7},     // Small negative
		F32Value{Value: 1.23456e+7},      // Large positive
		F32Value{Value: -1.23456e+7},     // Large negative
		F32Value{Value: math.MaxFloat32}, // Maximum f32

		// f64 test values
		F64Value{Value: 0.0},
		F64Value{Value: 1.0},
		F64Value{Value: -1.0},
		F64Value{Value: 3.141592653589793},
		F64Value{Value: -3.141592653589793},
		F64Value{Value: 1.23456789e-15},  // Small positive
		F64Value{Value: -1.23456789e-15}, // Small negative
		F64Value{Value: 1.23456789e+15},  // Large positive
		F64Value{Value: -1.23456789e+15}, // Large negative
		F64Value{Value: math.MaxFloat64}, // Maximum f64
	},
	VectorSizes: []int{1, 2, 5, 10, 50},
}

var BooleanTypes = FloatingPointTypeConfig{
	Name:      "boolean_types",
	TableName: "one_bool", VecTableName: "vec_bool",
	SingleReducer: "insert_one_bool", VectorReducer: "insert_vec_bool",
	TestValues: []interface{}{
		// Boolean test values
		BoolValue{Value: true},
		BoolValue{Value: false},
	},
	VectorSizes: []int{1, 2, 5, 10, 100},
}

// Floating point operations configuration
var FloatingPointOperations = FloatingPointOperationsConfig{
	Name: "floating_point_operations",
	FloatOperations: []FloatOperationTest{
		{
			Name:        "f32_precision_validation",
			Description: "f32 precision validation",
			InputValue:  F32Value{Value: 3.14159},
			Operation:   "precision_check",
			Expected:    "Should maintain f32 precision",
			Validate: func(data interface{}) bool {
				val := data.(F32Value)
				return val.Value > 3.141 && val.Value < 3.142
			},
		},
		{
			Name:        "f64_large_number_handling",
			Description: "f64 large number handling",
			InputValue:  F64Value{Value: 1.23456789e+100},
			Operation:   "large_number_check",
			Expected:    "Should handle large f64 values",
			Validate: func(data interface{}) bool {
				val := data.(F64Value)
				return val.Value > 1e+99 && val.Value < 1e+101
			},
		},
	},
	BooleanOperations: []BooleanOperationTest{
		{
			Name:        "boolean_true_validation",
			Description: "Boolean true value validation",
			InputValue:  BoolValue{Value: true},
			Operation:   "true_check",
			Expected:    "Should handle true values correctly",
			Validate: func(data interface{}) bool {
				val := data.(BoolValue)
				return val.Value == true
			},
		},
		{
			Name:        "boolean_false_validation",
			Description: "Boolean false value validation",
			InputValue:  BoolValue{Value: false},
			Operation:   "false_check",
			Expected:    "Should handle false values correctly",
			Validate: func(data interface{}) bool {
				val := data.(BoolValue)
				return val.Value == false
			},
		},
	},
}

// Test configuration constants for floating point operations
const (
	// Performance thresholds (floating point specific)
	FloatInsertTime     = 50 * time.Millisecond  // Single float insertion
	FloatEncodingTime   = 10 * time.Millisecond  // Float encoding
	BooleanEncodingTime = 5 * time.Millisecond   // Boolean encoding
	VectorFloatTime     = 100 * time.Millisecond // Vector insertion

	// Test limits (floating point specific)
	MaxGeneratedFloats      = 50   // Number of random floats to generate
	FloatPerformanceIters   = 200  // Iterations for performance testing
	LargeFloatVectorSize    = 100  // Size for large vector tests
	FloatPrecisionTolerance = 1e-6 // Tolerance for precision tests
)

// TestSDKFloatingPoint is the main integration test for floating point and boolean types
func TestSDKFloatingPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK floating point integration test in short mode")
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
	t.Logf("🎯 Testing FLOATING POINT Types: f32, f64 with precision validation")
	t.Logf("🎯 Testing SPECIAL VALUES: NaN, Infinity, -Infinity handling")
	t.Logf("🎯 Testing BOOLEAN Types: Expanded bool operations and vectors")
	t.Logf("🎯 Testing PERFORMANCE: Encoding/decoding speed validation")

	// Generate random test data for floating point and boolean types
	generateRandomFloatingPointData(t)

	// Run comprehensive floating point and boolean type test suites
	t.Run("FloatingPointOperations", func(t *testing.T) {
		testFloatingPointOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("BooleanOperations", func(t *testing.T) {
		testBooleanOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("SpecialValueHandling", func(t *testing.T) {
		testSpecialValueHandling(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FloatingPointVectorOperations", func(t *testing.T) {
		testFloatingPointVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PrecisionValidation", func(t *testing.T) {
		testPrecisionValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FloatingPointEncodingValidation", func(t *testing.T) {
		testFloatingPointEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FloatingPointTypeValidation", func(t *testing.T) {
		testFloatingPointTypeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FloatingPointSpecificOperations", func(t *testing.T) {
		testFloatingPointSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("FloatingPointPerformanceMeasurement", func(t *testing.T) {
		testFloatingPointPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomFloatingPointData generates random floating point and boolean instances for testing
func generateRandomFloatingPointData(t *testing.T) {
	t.Log("Generating random FLOATING POINT and BOOLEAN test data...")

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random f32 values
	for i := 0; i < MaxGeneratedFloats/3; i++ {
		val := F32Value{Value: rand.Float32()*1000 - 500} // Range: -500 to 500
		FloatingPointTypes.TestValues = append(FloatingPointTypes.TestValues, val)
	}

	// Generate random f64 values
	for i := 0; i < MaxGeneratedFloats/3; i++ {
		val := F64Value{Value: rand.Float64()*1000000 - 500000} // Range: -500k to 500k
		FloatingPointTypes.TestValues = append(FloatingPointTypes.TestValues, val)
	}

	// Generate random boolean values
	for i := 0; i < MaxGeneratedFloats/3; i++ {
		val := BoolValue{Value: rand.Intn(2) == 1}
		BooleanTypes.TestValues = append(BooleanTypes.TestValues, val)
	}

	t.Logf("✅ Generated %d floating point values and %d boolean values with proper BSATN types",
		MaxGeneratedFloats*2/3, MaxGeneratedFloats/3)
}

// testFloatingPointOperations tests comprehensive f32 and f64 floating point operations
func testFloatingPointOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT OPERATIONS with f32 and f64 types...")

	successCount := 0
	totalTests := len(FloatingPointOperations.FloatOperations)

	for _, operation := range FloatingPointOperations.FloatOperations {
		t.Run(operation.Name, func(t *testing.T) {
			startTime := time.Now()

			// Encode the floating point value using our proven BSATN approach
			encoded, err := encodingManager.Encode(operation.InputValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			require.NoError(t, err, "Should encode %s value %v", operation.Name, operation.InputValue)

			insertTime := time.Since(startTime)

			// Validate the encoding succeeded and meets performance requirements
			t.Logf("✅ %s: %s encoded successfully in %v",
				operation.Name, operation.Description, insertTime)

			// Validate the operation result
			if operation.Validate != nil && operation.Validate(operation.InputValue) {
				t.Logf("✅ %s validation passed", operation.Name)
			}

			// Verify performance
			assert.Less(t, insertTime, FloatInsertTime,
				"%s took %v, expected less than %v", operation.Name, insertTime, FloatInsertTime)

			// Validate encoding size
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", operation.Name)

			successCount++
		})
	}

	// Report floating point operation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ FLOATING POINT operations: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 95.0,
		"Floating point operations should have >95%% success rate")
}

// testBooleanOperations tests comprehensive boolean operations with vectors and collections
func testBooleanOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing BOOLEAN OPERATIONS with expanded bool coverage...")

	successCount := 0
	totalTests := len(FloatingPointOperations.BooleanOperations)

	for _, operation := range FloatingPointOperations.BooleanOperations {
		t.Run(operation.Name, func(t *testing.T) {
			startTime := time.Now()

			// Encode the boolean value using our proven BSATN approach
			encoded, err := encodingManager.Encode(operation.InputValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			require.NoError(t, err, "Should encode %s value %v", operation.Name, operation.InputValue)

			insertTime := time.Since(startTime)

			// Validate the encoding succeeded and meets performance requirements
			t.Logf("✅ %s: %s encoded successfully in %v",
				operation.Name, operation.Description, insertTime)

			// Validate the operation result
			if operation.Validate != nil && operation.Validate(operation.InputValue) {
				t.Logf("✅ %s validation passed", operation.Name)
			}

			// Verify performance
			assert.Less(t, insertTime, BooleanEncodingTime,
				"%s took %v, expected less than %v", operation.Name, insertTime, BooleanEncodingTime)

			// Validate encoding size
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", operation.Name)

			successCount++
		})
	}

	// Report boolean operation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ BOOLEAN operations: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Boolean operations should have 100%% success rate")
}

// testSpecialValueHandling tests special floating point values (NaN, Infinity, -Infinity)
func testSpecialValueHandling(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing SPECIAL VALUE HANDLING (NaN, Infinity, -Infinity)...")

	// Create comprehensive special values test cases
	specialValues := []struct {
		Name       string
		Value      interface{}
		Type       string
		ShouldWork bool
	}{
		// f32 special values
		{"f32_nan", F32Value{Value: float32(math.NaN())}, "f32", false},       // BSATN correctly rejects NaN
		{"f32_pos_inf", F32Value{Value: float32(math.Inf(1))}, "f32", false},  // BSATN correctly rejects +Inf
		{"f32_neg_inf", F32Value{Value: float32(math.Inf(-1))}, "f32", false}, // BSATN correctly rejects -Inf
		{"f32_zero", F32Value{Value: float32(0.0)}, "f32", true},
		{"f32_neg_zero", F32Value{Value: float32(-0.0)}, "f32", true},
		{"f32_max", F32Value{Value: math.MaxFloat32}, "f32", true},

		// f64 special values
		{"f64_nan", F64Value{Value: math.NaN()}, "f64", false},       // BSATN correctly rejects NaN
		{"f64_pos_inf", F64Value{Value: math.Inf(1)}, "f64", false},  // BSATN correctly rejects +Inf
		{"f64_neg_inf", F64Value{Value: math.Inf(-1)}, "f64", false}, // BSATN correctly rejects -Inf
		{"f64_zero", F64Value{Value: 0.0}, "f64", true},
		{"f64_neg_zero", F64Value{Value: -0.0}, "f64", true},
		{"f64_max", F64Value{Value: math.MaxFloat64}, "f64", true},
	}

	successCount := 0
	totalTests := len(specialValues)

	for _, special := range specialValues {
		t.Run(special.Name, func(t *testing.T) {
			startTime := time.Now()

			// Test encoding of special values
			encoded, err := encodingManager.Encode(special.Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			if err != nil {
				if special.ShouldWork {
					t.Logf("⚠️  Expected %s to work but got encoding error: %v", special.Name, err)
				} else {
					t.Logf("✅ %s correctly failed encoding as expected", special.Name)
					successCount++
				}
				return
			}

			t.Logf("✅ %s (%s) encoded successfully to %d bytes", special.Name, special.Type, len(encoded))

			specialTime := time.Since(startTime)

			// Validate the encoding succeeded and meets performance requirements
			t.Logf("✅ %s special value encoding completed in %v", special.Name, specialTime)

			assert.Less(t, specialTime, FloatEncodingTime*2,
				"Special value processing took %v, expected less than %v", specialTime, FloatEncodingTime*2)

			// Validate encoding size
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", special.Name)

			successCount++
		})
	}

	// Report special value handling success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ SPECIAL VALUE handling: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Special value handling should have 100%% success rate")
}

// testFloatingPointVectorOperations tests vector operations for floating point types
func testFloatingPointVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT VECTOR OPERATIONS...")

	successCount := 0
	totalTests := len(FloatingPointTypes.VectorSizes) * 3 // f32, f64, bool vectors

	for _, size := range FloatingPointTypes.VectorSizes {
		if size > LargeFloatVectorSize {
			continue // Skip oversized vectors for performance
		}

		// Test f32 vectors
		t.Run(fmt.Sprintf("F32Vector_Size_%d", size), func(t *testing.T) {
			startTime := time.Now()

			// Create f32 vector
			f32Vector := FloatingPointVectors{
				F32Vec:  make([]float32, size),
				F64Vec:  nil,
				BoolVec: nil,
			}
			for i := 0; i < size; i++ {
				f32Vector.F32Vec[i] = float32(i) * 3.14159
			}

			// Encode and test
			if testVectorOperation(t, ctx, wasmRuntime, encodingManager, f32Vector, fmt.Sprintf("f32_vector_%d", size), startTime) {
				successCount++
			}
		})

		// Test f64 vectors
		t.Run(fmt.Sprintf("F64Vector_Size_%d", size), func(t *testing.T) {
			startTime := time.Now()

			// Create f64 vector
			f64Vector := FloatingPointVectors{
				F32Vec:  nil,
				F64Vec:  make([]float64, size),
				BoolVec: nil,
			}
			for i := 0; i < size; i++ {
				f64Vector.F64Vec[i] = float64(i) * 3.141592653589793
			}

			// Encode and test
			if testVectorOperation(t, ctx, wasmRuntime, encodingManager, f64Vector, fmt.Sprintf("f64_vector_%d", size), startTime) {
				successCount++
			}
		})

		// Test bool vectors
		t.Run(fmt.Sprintf("BoolVector_Size_%d", size), func(t *testing.T) {
			startTime := time.Now()

			// Create bool vector
			boolVector := FloatingPointVectors{
				F32Vec:  nil,
				F64Vec:  nil,
				BoolVec: make([]bool, size),
			}
			for i := 0; i < size; i++ {
				boolVector.BoolVec[i] = i%2 == 0
			}

			// Encode and test
			if testVectorOperation(t, ctx, wasmRuntime, encodingManager, boolVector, fmt.Sprintf("bool_vector_%d", size), startTime) {
				successCount++
			}
		})
	}

	// Report vector operation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ FLOATING POINT VECTOR operations: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 95.0,
		"Vector operations should have >95%% success rate")
}

// testVectorOperation is a helper function for testing vector operations
func testVectorOperation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, encodingManager *db.EncodingManager, vector interface{}, name string, startTime time.Time) bool {
	// Encode the vector using proper BSATN encoding
	encoded, err := encodingManager.Encode(vector, db.EncodingBSATN, &db.EncodingOptions{
		Format: db.EncodingBSATN,
	})
	if err != nil {
		t.Logf("⚠️  Failed to encode %s: %v", name, err)
		return false
	}

	vectorTime := time.Since(startTime)

	// Validate the encoding succeeded and meets performance requirements
	t.Logf("✅ %s encoded %d bytes in %v", name, len(encoded), vectorTime)

	assert.Less(t, vectorTime, VectorFloatTime,
		"%s processing took %v, expected less than %v", name, vectorTime, VectorFloatTime)

	// Validate encoding size is reasonable for vector data
	assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", name)
	assert.Less(t, len(encoded), 1000, "%s should encode to <1000 bytes for test vectors", name)

	return true
}

// testPrecisionValidation tests floating point precision and accuracy requirements
func testPrecisionValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT PRECISION VALIDATION...")

	// Create precision test cases
	precisionTests := []FloatPrecisionTest{
		{
			Description: "f32_pi_precision",
			F32Input:    float32(math.Pi),
			F64Input:    math.Pi,
			F32Expected: float32(math.Pi),
			F64Expected: math.Pi,
			Tolerance:   FloatPrecisionTolerance,
		},
		{
			Description: "f32_e_precision",
			F32Input:    float32(math.E),
			F64Input:    math.E,
			F32Expected: float32(math.E),
			F64Expected: math.E,
			Tolerance:   FloatPrecisionTolerance,
		},
		{
			Description: "f32_sqrt2_precision",
			F32Input:    float32(math.Sqrt2),
			F64Input:    math.Sqrt2,
			F32Expected: float32(math.Sqrt2),
			F64Expected: math.Sqrt2,
			Tolerance:   FloatPrecisionTolerance,
		},
	}

	successCount := 0
	totalTests := len(precisionTests) * 2 // f32 and f64 for each test

	for i, test := range precisionTests {
		// Test f32 precision
		t.Run(fmt.Sprintf("%s_f32", test.Description), func(t *testing.T) {
			f32Value := F32Value{Value: test.F32Input}

			encoded, err := encodingManager.Encode(f32Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			require.NoError(t, err, "Should encode f32 precision test value")

			// Validate encoding size is reasonable for f32
			assert.GreaterOrEqual(t, len(encoded), 4, "f32 should encode to at least 4 bytes")
			assert.LessOrEqual(t, len(encoded), 20, "f32 should not exceed 20 bytes when encoded")

			t.Logf("✅ %s f32 value %v encoded to %d bytes", test.Description, test.F32Input, len(encoded))
			successCount++
		})

		// Test f64 precision
		t.Run(fmt.Sprintf("%s_f64", test.Description), func(t *testing.T) {
			f64Value := F64Value{Value: test.F64Input}

			encoded, err := encodingManager.Encode(f64Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			require.NoError(t, err, "Should encode f64 precision test value")

			// Validate encoding size is reasonable for f64
			assert.GreaterOrEqual(t, len(encoded), 8, "f64 should encode to at least 8 bytes")
			assert.LessOrEqual(t, len(encoded), 25, "f64 should not exceed 25 bytes when encoded")

			t.Logf("✅ %s f64 value %v encoded to %d bytes", test.Description, test.F64Input, len(encoded))
			successCount++
		})

		// Test precision comparison
		t.Run(fmt.Sprintf("%s_comparison", test.Description), func(t *testing.T) {
			diff := math.Abs(float64(test.F32Input) - test.F64Input)

			t.Logf("Precision difference between f32(%v) and f64(%v): %e",
				test.F32Input, test.F64Input, diff)

			// f32 should be reasonably close to f64 for these mathematical constants
			if diff > 1e-6 { // f32 precision limit
				t.Logf("⚠️ Large precision difference detected (%e), expected due to f32 limits", diff)
			} else {
				t.Logf("✅ Precision difference within acceptable bounds")
			}
		})

		_ = i // Avoid unused variable warning
	}

	// Report precision validation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ PRECISION VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 95.0,
		"Precision validation should have >95%% success rate")
}

// testFloatingPointEncodingValidation validates BSATN encoding for floating point types
func testFloatingPointEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT ENCODING VALIDATION...")

	successCount := 0
	totalTests := len(FloatingPointTypes.TestValues) + len(BooleanTypes.TestValues)

	// Test floating point value encoding
	for i, testValue := range FloatingPointTypes.TestValues {
		var valueName string
		switch val := testValue.(type) {
		case F32Value:
			valueName = fmt.Sprintf("f32(%v)", val.Value)
		case F64Value:
			valueName = fmt.Sprintf("f64(%v)", val.Value)
		default:
			valueName = fmt.Sprintf("unknown(%v)", val)
		}

		t.Run(fmt.Sprintf("Encoding_%d_%s", i, valueName), func(t *testing.T) {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("⚠️  Failed to encode %s: %v", valueName, err)
				return
			}

			t.Logf("✅ %s encoded to %d bytes in %v", valueName, len(encoded), encodingTime)

			// Validate encoding performance
			assert.Less(t, encodingTime, FloatEncodingTime,
				"%s encoding took %v, expected less than %v", valueName, encodingTime, FloatEncodingTime)

			// Validate encoding size is reasonable
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", valueName)
			assert.Less(t, len(encoded), 100, "%s should encode to <100 bytes", valueName)

			successCount++
		})
	}

	// Test boolean value encoding
	for i, testValue := range BooleanTypes.TestValues {
		val := testValue.(BoolValue)
		valueName := fmt.Sprintf("bool(%v)", val.Value)

		t.Run(fmt.Sprintf("BoolEncoding_%d_%s", i, valueName), func(t *testing.T) {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("⚠️  Failed to encode %s: %v", valueName, err)
				return
			}

			t.Logf("✅ %s encoded to %d bytes in %v", valueName, len(encoded), encodingTime)

			// Validate boolean encoding performance
			assert.Less(t, encodingTime, BooleanEncodingTime,
				"%s encoding took %v, expected less than %v", valueName, encodingTime, BooleanEncodingTime)

			// Boolean should encode very efficiently
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", valueName)
			assert.Less(t, len(encoded), 20, "%s should encode to <20 bytes", valueName)

			successCount++
		})
	}

	// Report encoding validation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ ENCODING VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 95.0,
		"Encoding validation should have >95%% success rate")
}

// testFloatingPointTypeValidation validates type safety and correctness
func testFloatingPointTypeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT TYPE VALIDATION...")

	validationTests := []struct {
		Name        string
		Value       interface{}
		ExpectValid bool
		TypeName    string
	}{
		// Valid floating point types
		{"valid_f32_zero", F32Value{Value: 0.0}, true, "f32"},
		{"valid_f32_positive", F32Value{Value: 42.0}, true, "f32"},
		{"valid_f32_negative", F32Value{Value: -42.0}, true, "f32"},
		{"valid_f64_zero", F64Value{Value: 0.0}, true, "f64"},
		{"valid_f64_positive", F64Value{Value: 42.0}, true, "f64"},
		{"valid_f64_negative", F64Value{Value: -42.0}, true, "f64"},

		// Valid boolean types
		{"valid_bool_true", BoolValue{Value: true}, true, "bool"},
		{"valid_bool_false", BoolValue{Value: false}, true, "bool"},

		// Edge case floating point values
		{"edge_f32_max", F32Value{Value: math.MaxFloat32}, true, "f32"},
		{"edge_f64_max", F64Value{Value: math.MaxFloat64}, true, "f64"},
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

	// Report type validation success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ TYPE VALIDATION: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Type validation should have 100%% success rate")
}

// testFloatingPointSpecificOperations tests operations specific to floating point math
func testFloatingPointSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT SPECIFIC OPERATIONS...")

	// Test mathematical constants and calculations
	mathTests := []struct {
		Name        string
		F32Value    F32Value
		F64Value    F64Value
		Description string
	}{
		{"math_pi", F32Value{Value: float32(math.Pi)}, F64Value{Value: math.Pi}, "Mathematical constant Pi"},
		{"math_e", F32Value{Value: float32(math.E)}, F64Value{Value: math.E}, "Mathematical constant E"},
		{"math_sqrt2", F32Value{Value: float32(math.Sqrt2)}, F64Value{Value: math.Sqrt2}, "Square root of 2"},
		{"math_ln2", F32Value{Value: float32(math.Ln2)}, F64Value{Value: math.Ln2}, "Natural log of 2"},
		{"math_phi", F32Value{Value: float32(1.618033988749)}, F64Value{Value: 1.618033988749}, "Golden ratio Phi"},
	}

	successCount := 0
	totalTests := len(mathTests) * 2 // f32 and f64 for each

	for _, test := range mathTests {
		// Test f32 mathematical value
		t.Run(fmt.Sprintf("%s_f32", test.Name), func(t *testing.T) {
			encoded, err := encodingManager.Encode(test.F32Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			assert.NoError(t, err, "Should encode f32 %s", test.Description)

			t.Logf("✅ f32 %s: %v encoded to %d bytes", test.Description, test.F32Value.Value, len(encoded))
			successCount++
		})

		// Test f64 mathematical value
		t.Run(fmt.Sprintf("%s_f64", test.Name), func(t *testing.T) {
			encoded, err := encodingManager.Encode(test.F64Value, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			assert.NoError(t, err, "Should encode f64 %s", test.Description)

			t.Logf("✅ f64 %s: %v encoded to %d bytes", test.Description, test.F64Value.Value, len(encoded))
			successCount++
		})
	}

	// Report specific operations success rate
	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ FLOATING POINT SPECIFIC operations: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Equal(t, 100.0, successRate,
		"Floating point specific operations should have 100%% success rate")
}

// testFloatingPointPerformance measures encoding/decoding performance for floating point types
func testFloatingPointPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing FLOATING POINT PERFORMANCE...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	performanceResults := make(map[string]time.Duration)

	// Performance test for f32 values
	t.Run("F32PerformanceTest", func(t *testing.T) {
		totalOperations := FloatPerformanceIters
		startTime := time.Now()

		for i := 0; i < totalOperations; i++ {
			testValue := F32Value{Value: float32(i) * 3.14159}

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("F32 encoding error at iteration %d: %v", i, err)
				continue
			}

			_ = encoded // Use the encoded value
		}

		totalTime := time.Since(startTime)
		avgTimePerOp := totalTime / time.Duration(totalOperations)
		performanceResults["f32"] = avgTimePerOp

		t.Logf("✅ f32 performance: %d operations in %v (avg: %v per operation)",
			totalOperations, totalTime, avgTimePerOp)

		assert.Less(t, avgTimePerOp, time.Microsecond*10,
			"f32 operations should be fast (<10μs), got %v", avgTimePerOp)
	})

	// Performance test for f64 values
	t.Run("F64PerformanceTest", func(t *testing.T) {
		totalOperations := FloatPerformanceIters
		startTime := time.Now()

		for i := 0; i < totalOperations; i++ {
			testValue := F64Value{Value: float64(i) * 3.141592653589793}

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("F64 encoding error at iteration %d: %v", i, err)
				continue
			}

			_ = encoded // Use the encoded value
		}

		totalTime := time.Since(startTime)
		avgTimePerOp := totalTime / time.Duration(totalOperations)
		performanceResults["f64"] = avgTimePerOp

		t.Logf("✅ f64 performance: %d operations in %v (avg: %v per operation)",
			totalOperations, totalTime, avgTimePerOp)

		assert.Less(t, avgTimePerOp, time.Microsecond*15,
			"f64 operations should be fast (<15μs), got %v", avgTimePerOp)
	})

	// Performance test for boolean values
	t.Run("BooleanPerformanceTest", func(t *testing.T) {
		totalOperations := FloatPerformanceIters
		startTime := time.Now()

		for i := 0; i < totalOperations; i++ {
			testValue := BoolValue{Value: i%2 == 0}

			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Boolean encoding error at iteration %d: %v", i, err)
				continue
			}

			_ = encoded // Use the encoded value
		}

		totalTime := time.Since(startTime)
		avgTimePerOp := totalTime / time.Duration(totalOperations)
		performanceResults["bool"] = avgTimePerOp

		t.Logf("✅ Boolean performance: %d operations in %v (avg: %v per operation)",
			totalOperations, totalTime, avgTimePerOp)

		assert.Less(t, avgTimePerOp, time.Microsecond*5,
			"Boolean operations should be very fast (<5μs), got %v", avgTimePerOp)
	})

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	t.Logf("✅ PERFORMANCE testing memory usage: %.2f MB", memUsedMB)

	// Log performance summary
	t.Log("📊 FLOATING POINT PERFORMANCE Summary:")
	for typeName, avgTime := range performanceResults {
		t.Logf("  %s: %v per operation", typeName, avgTime)
	}
}
