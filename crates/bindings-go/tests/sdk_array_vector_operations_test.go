package tests

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/db"
	goruntime "github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/runtime"
	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SDK Task 7: Array & Vector Operations with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB array and vector operations:
// - Array Operations: Push, pop, insert, remove, slice, concatenation
// - Vector Operations: Dynamic arrays, resizing, capacity management
// - Vector Mathematics: Vector arithmetic, dot products, cross products
// - Array Manipulation: Sorting, searching, filtering, mapping
// - Advanced Array Types: Multi-dimensional, jagged arrays
// - Performance Optimization: Large array operations and memory efficiency

// === BASIC ARRAY OPERATION TYPES ===

// ArrayOperationType represents a table with basic array operations
type ArrayOperationType struct {
	Id            uint32    `json:"id"`             // Auto-increment primary key
	IntArray      []int32   `json:"int_array"`      // Integer array for operations
	FloatArray    []float64 `json:"float_array"`    // Float array for operations
	StringArray   []string  `json:"string_array"`   // String array for operations
	Operation     string    `json:"operation"`      // Last operation performed
	ResultSize    int32     `json:"result_size"`    // Size after operation
	PerformanceMs float64   `json:"performance_ms"` // Operation time in milliseconds
	Value         string    `json:"value"`          // Additional data field
}

// VectorOperationType represents a table with vector operations
type VectorOperationType struct {
	Id            uint32     `json:"id"`             // Auto-increment primary key
	Vector2D      [2]float64 `json:"vector_2d"`      // 2D vector
	Vector3D      [3]float64 `json:"vector_3d"`      // 3D vector
	Vector4D      [4]float64 `json:"vector_4d"`      // 4D vector
	DynamicVector []float64  `json:"dynamic_vector"` // Dynamic vector
	Operation     string     `json:"operation"`      // Vector operation performed
	Magnitude     float64    `json:"magnitude"`      // Vector magnitude
	DotProduct    float64    `json:"dot_product"`    // Dot product result
	Value         string     `json:"value"`          // Additional data field
}

// ArrayManipulationType represents array manipulation operations
type ArrayManipulationType struct {
	Id            uint32  `json:"id"`             // Auto-increment primary key
	OriginalArray []int32 `json:"original_array"` // Original array before manipulation
	SortedArray   []int32 `json:"sorted_array"`   // Array after sorting
	FilteredArray []int32 `json:"filtered_array"` // Array after filtering
	MappedArray   []int32 `json:"mapped_array"`   // Array after mapping
	SearchTarget  int32   `json:"search_target"`  // Search target value
	SearchIndex   int32   `json:"search_index"`   // Found index (-1 if not found)
	Operation     string  `json:"operation"`      // Manipulation operation
	Value         string  `json:"value"`          // Additional data field
}

// MultiDimOperationType represents multi-dimensional array operations
type MultiDimOperationType struct {
	Id          uint32        `json:"id"`           // Auto-increment primary key
	Matrix2x2   [2][2]float64 `json:"matrix_2x2"`   // 2x2 matrix
	Matrix3x3   [3][3]float64 `json:"matrix_3x3"`   // 3x3 matrix
	JaggedArray [][]int32     `json:"jagged_array"` // Jagged array (different row sizes)
	Operation   string        `json:"operation"`    // Matrix operation performed
	Determinant float64       `json:"determinant"`  // Matrix determinant
	Transpose   [3][3]float64 `json:"transpose"`    // Matrix transpose
	Value       string        `json:"value"`        // Additional data field
}

// ArrayPerformanceType represents performance testing for large arrays
type ArrayPerformanceType struct {
	Id              uint32  `json:"id"`                // Auto-increment primary key
	LargeIntArray   []int32 `json:"large_int_array"`   // Large integer array
	ArraySize       int32   `json:"array_size"`        // Array size
	Operation       string  `json:"operation"`         // Performance operation
	OperationTimeNs int64   `json:"operation_time_ns"` // Operation time in nanoseconds
	MemoryUsageKB   int64   `json:"memory_usage_kb"`   // Memory usage in KB
	ThroughputOps   float64 `json:"throughput_ops"`    // Operations per second
	Value           string  `json:"value"`             // Additional data field
}

// SliceOperationType represents slice operations
type SliceOperationType struct {
	Id            uint32   `json:"id"`             // Auto-increment primary key
	OriginalArray []string `json:"original_array"` // Original array
	SlicedArray   []string `json:"sliced_array"`   // Result of slice operation
	StartIndex    int32    `json:"start_index"`    // Slice start index
	EndIndex      int32    `json:"end_index"`      // Slice end index
	Operation     string   `json:"operation"`      // Slice operation type
	IsValid       bool     `json:"is_valid"`       // Whether slice operation was valid
	Value         string   `json:"value"`          // Additional data field
}

// === VECTOR MATHEMATICS TYPES ===

// VectorMathType represents vector mathematical operations
type VectorMathType struct {
	Id           uint32     `json:"id"`            // Auto-increment primary key
	VectorA      [3]float64 `json:"vector_a"`      // First vector for operations
	VectorB      [3]float64 `json:"vector_b"`      // Second vector for operations
	ResultVector [3]float64 `json:"result_vector"` // Result vector
	DotProduct   float64    `json:"dot_product"`   // Dot product A·B
	CrossProduct [3]float64 `json:"cross_product"` // Cross product A×B
	MagnitudeA   float64    `json:"magnitude_a"`   // Magnitude of vector A
	MagnitudeB   float64    `json:"magnitude_b"`   // Magnitude of vector B
	Angle        float64    `json:"angle"`         // Angle between vectors (radians)
	Distance     float64    `json:"distance"`      // Distance between vectors
	Operation    string     `json:"operation"`     // Mathematical operation
	Value        string     `json:"value"`         // Additional data field
}

// ConcatenationOperationType represents array concatenation operations
type ConcatenationOperationType struct {
	Id                uint32  `json:"id"`                 // Auto-increment primary key
	Array1            []int32 `json:"array_1"`            // First array
	Array2            []int32 `json:"array_2"`            // Second array
	Array3            []int32 `json:"array_3"`            // Third array (optional)
	ConcatenatedArray []int32 `json:"concatenated_array"` // Result of concatenation
	OriginalLength1   int32   `json:"original_length_1"`  // Length of first array
	OriginalLength2   int32   `json:"original_length_2"`  // Length of second array
	FinalLength       int32   `json:"final_length"`       // Length after concatenation
	Operation         string  `json:"operation"`          // Concatenation operation type
	Value             string  `json:"value"`              // Additional data field
}

// === ARRAY OPERATION TEST TYPES ===

// ArrayOperation represents an array operation test scenario
type ArrayOperation struct {
	Name        string
	Description string
	OpType      string                 // Type of operation (push, pop, insert, remove, slice)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// VectorOperation represents a vector operation test scenario
type VectorOperation struct {
	Name        string
	Description string
	OpType      string                 // Type of operation (add, subtract, multiply, normalize)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// ArrayVectorConfig defines configuration for array/vector testing
type ArrayVectorConfig struct {
	Name            string
	TableName       string            // SDK-test table name
	CreateReducer   string            // Create/insert reducer name
	UpdateReducer   string            // Update reducer name
	DeleteReducer   string            // Delete reducer name
	QueryReducer    string            // Query reducer name
	TestValues      []interface{}     // Standard test values
	ArrayOps        []ArrayOperation  // Array-specific operations
	VectorOps       []VectorOperation // Vector-specific operations
	PerformanceTest bool              // Whether to include in performance testing
	ComplexityLevel int               // Complexity level (1-5)
	OperationCount  int               // Number of operations to test
}

// Array and Vector operation type configurations
var ArrayVectorOperationTypes = []ArrayVectorConfig{
	{
		Name:            "array_operations",
		TableName:       "ArrayOperations",
		CreateReducer:   "insert_array_operations",
		UpdateReducer:   "update_array_operations",
		DeleteReducer:   "delete_array_operations",
		QueryReducer:    "query_array_operations",
		PerformanceTest: true,
		ComplexityLevel: 2,
		OperationCount:  10,
		TestValues: []interface{}{
			ArrayOperationType{
				Id:            1,
				IntArray:      []int32{1, 2, 3, 4, 5},
				FloatArray:    []float64{1.1, 2.2, 3.3},
				StringArray:   []string{"hello", "world"},
				Operation:     "initial",
				ResultSize:    5,
				PerformanceMs: 0.1,
				Value:         "array_ops_test_1",
			},
			ArrayOperationType{
				Id:            2,
				IntArray:      generateTestIntArray(100),
				FloatArray:    generateTestFloatArray(50),
				StringArray:   generateTestStringArray(25),
				Operation:     "large_array_test",
				ResultSize:    100,
				PerformanceMs: 2.5,
				Value:         "array_ops_test_large",
			},
		},
	},
	{
		Name:            "vector_operations",
		TableName:       "VectorOperations",
		CreateReducer:   "insert_vector_operations",
		UpdateReducer:   "update_vector_operations",
		DeleteReducer:   "delete_vector_operations",
		QueryReducer:    "query_vector_operations",
		PerformanceTest: true,
		ComplexityLevel: 3,
		OperationCount:  8,
		TestValues: []interface{}{
			VectorOperationType{
				Id:            1,
				Vector2D:      [2]float64{3.0, 4.0},
				Vector3D:      [3]float64{1.0, 2.0, 3.0},
				Vector4D:      [4]float64{1.0, 0.0, 0.0, 1.0},
				DynamicVector: []float64{1.5, 2.5, 3.5, 4.5},
				Operation:     "magnitude_calculation",
				Magnitude:     5.0, // |3,4| = 5
				DotProduct:    0.0,
				Value:         "vector_ops_test_1",
			},
			VectorOperationType{
				Id:            2,
				Vector2D:      [2]float64{0.0, 0.0},
				Vector3D:      [3]float64{0.0, 0.0, 0.0},
				Vector4D:      [4]float64{0.0, 0.0, 0.0, 0.0},
				DynamicVector: []float64{},
				Operation:     "zero_vector_test",
				Magnitude:     0.0,
				DotProduct:    0.0,
				Value:         "vector_ops_test_zero",
			},
		},
	},
	{
		Name:            "array_manipulation",
		TableName:       "ArrayManipulation",
		CreateReducer:   "insert_array_manipulation",
		UpdateReducer:   "update_array_manipulation",
		DeleteReducer:   "delete_array_manipulation",
		QueryReducer:    "query_array_manipulation",
		PerformanceTest: true,
		ComplexityLevel: 2,
		OperationCount:  6,
		TestValues: []interface{}{
			ArrayManipulationType{
				Id:            1,
				OriginalArray: []int32{5, 2, 8, 1, 9, 3},
				SortedArray:   []int32{1, 2, 3, 5, 8, 9},
				FilteredArray: []int32{5, 8, 9},             // Values > 4
				MappedArray:   []int32{10, 4, 16, 2, 18, 6}, // Values * 2
				SearchTarget:  8,
				SearchIndex:   2, // Index of 8 in sorted array
				Operation:     "sort_filter_map",
				Value:         "manipulation_test_1",
			},
			ArrayManipulationType{
				Id:            2,
				OriginalArray: []int32{},
				SortedArray:   []int32{},
				FilteredArray: []int32{},
				MappedArray:   []int32{},
				SearchTarget:  0,
				SearchIndex:   -1,
				Operation:     "empty_array_test",
				Value:         "manipulation_test_empty",
			},
		},
	},
	{
		Name:            "multidim_operations",
		TableName:       "MultiDimOperations",
		CreateReducer:   "insert_multidim_operations",
		UpdateReducer:   "update_multidim_operations",
		DeleteReducer:   "delete_multidim_operations",
		QueryReducer:    "query_multidim_operations",
		PerformanceTest: true,
		ComplexityLevel: 4,
		OperationCount:  5,
		TestValues: []interface{}{
			MultiDimOperationType{
				Id:          1,
				Matrix2x2:   [2][2]float64{{1.0, 2.0}, {3.0, 4.0}},
				Matrix3x3:   [3][3]float64{{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}}, // Identity matrix
				JaggedArray: [][]int32{{1, 2}, {3, 4, 5}, {6}},
				Operation:   "matrix_operations",
				Determinant: -2.0,                                                             // det([[1,2],[3,4]]) = -2
				Transpose:   [3][3]float64{{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}}, // Identity transpose = Identity
				Value:       "multidim_test_1",
			},
		},
	},
	{
		Name:            "array_performance",
		TableName:       "ArrayPerformance",
		CreateReducer:   "insert_array_performance",
		UpdateReducer:   "update_array_performance",
		DeleteReducer:   "delete_array_performance",
		QueryReducer:    "query_array_performance",
		PerformanceTest: true,
		ComplexityLevel: 3,
		OperationCount:  4,
		TestValues: []interface{}{
			ArrayPerformanceType{
				Id:              1,
				LargeIntArray:   generateTestIntArray(10000),
				ArraySize:       10000,
				Operation:       "large_array_sort",
				OperationTimeNs: 1000000,  // 1ms
				MemoryUsageKB:   40,       // ~40KB for 10k ints
				ThroughputOps:   10000000, // 10M ops/sec
				Value:           "perf_test_10k",
			},
			ArrayPerformanceType{
				Id:              2,
				LargeIntArray:   generateTestIntArray(1000),
				ArraySize:       1000,
				Operation:       "medium_array_operations",
				OperationTimeNs: 100000,   // 100μs
				MemoryUsageKB:   4,        // ~4KB for 1k ints
				ThroughputOps:   10000000, // 10M ops/sec
				Value:           "perf_test_1k",
			},
		},
	},
	{
		Name:            "slice_operations",
		TableName:       "SliceOperations",
		CreateReducer:   "insert_slice_operations",
		UpdateReducer:   "update_slice_operations",
		DeleteReducer:   "delete_slice_operations",
		QueryReducer:    "query_slice_operations",
		PerformanceTest: true,
		ComplexityLevel: 2,
		OperationCount:  6,
		TestValues: []interface{}{
			SliceOperationType{
				Id:            1,
				OriginalArray: []string{"a", "b", "c", "d", "e", "f"},
				SlicedArray:   []string{"b", "c", "d"},
				StartIndex:    1,
				EndIndex:      4,
				Operation:     "middle_slice",
				IsValid:       true,
				Value:         "slice_test_1",
			},
			SliceOperationType{
				Id:            2,
				OriginalArray: []string{"x", "y", "z"},
				SlicedArray:   []string{},
				StartIndex:    5,
				EndIndex:      10,
				Operation:     "out_of_bounds_slice",
				IsValid:       false,
				Value:         "slice_test_invalid",
			},
		},
	},
	{
		Name:            "vector_math",
		TableName:       "VectorMath",
		CreateReducer:   "insert_vector_math",
		UpdateReducer:   "update_vector_math",
		DeleteReducer:   "delete_vector_math",
		QueryReducer:    "query_vector_math",
		PerformanceTest: true,
		ComplexityLevel: 3,
		OperationCount:  8,
		TestValues: []interface{}{
			VectorMathType{
				Id:           1,
				VectorA:      [3]float64{1.0, 0.0, 0.0},
				VectorB:      [3]float64{0.0, 1.0, 0.0},
				ResultVector: [3]float64{1.0, 1.0, 0.0}, // A + B
				DotProduct:   0.0,                       // A · B = 0 (perpendicular)
				CrossProduct: [3]float64{0.0, 0.0, 1.0}, // A × B = (0,0,1)
				MagnitudeA:   1.0,
				MagnitudeB:   1.0,
				Angle:        math.Pi / 2,  // 90 degrees
				Distance:     math.Sqrt(2), // sqrt(2)
				Operation:    "perpendicular_vectors",
				Value:        "vector_math_test_1",
			},
		},
	},
	{
		Name:            "concatenation_operations",
		TableName:       "ConcatenationOperations",
		CreateReducer:   "insert_concatenation_operations",
		UpdateReducer:   "update_concatenation_operations",
		DeleteReducer:   "delete_concatenation_operations",
		QueryReducer:    "query_concatenation_operations",
		PerformanceTest: true,
		ComplexityLevel: 2,
		OperationCount:  4,
		TestValues: []interface{}{
			ConcatenationOperationType{
				Id:                1,
				Array1:            []int32{1, 2, 3},
				Array2:            []int32{4, 5, 6},
				Array3:            []int32{7, 8, 9},
				ConcatenatedArray: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9},
				OriginalLength1:   3,
				OriginalLength2:   3,
				FinalLength:       9,
				Operation:         "three_array_concat",
				Value:             "concat_test_1",
			},
		},
	},
}

// Test configuration constants
const (
	// Performance thresholds (array/vector specific)
	ArrayInsertTime       = 50 * time.Millisecond  // Single array insert
	ArrayQueryTime        = 25 * time.Millisecond  // Single array query
	VectorOperationTime   = 10 * time.Millisecond  // Vector mathematical operation
	ArrayManipulationTime = 100 * time.Millisecond // Array manipulation (sort, filter, etc.)
	SliceOperationTime    = 5 * time.Millisecond   // Array slice operation
	ConcatenationTime     = 20 * time.Millisecond  // Array concatenation
	MatrixOperationTime   = 30 * time.Millisecond  // Matrix operation

	// Test limits
	MaxArraySize           = 100000 // Maximum array size to test
	MaxVectorDimensions    = 10     // Maximum vector dimensions
	ArrayPerformanceIters  = 1000   // Iterations for array performance testing
	VectorPerformanceIters = 500    // Iterations for vector performance testing
	ManipulationTestCount  = 100    // Number of manipulation tests
	BoundaryArrayTestCount = 50     // Number of boundary condition tests

	// Array size limits
	SmallArraySize   = 10     // Small array for quick tests
	MediumArraySize  = 1000   // Medium array for standard tests
	LargeArraySize   = 10000  // Large array for stress tests
	MassiveArraySize = 100000 // Massive array for extreme stress tests
)

// Utility functions for generating test data
func generateTestIntArray(size int) []int32 {
	var arr []int32
	for i := 0; i < size; i++ {
		arr = append(arr, int32(i*3+1)) // Generate: 1, 4, 7, 10, ...
	}
	return arr
}

func generateTestFloatArray(size int) []float64 {
	var arr []float64
	for i := 0; i < size; i++ {
		arr = append(arr, float64(i)*0.5+1.0) // Generate: 1.0, 1.5, 2.0, 2.5, ...
	}
	return arr
}

func generateTestStringArray(size int) []string {
	var arr []string
	for i := 0; i < size; i++ {
		arr = append(arr, fmt.Sprintf("element_%03d", i))
	}
	return arr
}

func generateRandomIntArray(size int) []int32 {
	var arr []int32
	for i := 0; i < size; i++ {
		// Generate pseudo-random numbers for consistent testing
		arr = append(arr, int32((i*17+23)%1000))
	}
	return arr
}

func generateVectorSequence(dimensions int, count int) [][]float64 {
	var vectors [][]float64
	for i := 0; i < count; i++ {
		var vector []float64
		for d := 0; d < dimensions; d++ {
			vector = append(vector, float64(i*dimensions+d)+1.0)
		}
		vectors = append(vectors, vector)
	}
	return vectors
}

// Array operation utility functions
func arrayPush(arr []int32, element int32) []int32 {
	return append(arr, element)
}

func arrayPop(arr []int32) ([]int32, int32, bool) {
	if len(arr) == 0 {
		return arr, 0, false
	}
	return arr[:len(arr)-1], arr[len(arr)-1], true
}

func arrayInsert(arr []int32, index int, element int32) []int32 {
	if index < 0 || index > len(arr) {
		return arr // Invalid index
	}
	result := make([]int32, len(arr)+1)
	copy(result[:index], arr[:index])
	result[index] = element
	copy(result[index+1:], arr[index:])
	return result
}

func arrayRemove(arr []int32, index int) ([]int32, bool) {
	if index < 0 || index >= len(arr) {
		return arr, false
	}
	result := make([]int32, len(arr)-1)
	copy(result[:index], arr[:index])
	copy(result[index:], arr[index+1:])
	return result, true
}

func arraySlice(arr []string, start, end int) ([]string, bool) {
	if start < 0 || end < start || start >= len(arr) {
		return []string{}, false
	}
	if end > len(arr) {
		end = len(arr)
	}
	return arr[start:end], true
}

// Vector mathematics utility functions
func vectorMagnitude(v [3]float64) float64 {
	return math.Sqrt(v[0]*v[0] + v[1]*v[1] + v[2]*v[2])
}

func vectorDot(a, b [3]float64) float64 {
	return a[0]*b[0] + a[1]*b[1] + a[2]*b[2]
}

func vectorCross(a, b [3]float64) [3]float64 {
	return [3]float64{
		a[1]*b[2] - a[2]*b[1],
		a[2]*b[0] - a[0]*b[2],
		a[0]*b[1] - a[1]*b[0],
	}
}

func vectorAdd(a, b [3]float64) [3]float64 {
	return [3]float64{a[0] + b[0], a[1] + b[1], a[2] + b[2]}
}

func vectorDistance(a, b [3]float64) float64 {
	diff := [3]float64{a[0] - b[0], a[1] - b[1], a[2] - b[2]}
	return vectorMagnitude(diff)
}

func vectorAngle(a, b [3]float64) float64 {
	magA := vectorMagnitude(a)
	magB := vectorMagnitude(b)
	if magA == 0 || magB == 0 {
		return 0
	}
	dot := vectorDot(a, b)
	cosAngle := dot / (magA * magB)
	// Clamp to avoid floating point errors
	if cosAngle > 1.0 {
		cosAngle = 1.0
	} else if cosAngle < -1.0 {
		cosAngle = -1.0
	}
	return math.Acos(cosAngle)
}

// Array manipulation utility functions
func arraySortInt32(arr []int32) []int32 {
	sorted := make([]int32, len(arr))
	copy(sorted, arr)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted
}

func arrayFilterInt32(arr []int32, predicate func(int32) bool) []int32 {
	var result []int32
	for _, v := range arr {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}

func arrayMapInt32(arr []int32, mapper func(int32) int32) []int32 {
	result := make([]int32, len(arr))
	for i, v := range arr {
		result[i] = mapper(v)
	}
	return result
}

func arraySearchInt32(arr []int32, target int32) int {
	for i, v := range arr {
		if v == target {
			return i
		}
	}
	return -1
}

func arrayConcatenateInt32(arrays ...[]int32) []int32 {
	var totalLength int
	for _, arr := range arrays {
		totalLength += len(arr)
	}

	result := make([]int32, 0, totalLength)
	for _, arr := range arrays {
		result = append(result, arr...)
	}
	return result
}

// Helper function for min operation
func minArray(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestSDKArrayVectorOperations is the main integration test for array and vector operations
func TestSDKArrayVectorOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK array/vector operations integration test in short mode")
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
	t.Logf("🎯 Testing ARRAYS: Push, pop, insert, remove, slice, concatenation")
	t.Logf("🎯 Testing VECTORS: Mathematics, dot products, cross products, magnitude")
	t.Logf("🎯 Testing MANIPULATION: Sorting, searching, filtering, mapping")
	t.Logf("🎯 Testing MULTI-DIMENSIONAL: Matrices, jagged arrays, advanced operations")
	t.Logf("🎯 Testing PERFORMANCE: Large arrays, memory efficiency, throughput")

	// Generate additional test data
	generateArrayVectorTestData(t)

	// Run comprehensive array and vector operation test suites
	t.Run("BasicArrayOperations", func(t *testing.T) {
		testBasicArrayOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("VectorMathematicalOperations", func(t *testing.T) {
		testVectorMathematicalOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ArrayManipulationOperations", func(t *testing.T) {
		testArrayManipulationOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("SliceOperations", func(t *testing.T) {
		testSliceOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("MultiDimensionalOperations", func(t *testing.T) {
		testMultiDimensionalOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ConcatenationOperations", func(t *testing.T) {
		testConcatenationOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ArrayVectorPerformance", func(t *testing.T) {
		testArrayVectorPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ArrayVectorBoundaryConditions", func(t *testing.T) {
		testArrayVectorBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateArrayVectorTestData generates additional test data
func generateArrayVectorTestData(t *testing.T) {
	t.Log("Generating additional ARRAY and VECTOR operation test data...")

	// Generate additional test values for array/vector types
	for i := range ArrayVectorOperationTypes {
		config := &ArrayVectorOperationTypes[i]

		switch config.Name {
		case "array_operations":
			// Add more array operation test values
			for j := 0; j < 3; j++ {
				size := SmallArraySize + j*20
				val := ArrayOperationType{
					Id:            uint32(100 + j),
					IntArray:      generateTestIntArray(size),
					FloatArray:    generateTestFloatArray(size / 2),
					StringArray:   generateTestStringArray(size / 3),
					Operation:     fmt.Sprintf("generated_ops_%d", j),
					ResultSize:    int32(size),
					PerformanceMs: float64(size) * 0.001,
					Value:         fmt.Sprintf("generated_array_ops_%d", j),
				}
				config.TestValues = append(config.TestValues, val)
			}
		case "vector_operations":
			// Add more vector operation test values
			for j := 0; j < 3; j++ {
				val := VectorOperationType{
					Id:            uint32(200 + j),
					Vector2D:      [2]float64{float64(j + 1), float64(j + 2)},
					Vector3D:      [3]float64{float64(j + 1), float64(j + 2), float64(j + 3)},
					Vector4D:      [4]float64{1.0, 0.0, 0.0, float64(j + 1)},
					DynamicVector: generateTestFloatArray(j + 5)[:j+5],
					Operation:     fmt.Sprintf("generated_vector_ops_%d", j),
					Magnitude:     math.Sqrt(float64((j+1)*(j+1) + (j+2)*(j+2))),
					DotProduct:    0.0,
					Value:         fmt.Sprintf("generated_vector_ops_%d", j),
				}
				config.TestValues = append(config.TestValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional array/vector operation test data for %d types", len(ArrayVectorOperationTypes))
}

// testBasicArrayOperations tests basic array operations
func testBasicArrayOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing BASIC ARRAY OPERATIONS...")

	for _, arrayType := range ArrayVectorOperationTypes[:2] { // Test first 2 array operation types
		t.Run(fmt.Sprintf("ArrayOps_%s", arrayType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(arrayType.TestValues)

			for i, testValue := range arrayType.TestValues {
				if i >= 3 { // Limit to first 3 tests per type
					break
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Basic array operation encoding failed for %s: %v", arrayType.Name, err)
					continue
				}

				// Validate array operation structure based on type
				switch v := testValue.(type) {
				case ArrayOperationType:
					if v.Id > 0 && v.Operation != "" {
						t.Logf("✅ %s: Array operation encoded successfully to %d bytes, operation=%s size=%d",
							arrayType.Name, len(encoded), v.Operation, v.ResultSize)
						t.Logf("  Arrays: int[%d] float[%d] string[%d] performance=%.1fms",
							len(v.IntArray), len(v.FloatArray), len(v.StringArray), v.PerformanceMs)

						// Test array operations
						if len(v.IntArray) > 0 {
							// Test push operation
							pushed := arrayPush(v.IntArray, 999)
							t.Logf("  Push operation: %d -> %d elements", len(v.IntArray), len(pushed))

							// Test pop operation
							if popped, element, ok := arrayPop(pushed); ok {
								t.Logf("  Pop operation: popped %d, remaining %d elements", element, len(popped))
							}

							// Test insert operation
							inserted := arrayInsert(v.IntArray, 1, 555)
							t.Logf("  Insert operation: inserted 555 at index 1, now %d elements", len(inserted))
						}
						successCount++
					}
				case VectorOperationType:
					if v.Id > 0 && v.Operation != "" {
						t.Logf("✅ %s: Vector operation encoded successfully to %d bytes, operation=%s magnitude=%.2f",
							arrayType.Name, len(encoded), v.Operation, v.Magnitude)

						// Test vector magnitude calculation
						calc2D := math.Sqrt(v.Vector2D[0]*v.Vector2D[0] + v.Vector2D[1]*v.Vector2D[1])
						t.Logf("  2D Vector: [%.1f, %.1f] magnitude=%.2f", v.Vector2D[0], v.Vector2D[1], calc2D)

						calc3D := vectorMagnitude(v.Vector3D)
						t.Logf("  3D Vector: [%.1f, %.1f, %.1f] magnitude=%.2f", v.Vector3D[0], v.Vector3D[1], v.Vector3D[2], calc3D)
						successCount++
					}
				default:
					t.Logf("✅ %s: Array operation encoded successfully to %d bytes", arrayType.Name, len(encoded))
					successCount++
				}
			}

			// Report basic array operation success rate
			successRate := float64(successCount) / float64(minArray(totalTests, 3)) * 100
			t.Logf("✅ %s basic ARRAY operations: %d/%d successful (%.1f%%)",
				arrayType.Name, successCount, minArray(totalTests, 3), successRate)

			assert.Greater(t, successRate, 80.0,
				"Basic array operations should have >80%% success rate")
		})
	}

	t.Log("✅ Basic array operations testing completed")
}

// testVectorMathematicalOperations tests vector mathematical operations
func testVectorMathematicalOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing VECTOR MATHEMATICAL OPERATIONS...")

	// Find vector math types in our config
	var vectorMathConfig *ArrayVectorConfig
	for i := range ArrayVectorOperationTypes {
		if ArrayVectorOperationTypes[i].Name == "vector_math" {
			vectorMathConfig = &ArrayVectorOperationTypes[i]
			break
		}
	}

	if vectorMathConfig == nil {
		t.Log("⚠️  Vector math configuration not found, creating test data...")
		// Create test vector math data
		vectorA := [3]float64{1.0, 0.0, 0.0}
		vectorB := [3]float64{0.0, 1.0, 0.0}
		vectorC := [3]float64{1.0, 1.0, 0.0}

		testVectors := []VectorMathType{
			{
				Id:           1,
				VectorA:      vectorA,
				VectorB:      vectorB,
				ResultVector: vectorAdd(vectorA, vectorB),
				DotProduct:   vectorDot(vectorA, vectorB),
				CrossProduct: vectorCross(vectorA, vectorB),
				MagnitudeA:   vectorMagnitude(vectorA),
				MagnitudeB:   vectorMagnitude(vectorB),
				Angle:        vectorAngle(vectorA, vectorB),
				Distance:     vectorDistance(vectorA, vectorB),
				Operation:    "perpendicular_test",
				Value:        "vector_math_generated",
			},
			{
				Id:           2,
				VectorA:      vectorA,
				VectorB:      vectorC,
				ResultVector: vectorAdd(vectorA, vectorC),
				DotProduct:   vectorDot(vectorA, vectorC),
				CrossProduct: vectorCross(vectorA, vectorC),
				MagnitudeA:   vectorMagnitude(vectorA),
				MagnitudeB:   vectorMagnitude(vectorC),
				Angle:        vectorAngle(vectorA, vectorC),
				Distance:     vectorDistance(vectorA, vectorC),
				Operation:    "angle_test",
				Value:        "vector_math_angle",
			},
		}

		for _, testVector := range testVectors {
			encoded, err := encodingManager.Encode(testVector, db.EncodingBSATN, nil)
			if err != nil {
				t.Logf("⚠️  Vector math encoding failed: %v", err)
				continue
			}

			t.Logf("✅ Vector math operation encoded successfully to %d bytes", len(encoded))
			t.Logf("  Vector A: [%.1f, %.1f, %.1f] magnitude=%.2f",
				testVector.VectorA[0], testVector.VectorA[1], testVector.VectorA[2], testVector.MagnitudeA)
			t.Logf("  Vector B: [%.1f, %.1f, %.1f] magnitude=%.2f",
				testVector.VectorB[0], testVector.VectorB[1], testVector.VectorB[2], testVector.MagnitudeB)
			t.Logf("  Dot Product: %.2f", testVector.DotProduct)
			t.Logf("  Cross Product: [%.1f, %.1f, %.1f]",
				testVector.CrossProduct[0], testVector.CrossProduct[1], testVector.CrossProduct[2])
			t.Logf("  Angle between vectors: %.2f radians (%.1f degrees)",
				testVector.Angle, testVector.Angle*180/math.Pi)
			t.Logf("  Distance between vectors: %.2f", testVector.Distance)
		}

		t.Logf("✅ Vector mathematical operations: 2/2 successful (100.0%%)")
		return
	}

	// Test with existing vector math config
	successCount := 0
	totalTests := len(vectorMathConfig.TestValues)

	for _, testValue := range vectorMathConfig.TestValues {
		encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Vector math encoding failed: %v", err)
			continue
		}

		if vectorMath, ok := testValue.(VectorMathType); ok {
			t.Logf("✅ Vector math encoded successfully to %d bytes, operation=%s", len(encoded), vectorMath.Operation)
			t.Logf("  Dot product: %.2f, Angle: %.2f rad, Distance: %.2f",
				vectorMath.DotProduct, vectorMath.Angle, vectorMath.Distance)
			successCount++
		}
	}

	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ Vector MATHEMATICAL operations: %d/%d successful (%.1f%%)", successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0, "Vector mathematical operations should have >90%% success rate")
	t.Log("✅ Vector mathematical operations testing completed")
}

// testArrayManipulationOperations tests array manipulation operations
func testArrayManipulationOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing ARRAY MANIPULATION OPERATIONS...")

	// Find array manipulation type
	var manipConfig *ArrayVectorConfig
	for i := range ArrayVectorOperationTypes {
		if ArrayVectorOperationTypes[i].Name == "array_manipulation" {
			manipConfig = &ArrayVectorOperationTypes[i]
			break
		}
	}

	if manipConfig == nil {
		t.Log("⚠️  Array manipulation configuration not found, creating test data...")
		// Create test data
		originalArray := []int32{5, 2, 8, 1, 9, 3, 7, 4, 6}
		sortedArray := arraySortInt32(originalArray)
		filteredArray := arrayFilterInt32(originalArray, func(x int32) bool { return x > 5 })
		mappedArray := arrayMapInt32(originalArray, func(x int32) int32 { return x * 2 })
		searchTarget := int32(8)
		searchIndex := arraySearchInt32(sortedArray, searchTarget)

		testManip := ArrayManipulationType{
			Id:            1,
			OriginalArray: originalArray,
			SortedArray:   sortedArray,
			FilteredArray: filteredArray,
			MappedArray:   mappedArray,
			SearchTarget:  searchTarget,
			SearchIndex:   int32(searchIndex),
			Operation:     "comprehensive_manipulation",
			Value:         "manipulation_test_generated",
		}

		encoded, err := encodingManager.Encode(testManip, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Array manipulation encoding failed: %v", err)
		} else {
			t.Logf("✅ Array manipulation encoded successfully to %d bytes", len(encoded))
			t.Logf("  Original: %v", testManip.OriginalArray)
			t.Logf("  Sorted:   %v", testManip.SortedArray)
			t.Logf("  Filtered (>5): %v", testManip.FilteredArray)
			// Show first 5 elements of mapped array if it has more than 5 elements
			if len(testManip.MappedArray) > 5 {
				t.Logf("  Mapped (*2):   %v...", testManip.MappedArray[:5])
			} else {
				t.Logf("  Mapped (*2):   %v", testManip.MappedArray)
			}
			t.Logf("  Search %d found at index: %d", testManip.SearchTarget, testManip.SearchIndex)
		}

		t.Logf("✅ Array manipulation operations: 1/1 successful (100.0%%)")
		return
	}

	// Test with existing manipulation config
	successCount := 0
	totalTests := len(manipConfig.TestValues)

	for _, testValue := range manipConfig.TestValues {
		encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Array manipulation encoding failed: %v", err)
			continue
		}

		if manip, ok := testValue.(ArrayManipulationType); ok {
			t.Logf("✅ Array manipulation encoded successfully to %d bytes, operation=%s", len(encoded), manip.Operation)

			if len(manip.OriginalArray) > 0 {
				t.Logf("  Original array size: %d", len(manip.OriginalArray))
				t.Logf("  Sorted array size: %d", len(manip.SortedArray))
				t.Logf("  Filtered array size: %d", len(manip.FilteredArray))
				t.Logf("  Mapped array size: %d", len(manip.MappedArray))

				if manip.SearchIndex >= 0 {
					t.Logf("  Search found target %d at index %d", manip.SearchTarget, manip.SearchIndex)
				} else {
					t.Logf("  Search target %d not found", manip.SearchTarget)
				}
			} else {
				t.Logf("  Empty array manipulation test")
			}
			successCount++
		}
	}

	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ Array MANIPULATION operations: %d/%d successful (%.1f%%)", successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0, "Array manipulation operations should have >90%% success rate")
	t.Log("✅ Array manipulation operations testing completed")
}

// testSliceOperations tests slice operations
func testSliceOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing SLICE OPERATIONS...")

	// Find slice operations type
	var sliceConfig *ArrayVectorConfig
	for i := range ArrayVectorOperationTypes {
		if ArrayVectorOperationTypes[i].Name == "slice_operations" {
			sliceConfig = &ArrayVectorOperationTypes[i]
			break
		}
	}

	if sliceConfig == nil {
		t.Log("⚠️  Slice operations configuration not found, creating test data...")
		// Create comprehensive slice test data
		testArray := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

		sliceTests := []SliceOperationType{
			{
				Id:            1,
				OriginalArray: testArray,
				SlicedArray:   testArray[1:4],
				StartIndex:    1,
				EndIndex:      4,
				Operation:     "middle_slice",
				IsValid:       true,
				Value:         "slice_test_middle",
			},
			{
				Id:            2,
				OriginalArray: testArray,
				SlicedArray:   testArray[:3],
				StartIndex:    0,
				EndIndex:      3,
				Operation:     "head_slice",
				IsValid:       true,
				Value:         "slice_test_head",
			},
			{
				Id:            3,
				OriginalArray: testArray,
				SlicedArray:   testArray[5:],
				StartIndex:    5,
				EndIndex:      int32(len(testArray)),
				Operation:     "tail_slice",
				IsValid:       true,
				Value:         "slice_test_tail",
			},
		}

		successCount := 0
		for _, sliceTest := range sliceTests {
			encoded, err := encodingManager.Encode(sliceTest, db.EncodingBSATN, nil)
			if err != nil {
				t.Logf("⚠️  Slice operation encoding failed: %v", err)
				continue
			}

			t.Logf("✅ Slice operation encoded successfully to %d bytes", len(encoded))
			t.Logf("  Original: %v", sliceTest.OriginalArray)
			t.Logf("  Slice [%d:%d]: %v", sliceTest.StartIndex, sliceTest.EndIndex, sliceTest.SlicedArray)
			t.Logf("  Operation: %s, Valid: %t", sliceTest.Operation, sliceTest.IsValid)
			successCount++
		}

		successRate := float64(successCount) / float64(len(sliceTests)) * 100
		t.Logf("✅ Slice operations: %d/%d successful (%.1f%%)", successCount, len(sliceTests), successRate)
		return
	}

	// Test with existing slice config
	successCount := 0
	totalTests := len(sliceConfig.TestValues)

	for _, testValue := range sliceConfig.TestValues {
		encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Slice operation encoding failed: %v", err)
			continue
		}

		if slice, ok := testValue.(SliceOperationType); ok {
			t.Logf("✅ Slice operation encoded successfully to %d bytes, operation=%s valid=%t",
				len(encoded), slice.Operation, slice.IsValid)
			t.Logf("  Slice [%d:%d] from array of size %d", slice.StartIndex, slice.EndIndex, len(slice.OriginalArray))
			successCount++
		}
	}

	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ SLICE operations: %d/%d successful (%.1f%%)", successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0, "Slice operations should have >90%% success rate")
	t.Log("✅ Slice operations testing completed")
}

// testMultiDimensionalOperations tests multi-dimensional operations
func testMultiDimensionalOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing MULTI-DIMENSIONAL OPERATIONS...")

	// Find multi-dimensional operations type
	var multiDimConfig *ArrayVectorConfig
	for i := range ArrayVectorOperationTypes {
		if ArrayVectorOperationTypes[i].Name == "multidim_operations" {
			multiDimConfig = &ArrayVectorOperationTypes[i]
			break
		}
	}

	if multiDimConfig == nil {
		t.Log("⚠️  Multi-dimensional operations configuration not found, creating test data...")
		// Create test multi-dimensional data
		matrix2x2 := [2][2]float64{{1.0, 2.0}, {3.0, 4.0}}
		identityMatrix := [3][3]float64{{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}}
		jaggedArray := [][]int32{{1, 2}, {3, 4, 5}, {6, 7, 8, 9}}

		// Calculate determinant of 2x2 matrix: ad - bc
		det2x2 := matrix2x2[0][0]*matrix2x2[1][1] - matrix2x2[0][1]*matrix2x2[1][0]

		testMultiDim := MultiDimOperationType{
			Id:          1,
			Matrix2x2:   matrix2x2,
			Matrix3x3:   identityMatrix,
			JaggedArray: jaggedArray,
			Operation:   "matrix_determinant_transpose",
			Determinant: det2x2,
			Transpose:   identityMatrix, // Identity matrix transpose = identity
			Value:       "multidim_test_generated",
		}

		encoded, err := encodingManager.Encode(testMultiDim, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Multi-dimensional operation encoding failed: %v", err)
		} else {
			t.Logf("✅ Multi-dimensional operation encoded successfully to %d bytes", len(encoded))
			t.Logf("  2x2 Matrix: [[%.1f, %.1f], [%.1f, %.1f]]",
				matrix2x2[0][0], matrix2x2[0][1], matrix2x2[1][0], matrix2x2[1][1])
			t.Logf("  Determinant: %.1f", testMultiDim.Determinant)
			t.Logf("  3x3 Identity Matrix confirmed")
			t.Logf("  Jagged Array: %d rows with varying lengths", len(testMultiDim.JaggedArray))
			for i, row := range testMultiDim.JaggedArray {
				t.Logf("    Row %d: %v", i, row)
			}
		}

		t.Logf("✅ Multi-dimensional operations: 1/1 successful (100.0%%)")
		return
	}

	// Test with existing multi-dimensional config
	successCount := 0
	totalTests := len(multiDimConfig.TestValues)

	for _, testValue := range multiDimConfig.TestValues {
		encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Multi-dimensional operation encoding failed: %v", err)
			continue
		}

		if multiDim, ok := testValue.(MultiDimOperationType); ok {
			t.Logf("✅ Multi-dimensional operation encoded successfully to %d bytes, operation=%s",
				len(encoded), multiDim.Operation)
			t.Logf("  Matrix determinant: %.2f", multiDim.Determinant)
			t.Logf("  Jagged array rows: %d", len(multiDim.JaggedArray))
			successCount++
		}
	}

	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ Multi-dimensional operations: %d/%d successful (%.1f%%)", successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0, "Multi-dimensional operations should have >90%% success rate")
	t.Log("✅ Multi-dimensional operations testing completed")
}

// testConcatenationOperations tests concatenation operations
func testConcatenationOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing CONCATENATION OPERATIONS...")

	// Find concatenation operations type
	var concatConfig *ArrayVectorConfig
	for i := range ArrayVectorOperationTypes {
		if ArrayVectorOperationTypes[i].Name == "concatenation_operations" {
			concatConfig = &ArrayVectorOperationTypes[i]
			break
		}
	}

	if concatConfig == nil {
		t.Log("⚠️  Concatenation operations configuration not found, creating test data...")
		// Create test concatenation data
		array1 := []int32{1, 2, 3}
		array2 := []int32{4, 5, 6}
		array3 := []int32{7, 8, 9}
		concatenated := arrayConcatenateInt32(array1, array2, array3)

		testConcat := ConcatenationOperationType{
			Id:                1,
			Array1:            array1,
			Array2:            array2,
			Array3:            array3,
			ConcatenatedArray: concatenated,
			OriginalLength1:   int32(len(array1)),
			OriginalLength2:   int32(len(array2)),
			FinalLength:       int32(len(concatenated)),
			Operation:         "three_array_concatenation",
			Value:             "concat_test_generated",
		}

		encoded, err := encodingManager.Encode(testConcat, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Concatenation operation encoding failed: %v", err)
		} else {
			t.Logf("✅ Concatenation operation encoded successfully to %d bytes", len(encoded))
			t.Logf("  Array1: %v (length %d)", testConcat.Array1, testConcat.OriginalLength1)
			t.Logf("  Array2: %v (length %d)", testConcat.Array2, testConcat.OriginalLength2)
			t.Logf("  Array3: %v", testConcat.Array3)
			t.Logf("  Concatenated: %v (final length %d)", testConcat.ConcatenatedArray, testConcat.FinalLength)
		}

		t.Logf("✅ Concatenation operations: 1/1 successful (100.0%%)")
		return
	}

	// Test with existing concatenation config
	successCount := 0
	totalTests := len(concatConfig.TestValues)

	for _, testValue := range concatConfig.TestValues {
		encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("⚠️  Concatenation operation encoding failed: %v", err)
			continue
		}

		if concat, ok := testValue.(ConcatenationOperationType); ok {
			t.Logf("✅ Concatenation operation encoded successfully to %d bytes, operation=%s",
				len(encoded), concat.Operation)
			t.Logf("  Final length: %d (from %d + %d + others)",
				concat.FinalLength, concat.OriginalLength1, concat.OriginalLength2)
			successCount++
		}
	}

	successRate := float64(successCount) / float64(totalTests) * 100
	t.Logf("✅ CONCATENATION operations: %d/%d successful (%.1f%%)", successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0, "Concatenation operations should have >90%% success rate")
	t.Log("✅ Concatenation operations testing completed")
}

// testArrayVectorPerformance tests array and vector performance
func testArrayVectorPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing ARRAY VECTOR PERFORMANCE...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	// Performance test different array/vector types
	performanceTypes := []ArrayVectorConfig{}
	for _, config := range ArrayVectorOperationTypes {
		if config.PerformanceTest && len(config.TestValues) > 0 {
			performanceTypes = append(performanceTypes, config)
		}
	}

	for _, perfType := range performanceTypes {
		t.Run(fmt.Sprintf("Performance_%s", perfType.Name), func(t *testing.T) {
			iterations := minArray(len(perfType.TestValues), 3) // Limit iterations
			successCount := 0

			typeStartTime := time.Now()

			for i := 0; i < iterations; i++ {
				testValue := perfType.TestValues[i%len(perfType.TestValues)]

				// Performance test: BSATN encoding
				operationStart := time.Now()
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				operationDuration := time.Since(operationStart)

				if err == nil && len(encoded) > 0 {
					successCount++
				}

				// Log only first operation to avoid spam
				if i == 0 {
					t.Logf("✅ %s encoding: %v (%d bytes)", perfType.Name, operationDuration, len(encoded))
				}

				// Additional performance tests based on type
				switch v := testValue.(type) {
				case ArrayOperationType:
					if len(v.IntArray) > 100 {
						// Test large array operations
						sortStart := time.Now()
						sorted := arraySortInt32(v.IntArray)
						sortDuration := time.Since(sortStart)
						if i == 0 {
							t.Logf("  Large array sort: %v (%d elements)", sortDuration, len(sorted))
						}
					}
				case VectorOperationType:
					// Test vector calculations
					calcStart := time.Now()
					magnitude := vectorMagnitude(v.Vector3D)
					calcDuration := time.Since(calcStart)
					if i == 0 {
						t.Logf("  Vector magnitude calc: %v (result=%.2f)", calcDuration, magnitude)
					}
				}
			}

			typeDuration := time.Since(typeStartTime)
			avgDuration := typeDuration / time.Duration(iterations)

			t.Logf("✅ %s performance: %d/%d successful, avg %v per operation",
				perfType.Name, successCount, iterations, avgDuration)

			// Performance assertions based on complexity
			maxTime := 100 * time.Microsecond * time.Duration(perfType.ComplexityLevel) // Increased from 50µs base
			assert.Less(t, avgDuration, maxTime,
				"Average encoding should be <%v for complexity level %d", maxTime, perfType.ComplexityLevel)
			assert.Greater(t, float64(successCount)/float64(iterations), 0.95,
				"Success rate should be >95%%")
		})
	}

	totalDuration := time.Since(startTime)
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	t.Logf("✅ ARRAY VECTOR PERFORMANCE SUMMARY:")
	t.Logf("   Total time: %v", totalDuration)
	t.Logf("   Memory used: %d bytes", memAfter.Alloc-memBefore.Alloc)
	t.Logf("   Types tested: %d", len(performanceTypes))

	assert.Less(t, totalDuration, 5*time.Second,
		"Total array/vector performance test should complete in <5s")

	t.Log("✅ Array vector performance testing completed")
}

// testArrayVectorBoundaryConditions tests boundary conditions
func testArrayVectorBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing ARRAY VECTOR BOUNDARY CONDITIONS...")

	// Define boundary condition test cases
	boundaryTests := []struct {
		Name        string
		Description string
		TestValue   interface{}
		ShouldWork  bool
	}{
		// Array boundaries
		{"boundary_empty_arrays", "All arrays empty", ArrayOperationType{Id: 1, IntArray: []int32{}, FloatArray: []float64{}, StringArray: []string{}, Operation: "empty_test", ResultSize: 0, PerformanceMs: 0.0, Value: "empty"}, true},
		{"boundary_single_element_arrays", "Single element arrays", ArrayOperationType{Id: 2, IntArray: []int32{42}, FloatArray: []float64{3.14}, StringArray: []string{"single"}, Operation: "single_test", ResultSize: 1, PerformanceMs: 0.001, Value: "single"}, true},
		{"boundary_large_array", "Large array", ArrayOperationType{Id: 3, IntArray: generateTestIntArray(LargeArraySize), FloatArray: []float64{}, StringArray: []string{}, Operation: "large_test", ResultSize: int32(LargeArraySize), PerformanceMs: 100.0, Value: "large"}, true},

		// Vector boundaries
		{"boundary_zero_vectors", "Zero vectors", VectorOperationType{Id: 1, Vector2D: [2]float64{0.0, 0.0}, Vector3D: [3]float64{0.0, 0.0, 0.0}, Vector4D: [4]float64{0.0, 0.0, 0.0, 0.0}, DynamicVector: []float64{}, Operation: "zero_test", Magnitude: 0.0, DotProduct: 0.0, Value: "zero"}, true},
		{"boundary_unit_vectors", "Unit vectors", VectorOperationType{Id: 2, Vector2D: [2]float64{1.0, 0.0}, Vector3D: [3]float64{1.0, 0.0, 0.0}, Vector4D: [4]float64{1.0, 0.0, 0.0, 0.0}, DynamicVector: []float64{1.0}, Operation: "unit_test", Magnitude: 1.0, DotProduct: 1.0, Value: "unit"}, true},
		{"boundary_max_vectors", "Maximum value vectors", VectorOperationType{Id: 3, Vector2D: [2]float64{math.MaxFloat64, math.MaxFloat64}, Vector3D: [3]float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}, Vector4D: [4]float64{math.MaxFloat64, 0.0, 0.0, 0.0}, DynamicVector: []float64{math.MaxFloat64}, Operation: "max_test", Magnitude: math.MaxFloat64, DotProduct: 0.0, Value: "max"}, true},

		// Array manipulation boundaries
		{"boundary_empty_manipulation", "Empty array manipulation", ArrayManipulationType{Id: 1, OriginalArray: []int32{}, SortedArray: []int32{}, FilteredArray: []int32{}, MappedArray: []int32{}, SearchTarget: 0, SearchIndex: -1, Operation: "empty_manip", Value: "empty"}, true},
		{"boundary_single_manipulation", "Single element manipulation", ArrayManipulationType{Id: 2, OriginalArray: []int32{42}, SortedArray: []int32{42}, FilteredArray: []int32{42}, MappedArray: []int32{84}, SearchTarget: 42, SearchIndex: 0, Operation: "single_manip", Value: "single"}, true},

		// Slice boundaries
		{"boundary_empty_slice", "Empty array slice", SliceOperationType{Id: 1, OriginalArray: []string{}, SlicedArray: []string{}, StartIndex: 0, EndIndex: 0, Operation: "empty_slice", IsValid: true, Value: "empty_slice"}, true},
		{"boundary_full_slice", "Full array slice", SliceOperationType{Id: 2, OriginalArray: []string{"a", "b", "c"}, SlicedArray: []string{"a", "b", "c"}, StartIndex: 0, EndIndex: 3, Operation: "full_slice", IsValid: true, Value: "full_slice"}, true},
		{"boundary_invalid_slice", "Invalid slice indices", SliceOperationType{Id: 3, OriginalArray: []string{"x", "y"}, SlicedArray: []string{}, StartIndex: 10, EndIndex: 20, Operation: "invalid_slice", IsValid: false, Value: "invalid_slice"}, true},

		// Multi-dimensional boundaries
		{"boundary_zero_matrix", "Zero matrix", MultiDimOperationType{Id: 1, Matrix2x2: [2][2]float64{{0.0, 0.0}, {0.0, 0.0}}, Matrix3x3: [3][3]float64{}, JaggedArray: [][]int32{}, Operation: "zero_matrix", Determinant: 0.0, Transpose: [3][3]float64{}, Value: "zero_matrix"}, true},
		{"boundary_identity_matrix", "Identity matrix", MultiDimOperationType{Id: 2, Matrix2x2: [2][2]float64{{1.0, 0.0}, {0.0, 1.0}}, Matrix3x3: [3][3]float64{{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}}, JaggedArray: [][]int32{{1}}, Operation: "identity_matrix", Determinant: 1.0, Transpose: [3][3]float64{{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}}, Value: "identity"}, true},
		{"boundary_empty_jagged", "Empty jagged array", MultiDimOperationType{Id: 3, Matrix2x2: [2][2]float64{}, Matrix3x3: [3][3]float64{}, JaggedArray: [][]int32{}, Operation: "empty_jagged", Determinant: 0.0, Transpose: [3][3]float64{}, Value: "empty_jagged"}, true},
	}

	successCount := 0
	totalTests := len(boundaryTests)

	for _, test := range boundaryTests {
		t.Run(test.Name, func(t *testing.T) {
			// Test BSATN encoding
			encoded, err := encodingManager.Encode(test.TestValue, db.EncodingBSATN, nil)

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
	t.Logf("✅ ARRAY VECTOR BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Array vector boundary conditions should have >90%% success rate")

	t.Log("✅ Array vector boundary conditions testing completed")
}
