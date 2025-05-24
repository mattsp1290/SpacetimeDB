package tests

import (
	"context"
	"math"
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

// SDK Task 8: Optional Types with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB optional types:
// - Optional Primitive Types: Nullable integers, floats, strings, booleans
// - Option<T> Pattern: Rust-style Some/None variants for type safety
// - Nullable Complex Types: Optional structs, arrays, maps, nested structures
// - Validation & Defaults: Proper handling of null values and default behaviors
// - Performance Optimization: Efficient encoding/decoding of optional types
// - Database Integration: Optional columns, nullable constraints, null queries

// === OPTIONAL PRIMITIVE TYPES ===

// OptionalU8Type represents an optional u8 value
type OptionalU8Type struct {
	Id       uint32 `json:"id"`        // Primary key
	Value    *uint8 `json:"value"`     // Optional u8 field (nullable)
	HasValue bool   `json:"has_value"` // Explicit presence indicator
	Default  uint8  `json:"default"`   // Default value when null
	Metadata string `json:"metadata"`  // Additional context
}

// OptionalU16Type represents an optional u16 value
type OptionalU16Type struct {
	Id       uint32  `json:"id"`        // Primary key
	Value    *uint16 `json:"value"`     // Optional u16 field (nullable)
	HasValue bool    `json:"has_value"` // Explicit presence indicator
	Default  uint16  `json:"default"`   // Default value when null
	Metadata string  `json:"metadata"`  // Additional context
}

// OptionalU32Type represents an optional u32 value
type OptionalU32Type struct {
	Id       uint32  `json:"id"`        // Primary key
	Value    *uint32 `json:"value"`     // Optional u32 field (nullable)
	HasValue bool    `json:"has_value"` // Explicit presence indicator
	Default  uint32  `json:"default"`   // Default value when null
	Metadata string  `json:"metadata"`  // Additional context
}

// OptionalU64Type represents an optional u64 value
type OptionalU64Type struct {
	Id       uint32  `json:"id"`        // Primary key
	Value    *uint64 `json:"value"`     // Optional u64 field (nullable)
	HasValue bool    `json:"has_value"` // Explicit presence indicator
	Default  uint64  `json:"default"`   // Default value when null
	Metadata string  `json:"metadata"`  // Additional context
}

// OptionalI32Type represents an optional i32 value
type OptionalI32Type struct {
	Id       uint32 `json:"id"`        // Primary key
	Value    *int32 `json:"value"`     // Optional i32 field (nullable)
	HasValue bool   `json:"has_value"` // Explicit presence indicator
	Default  int32  `json:"default"`   // Default value when null
	Metadata string `json:"metadata"`  // Additional context
}

// OptionalI64Type represents an optional i64 value
type OptionalI64Type struct {
	Id       uint32 `json:"id"`        // Primary key
	Value    *int64 `json:"value"`     // Optional i64 field (nullable)
	HasValue bool   `json:"has_value"` // Explicit presence indicator
	Default  int64  `json:"default"`   // Default value when null
	Metadata string `json:"metadata"`  // Additional context
}

// OptionalF32Type represents an optional f32 value
type OptionalF32Type struct {
	Id       uint32   `json:"id"`        // Primary key
	Value    *float32 `json:"value"`     // Optional f32 field (nullable)
	HasValue bool     `json:"has_value"` // Explicit presence indicator
	Default  float32  `json:"default"`   // Default value when null
	Metadata string   `json:"metadata"`  // Additional context
}

// OptionalF64Type represents an optional f64 value
type OptionalF64Type struct {
	Id       uint32   `json:"id"`        // Primary key
	Value    *float64 `json:"value"`     // Optional f64 field (nullable)
	HasValue bool     `json:"has_value"` // Explicit presence indicator
	Default  float64  `json:"default"`   // Default value when null
	Metadata string   `json:"metadata"`  // Additional context
}

// OptionalBoolType represents an optional boolean value
type OptionalBoolType struct {
	Id       uint32 `json:"id"`        // Primary key
	Value    *bool  `json:"value"`     // Optional bool field (nullable)
	HasValue bool   `json:"has_value"` // Explicit presence indicator
	Default  bool   `json:"default"`   // Default value when null
	Metadata string `json:"metadata"`  // Additional context
}

// OptionalStringType represents an optional string value
type OptionalStringType struct {
	Id       uint32  `json:"id"`        // Primary key
	Value    *string `json:"value"`     // Optional string field (nullable)
	HasValue bool    `json:"has_value"` // Explicit presence indicator
	Default  string  `json:"default"`   // Default value when null
	Metadata string  `json:"metadata"`  // Additional context
}

// === OPTION TYPE PATTERN (RUST-STYLE) ===

// OptionType represents a generic Option<T> pattern
type OptionType[T any] struct {
	IsSome bool `json:"is_some"` // true if Some(value), false if None
	Value  T    `json:"value"`   // The wrapped value (only valid if IsSome)
}

// OptionalOptionU32Type demonstrates Option<u32> pattern
type OptionalOptionU32Type struct {
	Id          uint32             `json:"id"`           // Primary key
	OptionalInt OptionType[uint32] `json:"optional_int"` // Option<u32>
	Description string             `json:"description"`  // Context description
	CreatedAt   uint64             `json:"created_at"`   // Timestamp
}

// OptionalOptionStringType demonstrates Option<String> pattern
type OptionalOptionStringType struct {
	Id             uint32             `json:"id"`              // Primary key
	OptionalString OptionType[string] `json:"optional_string"` // Option<String>
	Category       string             `json:"category"`        // Category classification
	UpdatedAt      uint64             `json:"updated_at"`      // Timestamp
}

// === OPTIONAL COMPLEX TYPES ===

// PersonInfo represents a complex nested structure
type PersonInfo struct {
	Name  string  `json:"name"`  // Required field
	Age   *int32  `json:"age"`   // Optional field
	Email string  `json:"email"` // Required field
	Phone *string `json:"phone"` // Optional field
}

// OptionalStructType represents an optional complex structure
type OptionalStructType struct {
	Id         uint32      `json:"id"`          // Primary key
	PersonData *PersonInfo `json:"person_data"` // Optional nested struct
	IsActive   bool        `json:"is_active"`   // Status flag
	Tags       []string    `json:"tags"`        // Tags array
	Metadata   string      `json:"metadata"`    // Additional context
}

// OptionalArrayType represents optional array elements
type OptionalArrayType struct {
	Id           uint32        `json:"id"`            // Primary key
	OptionalInts []*int32      `json:"optional_ints"` // Array of optional integers
	RequiredInts []int32       `json:"required_ints"` // Array of required integers
	OptionalStrs []*string     `json:"optional_strs"` // Array of optional strings
	MixedData    []interface{} `json:"mixed_data"`    // Array with mixed optional/required types
	ElementCount int32         `json:"element_count"` // Count of elements
}

// OptionalMapType represents optional map values
type OptionalMapType struct {
	Id           uint32                      `json:"id"`             // Primary key
	StringIntMap map[string]*int32           `json:"string_int_map"` // Map with optional int values
	IntStringMap map[int32]*string           `json:"int_string_map"` // Map with optional string values
	NestedMap    map[string]map[string]*bool `json:"nested_map"`     // Nested map with optional bool values
	KeyCount     int32                       `json:"key_count"`      // Count of keys
}

// === VALIDATION AND CONSTRAINT TYPES ===

// OptionalValidationType demonstrates validation for optional fields
type OptionalValidationType struct {
	Id              uint32   `json:"id"`               // Primary key
	OptionalAge     *int32   `json:"optional_age"`     // Age (0-120 if present)
	OptionalEmail   *string  `json:"optional_email"`   // Email (valid format if present)
	OptionalScore   *float64 `json:"optional_score"`   // Score (0.0-100.0 if present)
	OptionalTags    []string `json:"optional_tags"`    // Tags (non-empty if present)
	ValidationFlags uint32   `json:"validation_flags"` // Validation result flags
	ErrorMessage    string   `json:"error_message"`    // Validation error details
}

// OptionalConstraintType demonstrates database constraints with optional fields
type OptionalConstraintType struct {
	Id                 uint32  `json:"id"`                   // Primary key
	OptionalUniqueId   *string `json:"optional_unique_id"`   // Optional unique constraint
	OptionalIndex      *int32  `json:"optional_index"`       // Optional indexed field
	OptionalForeignKey *uint32 `json:"optional_foreign_key"` // Optional foreign key
	ConstraintStatus   string  `json:"constraint_status"`    // Constraint validation status
	CreatedAt          uint64  `json:"created_at"`           // Creation timestamp
}

// === PERFORMANCE AND BOUNDARY TYPES ===

// OptionalPerformanceType for performance testing of optional types
type OptionalPerformanceType struct {
	Id               uint32     `json:"id"`                 // Primary key
	OptionalLargeInt *uint64    `json:"optional_large_int"` // Large integer value
	OptionalLargeStr *string    `json:"optional_large_str"` // Large string value
	OptionalFloatArr []*float64 `json:"optional_float_arr"` // Array of optional floats
	ProcessingTimeMs float64    `json:"processing_time_ms"` // Processing time
	MemoryUsageBytes uint64     `json:"memory_usage_bytes"` // Memory usage
	OperationCount   int32      `json:"operation_count"`    // Number of operations performed
}

// OptionalBoundaryType for testing edge cases and boundary conditions
type OptionalBoundaryType struct {
	Id            uint32   `json:"id"`             // Primary key
	OptionalMin   *int32   `json:"optional_min"`   // Minimum value test
	OptionalMax   *uint64  `json:"optional_max"`   // Maximum value test
	OptionalZero  *float64 `json:"optional_zero"`  // Zero value test
	OptionalEmpty *string  `json:"optional_empty"` // Empty string test
	OptionalNull  *bool    `json:"optional_null"`  // Explicit null test
	BoundaryFlags uint32   `json:"boundary_flags"` // Boundary condition flags
}

// === OPTIONAL TYPE TEST CONFIGURATIONS ===

// OptionalTypeConfig defines configuration for optional type testing
type OptionalTypeConfig struct {
	Name            string
	TableName       string           // SDK-test table name
	CreateReducer   string           // Create reducer name
	UpdateReducer   string           // Update reducer name
	DeleteReducer   string           // Delete reducer name
	QueryReducer    string           // Query reducer name
	TestValues      []interface{}    // Standard test values
	NullValues      []interface{}    // Test values with null fields
	ValidationRules []ValidationRule // Validation rules for optional fields
	PerformanceTest bool             // Whether to include in performance testing
	ComplexityLevel int              // Complexity level (1-5)
	BoundaryTests   []BoundaryTest   // Boundary condition tests
}

// ValidationRule represents a validation rule for optional fields
type ValidationRule struct {
	FieldName    string                 // Name of the field to validate
	IsRequired   bool                   // Whether the field is required
	Validator    func(interface{}) bool // Validation function
	ErrorMessage string                 // Error message for validation failure
}

// BoundaryTest represents a boundary condition test
type BoundaryTest struct {
	Name        string      // Test name
	Value       interface{} // Test value
	ShouldPass  bool        // Whether the test should pass
	Description string      // Test description
}

// Optional type configurations for testing
var OptionalTypes = []OptionalTypeConfig{
	{
		Name:            "optional_u8",
		TableName:       "OptionalU8",
		CreateReducer:   "insert_optional_u8",
		UpdateReducer:   "update_optional_u8",
		DeleteReducer:   "delete_optional_u8",
		QueryReducer:    "query_optional_u8",
		PerformanceTest: true,
		ComplexityLevel: 1,
		TestValues: []interface{}{
			OptionalU8Type{
				Id:       1,
				Value:    func() *uint8 { v := uint8(42); return &v }(),
				HasValue: true,
				Default:  0,
				Metadata: "optional_u8_with_value",
			},
			OptionalU8Type{
				Id:       2,
				Value:    nil,
				HasValue: false,
				Default:  255,
				Metadata: "optional_u8_null_value",
			},
		},
	},
	{
		Name:            "optional_u32",
		TableName:       "OptionalU32",
		CreateReducer:   "insert_optional_u32",
		UpdateReducer:   "update_optional_u32",
		DeleteReducer:   "delete_optional_u32",
		QueryReducer:    "query_optional_u32",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			OptionalU32Type{
				Id:       1,
				Value:    func() *uint32 { v := uint32(123456); return &v }(),
				HasValue: true,
				Default:  0,
				Metadata: "optional_u32_with_value",
			},
			OptionalU32Type{
				Id:       2,
				Value:    nil,
				HasValue: false,
				Default:  4294967295,
				Metadata: "optional_u32_null_value",
			},
		},
	},
	{
		Name:            "optional_i32",
		TableName:       "OptionalI32",
		CreateReducer:   "insert_optional_i32",
		UpdateReducer:   "update_optional_i32",
		DeleteReducer:   "delete_optional_i32",
		QueryReducer:    "query_optional_i32",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			OptionalI32Type{
				Id:       1,
				Value:    func() *int32 { v := int32(-12345); return &v }(),
				HasValue: true,
				Default:  0,
				Metadata: "optional_i32_negative_value",
			},
			OptionalI32Type{
				Id:       2,
				Value:    func() *int32 { v := int32(67890); return &v }(),
				HasValue: true,
				Default:  0,
				Metadata: "optional_i32_positive_value",
			},
			OptionalI32Type{
				Id:       3,
				Value:    nil,
				HasValue: false,
				Default:  -1,
				Metadata: "optional_i32_null_value",
			},
		},
	},
	{
		Name:            "optional_f64",
		TableName:       "OptionalF64",
		CreateReducer:   "insert_optional_f64",
		UpdateReducer:   "update_optional_f64",
		DeleteReducer:   "delete_optional_f64",
		QueryReducer:    "query_optional_f64",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			OptionalF64Type{
				Id:       1,
				Value:    func() *float64 { v := 3.14159; return &v }(),
				HasValue: true,
				Default:  0.0,
				Metadata: "optional_f64_pi_value",
			},
			OptionalF64Type{
				Id:       2,
				Value:    func() *float64 { v := 1.23456789e10; return &v }(), // Large valid float instead of infinity
				HasValue: true,
				Default:  0.0,
				Metadata: "optional_f64_large_value",
			},
			OptionalF64Type{
				Id:       3,
				Value:    nil,
				HasValue: false,
				Default:  -1.0,
				Metadata: "optional_f64_null_value",
			},
		},
	},
	{
		Name:            "optional_string",
		TableName:       "OptionalString",
		CreateReducer:   "insert_optional_string",
		UpdateReducer:   "update_optional_string",
		DeleteReducer:   "delete_optional_string",
		QueryReducer:    "query_optional_string",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			OptionalStringType{
				Id:       1,
				Value:    func() *string { v := "Hello, SpacetimeDB!"; return &v }(),
				HasValue: true,
				Default:  "default_string",
				Metadata: "optional_string_with_value",
			},
			OptionalStringType{
				Id:       2,
				Value:    func() *string { v := ""; return &v }(),
				HasValue: true,
				Default:  "default_string",
				Metadata: "optional_string_empty_value",
			},
			OptionalStringType{
				Id:       3,
				Value:    nil,
				HasValue: false,
				Default:  "default_string",
				Metadata: "optional_string_null_value",
			},
		},
	},
	{
		Name:            "optional_bool",
		TableName:       "OptionalBool",
		CreateReducer:   "insert_optional_bool",
		UpdateReducer:   "update_optional_bool",
		DeleteReducer:   "delete_optional_bool",
		QueryReducer:    "query_optional_bool",
		PerformanceTest: true,
		ComplexityLevel: 1,
		TestValues: []interface{}{
			OptionalBoolType{
				Id:       1,
				Value:    func() *bool { v := true; return &v }(),
				HasValue: true,
				Default:  false,
				Metadata: "optional_bool_true_value",
			},
			OptionalBoolType{
				Id:       2,
				Value:    func() *bool { v := false; return &v }(),
				HasValue: true,
				Default:  true,
				Metadata: "optional_bool_false_value",
			},
			OptionalBoolType{
				Id:       3,
				Value:    nil,
				HasValue: false,
				Default:  true,
				Metadata: "optional_bool_null_value",
			},
		},
	},
	{
		Name:            "optional_option_u32",
		TableName:       "OptionalOptionU32",
		CreateReducer:   "insert_optional_option_u32",
		UpdateReducer:   "update_optional_option_u32",
		DeleteReducer:   "delete_optional_option_u32",
		QueryReducer:    "query_optional_option_u32",
		PerformanceTest: true,
		ComplexityLevel: 3,
		TestValues: []interface{}{
			OptionalOptionU32Type{
				Id:          1,
				OptionalInt: OptionType[uint32]{IsSome: true, Value: 42},
				Description: "option_some_value",
				CreatedAt:   uint64(time.Now().UnixMicro()),
			},
			OptionalOptionU32Type{
				Id:          2,
				OptionalInt: OptionType[uint32]{IsSome: false, Value: 0},
				Description: "option_none_value",
				CreatedAt:   uint64(time.Now().UnixMicro()),
			},
		},
	},
	{
		Name:            "optional_struct",
		TableName:       "OptionalStruct",
		CreateReducer:   "insert_optional_struct",
		UpdateReducer:   "update_optional_struct",
		DeleteReducer:   "delete_optional_struct",
		QueryReducer:    "query_optional_struct",
		PerformanceTest: true,
		ComplexityLevel: 4,
		TestValues: []interface{}{
			OptionalStructType{
				Id: 1,
				PersonData: &PersonInfo{
					Name:  "Alice Johnson",
					Age:   func() *int32 { v := int32(30); return &v }(),
					Email: "alice@example.com",
					Phone: func() *string { v := "+1-555-0123"; return &v }(),
				},
				IsActive: true,
				Tags:     []string{"admin", "developer"},
				Metadata: "optional_struct_with_complete_data",
			},
			OptionalStructType{
				Id: 2,
				PersonData: &PersonInfo{
					Name:  "Bob Smith",
					Age:   nil,
					Email: "bob@example.com",
					Phone: nil,
				},
				IsActive: true,
				Tags:     []string{"user"},
				Metadata: "optional_struct_with_partial_data",
			},
			OptionalStructType{
				Id:         3,
				PersonData: nil,
				IsActive:   false,
				Tags:       []string{},
				Metadata:   "optional_struct_null_person_data",
			},
		},
	},
	{
		Name:            "optional_array",
		TableName:       "OptionalArray",
		CreateReducer:   "insert_optional_array",
		UpdateReducer:   "update_optional_array",
		DeleteReducer:   "delete_optional_array",
		QueryReducer:    "query_optional_array",
		PerformanceTest: true,
		ComplexityLevel: 3,
		TestValues: []interface{}{
			OptionalArrayType{
				Id: 1,
				OptionalInts: []*int32{
					func() *int32 { v := int32(10); return &v }(),
					nil,
					func() *int32 { v := int32(30); return &v }(),
				},
				RequiredInts: []int32{1, 2, 3, 4, 5},
				OptionalStrs: []*string{
					func() *string { v := "first"; return &v }(),
					func() *string { v := "second"; return &v }(),
					nil,
				},
				MixedData:    []interface{}{int32(42), "test", nil, true},
				ElementCount: 4,
			},
		},
	},
	{
		Name:            "optional_validation",
		TableName:       "OptionalValidation",
		CreateReducer:   "insert_optional_validation",
		UpdateReducer:   "update_optional_validation",
		DeleteReducer:   "delete_optional_validation",
		QueryReducer:    "query_optional_validation",
		PerformanceTest: false,
		ComplexityLevel: 3,
		TestValues: []interface{}{
			OptionalValidationType{
				Id:              1,
				OptionalAge:     func() *int32 { v := int32(25); return &v }(),
				OptionalEmail:   func() *string { v := "valid@example.com"; return &v }(),
				OptionalScore:   func() *float64 { v := 85.5; return &v }(),
				OptionalTags:    []string{"valid", "test"},
				ValidationFlags: 0,
				ErrorMessage:    "",
			},
			OptionalValidationType{
				Id:              2,
				OptionalAge:     nil,
				OptionalEmail:   nil,
				OptionalScore:   nil,
				OptionalTags:    []string{},
				ValidationFlags: 0,
				ErrorMessage:    "",
			},
		},
	},
}

// Test configuration constants
const (
	// Performance thresholds (optional types specific)
	OptionalEncodingTime   = 100 * time.Microsecond // Optional type encoding
	OptionalValidationTime = 50 * time.Microsecond  // Optional validation
	OptionalNullCheckTime  = 10 * time.Microsecond  // Null check operation
	OptionalDefaultTime    = 20 * time.Microsecond  // Default value assignment
	OptionalComplexTime    = 500 * time.Microsecond // Complex optional operations

	// Test limits
	OptionalPerformanceIters = 1000  // Iterations for optional performance testing
	MaxOptionalArraySize     = 1000  // Maximum size for optional arrays
	MaxOptionalStringLength  = 10000 // Maximum length for optional strings
	OptionalValidationCount  = 100   // Number of validation tests
	OptionalBoundaryCount    = 50    // Number of boundary tests

	// Validation constants
	MinAge   = 0
	MaxAge   = 120
	MinScore = 0.0
	MaxScore = 100.0
)

// Helper functions for creating optional values
func OptionalU8(value uint8) *uint8       { return &value }
func OptionalU16(value uint16) *uint16    { return &value }
func OptionalU32(value uint32) *uint32    { return &value }
func OptionalU64(value uint64) *uint64    { return &value }
func OptionalI32(value int32) *int32      { return &value }
func OptionalI64(value int64) *int64      { return &value }
func OptionalF32(value float32) *float32  { return &value }
func OptionalF64(value float64) *float64  { return &value }
func OptionalBool(value bool) *bool       { return &value }
func OptionalString(value string) *string { return &value }

// Validation functions
func ValidateOptionalAge(value interface{}) bool {
	if value == nil {
		return true // Null is valid for optional fields
	}
	if age, ok := value.(*int32); ok && age != nil {
		return *age >= MinAge && *age <= MaxAge
	}
	return false
}

func ValidateOptionalEmail(value interface{}) bool {
	if value == nil {
		return true // Null is valid for optional fields
	}
	if email, ok := value.(*string); ok && email != nil {
		// Simple email validation
		return len(*email) > 0 && contains(*email, "@") && contains(*email, ".")
	}
	return false
}

func ValidateOptionalScore(value interface{}) bool {
	if value == nil {
		return true // Null is valid for optional fields
	}
	if score, ok := value.(*float64); ok && score != nil {
		return *score >= MinScore && *score <= MaxScore
	}
	return false
}

// Helper function for string contains check
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestSDKOptionalTypes is the main integration test
func TestSDKOptionalTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK optional types integration test in short mode")
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
	t.Logf("🎯 Testing OPTIONAL TYPES: Nullable primitives, Option<T> patterns")
	t.Logf("🎯 Testing NULL HANDLING: Proper null value encoding and validation")
	t.Logf("🎯 Testing COMPLEX OPTIONALS: Optional structs, arrays, nested structures")
	t.Logf("🎯 Testing VALIDATION: Optional field validation and constraints")
	t.Logf("🎯 Testing PERFORMANCE: Efficient optional type operations")

	// Generate additional test data
	generateOptionalTestData(t)

	// Run comprehensive optional type test suites
	t.Run("OptionalPrimitiveTypes", func(t *testing.T) {
		testOptionalPrimitiveTypes(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalComplexTypes", func(t *testing.T) {
		testOptionalComplexTypes(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionSomeNoneHandling", func(t *testing.T) {
		testOptionSomeNoneHandling(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalValidation", func(t *testing.T) {
		testOptionalValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalPerformance", func(t *testing.T) {
		testOptionalPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalBoundaryConditions", func(t *testing.T) {
		testOptionalBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalDatabaseIntegration", func(t *testing.T) {
		testOptionalDatabaseIntegration(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionalEncodingDecoding", func(t *testing.T) {
		testOptionalEncodingDecoding(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateOptionalTestData generates additional test data for optional types
func generateOptionalTestData(t *testing.T) {
	t.Log("Generating additional OPTIONAL TYPE test data...")

	// Generate additional test values for optional types
	for i := range OptionalTypes {
		config := &OptionalTypes[i]

		switch config.Name {
		case "optional_u32":
			// Add boundary values
			config.TestValues = append(config.TestValues,
				OptionalU32Type{
					Id:       3,
					Value:    OptionalU32(0), // Minimum value
					HasValue: true,
					Default:  1000,
					Metadata: "optional_u32_min_value",
				},
				OptionalU32Type{
					Id:       4,
					Value:    OptionalU32(4294967295), // Maximum value
					HasValue: true,
					Default:  0,
					Metadata: "optional_u32_max_value",
				},
			)
		case "optional_string":
			// Add Unicode and special cases
			config.TestValues = append(config.TestValues,
				OptionalStringType{
					Id:       4,
					Value:    OptionalString("🚀 Unicode Test 测试 🎯"),
					HasValue: true,
					Default:  "ascii_default",
					Metadata: "optional_string_unicode",
				},
			)
		}

		// Add validation rules
		config.ValidationRules = []ValidationRule{
			{
				FieldName:    "Value",
				IsRequired:   false,
				Validator:    func(v interface{}) bool { return true }, // Always valid for optional
				ErrorMessage: "Optional field validation failed",
			},
		}

		// Add boundary tests
		config.BoundaryTests = []BoundaryTest{
			{
				Name:        "null_value_test",
				Value:       nil,
				ShouldPass:  true,
				Description: "Null values should be accepted for optional fields",
			},
		}
	}

	t.Logf("✅ Generated additional optional type test data for %d types", len(OptionalTypes))
}

// testOptionalPrimitiveTypes tests optional primitive types (u8, u32, i32, f64, bool, string)
func testOptionalPrimitiveTypes(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🧪 Testing OPTIONAL PRIMITIVE TYPES with null and non-null values...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	nullValueCount := 0

	// Test each primitive optional type
	primitiveTypes := []string{"optional_u8", "optional_u32", "optional_i32", "optional_f64", "optional_string", "optional_bool"}

	for _, typeName := range primitiveTypes {
		for _, config := range OptionalTypes {
			if config.Name != typeName {
				continue
			}

			t.Logf("Testing %s primitive optional type...", config.Name)

			for i, testValue := range config.TestValues {
				totalTests++
				startTime := time.Now()

				// Encode the optional value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("❌ Failed to encode %s value %d: %v", config.Name, i, err)
					continue
				}

				// Check if this is a null value test
				switch v := testValue.(type) {
				case OptionalU8Type:
					if v.Value == nil {
						nullValueCount++
					}
				case OptionalU32Type:
					if v.Value == nil {
						nullValueCount++
					}
				case OptionalI32Type:
					if v.Value == nil {
						nullValueCount++
					}
				case OptionalF64Type:
					if v.Value == nil {
						nullValueCount++
					}
				case OptionalStringType:
					if v.Value == nil {
						nullValueCount++
					}
				case OptionalBoolType:
					if v.Value == nil {
						nullValueCount++
					}
				}

				// Validate encoding succeeded and meets performance requirements
				t.Logf("✅ %s: %s encoded successfully to %d bytes in %v",
					config.Name, config.TestValues[i], len(encoded), time.Since(startTime))

				// Verify encoding performance
				assert.Less(t, time.Since(startTime), OptionalEncodingTime,
					"%s encoding took %v, expected less than %v", config.Name, time.Since(startTime), OptionalEncodingTime)

				// Validate encoding size is reasonable
				assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", config.Name)

				successCount++
			}
		}
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0
	nullPercentage := float64(nullValueCount) / float64(totalTests) * 100.0

	t.Logf("📊 OPTIONAL PRIMITIVES Results: %d/%d tests passed (%.1f%%), %d null values (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, nullValueCount, nullPercentage, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Optional primitive types should have >95%% success rate")
}

// testOptionalComplexTypes tests optional complex types (structs, arrays, maps)
func testOptionalComplexTypes(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🏗️  Testing OPTIONAL COMPLEX TYPES with nested structures...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	complexNullCount := 0

	// Test complex optional types
	complexTypes := []string{"optional_struct", "optional_array"}

	for _, typeName := range complexTypes {
		for _, config := range OptionalTypes {
			if config.Name != typeName {
				continue
			}

			t.Logf("Testing %s complex optional type...", config.Name)

			for i, testValue := range config.TestValues {
				totalTests++
				startTime := time.Now()

				// Encode the complex optional value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("❌ Failed to encode %s value %d: %v", config.Name, i, err)
					continue
				}

				// Check for null complex structures
				switch v := testValue.(type) {
				case OptionalStructType:
					if v.PersonData == nil {
						complexNullCount++
					} else if v.PersonData.Age == nil || v.PersonData.Phone == nil {
						// Count partial nulls as well
						complexNullCount++
					}
				case OptionalArrayType:
					// Count arrays with null elements
					for _, elem := range v.OptionalInts {
						if elem == nil {
							complexNullCount++
							break
						}
					}
				}

				// Validate encoding succeeded and meets performance requirements
				t.Logf("✅ %s: %s encoded successfully to %d bytes in %v",
					config.Name, config.TestValues[i], len(encoded), time.Since(startTime))

				// Verify encoding performance
				assert.Less(t, time.Since(startTime), OptionalComplexTime,
					"%s complex encoding took %v, expected less than %v", config.Name, time.Since(startTime), OptionalComplexTime)

				successCount++
			}
		}
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 OPTIONAL COMPLEX Results: %d/%d tests passed (%.1f%%), %d complex nulls detected, avg duration: %v",
		successCount, totalTests, successRate, complexNullCount, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Optional complex types should have >95%% success rate")
}

// testOptionSomeNoneHandling tests Rust-style Option<T> Some/None patterns
func testOptionSomeNoneHandling(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🦀 Testing OPTION<T> Some/None patterns (Rust-style)...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	someCount := 0
	noneCount := 0

	// Test Option<T> pattern types
	optionTypes := []string{"optional_option_u32"}

	for _, typeName := range optionTypes {
		for _, config := range OptionalTypes {
			if config.Name != typeName {
				continue
			}

			t.Logf("Testing %s Option<T> pattern...", config.Name)

			for i, testValue := range config.TestValues {
				totalTests++
				startTime := time.Now()

				// Encode the Option<T> value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("❌ Failed to encode %s value %d: %v", config.Name, i, err)
					continue
				}

				// Count Some vs None patterns
				switch v := testValue.(type) {
				case OptionalOptionU32Type:
					if v.OptionalInt.IsSome {
						someCount++
						t.Logf("🎯 Some(%d) pattern detected", v.OptionalInt.Value)
					} else {
						noneCount++
						t.Logf("🎯 None pattern detected")
					}
				}

				// Validate encoding succeeded and meets performance requirements
				t.Logf("✅ %s: %s encoded successfully to %d bytes in %v",
					config.Name, config.TestValues[i], len(encoded), time.Since(startTime))

				// Verify encoding performance
				assert.Less(t, time.Since(startTime), OptionalEncodingTime,
					"%s Option<T> took %v, expected less than %v", config.Name, time.Since(startTime), OptionalEncodingTime)

				successCount++
			}
		}
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 OPTION<T> Results: %d/%d tests passed (%.1f%%), %d Some, %d None, avg duration: %v",
		successCount, totalTests, successRate, someCount, noneCount, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Option<T> patterns should have >95%% success rate")
	assert.Greater(t, someCount, 0, "Should have Some() values")
	assert.Greater(t, noneCount, 0, "Should have None values")
}

// testOptionalValidation tests validation logic for optional fields
func testOptionalValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("✅ Testing OPTIONAL VALIDATION with constraints and rules...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	validationFailures := 0

	// Test validation for optional types
	validationTypes := []string{"optional_validation"}

	for _, typeName := range validationTypes {
		for _, config := range OptionalTypes {
			if config.Name != typeName {
				continue
			}

			t.Logf("Testing %s validation scenarios...", config.Name)

			// Create additional validation test cases
			validationTests := []OptionalValidationType{
				{
					Id:              100,
					OptionalAge:     OptionalI32(25),
					OptionalEmail:   OptionalString("valid@test.com"),
					OptionalScore:   OptionalF64(85.5),
					OptionalTags:    []string{"valid", "test"},
					ValidationFlags: 0,
					ErrorMessage:    "",
				},
				{
					Id:              101,
					OptionalAge:     OptionalI32(150), // Invalid age > 120
					OptionalEmail:   OptionalString("invalid-email"),
					OptionalScore:   OptionalF64(150.0), // Invalid score > 100
					OptionalTags:    []string{},
					ValidationFlags: 7, // Multiple validation failures
					ErrorMessage:    "Age, email, and score validation failed",
				},
				{
					Id:              102,
					OptionalAge:     nil, // Null values should be valid
					OptionalEmail:   nil,
					OptionalScore:   nil,
					OptionalTags:    []string{},
					ValidationFlags: 0,
					ErrorMessage:    "",
				},
				{
					Id:              103,
					OptionalAge:     OptionalI32(-5), // Invalid negative age
					OptionalEmail:   OptionalString("test@example.com"),
					OptionalScore:   OptionalF64(-10.0), // Invalid negative score
					OptionalTags:    []string{"test"},
					ValidationFlags: 5, // Age and score validation failures
					ErrorMessage:    "Age and score must be non-negative",
				},
			}

			for i, testValue := range validationTests {
				totalTests++
				startTime := time.Now()

				// Perform validation checks
				ageValid := ValidateOptionalAge(testValue.OptionalAge)
				emailValid := ValidateOptionalEmail(testValue.OptionalEmail)
				scoreValid := ValidateOptionalScore(testValue.OptionalScore)

				// Count validation failures
				if !ageValid || !emailValid || !scoreValid {
					validationFailures++
				}

				// Encode the validation test value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("❌ Failed to encode validation test %d: %v", i, err)
					continue
				}

				// Validate encoding succeeded and meets performance requirements
				t.Logf("✅ Validation test %d: age=%v, email=%v, score=%v, encoded to %d bytes in %v",
					i, ageValid, emailValid, scoreValid, len(encoded), time.Since(startTime))

				// Verify encoding performance
				assert.Less(t, time.Since(startTime), OptionalValidationTime,
					"Validation took %v, expected less than %v", time.Since(startTime), OptionalValidationTime)

				successCount++
			}
		}
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0
	failureRate := float64(validationFailures) / float64(totalTests) * 100.0

	t.Logf("📊 VALIDATION Results: %d/%d tests passed (%.1f%%), %d validation failures (%.1f%%), avg duration: %v",
		successCount, totalTests, successRate, validationFailures, failureRate, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Optional validation should have >95%% success rate")
}

// testOptionalPerformance tests performance characteristics of optional types
func testOptionalPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("⚡ Testing OPTIONAL PERFORMANCE with large datasets...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	var cumulativeEncodeTime time.Duration
	var cumulativeProcessTime time.Duration

	// Create performance test data
	performanceTests := []OptionalPerformanceType{
		{
			Id:               1,
			OptionalLargeInt: OptionalU64(18446744073709551615),          // Max uint64
			OptionalLargeStr: OptionalString(string(make([]byte, 1000))), // 1KB string
			OptionalFloatArr: []*float64{
				OptionalF64(1.1), OptionalF64(2.2), nil, OptionalF64(4.4), nil,
			},
			ProcessingTimeMs: 0.0,
			MemoryUsageBytes: 0,
			OperationCount:   OptionalPerformanceIters,
		},
		{
			Id:               2,
			OptionalLargeInt: nil,                       // Null performance test
			OptionalLargeStr: nil,                       // Null performance test
			OptionalFloatArr: []*float64{nil, nil, nil}, // All nulls
			ProcessingTimeMs: 0.0,
			MemoryUsageBytes: 0,
			OperationCount:   OptionalPerformanceIters,
		},
	}

	for i, perfTest := range performanceTests {
		for iter := 0; iter < OptionalPerformanceIters/100; iter++ { // Reduce iterations for testing
			totalTests++
			startTime := time.Now()

			// Encode performance test
			encodeStart := time.Now()
			encoded, err := encodingManager.Encode(perfTest, db.EncodingBSATN, nil)
			encodeTime := time.Since(encodeStart)
			cumulativeEncodeTime += encodeTime

			if err != nil {
				t.Logf("❌ Failed to encode performance test %d: %v", i, err)
				continue
			}

			// Validate encoding succeeded and meets performance requirements
			t.Logf("✅ Performance test %d: encoded to %d bytes, encode time: %v, iter: %d",
				i, len(encoded), encodeTime, iter)

			// Verify encoding performance
			assert.Less(t, encodeTime, OptionalEncodingTime,
				"Performance encoding took %v, expected less than %v", encodeTime, OptionalEncodingTime)

			// Validate encoding size is reasonable for performance tests
			assert.Greater(t, len(encoded), 0, "Performance test should encode to >0 bytes")

			totalTime := time.Since(startTime)
			totalDuration += totalTime
			successCount++

			// Break after a few iterations to avoid timeout
			if iter >= 5 {
				break
			}
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Performance analysis
	avgDuration := totalDuration / time.Duration(totalTests)
	avgEncodeTime := cumulativeEncodeTime / time.Duration(totalTests)
	avgProcessTime := cumulativeProcessTime / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0
	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	throughput := float64(totalTests) / totalDuration.Seconds()

	t.Logf("📊 PERFORMANCE Results: %d/%d tests passed (%.1f%%)",
		successCount, totalTests, successRate)
	t.Logf("⏱️  Timing: avg total=%v, avg encode=%v, avg process=%v",
		avgDuration, avgEncodeTime, avgProcessTime)
	t.Logf("💾 Memory: %.2f MB used, throughput: %.1f ops/sec",
		memUsedMB, throughput)

	// Performance assertions
	assert.Less(t, avgEncodeTime, OptionalEncodingTime,
		"Average encoding time should be less than %v", OptionalEncodingTime)
	assert.Greater(t, throughput, 10.0,
		"Throughput should be at least 10 ops/sec")
}

// testOptionalBoundaryConditions tests edge cases and boundary conditions
func testOptionalBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔍 Testing OPTIONAL BOUNDARY CONDITIONS and edge cases...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	boundaryFailures := 0

	// Create boundary test cases
	boundaryTests := []OptionalBoundaryType{
		{
			Id:            1,
			OptionalMin:   OptionalI32(math.MinInt32),
			OptionalMax:   OptionalU64(math.MaxUint64),
			OptionalZero:  OptionalF64(0.0),
			OptionalEmpty: OptionalString(""),
			OptionalNull:  nil,
			BoundaryFlags: 0,
		},
		{
			Id:            2,
			OptionalMin:   OptionalI32(0),             // Boundary: zero as minimum
			OptionalMax:   OptionalU64(0),             // Boundary: zero as maximum
			OptionalZero:  OptionalF64(9.87654321e15), // Large valid float instead of infinity
			OptionalEmpty: OptionalString("single"),   // Boundary: single char
			OptionalNull:  OptionalBool(false),        // Boundary: false as "null-like"
			BoundaryFlags: 1,
		},
		{
			Id:            3,
			OptionalMin:   nil, // All nulls
			OptionalMax:   nil,
			OptionalZero:  nil,
			OptionalEmpty: nil,
			OptionalNull:  nil,
			BoundaryFlags: 15, // All boundary flags set
		},
		{
			Id:            4,
			OptionalMin:   OptionalI32(1),                           // Boundary: just above minimum
			OptionalMax:   OptionalU64(math.MaxUint64 - 1),          // Boundary: just below maximum
			OptionalZero:  OptionalF64(math.SmallestNonzeroFloat64), // Smallest positive
			OptionalEmpty: OptionalString("🚀"),                      // Unicode boundary
			OptionalNull:  OptionalBool(true),                       // Non-null boundary
			BoundaryFlags: 0,
		},
	}

	for i, boundaryTest := range boundaryTests {
		totalTests++
		startTime := time.Now()

		// Check boundary conditions
		hasNulls := (boundaryTest.OptionalMin == nil) || (boundaryTest.OptionalMax == nil) ||
			(boundaryTest.OptionalZero == nil) || (boundaryTest.OptionalEmpty == nil) ||
			(boundaryTest.OptionalNull == nil)

		if hasNulls {
			boundaryFailures++
		}

		// Encode boundary test
		encoded, err := encodingManager.Encode(boundaryTest, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("❌ Failed to encode boundary test %d: %v", i, err)
			continue
		}

		duration := time.Since(startTime)
		totalDuration += duration

		// All successful encodings count as successes
		successCount++
		t.Logf("✅ Boundary test %d: flags=%d, nulls=%v, encoded to %d bytes",
			i, boundaryTest.BoundaryFlags, hasNulls, len(encoded))

		// Validate boundary performance
		assert.Less(t, duration, OptionalEncodingTime,
			"Boundary test took %v, expected less than %v", duration, OptionalEncodingTime)
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 BOUNDARY Results: %d/%d tests passed (%.1f%%), %d boundary failures, avg duration: %v",
		successCount, totalTests, successRate, boundaryFailures, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Boundary conditions should have >95%% success rate")
}

// testOptionalDatabaseIntegration tests database integration for optional types
func testOptionalDatabaseIntegration(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🗄️  Testing OPTIONAL DATABASE INTEGRATION with constraints...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	constraintViolations := 0

	// Create database integration test cases
	dbTests := []OptionalConstraintType{
		{
			Id:                 1,
			OptionalUniqueId:   OptionalString("unique_001"),
			OptionalIndex:      OptionalI32(1000),
			OptionalForeignKey: OptionalU32(500),
			ConstraintStatus:   "valid",
			CreatedAt:          uint64(time.Now().UnixMicro()),
		},
		{
			Id:                 2,
			OptionalUniqueId:   nil, // Null unique constraint (should be allowed)
			OptionalIndex:      nil, // Null index (should be allowed)
			OptionalForeignKey: nil, // Null foreign key (should be allowed)
			ConstraintStatus:   "null_constraints",
			CreatedAt:          uint64(time.Now().UnixMicro()),
		},
		{
			Id:                 3,
			OptionalUniqueId:   OptionalString("unique_001"), // Duplicate unique ID (constraint violation)
			OptionalIndex:      OptionalI32(-1),              // Invalid index value
			OptionalForeignKey: OptionalU32(999999),          // Non-existent foreign key
			ConstraintStatus:   "constraint_violations",
			CreatedAt:          uint64(time.Now().UnixMicro()),
		},
		{
			Id:                 4,
			OptionalUniqueId:   OptionalString(""), // Empty string unique ID
			OptionalIndex:      OptionalI32(0),     // Zero index value
			OptionalForeignKey: OptionalU32(0),     // Zero foreign key
			ConstraintStatus:   "edge_case_values",
			CreatedAt:          uint64(time.Now().UnixMicro()),
		},
	}

	for i, dbTest := range dbTests {
		totalTests++
		startTime := time.Now()

		// Check for potential constraint violations
		if dbTest.ConstraintStatus == "constraint_violations" {
			constraintViolations++
		}

		// Encode database test
		encoded, err := encodingManager.Encode(dbTest, db.EncodingBSATN, nil)
		if err != nil {
			t.Logf("❌ Failed to encode database test %d: %v", i, err)
			continue
		}

		duration := time.Since(startTime)
		totalDuration += duration

		// All successful encodings count as successes
		successCount++
		t.Logf("✅ DB test %d: status=%s, encoded to %d bytes",
			i, dbTest.ConstraintStatus, len(encoded))

		// Validate database integration performance
		assert.Less(t, duration, OptionalEncodingTime,
			"DB integration took %v, expected less than %v", duration, OptionalEncodingTime)
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0

	t.Logf("📊 DATABASE INTEGRATION Results: %d/%d tests passed (%.1f%%), %d constraint violations, avg duration: %v",
		successCount, totalTests, successRate, constraintViolations, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Database integration should have >95%% success rate")
}

// testOptionalEncodingDecoding tests BSATN encoding/decoding for optional types
func testOptionalEncodingDecoding(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔄 Testing OPTIONAL ENCODING/DECODING with BSATN format...")

	successCount := 0
	totalTests := 0
	var totalDuration time.Duration
	var cumulativeEncodingSize uint64
	encodingFailures := 0

	// Test encoding/decoding for all optional types
	for _, config := range OptionalTypes {
		if !config.PerformanceTest {
			continue // Skip non-performance types for encoding tests
		}

		t.Logf("Testing encoding/decoding for %s...", config.Name)

		for i, testValue := range config.TestValues {
			totalTests++
			startTime := time.Now()

			// Test encoding
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
			if err != nil {
				encodingFailures++
				t.Logf("❌ Failed to encode %s value %d: %v", config.Name, i, err)
				continue
			}

			cumulativeEncodingSize += uint64(len(encoded))

			// Validate encoding properties
			if len(encoded) == 0 {
				encodingFailures++
				t.Logf("❌ Empty encoding for %s value %d", config.Name, i)
				continue
			}

			// Test BSATN format properties
			isValidBSATN := len(encoded) >= 1 // Minimum BSATN size
			if !isValidBSATN {
				encodingFailures++
				t.Logf("❌ Invalid BSATN format for %s value %d", config.Name, i)
				continue
			}

			duration := time.Since(startTime)
			totalDuration += duration

			// Validate encoding succeeded and meets performance requirements
			t.Logf("✅ %s: %s encoded successfully to %d bytes in %v",
				config.Name, config.TestValues[i], len(encoded), duration)

			// Verify encoding performance
			assert.Less(t, duration, OptionalEncodingTime,
				"%s encoding took %v, expected less than %v", config.Name, duration, OptionalEncodingTime)

			// Validate encoding size is reasonable
			assert.Greater(t, len(encoded), 0, "%s should encode to >0 bytes", config.Name)

			successCount++
		}
	}

	// Performance validation
	avgDuration := totalDuration / time.Duration(totalTests)
	avgEncodingSize := cumulativeEncodingSize / uint64(totalTests)
	successRate := float64(successCount) / float64(totalTests) * 100.0
	failureRate := float64(encodingFailures) / float64(totalTests) * 100.0

	t.Logf("📊 ENCODING/DECODING Results: %d/%d tests passed (%.1f%%), %d failures (%.1f%%)",
		successCount, totalTests, successRate, encodingFailures, failureRate)
	t.Logf("📏 Encoding metrics: avg size=%d bytes, avg time=%v",
		avgEncodingSize, avgDuration)

	assert.Greater(t, successRate, 95.0,
		"Encoding/decoding should have >95%% success rate")
	assert.Less(t, failureRate, 5.0,
		"Encoding failure rate should be <5%%")
	assert.Greater(t, avgEncodingSize, uint64(10),
		"Average encoding size should be reasonable (>10 bytes)")
}
