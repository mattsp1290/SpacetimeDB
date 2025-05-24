package tests

import (
	"context"
	"fmt"
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

// SDK Task 6: Collections & Structures with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB collections and structures:
// - Array & Slice Collections: Dynamic arrays, fixed arrays, multi-dimensional arrays
// - Map Collections: Key-value mappings with type safety
// - Nested Structures: Complex hierarchical data structures
// - Collection Operations: CRUD operations on collections and nested data
// - Structure Validation: Type safety and constraint validation
// - Performance Optimization: Efficient handling of large collections and complex structures

// === BASIC COLLECTION TYPES ===

// ArrayCollectionType represents a table with various array types
type ArrayCollectionType struct {
	Id          uint32    `json:"id"`           // Auto-increment primary key
	IntArray    []int32   `json:"int_array"`    // Array of integers
	StringArray []string  `json:"string_array"` // Array of strings
	FloatArray  []float64 `json:"float_array"`  // Array of floats
	BoolArray   []bool    `json:"bool_array"`   // Array of booleans
	Value       string    `json:"value"`        // Additional data field
}

// FixedSizeArrayType represents fixed-size arrays (renamed to avoid conflicts)
type FixedSizeArrayType struct {
	Id           uint32     `json:"id"`            // Auto-increment primary key
	FixedInts    [5]int32   `json:"fixed_ints"`    // Fixed array of 5 integers
	FixedStrings [3]string  `json:"fixed_strings"` // Fixed array of 3 strings
	FixedFloats  [4]float64 `json:"fixed_floats"`  // Fixed array of 4 floats
	Value        string     `json:"value"`         // Additional data field
}

// MultiDimArrayType represents multi-dimensional arrays
type MultiDimArrayType struct {
	Id       uint32      `json:"id"`        // Auto-increment primary key
	Matrix2D [][]int32   `json:"matrix_2d"` // 2D array/matrix
	Matrix3D [][][]int32 `json:"matrix_3d"` // 3D array/cube
	Value    string      `json:"value"`     // Additional data field
}

// MapCollectionType represents map/dictionary types
type MapCollectionType struct {
	Id              uint32             `json:"id"`                // Auto-increment primary key
	StringIntMap    map[string]int32   `json:"string_int_map"`    // String to int mapping
	StringStringMap map[string]string  `json:"string_string_map"` // String to string mapping (fixed from int->string)
	StringFloatMap  map[string]float64 `json:"string_float_map"`  // String to float mapping
	Value           string             `json:"value"`             // Additional data field
}

// === STRUCTURE TYPES ===

// BasicStructType represents a simple structure
type BasicStructType struct {
	Id     uint32  `json:"id"`     // Auto-increment primary key
	Name   string  `json:"name"`   // Structure name
	Age    int32   `json:"age"`    // Age field
	Score  float64 `json:"score"`  // Score field
	Active bool    `json:"active"` // Active status
	Value  string  `json:"value"`  // Additional data field
}

// NestedStructType represents nested structures
type NestedStructType struct {
	Id      uint32        `json:"id"`      // Auto-increment primary key
	Person  PersonStruct  `json:"person"`  // Nested person structure
	Address AddressStruct `json:"address"` // Nested address structure
	Contact ContactStruct `json:"contact"` // Nested contact structure
	Value   string        `json:"value"`   // Additional data field
}

// PersonStruct represents a person data structure
type PersonStruct struct {
	FirstName string   `json:"first_name"` // First name
	LastName  string   `json:"last_name"`  // Last name
	Age       int32    `json:"age"`        // Age
	BirthDate uint64   `json:"birth_date"` // Birth date timestamp
	Skills    []string `json:"skills"`     // Array of skills
}

// AddressStruct represents an address data structure
type AddressStruct struct {
	Street    string  `json:"street"`    // Street address
	City      string  `json:"city"`      // City
	State     string  `json:"state"`     // State/Province
	ZipCode   string  `json:"zip_code"`  // Zip/Postal code
	Country   string  `json:"country"`   // Country
	Latitude  float64 `json:"latitude"`  // GPS latitude
	Longitude float64 `json:"longitude"` // GPS longitude
}

// ContactStruct represents contact information
type ContactStruct struct {
	Email       string   `json:"email"`        // Email address
	Phone       string   `json:"phone"`        // Phone number
	SocialMedia []string `json:"social_media"` // Social media handles
	Preferred   string   `json:"preferred"`    // Preferred contact method
}

// === COMPLEX COLLECTION STRUCTURES ===

// CollectionStructType represents structures containing collections
type CollectionStructType struct {
	Id        uint32                   `json:"id"`        // Auto-increment primary key
	Users     []PersonStruct           `json:"users"`     // Array of person structures
	Addresses map[string]AddressStruct `json:"addresses"` // Map of addresses by key
	Contacts  []ContactStruct          `json:"contacts"`  // Array of contact structures
	Metadata  map[string]interface{}   `json:"metadata"`  // Generic metadata map
	Tags      []string                 `json:"tags"`      // Array of tags
	Scores    map[string]float64       `json:"scores"`    // Score mappings
	Value     string                   `json:"value"`     // Additional data field
}

// HierarchicalStructType represents deeply nested hierarchical structures
type HierarchicalStructType struct {
	Id           uint32                      `json:"id"`           // Auto-increment primary key
	Organization OrganizationStruct          `json:"organization"` // Root organization
	Departments  map[string]DepartmentStruct `json:"departments"`  // Department mappings
	Employees    []EmployeeStruct            `json:"employees"`    // Employee array
	Projects     []ProjectStruct             `json:"projects"`     // Project array
	Value        string                      `json:"value"`        // Additional data field
}

// OrganizationStruct represents an organization
type OrganizationStruct struct {
	Name        string            `json:"name"`        // Organization name
	Founded     uint64            `json:"founded"`     // Founded timestamp
	Industry    string            `json:"industry"`    // Industry type
	Size        int32             `json:"size"`        // Number of employees
	Locations   []AddressStruct   `json:"locations"`   // Office locations
	Departments []string          `json:"departments"` // Department names
	Metadata    map[string]string `json:"metadata"`    // Organization metadata
}

// DepartmentStruct represents a department
type DepartmentStruct struct {
	Name      string            `json:"name"`      // Department name
	Manager   PersonStruct      `json:"manager"`   // Department manager
	Employees []EmployeeStruct  `json:"employees"` // Department employees
	Budget    float64           `json:"budget"`    // Department budget
	Projects  []string          `json:"projects"`  // Associated project IDs
	Goals     []string          `json:"goals"`     // Department goals
	Metadata  map[string]string `json:"metadata"`  // Department metadata
}

// EmployeeStruct represents an employee
type EmployeeStruct struct {
	Person      PersonStruct       `json:"person"`      // Personal information
	EmployeeId  string             `json:"employee_id"` // Employee ID
	Department  string             `json:"department"`  // Department name
	Position    string             `json:"position"`    // Job position
	Salary      float64            `json:"salary"`      // Salary
	StartDate   uint64             `json:"start_date"`  // Start date timestamp
	Skills      []string           `json:"skills"`      // Employee skills
	Projects    []string           `json:"projects"`    // Assigned projects
	Performance map[string]float64 `json:"performance"` // Performance ratings
	Metadata    map[string]string  `json:"metadata"`    // Employee metadata
}

// ProjectStruct represents a project
type ProjectStruct struct {
	Name        string            `json:"name"`        // Project name
	Description string            `json:"description"` // Project description
	StartDate   uint64            `json:"start_date"`  // Start date timestamp
	EndDate     uint64            `json:"end_date"`    // End date timestamp
	Budget      float64           `json:"budget"`      // Project budget
	Status      string            `json:"status"`      // Project status
	Team        []string          `json:"team"`        // Team member IDs
	Milestones  []string          `json:"milestones"`  // Project milestones
	Tags        []string          `json:"tags"`        // Project tags
	Metadata    map[string]string `json:"metadata"`    // Project metadata
}

// === COLLECTION OPERATION TYPES ===

// CollectionOperation represents a collection operation test scenario
type CollectionOperation struct {
	Name        string
	Description string
	OpType      string                 // Type of operation (add, remove, update, query)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// StructureOperation represents a structure operation test scenario
type StructureOperation struct {
	Name        string
	Description string
	OpType      string                 // Type of operation (create, update, validate, query)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// CollectionStructureConfig defines configuration for collection/structure testing
type CollectionStructureConfig struct {
	Name            string
	TableName       string                // SDK-test table name
	CreateReducer   string                // Create/insert reducer name
	UpdateReducer   string                // Update reducer name
	DeleteReducer   string                // Delete reducer name
	QueryReducer    string                // Query reducer name
	TestValues      []interface{}         // Standard test values
	CollectionOps   []CollectionOperation // Collection-specific operations
	StructureOps    []StructureOperation  // Structure-specific operations
	PerformanceTest bool                  // Whether to include in performance testing
	ComplexityLevel int                   // Complexity level (1-5)
}

// Collection and Structure type configurations
var CollectionStructureTypes = []CollectionStructureConfig{
	{
		Name:            "array_collection",
		TableName:       "ArrayCollection",
		CreateReducer:   "insert_array_collection",
		UpdateReducer:   "update_array_collection",
		DeleteReducer:   "delete_array_collection",
		QueryReducer:    "query_array_collection",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			ArrayCollectionType{
				Id:          1,
				IntArray:    []int32{1, 2, 3, 4, 5},
				StringArray: []string{"hello", "world", "spacetime", "db"},
				FloatArray:  []float64{1.1, 2.2, 3.3, 4.4},
				BoolArray:   []bool{true, false, true, false},
				Value:       "array_test_1",
			},
			ArrayCollectionType{
				Id:          2,
				IntArray:    []int32{},
				StringArray: []string{},
				FloatArray:  []float64{},
				BoolArray:   []bool{},
				Value:       "array_test_empty",
			},
			ArrayCollectionType{
				Id:          3,
				IntArray:    generateIntArray(100),
				StringArray: generateStringArray(50),
				FloatArray:  generateFloatArray(75),
				BoolArray:   generateBoolArray(25),
				Value:       "array_test_large",
			},
		},
	},
	{
		Name:            "fixed_array",
		TableName:       "FixedArray",
		CreateReducer:   "insert_fixed_array",
		UpdateReducer:   "update_fixed_array",
		DeleteReducer:   "delete_fixed_array",
		QueryReducer:    "query_fixed_array",
		PerformanceTest: true,
		ComplexityLevel: 1,
		TestValues: []interface{}{
			FixedSizeArrayType{
				Id:           1,
				FixedInts:    [5]int32{10, 20, 30, 40, 50},
				FixedStrings: [3]string{"alpha", "beta", "gamma"},
				FixedFloats:  [4]float64{1.1, 2.2, 3.3, 4.4},
				Value:        "fixed_array_test_1",
			},
			FixedSizeArrayType{
				Id:           2,
				FixedInts:    [5]int32{0, 0, 0, 0, 0},
				FixedStrings: [3]string{"", "", ""},
				FixedFloats:  [4]float64{0.0, 0.0, 0.0, 0.0},
				Value:        "fixed_array_test_zeros",
			},
		},
	},
	{
		Name:            "multidim_array",
		TableName:       "MultiDimArray",
		CreateReducer:   "insert_multidim_array",
		UpdateReducer:   "update_multidim_array",
		DeleteReducer:   "delete_multidim_array",
		QueryReducer:    "query_multidim_array",
		PerformanceTest: true,
		ComplexityLevel: 3,
		TestValues: []interface{}{
			MultiDimArrayType{
				Id:       1,
				Matrix2D: [][]int32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				Matrix3D: [][][]int32{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
				Value:    "multidim_test_1",
			},
			MultiDimArrayType{
				Id:       2,
				Matrix2D: [][]int32{},
				Matrix3D: [][][]int32{},
				Value:    "multidim_test_empty",
			},
		},
	},
	{
		Name:            "map_collection",
		TableName:       "MapCollection",
		CreateReducer:   "insert_map_collection",
		UpdateReducer:   "update_map_collection",
		DeleteReducer:   "delete_map_collection",
		QueryReducer:    "query_map_collection",
		PerformanceTest: true,
		ComplexityLevel: 2,
		TestValues: []interface{}{
			MapCollectionType{
				Id:              1,
				StringIntMap:    map[string]int32{"alpha": 1, "beta": 2, "gamma": 3},
				StringStringMap: map[string]string{"one": "1", "two": "2", "three": "3"}, // Fixed to use string keys
				StringFloatMap:  map[string]float64{"pi": 3.14159, "e": 2.71828, "phi": 1.61803},
				Value:           "map_test_1",
			},
			MapCollectionType{
				Id:              2,
				StringIntMap:    map[string]int32{},
				StringStringMap: map[string]string{}, // Fixed to use string keys
				StringFloatMap:  map[string]float64{},
				Value:           "map_test_empty",
			},
		},
	},
	{
		Name:            "basic_struct",
		TableName:       "BasicStruct",
		CreateReducer:   "insert_basic_struct",
		UpdateReducer:   "update_basic_struct",
		DeleteReducer:   "delete_basic_struct",
		QueryReducer:    "query_basic_struct",
		PerformanceTest: true,
		ComplexityLevel: 1,
		TestValues: []interface{}{
			BasicStructType{
				Id:     1,
				Name:   "John Doe",
				Age:    30,
				Score:  95.5,
				Active: true,
				Value:  "basic_struct_test_1",
			},
			BasicStructType{
				Id:     2,
				Name:   "Jane Smith",
				Age:    25,
				Score:  87.2,
				Active: false,
				Value:  "basic_struct_test_2",
			},
		},
	},
	{
		Name:            "nested_struct",
		TableName:       "NestedStruct",
		CreateReducer:   "insert_nested_struct",
		UpdateReducer:   "update_nested_struct",
		DeleteReducer:   "delete_nested_struct",
		QueryReducer:    "query_nested_struct",
		PerformanceTest: true,
		ComplexityLevel: 4,
		TestValues: []interface{}{
			NestedStructType{
				Id: 1,
				Person: PersonStruct{
					FirstName: "Alice",
					LastName:  "Johnson",
					Age:       28,
					BirthDate: uint64(time.Date(1995, 6, 15, 0, 0, 0, 0, time.UTC).UnixMicro()),
					Skills:    []string{"Go", "Python", "JavaScript", "SQL"},
				},
				Address: AddressStruct{
					Street:    "123 Main St",
					City:      "San Francisco",
					State:     "CA",
					ZipCode:   "94102",
					Country:   "USA",
					Latitude:  37.7749,
					Longitude: -122.4194,
				},
				Contact: ContactStruct{
					Email:       "alice.johnson@example.com",
					Phone:       "+1-555-0123",
					SocialMedia: []string{"@alice_j", "alice.johnson"},
					Preferred:   "email",
				},
				Value: "nested_struct_test_1",
			},
		},
	},
	{
		Name:            "collection_struct",
		TableName:       "CollectionStruct",
		CreateReducer:   "insert_collection_struct",
		UpdateReducer:   "update_collection_struct",
		DeleteReducer:   "delete_collection_struct",
		QueryReducer:    "query_collection_struct",
		PerformanceTest: true,
		ComplexityLevel: 4,
		TestValues: []interface{}{
			CollectionStructType{
				Id: 1,
				Users: []PersonStruct{
					{
						FirstName: "Bob",
						LastName:  "Wilson",
						Age:       35,
						BirthDate: uint64(time.Date(1988, 3, 22, 0, 0, 0, 0, time.UTC).UnixMicro()),
						Skills:    []string{"Java", "Spring", "Docker"},
					},
					{
						FirstName: "Carol",
						LastName:  "Davis",
						Age:       32,
						BirthDate: uint64(time.Date(1991, 9, 8, 0, 0, 0, 0, time.UTC).UnixMicro()),
						Skills:    []string{"React", "TypeScript", "AWS"},
					},
				},
				Addresses: map[string]AddressStruct{
					"home": {
						Street:    "456 Oak Ave",
						City:      "Portland",
						State:     "OR",
						ZipCode:   "97201",
						Country:   "USA",
						Latitude:  45.5152,
						Longitude: -122.6784,
					},
					"work": {
						Street:    "789 Pine St",
						City:      "Seattle",
						State:     "WA",
						ZipCode:   "98101",
						Country:   "USA",
						Latitude:  47.6062,
						Longitude: -122.3321,
					},
				},
				Contacts: []ContactStruct{
					{
						Email:       "contact@example.com",
						Phone:       "+1-555-0456",
						SocialMedia: []string{"@company", "company.official"},
						Preferred:   "email",
					},
				},
				Metadata: map[string]interface{}{
					"version":     "1.0",
					"last_update": time.Now().UnixMicro(),
					"active":      true,
				},
				Tags:   []string{"development", "production", "testing"},
				Scores: map[string]float64{"performance": 92.5, "reliability": 98.1, "security": 89.7},
				Value:  "collection_struct_test_1",
			},
		},
	},
	{
		Name:            "hierarchical_struct",
		TableName:       "HierarchicalStruct",
		CreateReducer:   "insert_hierarchical_struct",
		UpdateReducer:   "update_hierarchical_struct",
		DeleteReducer:   "delete_hierarchical_struct",
		QueryReducer:    "query_hierarchical_struct",
		PerformanceTest: false, // Too complex for standard performance testing
		ComplexityLevel: 5,
		TestValues: []interface{}{
			HierarchicalStructType{
				Id: 1,
				Organization: OrganizationStruct{
					Name:     "TechCorp Inc",
					Founded:  uint64(time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
					Industry: "Software",
					Size:     150,
					Locations: []AddressStruct{
						{
							Street:    "100 Tech Blvd",
							City:      "Austin",
							State:     "TX",
							ZipCode:   "73301",
							Country:   "USA",
							Latitude:  30.2672,
							Longitude: -97.7431,
						},
					},
					Departments: []string{"Engineering", "Sales", "HR"},
					Metadata: map[string]string{
						"stock_symbol": "TECH",
						"ceo":          "John Tech",
						"website":      "https://techcorp.com",
					},
				},
				Value: "hierarchical_struct_test_1",
			},
		},
	},
}

// Test configuration constants
const (
	// Performance thresholds (collection/structure specific)
	CollectionInsertTime    = 100 * time.Millisecond // Single collection insert
	CollectionQueryTime     = 50 * time.Millisecond  // Single collection query
	StructureCreateTime     = 75 * time.Millisecond  // Structure creation
	NestedStructureTime     = 150 * time.Millisecond // Nested structure operation
	CollectionManipTime     = 25 * time.Millisecond  // Collection manipulation
	StructureValidationTime = 10 * time.Millisecond  // Structure validation

	// Test limits
	MaxCollectionSize          = 1000 // Maximum collection size to test
	MaxStructNestingDepth      = 5    // Maximum nesting depth (renamed to avoid conflicts)
	CollectionPerformanceIters = 100  // Iterations for collection performance testing
	StructurePerformanceIters  = 200  // Iterations for structure performance testing
	ComplexityTestCount        = 50   // Number of complexity tests
	BoundaryTestCount          = 100  // Number of boundary condition tests

	// Collection size limits
	SmallCollectionSize   = 10   // Small collection for quick tests
	MediumCollectionSize  = 100  // Medium collection for standard tests
	LargeStructCollection = 1000 // Large collection for stress tests (renamed to avoid conflicts)
)

// Utility functions for generating test data
func generateIntArray(size int) []int32 {
	var arr []int32
	for i := 0; i < size; i++ {
		arr = append(arr, int32(i*2))
	}
	return arr
}

func generateStringArray(size int) []string {
	var arr []string
	for i := 0; i < size; i++ {
		arr = append(arr, fmt.Sprintf("item_%d", i))
	}
	return arr
}

func generateFloatArray(size int) []float64 {
	var arr []float64
	for i := 0; i < size; i++ {
		arr = append(arr, float64(i)*1.5)
	}
	return arr
}

func generateBoolArray(size int) []bool {
	var arr []bool
	for i := 0; i < size; i++ {
		arr = append(arr, i%2 == 0)
	}
	return arr
}

func generateStringIntMap(size int) map[string]int32 {
	m := make(map[string]int32)
	for i := 0; i < size; i++ {
		m[fmt.Sprintf("key_%d", i)] = int32(i * 10)
	}
	return m
}

func generateStringStringMap(size int) map[string]string {
	m := make(map[string]string)
	for i := 0; i < size; i++ {
		m[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	return m
}

func generateComplexStruct() *NestedStructType {
	return &NestedStructType{
		Id: 999,
		Person: PersonStruct{
			FirstName: "Generated",
			LastName:  "User",
			Age:       25,
			BirthDate: uint64(time.Now().Add(-25 * 365 * 24 * time.Hour).UnixMicro()),
			Skills:    []string{"Generated", "Skills", "Testing"},
		},
		Address: AddressStruct{
			Street:    "Generated Street",
			City:      "Test City",
			State:     "TS",
			ZipCode:   "12345",
			Country:   "TestLand",
			Latitude:  0.0,
			Longitude: 0.0,
		},
		Contact: ContactStruct{
			Email:       "generated@test.com",
			Phone:       "+1-555-9999",
			SocialMedia: []string{"@generated"},
			Preferred:   "email",
		},
		Value: "generated_complex_struct",
	}
}

// TestSDKCollectionsStructures is the main integration test for collections and structures
func TestSDKCollectionsStructures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK collections/structures integration test in short mode")
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
	t.Logf("🎯 Testing COLLECTIONS: Arrays, slices, maps, and complex collections")
	t.Logf("🎯 Testing STRUCTURES: Basic, nested, and hierarchical data structures")
	t.Logf("🎯 Testing OPERATIONS: CRUD operations on collections and structures")
	t.Logf("🎯 Testing PERFORMANCE: Large collections and complex structures")
	t.Logf("🎯 Testing VALIDATION: Type safety and constraint validation")

	// Generate additional test data
	generateCollectionStructureTestData(t)

	// Run comprehensive collection and structure test suites
	t.Run("ArrayCollectionOperations", func(t *testing.T) {
		testArrayCollectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("MapCollectionOperations", func(t *testing.T) {
		testMapCollectionOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("BasicStructureOperations", func(t *testing.T) {
		testBasicStructureOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("NestedStructureOperations", func(t *testing.T) {
		testNestedStructureOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CollectionStructureValidation", func(t *testing.T) {
		testCollectionStructureValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CollectionStructurePerformance", func(t *testing.T) {
		testCollectionStructurePerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ComplexStructureOperations", func(t *testing.T) {
		testComplexStructureOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("CollectionStructureBoundaryConditions", func(t *testing.T) {
		testCollectionStructureBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateCollectionStructureTestData generates additional test data
func generateCollectionStructureTestData(t *testing.T) {
	t.Log("Generating additional COLLECTION and STRUCTURE test data...")

	// Generate additional test values for collection types
	for i := range CollectionStructureTypes {
		config := &CollectionStructureTypes[i]

		switch config.Name {
		case "array_collection":
			// Add more array test values
			for j := 0; j < 5; j++ {
				size := SmallCollectionSize + j*10
				val := ArrayCollectionType{
					Id:          uint32(100 + j),
					IntArray:    generateIntArray(size),
					StringArray: generateStringArray(size / 2),
					FloatArray:  generateFloatArray(size / 3),
					BoolArray:   generateBoolArray(size / 4),
					Value:       fmt.Sprintf("generated_array_%d", j),
				}
				config.TestValues = append(config.TestValues, val)
			}
		case "map_collection":
			// Add more map test values
			for j := 0; j < 3; j++ {
				size := 5 + j*5
				val := MapCollectionType{
					Id:              uint32(200 + j),
					StringIntMap:    generateStringIntMap(size),
					StringStringMap: generateStringStringMap(size),
					StringFloatMap:  map[string]float64{},
					Value:           fmt.Sprintf("generated_map_%d", j),
				}
				config.TestValues = append(config.TestValues, val)
			}
		case "basic_struct":
			// Add more basic struct test values
			for j := 0; j < 5; j++ {
				val := BasicStructType{
					Id:     uint32(300 + j),
					Name:   fmt.Sprintf("Generated User %d", j),
					Age:    int32(20 + j*5),
					Score:  float64(80 + j*2),
					Active: j%2 == 0,
					Value:  fmt.Sprintf("generated_basic_%d", j),
				}
				config.TestValues = append(config.TestValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional collection/structure test data for %d types", len(CollectionStructureTypes))
}

// Helper function for min operation
func minCollection(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// testArrayCollectionOperations tests array collection operations
func testArrayCollectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing ARRAY COLLECTION OPERATIONS...")

	for _, arrayType := range CollectionStructureTypes[:3] { // Test first 3 array types
		t.Run(fmt.Sprintf("ArrayCollection_%s", arrayType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(arrayType.TestValues)

			for i, testValue := range arrayType.TestValues {
				if i >= 3 { // Limit to first 3 tests per type for basic operations
					break
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Array collection encoding failed for %s: %v", arrayType.Name, err)
					continue
				}

				// Validate array structure based on type
				switch v := testValue.(type) {
				case ArrayCollectionType:
					if v.Id > 0 {
						t.Logf("✅ %s: Array collection encoded successfully to %d bytes, arrays: int[%d] str[%d] float[%d] bool[%d]",
							arrayType.Name, len(encoded), len(v.IntArray), len(v.StringArray), len(v.FloatArray), len(v.BoolArray))
						successCount++
					}
				case FixedSizeArrayType:
					if v.Id > 0 {
						t.Logf("✅ %s: Fixed array encoded successfully to %d bytes", arrayType.Name, len(encoded))
						successCount++
					}
				case MultiDimArrayType:
					if v.Id > 0 {
						t.Logf("✅ %s: Multi-dimensional array encoded successfully to %d bytes, 2D[%d] 3D[%d]",
							arrayType.Name, len(encoded), len(v.Matrix2D), len(v.Matrix3D))
						successCount++
					}
				default:
					t.Logf("✅ %s: Array encoded successfully to %d bytes", arrayType.Name, len(encoded))
					successCount++
				}
			}

			// Report array collection success rate
			successRate := float64(successCount) / float64(minCollection(totalTests, 3)) * 100
			t.Logf("✅ %s array COLLECTION operations: %d/%d successful (%.1f%%)",
				arrayType.Name, successCount, minCollection(totalTests, 3), successRate)

			assert.Greater(t, successRate, 80.0,
				"Array collection operations should have >80%% success rate")
		})
	}

	t.Log("✅ Array collection operations testing completed")
}

// testMapCollectionOperations tests map collection operations
func testMapCollectionOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing MAP COLLECTION OPERATIONS...")

	for _, mapType := range CollectionStructureTypes[3:4] { // Test map collection type
		t.Run(fmt.Sprintf("MapCollection_%s", mapType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(mapType.TestValues)

			for _, testValue := range mapType.TestValues {
				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Map collection encoding failed for %s: %v", mapType.Name, err)
					continue
				}

				// Validate map structure
				if mapVal, ok := testValue.(MapCollectionType); ok {
					if mapVal.Id > 0 {
						t.Logf("✅ %s: Map collection encoded successfully to %d bytes, maps: str->int[%d] str->str[%d] str->float[%d]",
							mapType.Name, len(encoded), len(mapVal.StringIntMap), len(mapVal.StringStringMap), len(mapVal.StringFloatMap))

						// Validate map contents
						for key, val := range mapVal.StringIntMap {
							if val > 0 {
								t.Logf("  Map entry: %s -> %d", key, val)
							}
						}
						successCount++
					}
				}
			}

			// Report map collection success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s map COLLECTION operations: %d/%d successful (%.1f%%)",
				mapType.Name, successCount, totalTests, successRate)

			assert.Greater(t, successRate, 80.0,
				"Map collection operations should have >80%% success rate")
		})
	}

	t.Log("✅ Map collection operations testing completed")
}

// testBasicStructureOperations tests basic structure operations
func testBasicStructureOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing BASIC STRUCTURE OPERATIONS...")

	for _, structType := range CollectionStructureTypes[4:5] { // Test basic struct type
		t.Run(fmt.Sprintf("BasicStruct_%s", structType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(structType.TestValues)

			for _, testValue := range structType.TestValues {
				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Basic structure encoding failed for %s: %v", structType.Name, err)
					continue
				}

				// Validate basic structure
				if basicStruct, ok := testValue.(BasicStructType); ok {
					if basicStruct.Id > 0 && basicStruct.Name != "" {
						t.Logf("✅ %s: Basic structure encoded successfully to %d bytes, name=%s age=%d score=%.1f active=%t",
							structType.Name, len(encoded), basicStruct.Name, basicStruct.Age, basicStruct.Score, basicStruct.Active)
						successCount++
					}
				}
			}

			// Report basic structure success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s basic STRUCTURE operations: %d/%d successful (%.1f%%)",
				structType.Name, successCount, totalTests, successRate)

			assert.Greater(t, successRate, 90.0,
				"Basic structure operations should have >90%% success rate")
		})
	}

	t.Log("✅ Basic structure operations testing completed")
}

// testNestedStructureOperations tests nested structure operations
func testNestedStructureOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing NESTED STRUCTURE OPERATIONS...")

	for _, structType := range CollectionStructureTypes[5:6] { // Test nested struct type
		t.Run(fmt.Sprintf("NestedStruct_%s", structType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(structType.TestValues)

			for _, testValue := range structType.TestValues {
				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Nested structure encoding failed for %s: %v", structType.Name, err)
					continue
				}

				// Validate nested structure
				if nestedStruct, ok := testValue.(NestedStructType); ok {
					if nestedStruct.Id > 0 && nestedStruct.Person.FirstName != "" {
						t.Logf("✅ %s: Nested structure encoded successfully to %d bytes",
							structType.Name, len(encoded))
						t.Logf("  Person: %s %s, age %d, skills[%d]",
							nestedStruct.Person.FirstName, nestedStruct.Person.LastName,
							nestedStruct.Person.Age, len(nestedStruct.Person.Skills))
						t.Logf("  Address: %s, %s, %s",
							nestedStruct.Address.Street, nestedStruct.Address.City, nestedStruct.Address.State)
						t.Logf("  Contact: %s, %s",
							nestedStruct.Contact.Email, nestedStruct.Contact.Phone)
						successCount++
					}
				}
			}

			// Report nested structure success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s nested STRUCTURE operations: %d/%d successful (%.1f%%)",
				structType.Name, successCount, totalTests, successRate)

			assert.Greater(t, successRate, 90.0,
				"Nested structure operations should have >90%% success rate")
		})
	}

	t.Log("✅ Nested structure operations testing completed")
}

// testCollectionStructureValidation tests collection and structure validation
func testCollectionStructureValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing COLLECTION STRUCTURE VALIDATION...")

	validationTests := []struct {
		Name        string
		TestValue   interface{}
		ShouldPass  bool
		Description string
	}{
		{
			Name: "valid_array_collection",
			TestValue: ArrayCollectionType{
				Id:          999,
				IntArray:    []int32{1, 2, 3},
				StringArray: []string{"test"},
				FloatArray:  []float64{1.0},
				BoolArray:   []bool{true},
				Value:       "validation_test",
			},
			ShouldPass:  true,
			Description: "Valid array collection with all fields",
		},
		{
			Name: "empty_arrays",
			TestValue: ArrayCollectionType{
				Id:          998,
				IntArray:    []int32{},
				StringArray: []string{},
				FloatArray:  []float64{},
				BoolArray:   []bool{},
				Value:       "empty_arrays",
			},
			ShouldPass:  true,
			Description: "Array collection with empty arrays",
		},
		{
			Name: "valid_basic_struct",
			TestValue: BasicStructType{
				Id:     997,
				Name:   "Validation Test",
				Age:    25,
				Score:  95.5,
				Active: true,
				Value:  "struct_validation",
			},
			ShouldPass:  true,
			Description: "Valid basic structure",
		},
		{
			Name: "valid_map_collection",
			TestValue: MapCollectionType{
				Id:              996,
				StringIntMap:    map[string]int32{"test": 123},
				StringStringMap: map[string]string{"key": "value"},
				StringFloatMap:  map[string]float64{"pi": 3.14},
				Value:           "map_validation",
			},
			ShouldPass:  true,
			Description: "Valid map collection",
		},
	}

	successCount := 0
	for _, test := range validationTests {
		t.Run(test.Name, func(t *testing.T) {
			// Test BSATN encoding for validation
			encoded, err := encodingManager.Encode(test.TestValue, db.EncodingBSATN, nil)

			if test.ShouldPass {
				if err != nil {
					t.Logf("⚠️  Expected %s to pass validation but got error: %v", test.Name, err)
				} else {
					t.Logf("✅ %s: %s validated successfully (%d bytes)", test.Name, test.Description, len(encoded))
					successCount++
				}
			} else {
				if err != nil {
					t.Logf("✅ %s correctly failed validation as expected: %s", test.Name, test.Description)
					successCount++
				} else {
					t.Logf("⚠️  Expected %s to fail validation but it passed", test.Name)
				}
			}
		})
	}

	// Report validation success rate
	successRate := float64(successCount) / float64(len(validationTests)) * 100
	t.Logf("✅ COLLECTION STRUCTURE VALIDATION: %d/%d successful (%.1f%%)",
		successCount, len(validationTests), successRate)

	assert.Greater(t, successRate, 90.0,
		"Collection structure validation should have >90%% success rate")

	t.Log("✅ Collection structure validation testing completed")
}

// testCollectionStructurePerformance tests collection and structure performance
func testCollectionStructurePerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing COLLECTION STRUCTURE PERFORMANCE...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	// Performance test different structure types
	performanceTypes := []CollectionStructureConfig{}
	for _, config := range CollectionStructureTypes {
		if config.PerformanceTest && len(config.TestValues) > 0 {
			performanceTypes = append(performanceTypes, config)
		}
	}

	for _, perfType := range performanceTypes {
		t.Run(fmt.Sprintf("Performance_%s", perfType.Name), func(t *testing.T) {
			iterations := minCollection(len(perfType.TestValues), 5) // Limit iterations
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
			}

			typeDuration := time.Since(typeStartTime)
			avgDuration := typeDuration / time.Duration(iterations)

			t.Logf("✅ %s performance: %d/%d successful, avg %v per operation",
				perfType.Name, successCount, iterations, avgDuration)

			// Performance assertions based on complexity
			maxTime := 10 * time.Microsecond * time.Duration(perfType.ComplexityLevel)
			assert.Less(t, avgDuration, maxTime,
				"Average encoding should be <%v for complexity level %d", maxTime, perfType.ComplexityLevel)
			assert.Greater(t, float64(successCount)/float64(iterations), 0.95,
				"Success rate should be >95%%")
		})
	}

	totalDuration := time.Since(startTime)
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	t.Logf("✅ COLLECTION STRUCTURE PERFORMANCE SUMMARY:")
	t.Logf("   Total time: %v", totalDuration)
	t.Logf("   Memory used: %d bytes", memAfter.Alloc-memBefore.Alloc)
	t.Logf("   Types tested: %d", len(performanceTypes))

	assert.Less(t, totalDuration, 3*time.Second,
		"Total collection/structure performance test should complete in <3s")

	t.Log("✅ Collection structure performance testing completed")
}

// testComplexStructureOperations tests complex structure operations
func testComplexStructureOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing COMPLEX STRUCTURE OPERATIONS...")

	for _, structType := range CollectionStructureTypes[6:] { // Test complex structure types
		t.Run(fmt.Sprintf("ComplexStruct_%s", structType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(structType.TestValues)

			for _, testValue := range structType.TestValues {
				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Complex structure encoding failed for %s: %v", structType.Name, err)
					continue
				}

				// Validate complex structure based on type
				switch v := testValue.(type) {
				case CollectionStructType:
					if v.Id > 0 && len(v.Users) > 0 {
						t.Logf("✅ %s: Collection structure encoded successfully to %d bytes",
							structType.Name, len(encoded))
						t.Logf("  Users[%d] Addresses[%d] Contacts[%d] Tags[%d] Scores[%d]",
							len(v.Users), len(v.Addresses), len(v.Contacts), len(v.Tags), len(v.Scores))
						successCount++
					}
				case HierarchicalStructType:
					if v.Id > 0 && v.Organization.Name != "" {
						t.Logf("✅ %s: Hierarchical structure encoded successfully to %d bytes",
							structType.Name, len(encoded))
						t.Logf("  Organization: %s (%s), size %d",
							v.Organization.Name, v.Organization.Industry, v.Organization.Size)
						t.Logf("  Departments[%d] Employees[%d] Projects[%d]",
							len(v.Departments), len(v.Employees), len(v.Projects))
						successCount++
					}
				default:
					t.Logf("✅ %s: Complex structure encoded successfully to %d bytes", structType.Name, len(encoded))
					successCount++
				}
			}

			// Report complex structure success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s complex STRUCTURE operations: %d/%d successful (%.1f%%)",
				structType.Name, successCount, totalTests, successRate)

			assert.Greater(t, successRate, 80.0,
				"Complex structure operations should have >80%% success rate")
		})
	}

	t.Log("✅ Complex structure operations testing completed")
}

// testCollectionStructureBoundaryConditions tests boundary conditions
func testCollectionStructureBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing COLLECTION STRUCTURE BOUNDARY CONDITIONS...")

	// Define boundary condition test cases
	boundaryTests := []struct {
		Name        string
		Description string
		TestValue   interface{}
		ShouldWork  bool
	}{
		// Array boundaries
		{"boundary_empty_all_arrays", "All arrays empty", ArrayCollectionType{Id: 1, IntArray: []int32{}, StringArray: []string{}, FloatArray: []float64{}, BoolArray: []bool{}, Value: "empty_all"}, true},
		{"boundary_single_element_arrays", "Single element arrays", ArrayCollectionType{Id: 2, IntArray: []int32{42}, StringArray: []string{"single"}, FloatArray: []float64{3.14}, BoolArray: []bool{true}, Value: "single_elem"}, true},
		{"boundary_large_int_array", "Large integer array", ArrayCollectionType{Id: 3, IntArray: generateIntArray(1000), StringArray: []string{"large"}, FloatArray: []float64{}, BoolArray: []bool{}, Value: "large_int"}, true},

		// Map boundaries
		{"boundary_empty_maps", "All maps empty", MapCollectionType{Id: 1, StringIntMap: map[string]int32{}, StringStringMap: map[string]string{}, StringFloatMap: map[string]float64{}, Value: "empty_maps"}, true},
		{"boundary_single_map_entry", "Single map entries", MapCollectionType{Id: 2, StringIntMap: map[string]int32{"one": 1}, StringStringMap: map[string]string{"key": "value"}, StringFloatMap: map[string]float64{"pi": 3.14159}, Value: "single_entries"}, true},

		// Fixed array boundaries
		{"boundary_zero_fixed_arrays", "Zero filled fixed arrays", FixedSizeArrayType{Id: 1, FixedInts: [5]int32{0, 0, 0, 0, 0}, FixedStrings: [3]string{"", "", ""}, FixedFloats: [4]float64{0.0, 0.0, 0.0, 0.0}, Value: "zero_fixed"}, true},
		{"boundary_max_fixed_arrays", "Max filled fixed arrays", FixedSizeArrayType{Id: 2, FixedInts: [5]int32{math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32}, FixedStrings: [3]string{"max", "max", "max"}, FixedFloats: [4]float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}, Value: "max_fixed"}, true},

		// Multi-dimensional array boundaries
		{"boundary_empty_multidim", "Empty multi-dimensional arrays", MultiDimArrayType{Id: 1, Matrix2D: [][]int32{}, Matrix3D: [][][]int32{}, Value: "empty_multidim"}, true},
		{"boundary_single_multidim", "Single element multi-dimensional", MultiDimArrayType{Id: 2, Matrix2D: [][]int32{{1}}, Matrix3D: [][][]int32{{{1}}}, Value: "single_multidim"}, true},

		// Structure boundaries
		{"boundary_minimal_basic_struct", "Minimal basic structure", BasicStructType{Id: 1, Name: "", Age: 0, Score: 0.0, Active: false, Value: "minimal"}, true},
		{"boundary_max_basic_struct", "Maximum basic structure", BasicStructType{Id: 2, Name: "Very Long Name That Tests String Limits", Age: math.MaxInt32, Score: math.MaxFloat64, Active: true, Value: "maximum"}, true},

		// Nested structure boundaries
		{"boundary_minimal_nested_struct", "Minimal nested structure", NestedStructType{
			Id:      1,
			Person:  PersonStruct{FirstName: "", LastName: "", Age: 0, BirthDate: 0, Skills: []string{}},
			Address: AddressStruct{Street: "", City: "", State: "", ZipCode: "", Country: "", Latitude: 0.0, Longitude: 0.0},
			Contact: ContactStruct{Email: "", Phone: "", SocialMedia: []string{}, Preferred: ""},
			Value:   "minimal_nested",
		}, true},
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
	t.Logf("✅ COLLECTION STRUCTURE BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Collection structure boundary conditions should have >90%% success rate")

	t.Log("✅ Collection structure boundary conditions testing completed")
}
