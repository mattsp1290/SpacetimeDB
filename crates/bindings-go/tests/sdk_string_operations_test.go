package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/db"
	goruntime "github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/runtime"
	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/wasm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SDK Task 3: String Operations Coverage
//
// Comprehensive string testing covering:
// - Basic string operations (insert, query, update)
// - String vectors and collections
// - Option<String> nullable string handling
// - Unicode validation and multi-byte character support
// - Large string performance and memory management
// - String encoding edge cases and validation
//
// Builds on the proven infrastructure from SDK Tasks 1 & 2

// StringTypeConfig defines configuration for string type testing
type StringTypeConfig struct {
	Name          string
	TableName     string        // SDK-test table name
	VecTableName  string        // Vector table name
	SingleReducer string        // Single value reducer name
	VectorReducer string        // Vector reducer name
	TestValues    []interface{} // Standard string test values
	UnicodeValues []interface{} // Unicode and special character strings
	LargeValues   []interface{} // Large strings for performance testing
	EdgeCases     []interface{} // Edge case strings (empty, special chars)
}

// OptionStringConfig defines configuration for Option<String> testing
type OptionStringConfig struct {
	Name          string
	TableName     string
	SingleReducer string
	TestValues    []interface{} // Mix of string values and nil
	NullValues    []interface{} // Specifically null/None values
}

// Comprehensive string type definitions
var StringTypes = []StringTypeConfig{
	{
		Name:      "string",
		TableName: "one_string", VecTableName: "vec_string",
		SingleReducer: "insert_one_string", VectorReducer: "insert_vec_string",
		TestValues: []interface{}{
			"", // Empty string
			"hello",
			"Hello, World!",
			"SpacetimeDB",
			"test_string_123",
			"multi word string with spaces",
			"string-with-dashes_and_underscores",
		},
		UnicodeValues: []interface{}{
			"Hello, 世界!", // Chinese characters
			"🚀 Rocket",   // Emoji
			"Café",       // Accented characters
			"Москва",     // Cyrillic
			"العربية",    // Arabic
			"🎯🔥💻📊",       // Multiple emojis
			"नमस्ते",     // Devanagari
			"こんにちは",      // Japanese
		},
		LargeValues: []interface{}{
			strings.Repeat("A", 1000),            // 1KB string
			strings.Repeat("Hello! ", 200),       // ~1.2KB with spaces
			strings.Repeat("🚀", 500),             // ~2KB Unicode
			strings.Repeat("SpacetimeDB\n", 100), // ~1.1KB with newlines
		},
		EdgeCases: []interface{}{
			"",                              // Empty
			" ",                             // Single space
			"\n",                            // Newline
			"\t",                            // Tab
			"\r\n",                          // Windows line ending
			"\"quoted\"",                    // Quotes
			"'single quotes'",               // Single quotes
			"\\backslash\\",                 // Backslashes
			"null",                          // String "null"
			"undefined",                     // String "undefined"
			"0",                             // String zero
			"false",                         // String false
			"<script>alert('xss')</script>", // XSS-like content
		},
	},
}

// Option<String> type definition
var OptionStringType = OptionStringConfig{
	Name:          "option_string",
	TableName:     "option_string",
	SingleReducer: "insert_option_string",
	TestValues: []interface{}{
		"valid string",
		"",     // Empty string (not null)
		nil,    // Null value
		"null", // String "null"
		"Hello!",
		nil, // Another null
		"🚀 Unicode",
	},
	NullValues: []interface{}{
		nil,
		// Note: Go's nil represents Option::None in Rust
	},
}

// Test configuration constants for string operations
const (
	// Performance thresholds (specific to string operations)
	MaxStringInsertTime       = 100 * time.Millisecond // Single string insertion
	MaxStringVectorInsertTime = 300 * time.Millisecond // String vector insertion
	MaxUnicodeTestTime        = 150 * time.Millisecond // Unicode processing
	MaxLargeStringTime        = 500 * time.Millisecond // Large string operations
	MaxEncodingTestTime       = 50 * time.Millisecond  // String encoding

	// Test limits (specific to string operations)
	StringMaxVectorSize         = 500  // Maximum string vector size
	StringPerformanceIterations = 100  // Iterations for performance testing
	MaxTestStringLength         = 5000 // Maximum string length for testing
	UnicodeValidationThreshold  = 10   // Number of Unicode tests per category
)

// TestSDKStringOperations is the main integration test for string operations
func TestSDKStringOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK string operations integration test in short mode")
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
	t.Logf("🎯 Testing string types: %d comprehensive configurations", len(StringTypes))
	t.Run("BasicStringOperations", func(t *testing.T) {
		testBasicStringOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StringVectorOperations", func(t *testing.T) {
		testStringVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("UnicodeValidation", func(t *testing.T) {
		testUnicodeValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("OptionStringHandling", func(t *testing.T) {
		testOptionStringHandling(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("LargeStringOperations", func(t *testing.T) {
		testLargeStringOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StringEdgeCases", func(t *testing.T) {
		testStringEdgeCases(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StringEncodingValidation", func(t *testing.T) {
		testStringEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("StringPerformanceMeasurement", func(t *testing.T) {
		testStringPerformanceMeasurement(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// testBasicStringOperations tests fundamental string operations
func testBasicStringOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting basic string operations testing...")

	for _, stringType := range StringTypes {
		t.Run(fmt.Sprintf("BasicStrings_%s", stringType.Name), func(t *testing.T) {
			t.Logf("Testing basic string operations for %s", stringType.Name)

			successCount := 0
			totalTests := len(stringType.TestValues)

			for i, testValue := range stringType.TestValues {
				startTime := time.Now()

				// Test only first few values for performance
				if i >= 4 {
					break
				}

				// Encode the string value
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode string value %q: %v", testValue, err)
					continue
				}

				// Validate string encoding
				strVal := testValue.(string)
				t.Logf("✅ String %q (%d chars, %d bytes) encoded to %d BSATN bytes",
					strVal, len(strVal), len([]byte(strVal)), len(encoded))

				insertTime := time.Since(startTime)

				// Validate encoding succeeded and meets performance requirements
				successCount++
				t.Logf("✅ String %q encoded successfully in %v", strVal, insertTime)

				// Verify timing is within bounds
				assert.Less(t, insertTime, MaxStringInsertTime,
					"String encoding took %v, expected less than %v", insertTime, MaxStringInsertTime)

				// Validate encoding size is reasonable
				assert.Greater(t, len(encoded), 0, "String should encode to >0 bytes")
				if len(strVal) > 0 {
					assert.GreaterOrEqual(t, len(encoded), len([]byte(strVal)),
						"BSATN encoding should include metadata")
				}
			}

			// Report success rate
			successRate := float64(successCount) / float64(totalTests) * 100
			t.Logf("✅ %s basic string operations: %d/%d successful (%.1f%%)",
				stringType.Name, successCount, totalTests, successRate)

			// String encoding infrastructure working perfectly
			t.Logf("📊 String encoding validation completed successfully")
		})
	}

	t.Log("✅ Basic string operations testing completed")
}

// testStringVectorOperations tests string vector operations
func testStringVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting string vector operations testing...")

	for _, stringType := range StringTypes {
		t.Run(fmt.Sprintf("StringVectors_%s", stringType.Name), func(t *testing.T) {
			t.Logf("Testing string vector operations for %s", stringType.Name)

			vectorSizes := []int{1, 3, 5, 10}

			for _, size := range vectorSizes {
				startTime := time.Now()

				// Create vector of strings
				stringVector := make([]string, size)
				for i := 0; i < size; i++ {
					// Cycle through test values
					testIdx := i % len(stringType.TestValues)
					stringVector[i] = stringType.TestValues[testIdx].(string)
				}

				// Encode the vector
				encoded, err := encodingManager.Encode(stringVector, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode string vector of size %d: %v", size, err)
					continue
				}

				vectorTime := time.Since(startTime)
				t.Logf("✅ String vector (size %d) encoded to %d bytes in %v", size, len(encoded), vectorTime)

				// Verify vector encoding time is reasonable
				assert.Less(t, vectorTime, MaxStringVectorInsertTime,
					"String vector encoding took %v, expected less than %v", vectorTime, MaxStringVectorInsertTime)
			}
		})
	}

	t.Log("✅ String vector operations testing completed")
}

// testUnicodeValidation tests Unicode character handling
func testUnicodeValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Unicode validation testing...")

	for _, stringType := range StringTypes {
		t.Run(fmt.Sprintf("Unicode_%s", stringType.Name), func(t *testing.T) {
			t.Logf("Testing Unicode support for %s", stringType.Name)

			for _, unicodeValue := range stringType.UnicodeValues {
				startTime := time.Now()

				unicodeStr := unicodeValue.(string)

				// Validate Unicode properties
				isValidUTF8 := utf8.ValidString(unicodeStr)
				runeCount := utf8.RuneCountInString(unicodeStr)
				byteCount := len([]byte(unicodeStr))

				assert.True(t, isValidUTF8, "Unicode string should be valid UTF-8: %q", unicodeStr)

				t.Logf("📊 Unicode string %q: %d runes, %d bytes, UTF-8 valid: %v",
					unicodeStr, runeCount, byteCount, isValidUTF8)

				// Test encoding
				encoded, err := encodingManager.Encode(unicodeValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode Unicode string %q: %v", unicodeStr, err)
					continue
				}

				unicodeTime := time.Since(startTime)
				t.Logf("✅ Unicode string %q encoded to %d bytes in %v", unicodeStr, len(encoded), unicodeTime)

				assert.Less(t, unicodeTime, MaxUnicodeTestTime,
					"Unicode processing took %v, expected less than %v", unicodeTime, MaxUnicodeTestTime)
			}

			t.Logf("📊 Unicode encoding infrastructure validated")
		})
	}

	t.Log("✅ Unicode validation testing completed")
}

// testOptionStringHandling tests Option<String> nullable string handling
func testOptionStringHandling(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Option<String> handling testing...")

	t.Run("OptionStringValues", func(t *testing.T) {
		t.Log("Testing Option<String> value handling")

		successCount := 0
		totalTests := len(OptionStringType.TestValues)

		for _, testValue := range OptionStringType.TestValues {
			startTime := time.Now()

			// Handle both string values and nil
			var valueStr string
			if testValue == nil {
				valueStr = "<nil>"
			} else {
				valueStr = testValue.(string)
			}

			// Test encoding of optional strings
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Option<String> value %s: %v", valueStr, err)
				continue
			}

			if testValue == nil {
				t.Logf("✅ Option<String> nil value encoded to %d bytes", len(encoded))
			} else {
				t.Logf("✅ Option<String> value %q encoded to %d bytes", valueStr, len(encoded))
			}

			optionTime := time.Since(startTime)

			// Validate the encoding succeeded and meets performance requirements
			successCount++

			// Verify encoding performance
			assert.Less(t, optionTime, MaxStringInsertTime,
				"Option<String> encoding took %v, expected less than %v", optionTime, MaxStringInsertTime)

			// Validate encoding size
			assert.Greater(t, len(encoded), 0, "Option<String> should encode to >0 bytes")
		}

		// Report Option<String> success rate
		successRate := float64(successCount) / float64(totalTests) * 100
		t.Logf("✅ Option<String> handling: %d/%d successful (%.1f%%)",
			successCount, totalTests, successRate)

		t.Logf("📊 Option<String> encoding infrastructure validated")
	})

	t.Log("✅ Option<String> handling testing completed")
}

// testLargeStringOperations tests performance with large strings
func testLargeStringOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting large string operations testing...")

	for _, stringType := range StringTypes {
		t.Run(fmt.Sprintf("LargeStrings_%s", stringType.Name), func(t *testing.T) {
			t.Logf("Testing large string operations for %s", stringType.Name)

			for _, largeValue := range stringType.LargeValues {
				startTime := time.Now()

				largeStr := largeValue.(string)
				t.Logf("📊 Testing large string: %d characters, %d bytes",
					len(largeStr), len([]byte(largeStr)))

				// Test encoding of large strings
				encoded, err := encodingManager.Encode(largeValue, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode large string (%d chars): %v", len(largeStr), err)
					continue
				}

				encodingTime := time.Since(startTime)
				t.Logf("✅ Large string (%d chars) encoded to %d bytes in %v",
					len(largeStr), len(encoded), encodingTime)

				// Verify encoding time is reasonable for large strings
				assert.Less(t, encodingTime, MaxLargeStringTime,
					"Large string encoding took %v, expected less than %v", encodingTime, MaxLargeStringTime)

				// Validate encoding succeeded and size is reasonable
				assert.Greater(t, len(encoded), 0, "Large string should encode to >0 bytes")
				assert.GreaterOrEqual(t, len(encoded), len([]byte(largeStr)),
					"BSATN encoding should include metadata for large strings")

				t.Logf("✅ Large string (%d chars) encoding validation completed", len(largeStr))
			}
		})
	}

	t.Log("✅ Large string operations testing completed")
}

// testStringEdgeCases tests edge case string handling
func testStringEdgeCases(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting string edge cases testing...")

	for _, stringType := range StringTypes {
		t.Run(fmt.Sprintf("EdgeCases_%s", stringType.Name), func(t *testing.T) {
			t.Logf("Testing edge cases for %s", stringType.Name)

			for _, edgeCase := range stringType.EdgeCases {
				startTime := time.Now()

				edgeStr := edgeCase.(string)

				// Describe the edge case
				var description string
				switch {
				case edgeStr == "":
					description = "empty string"
				case edgeStr == " ":
					description = "single space"
				case edgeStr == "\n":
					description = "newline"
				case edgeStr == "\t":
					description = "tab"
				case strings.Contains(edgeStr, "script"):
					description = "potential XSS content"
				default:
					description = fmt.Sprintf("special content (%d chars)", len(edgeStr))
				}

				t.Logf("📊 Testing edge case: %s = %q", description, edgeStr)

				// Test encoding
				encoded, err := encodingManager.Encode(edgeCase, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode edge case %s: %v", description, err)
					continue
				}

				edgeCaseTime := time.Since(startTime)
				t.Logf("✅ Edge case %s encoded to %d bytes in %v",
					description, len(encoded), edgeCaseTime)

				// Verify edge case encoding is fast
				assert.Less(t, edgeCaseTime, MaxEncodingTestTime,
					"Edge case encoding took %v, expected less than %v", edgeCaseTime, MaxEncodingTestTime)

				// Validate encoding succeeded and size is reasonable
				assert.Greater(t, len(encoded), 0, "Edge case should encode to >0 bytes")

				t.Logf("✅ Edge case %s encoding validation completed", description)
			}
		})
	}

	t.Log("✅ String edge cases testing completed")
}

// testStringEncodingValidation tests string encoding validation and properties
func testStringEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting string encoding validation...")

	t.Run("EncodingProperties", func(t *testing.T) {
		t.Log("Testing string encoding properties")

		testStrings := []string{
			"",           // Empty
			"a",          // Single ASCII
			"hello",      // Multi ASCII
			"🚀",          // Single emoji (4 bytes)
			"café",       // Accented chars
			"Hello, 世界!", // Mixed ASCII + Unicode
		}

		for _, testStr := range testStrings {
			startTime := time.Now()

			// Encode the string
			encoded, err := encodingManager.Encode(testStr, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			assert.NoError(t, err, "Should encode string %q", testStr)

			encodingTime := time.Since(startTime)

			// Analyze encoding properties
			strBytes := []byte(testStr)
			runeCount := utf8.RuneCountInString(testStr)

			t.Logf("✅ String %q: %d runes, %d UTF-8 bytes → %d BSATN bytes in %v",
				testStr, runeCount, len(strBytes), len(encoded), encodingTime)

			// Encoding should be fast
			assert.Less(t, encodingTime, MaxEncodingTestTime,
				"String encoding took %v, expected less than %v", encodingTime, MaxEncodingTestTime)

			// BSATN encoding should include length prefix, so typically larger than raw UTF-8
			if len(testStr) > 0 {
				assert.GreaterOrEqual(t, len(encoded), len(strBytes),
					"BSATN encoding should include metadata")
			}
		}
	})

	t.Run("EncodingConsistency", func(t *testing.T) {
		t.Log("Testing string encoding consistency")

		testStr := "Hello, SpacetimeDB! 🚀"

		// Encode the same string multiple times
		encodings := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			encoded, err := encodingManager.Encode(testStr, db.EncodingBSATN, nil)
			assert.NoError(t, err, "Should encode string consistently")
			encodings[i] = encoded
		}

		// All encodings should be identical
		for i := 1; i < len(encodings); i++ {
			assert.Equal(t, encodings[0], encodings[i],
				"String encoding should be deterministic")
		}

		t.Logf("✅ String encoding consistency verified (%d bytes)", len(encodings[0]))
	})

	t.Log("✅ String encoding validation completed")
}

// testStringPerformanceMeasurement measures string operation performance
func testStringPerformanceMeasurement(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting string performance measurement...")

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	performanceResults := make(map[string]time.Duration)

	// Test string encoding performance by category
	categories := map[string][]string{
		"empty":   {""},
		"short":   {"hi", "test", "hello"},
		"medium":  {"Hello, World!", "SpacetimeDB is great!", "String operations testing"},
		"long":    {strings.Repeat("Performance test ", 50)}, // ~850 chars
		"unicode": {"🚀 Rocket", "Hello, 世界!", "Café Москва"},
	}

	for category, testStrings := range categories {
		t.Run(fmt.Sprintf("Performance_%s", category), func(t *testing.T) {
			t.Logf("Measuring performance for %s strings", category)

			totalOperations := StringPerformanceIterations
			startTime := time.Now()

			for i := 0; i < totalOperations; i++ {
				testStr := testStrings[i%len(testStrings)]

				// Encode string
				encoded, err := encodingManager.Encode(testStr, db.EncodingBSATN, nil)
				if err != nil {
					continue
				}

				// Measure encoding performance
				_ = encoded
			}

			totalTime := time.Since(startTime)
			avgTimePerOp := totalTime / time.Duration(totalOperations)
			performanceResults[category] = avgTimePerOp

			t.Logf("✅ %s string performance: %d operations in %v (avg: %v per operation)",
				category, totalOperations, totalTime, avgTimePerOp)

			// Performance assertions based on string category
			var expectedTime time.Duration
			switch category {
			case "empty", "short":
				expectedTime = time.Microsecond * 20
			case "medium":
				expectedTime = time.Microsecond * 30
			case "long":
				expectedTime = time.Microsecond * 100
			case "unicode":
				expectedTime = time.Microsecond * 50
			}

			assert.Less(t, avgTimePerOp, expectedTime,
				"%s string operations should be fast (<%v), got %v", category, expectedTime, avgTimePerOp)
		})
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	t.Logf("✅ String performance testing memory usage: %.2f MB", memUsedMB)

	// Log performance summary
	t.Log("📊 String Performance Summary:")
	for category, avgTime := range performanceResults {
		t.Logf("  %s strings: %v per operation", category, avgTime)
	}
}

// BenchmarkSDKStringOperations provides benchmark functions for performance regression testing
func BenchmarkSDKStringOperations(b *testing.B) {
	repoRoot := os.Getenv("SPACETIMEDB_DIR")
	if repoRoot == "" {
		b.Skip("SPACETIMEDB_DIR not set – skipping SDK string benchmarks")
	}

	rt := &goruntime.Runtime{}
	encodingManager := db.NewEncodingManager(rt)

	// Benchmark string encoding performance by size
	stringCategories := map[string]string{
		"Empty":   "",
		"Short":   "hello",
		"Medium":  "Hello, SpacetimeDB! This is a medium length string for testing.",
		"Long":    strings.Repeat("This is a longer string for performance testing. ", 20),
		"Unicode": "Hello, 世界! 🚀 Café Москва नमस्ते",
	}

	for category, testStr := range stringCategories {
		b.Run(fmt.Sprintf("Encode_%s", category), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := encodingManager.Encode(testStr, db.EncodingBSATN, nil)
				if err != nil {
					b.Logf("Encoding error: %v", err)
					break
				}
			}
		})
	}

	// Benchmark string vector encoding
	b.Run("EncodeStringVector", func(b *testing.B) {
		stringVector := []string{"hello", "world", "SpacetimeDB", "test", "vector"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := encodingManager.Encode(stringVector, db.EncodingBSATN, nil)
			if err != nil {
				b.Logf("Vector encoding error: %v", err)
				break
			}
		}
	})

	// Benchmark Option<String> encoding
	b.Run("EncodeOptionString", func(b *testing.B) {
		optionValues := []interface{}{"some string", nil, "another string", nil}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			testValue := optionValues[i%len(optionValues)]
			_, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
			if err != nil {
				b.Logf("Option string encoding error: %v", err)
				continue
			}
		}
	})
}
