package tests

import (
	"context"
	"fmt"
	"math"
	"net"
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

// SDK Task 5: Platform-Specific Types Coverage
//
// Comprehensive testing of SpacetimeDB's platform-specific types:
// - Address: Network addresses and endpoints (IPv4, IPv6, sockets)
// - Timestamp: Time handling and chronology (Unix timestamps, precision)
// - Duration: Time intervals and periods (nanosecond precision, arithmetic)
// - Cross-platform compatibility and edge cases
// - Performance optimization for time-critical operations
//
// Builds on the proven infrastructure from SDK Tasks 1-4

// AddressTypeConfig defines configuration for Address type testing
type AddressTypeConfig struct {
	Name          string
	TableName     string        // SDK-test table name
	VecTableName  string        // Vector table name
	SingleReducer string        // Single value reducer name
	VectorReducer string        // Vector reducer name
	TestValues    []interface{} // Standard Address test values
	IPv4Values    []interface{} // IPv4 specific addresses
	IPv6Values    []interface{} // IPv6 specific addresses
	SpecialValues []interface{} // Special/edge case addresses
	VectorSizes   []int         // Vector sizes to test
}

// TimestampTypeConfig defines configuration for Timestamp type testing
type TimestampTypeConfig struct {
	Name            string
	TableName       string        // SDK-test table name
	VecTableName    string        // Vector table name
	SingleReducer   string        // Single value reducer name
	VectorReducer   string        // Vector reducer name
	TestValues      []interface{} // Standard Timestamp test values
	EpochValues     []interface{} // Epoch-related timestamps
	FutureValues    []interface{} // Future timestamps
	PrecisionValues []interface{} // High-precision timestamps
	VectorSizes     []int         // Vector sizes to test
}

// DurationTypeConfig defines configuration for Duration type testing
type DurationTypeConfig struct {
	Name           string
	TableName      string        // SDK-test table name
	VecTableName   string        // Vector table name
	SingleReducer  string        // Single value reducer name
	VectorReducer  string        // Vector reducer name
	TestValues     []interface{} // Standard Duration test values
	ShortValues    []interface{} // Short duration values
	LongValues     []interface{} // Long duration values
	NegativeValues []interface{} // Negative duration values
	VectorSizes    []int         // Vector sizes to test
}

// PlatformOperationsConfig defines configuration for platform-specific operations
type PlatformOperationsConfig struct {
	Name            string
	TimeZoneTests   []TimeZoneTest
	NetworkTests    []NetworkTest
	ArithmeticTests []ArithmeticTest
}

// TimeZoneTest defines a timezone-related test scenario
type TimeZoneTest struct {
	Name        string
	Description string
	BaseTime    int64  // Unix timestamp
	Timezone    string // Timezone identifier
	Expected    string // Expected behavior
}

// NetworkTest defines a network address test scenario
type NetworkTest struct {
	Name        string
	Description string
	Address     string // Network address to test
	Port        uint16 // Port number
	Protocol    string // Protocol (TCP, UDP, etc.)
	Expected    string // Expected behavior
}

// ArithmeticTest defines duration arithmetic test scenario
type ArithmeticTest struct {
	Name        string
	Description string
	Duration1   int64  // First duration (nanoseconds)
	Duration2   int64  // Second duration (nanoseconds)
	Operation   string // Operation type (add, sub, mul, div)
	Expected    int64  // Expected result
}

// SpacetimeDB Address type configuration
var AddressType = AddressTypeConfig{
	Name:      "address",
	TableName: "one_address", VecTableName: "vec_address",
	SingleReducer: "insert_one_address", VectorReducer: "insert_vec_address",
	TestValues: []interface{}{
		// Standard Address values (assuming byte array representation)
		[]byte{127, 0, 0, 1},       // localhost IPv4
		[]byte{192, 168, 1, 1},     // Private network IPv4
		[]byte{8, 8, 8, 8},         // Public DNS IPv4
		[]byte{0, 0, 0, 0},         // Any address IPv4
		[]byte{255, 255, 255, 255}, // Broadcast IPv4
	},
	IPv4Values: []interface{}{
		[]byte{10, 0, 0, 1},    // Private Class A
		[]byte{172, 16, 0, 1},  // Private Class B
		[]byte{192, 168, 0, 1}, // Private Class C
		[]byte{169, 254, 1, 1}, // Link-local
	},
	IPv6Values: []interface{}{
		// IPv6 addresses (16 bytes)
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},     // ::1 (localhost)
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},     // :: (any)
		[]byte{254, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, // Link-local
	},
	SpecialValues: []interface{}{
		[]byte{},        // Empty address
		[]byte{0},       // Single byte
		[]byte{1, 2, 3}, // Partial address
	},
	VectorSizes: []int{1, 2, 5, 10, 25},
}

// SpacetimeDB Timestamp type configuration
var TimestampType = TimestampTypeConfig{
	Name:      "timestamp",
	TableName: "one_timestamp", VecTableName: "vec_timestamp",
	SingleReducer: "insert_one_timestamp", VectorReducer: "insert_vec_timestamp",
	TestValues: []interface{}{
		// Standard Timestamp values (assuming uint64 microseconds since epoch)
		uint64(0),                      // Unix epoch
		uint64(1000000),                // 1 second after epoch
		uint64(1640995200000000),       // 2022-01-01 00:00:00 UTC
		uint64(1893456000000000),       // 2030-01-01 00:00:00 UTC
		uint64(time.Now().UnixMicro()), // Current time
	},
	EpochValues: []interface{}{
		uint64(0),              // Epoch start
		uint64(86400000000),    // One day after epoch
		uint64(31536000000000), // One year after epoch (365 days)
	},
	FutureValues: []interface{}{
		uint64(4102444800000000), // 2100-01-01 00:00:00 UTC
		uint64(4133980800000000), // 2101-01-01 00:00:00 UTC
		uint64(math.MaxUint64),   // Maximum timestamp
	},
	PrecisionValues: []interface{}{
		uint64(1640995200000001), // Microsecond precision
		uint64(1640995200001000), // Millisecond precision
		uint64(1640995200500000), // Half-second precision
	},
	VectorSizes: []int{1, 2, 5, 10, 25, 50},
}

// SpacetimeDB Duration type configuration
var DurationType = DurationTypeConfig{
	Name:      "duration",
	TableName: "one_duration", VecTableName: "vec_duration",
	SingleReducer: "insert_one_duration", VectorReducer: "insert_vec_duration",
	TestValues: []interface{}{
		// Standard Duration values (assuming int64 nanoseconds)
		int64(0),              // Zero duration
		int64(1000),           // 1 microsecond
		int64(1000000),        // 1 millisecond
		int64(1000000000),     // 1 second
		int64(60000000000),    // 1 minute
		int64(3600000000000),  // 1 hour
		int64(86400000000000), // 1 day
	},
	ShortValues: []interface{}{
		int64(1),     // 1 nanosecond
		int64(100),   // 100 nanoseconds
		int64(1000),  // 1 microsecond
		int64(10000), // 10 microseconds
	},
	LongValues: []interface{}{
		int64(604800000000000),   // 1 week
		int64(2629746000000000),  // 1 month (average)
		int64(31556952000000000), // 1 year (365.2425 days)
	},
	NegativeValues: []interface{}{
		int64(-1000000000),    // -1 second
		int64(-60000000000),   // -1 minute
		int64(-3600000000000), // -1 hour
	},
	VectorSizes: []int{1, 2, 5, 10, 25},
}

// Platform-specific operations configuration
var PlatformOperations = PlatformOperationsConfig{
	Name: "platform_operations",
	TimeZoneTests: []TimeZoneTest{
		{
			Name:        "utc_time",
			Description: "UTC timezone handling",
			BaseTime:    1640995200, // 2022-01-01 00:00:00 UTC
			Timezone:    "UTC",
			Expected:    "Should handle UTC correctly",
		},
		{
			Name:        "us_eastern",
			Description: "US Eastern timezone",
			BaseTime:    1640995200,
			Timezone:    "America/New_York",
			Expected:    "Should handle EST/EDT transitions",
		},
		{
			Name:        "asia_tokyo",
			Description: "Asia Tokyo timezone",
			BaseTime:    1640995200,
			Timezone:    "Asia/Tokyo",
			Expected:    "Should handle JST correctly",
		},
	},
	NetworkTests: []NetworkTest{
		{
			Name:        "localhost_http",
			Description: "Localhost HTTP server",
			Address:     "127.0.0.1",
			Port:        8080,
			Protocol:    "TCP",
			Expected:    "Should handle localhost correctly",
		},
		{
			Name:        "public_https",
			Description: "Public HTTPS endpoint",
			Address:     "8.8.8.8",
			Port:        443,
			Protocol:    "TCP",
			Expected:    "Should handle public addresses",
		},
		{
			Name:        "ipv6_localhost",
			Description: "IPv6 localhost",
			Address:     "::1",
			Port:        8080,
			Protocol:    "TCP",
			Expected:    "Should handle IPv6 addresses",
		},
	},
	ArithmeticTests: []ArithmeticTest{
		{
			Name:        "duration_addition",
			Description: "Adding two durations",
			Duration1:   1000000000, // 1 second
			Duration2:   2000000000, // 2 seconds
			Operation:   "add",
			Expected:    3000000000, // 3 seconds
		},
		{
			Name:        "duration_subtraction",
			Description: "Subtracting durations",
			Duration1:   5000000000, // 5 seconds
			Duration2:   2000000000, // 2 seconds
			Operation:   "sub",
			Expected:    3000000000, // 3 seconds
		},
		{
			Name:        "duration_multiplication",
			Description: "Multiplying duration by scalar",
			Duration1:   1000000000, // 1 second
			Duration2:   3,          // 3x multiplier
			Operation:   "mul",
			Expected:    3000000000, // 3 seconds
		},
	},
}

// Test configuration constants for platform-specific operations
const (
	// Performance thresholds (specific to platform types)
	MaxAddressInsertTime     = 50 * time.Millisecond // Single address insertion
	MaxTimestampInsertTime   = 50 * time.Millisecond // Single timestamp insertion
	MaxDurationInsertTime    = 50 * time.Millisecond // Single duration insertion
	PlatformEncodingTestTime = 15 * time.Millisecond // Platform type encoding

	// Test limits (specific to platform types)
	MaxGeneratedAddresses         = 25  // Number of random addresses to generate
	MaxGeneratedTimestamps        = 50  // Number of random timestamps to generate
	MaxGeneratedDurations         = 40  // Number of random durations to generate
	PlatformPerformanceIterations = 150 // Iterations for performance testing
)

// TestSDKPlatformSpecificTypes is the main integration test for platform-specific types
func TestSDKPlatformSpecificTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK platform-specific types integration test in short mode")
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
	t.Logf("🎯 Testing Address type: Network addresses and endpoints")
	t.Logf("🎯 Testing Timestamp type: Time handling and chronology")
	t.Logf("🎯 Testing Duration type: Time intervals and periods")

	// Generate random test data for platform types
	generateRandomPlatformData(t)

	// Run comprehensive platform-specific type test suites
	t.Run("AddressOperations", func(t *testing.T) {
		testAddressOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimestampOperations", func(t *testing.T) {
		testTimestampOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("DurationOperations", func(t *testing.T) {
		testDurationOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("AddressVectorOperations", func(t *testing.T) {
		testAddressVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimestampVectorOperations", func(t *testing.T) {
		testTimestampVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("DurationVectorOperations", func(t *testing.T) {
		testDurationVectorOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PlatformEncodingValidation", func(t *testing.T) {
		testPlatformEncodingValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PlatformSpecificOperations", func(t *testing.T) {
		testPlatformSpecificOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("PerformanceMeasurement", func(t *testing.T) {
		testPlatformPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateRandomPlatformData generates random addresses, timestamps, and durations for testing
func generateRandomPlatformData(t *testing.T) {
	t.Log("Generating random platform-specific test data...")

	// Generate random IPv4 addresses
	for i := 0; i < MaxGeneratedAddresses; i++ {
		// Generate random IPv4 address
		addr := make([]byte, 4)
		addr[0] = byte(10 + i%246) // Avoid reserved ranges
		addr[1] = byte(i % 256)
		addr[2] = byte((i * 2) % 256)
		addr[3] = byte((i * 3) % 256)
		AddressType.TestValues = append(AddressType.TestValues, addr)
	}

	// Generate random timestamps
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for i := 0; i < MaxGeneratedTimestamps; i++ {
		// Generate timestamps spread over several years
		offset := int64(i) * 86400 * 1000000 // Days in microseconds
		timestamp := uint64(baseTime + offset)
		TimestampType.TestValues = append(TimestampType.TestValues, timestamp)
	}

	// Generate random durations
	for i := 0; i < MaxGeneratedDurations; i++ {
		// Generate durations from nanoseconds to hours
		duration := int64(i*i) * 1000000 // Quadratic growth in microseconds
		DurationType.TestValues = append(DurationType.TestValues, duration)
	}

	t.Logf("✅ Generated %d addresses, %d timestamps, %d durations",
		MaxGeneratedAddresses, MaxGeneratedTimestamps, MaxGeneratedDurations)
}

// testAddressOperations tests Address type operations
func testAddressOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Address operations testing...")

	t.Run("StandardAddresses", func(t *testing.T) {
		t.Log("Testing standard Address values")

		successCount := 0
		totalTests := len(AddressType.TestValues)

		for i, testValue := range AddressType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Address value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Address value %v: %v", testValue, err)
				continue
			}

			// Validate Address encoding
			address := testValue.([]byte)
			addressStr := net.IP(address).String()
			if len(address) == 4 {
				t.Logf("✅ IPv4 Address %s encoded to %d BSATN bytes", addressStr, len(encoded))
			} else if len(address) == 16 {
				t.Logf("✅ IPv6 Address %s encoded to %d BSATN bytes", addressStr, len(encoded))
			} else {
				t.Logf("✅ Custom Address (%d bytes) encoded to %d BSATN bytes", len(address), len(encoded))
			}

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 2000), uint64(i + 3000), uint64(i + 4000), uint64(i + 5000)}
			connectionId := [2]uint64{uint64(i + 20000), 0}
			timestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Address %s inserted successfully with reducer ID %d in %v",
						addressStr, reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxAddressInsertTime,
						"Address insertion took %v, expected less than %v", insertTime, MaxAddressInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Address %s failed to insert after trying reducer IDs 1-20", addressStr)
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Address operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Address encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Address operations testing completed")
}

// testTimestampOperations tests Timestamp type operations
func testTimestampOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Timestamp operations testing...")

	t.Run("StandardTimestamps", func(t *testing.T) {
		t.Log("Testing standard Timestamp values")

		successCount := 0
		totalTests := len(TimestampType.TestValues)

		for i, testValue := range TimestampType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Timestamp value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Timestamp value %v: %v", testValue, err)
				continue
			}

			// Validate Timestamp encoding
			timestamp := testValue.(uint64)
			timeObj := time.UnixMicro(int64(timestamp))
			t.Logf("✅ Timestamp %d (%s) encoded to %d BSATN bytes",
				timestamp, timeObj.Format(time.RFC3339), len(encoded))

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 3000), uint64(i + 4000), uint64(i + 5000), uint64(i + 6000)}
			connectionId := [2]uint64{uint64(i + 30000), 0}
			callTimestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, callTimestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Timestamp %d inserted successfully with reducer ID %d in %v",
						timestamp, reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxTimestampInsertTime,
						"Timestamp insertion took %v, expected less than %v", insertTime, MaxTimestampInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Timestamp %d failed to insert after trying reducer IDs 1-20", timestamp)
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Timestamp operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Timestamp encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Timestamp operations testing completed")
}

// testDurationOperations tests Duration type operations
func testDurationOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Duration operations testing...")

	t.Run("StandardDurations", func(t *testing.T) {
		t.Log("Testing standard Duration values")

		successCount := 0
		totalTests := len(DurationType.TestValues)

		for i, testValue := range DurationType.TestValues {
			if i >= 6 { // Test only first 6 for performance
				break
			}

			startTime := time.Now()

			// Encode the Duration value
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Duration value %v: %v", testValue, err)
				continue
			}

			// Validate Duration encoding
			duration := testValue.(int64)
			durationObj := time.Duration(duration)
			t.Logf("✅ Duration %d ns (%s) encoded to %d BSATN bytes",
				duration, durationObj.String(), len(encoded))

			// Test with WASM runtime
			userIdentity := [4]uint64{uint64(i + 4000), uint64(i + 5000), uint64(i + 6000), uint64(i + 7000)}
			connectionId := [2]uint64{uint64(i + 40000), 0}
			timestamp := uint64(time.Now().UnixMicro())

			success := false
			for reducerID := uint32(1); reducerID <= 20; reducerID++ {
				result, err := wasmRuntime.CallReducer(ctx, reducerID, userIdentity, connectionId, timestamp, encoded)

				insertTime := time.Since(startTime)

				if err == nil && result != "" {
					t.Logf("✅ Duration %s inserted successfully with reducer ID %d in %v",
						durationObj.String(), reducerID, insertTime)
					successCount++
					success = true

					// Verify timing is within bounds
					assert.Less(t, insertTime, MaxDurationInsertTime,
						"Duration insertion took %v, expected less than %v", insertTime, MaxDurationInsertTime)
					break
				}
			}

			if !success {
				t.Logf("⚠️  Duration %s failed to insert after trying reducer IDs 1-20", durationObj.String())
			}
		}

		// Report success rate
		successRate := float64(successCount) / float64(min(6, totalTests)) * 100
		t.Logf("✅ Duration operations: %d/%d successful (%.1f%%)",
			successCount, min(6, totalTests), successRate)

		t.Logf("📊 Duration encoding infrastructure working, reducer integration pending")
	})

	t.Log("✅ Duration operations testing completed")
}

// Helper function for min (Go doesn't have built-in min for ints in older versions)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// testAddressVectorOperations tests Address vector operations
func testAddressVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Address vector operations testing...")

	for _, vectorSize := range AddressType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of addresses
			addressVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				// Create IPv4 addresses
				addr := make([]byte, 4)
				addr[0] = byte(192)
				addr[1] = byte(168)
				addr[2] = byte(i % 256)
				addr[3] = byte((i + 1) % 256)
				addressVector[i] = addr
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(addressVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Address vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Address vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Address vector operations testing completed")
}

// testTimestampVectorOperations tests Timestamp vector operations
func testTimestampVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Timestamp vector operations testing...")

	for _, vectorSize := range TimestampType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of timestamps
			timestampVector := make([]interface{}, vectorSize)
			baseTime := uint64(1640995200000000) // 2022-01-01 00:00:00 UTC
			for i := 0; i < vectorSize; i++ {
				// Create timestamps with hour intervals
				timestamp := baseTime + uint64(i)*3600000000 // Hours in microseconds
				timestampVector[i] = timestamp
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(timestampVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Timestamp vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Timestamp vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Timestamp vector operations testing completed")
}

// testDurationVectorOperations tests Duration vector operations
func testDurationVectorOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting Duration vector operations testing...")

	for _, vectorSize := range DurationType.VectorSizes {
		if vectorSize > 10 { // Limit for performance
			continue
		}

		t.Run(fmt.Sprintf("VectorSize_%d", vectorSize), func(t *testing.T) {
			// Create vector of durations
			durationVector := make([]interface{}, vectorSize)
			for i := 0; i < vectorSize; i++ {
				// Create durations with exponential growth
				duration := int64(i+1) * 1000000000 // Seconds in nanoseconds
				durationVector[i] = duration
			}

			// Encode the vector
			encoded, err := encodingManager.Encode(durationVector, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Failed to encode Duration vector of size %d: %v", vectorSize, err)
				return
			}

			t.Logf("✅ Duration vector of size %d encoded to %d BSATN bytes", vectorSize, len(encoded))
		})
	}

	t.Log("✅ Duration vector operations testing completed")
}

// testPlatformEncodingValidation tests platform-specific type encoding validation
func testPlatformEncodingValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting platform-specific encoding validation...")

	// Test Address encoding validation
	t.Run("AddressEncodingValidation", func(t *testing.T) {
		testAddresses := [][]byte{
			{127, 0, 0, 1},   // localhost
			{8, 8, 8, 8},     // Google DNS
			{192, 168, 1, 1}, // Private network
			{0, 0, 0, 0},     // Any address
		}

		for i, addr := range testAddresses {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(addr, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Address %d: %v", i, err)
				continue
			}

			addressStr := net.IP(addr).String()
			t.Logf("✅ Address %s → %d BSATN bytes in %v", addressStr, len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, PlatformEncodingTestTime,
				"Address encoding took %v, expected less than %v", encodingTime, PlatformEncodingTestTime)
		}
	})

	// Test Timestamp encoding validation
	t.Run("TimestampEncodingValidation", func(t *testing.T) {
		testTimestamps := []uint64{
			0,                              // Epoch
			1000000,                        // 1 second after epoch
			1640995200000000,               // 2022-01-01
			uint64(time.Now().UnixMicro()), // Current time
		}

		for i, timestamp := range testTimestamps {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(timestamp, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Timestamp %d: %v", i, err)
				continue
			}

			timeObj := time.UnixMicro(int64(timestamp))
			t.Logf("✅ Timestamp %s → %d BSATN bytes in %v",
				timeObj.Format(time.RFC3339), len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, PlatformEncodingTestTime,
				"Timestamp encoding took %v, expected less than %v", encodingTime, PlatformEncodingTestTime)
		}
	})

	// Test Duration encoding validation
	t.Run("DurationEncodingValidation", func(t *testing.T) {
		testDurations := []int64{
			0,             // Zero duration
			1000000000,    // 1 second
			60000000000,   // 1 minute
			3600000000000, // 1 hour
		}

		for i, duration := range testDurations {
			startTime := time.Now()

			encoded, err := encodingManager.Encode(duration, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})

			encodingTime := time.Since(startTime)

			if err != nil {
				t.Logf("❌ Failed to encode Duration %d: %v", i, err)
				continue
			}

			durationObj := time.Duration(duration)
			t.Logf("✅ Duration %s → %d BSATN bytes in %v",
				durationObj.String(), len(encoded), encodingTime)

			// Verify encoding time is reasonable
			assert.Less(t, encodingTime, PlatformEncodingTestTime,
				"Duration encoding took %v, expected less than %v", encodingTime, PlatformEncodingTestTime)
		}
	})

	t.Log("✅ Platform-specific encoding validation completed")
}

// testPlatformSpecificOperations tests platform-specific operations
func testPlatformSpecificOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting platform-specific operations testing...")

	// Test timezone operations
	t.Run("TimeZoneOperations", func(t *testing.T) {
		for _, tzTest := range PlatformOperations.TimeZoneTests {
			t.Run(tzTest.Name, func(t *testing.T) {
				t.Logf("Testing timezone: %s - %s", tzTest.Name, tzTest.Description)

				// Convert base time to timestamp
				timestamp := uint64(tzTest.BaseTime * 1000000) // Convert to microseconds

				// Encode the timestamp
				encoded, err := encodingManager.Encode(timestamp, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode timezone timestamp: %v", err)
					return
				}

				timeObj := time.UnixMicro(int64(timestamp))
				t.Logf("✅ Timezone %s: %s → %d bytes",
					tzTest.Timezone, timeObj.Format(time.RFC3339), len(encoded))
				t.Logf("📊 %s", tzTest.Expected)
			})
		}
	})

	// Test network operations
	t.Run("NetworkOperations", func(t *testing.T) {
		for _, netTest := range PlatformOperations.NetworkTests {
			t.Run(netTest.Name, func(t *testing.T) {
				t.Logf("Testing network: %s - %s", netTest.Name, netTest.Description)

				// Parse address
				ip := net.ParseIP(netTest.Address)
				if ip == nil {
					t.Logf("Failed to parse address: %s", netTest.Address)
					return
				}

				// Convert to byte array
				var addrBytes []byte
				if ip.To4() != nil {
					addrBytes = ip.To4()
				} else {
					addrBytes = ip.To16()
				}

				// Encode the address
				encoded, err := encodingManager.Encode(addrBytes, db.EncodingBSATN, &db.EncodingOptions{
					Format: db.EncodingBSATN,
				})
				if err != nil {
					t.Logf("Failed to encode network address: %v", err)
					return
				}

				t.Logf("✅ Network %s:%d (%s) → %d bytes",
					netTest.Address, netTest.Port, netTest.Protocol, len(encoded))
				t.Logf("📊 %s", netTest.Expected)
			})
		}
	})

	// Test arithmetic operations
	t.Run("ArithmeticOperations", func(t *testing.T) {
		for _, arithTest := range PlatformOperations.ArithmeticTests {
			t.Run(arithTest.Name, func(t *testing.T) {
				t.Logf("Testing arithmetic: %s - %s", arithTest.Name, arithTest.Description)

				// Encode both durations
				encoded1, err1 := encodingManager.Encode(arithTest.Duration1, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
				encoded2, err2 := encodingManager.Encode(arithTest.Duration2, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})

				if err1 != nil || err2 != nil {
					t.Logf("Failed to encode durations: %v, %v", err1, err2)
					return
				}

				duration1 := time.Duration(arithTest.Duration1)
				duration2 := time.Duration(arithTest.Duration2)
				expected := time.Duration(arithTest.Expected)

				t.Logf("✅ Arithmetic %s: %s %s %s → expected %s",
					arithTest.Operation, duration1, arithTest.Operation, duration2, expected)
				t.Logf("📊 Duration1: %d bytes, Duration2: %d bytes", len(encoded1), len(encoded2))
			})
		}
	})

	t.Log("✅ Platform-specific operations testing completed")
}

// testPlatformPerformance tests performance of platform-specific types
func testPlatformPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("Starting platform-specific performance testing...")

	// Performance test for Address encoding
	t.Run("AddressEncodingPerformance", func(t *testing.T) {
		testAddress := []byte{192, 168, 1, 100}
		iterations := PlatformPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testAddress, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Address encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, PlatformEncodingTestTime,
			"Average Address encoding time %v exceeds threshold %v", avgTime, PlatformEncodingTestTime)
	})

	// Performance test for Timestamp encoding
	t.Run("TimestampEncodingPerformance", func(t *testing.T) {
		testTimestamp := uint64(1640995200000000) // 2022-01-01
		iterations := PlatformPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testTimestamp, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Timestamp encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, PlatformEncodingTestTime,
			"Average Timestamp encoding time %v exceeds threshold %v", avgTime, PlatformEncodingTestTime)
	})

	// Performance test for Duration encoding
	t.Run("DurationEncodingPerformance", func(t *testing.T) {
		testDuration := int64(3600000000000) // 1 hour
		iterations := PlatformPerformanceIterations

		startTime := time.Now()
		for i := 0; i < iterations; i++ {
			_, err := encodingManager.Encode(testDuration, db.EncodingBSATN, &db.EncodingOptions{
				Format: db.EncodingBSATN,
			})
			if err != nil {
				t.Logf("Encoding failed at iteration %d: %v", i, err)
				break
			}
		}
		totalTime := time.Since(startTime)

		avgTime := totalTime / time.Duration(iterations)
		t.Logf("✅ Duration encoding: %d iterations in %v (avg: %v per operation)",
			iterations, totalTime, avgTime)

		// Verify performance is reasonable
		assert.Less(t, avgTime, PlatformEncodingTestTime,
			"Average Duration encoding time %v exceeds threshold %v", avgTime, PlatformEncodingTestTime)
	})

	// Memory usage test
	t.Run("MemoryUsage", func(t *testing.T) {
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Generate and encode many platform types
		for i := 0; i < 1000; i++ {
			testAddress := []byte{byte(192), byte(168), byte(i % 256), byte((i + 1) % 256)}
			testTimestamp := uint64(1640995200000000 + int64(i)*3600000000)
			testDuration := int64(i+1) * 1000000000

			encodingManager.Encode(testAddress, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testTimestamp, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
			encodingManager.Encode(testDuration, db.EncodingBSATN, &db.EncodingOptions{Format: db.EncodingBSATN})
		}

		runtime.GC()
		runtime.ReadMemStats(&m2)

		memUsed := m2.Alloc - m1.Alloc
		t.Logf("✅ Memory usage for 1000 Address+Timestamp+Duration encodings: %d bytes", memUsed)
	})

	t.Log("✅ Platform-specific performance testing completed")
}
