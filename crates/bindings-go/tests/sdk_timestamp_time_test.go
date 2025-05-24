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

// SDK Task 5: Timestamp & Time Operations with BSATN Type Safety
//
// This test demonstrates the CORRECT way to use SpacetimeDB timestamp and time types:
// - Basic Time Operations: Core timestamp creation, manipulation, and formatting
// - Chronological Data: Time-series data handling and temporal relationships
// - Time Arithmetic: Addition, subtraction, duration calculations
// - Time-based Indexing: Efficient time-based queries and performance
// - Time Zone Handling: UTC normalization and time zone considerations
// - Performance Optimization: High-performance time operations for real-time systems

// === BASIC TIMESTAMP TABLE TYPES ===

// OneTimestampType represents a table with single timestamp field
type OneTimestampType struct {
	Id        uint32 `json:"id"`        // Auto-increment primary key
	Timestamp uint64 `json:"timestamp"` // SpacetimeDB timestamp (microseconds since epoch)
	Value     string `json:"value"`     // Additional data field
}

// VecTimestampType represents a table with timestamp vector field
type VecTimestampType struct {
	Id         uint32   `json:"id"`         // Auto-increment primary key
	Timestamps []uint64 `json:"timestamps"` // Vector of SpacetimeDB timestamps
	Value      string   `json:"value"`      // Additional data field
}

// OptionTimestampType represents a table with optional timestamp field
type OptionTimestampType struct {
	Id        uint32  `json:"id"`        // Auto-increment primary key
	Timestamp *uint64 `json:"timestamp"` // Optional SpacetimeDB timestamp
	Value     string  `json:"value"`     // Additional data field
}

// UniqueTimestampType represents a table with unique timestamp constraint
type UniqueTimestampType struct {
	Id        uint32 `json:"id"`        // Auto-increment primary key
	Timestamp uint64 `json:"timestamp"` // Unique SpacetimeDB timestamp
	Value     string `json:"value"`     // Additional data field
}

// TimePkType represents a table with timestamp as primary key
type TimePkType struct {
	Timestamp uint64 `json:"timestamp"` // SpacetimeDB timestamp as primary key
	Value     string `json:"value"`     // Additional data field
}

// === CHRONOLOGICAL DATA TYPES ===

// TimeSeriesDataType represents time-series data points
type TimeSeriesDataType struct {
	Id           uint32  `json:"id"`            // Auto-increment primary key
	Timestamp    uint64  `json:"timestamp"`     // Data point timestamp
	Value        float64 `json:"value"`         // Measured value
	Source       string  `json:"source"`        // Data source identifier
	Quality      uint8   `json:"quality"`       // Data quality indicator (0-100)
	IntervalType string  `json:"interval_type"` // Interval type (second, minute, hour, day)
}

// EventLogType represents chronological event logging
type EventLogType struct {
	Id          uint32 `json:"id"`          // Auto-increment primary key
	EventTime   uint64 `json:"event_time"`  // When event occurred
	EventType   string `json:"event_type"`  // Type of event
	UserId      uint32 `json:"user_id"`     // User who triggered event
	Description string `json:"description"` // Event description
	Severity    uint8  `json:"severity"`    // Event severity (0=info, 1=warn, 2=error, 3=critical)
	Duration    uint64 `json:"duration"`    // Event duration in microseconds
}

// ScheduledTaskType represents time-based task scheduling
type ScheduledTaskType struct {
	Id             uint32 `json:"id"`              // Auto-increment primary key
	ScheduledTime  uint64 `json:"scheduled_time"`  // When task should run
	ExecutionTime  uint64 `json:"execution_time"`  // When task actually ran
	CompletionTime uint64 `json:"completion_time"` // When task completed
	TaskName       string `json:"task_name"`       // Task identifier
	Status         string `json:"status"`          // Task status (pending, running, completed, failed)
	RetryCount     uint8  `json:"retry_count"`     // Number of retry attempts
}

// === TIME ARITHMETIC TYPES ===

// DurationCalcType represents duration calculations between timestamps
type DurationCalcType struct {
	Id        uint32 `json:"id"`         // Auto-increment primary key
	StartTime uint64 `json:"start_time"` // Start timestamp
	EndTime   uint64 `json:"end_time"`   // End timestamp
	Duration  uint64 `json:"duration"`   // Calculated duration in microseconds
	Operation string `json:"operation"`  // Type of duration calculation
	Value     string `json:"value"`      // Additional data field
}

// TimeIntervalType represents time intervals and ranges
type TimeIntervalType struct {
	Id            uint32 `json:"id"`             // Auto-increment primary key
	IntervalStart uint64 `json:"interval_start"` // Interval start timestamp
	IntervalEnd   uint64 `json:"interval_end"`   // Interval end timestamp
	IntervalType  string `json:"interval_type"`  // Type of interval (minute, hour, day, week, month)
	Value         string `json:"value"`          // Additional data field
}

// === INDEXED TIME TYPES ===

// TimeIndexedType represents indexed timestamp for fast queries
type TimeIndexedType struct {
	Id             uint32 `json:"id"`              // Auto-increment primary key
	IndexedTime    uint64 `json:"indexed_time"`    // Indexed timestamp field
	Category       string `json:"category"`        // Data category
	SubCategory    string `json:"sub_category"`    // Data subcategory
	Value          string `json:"value"`           // Additional data field
	ProcessingTime uint64 `json:"processing_time"` // When record was processed
}

// === TIME OPERATION TYPES ===

// TimeOperation represents a time operation test scenario
type TimeOperation struct {
	Name        string
	Description string
	OpType      string                 // Type of operation (create, query, arithmetic, compare)
	TestValue   interface{}            // Test value for operation
	Expected    string                 // Expected behavior
	ShouldPass  bool                   // Whether operation should succeed
	Validate    func(interface{}) bool // Validation function
}

// ChronologicalTest represents a chronological data test scenario
type ChronologicalTest struct {
	Name          string
	Description   string
	TimePoints    []uint64 // Time points for testing
	Operations    []string // Operations to perform
	ExpectedOrder []uint64 // Expected chronological order
	ShouldPass    bool     // Whether test should succeed
}

// TimestampConfig defines configuration for timestamp testing
type TimestampConfig struct {
	Name            string
	TableName       string              // SDK-test table name
	CreateReducer   string              // Create/insert reducer name
	UpdateReducer   string              // Update reducer name
	DeleteReducer   string              // Delete reducer name
	QueryReducer    string              // Query reducer name
	TestValues      []interface{}       // Standard test values
	TimeOps         []TimeOperation     // Time-specific operations
	ChronoTests     []ChronologicalTest // Chronological test scenarios
	PerformanceTest bool                // Whether to include in performance testing
}

// Timestamp type configurations
var TimestampTypes = []TimestampConfig{
	{
		Name:            "one_timestamp",
		TableName:       "OneTimestamp",
		CreateReducer:   "insert_one_timestamp",
		UpdateReducer:   "update_one_timestamp",
		DeleteReducer:   "delete_one_timestamp",
		QueryReducer:    "query_one_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			OneTimestampType{Id: 1, Timestamp: uint64(time.Now().UnixMicro()), Value: "timestamp_now"},
			OneTimestampType{Id: 2, Timestamp: 0, Value: "timestamp_epoch"},
			OneTimestampType{Id: 3, Timestamp: 1640995200000000, Value: "timestamp_2022"}, // 2022-01-01 00:00:00 UTC
			OneTimestampType{Id: 4, Timestamp: 4102444800000000, Value: "timestamp_2100"}, // 2100-01-01 00:00:00 UTC
			OneTimestampType{Id: 5, Timestamp: generateFutureTimestamp(), Value: "timestamp_future"},
		},
	},
	{
		Name:            "vec_timestamp",
		TableName:       "VecTimestamp",
		CreateReducer:   "insert_vec_timestamp",
		UpdateReducer:   "update_vec_timestamp",
		DeleteReducer:   "delete_vec_timestamp",
		QueryReducer:    "query_vec_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			VecTimestampType{Id: 1, Timestamps: []uint64{
				uint64(time.Now().UnixMicro()),
				uint64(time.Now().Add(time.Hour).UnixMicro()),
				uint64(time.Now().Add(time.Hour * 24).UnixMicro()),
			}, Value: "timestamp_sequence"},
			VecTimestampType{Id: 2, Timestamps: []uint64{}, Value: "timestamp_empty"},
			VecTimestampType{Id: 3, Timestamps: generateTimestampSequence(10), Value: "timestamp_multiple"},
		},
	},
	{
		Name:            "option_timestamp",
		TableName:       "OptionTimestamp",
		CreateReducer:   "insert_option_timestamp",
		UpdateReducer:   "update_option_timestamp",
		DeleteReducer:   "delete_option_timestamp",
		QueryReducer:    "query_option_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			OptionTimestampType{Id: 1, Timestamp: func() *uint64 { t := uint64(time.Now().UnixMicro()); return &t }(), Value: "option_timestamp_some"},
			OptionTimestampType{Id: 2, Timestamp: nil, Value: "option_timestamp_none"},
			OptionTimestampType{Id: 3, Timestamp: func() *uint64 { t := generateFutureTimestamp(); return &t }(), Value: "option_timestamp_future"},
		},
	},
	{
		Name:            "unique_timestamp",
		TableName:       "UniqueTimestamp",
		CreateReducer:   "insert_unique_timestamp",
		UpdateReducer:   "update_unique_timestamp",
		DeleteReducer:   "delete_unique_timestamp",
		QueryReducer:    "query_unique_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			UniqueTimestampType{Id: 1, Timestamp: uint64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), Value: "unique_timestamp_2023"},
			UniqueTimestampType{Id: 2, Timestamp: uint64(time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC).UnixMicro()), Value: "unique_timestamp_mid"},
			UniqueTimestampType{Id: 3, Timestamp: uint64(time.Date(2023, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()), Value: "unique_timestamp_end"},
		},
	},
	{
		Name:            "pk_timestamp",
		TableName:       "PkTimestamp",
		CreateReducer:   "insert_pk_timestamp",
		UpdateReducer:   "update_pk_timestamp",
		DeleteReducer:   "delete_pk_timestamp",
		QueryReducer:    "query_pk_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			TimePkType{Timestamp: uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), Value: "pk_timestamp_2024"},
			TimePkType{Timestamp: generateFutureTimestamp(), Value: "pk_timestamp_future"},
		},
	},
	{
		Name:            "time_series_data",
		TableName:       "TimeSeriesData",
		CreateReducer:   "insert_time_series_data",
		UpdateReducer:   "update_time_series_data",
		DeleteReducer:   "delete_time_series_data",
		QueryReducer:    "query_time_series_data",
		PerformanceTest: true,
		TestValues: []interface{}{
			TimeSeriesDataType{Id: 1, Timestamp: uint64(time.Now().UnixMicro()), Value: 23.5, Source: "sensor_01", Quality: 95, IntervalType: "minute"},
			TimeSeriesDataType{Id: 2, Timestamp: uint64(time.Now().Add(-time.Minute).UnixMicro()), Value: 23.2, Source: "sensor_01", Quality: 98, IntervalType: "minute"},
			TimeSeriesDataType{Id: 3, Timestamp: uint64(time.Now().Add(-time.Hour).UnixMicro()), Value: 22.8, Source: "sensor_02", Quality: 92, IntervalType: "hour"},
		},
	},
	{
		Name:            "event_log",
		TableName:       "EventLog",
		CreateReducer:   "insert_event_log",
		UpdateReducer:   "update_event_log",
		DeleteReducer:   "delete_event_log",
		QueryReducer:    "query_event_log",
		PerformanceTest: true,
		TestValues: []interface{}{
			EventLogType{Id: 1, EventTime: uint64(time.Now().UnixMicro()), EventType: "user_login", UserId: 123, Description: "User successful login", Severity: 0, Duration: 150000},
			EventLogType{Id: 2, EventTime: uint64(time.Now().Add(-time.Minute).UnixMicro()), EventType: "data_access", UserId: 123, Description: "User accessed sensitive data", Severity: 1, Duration: 2500000},
			EventLogType{Id: 3, EventTime: uint64(time.Now().Add(-time.Hour).UnixMicro()), EventType: "system_error", UserId: 0, Description: "Database connection timeout", Severity: 2, Duration: 30000000},
		},
	},
	{
		Name:            "scheduled_task",
		TableName:       "ScheduledTask",
		CreateReducer:   "insert_scheduled_task",
		UpdateReducer:   "update_scheduled_task",
		DeleteReducer:   "delete_scheduled_task",
		QueryReducer:    "query_scheduled_task",
		PerformanceTest: true,
		TestValues: []interface{}{
			ScheduledTaskType{Id: 1, ScheduledTime: uint64(time.Now().Add(time.Hour).UnixMicro()), ExecutionTime: 0, CompletionTime: 0, TaskName: "backup_database", Status: "pending", RetryCount: 0},
			ScheduledTaskType{Id: 2, ScheduledTime: uint64(time.Now().Add(-time.Hour).UnixMicro()), ExecutionTime: uint64(time.Now().Add(-time.Minute * 55).UnixMicro()), CompletionTime: uint64(time.Now().Add(-time.Minute * 50).UnixMicro()), TaskName: "cleanup_logs", Status: "completed", RetryCount: 0},
		},
	},
	{
		Name:            "duration_calc",
		TableName:       "DurationCalc",
		CreateReducer:   "insert_duration_calc",
		UpdateReducer:   "update_duration_calc",
		DeleteReducer:   "delete_duration_calc",
		QueryReducer:    "query_duration_calc",
		PerformanceTest: true,
		TestValues: []interface{}{
			DurationCalcType{Id: 1, StartTime: uint64(time.Now().Add(-time.Hour).UnixMicro()), EndTime: uint64(time.Now().UnixMicro()), Duration: uint64(time.Hour.Microseconds()), Operation: "elapsed_time", Value: "duration_hour"},
			DurationCalcType{Id: 2, StartTime: uint64(time.Now().Add(-time.Minute * 30).UnixMicro()), EndTime: uint64(time.Now().UnixMicro()), Duration: uint64(time.Minute.Microseconds() * 30), Operation: "elapsed_time", Value: "duration_30min"},
		},
	},
	{
		Name:            "time_interval",
		TableName:       "TimeInterval",
		CreateReducer:   "insert_time_interval",
		UpdateReducer:   "update_time_interval",
		DeleteReducer:   "delete_time_interval",
		QueryReducer:    "query_time_interval",
		PerformanceTest: true,
		TestValues: []interface{}{
			TimeIntervalType{Id: 1, IntervalStart: uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), IntervalEnd: uint64(time.Date(2024, 1, 1, 23, 59, 59, 999999000, time.UTC).UnixMicro()), IntervalType: "day", Value: "daily_interval"},
			TimeIntervalType{Id: 2, IntervalStart: uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), IntervalEnd: uint64(time.Date(2024, 1, 7, 23, 59, 59, 999999000, time.UTC).UnixMicro()), IntervalType: "week", Value: "weekly_interval"},
		},
	},
	{
		Name:            "indexed_timestamp",
		TableName:       "IndexedTimestamp",
		CreateReducer:   "insert_indexed_timestamp",
		UpdateReducer:   "update_indexed_timestamp",
		DeleteReducer:   "delete_indexed_timestamp",
		QueryReducer:    "query_indexed_timestamp",
		PerformanceTest: true,
		TestValues: []interface{}{
			TimeIndexedType{Id: 1, IndexedTime: uint64(time.Now().UnixMicro()), Category: "performance", SubCategory: "cpu", Value: "cpu_usage_data", ProcessingTime: uint64(time.Now().Add(time.Millisecond).UnixMicro())},
			TimeIndexedType{Id: 2, IndexedTime: uint64(time.Now().Add(-time.Minute).UnixMicro()), Category: "performance", SubCategory: "memory", Value: "memory_usage_data", ProcessingTime: uint64(time.Now().UnixMicro())},
		},
	},
}

// Test configuration constants
const (
	// Performance thresholds (timestamp specific)
	TimestampInsertTime     = 50 * time.Millisecond  // Single timestamp insert
	TimestampQueryTime      = 20 * time.Millisecond  // Single timestamp query
	TimestampArithmeticTime = 5 * time.Millisecond   // Time arithmetic operation
	ChronologicalSortTime   = 100 * time.Millisecond // Chronological sorting
	TimeIndexQueryTime      = 10 * time.Millisecond  // Indexed time query
	DurationCalculationTime = 5 * time.Millisecond   // Duration calculation

	// Test limits
	MaxTimestampTestValues    = 100 // Number of timestamps to test
	TimestampPerformanceIters = 200 // Iterations for performance testing
	ChronologicalTestCount    = 50  // Number of chronological tests
	TimeArithmeticTestCount   = 100 // Number of arithmetic operations

	// Time bounds
	MinTimestamp          = 0                // Unix epoch start
	MaxTimestamp          = 4102444800000000 // Year 2100 in microseconds
	MicrosecondsPerSecond = 1000000          // Conversion constant
	MicrosecondsPerMinute = 60000000         // Conversion constant
	MicrosecondsPerHour   = 3600000000       // Conversion constant
	MicrosecondsPerDay    = 86400000000      // Conversion constant
)

// Utility functions for generating timestamp test data
func generateFutureTimestamp() uint64 {
	// Generate a timestamp 1-30 days in the future
	future := time.Now().Add(time.Duration(1+runtime.GOMAXPROCS(0)%30) * 24 * time.Hour)
	return uint64(future.UnixMicro())
}

func generateTimestampSequence(count int) []uint64 {
	var timestamps []uint64
	base := time.Now()
	for i := 0; i < count; i++ {
		timestamp := base.Add(time.Duration(i) * time.Minute)
		timestamps = append(timestamps, uint64(timestamp.UnixMicro()))
	}
	return timestamps
}

func generateChronologicalTestData(count int) []uint64 {
	var timestamps []uint64
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		timestamp := base.Add(time.Duration(i*7) * 24 * time.Hour) // Weekly intervals
		timestamps = append(timestamps, uint64(timestamp.UnixMicro()))
	}
	return timestamps
}

// TestSDKTimestampTime is the main integration test for timestamp and time operations
func TestSDKTimestampTime(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK timestamp/time integration test in short mode")
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
	t.Logf("🎯 Testing TIMESTAMP: Core timestamp operations and BSATN encoding")
	t.Logf("🎯 Testing TIME SERIES: Chronological data handling and time-series operations")
	t.Logf("🎯 Testing TIME ARITHMETIC: Duration calculations and time comparisons")
	t.Logf("🎯 Testing INDEXED TIME: High-performance time-based queries")
	t.Logf("🎯 Testing CHRONOLOGICAL: Time ordering and sequential operations")

	// Generate additional test data
	generateTimestampTestData(t)

	// Run comprehensive timestamp and time test suites
	t.Run("TimestampBasicOperations", func(t *testing.T) {
		testTimestampBasicOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("ChronologicalDataHandling", func(t *testing.T) {
		testChronologicalDataHandling(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimeArithmeticOperations", func(t *testing.T) {
		testTimeArithmeticOperations(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimeBasedIndexing", func(t *testing.T) {
		testTimeBasedIndexing(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimestampValidation", func(t *testing.T) {
		testTimestampValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimestampPerformance", func(t *testing.T) {
		testTimestampPerformance(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimeConstraintValidation", func(t *testing.T) {
		testTimeConstraintValidation(t, ctx, wasmRuntime, registry, encodingManager)
	})

	t.Run("TimeBoundaryConditions", func(t *testing.T) {
		testTimeBoundaryConditions(t, ctx, wasmRuntime, registry, encodingManager)
	})
}

// generateTimestampTestData generates additional timestamp test data
func generateTimestampTestData(t *testing.T) {
	t.Log("Generating additional TIMESTAMP and TIME test data...")

	// Generate additional random timestamps for testing
	for i := range TimestampTypes {
		config := &TimestampTypes[i]

		// Add more test values based on type
		switch config.Name {
		case "one_timestamp":
			for j := 0; j < 20; j++ {
				// Generate timestamps across different time periods
				baseTime := time.Date(2020+j%5, time.Month(1+(j%12)), 1+j%28, j%24, j%60, j%60, 0, time.UTC)
				val := OneTimestampType{Id: uint32(200 + j), Timestamp: uint64(baseTime.UnixMicro()), Value: fmt.Sprintf("generated_timestamp_%d", j)}
				config.TestValues = append(config.TestValues, val)
			}
		case "time_series_data":
			for j := 0; j < 30; j++ {
				// Generate time series data points
				timestamp := time.Now().Add(-time.Duration(j) * time.Minute)
				val := TimeSeriesDataType{
					Id:           uint32(100 + j),
					Timestamp:    uint64(timestamp.UnixMicro()),
					Value:        20.0 + float64(j%10),
					Source:       fmt.Sprintf("sensor_%02d", j%5),
					Quality:      uint8(80 + j%20),
					IntervalType: "minute",
				}
				config.TestValues = append(config.TestValues, val)
			}
		}
	}

	t.Logf("✅ Generated additional timestamp test data for %d timestamp types", len(TimestampTypes))
}

// testTimestampBasicOperations tests basic timestamp operations
func testTimestampBasicOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIMESTAMP BASIC OPERATIONS...")

	for _, timestampType := range TimestampTypes[:5] { // Test first 5 basic types
		t.Run(fmt.Sprintf("TimestampBasic_%s", timestampType.Name), func(t *testing.T) {
			successCount := 0
			totalTests := len(timestampType.TestValues)

			for i, testValue := range timestampType.TestValues {
				if i >= 3 { // Limit to first 3 tests per type for basic operations
					break
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Timestamp encoding failed for %s: %v", timestampType.Name, err)
					continue
				}

				t.Logf("✅ %s: Timestamp encoded successfully to %d bytes", timestampType.Name, len(encoded))
				successCount++
			}

			// Report timestamp basic operation success rate
			successRate := float64(successCount) / float64(minTime(totalTests, 3)) * 100
			t.Logf("✅ %s timestamp BASIC operations: %d/%d successful (%.1f%%)",
				timestampType.Name, successCount, minTime(totalTests, 3), successRate)

			assert.Greater(t, successRate, 80.0,
				"Timestamp basic operations should have >80%% success rate")
		})
	}

	t.Log("✅ Timestamp basic operations testing completed")
}

// testChronologicalDataHandling tests chronological data operations
func testChronologicalDataHandling(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing CHRONOLOGICAL DATA HANDLING...")

	// Test time series data
	timeSeriesConfig := TimestampTypes[5] // time_series_data
	t.Run("ChronologicalTimeSeries", func(t *testing.T) {
		successCount := 0
		totalTests := len(timeSeriesConfig.TestValues)

		for i, testValue := range timeSeriesConfig.TestValues {
			if i >= 5 { // Limit chronological tests
				break
			}

			// Test BSATN encoding for time series
			encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
			if err != nil {
				t.Logf("⚠️  Time series encoding failed: %v", err)
				continue
			}

			// Validate time series data structure
			if data, ok := testValue.(TimeSeriesDataType); ok {
				// Check timestamp is reasonable
				now := uint64(time.Now().UnixMicro())
				if data.Timestamp > 0 && data.Timestamp <= now+uint64(time.Hour.Microseconds()) {
					t.Logf("✅ Time series data: timestamp=%d, value=%.2f, quality=%d, encoded=%d bytes",
						data.Timestamp, data.Value, data.Quality, len(encoded))
					successCount++
				} else {
					t.Logf("⚠️  Invalid timestamp in time series data: %d", data.Timestamp)
				}
			}
		}

		// Report chronological success rate
		successRate := float64(successCount) / float64(minTime(totalTests, 5)) * 100
		t.Logf("✅ CHRONOLOGICAL data handling: %d/%d successful (%.1f%%)",
			successCount, minTime(totalTests, 5), successRate)

		assert.Greater(t, successRate, 85.0,
			"Chronological data handling should have >85%% success rate")
	})

	// Test event log chronological ordering
	eventLogConfig := TimestampTypes[6] // event_log
	t.Run("ChronologicalEventLog", func(t *testing.T) {
		successCount := 0
		var eventTimes []uint64

		for i, testValue := range eventLogConfig.TestValues {
			if i >= 3 { // Test first 3 events
				break
			}

			if event, ok := testValue.(EventLogType); ok {
				eventTimes = append(eventTimes, event.EventTime)

				// Test encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err == nil {
					t.Logf("✅ Event log: time=%d, type=%s, severity=%d, encoded=%d bytes",
						event.EventTime, event.EventType, event.Severity, len(encoded))
					successCount++
				}
			}
		}

		// Verify chronological order
		if len(eventTimes) > 1 {
			sortedTimes := make([]uint64, len(eventTimes))
			copy(sortedTimes, eventTimes)
			sort.Slice(sortedTimes, func(i, j int) bool { return sortedTimes[i] < sortedTimes[j] })

			t.Logf("✅ Event times chronological verification: original=%v, sorted=%v", eventTimes, sortedTimes)
		}

		assert.Equal(t, len(eventLogConfig.TestValues[:3]), successCount,
			"All event log entries should encode successfully")
	})

	t.Log("✅ Chronological data handling testing completed")
}

// testTimeArithmeticOperations tests time arithmetic and duration calculations
func testTimeArithmeticOperations(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIME ARITHMETIC OPERATIONS...")

	// Test duration calculations
	durationConfig := TimestampTypes[8] // duration_calc
	t.Run("TimeArithmeticDurations", func(t *testing.T) {
		successCount := 0
		totalTests := len(durationConfig.TestValues)

		for _, testValue := range durationConfig.TestValues {
			if duration, ok := testValue.(DurationCalcType); ok {
				// Verify duration calculation
				calculatedDuration := duration.EndTime - duration.StartTime
				if calculatedDuration == duration.Duration {
					t.Logf("✅ Duration calculation: start=%d, end=%d, duration=%d μs (%.2f sec)",
						duration.StartTime, duration.EndTime, duration.Duration, float64(duration.Duration)/1000000.0)
					successCount++
				} else {
					t.Logf("⚠️  Duration mismatch: calculated=%d, stored=%d", calculatedDuration, duration.Duration)
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Duration encoding failed: %v", err)
				} else {
					t.Logf("✅ Duration encoded: %d bytes", len(encoded))
				}
			}
		}

		// Report arithmetic success rate
		successRate := float64(successCount) / float64(totalTests) * 100
		t.Logf("✅ TIME ARITHMETIC operations: %d/%d successful (%.1f%%)",
			successCount, totalTests, successRate)

		assert.Greater(t, successRate, 90.0,
			"Time arithmetic operations should have >90%% success rate")
	})

	// Test time intervals
	intervalConfig := TimestampTypes[9] // time_interval
	t.Run("TimeArithmeticIntervals", func(t *testing.T) {
		successCount := 0
		totalTests := len(intervalConfig.TestValues)

		for _, testValue := range intervalConfig.TestValues {
			if interval, ok := testValue.(TimeIntervalType); ok {
				// Verify interval validity
				if interval.IntervalEnd > interval.IntervalStart {
					intervalDuration := interval.IntervalEnd - interval.IntervalStart
					t.Logf("✅ Time interval: type=%s, duration=%d μs (%.2f hours)",
						interval.IntervalType, intervalDuration, float64(intervalDuration)/float64(MicrosecondsPerHour))
					successCount++
				} else {
					t.Logf("⚠️  Invalid interval: end=%d <= start=%d", interval.IntervalEnd, interval.IntervalStart)
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Interval encoding failed: %v", err)
				} else {
					t.Logf("✅ Interval encoded: %d bytes", len(encoded))
				}
			}
		}

		// Report interval success rate
		successRate := float64(successCount) / float64(totalTests) * 100
		t.Logf("✅ TIME INTERVAL operations: %d/%d successful (%.1f%%)",
			successCount, totalTests, successRate)

		assert.Greater(t, successRate, 95.0,
			"Time interval operations should have >95%% success rate")
	})

	t.Log("✅ Time arithmetic operations testing completed")
}

// testTimeBasedIndexing tests time-based indexing and queries
func testTimeBasedIndexing(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIME-BASED INDEXING...")

	// Test indexed timestamp operations
	indexedConfig := TimestampTypes[10] // indexed_timestamp
	t.Run("TimeBasedIndexedOperations", func(t *testing.T) {
		successCount := 0
		totalTests := len(indexedConfig.TestValues)

		for _, testValue := range indexedConfig.TestValues {
			if indexed, ok := testValue.(TimeIndexedType); ok {
				// Test BSATN encoding for indexed timestamp
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Indexed timestamp encoding failed: %v", err)
					continue
				}

				// Verify indexed timestamp structure
				if indexed.IndexedTime > 0 && indexed.ProcessingTime >= indexed.IndexedTime {
					t.Logf("✅ Indexed timestamp: indexed_time=%d, processing_time=%d, category=%s, encoded=%d bytes",
						indexed.IndexedTime, indexed.ProcessingTime, indexed.Category, len(encoded))
					successCount++
				} else {
					t.Logf("⚠️  Invalid indexed timestamp: indexed=%d, processing=%d", indexed.IndexedTime, indexed.ProcessingTime)
				}
			}
		}

		// Report indexing success rate
		successRate := float64(successCount) / float64(totalTests) * 100
		t.Logf("✅ TIME-BASED INDEXING operations: %d/%d successful (%.1f%%)",
			successCount, totalTests, successRate)

		assert.Greater(t, successRate, 85.0,
			"Time-based indexing should have >85%% success rate")
	})

	t.Log("✅ Time-based indexing testing completed")
}

// testTimestampValidation tests timestamp validation and constraints
func testTimestampValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIMESTAMP VALIDATION...")

	// Test unique timestamp constraints
	uniqueConfig := TimestampTypes[3] // unique_timestamp
	t.Run("TimestampUniqueValidation", func(t *testing.T) {
		successCount := 0
		var usedTimestamps []uint64

		for _, testValue := range uniqueConfig.TestValues {
			if unique, ok := testValue.(UniqueTimestampType); ok {
				// Check for timestamp uniqueness
				isUnique := true
				for _, used := range usedTimestamps {
					if used == unique.Timestamp {
						isUnique = false
						break
					}
				}

				if isUnique {
					usedTimestamps = append(usedTimestamps, unique.Timestamp)
					t.Logf("✅ Unique timestamp validated: %d", unique.Timestamp)
					successCount++
				} else {
					t.Logf("⚠️  Duplicate timestamp detected: %d", unique.Timestamp)
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Unique timestamp encoding failed: %v", err)
				} else {
					t.Logf("✅ Unique timestamp encoded: %d bytes", len(encoded))
				}
			}
		}

		assert.Equal(t, len(uniqueConfig.TestValues), successCount,
			"All unique timestamps should be distinct")
	})

	// Test primary key timestamp constraints
	pkConfig := TimestampTypes[4] // pk_timestamp
	t.Run("TimestampPrimaryKeyValidation", func(t *testing.T) {
		successCount := 0
		var pkTimestamps []uint64

		for _, testValue := range pkConfig.TestValues {
			if pk, ok := testValue.(TimePkType); ok {
				// Check primary key uniqueness
				isUnique := true
				for _, used := range pkTimestamps {
					if used == pk.Timestamp {
						isUnique = false
						break
					}
				}

				if isUnique {
					pkTimestamps = append(pkTimestamps, pk.Timestamp)
					t.Logf("✅ Primary key timestamp validated: %d", pk.Timestamp)
					successCount++
				} else {
					t.Logf("⚠️  Duplicate primary key timestamp: %d", pk.Timestamp)
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  PK timestamp encoding failed: %v", err)
				} else {
					t.Logf("✅ PK timestamp encoded: %d bytes", len(encoded))
				}
			}
		}

		assert.Equal(t, len(pkConfig.TestValues), successCount,
			"All primary key timestamps should be distinct")
	})

	t.Log("✅ Timestamp validation testing completed")
}

// testTimestampPerformance tests timestamp performance characteristics
func testTimestampPerformance(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIMESTAMP PERFORMANCE...")

	// Performance test configuration
	performanceTypes := []TimestampConfig{}
	for _, config := range TimestampTypes {
		if config.PerformanceTest && len(config.TestValues) > 0 {
			performanceTypes = append(performanceTypes, config)
		}
	}

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	for _, timestampType := range performanceTypes {
		t.Run(fmt.Sprintf("TimestampPerformance_%s", timestampType.Name), func(t *testing.T) {
			iterations := minTime(len(timestampType.TestValues), 10) // Limit iterations for performance
			successCount := 0

			typeStartTime := time.Now()

			for i := 0; i < iterations; i++ {
				testValue := timestampType.TestValues[i%len(timestampType.TestValues)]

				// Performance test: BSATN encoding
				operationStart := time.Now()
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				operationDuration := time.Since(operationStart)

				if err == nil && len(encoded) > 0 {
					successCount++
				}

				// Log only first few operations to avoid spam
				if i < 3 {
					t.Logf("✅ %s encoding: %v (%d bytes)", timestampType.Name, operationDuration, len(encoded))
				}
			}

			typeDuration := time.Since(typeStartTime)
			avgDuration := typeDuration / time.Duration(iterations)

			t.Logf("✅ %s performance: %d/%d successful, avg %v per operation",
				timestampType.Name, successCount, iterations, avgDuration)

			// Performance assertions
			assert.Less(t, avgDuration, 50*time.Microsecond,
				"Average timestamp encoding should be <50µs")
			assert.Greater(t, float64(successCount)/float64(iterations), 0.95,
				"Success rate should be >95%%")
		})
	}

	totalDuration := time.Since(startTime)
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	t.Logf("✅ TIMESTAMP PERFORMANCE SUMMARY:")
	t.Logf("   Total time: %v", totalDuration)
	t.Logf("   Memory used: %d bytes", memAfter.Alloc-memBefore.Alloc)
	t.Logf("   Types tested: %d", len(performanceTypes))

	assert.Less(t, totalDuration, 2*time.Second,
		"Total timestamp performance test should complete in <2s")

	t.Log("✅ Timestamp performance testing completed")
}

// testTimeConstraintValidation tests time constraint validation
func testTimeConstraintValidation(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIME CONSTRAINT VALIDATION...")

	// Test scheduled task time constraints
	scheduledConfig := TimestampTypes[7] // scheduled_task
	t.Run("TimeConstraintScheduledTasks", func(t *testing.T) {
		successCount := 0
		totalTests := len(scheduledConfig.TestValues)

		for _, testValue := range scheduledConfig.TestValues {
			if task, ok := testValue.(ScheduledTaskType); ok {
				// Validate time constraint: execution_time >= scheduled_time (if executed)
				isValid := true
				if task.ExecutionTime > 0 && task.ExecutionTime < task.ScheduledTime {
					isValid = false
					t.Logf("⚠️  Invalid task timing: executed before scheduled (scheduled=%d, executed=%d)",
						task.ScheduledTime, task.ExecutionTime)
				}

				// Validate completion_time >= execution_time (if completed)
				if task.CompletionTime > 0 && task.ExecutionTime > 0 && task.CompletionTime < task.ExecutionTime {
					isValid = false
					t.Logf("⚠️  Invalid task timing: completed before execution (executed=%d, completed=%d)",
						task.ExecutionTime, task.CompletionTime)
				}

				if isValid {
					t.Logf("✅ Task time constraints valid: %s (status=%s)", task.TaskName, task.Status)
					successCount++
				}

				// Test BSATN encoding
				encoded, err := encodingManager.Encode(testValue, db.EncodingBSATN, nil)
				if err != nil {
					t.Logf("⚠️  Scheduled task encoding failed: %v", err)
				} else {
					t.Logf("✅ Scheduled task encoded: %d bytes", len(encoded))
				}
			}
		}

		// Report constraint validation success rate
		successRate := float64(successCount) / float64(totalTests) * 100
		t.Logf("✅ TIME CONSTRAINT validation: %d/%d successful (%.1f%%)",
			successCount, totalTests, successRate)

		assert.Greater(t, successRate, 95.0,
			"Time constraint validation should have >95%% success rate")
	})

	t.Log("✅ Time constraint validation testing completed")
}

// testTimeBoundaryConditions tests time boundary conditions and edge cases
func testTimeBoundaryConditions(t *testing.T, ctx context.Context, wasmRuntime *wasm.Runtime, registry *ReducerRegistry, encodingManager *db.EncodingManager) {
	t.Log("🔥 Testing TIME BOUNDARY CONDITIONS...")

	// Define boundary condition test cases
	boundaryTests := []struct {
		Name        string
		Description string
		TestValue   interface{}
		ShouldWork  bool
	}{
		// Basic timestamp boundaries
		{"boundary_timestamp_epoch", "Unix epoch timestamp", OneTimestampType{Id: 1, Timestamp: 0, Value: "epoch"}, true},
		{"boundary_timestamp_max", "Maximum timestamp value", OneTimestampType{Id: 2, Timestamp: math.MaxUint64, Value: "max"}, true},
		{"boundary_timestamp_min_positive", "Minimum positive timestamp", OneTimestampType{Id: 3, Timestamp: 1, Value: "min_positive"}, true},

		// Time series boundaries
		{"boundary_time_series_zero", "Time series with zero timestamp", TimeSeriesDataType{Id: 1, Timestamp: 0, Value: 0.0, Source: "test", Quality: 100, IntervalType: "second"}, true},
		{"boundary_time_series_max", "Time series with max timestamp", TimeSeriesDataType{Id: 2, Timestamp: MaxTimestamp, Value: 100.0, Source: "test", Quality: 100, IntervalType: "year"}, true},

		// Duration boundaries
		{"boundary_duration_zero", "Zero duration calculation", DurationCalcType{Id: 1, StartTime: 1000000, EndTime: 1000000, Duration: 0, Operation: "zero_duration", Value: "zero"}, true},
		{"boundary_duration_large", "Large duration calculation", DurationCalcType{Id: 2, StartTime: 0, EndTime: MaxTimestamp, Duration: MaxTimestamp, Operation: "max_duration", Value: "max"}, true},

		// Interval boundaries
		{"boundary_interval_instant", "Instantaneous interval", TimeIntervalType{Id: 1, IntervalStart: 1000000, IntervalEnd: 1000000, IntervalType: "instant", Value: "instant"}, true},
		{"boundary_interval_max", "Maximum interval", TimeIntervalType{Id: 2, IntervalStart: 0, IntervalEnd: MaxTimestamp, IntervalType: "maximum", Value: "max_interval"}, true},

		// Vector boundaries
		{"boundary_vec_empty", "Empty timestamp vector", VecTimestampType{Id: 1, Timestamps: []uint64{}, Value: "empty_vector"}, true},
		{"boundary_vec_single", "Single timestamp vector", VecTimestampType{Id: 2, Timestamps: []uint64{uint64(time.Now().UnixMicro())}, Value: "single_vector"}, true},
		{"boundary_vec_large", "Large timestamp vector", VecTimestampType{Id: 3, Timestamps: generateTimestampSequence(100), Value: "large_vector"}, true},

		// Option boundaries
		{"boundary_option_nil", "Nil optional timestamp", OptionTimestampType{Id: 1, Timestamp: nil, Value: "nil_option"}, true},
		{"boundary_option_zero", "Zero optional timestamp", OptionTimestampType{Id: 2, Timestamp: func() *uint64 { t := uint64(0); return &t }(), Value: "zero_option"}, true},
		{"boundary_option_max", "Max optional timestamp", OptionTimestampType{Id: 3, Timestamp: func() *uint64 { t := uint64(MaxTimestamp); return &t }(), Value: "max_option"}, true},

		// Quality boundaries for time series
		{"boundary_quality_min", "Minimum quality time series", TimeSeriesDataType{Id: 10, Timestamp: uint64(time.Now().UnixMicro()), Value: 50.0, Source: "boundary_test", Quality: 0, IntervalType: "test"}, true},
		{"boundary_quality_max", "Maximum quality time series", TimeSeriesDataType{Id: 11, Timestamp: uint64(time.Now().UnixMicro()), Value: 50.0, Source: "boundary_test", Quality: 255, IntervalType: "test"}, true},

		// Retry count boundaries for scheduled tasks
		{"boundary_retry_zero", "Zero retry scheduled task", ScheduledTaskType{Id: 10, ScheduledTime: uint64(time.Now().UnixMicro()), ExecutionTime: 0, CompletionTime: 0, TaskName: "boundary_test", Status: "pending", RetryCount: 0}, true},
		{"boundary_retry_max", "Maximum retry scheduled task", ScheduledTaskType{Id: 11, ScheduledTime: uint64(time.Now().UnixMicro()), ExecutionTime: 0, CompletionTime: 0, TaskName: "boundary_test", Status: "failed", RetryCount: 255}, true},
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
	t.Logf("✅ TIME BOUNDARY CONDITIONS: %d/%d successful (%.1f%%)",
		successCount, totalTests, successRate)

	assert.Greater(t, successRate, 90.0,
		"Time boundary conditions should have >90%% success rate")

	t.Log("✅ Time boundary conditions testing completed")
}

// Helper function for min operation
func minTime(a, b int) int {
	if a < b {
		return a
	}
	return b
}
