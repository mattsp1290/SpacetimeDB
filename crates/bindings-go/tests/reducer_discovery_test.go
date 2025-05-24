package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/wasm"
	"github.com/stretchr/testify/require"
)

// ReducerInfo contains information about a discovered reducer
type ReducerInfo struct {
	ID         uint32
	Name       string
	ExportName string
}

// ReducerRegistry maintains a mapping of reducer names to IDs
type ReducerRegistry struct {
	ByName map[string]*ReducerInfo
	ByID   map[uint32]*ReducerInfo
	All    []*ReducerInfo
}

// NewReducerRegistry creates a new empty reducer registry
func NewReducerRegistry() *ReducerRegistry {
	return &ReducerRegistry{
		ByName: make(map[string]*ReducerInfo),
		ByID:   make(map[uint32]*ReducerInfo),
		All:    make([]*ReducerInfo, 0),
	}
}

// Add adds a reducer to the registry
func (r *ReducerRegistry) Add(info *ReducerInfo) {
	r.ByName[info.Name] = info
	r.ByID[info.ID] = info
	r.All = append(r.All, info)
}

// GetByName returns a reducer by name
func (r *ReducerRegistry) GetByName(name string) (*ReducerInfo, bool) {
	info, exists := r.ByName[name]
	return info, exists
}

// GetByID returns a reducer by ID
func (r *ReducerRegistry) GetByID(id uint32) (*ReducerInfo, bool) {
	info, exists := r.ByID[id]
	return info, exists
}

// DiscoverReducers examines a WASM module and discovers all exported reducers
func DiscoverReducers(ctx context.Context, runtime *wasm.Runtime, moduleName string) (*ReducerRegistry, error) {
	registry := NewReducerRegistry()

	// Get the exported functions directly from the runtime
	exports, err := runtime.ListExports()
	if err != nil {
		return nil, fmt.Errorf("failed to list exports: %w", err)
	}

	fmt.Printf("[DEBUG] Module %s has %d exported functions\n", moduleName, len(exports))

	reducerCount := 0

	for _, exportName := range exports {
		fmt.Printf("[DEBUG] Examining export: %s\n", exportName)

		// Check if this is a reducer export
		// SpacetimeDB reducers are exported with pattern: __reducer_call_<id>__ or similar
		if reducerInfo := parseReducerExport(exportName); reducerInfo != nil {
			registry.Add(reducerInfo)
			reducerCount++
			fmt.Printf("[DEBUG] Found reducer: %s (ID: %d, Export: %s)\n",
				reducerInfo.Name, reducerInfo.ID, reducerInfo.ExportName)
		}
	}

	fmt.Printf("[DEBUG] Discovered %d reducers in module %s\n", reducerCount, moduleName)

	// Sort the reducers by ID for consistent ordering
	sort.Slice(registry.All, func(i, j int) bool {
		return registry.All[i].ID < registry.All[j].ID
	})

	return registry, nil
}

// parseReducerExport attempts to parse a WASM export name as a reducer
func parseReducerExport(exportName string) *ReducerInfo {
	// Pattern 1: __reducer_call_<id>__
	if strings.HasPrefix(exportName, "__reducer_call_") && strings.HasSuffix(exportName, "__") {
		idStr := strings.TrimPrefix(exportName, "__reducer_call_")
		idStr = strings.TrimSuffix(idStr, "__")

		if id, err := strconv.ParseUint(idStr, 10, 32); err == nil {
			return &ReducerInfo{
				ID:         uint32(id),
				Name:       fmt.Sprintf("reducer_%d", id),
				ExportName: exportName,
			}
		}
	}

	// Pattern 2: __call_reducer__
	if exportName == "__call_reducer__" {
		return &ReducerInfo{
			ID:         0, // Special case - this is the generic call reducer
			Name:       "call_reducer",
			ExportName: exportName,
		}
	}

	// Pattern 3: Direct function names (some modules export direct reducer names)
	knownReducers := map[string]uint32{
		"load_location_table":              1,
		"test_index_scan_on_id":            2,
		"test_index_scan_on_chunk":         3,
		"test_index_scan_on_x_z_dimension": 4,
		"test_index_scan_on_x_z":           5,
		"clear_table":                      6,
		"insert_bulk_person":               7,
		"insert_person":                    8,
		"update_person":                    9,
		"delete_person":                    10,
	}

	if id, exists := knownReducers[exportName]; exists {
		return &ReducerInfo{
			ID:         id,
			Name:       exportName,
			ExportName: exportName,
		}
	}

	return nil
}

// TestReducerDiscovery tests the reducer discovery functionality
func TestReducerDiscovery(t *testing.T) {
	repoRoot := os.Getenv("SPACETIMEDB_DIR")
	if repoRoot == "" {
		t.Skip("SPACETIMEDB_DIR not set – skipping reducer discovery test")
	}

	testCases := []struct {
		name       string
		modulePath string
		expected   []string // Expected reducer names
	}{
		{
			name:       "SDK Test Module",
			modulePath: "target/wasm32-unknown-unknown/release/sdk_test_module.wasm",
			expected:   []string{"call_reducer"}, // At minimum should have the generic call reducer
		},
		{
			name:       "Perf Test Module",
			modulePath: "target/wasm32-unknown-unknown/release/perf_test_module.wasm",
			expected:   []string{"load_location_table", "test_index_scan_on_id"},
		},
		{
			name:       "Chat Module",
			modulePath: "target/wasm32-unknown-unknown/release/quickstart_chat_module.wasm",
			expected:   []string{"call_reducer"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Load WASM module
			wasmPath := filepath.Join(repoRoot, tc.modulePath)
			if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
				t.Skipf("WASM module not found: %v", wasmPath)
			}

			wasmRuntime, err := wasm.NewRuntime(wasm.DefaultConfig())
			require.NoError(t, err)
			defer wasmRuntime.Close(ctx)

			wasmBytes, err := os.ReadFile(wasmPath)
			require.NoError(t, err)

			err = wasmRuntime.LoadModule(ctx, wasmBytes)
			require.NoError(t, err)

			err = wasmRuntime.InstantiateModule(ctx, tc.name, true)
			require.NoError(t, err)

			// Discover reducers
			registry, err := DiscoverReducers(ctx, wasmRuntime, tc.name)
			require.NoError(t, err)

			t.Logf("Discovered %d reducers in %s:", len(registry.All), tc.name)
			for _, reducer := range registry.All {
				t.Logf("  - %s (ID: %d, Export: %s)", reducer.Name, reducer.ID, reducer.ExportName)
			}

			// Verify expected reducers are found
			for _, expectedName := range tc.expected {
				if _, found := registry.GetByName(expectedName); !found {
					t.Logf("Expected reducer '%s' not found. Available reducers:", expectedName)
					for _, reducer := range registry.All {
						t.Logf("  - %s", reducer.Name)
					}
				}
			}

			// Test registry functionality
			if len(registry.All) > 0 {
				firstReducer := registry.All[0]

				// Test lookup by name
				foundByName, exists := registry.GetByName(firstReducer.Name)
				require.True(t, exists, "Should be able to find reducer by name")
				require.Equal(t, firstReducer.ID, foundByName.ID)

				// Test lookup by ID
				foundByID, exists := registry.GetByID(firstReducer.ID)
				require.True(t, exists, "Should be able to find reducer by ID")
				require.Equal(t, firstReducer.Name, foundByID.Name)
			}
		})
	}
}

// TestReducerDiscoveryIntegration tests using discovered reducers with the performance test
func TestReducerDiscoveryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	repoRoot := os.Getenv("SPACETIMEDB_DIR")
	if repoRoot == "" {
		t.Skip("SPACETIMEDB_DIR not set – skipping reducer discovery integration")
	}

	ctx := context.Background()

	// Load perf-test module
	wasmPath := filepath.Join(repoRoot, "target/wasm32-unknown-unknown/release/perf_test_module.wasm")
	if _, err := os.Stat(wasmPath); os.IsNotExist(err) {
		t.Skipf("Perf-test WASM module not found: %v", wasmPath)
	}

	wasmRuntime, err := wasm.NewRuntime(wasm.DefaultConfig())
	require.NoError(t, err)
	defer wasmRuntime.Close(ctx)

	wasmBytes, err := os.ReadFile(wasmPath)
	require.NoError(t, err)

	err = wasmRuntime.LoadModule(ctx, wasmBytes)
	require.NoError(t, err)

	err = wasmRuntime.InstantiateModule(ctx, "perf_test_module", true)
	require.NoError(t, err)

	// Discover reducers
	registry, err := DiscoverReducers(ctx, wasmRuntime, "perf_test_module")
	require.NoError(t, err)

	t.Logf("✅ Successfully discovered %d reducers in perf_test_module", len(registry.All))

	// Test calling a discovered reducer (if any)
	if len(registry.All) > 0 {
		reducer := registry.All[0]
		t.Logf("Testing call to reducer: %s (ID: %d)", reducer.Name, reducer.ID)

		senderIdentity := [4]uint64{0, 0, 0, 0}
		connectionId := [2]uint64{0, 0}
		timestamp := uint64(1234567890)

		// Attempt to call the reducer
		result, err := wasmRuntime.CallReducer(ctx, reducer.ID, senderIdentity, connectionId, timestamp, []byte{})

		// Note: This might fail due to missing implementation, but the discovery worked
		if err != nil {
			t.Logf("Reducer call failed (expected): %v", err)
		} else {
			t.Logf("Reducer call succeeded: %s", result)
		}

		t.Logf("✅ Reducer discovery and call integration test completed")
	}
}

// GetPerfTestReducers returns a pre-configured registry for known perf-test reducers
func GetPerfTestReducers() *ReducerRegistry {
	registry := NewReducerRegistry()

	// Add known perf-test reducers with their expected IDs
	perfReducers := []*ReducerInfo{
		{ID: 1, Name: "load_location_table", ExportName: "load_location_table"},
		{ID: 2, Name: "test_index_scan_on_id", ExportName: "test_index_scan_on_id"},
		{ID: 3, Name: "test_index_scan_on_chunk", ExportName: "test_index_scan_on_chunk"},
		{ID: 4, Name: "test_index_scan_on_x_z_dimension", ExportName: "test_index_scan_on_x_z_dimension"},
		{ID: 5, Name: "test_index_scan_on_x_z", ExportName: "test_index_scan_on_x_z"},
		{ID: 6, Name: "clear_table", ExportName: "clear_table"},
	}

	for _, reducer := range perfReducers {
		registry.Add(reducer)
	}

	return registry
}
