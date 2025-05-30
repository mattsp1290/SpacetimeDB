package reducers

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/clockworklabs/SpacetimeDB/crates/bindings-go/pkg/spacetimedb/realtime"
)

// 🔥 BLACKHOLIO REDUCER SYSTEM - THE ULTIMATE SPACETIMEDB GO INTEGRATION!
// Leveraging Go 1.24's INSANE WASM capabilities for reactive multiplayer gaming!

// ReducerType represents the type of reducer
type ReducerType int

const (
	ReducerTypeInit ReducerType = iota
	ReducerTypeUpdate
	ReducerTypeConnect
	ReducerTypeDisconnect
	ReducerTypeScheduled
	ReducerTypeCustom
)

// LifecycleType represents the type of lifecycle event
type LifecycleType int

const (
	LifecycleInit LifecycleType = iota
	LifecycleUpdate
	LifecycleConnect
	LifecycleDisconnect
)

// ReducerResult represents the result of a reducer execution
type ReducerResult struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   error       `json:"-"`
}

// ReducerFunction represents a function that can be called as a reducer
type ReducerFunction interface {
	Execute(ctx *ReducerContext, args []byte) ReducerResult
	Name() string
	Description() string
}

// LifecycleFunction represents a function that can be called for lifecycle events
type LifecycleFunction interface {
	Execute(ctx *ReducerContext, lifecycleType LifecycleType) ReducerResult
	Name() string
	Type() LifecycleType
}

// GenericReducer provides a simple implementation of ReducerFunction
type GenericReducer struct {
	name        string
	description string
	handler     func(ctx *ReducerContext, args []byte) ReducerResult
}

// GenericLifecycleFunction provides a simple implementation of LifecycleFunction
type GenericLifecycleFunction struct {
	name          string
	lifecycleType LifecycleType
	handler       func(ctx *ReducerContext, lifecycleType LifecycleType) ReducerResult
}

// ReducerMetrics tracks performance metrics for reducers
type ReducerMetrics struct {
	ExecutionCount int64         `json:"execution_count"`
	TotalTime      time.Duration `json:"total_time"`
	AverageTime    time.Duration `json:"average_time"`
	ErrorCount     int64         `json:"error_count"`
	LastExecutedAt time.Time     `json:"last_executed_at"`
	Name           string        `json:"name"`
}

func (rt ReducerType) String() string {
	switch rt {
	case ReducerTypeInit:
		return "init"
	case ReducerTypeUpdate:
		return "update"
	case ReducerTypeConnect:
		return "connect"
	case ReducerTypeDisconnect:
		return "disconnect"
	case ReducerTypeScheduled:
		return "scheduled"
	case ReducerTypeCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// ReducerContext provides access to SpacetimeDB runtime services
type ReducerContext struct {
	// Database access
	Database *DatabaseContext

	// Real-time events
	Events *realtime.EventBus

	// Execution context
	ReducerName string
	CallTime    time.Time
	Sender      *Identity
	Random      *RandomContext

	// Logging
	Logger *LogContext

	// Go 1.24 WASM integration
	WasmModule *WasmContext

	// Internal context
	ctx context.Context
}

// DatabaseContext provides database access through our Phase 4 system
type DatabaseContext struct {
	tables map[string]interface{} // Type-safe table accessors (kept private)
	mu     sync.RWMutex
}

// GetTables provides controlled access to the tables map
func (dc *DatabaseContext) GetTables() map[string]interface{} {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	// Return a copy to prevent external modification
	tablesCopy := make(map[string]interface{})
	for name, table := range dc.tables {
		tablesCopy[name] = table
	}
	return tablesCopy
}

// SetTable safely sets a table in the context
func (dc *DatabaseContext) SetTable(name string, table interface{}) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.tables[name] = table
}

// HasTable checks if a table exists
func (dc *DatabaseContext) HasTable(name string) bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	_, exists := dc.tables[name]
	return exists
}

// Identity represents a client or system identity
type Identity struct {
	ID       []byte    `json:"id"`
	Name     string    `json:"name,omitempty"`
	Address  string    `json:"address,omitempty"`
	IsSystem bool      `json:"is_system"`
	JoinedAt time.Time `json:"joined_at"`
}

// RandomContext provides deterministic random number generation
type RandomContext struct {
	seed   int64
	source *deterministicRandom
}

// LogContext provides structured logging for reducers
type LogContext struct {
	level  LogLevel
	fields map[string]interface{}
	mu     sync.RWMutex
}

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// WasmContext leverages Go 1.24's AWESOME WASM capabilities!
type WasmContext struct {
	ModuleID     string
	MemoryAccess *WasmMemory
	CallStack    []string
	Permissions  WasmPermissions
}

type WasmMemory struct {
	BaseAddr uintptr
	Size     int
	mu       sync.RWMutex
}

type WasmPermissions struct {
	CanAccessDatabase bool
	CanEmitEvents     bool
	CanSchedule       bool
	MaxMemory         int
}

// Reducer represents a SpacetimeDB reducer function
type Reducer interface {
	// Core reducer interface
	Name() string
	Type() ReducerType
	Execute(ctx *ReducerContext, args []interface{}) (interface{}, error)

	// Metadata
	Description() string
	Parameters() []ParameterInfo
	ReturnType() reflect.Type

	// Validation
	ValidateArgs(args []interface{}) error
}

// ParameterInfo describes a reducer parameter
type ParameterInfo struct {
	Name     string       `json:"name"`
	Type     reflect.Type `json:"-"`
	TypeName string       `json:"type"`
	Required bool         `json:"required"`
	Default  interface{}  `json:"default,omitempty"`
}

// BaseReducer provides common reducer functionality
type BaseReducer struct {
	name        string
	reducerType ReducerType
	description string
	parameters  []ParameterInfo
	returnType  reflect.Type
	handler     ReducerHandler
}

// ReducerHandler is the actual function that executes the reducer logic
type ReducerHandler func(ctx *ReducerContext, args []interface{}) (interface{}, error)

// NewReducer creates a new reducer with the specified configuration
func NewReducer(name string, rType ReducerType, handler ReducerHandler) *BaseReducer {
	return &BaseReducer{
		name:        name,
		reducerType: rType,
		handler:     handler,
		parameters:  make([]ParameterInfo, 0),
	}
}

// Implement Reducer interface
func (br *BaseReducer) Name() string                { return br.name }
func (br *BaseReducer) Type() ReducerType           { return br.reducerType }
func (br *BaseReducer) Description() string         { return br.description }
func (br *BaseReducer) Parameters() []ParameterInfo { return br.parameters }
func (br *BaseReducer) ReturnType() reflect.Type    { return br.returnType }

func (br *BaseReducer) Execute(ctx *ReducerContext, args []interface{}) (interface{}, error) {
	if br.handler == nil {
		return nil, fmt.Errorf("reducer %s has no handler", br.name)
	}

	// Validate arguments
	if err := br.ValidateArgs(args); err != nil {
		return nil, fmt.Errorf("argument validation failed: %w", err)
	}

	// Execute with timing and logging
	start := time.Now()
	ctx.Logger.Info("Executing reducer", map[string]interface{}{
		"reducer": br.name,
		"type":    br.reducerType.String(),
		"args":    len(args),
	})

	result, err := br.handler(ctx, args)

	duration := time.Since(start)
	ctx.Logger.Info("Reducer completed", map[string]interface{}{
		"reducer":  br.name,
		"duration": duration,
		"success":  err == nil,
	})

	return result, err
}

func (br *BaseReducer) ValidateArgs(args []interface{}) error {
	if len(args) != len(br.parameters) {
		return fmt.Errorf("expected %d arguments, got %d", len(br.parameters), len(args))
	}

	for i, param := range br.parameters {
		if i >= len(args) {
			if param.Required {
				return fmt.Errorf("missing required parameter: %s", param.Name)
			}
			continue
		}

		// Type validation would go here
		// Using reflection to check types against param.Type
	}

	return nil
}

// Builder methods for reducer configuration
func (br *BaseReducer) WithDescription(desc string) *BaseReducer {
	br.description = desc
	return br
}

func (br *BaseReducer) WithParameter(name string, paramType reflect.Type, required bool) *BaseReducer {
	param := ParameterInfo{
		Name:     name,
		Type:     paramType,
		TypeName: paramType.String(),
		Required: required,
	}
	br.parameters = append(br.parameters, param)
	return br
}

func (br *BaseReducer) WithReturnType(returnType reflect.Type) *BaseReducer {
	br.returnType = returnType
	return br
}

// ReducerRegistry manages all registered reducers
type ReducerRegistry struct {
	reducers map[string]Reducer
	mu       sync.RWMutex

	// Event integration
	eventBus *realtime.EventBus

	// Statistics
	stats RegistryStats
}

type RegistryStats struct {
	TotalReducers      int64 `json:"total_reducers"`
	ExecutionCount     int64 `json:"execution_count"`
	TotalExecutionTime int64 `json:"total_execution_time_ns"`
	ErrorCount         int64 `json:"error_count"`
}

// NewReducerRegistry creates a new reducer registry
func NewReducerRegistry() *ReducerRegistry {
	return &ReducerRegistry{
		reducers: make(map[string]Reducer),
		eventBus: realtime.NewEventBus(),
	}
}

// Register adds a reducer to the registry
func (rr *ReducerRegistry) Register(reducer Reducer) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	name := reducer.Name()
	if _, exists := rr.reducers[name]; exists {
		return fmt.Errorf("reducer %s already registered", name)
	}

	rr.reducers[name] = reducer
	rr.stats.TotalReducers++

	// Emit registration event
	rr.eventBus.PublishEvent(&realtime.TableEvent{
		Type:      realtime.EventInsert,
		TableName: "reducers",
		Entity: map[string]interface{}{
			"name":        name,
			"type":        reducer.Type().String(),
			"description": reducer.Description(),
		},
	})

	return nil
}

// Get retrieves a reducer by name
func (rr *ReducerRegistry) Get(name string) (Reducer, bool) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	reducer, exists := rr.reducers[name]
	return reducer, exists
}

// List returns all registered reducers
func (rr *ReducerRegistry) List() []Reducer {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	reducers := make([]Reducer, 0, len(rr.reducers))
	for _, reducer := range rr.reducers {
		reducers = append(reducers, reducer)
	}
	return reducers
}

// Execute runs a reducer with the given arguments
func (rr *ReducerRegistry) Execute(ctx context.Context, name string, args []interface{}) (interface{}, error) {
	reducer, exists := rr.Get(name)
	if !exists {
		return nil, fmt.Errorf("reducer %s not found", name)
	}

	// Create reducer context
	reducerCtx := &ReducerContext{
		Database:    NewDatabaseContext(),
		Events:      rr.eventBus,
		ReducerName: name,
		CallTime:    time.Now(),
		Logger:      NewLogContext(),
		WasmModule:  NewWasmContext(),
		ctx:         ctx,
	}

	// Execute reducer
	start := time.Now()
	result, err := reducer.Execute(reducerCtx, args)
	duration := time.Since(start)

	// Update statistics
	rr.mu.Lock()
	rr.stats.ExecutionCount++
	rr.stats.TotalExecutionTime += duration.Nanoseconds()
	if err != nil {
		rr.stats.ErrorCount++
	}
	rr.mu.Unlock()

	return result, err
}

// Stats returns current registry statistics
func (rr *ReducerRegistry) Stats() RegistryStats {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.stats
}

// Context creation functions

func NewDatabaseContext() *DatabaseContext {
	return &DatabaseContext{
		tables: make(map[string]interface{}),
	}
}

func NewLogContext() *LogContext {
	return &LogContext{
		level:  LogLevelInfo,
		fields: make(map[string]interface{}),
	}
}

func NewWasmContext() *WasmContext {
	return &WasmContext{
		ModuleID: fmt.Sprintf("wasm_%d", time.Now().UnixNano()),
		MemoryAccess: &WasmMemory{
			Size: 1024 * 1024, // 1MB default
		},
		CallStack: make([]string, 0),
		Permissions: WasmPermissions{
			CanAccessDatabase: true,
			CanEmitEvents:     true,
			CanSchedule:       true,
			MaxMemory:         10 * 1024 * 1024, // 10MB max
		},
	}
}

// Random context methods
func (rc *RandomContext) Int63() int64 {
	if rc.source == nil {
		rc.source = newDeterministicRandom(rc.seed)
	}
	return rc.source.Int63()
}

func (rc *RandomContext) Float64() float64 {
	return float64(rc.Int63()) / (1 << 63)
}

// Log context methods
func (lc *LogContext) Debug(msg string, fields map[string]interface{}) {
	if lc.level <= LogLevelDebug {
		lc.log("DEBUG", msg, fields)
	}
}

func (lc *LogContext) Info(msg string, fields map[string]interface{}) {
	if lc.level <= LogLevelInfo {
		lc.log("INFO", msg, fields)
	}
}

func (lc *LogContext) Warn(msg string, fields map[string]interface{}) {
	if lc.level <= LogLevelWarn {
		lc.log("WARN", msg, fields)
	}
}

func (lc *LogContext) Error(msg string, fields map[string]interface{}) {
	if lc.level <= LogLevelError {
		lc.log("ERROR", msg, fields)
	}
}

func (lc *LogContext) log(level, msg string, fields map[string]interface{}) {
	lc.mu.RLock()
	allFields := make(map[string]interface{})
	for k, v := range lc.fields {
		allFields[k] = v
	}
	for k, v := range fields {
		allFields[k] = v
	}
	lc.mu.RUnlock()

	// In a real implementation, this would use a proper logging framework
	fmt.Printf("[%s] %s %+v\n", level, msg, allFields)
}

// Simple deterministic random implementation
type deterministicRandom struct {
	state uint64
}

func newDeterministicRandom(seed int64) *deterministicRandom {
	return &deterministicRandom{state: uint64(seed)}
}

func (dr *deterministicRandom) Int63() int64 {
	dr.state = dr.state*1103515245 + 12345
	return int64(dr.state >> 1)
}

// Global registry instance
var globalRegistry = NewReducerRegistry()

// Global functions for easy access
func Register(reducer Reducer) error {
	return globalRegistry.Register(reducer)
}

func Execute(ctx context.Context, name string, args ...interface{}) (interface{}, error) {
	return globalRegistry.Execute(ctx, name, args)
}

func GetRegistry() *ReducerRegistry {
	return globalRegistry
}

// Constructor functions for types expected by spacetimedb package

// NewSuccessResult creates a successful reducer result
func NewSuccessResult() ReducerResult {
	return ReducerResult{Success: true}
}

// NewSuccessResultWithMessage creates a successful reducer result with a message
func NewSuccessResultWithMessage(message string) ReducerResult {
	return ReducerResult{Success: true, Message: message}
}

// NewErrorResult creates an error reducer result
func NewErrorResult(err error) ReducerResult {
	return ReducerResult{Success: false, Error: err}
}

// NewErrorResultWithMessage creates an error reducer result with a message
func NewErrorResultWithMessage(message string, err error) ReducerResult {
	return ReducerResult{Success: false, Message: message, Error: err}
}

// NewGenericReducer creates a new generic reducer
func NewGenericReducer(name, description string, handler func(ctx *ReducerContext, args []byte) ReducerResult) *GenericReducer {
	return &GenericReducer{
		name:        name,
		description: description,
		handler:     handler,
	}
}

// Implement ReducerFunction interface for GenericReducer
func (gr *GenericReducer) Execute(ctx *ReducerContext, args []byte) ReducerResult {
	if gr.handler == nil {
		return NewErrorResult(fmt.Errorf("no handler defined"))
	}
	return gr.handler(ctx, args)
}

func (gr *GenericReducer) Name() string {
	return gr.name
}

func (gr *GenericReducer) Description() string {
	return gr.description
}

// NewGenericLifecycleFunction creates a new generic lifecycle function
func NewGenericLifecycleFunction(name string, lifecycleType LifecycleType, handler func(ctx *ReducerContext, lifecycleType LifecycleType) ReducerResult) *GenericLifecycleFunction {
	return &GenericLifecycleFunction{
		name:          name,
		lifecycleType: lifecycleType,
		handler:       handler,
	}
}

// Implement LifecycleFunction interface for GenericLifecycleFunction
func (glf *GenericLifecycleFunction) Execute(ctx *ReducerContext, lifecycleType LifecycleType) ReducerResult {
	if glf.handler == nil {
		return NewErrorResult(fmt.Errorf("no handler defined"))
	}
	return glf.handler(ctx, lifecycleType)
}

func (glf *GenericLifecycleFunction) Name() string {
	return glf.name
}

func (glf *GenericLifecycleFunction) Type() LifecycleType {
	return glf.lifecycleType
}

// NewReducerMetrics creates a new reducer metrics instance
func NewReducerMetrics(name string) *ReducerMetrics {
	return &ReducerMetrics{
		Name: name,
	}
}

// Global reducer registration functions
func RegisterReducer(reducer ReducerFunction) error {
	// This would integrate with a global reducer registry
	// For now, return nil
	return nil
}

func RegisterLifecycleFunction(fn LifecycleFunction) error {
	// This would integrate with a global lifecycle registry
	// For now, return nil
	return nil
}

func GetReducer(name string) (ReducerFunction, bool) {
	// This would retrieve from a global reducer registry
	// For now, return nil, false
	return nil, false
}

func GetLifecycleFunction(name string) (LifecycleFunction, bool) {
	// This would retrieve from a global lifecycle registry
	// For now, return nil, false
	return nil, false
}

// Database operations for ReducerContext

// Insert inserts a record into a table
func (rc *ReducerContext) Insert(data interface{}) error {
	// TODO: Implement actual database insertion
	// For now, store in the database context
	rc.Database.mu.Lock()
	defer rc.Database.mu.Unlock()

	// Get the table name from the struct type
	tableName := getTableName(data)
	if tableName == "" {
		return fmt.Errorf("unable to determine table name for type %T", data)
	}

	// Store in mock table for now
	if rc.Database.tables[tableName] == nil {
		rc.Database.tables[tableName] = make([]interface{}, 0)
	}

	if table, ok := rc.Database.tables[tableName].([]interface{}); ok {
		rc.Database.tables[tableName] = append(table, data)
	}

	return nil
}

// Iterator interface for database iteration
type Iterator interface {
	Next() bool
	Read(dest interface{}) error
	Close() error
	Err() error
}

// MockIterator provides a simple iterator implementation
type MockIterator struct {
	data    []interface{}
	index   int
	lastErr error
	closed  bool
}

// Iter creates an iterator for a table
func (rc *ReducerContext) Iter(tableName string) (Iterator, error) {
	rc.Database.mu.RLock()
	defer rc.Database.mu.RUnlock()

	tableData, exists := rc.Database.tables[tableName]
	if !exists {
		return &MockIterator{data: make([]interface{}, 0)}, nil
	}

	if data, ok := tableData.([]interface{}); ok {
		return &MockIterator{
			data:  data,
			index: -1,
		}, nil
	}

	return nil, fmt.Errorf("table %s has invalid data format", tableName)
}

// DeleteByID deletes a record from a table by ID
func (rc *ReducerContext) DeleteByID(tableName string, id interface{}) error {
	rc.Database.mu.Lock()
	defer rc.Database.mu.Unlock()

	tableData, exists := rc.Database.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	if data, ok := tableData.([]interface{}); ok {
		// Find and remove the item with matching ID
		for i, item := range data {
			if itemID := getIDFromStruct(item); itemID != nil && compareIDs(itemID, id) {
				// Remove item at index i
				rc.Database.tables[tableName] = append(data[:i], data[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("record with ID %v not found in table %s", id, tableName)
	}

	return fmt.Errorf("table %s has invalid data format", tableName)
}

// MockIterator implementation

func (mi *MockIterator) Next() bool {
	if mi.closed {
		return false
	}
	mi.index++
	return mi.index < len(mi.data)
}

func (mi *MockIterator) Read(dest interface{}) error {
	if mi.closed {
		return fmt.Errorf("iterator is closed")
	}
	if mi.index < 0 || mi.index >= len(mi.data) {
		return fmt.Errorf("iterator index out of bounds")
	}

	// Simple copy for demonstration - in real implementation would use reflection
	// For now, assume dest is compatible with data[index]
	if destPtr, ok := dest.(*interface{}); ok {
		*destPtr = mi.data[mi.index]
		return nil
	}

	// Try to copy the data using reflection
	return copyStruct(mi.data[mi.index], dest)
}

func (mi *MockIterator) Close() error {
	mi.closed = true
	return nil
}

func (mi *MockIterator) Err() error {
	return mi.lastErr
}

// Helper functions

func getTableName(data interface{}) string {
	// Extract table name from struct type
	switch data.(type) {
	case BsatnTestResult:
		return "bsatn_test_result"
	default:
		// Use type name as fallback
		return fmt.Sprintf("%T", data)
	}
}

func getIDFromStruct(item interface{}) interface{} {
	// Extract ID field from struct using reflection
	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	// Look for common ID field names
	for _, fieldName := range []string{"Id", "ID", "id"} {
		field := val.FieldByName(fieldName)
		if field.IsValid() && field.CanInterface() {
			return field.Interface()
		}
	}

	return nil
}

func compareIDs(id1, id2 interface{}) bool {
	return fmt.Sprintf("%v", id1) == fmt.Sprintf("%v", id2)
}

func copyStruct(src, dest interface{}) error {
	// Simple struct copying using reflection
	srcVal := reflect.ValueOf(src)
	destVal := reflect.ValueOf(dest)

	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destVal = destVal.Elem()

	if srcVal.Type() != destVal.Type() {
		return fmt.Errorf("source and destination types don't match: %v vs %v", srcVal.Type(), destVal.Type())
	}

	destVal.Set(srcVal)
	return nil
}

// BsatnTestResult struct definition for the helper functions above
type BsatnTestResult struct {
	Id        uint32  `json:"id" bsatn:"id"`
	TestName  string  `json:"testname" bsatn:"testname"`
	InputData string  `json:"inputdata" bsatn:"inputdata"`
	BsatnData []uint8 `json:"bsatndata" bsatn:"bsatndata"`
}
