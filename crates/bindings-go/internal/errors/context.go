// Package errors provides advanced context management for SpacetimeDB Go bindings.
// This module implements enterprise-grade error context including distributed tracing,
// request correlation, performance metrics, and multi-tenant error isolation.
package errors

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"runtime"
	"sync"
	"time"
)

// ContextKey represents keys for context values
type ContextKey string

const (
	// Context keys for error tracking
	ContextKeyRequestID   ContextKey = "request_id"
	ContextKeyUserID      ContextKey = "user_id"
	ContextKeySessionID   ContextKey = "session_id"
	ContextKeyTenantID    ContextKey = "tenant_id"
	ContextKeyTraceID     ContextKey = "trace_id"
	ContextKeySpanID      ContextKey = "span_id"
	ContextKeyOperationID ContextKey = "operation_id"
	ContextKeyComponent   ContextKey = "component"
	ContextKeyStartTime   ContextKey = "start_time"
	ContextKeyMetrics     ContextKey = "metrics"
)

// DistributedTraceContext represents distributed tracing context
type DistributedTraceContext struct {
	TraceID       string                 `json:"trace_id"`
	SpanID        string                 `json:"span_id"`
	ParentSpanID  string                 `json:"parent_span_id,omitempty"`
	OperationName string                 `json:"operation_name"`
	StartTime     time.Time              `json:"start_time"`
	EndTime       *time.Time             `json:"end_time,omitempty"`
	Duration      *time.Duration         `json:"duration,omitempty"`
	Tags          map[string]interface{} `json:"tags"`
	Logs          []TraceLog             `json:"logs"`
	BaggageItems  map[string]string      `json:"baggage_items"`
	mutex         sync.RWMutex
}

// TraceLog represents a log entry in a trace
type TraceLog struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields"`
}

// NewDistributedTraceContext creates a new distributed trace context
func NewDistributedTraceContext(operationName string) *DistributedTraceContext {
	return &DistributedTraceContext{
		TraceID:       generateID(),
		SpanID:        generateID(),
		OperationName: operationName,
		StartTime:     time.Now(),
		Tags:          make(map[string]interface{}),
		Logs:          make([]TraceLog, 0),
		BaggageItems:  make(map[string]string),
	}
}

// CreateChildSpan creates a child span from this context
func (dtc *DistributedTraceContext) CreateChildSpan(operationName string) *DistributedTraceContext {
	child := &DistributedTraceContext{
		TraceID:       dtc.TraceID,
		SpanID:        generateID(),
		ParentSpanID:  dtc.SpanID,
		OperationName: operationName,
		StartTime:     time.Now(),
		Tags:          make(map[string]interface{}),
		Logs:          make([]TraceLog, 0),
		BaggageItems:  make(map[string]string),
	}

	// Copy baggage items to child
	dtc.mutex.RLock()
	for key, value := range dtc.BaggageItems {
		child.BaggageItems[key] = value
	}
	dtc.mutex.RUnlock()

	return child
}

// SetTag sets a tag on the trace
func (dtc *DistributedTraceContext) SetTag(key string, value interface{}) {
	dtc.mutex.Lock()
	defer dtc.mutex.Unlock()
	dtc.Tags[key] = value
}

// GetTag gets a tag from the trace
func (dtc *DistributedTraceContext) GetTag(key string) (interface{}, bool) {
	dtc.mutex.RLock()
	defer dtc.mutex.RUnlock()
	value, exists := dtc.Tags[key]
	return value, exists
}

// SetBaggageItem sets a baggage item (propagates to child spans)
func (dtc *DistributedTraceContext) SetBaggageItem(key, value string) {
	dtc.mutex.Lock()
	defer dtc.mutex.Unlock()
	dtc.BaggageItems[key] = value
}

// GetBaggageItem gets a baggage item
func (dtc *DistributedTraceContext) GetBaggageItem(key string) (string, bool) {
	dtc.mutex.RLock()
	defer dtc.mutex.RUnlock()
	value, exists := dtc.BaggageItems[key]
	return value, exists
}

// LogEvent logs an event in the trace
func (dtc *DistributedTraceContext) LogEvent(level, message string, fields map[string]interface{}) {
	log := TraceLog{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Fields:    fields,
	}

	dtc.mutex.Lock()
	defer dtc.mutex.Unlock()
	dtc.Logs = append(dtc.Logs, log)
}

// Finish marks the span as completed
func (dtc *DistributedTraceContext) Finish() {
	now := time.Now()
	duration := now.Sub(dtc.StartTime)

	dtc.mutex.Lock()
	defer dtc.mutex.Unlock()
	dtc.EndTime = &now
	dtc.Duration = &duration
}

// IsFinished returns true if the span is finished
func (dtc *DistributedTraceContext) IsFinished() bool {
	dtc.mutex.RLock()
	defer dtc.mutex.RUnlock()
	return dtc.EndTime != nil
}

// GetDuration returns the span duration (if finished)
func (dtc *DistributedTraceContext) GetDuration() *time.Duration {
	dtc.mutex.RLock()
	defer dtc.mutex.RUnlock()
	return dtc.Duration
}

// RequestCorrelationContext manages request correlation across services
type RequestCorrelationContext struct {
	RequestID   string                 `json:"request_id"`
	UserID      string                 `json:"user_id,omitempty"`
	SessionID   string                 `json:"session_id,omitempty"`
	TenantID    string                 `json:"tenant_id,omitempty"`
	Component   string                 `json:"component"`
	StartTime   time.Time              `json:"start_time"`
	Metadata    map[string]interface{} `json:"metadata"`
	Breadcrumbs []Breadcrumb           `json:"breadcrumbs"`
	mutex       sync.RWMutex
}

// Breadcrumb represents a breadcrumb in the request path
type Breadcrumb struct {
	Component string                 `json:"component"`
	Action    string                 `json:"action"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// NewRequestCorrelationContext creates a new request correlation context
func NewRequestCorrelationContext(component string) *RequestCorrelationContext {
	return &RequestCorrelationContext{
		RequestID:   generateID(),
		Component:   component,
		StartTime:   time.Now(),
		Metadata:    make(map[string]interface{}),
		Breadcrumbs: make([]Breadcrumb, 0),
	}
}

// WithUser sets user information
func (rcc *RequestCorrelationContext) WithUser(userID string) *RequestCorrelationContext {
	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	rcc.UserID = userID
	return rcc
}

// WithSession sets session information
func (rcc *RequestCorrelationContext) WithSession(sessionID string) *RequestCorrelationContext {
	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	rcc.SessionID = sessionID
	return rcc
}

// WithTenant sets tenant information
func (rcc *RequestCorrelationContext) WithTenant(tenantID string) *RequestCorrelationContext {
	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	rcc.TenantID = tenantID
	return rcc
}

// SetMetadata sets metadata
func (rcc *RequestCorrelationContext) SetMetadata(key string, value interface{}) {
	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	rcc.Metadata[key] = value
}

// GetMetadata gets metadata
func (rcc *RequestCorrelationContext) GetMetadata(key string) (interface{}, bool) {
	rcc.mutex.RLock()
	defer rcc.mutex.RUnlock()
	value, exists := rcc.Metadata[key]
	return value, exists
}

// AddBreadcrumb adds a breadcrumb to the request path
func (rcc *RequestCorrelationContext) AddBreadcrumb(component, action string, data map[string]interface{}) {
	breadcrumb := Breadcrumb{
		Component: component,
		Action:    action,
		Timestamp: time.Now(),
		Data:      data,
	}

	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	rcc.Breadcrumbs = append(rcc.Breadcrumbs, breadcrumb)
}

// GetBreadcrumbs returns all breadcrumbs
func (rcc *RequestCorrelationContext) GetBreadcrumbs() []Breadcrumb {
	rcc.mutex.RLock()
	defer rcc.mutex.RUnlock()

	result := make([]Breadcrumb, len(rcc.Breadcrumbs))
	copy(result, rcc.Breadcrumbs)
	return result
}

// PerformanceMetricsContext tracks performance metrics for error analysis
type PerformanceMetricsContext struct {
	Counters   map[string]int64           `json:"counters"`
	Gauges     map[string]float64         `json:"gauges"`
	Histograms map[string][]float64       `json:"histograms"`
	Timers     map[string][]time.Duration `json:"timers"`
	StartTime  time.Time                  `json:"start_time"`
	mutex      sync.RWMutex
}

// NewPerformanceMetricsContext creates a new performance metrics context
func NewPerformanceMetricsContext() *PerformanceMetricsContext {
	return &PerformanceMetricsContext{
		Counters:   make(map[string]int64),
		Gauges:     make(map[string]float64),
		Histograms: make(map[string][]float64),
		Timers:     make(map[string][]time.Duration),
		StartTime:  time.Now(),
	}
}

// IncrementCounter increments a counter metric
func (pmc *PerformanceMetricsContext) IncrementCounter(name string, delta int64) {
	pmc.mutex.Lock()
	defer pmc.mutex.Unlock()
	pmc.Counters[name] += delta
}

// SetGauge sets a gauge metric
func (pmc *PerformanceMetricsContext) SetGauge(name string, value float64) {
	pmc.mutex.Lock()
	defer pmc.mutex.Unlock()
	pmc.Gauges[name] = value
}

// RecordHistogram records a histogram value
func (pmc *PerformanceMetricsContext) RecordHistogram(name string, value float64) {
	pmc.mutex.Lock()
	defer pmc.mutex.Unlock()
	pmc.Histograms[name] = append(pmc.Histograms[name], value)
}

// RecordTimer records a timer value
func (pmc *PerformanceMetricsContext) RecordTimer(name string, duration time.Duration) {
	pmc.mutex.Lock()
	defer pmc.mutex.Unlock()
	pmc.Timers[name] = append(pmc.Timers[name], duration)
}

// GetMetrics returns a snapshot of all metrics
func (pmc *PerformanceMetricsContext) GetMetrics() map[string]interface{} {
	pmc.mutex.RLock()
	defer pmc.mutex.RUnlock()

	return map[string]interface{}{
		"counters":   copyInt64Map(pmc.Counters),
		"gauges":     copyFloat64Map(pmc.Gauges),
		"histograms": copyFloat64SliceMap(pmc.Histograms),
		"timers":     copyDurationSliceMap(pmc.Timers),
		"startTime":  pmc.StartTime,
		"uptime":     time.Since(pmc.StartTime),
	}
}

// MultiTenantErrorContext provides multi-tenant error isolation
type MultiTenantErrorContext struct {
	TenantID      string                 `json:"tenant_id"`
	Isolation     TenantIsolationLevel   `json:"isolation"`
	Quotas        map[string]int64       `json:"quotas"`
	Usage         map[string]int64       `json:"usage"`
	Permissions   []string               `json:"permissions"`
	Configuration map[string]interface{} `json:"configuration"`
	mutex         sync.RWMutex
}

// TenantIsolationLevel represents tenant isolation levels
type TenantIsolationLevel int

const (
	IsolationShared TenantIsolationLevel = iota
	IsolationDedicated
	IsolationEncrypted
)

// String returns string representation of isolation level
func (til TenantIsolationLevel) String() string {
	switch til {
	case IsolationShared:
		return "shared"
	case IsolationDedicated:
		return "dedicated"
	case IsolationEncrypted:
		return "encrypted"
	default:
		return "unknown"
	}
}

// NewMultiTenantErrorContext creates a new multi-tenant error context
func NewMultiTenantErrorContext(tenantID string, isolation TenantIsolationLevel) *MultiTenantErrorContext {
	return &MultiTenantErrorContext{
		TenantID:      tenantID,
		Isolation:     isolation,
		Quotas:        make(map[string]int64),
		Usage:         make(map[string]int64),
		Permissions:   make([]string, 0),
		Configuration: make(map[string]interface{}),
	}
}

// SetQuota sets a resource quota for the tenant
func (mtec *MultiTenantErrorContext) SetQuota(resource string, limit int64) {
	mtec.mutex.Lock()
	defer mtec.mutex.Unlock()
	mtec.Quotas[resource] = limit
}

// IncrementUsage increments resource usage
func (mtec *MultiTenantErrorContext) IncrementUsage(resource string, delta int64) bool {
	mtec.mutex.Lock()
	defer mtec.mutex.Unlock()

	currentUsage := mtec.Usage[resource]
	quota, hasQuota := mtec.Quotas[resource]

	if hasQuota && currentUsage+delta > quota {
		return false // Quota exceeded
	}

	mtec.Usage[resource] = currentUsage + delta
	return true
}

// CheckQuota checks if a resource operation would exceed quota
func (mtec *MultiTenantErrorContext) CheckQuota(resource string, amount int64) bool {
	mtec.mutex.RLock()
	defer mtec.mutex.RUnlock()

	currentUsage := mtec.Usage[resource]
	quota, hasQuota := mtec.Quotas[resource]

	if !hasQuota {
		return true // No quota limit
	}

	return currentUsage+amount <= quota
}

// HasPermission checks if tenant has a specific permission
func (mtec *MultiTenantErrorContext) HasPermission(permission string) bool {
	mtec.mutex.RLock()
	defer mtec.mutex.RUnlock()

	for _, p := range mtec.Permissions {
		if p == permission {
			return true
		}
	}
	return false
}

// AddPermission adds a permission to the tenant
func (mtec *MultiTenantErrorContext) AddPermission(permission string) {
	mtec.mutex.Lock()
	defer mtec.mutex.Unlock()

	// Check if permission already exists
	for _, p := range mtec.Permissions {
		if p == permission {
			return
		}
	}

	mtec.Permissions = append(mtec.Permissions, permission)
}

// GetQuotaUsage returns quota usage information
func (mtec *MultiTenantErrorContext) GetQuotaUsage() map[string]map[string]int64 {
	mtec.mutex.RLock()
	defer mtec.mutex.RUnlock()

	result := make(map[string]map[string]int64)
	for resource, quota := range mtec.Quotas {
		usage := mtec.Usage[resource]
		result[resource] = map[string]int64{
			"quota":     quota,
			"usage":     usage,
			"remaining": quota - usage,
		}
	}

	return result
}

// EnhancedErrorContextManager manages enhanced error context
type EnhancedErrorContextManager struct {
	contextStore map[string]*EnhancedErrorContext
	mutex        sync.RWMutex
}

// EnhancedErrorContext represents enhanced error context
type EnhancedErrorContext struct {
	RequestContext     *RequestCorrelationContext `json:"request_context,omitempty"`
	TraceContext       *DistributedTraceContext   `json:"trace_context,omitempty"`
	MetricsContext     *PerformanceMetricsContext `json:"metrics_context,omitempty"`
	MultiTenantContext *MultiTenantErrorContext   `json:"multi_tenant_context,omitempty"`
	SystemContext      map[string]interface{}     `json:"system_context"`
	CreatedAt          time.Time                  `json:"created_at"`
}

// NewEnhancedErrorContextManager creates a new enhanced error context manager
func NewEnhancedErrorContextManager() *EnhancedErrorContextManager {
	return &EnhancedErrorContextManager{
		contextStore: make(map[string]*EnhancedErrorContext),
	}
}

// CreateContext creates a new enhanced error context
func (eecm *EnhancedErrorContextManager) CreateContext(contextID string) *EnhancedErrorContext {
	context := &EnhancedErrorContext{
		SystemContext: make(map[string]interface{}),
		CreatedAt:     time.Now(),
	}

	// Add system information
	context.SystemContext["go_version"] = runtime.Version()
	context.SystemContext["goos"] = runtime.GOOS
	context.SystemContext["goarch"] = runtime.GOARCH
	context.SystemContext["num_cpu"] = runtime.NumCPU()
	context.SystemContext["goroutines"] = runtime.NumGoroutine()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	context.SystemContext["memory"] = map[string]interface{}{
		"alloc":       memStats.Alloc,
		"total_alloc": memStats.TotalAlloc,
		"sys":         memStats.Sys,
		"num_gc":      memStats.NumGC,
	}

	eecm.mutex.Lock()
	defer eecm.mutex.Unlock()
	eecm.contextStore[contextID] = context

	return context
}

// GetContext retrieves an enhanced error context
func (eecm *EnhancedErrorContextManager) GetContext(contextID string) (*EnhancedErrorContext, bool) {
	eecm.mutex.RLock()
	defer eecm.mutex.RUnlock()
	context, exists := eecm.contextStore[contextID]
	return context, exists
}

// UpdateContext updates an enhanced error context
func (eecm *EnhancedErrorContextManager) UpdateContext(contextID string, updater func(*EnhancedErrorContext)) bool {
	eecm.mutex.Lock()
	defer eecm.mutex.Unlock()

	if context, exists := eecm.contextStore[contextID]; exists {
		updater(context)
		return true
	}
	return false
}

// RemoveContext removes an enhanced error context
func (eecm *EnhancedErrorContextManager) RemoveContext(contextID string) {
	eecm.mutex.Lock()
	defer eecm.mutex.Unlock()
	delete(eecm.contextStore, contextID)
}

// WithEnhancedContext adds enhanced error context to a Go context
func WithEnhancedContext(ctx context.Context, enhanced *EnhancedErrorContext) context.Context {
	if enhanced.RequestContext != nil {
		ctx = context.WithValue(ctx, ContextKeyRequestID, enhanced.RequestContext.RequestID)
		ctx = context.WithValue(ctx, ContextKeyUserID, enhanced.RequestContext.UserID)
		ctx = context.WithValue(ctx, ContextKeySessionID, enhanced.RequestContext.SessionID)
		ctx = context.WithValue(ctx, ContextKeyTenantID, enhanced.RequestContext.TenantID)
	}

	if enhanced.TraceContext != nil {
		ctx = context.WithValue(ctx, ContextKeyTraceID, enhanced.TraceContext.TraceID)
		ctx = context.WithValue(ctx, ContextKeySpanID, enhanced.TraceContext.SpanID)
	}

	if enhanced.MetricsContext != nil {
		ctx = context.WithValue(ctx, ContextKeyMetrics, enhanced.MetricsContext)
		ctx = context.WithValue(ctx, ContextKeyStartTime, enhanced.MetricsContext.StartTime)
	}

	return ctx
}

// Helper functions

// generateID generates a random ID
func generateID() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// copyInt64Map creates a copy of an int64 map
func copyInt64Map(original map[string]int64) map[string]int64 {
	copy := make(map[string]int64)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

// copyFloat64Map creates a copy of a float64 map
func copyFloat64Map(original map[string]float64) map[string]float64 {
	copy := make(map[string]float64)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

// copyFloat64SliceMap creates a copy of a float64 slice map
func copyFloat64SliceMap(original map[string][]float64) map[string][]float64 {
	result := make(map[string][]float64)
	for k, v := range original {
		slice := make([]float64, len(v))
		copy(slice, v)
		result[k] = slice
	}
	return result
}

// copyDurationSliceMap creates a copy of a duration slice map
func copyDurationSliceMap(original map[string][]time.Duration) map[string][]time.Duration {
	result := make(map[string][]time.Duration)
	for k, v := range original {
		slice := make([]time.Duration, len(v))
		copy(slice, v)
		result[k] = slice
	}
	return result
}
