// Package errors provides advanced error debugging utilities for SpacetimeDB Go bindings.
// This module implements developer productivity tools for error analysis, tracking,
// debugging, and troubleshooting in development and production environments.
package errors

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrorPattern represents a pattern of similar errors
type ErrorPattern struct {
	Pattern     string                 `json:"pattern"`
	Count       int                    `json:"count"`
	FirstSeen   time.Time              `json:"first_seen"`
	LastSeen    time.Time              `json:"last_seen"`
	Examples    []string               `json:"examples"`
	Metadata    map[string]interface{} `json:"metadata"`
	Severity    ErrorSeverity          `json:"severity"`
	Category    ErrorCategory          `json:"category"`
	Suggestions []string               `json:"suggestions"`
}

// ErrorSeverity represents error severity levels
type ErrorSeverity int

const (
	SeverityLow ErrorSeverity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

// String returns string representation of error severity
func (s ErrorSeverity) String() string {
	switch s {
	case SeverityLow:
		return "low"
	case SeverityMedium:
		return "medium"
	case SeverityHigh:
		return "high"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// ErrorCategory represents error categories
type ErrorCategory int

const (
	CategoryNetwork ErrorCategory = iota
	CategoryDatabase
	CategorySerialization
	CategoryValidation
	CategoryMemory
	CategoryAuthentication
	CategoryPermission
	CategoryTimeout
	CategoryResource
	CategoryUnknown
)

// String returns string representation of error category
func (c ErrorCategory) String() string {
	switch c {
	case CategoryNetwork:
		return "network"
	case CategoryDatabase:
		return "database"
	case CategorySerialization:
		return "serialization"
	case CategoryValidation:
		return "validation"
	case CategoryMemory:
		return "memory"
	case CategoryAuthentication:
		return "authentication"
	case CategoryPermission:
		return "permission"
	case CategoryTimeout:
		return "timeout"
	case CategoryResource:
		return "resource"
	default:
		return "unknown"
	}
}

// ErrorFrequencyTracker tracks error frequency and patterns
type ErrorFrequencyTracker struct {
	patterns       map[string]*ErrorPattern
	mutex          sync.RWMutex
	maxSize        int
	alertThreshold int
	alertCallback  func(*ErrorPattern)
}

// NewErrorFrequencyTracker creates a new error frequency tracker
func NewErrorFrequencyTracker(maxSize, alertThreshold int) *ErrorFrequencyTracker {
	return &ErrorFrequencyTracker{
		patterns:       make(map[string]*ErrorPattern),
		maxSize:        maxSize,
		alertThreshold: alertThreshold,
		alertCallback:  func(*ErrorPattern) {}, // Default no-op
	}
}

// SetAlertCallback sets the callback for high-frequency error alerts
func (eft *ErrorFrequencyTracker) SetAlertCallback(callback func(*ErrorPattern)) {
	eft.mutex.Lock()
	defer eft.mutex.Unlock()
	eft.alertCallback = callback
}

// RecordError records an error and tracks its pattern
func (eft *ErrorFrequencyTracker) RecordError(err error) {
	if err == nil {
		return
	}

	pattern := eft.extractPattern(err.Error())
	category := eft.categorizeError(err)
	severity := eft.assessSeverity(err, category)

	eft.mutex.Lock()
	defer eft.mutex.Unlock()

	now := time.Now()

	if existing, exists := eft.patterns[pattern]; exists {
		existing.Count++
		existing.LastSeen = now
		existing.Severity = severity // Update severity based on latest occurrence

		// Add example if we don't have too many
		if len(existing.Examples) < 5 {
			existing.Examples = append(existing.Examples, err.Error())
		}

		// Check for alert threshold
		if existing.Count >= eft.alertThreshold {
			go eft.alertCallback(existing)
		}
	} else {
		// Enforce max size
		if len(eft.patterns) >= eft.maxSize {
			eft.evictOldestPattern()
		}

		eft.patterns[pattern] = &ErrorPattern{
			Pattern:     pattern,
			Count:       1,
			FirstSeen:   now,
			LastSeen:    now,
			Examples:    []string{err.Error()},
			Metadata:    make(map[string]interface{}),
			Severity:    severity,
			Category:    category,
			Suggestions: eft.generateSuggestions(category, err),
		}
	}
}

// extractPattern extracts a pattern from error message
func (eft *ErrorFrequencyTracker) extractPattern(errMsg string) string {
	// Replace numbers with placeholders
	re1 := regexp.MustCompile(`\b\d+\b`)
	pattern := re1.ReplaceAllString(errMsg, "{number}")

	// Replace hex values with placeholders
	re2 := regexp.MustCompile(`\b0x[0-9a-fA-F]+\b`)
	pattern = re2.ReplaceAllString(pattern, "{hex}")

	// Replace quoted strings with placeholders
	re3 := regexp.MustCompile(`"[^"]*"`)
	pattern = re3.ReplaceAllString(pattern, "{string}")

	// Replace UUIDs with placeholders
	re4 := regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	pattern = re4.ReplaceAllString(pattern, "{uuid}")

	return pattern
}

// categorizeError categorizes error based on message content
func (eft *ErrorFrequencyTracker) categorizeError(err error) ErrorCategory {
	msg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(msg, "network") || strings.Contains(msg, "connection") || strings.Contains(msg, "dial"):
		return CategoryNetwork
	case strings.Contains(msg, "database") || strings.Contains(msg, "table") || strings.Contains(msg, "index"):
		return CategoryDatabase
	case strings.Contains(msg, "bsatn") || strings.Contains(msg, "decode") || strings.Contains(msg, "serialize"):
		return CategorySerialization
	case strings.Contains(msg, "validation") || strings.Contains(msg, "invalid") || strings.Contains(msg, "bounds"):
		return CategoryValidation
	case strings.Contains(msg, "memory") || strings.Contains(msg, "allocation") || strings.Contains(msg, "exhausted"):
		return CategoryMemory
	case strings.Contains(msg, "auth") || strings.Contains(msg, "credential"):
		return CategoryAuthentication
	case strings.Contains(msg, "permission") || strings.Contains(msg, "forbidden") || strings.Contains(msg, "unauthorized"):
		return CategoryPermission
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return CategoryTimeout
	case strings.Contains(msg, "resource") || strings.Contains(msg, "limit") || strings.Contains(msg, "quota"):
		return CategoryResource
	default:
		return CategoryUnknown
	}
}

// assessSeverity assesses error severity based on category and content
func (eft *ErrorFrequencyTracker) assessSeverity(err error, category ErrorCategory) ErrorSeverity {
	msg := strings.ToLower(err.Error())

	// Critical indicators
	if strings.Contains(msg, "panic") || strings.Contains(msg, "fatal") || strings.Contains(msg, "critical") {
		return SeverityCritical
	}

	// High severity by category
	switch category {
	case CategoryMemory, CategoryDatabase:
		return SeverityHigh
	case CategoryNetwork, CategoryTimeout:
		return SeverityMedium
	case CategoryValidation, CategorySerialization:
		return SeverityLow
	default:
		return SeverityMedium
	}
}

// generateSuggestions generates suggestions for fixing errors
func (eft *ErrorFrequencyTracker) generateSuggestions(category ErrorCategory, err error) []string {
	switch category {
	case CategoryNetwork:
		return []string{
			"Check network connectivity",
			"Verify service endpoints are accessible",
			"Review firewall and security group settings",
			"Consider implementing retry logic with exponential backoff",
		}
	case CategoryDatabase:
		return []string{
			"Verify table and index names are correct",
			"Check database connection parameters",
			"Review query syntax and constraints",
			"Consider adding proper error handling for database operations",
		}
	case CategorySerialization:
		return []string{
			"Verify BSATN schema compatibility",
			"Check data types match expected format",
			"Review serialization/deserialization logic",
			"Consider validating data before serialization",
		}
	case CategoryValidation:
		return []string{
			"Review input validation rules",
			"Check boundary conditions and edge cases",
			"Verify data types and formats",
			"Consider adding client-side validation",
		}
	case CategoryMemory:
		return []string{
			"Monitor memory usage patterns",
			"Review memory allocation and cleanup",
			"Consider implementing memory pooling",
			"Check for memory leaks in long-running operations",
		}
	case CategoryTimeout:
		return []string{
			"Review timeout configurations",
			"Consider increasing timeout values for slow operations",
			"Implement proper cancellation handling",
			"Add progress monitoring for long operations",
		}
	default:
		return []string{
			"Review error logs for more context",
			"Check system resources and configuration",
			"Consider adding more specific error handling",
		}
	}
}

// evictOldestPattern removes the oldest error pattern
func (eft *ErrorFrequencyTracker) evictOldestPattern() {
	var oldest string
	var oldestTime time.Time = time.Now()

	for pattern, info := range eft.patterns {
		if info.FirstSeen.Before(oldestTime) {
			oldest = pattern
			oldestTime = info.FirstSeen
		}
	}

	if oldest != "" {
		delete(eft.patterns, oldest)
	}
}

// GetTopPatterns returns the most frequent error patterns
func (eft *ErrorFrequencyTracker) GetTopPatterns(limit int) []*ErrorPattern {
	eft.mutex.RLock()
	defer eft.mutex.RUnlock()

	patterns := make([]*ErrorPattern, 0, len(eft.patterns))
	for _, pattern := range eft.patterns {
		patterns = append(patterns, pattern)
	}

	// Sort by count (descending)
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].Count > patterns[j].Count
	})

	if limit > 0 && limit < len(patterns) {
		patterns = patterns[:limit]
	}

	return patterns
}

// GetPatternsByCategory returns patterns filtered by category
func (eft *ErrorFrequencyTracker) GetPatternsByCategory(category ErrorCategory) []*ErrorPattern {
	eft.mutex.RLock()
	defer eft.mutex.RUnlock()

	var patterns []*ErrorPattern
	for _, pattern := range eft.patterns {
		if pattern.Category == category {
			patterns = append(patterns, pattern)
		}
	}

	return patterns
}

// GetStats returns tracker statistics
func (eft *ErrorFrequencyTracker) GetStats() map[string]interface{} {
	eft.mutex.RLock()
	defer eft.mutex.RUnlock()

	totalErrors := 0
	categoryCounts := make(map[string]int)
	severityCounts := make(map[string]int)

	for _, pattern := range eft.patterns {
		totalErrors += pattern.Count
		categoryCounts[pattern.Category.String()]++
		severityCounts[pattern.Severity.String()]++
	}

	return map[string]interface{}{
		"totalPatterns":  len(eft.patterns),
		"totalErrors":    totalErrors,
		"categoryCounts": categoryCounts,
		"severityCounts": severityCounts,
		"maxSize":        eft.maxSize,
		"alertThreshold": eft.alertThreshold,
	}
}

// StackTraceEnhancer enhances stack traces with additional context
type StackTraceEnhancer struct {
	sourceCache map[string][]string
	mutex       sync.RWMutex
}

// NewStackTraceEnhancer creates a new stack trace enhancer
func NewStackTraceEnhancer() *StackTraceEnhancer {
	return &StackTraceEnhancer{
		sourceCache: make(map[string][]string),
	}
}

// EnhancedStackTrace represents an enhanced stack trace
type EnhancedStackTrace struct {
	Frames []EnhancedFrame `json:"frames"`
	Error  string          `json:"error"`
}

// EnhancedFrame represents an enhanced stack frame
type EnhancedFrame struct {
	Function string            `json:"function"`
	File     string            `json:"file"`
	Line     int               `json:"line"`
	Context  map[string]string `json:"context"`
	Source   []string          `json:"source"`
}

// CaptureStackTrace captures and enhances the current stack trace
func (ste *StackTraceEnhancer) CaptureStackTrace(err error, skipFrames int) *EnhancedStackTrace {
	const maxFrames = 32
	pcs := make([]uintptr, maxFrames)
	n := runtime.Callers(skipFrames+2, pcs) // +2 to skip this function and runtime.Callers

	frames := runtime.CallersFrames(pcs[:n])
	enhancedFrames := make([]EnhancedFrame, 0, n)

	for {
		frame, more := frames.Next()

		enhancedFrame := EnhancedFrame{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
			Context:  make(map[string]string),
			Source:   ste.getSourceContext(frame.File, frame.Line),
		}

		// Add additional context
		enhancedFrame.Context["pc"] = fmt.Sprintf("0x%x", frame.PC)
		if frame.Entry != 0 {
			enhancedFrame.Context["entry"] = fmt.Sprintf("0x%x", frame.Entry)
		}

		enhancedFrames = append(enhancedFrames, enhancedFrame)

		if !more {
			break
		}
	}

	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	return &EnhancedStackTrace{
		Frames: enhancedFrames,
		Error:  errorMsg,
	}
}

// getSourceContext gets source code context around a line
func (ste *StackTraceEnhancer) getSourceContext(file string, line int) []string {
	// This is a simplified implementation
	// In a real implementation, you might want to read the actual source files
	// For now, return placeholder context
	return []string{
		fmt.Sprintf("// Line %d context not available", line-1),
		fmt.Sprintf("// Line %d (error location)", line),
		fmt.Sprintf("// Line %d context not available", line+1),
	}
}

// FormatStackTrace formats an enhanced stack trace for display
func (ste *StackTraceEnhancer) FormatStackTrace(trace *EnhancedStackTrace) string {
	var builder strings.Builder

	if trace.Error != "" {
		builder.WriteString(fmt.Sprintf("Error: %s\n\n", trace.Error))
	}

	builder.WriteString("Enhanced Stack Trace:\n")
	for i, frame := range trace.Frames {
		builder.WriteString(fmt.Sprintf("%d. %s\n", i+1, frame.Function))
		builder.WriteString(fmt.Sprintf("   %s:%d\n", frame.File, frame.Line))

		if len(frame.Source) > 0 {
			builder.WriteString("   Source context:\n")
			for _, line := range frame.Source {
				builder.WriteString(fmt.Sprintf("     %s\n", line))
			}
		}

		if len(frame.Context) > 0 {
			builder.WriteString("   Context:\n")
			for key, value := range frame.Context {
				builder.WriteString(fmt.Sprintf("     %s: %s\n", key, value))
			}
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

// ErrorReproductionHelper helps reproduce errors in development
type ErrorReproductionHelper struct {
	reproductionSteps map[string][]ReproductionStep
	mutex             sync.RWMutex
}

// ReproductionStep represents a step to reproduce an error
type ReproductionStep struct {
	Action         string                 `json:"action"`
	Parameters     map[string]interface{} `json:"parameters"`
	ExpectedResult string                 `json:"expected_result"`
	Timestamp      time.Time              `json:"timestamp"`
}

// NewErrorReproductionHelper creates a new error reproduction helper
func NewErrorReproductionHelper() *ErrorReproductionHelper {
	return &ErrorReproductionHelper{
		reproductionSteps: make(map[string][]ReproductionStep),
	}
}

// RecordStep records a reproduction step for an error
func (erh *ErrorReproductionHelper) RecordStep(errorPattern string, action string, params map[string]interface{}, expected string) {
	step := ReproductionStep{
		Action:         action,
		Parameters:     params,
		ExpectedResult: expected,
		Timestamp:      time.Now(),
	}

	erh.mutex.Lock()
	defer erh.mutex.Unlock()

	erh.reproductionSteps[errorPattern] = append(erh.reproductionSteps[errorPattern], step)
}

// GetReproductionSteps returns reproduction steps for an error pattern
func (erh *ErrorReproductionHelper) GetReproductionSteps(errorPattern string) []ReproductionStep {
	erh.mutex.RLock()
	defer erh.mutex.RUnlock()

	steps, exists := erh.reproductionSteps[errorPattern]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	result := make([]ReproductionStep, len(steps))
	copy(result, steps)
	return result
}

// GenerateReproductionGuide generates a reproduction guide for an error pattern
func (erh *ErrorReproductionHelper) GenerateReproductionGuide(errorPattern string) string {
	steps := erh.GetReproductionSteps(errorPattern)
	if len(steps) == 0 {
		return "No reproduction steps available for this error pattern."
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Reproduction Guide for: %s\n\n", errorPattern))

	for i, step := range steps {
		builder.WriteString(fmt.Sprintf("Step %d: %s\n", i+1, step.Action))

		if len(step.Parameters) > 0 {
			builder.WriteString("Parameters:\n")
			for key, value := range step.Parameters {
				builder.WriteString(fmt.Sprintf("  %s: %v\n", key, value))
			}
		}

		if step.ExpectedResult != "" {
			builder.WriteString(fmt.Sprintf("Expected: %s\n", step.ExpectedResult))
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

// InteractiveDebugger provides interactive debugging capabilities
type InteractiveDebugger struct {
	breakpoints map[string]bool
	watchlist   map[string]interface{}
	mutex       sync.RWMutex
	logger      *log.Logger
}

// NewInteractiveDebugger creates a new interactive debugger
func NewInteractiveDebugger(logger *log.Logger) *InteractiveDebugger {
	if logger == nil {
		logger = log.Default()
	}

	return &InteractiveDebugger{
		breakpoints: make(map[string]bool),
		watchlist:   make(map[string]interface{}),
		logger:      logger,
	}
}

// SetBreakpoint sets a breakpoint at a specific location
func (id *InteractiveDebugger) SetBreakpoint(location string) {
	id.mutex.Lock()
	defer id.mutex.Unlock()
	id.breakpoints[location] = true
	id.logger.Printf("Breakpoint set at: %s", location)
}

// RemoveBreakpoint removes a breakpoint
func (id *InteractiveDebugger) RemoveBreakpoint(location string) {
	id.mutex.Lock()
	defer id.mutex.Unlock()
	delete(id.breakpoints, location)
	id.logger.Printf("Breakpoint removed from: %s", location)
}

// CheckBreakpoint checks if a breakpoint is set at a location
func (id *InteractiveDebugger) CheckBreakpoint(location string) bool {
	id.mutex.RLock()
	defer id.mutex.RUnlock()
	return id.breakpoints[location]
}

// AddWatch adds a variable to the watch list
func (id *InteractiveDebugger) AddWatch(name string, value interface{}) {
	id.mutex.Lock()
	defer id.mutex.Unlock()
	id.watchlist[name] = value
	id.logger.Printf("Added to watchlist: %s = %v", name, value)
}

// GetWatchlist returns the current watch list
func (id *InteractiveDebugger) GetWatchlist() map[string]interface{} {
	id.mutex.RLock()
	defer id.mutex.RUnlock()

	result := make(map[string]interface{})
	for name, value := range id.watchlist {
		result[name] = value
	}
	return result
}

// DumpDebugInfo dumps current debugging information
func (id *InteractiveDebugger) DumpDebugInfo() string {
	id.mutex.RLock()
	defer id.mutex.RUnlock()

	info := map[string]interface{}{
		"breakpoints": id.breakpoints,
		"watchlist":   id.watchlist,
		"timestamp":   time.Now(),
	}

	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error marshaling debug info: %v", err)
	}

	return string(data)
}
