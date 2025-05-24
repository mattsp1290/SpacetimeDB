// Package errors provides advanced error recovery mechanisms for SpacetimeDB Go bindings.
// This module implements enterprise-grade error recovery patterns including circuit breakers,
// graceful degradation, automatic failover, and health monitoring integration.
package errors

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int32

const (
	// Closed - circuit is functioning normally
	Closed CircuitBreakerState = iota
	// Open - circuit is open, failing fast
	Open
	// HalfOpen - circuit is testing if service has recovered
	HalfOpen
)

// String returns string representation of circuit breaker state
func (s CircuitBreakerState) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig configures circuit breaker behavior
type CircuitBreakerConfig struct {
	// MaxFailures before opening circuit
	MaxFailures int
	// ResetTimeout before attempting half-open
	ResetTimeout time.Duration
	// SuccessThreshold for closing circuit from half-open
	SuccessThreshold int
	// OnStateChange callback when state changes
	OnStateChange func(from, to CircuitBreakerState)
	// IsRetriableError determines if error should count as failure
	IsRetriableError func(error) bool
}

// DefaultCircuitBreakerConfig returns sensible defaults
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		SuccessThreshold: 3,
		OnStateChange:    func(from, to CircuitBreakerState) {},
		IsRetriableError: func(err error) bool { return true },
	}
}

// CircuitBreaker implements the circuit breaker pattern for error recovery
type CircuitBreaker struct {
	config       *CircuitBreakerConfig
	state        int32 // CircuitBreakerState
	failures     int32
	successes    int32
	lastFailTime time.Time
	mutex        sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker with given config
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	return &CircuitBreaker{
		config: config,
		state:  int32(Closed),
	}
}

// Execute runs the operation with circuit breaker protection
func (cb *CircuitBreaker) Execute(operation func() error) error {
	// Check if circuit allows execution
	if err := cb.allowRequest(); err != nil {
		return err
	}

	// Execute operation
	err := operation()

	// Record result
	cb.recordResult(err)

	return err
}

// allowRequest checks if the circuit breaker allows the request
func (cb *CircuitBreaker) allowRequest() error {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	switch state {
	case Closed:
		return nil
	case Open:
		cb.mutex.RLock()
		lastFailTime := cb.lastFailTime
		cb.mutex.RUnlock()

		if time.Since(lastFailTime) > cb.config.ResetTimeout {
			// Transition to half-open
			cb.setState(HalfOpen)
			return nil
		}
		return fmt.Errorf("circuit breaker is open")
	case HalfOpen:
		return nil
	default:
		return fmt.Errorf("unknown circuit breaker state: %v", state)
	}
}

// recordResult records the result of an operation execution
func (cb *CircuitBreaker) recordResult(err error) {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	if err != nil && cb.config.IsRetriableError(err) {
		cb.recordFailure()
	} else if err == nil {
		cb.recordSuccess()
	}

	// State transition logic
	switch state {
	case Closed:
		if atomic.LoadInt32(&cb.failures) >= int32(cb.config.MaxFailures) {
			cb.setState(Open)
		}
	case HalfOpen:
		if err != nil && cb.config.IsRetriableError(err) {
			cb.setState(Open)
		} else if atomic.LoadInt32(&cb.successes) >= int32(cb.config.SuccessThreshold) {
			cb.setState(Closed)
		}
	}
}

// recordFailure increments failure count
func (cb *CircuitBreaker) recordFailure() {
	atomic.AddInt32(&cb.failures, 1)
	cb.mutex.Lock()
	cb.lastFailTime = time.Now()
	cb.mutex.Unlock()
}

// recordSuccess increments success count and may reset failures
func (cb *CircuitBreaker) recordSuccess() {
	atomic.AddInt32(&cb.successes, 1)
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))
	if state == Closed {
		atomic.StoreInt32(&cb.failures, 0)
	}
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(newState CircuitBreakerState) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	oldState := CircuitBreakerState(atomic.LoadInt32(&cb.state))
	if oldState == newState {
		return
	}

	atomic.StoreInt32(&cb.state, int32(newState))

	// Reset counters on state change
	switch newState {
	case Open:
		atomic.StoreInt32(&cb.successes, 0)
	case Closed:
		atomic.StoreInt32(&cb.failures, 0)
		atomic.StoreInt32(&cb.successes, 0)
	case HalfOpen:
		atomic.StoreInt32(&cb.successes, 0)
	}

	cb.config.OnStateChange(oldState, newState)
}

// GetState returns current circuit breaker state
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&cb.state))
}

// GetStats returns current circuit breaker statistics
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	return map[string]interface{}{
		"state":        cb.GetState().String(),
		"failures":     atomic.LoadInt32(&cb.failures),
		"successes":    atomic.LoadInt32(&cb.successes),
		"lastFailTime": cb.lastFailTime,
	}
}

// GracefulDegradationPolicy defines how to handle service degradation
type GracefulDegradationPolicy struct {
	// FallbackFunction called when primary operation fails
	FallbackFunction func(error) (interface{}, error)
	// MaxDegradationLevel maximum level of degradation allowed (0-10)
	MaxDegradationLevel int
	// CurrentLevel tracks current degradation level
	CurrentLevel int32
	// RecoveryThreshold successes needed to reduce degradation level
	RecoveryThreshold int
	// DegradationTimeout how long to wait before increasing degradation
	DegradationTimeout time.Duration
	// SuccessCount tracks consecutive successes
	successCount int32
	// LastDegradation tracks when degradation last increased
	lastDegradation time.Time
	mutex           sync.RWMutex
}

// NewGracefulDegradationPolicy creates a new graceful degradation policy
func NewGracefulDegradationPolicy() *GracefulDegradationPolicy {
	return &GracefulDegradationPolicy{
		MaxDegradationLevel: 10,
		RecoveryThreshold:   5,
		DegradationTimeout:  5 * time.Minute,
	}
}

// ExecuteWithDegradation executes operation with graceful degradation
func (gdp *GracefulDegradationPolicy) ExecuteWithDegradation(operation func() (interface{}, error)) (interface{}, error) {
	result, err := operation()

	if err != nil {
		gdp.handleFailure()
		if gdp.FallbackFunction != nil {
			return gdp.FallbackFunction(err)
		}
		return nil, err
	}

	gdp.handleSuccess()
	return result, nil
}

// handleFailure handles operation failure
func (gdp *GracefulDegradationPolicy) handleFailure() {
	gdp.mutex.Lock()
	defer gdp.mutex.Unlock()

	// Reset success count
	atomic.StoreInt32(&gdp.successCount, 0)

	// Increase degradation level if timeout has passed
	if time.Since(gdp.lastDegradation) > gdp.DegradationTimeout {
		currentLevel := atomic.LoadInt32(&gdp.CurrentLevel)
		if currentLevel < int32(gdp.MaxDegradationLevel) {
			atomic.AddInt32(&gdp.CurrentLevel, 1)
			gdp.lastDegradation = time.Now()
		}
	}
}

// handleSuccess handles operation success
func (gdp *GracefulDegradationPolicy) handleSuccess() {
	successCount := atomic.AddInt32(&gdp.successCount, 1)

	// Reduce degradation level if recovery threshold met
	if successCount >= int32(gdp.RecoveryThreshold) {
		gdp.mutex.Lock()
		currentLevel := atomic.LoadInt32(&gdp.CurrentLevel)
		if currentLevel > 0 {
			atomic.AddInt32(&gdp.CurrentLevel, -1)
			atomic.StoreInt32(&gdp.successCount, 0)
		}
		gdp.mutex.Unlock()
	}
}

// GetDegradationLevel returns current degradation level
func (gdp *GracefulDegradationPolicy) GetDegradationLevel() int {
	return int(atomic.LoadInt32(&gdp.CurrentLevel))
}

// HealthChecker monitors system health for error recovery
type HealthChecker struct {
	checks   map[string]HealthCheck
	mutex    sync.RWMutex
	interval time.Duration
	timeout  time.Duration
	results  map[string]*HealthResult
}

// HealthCheck represents a health check function
type HealthCheck func(context.Context) error

// HealthResult represents the result of a health check
type HealthResult struct {
	Healthy   bool
	LastCheck time.Time
	Error     error
	Duration  time.Duration
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		checks:   make(map[string]HealthCheck),
		results:  make(map[string]*HealthResult),
		interval: interval,
		timeout:  timeout,
	}
}

// AddCheck adds a health check
func (hc *HealthChecker) AddCheck(name string, check HealthCheck) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	hc.checks[name] = check
}

// RemoveCheck removes a health check
func (hc *HealthChecker) RemoveCheck(name string) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	delete(hc.checks, name)
	delete(hc.results, name)
}

// RunChecks runs all health checks
func (hc *HealthChecker) RunChecks(ctx context.Context) map[string]*HealthResult {
	hc.mutex.RLock()
	checks := make(map[string]HealthCheck)
	for name, check := range hc.checks {
		checks[name] = check
	}
	hc.mutex.RUnlock()

	results := make(map[string]*HealthResult)
	var wg sync.WaitGroup

	for name, check := range checks {
		wg.Add(1)
		go func(name string, check HealthCheck) {
			defer wg.Done()

			checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
			defer cancel()

			start := time.Now()
			err := check(checkCtx)
			duration := time.Since(start)

			result := &HealthResult{
				Healthy:   err == nil,
				LastCheck: start,
				Error:     err,
				Duration:  duration,
			}

			hc.mutex.Lock()
			hc.results[name] = result
			hc.mutex.Unlock()

			results[name] = result
		}(name, check)
	}

	wg.Wait()
	return results
}

// GetResults returns latest health check results
func (hc *HealthChecker) GetResults() map[string]*HealthResult {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	results := make(map[string]*HealthResult)
	for name, result := range hc.results {
		results[name] = result
	}
	return results
}

// IsHealthy returns true if all checks are healthy
func (hc *HealthChecker) IsHealthy() bool {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	for _, result := range hc.results {
		if !result.Healthy {
			return false
		}
	}
	return true
}

// FailoverManager manages automatic failover for error recovery
type FailoverManager struct {
	primary      func() error
	secondary    func() error
	detector     func() bool // returns true if primary is healthy
	mutex        sync.RWMutex
	usingPrimary bool
	lastSwitch   time.Time
	minInterval  time.Duration
}

// NewFailoverManager creates a new failover manager
func NewFailoverManager(primary, secondary func() error, detector func() bool) *FailoverManager {
	return &FailoverManager{
		primary:      primary,
		secondary:    secondary,
		detector:     detector,
		usingPrimary: true,
		minInterval:  10 * time.Second,
	}
}

// Execute runs operation with automatic failover
func (fm *FailoverManager) Execute() error {
	fm.mutex.RLock()
	usingPrimary := fm.usingPrimary
	lastSwitch := fm.lastSwitch
	fm.mutex.RUnlock()

	// Check if we should switch (but not too frequently)
	if time.Since(lastSwitch) > fm.minInterval {
		if usingPrimary && !fm.detector() {
			fm.switchToSecondary()
			usingPrimary = false
		} else if !usingPrimary && fm.detector() {
			fm.switchToPrimary()
			usingPrimary = true
		}
	}

	// Execute appropriate function
	if usingPrimary {
		return fm.primary()
	}
	return fm.secondary()
}

// switchToPrimary switches to primary service
func (fm *FailoverManager) switchToPrimary() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.usingPrimary = true
	fm.lastSwitch = time.Now()
}

// switchToSecondary switches to secondary service
func (fm *FailoverManager) switchToSecondary() {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	fm.usingPrimary = false
	fm.lastSwitch = time.Now()
}

// IsUsingPrimary returns true if using primary service
func (fm *FailoverManager) IsUsingPrimary() bool {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.usingPrimary
}

// GetStats returns failover statistics
func (fm *FailoverManager) GetStats() map[string]interface{} {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	return map[string]interface{}{
		"usingPrimary": fm.usingPrimary,
		"lastSwitch":   fm.lastSwitch,
	}
}
