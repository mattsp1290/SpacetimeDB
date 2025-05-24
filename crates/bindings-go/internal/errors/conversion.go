// Package errors provides advanced error conversion utilities for SpacetimeDB Go bindings.
// This module implements sophisticated error type conversion between different formats,
// WASM error marshaling/unmarshaling, and cross-system error correlation.
package errors

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	spacetimedb "github.com/clockworklabs/SpacetimeDB/crates/bindings-go/internal/types"
)

// ErrorTypeRegistry manages registered error types for conversion
type ErrorTypeRegistry struct {
	converters map[reflect.Type]ErrorConverter
	decoders   map[string]ErrorDecoder
	mutex      sync.RWMutex
}

// ErrorConverter converts errors to specific formats
type ErrorConverter interface {
	ConvertTo(err error, targetFormat string) (interface{}, error)
	ConvertFrom(data interface{}, sourceFormat string) (error, error)
	SupportedFormats() []string
}

// ErrorDecoder decodes errors from specific formats
type ErrorDecoder interface {
	Decode(data []byte) (error, error)
	Encode(err error) ([]byte, error)
	Format() string
}

// NewErrorTypeRegistry creates a new error type registry
func NewErrorTypeRegistry() *ErrorTypeRegistry {
	registry := &ErrorTypeRegistry{
		converters: make(map[reflect.Type]ErrorConverter),
		decoders:   make(map[string]ErrorDecoder),
	}

	// Register default converters
	registry.registerDefaultConverters()

	return registry
}

// RegisterConverter registers an error converter for a specific type
func (etr *ErrorTypeRegistry) RegisterConverter(errorType reflect.Type, converter ErrorConverter) {
	etr.mutex.Lock()
	defer etr.mutex.Unlock()
	etr.converters[errorType] = converter
}

// RegisterDecoder registers an error decoder for a specific format
func (etr *ErrorTypeRegistry) RegisterDecoder(format string, decoder ErrorDecoder) {
	etr.mutex.Lock()
	defer etr.mutex.Unlock()
	etr.decoders[format] = decoder
}

// ConvertError converts an error to a specific format
func (etr *ErrorTypeRegistry) ConvertError(err error, targetFormat string) (interface{}, error) {
	if err == nil {
		return nil, nil
	}

	etr.mutex.RLock()
	defer etr.mutex.RUnlock()

	// Find converter for error type
	errorType := reflect.TypeOf(err)
	if converter, exists := etr.converters[errorType]; exists {
		return converter.ConvertTo(err, targetFormat)
	}

	// Try interface types
	for registeredType, converter := range etr.converters {
		if errorType.Implements(registeredType) {
			return converter.ConvertTo(err, targetFormat)
		}
	}

	// Fall back to generic conversion
	return etr.genericConversion(err, targetFormat)
}

// DecodeError decodes an error from a specific format
func (etr *ErrorTypeRegistry) DecodeError(data []byte, format string) (error, error) {
	etr.mutex.RLock()
	defer etr.mutex.RUnlock()

	if decoder, exists := etr.decoders[format]; exists {
		return decoder.Decode(data)
	}

	return nil, fmt.Errorf("no decoder found for format: %s", format)
}

// EncodeError encodes an error to a specific format
func (etr *ErrorTypeRegistry) EncodeError(err error, format string) ([]byte, error) {
	if err == nil {
		return nil, nil
	}

	etr.mutex.RLock()
	defer etr.mutex.RUnlock()

	if decoder, exists := etr.decoders[format]; exists {
		return decoder.Encode(err)
	}

	return nil, fmt.Errorf("no encoder found for format: %s", format)
}

// registerDefaultConverters registers default error converters
func (etr *ErrorTypeRegistry) registerDefaultConverters() {
	// Register SpacetimeError converter
	etr.converters[reflect.TypeOf(&spacetimedb.SpacetimeError{})] = &SpacetimeErrorConverter{}

	// Register Errno converter
	etr.converters[reflect.TypeOf(&spacetimedb.Errno{})] = &ErrnoConverter{}

	// Register generic error converter
	etr.converters[reflect.TypeOf((*error)(nil)).Elem()] = &GenericErrorConverter{}

	// Register decoders
	etr.decoders["json"] = &JSONErrorDecoder{}
	etr.decoders["wasm"] = &WASMErrorDecoder{}
	etr.decoders["string"] = &StringErrorDecoder{}
	etr.decoders["errno"] = &ErrnoDecoder{}
}

// genericConversion provides generic error conversion
func (etr *ErrorTypeRegistry) genericConversion(err error, targetFormat string) (interface{}, error) {
	switch targetFormat {
	case "json":
		return map[string]interface{}{
			"type":    reflect.TypeOf(err).String(),
			"message": err.Error(),
		}, nil
	case "string":
		return err.Error(), nil
	case "wasm":
		return &WASMError{
			Code:    1,
			Message: err.Error(),
			Type:    reflect.TypeOf(err).String(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported target format: %s", targetFormat)
	}
}

// SpacetimeErrorConverter converts SpacetimeError instances
type SpacetimeErrorConverter struct{}

func (sec *SpacetimeErrorConverter) ConvertTo(err error, targetFormat string) (interface{}, error) {
	spacetimeErr, ok := err.(*spacetimedb.SpacetimeError)
	if !ok {
		return nil, fmt.Errorf("expected SpacetimeError, got %T", err)
	}

	switch targetFormat {
	case "json":
		return map[string]interface{}{
			"type":    "SpacetimeError",
			"errno":   spacetimeErr.Errno,
			"context": spacetimeErr.Context,
			"cause":   spacetimeErr.Cause,
		}, nil
	case "wasm":
		code := uint16(1)
		if spacetimeErr.Errno != nil {
			code = spacetimeErr.Errno.Code()
		}
		return &WASMError{
			Code:    int32(code),
			Message: spacetimeErr.Error(),
			Type:    "SpacetimeError",
			Context: spacetimeErr.Context,
		}, nil
	case "string":
		return spacetimeErr.Error(), nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", targetFormat)
	}
}

func (sec *SpacetimeErrorConverter) ConvertFrom(data interface{}, sourceFormat string) (error, error) {
	switch sourceFormat {
	case "json":
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map[string]interface{}, got %T", data)
		}

		// Extract errno
		var errno *spacetimedb.Errno
		if _, exists := dataMap["errno"]; exists {
			// Simplified errno reconstruction
			errno = spacetimedb.NewErrno(1)
		}

		// Extract context
		var context *spacetimedb.ErrorContext
		if contextData, exists := dataMap["context"]; exists && contextData != nil {
			// Implementation depends on context structure
			context = &spacetimedb.ErrorContext{} // Simplified
		}

		return &spacetimedb.SpacetimeError{
			Errno:   errno,
			Context: context,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported source format: %s", sourceFormat)
	}
}

func (sec *SpacetimeErrorConverter) SupportedFormats() []string {
	return []string{"json", "wasm", "string"}
}

// ErrnoConverter converts Errno instances
type ErrnoConverter struct{}

func (ec *ErrnoConverter) ConvertTo(err error, targetFormat string) (interface{}, error) {
	errno, ok := err.(*spacetimedb.Errno)
	if !ok {
		return nil, fmt.Errorf("expected Errno, got %T", err)
	}

	switch targetFormat {
	case "json":
		return map[string]interface{}{
			"type":    "Errno",
			"code":    errno.Code(),
			"message": errno.Error(),
		}, nil
	case "wasm":
		return &WASMError{
			Code:    int32(errno.Code()),
			Message: errno.Error(),
			Type:    "Errno",
		}, nil
	case "string":
		return errno.Error(), nil
	case "errno":
		return errno.Code(), nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", targetFormat)
	}
}

func (ec *ErrnoConverter) ConvertFrom(data interface{}, sourceFormat string) (error, error) {
	switch sourceFormat {
	case "json":
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map[string]interface{}, got %T", data)
		}

		codeFloat, ok := dataMap["code"].(float64)
		if !ok {
			return nil, fmt.Errorf("invalid code in errno data")
		}

		return spacetimedb.NewErrno(uint16(codeFloat)), nil
	case "errno":
		code, ok := data.(uint16)
		if !ok {
			codeInt, ok := data.(int)
			if !ok {
				return nil, fmt.Errorf("expected uint16 or int, got %T", data)
			}
			code = uint16(codeInt)
		}
		return spacetimedb.NewErrno(code), nil
	default:
		return nil, fmt.Errorf("unsupported source format: %s", sourceFormat)
	}
}

func (ec *ErrnoConverter) SupportedFormats() []string {
	return []string{"json", "wasm", "string", "errno"}
}

// GenericErrorConverter converts generic error instances
type GenericErrorConverter struct{}

func (gec *GenericErrorConverter) ConvertTo(err error, targetFormat string) (interface{}, error) {
	switch targetFormat {
	case "json":
		return map[string]interface{}{
			"type":    "error",
			"message": err.Error(),
		}, nil
	case "wasm":
		return &WASMError{
			Code:    1,
			Message: err.Error(),
			Type:    "error",
		}, nil
	case "string":
		return err.Error(), nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", targetFormat)
	}
}

func (gec *GenericErrorConverter) ConvertFrom(data interface{}, sourceFormat string) (error, error) {
	switch sourceFormat {
	case "json":
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map[string]interface{}, got %T", data)
		}

		message, ok := dataMap["message"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid message in error data")
		}

		return fmt.Errorf("%s", message), nil
	case "string":
		message, ok := data.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", data)
		}
		return fmt.Errorf("%s", message), nil
	default:
		return nil, fmt.Errorf("unsupported source format: %s", sourceFormat)
	}
}

func (gec *GenericErrorConverter) SupportedFormats() []string {
	return []string{"json", "wasm", "string"}
}

// WASMError represents an error in WASM format
type WASMError struct {
	Code    int32       `json:"code"`
	Message string      `json:"message"`
	Type    string      `json:"type"`
	Context interface{} `json:"context,omitempty"`
	Stack   []string    `json:"stack,omitempty"`
}

func (we *WASMError) Error() string {
	return fmt.Sprintf("WASM Error [%d]: %s", we.Code, we.Message)
}

// JSONErrorDecoder handles JSON error encoding/decoding
type JSONErrorDecoder struct{}

func (jed *JSONErrorDecoder) Decode(data []byte) (error, error) {
	var errorData map[string]interface{}
	if err := json.Unmarshal(data, &errorData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON error: %w", err)
	}

	errorType, ok := errorData["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid error type")
	}

	message, ok := errorData["message"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid error message")
	}

	switch errorType {
	case "SpacetimeError":
		// Reconstruct SpacetimeError
		var errno *spacetimedb.Errno
		if _, exists := errorData["errno"]; exists {
			// Simplified errno reconstruction
			errno = spacetimedb.NewErrno(1)
		}

		return &spacetimedb.SpacetimeError{
			Errno: errno,
			Context: &spacetimedb.ErrorContext{
				Category:  "unknown",
				Operation: "unknown",
			},
		}, nil
	case "Errno":
		codeFloat, ok := errorData["code"].(float64)
		if !ok {
			return nil, fmt.Errorf("invalid errno code")
		}
		return spacetimedb.NewErrno(uint16(codeFloat)), nil
	default:
		return fmt.Errorf("%s", message), nil
	}
}

func (jed *JSONErrorDecoder) Encode(err error) ([]byte, error) {
	var errorData map[string]interface{}

	switch e := err.(type) {
	case *spacetimedb.SpacetimeError:
		errorData = map[string]interface{}{
			"type":    "SpacetimeError",
			"message": e.Error(),
			"errno":   e.Errno,
			"context": e.Context,
		}
	case *spacetimedb.Errno:
		errorData = map[string]interface{}{
			"type":    "Errno",
			"code":    e.Code(),
			"message": e.Error(),
		}
	case *WASMError:
		errorData = map[string]interface{}{
			"type":    "WASMError",
			"code":    e.Code,
			"message": e.Message,
			"context": e.Context,
			"stack":   e.Stack,
		}
	default:
		errorData = map[string]interface{}{
			"type":    "error",
			"message": err.Error(),
		}
	}

	return json.Marshal(errorData)
}

func (jed *JSONErrorDecoder) Format() string {
	return "json"
}

// WASMErrorDecoder handles WASM error encoding/decoding
type WASMErrorDecoder struct{}

func (wed *WASMErrorDecoder) Decode(data []byte) (error, error) {
	var wasmError WASMError
	if err := json.Unmarshal(data, &wasmError); err != nil {
		return nil, fmt.Errorf("failed to unmarshal WASM error: %w", err)
	}

	return &wasmError, nil
}

func (wed *WASMErrorDecoder) Encode(err error) ([]byte, error) {
	var wasmError *WASMError

	switch e := err.(type) {
	case *WASMError:
		wasmError = e
	case *spacetimedb.SpacetimeError:
		code := int32(1)
		if e.Errno != nil {
			code = int32(e.Errno.Code())
		}
		wasmError = &WASMError{
			Code:    code,
			Message: e.Error(),
			Type:    "SpacetimeError",
			Context: e.Context,
		}
	case *spacetimedb.Errno:
		wasmError = &WASMError{
			Code:    int32(e.Code()),
			Message: e.Error(),
			Type:    "Errno",
		}
	default:
		wasmError = &WASMError{
			Code:    1,
			Message: err.Error(),
			Type:    "error",
		}
	}

	return json.Marshal(wasmError)
}

func (wed *WASMErrorDecoder) Format() string {
	return "wasm"
}

// StringErrorDecoder handles string error encoding/decoding
type StringErrorDecoder struct{}

func (sed *StringErrorDecoder) Decode(data []byte) (error, error) {
	return fmt.Errorf("%s", string(data)), nil
}

func (sed *StringErrorDecoder) Encode(err error) ([]byte, error) {
	return []byte(err.Error()), nil
}

func (sed *StringErrorDecoder) Format() string {
	return "string"
}

// ErrnoDecoder handles errno code encoding/decoding
type ErrnoDecoder struct{}

func (ed *ErrnoDecoder) Decode(data []byte) (error, error) {
	code, err := strconv.ParseUint(string(data), 10, 16)
	if err != nil {
		return nil, fmt.Errorf("failed to parse errno code: %w", err)
	}

	return spacetimedb.NewErrno(uint16(code)), nil
}

func (ed *ErrnoDecoder) Encode(err error) ([]byte, error) {
	switch e := err.(type) {
	case *spacetimedb.Errno:
		return []byte(strconv.FormatUint(uint64(e.Code()), 10)), nil
	case *spacetimedb.SpacetimeError:
		if e.Errno != nil {
			return []byte(strconv.FormatUint(uint64(e.Errno.Code()), 10)), nil
		}
		return []byte("1"), nil
	default:
		return []byte("1"), nil
	}
}

func (ed *ErrnoDecoder) Format() string {
	return "errno"
}

// CrossSystemErrorCorrelator correlates errors across different systems
type CrossSystemErrorCorrelator struct {
	correlations map[string][]CorrelatedError
	mutex        sync.RWMutex
}

// CorrelatedError represents an error correlated across systems
type CorrelatedError struct {
	System      string                 `json:"system"`
	ErrorID     string                 `json:"error_id"`
	Error       error                  `json:"error"`
	Timestamp   int64                  `json:"timestamp"`
	Metadata    map[string]interface{} `json:"metadata"`
	Correlation string                 `json:"correlation"`
}

// NewCrossSystemErrorCorrelator creates a new error correlator
func NewCrossSystemErrorCorrelator() *CrossSystemErrorCorrelator {
	return &CrossSystemErrorCorrelator{
		correlations: make(map[string][]CorrelatedError),
	}
}

// CorrelateError correlates an error with a correlation ID
func (csec *CrossSystemErrorCorrelator) CorrelateError(correlationID, system, errorID string, err error, metadata map[string]interface{}) {
	correlatedError := CorrelatedError{
		System:      system,
		ErrorID:     errorID,
		Error:       err,
		Timestamp:   time.Now().Unix(),
		Metadata:    metadata,
		Correlation: correlationID,
	}

	csec.mutex.Lock()
	defer csec.mutex.Unlock()

	csec.correlations[correlationID] = append(csec.correlations[correlationID], correlatedError)
}

// GetCorrelatedErrors returns all errors for a correlation ID
func (csec *CrossSystemErrorCorrelator) GetCorrelatedErrors(correlationID string) []CorrelatedError {
	csec.mutex.RLock()
	defer csec.mutex.RUnlock()

	errors, exists := csec.correlations[correlationID]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	result := make([]CorrelatedError, len(errors))
	copy(result, errors)
	return result
}

// ErrorChainAnalyzer analyzes error chains and relationships
type ErrorChainAnalyzer struct{}

// ErrorChain represents a chain of related errors
type ErrorChain struct {
	RootError  error                  `json:"root_error"`
	Chain      []error                `json:"chain"`
	Metadata   map[string]interface{} `json:"metadata"`
	Depth      int                    `json:"depth"`
	Categories []string               `json:"categories"`
}

// AnalyzeChain analyzes an error chain
func (eca *ErrorChainAnalyzer) AnalyzeChain(err error) *ErrorChain {
	if err == nil {
		return nil
	}

	chain := &ErrorChain{
		RootError:  err,
		Chain:      make([]error, 0),
		Metadata:   make(map[string]interface{}),
		Categories: make([]string, 0),
	}

	// Build error chain
	current := err
	for current != nil {
		chain.Chain = append(chain.Chain, current)
		chain.Depth++

		// Add category if we can determine it
		if category := eca.categorizeError(current); category != "" {
			chain.Categories = append(chain.Categories, category)
		}

		// Try to unwrap the error
		if unwrapper, ok := current.(interface{ Unwrap() error }); ok {
			current = unwrapper.Unwrap()
		} else {
			break
		}
	}

	// Add metadata
	chain.Metadata["total_errors"] = len(chain.Chain)
	chain.Metadata["unique_categories"] = eca.uniqueCategories(chain.Categories)
	chain.Metadata["analysis_timestamp"] = time.Now().Unix()

	return chain
}

// categorizeError categorizes an error
func (eca *ErrorChainAnalyzer) categorizeError(err error) string {
	errMsg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errMsg, "network"):
		return "network"
	case strings.Contains(errMsg, "database"):
		return "database"
	case strings.Contains(errMsg, "memory"):
		return "memory"
	case strings.Contains(errMsg, "timeout"):
		return "timeout"
	case strings.Contains(errMsg, "validation"):
		return "validation"
	default:
		return ""
	}
}

// uniqueCategories returns unique categories
func (eca *ErrorChainAnalyzer) uniqueCategories(categories []string) []string {
	seen := make(map[string]bool)
	var unique []string

	for _, category := range categories {
		if category != "" && !seen[category] {
			seen[category] = true
			unique = append(unique, category)
		}
	}

	return unique
}

// Default global registry
var defaultRegistry = NewErrorTypeRegistry()

// ConvertError converts an error using the default registry
func ConvertError(err error, targetFormat string) (interface{}, error) {
	return defaultRegistry.ConvertError(err, targetFormat)
}

// DecodeError decodes an error using the default registry
func DecodeError(data []byte, format string) (error, error) {
	return defaultRegistry.DecodeError(data, format)
}

// EncodeError encodes an error using the default registry
func EncodeError(err error, format string) ([]byte, error) {
	return defaultRegistry.EncodeError(err, format)
}
