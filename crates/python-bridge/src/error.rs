//! Error handling for the Python bridge
//! 
//! This module provides comprehensive error types and utilities for handling errors
//! that occur during Python bridge operations, including type conversions and validations.

use wasm_bindgen::prelude::*;
use thiserror::Error;

/// Result type for Python bridge operations
pub type BridgeResult<T> = Result<T, PythonBridgeError>;

/// Result type for type conversion operations
pub type ConversionResult<T> = Result<T, ConversionError>;

/// Result type for validation operations
pub type ValidationResult<T> = Result<T, ValidationError>;

/// Error types for Python bridge operations
#[derive(Error, Debug)]
pub enum PythonBridgeError {
    #[error("Pyodide runtime error: {0}")]
    PyodideError(String),
    
    #[error("Python execution error: {0}")]
    PythonExecutionError(String),
    
    #[error("BSATN conversion error: {0}")]
    BsatnConversionError(String),
    
    #[error("Type mismatch error: expected {expected}, got {actual}")]
    TypeMismatchError { expected: String, actual: String },
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Python module not initialized")]
    ModuleNotInitialized,
    
    #[error("Python function not found: {0}")]
    FunctionNotFound(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Reducer not found: {0}")]
    ReducerNotFound(String),
    
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    
    #[error("Memory allocation error: {0}")]
    MemoryError(String),
    
    #[error("JavaScript interop error: {0}")]
    JsError(String),
    
    #[error("SpacetimeDB ABI error: {0}")]
    AbiError(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Runtime error: {0}")]
    RuntimeError(String),
}

/// Specific errors for type conversion operations
#[derive(Error, Debug, Clone)]
pub enum ConversionError {
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },
    
    #[error("Unsupported type: {type_name} in context: {context}")]
    UnsupportedType { type_name: String, context: String },
    
    #[error("Number out of range: {value} cannot be converted to {target_type}")]
    NumberOutOfRange { value: f64, target_type: String },
    
    #[error("Number parse error: '{value}' is not a valid {target_type}")]
    NumberParseError { value: String, target_type: String },
    
    #[error("Property set error: {0:?}")]
    PropertySetError(JsValue),
    
    #[error("Null or undefined value encountered")]
    NullValue,
    
    #[error("Invalid product structure: missing fields {missing_fields:?}")]
    InvalidProductStructure { missing_fields: Vec<String> },
    
    #[error("Invalid sum variant: tag {variant_tag}, expected one of {expected_variants:?}")]
    InvalidSumVariant { variant_tag: u8, expected_variants: Vec<u8> },
    
    #[error("Array type mismatch: expected homogeneous array of {expected_type}")]
    ArrayTypeMismatch { expected_type: String },
    
    #[error("Conversion cache error: {0}")]
    CacheError(String),
    
    #[error("Python introspection error: {0}")]
    IntrospectionError(String),
}

/// Specific errors for validation operations
#[derive(Error, Debug, Clone)]
pub enum ValidationError {
    #[error("Schema validation failed: {message}")]
    SchemaValidationFailed { message: String },
    
    #[error("Required field missing: {field_name}")]
    RequiredFieldMissing { field_name: String },
    
    #[error("Field type validation failed: field {field_name} expected {expected_type}, found {actual_type}")]
    FieldTypeValidationFailed {
        field_name: String,
        expected_type: String,
        actual_type: String,
    },
    
    #[error("Constraint validation failed: {constraint} on field {field_name}")]
    ConstraintValidationFailed { constraint: String, field_name: String },
    
    #[error("Custom validation failed: {message}")]
    CustomValidationFailed { message: String },
}

impl ConversionError {
    /// Get a user-friendly error message with suggestions
    pub fn user_friendly_message(&self) -> String {
        match self {
            ConversionError::TypeMismatch { expected, found } => {
                format!(
                    "Expected type '{}' but found '{}'. Please check your Python object structure and ensure it matches the expected SpacetimeDB type.",
                    expected, found
                )
            }
            ConversionError::UnsupportedType { type_name, context } => {
                format!(
                    "The type '{}' is not supported in {}. Please use one of the supported SpacetimeDB types (bool, int, float, str, list, dict).",
                    type_name, context
                )
            }
            ConversionError::NumberOutOfRange { value, target_type } => {
                format!(
                    "Number {} is out of range for type {}. Please ensure your number fits within the valid range for this type.",
                    value, target_type
                )
            }
            ConversionError::NumberParseError { value, target_type } => {
                format!(
                    "Cannot parse '{}' as {}. Please ensure you're providing a valid numeric string.",
                    value, target_type
                )
            }
            ConversionError::NullValue => {
                "Null or undefined values are not allowed. Please provide a valid value.".to_string()
            }
            ConversionError::InvalidProductStructure { missing_fields } => {
                format!(
                    "Missing required fields: {}. Please ensure your Python object has all required properties.",
                    missing_fields.join(", ")
                )
            }
            ConversionError::InvalidSumVariant { variant_tag, expected_variants } => {
                format!(
                    "Invalid variant tag {}. Expected one of: {:?}. Please check your union type structure.",
                    variant_tag, expected_variants
                )
            }
            ConversionError::ArrayTypeMismatch { expected_type } => {
                format!(
                    "Array elements must all be of type {}. Please ensure all array elements are the same type.",
                    expected_type
                )
            }
            _ => self.to_string(),
        }
    }
}

impl ValidationError {
    /// Get a user-friendly error message with suggestions
    pub fn user_friendly_message(&self) -> String {
        match self {
            ValidationError::SchemaValidationFailed { message } => {
                format!("Schema validation failed: {}. Please check your data structure against the expected schema.", message)
            }
            ValidationError::RequiredFieldMissing { field_name } => {
                format!("Required field '{}' is missing. Please ensure all required fields are provided.", field_name)
            }
            ValidationError::FieldTypeValidationFailed { field_name, expected_type, actual_type } => {
                format!(
                    "Field '{}' has incorrect type. Expected {}, but found {}. Please check the field type.",
                    field_name, expected_type, actual_type
                )
            }
            ValidationError::ConstraintValidationFailed { constraint, field_name } => {
                format!(
                    "Constraint '{}' failed for field '{}'. Please ensure the field value meets the specified constraint.",
                    constraint, field_name
                )
            }
            ValidationError::CustomValidationFailed { message } => {
                format!("Validation failed: {}. Please check your data and try again.", message)
            }
        }
    }
}

impl PythonBridgeError {
    /// Create a new Pyodide error
    pub fn pyodide_error(msg: impl Into<String>) -> Self {
        Self::PyodideError(msg.into())
    }
    
    /// Create a new Python execution error
    pub fn python_execution_error(msg: impl Into<String>) -> Self {
        Self::PythonExecutionError(msg.into())
    }
    
    /// Create a new BSATN conversion error
    pub fn bsatn_conversion_error(msg: impl Into<String>) -> Self {
        Self::BsatnConversionError(msg.into())
    }
    
    /// Create a new type mismatch error
    pub fn type_mismatch_error(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::TypeMismatchError {
            expected: expected.into(),
            actual: actual.into(),
        }
    }
    
    /// Create a new JavaScript interop error
    pub fn js_error(msg: impl Into<String>) -> Self {
        Self::JsError(msg.into())
    }
    
    /// Create a new runtime error
    pub fn runtime_error(msg: impl Into<String>) -> Self {
        Self::RuntimeError(msg.into())
    }
}

// Conversion implementations
impl From<ConversionError> for PythonBridgeError {
    fn from(error: ConversionError) -> Self {
        Self::BsatnConversionError(error.to_string())
    }
}

impl From<ValidationError> for PythonBridgeError {
    fn from(error: ValidationError) -> Self {
        Self::InvalidArgument(error.to_string())
    }
}

impl From<ConversionError> for JsValue {
    fn from(error: ConversionError) -> Self {
        JsValue::from_str(&error.user_friendly_message())
    }
}

impl From<ValidationError> for JsValue {
    fn from(error: ValidationError) -> Self {
        JsValue::from_str(&error.user_friendly_message())
    }
}

/// Convert PythonBridgeError to JsValue for WASM bindings
impl From<PythonBridgeError> for JsValue {
    fn from(error: PythonBridgeError) -> Self {
        JsValue::from_str(&error.to_string())
    }
}

/// Convert JsValue to PythonBridgeError
impl From<JsValue> for PythonBridgeError {
    fn from(js_value: JsValue) -> Self {
        if let Some(error_str) = js_value.as_string() {
            PythonBridgeError::JsError(error_str)
        } else {
            PythonBridgeError::JsError("Unknown JavaScript error".to_string())
        }
    }
}

/// Convert serde_json::Error to PythonBridgeError
impl From<serde_json::Error> for PythonBridgeError {
    fn from(error: serde_json::Error) -> Self {
        PythonBridgeError::SerializationError(error.to_string())
    }
}

/// Convert std::string::FromUtf8Error to PythonBridgeError
impl From<std::string::FromUtf8Error> for PythonBridgeError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        PythonBridgeError::DeserializationError(format!("UTF-8 conversion error: {}", error))
    }
}

/// Helper trait for converting Results to bridge-compatible format
pub trait ToBridgeResult<T> {
    fn to_bridge_result(self) -> BridgeResult<T>;
}

impl<T, E> ToBridgeResult<T> for Result<T, E>
where
    E: Into<PythonBridgeError>,
{
    fn to_bridge_result(self) -> BridgeResult<T> {
        self.map_err(|e| e.into())
    }
}

/// Utility function for creating error messages with context
pub fn with_context<T>(result: BridgeResult<T>, context: &str) -> BridgeResult<T> {
    result.map_err(|e| PythonBridgeError::RuntimeError(format!("{}: {}", context, e)))
}

/// Macro for creating error results quickly
#[macro_export]
macro_rules! bridge_error {
    ($variant:ident, $msg:expr) => {
        Err(PythonBridgeError::$variant($msg.to_string()))
    };
    ($variant:ident, $fmt:expr, $($arg:tt)*) => {
        Err(PythonBridgeError::$variant(format!($fmt, $($arg)*)))
    };
}

/// Macro for creating conversion error results quickly
#[macro_export]
macro_rules! conversion_error {
    ($variant:ident, $($field:ident: $value:expr),*) => {
        Err(ConversionError::$variant { $($field: $value),* })
    };
}

/// Macro for creating validation error results quickly
#[macro_export]
macro_rules! validation_error {
    ($variant:ident, $($field:ident: $value:expr),*) => {
        Err(ValidationError::$variant { $($field: $value),* })
    };
}

/// Macro for unwrapping JsValue results with proper error conversion
#[macro_export]
macro_rules! js_unwrap {
    ($result:expr) => {
        $result.map_err(PythonBridgeError::from)?
    };
    ($result:expr, $context:expr) => {
        $result.map_err(|e| PythonBridgeError::JsError(format!("{}: {:?}", $context, e)))?
    };
}

/// Helper function to log errors with proper formatting
pub fn log_bridge_error(error: &PythonBridgeError) {
    web_sys::console::error_1(&format!("Python Bridge Error: {}", error).into());
}

/// Helper function to log conversion errors with proper formatting
pub fn log_conversion_error(error: &ConversionError) {
    web_sys::console::error_1(&format!("Type Conversion Error: {}", error.user_friendly_message()).into());
}

/// Helper function to log validation errors with proper formatting
pub fn log_validation_error(error: &ValidationError) {
    web_sys::console::error_1(&format!("Validation Error: {}", error.user_friendly_message()).into());
}

/// Helper function to create a JsValue error with proper formatting
pub fn create_js_error(message: &str) -> JsValue {
    JsValue::from_str(&format!("PythonBridgeError: {}", message))
}

/// Error recovery utilities
pub struct ErrorRecovery;

impl ErrorRecovery {
    /// Attempt to recover from a Python execution error
    pub fn recover_from_python_error(error: &PythonBridgeError) -> Option<String> {
        match error {
            PythonBridgeError::PythonExecutionError(msg) => {
                // Extract useful information from Python traceback
                if msg.contains("NameError") {
                    Some("Suggestion: Check if all variables and functions are properly defined".to_string())
                } else if msg.contains("TypeError") {
                    Some("Suggestion: Check function arguments and return types".to_string())
                } else if msg.contains("AttributeError") {
                    Some("Suggestion: Check if the object has the required attributes or methods".to_string())
                } else {
                    None
                }
            }
            PythonBridgeError::FunctionNotFound(name) => {
                Some(format!("Suggestion: Ensure function '{}' is defined and decorated with @reducer", name))
            }
            PythonBridgeError::TableNotFound(name) => {
                Some(format!("Suggestion: Ensure table '{}' is defined and decorated with @table", name))
            }
            _ => None,
        }
    }
    
    /// Attempt to recover from conversion errors
    pub fn recover_from_conversion_error(error: &ConversionError) -> Option<String> {
        match error {
            ConversionError::TypeMismatch { expected, found } => {
                Some(format!("Try converting your {} to {} or adjust your data structure.", found, expected))
            }
            ConversionError::NumberOutOfRange { target_type, .. } => {
                Some(format!("Use a smaller number that fits within the {} range.", target_type))
            }
            ConversionError::InvalidProductStructure { missing_fields } => {
                Some(format!("Add the missing fields: {}", missing_fields.join(", ")))
            }
            _ => None,
        }
    }
    
    /// Get user-friendly error message
    pub fn get_user_friendly_message(error: &PythonBridgeError) -> String {
        match error {
            PythonBridgeError::PyodideError(_) => {
                "There was an error with the Python runtime. Please check your Python code for syntax errors.".to_string()
            }
            PythonBridgeError::PythonExecutionError(_) => {
                "Your Python code encountered an execution error. Please check the console for details.".to_string()
            }
            PythonBridgeError::BsatnConversionError(_) => {
                "There was an error converting data between Python and SpacetimeDB. Please check your data types.".to_string()
            }
            PythonBridgeError::ModuleNotInitialized => {
                "The Python module is not properly initialized. Please ensure your module code is loaded.".to_string()
            }
            PythonBridgeError::FunctionNotFound(name) => {
                format!("The reducer function '{}' was not found. Please ensure it's defined and decorated with @reducer.", name)
            }
            PythonBridgeError::TableNotFound(name) => {
                format!("The table '{}' was not found. Please ensure it's defined and decorated with @table.", name)
            }
            _ => error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let error = PythonBridgeError::python_execution_error("Test error");
        assert!(matches!(error, PythonBridgeError::PythonExecutionError(_)));
    }

    #[test]
    fn test_error_conversion() {
        let bridge_error = PythonBridgeError::runtime_error("Test");
        let js_value: JsValue = bridge_error.into();
        assert!(js_value.is_string());
    }
    
    #[test]
    fn test_error_recovery() {
        let error = PythonBridgeError::FunctionNotFound("test_func".to_string());
        let suggestion = ErrorRecovery::recover_from_python_error(&error);
        assert!(suggestion.is_some());
        assert!(suggestion.unwrap().contains("@reducer"));
    }
    
    #[test]
    fn test_conversion_error() {
        let error = ConversionError::TypeMismatch {
            expected: "int".to_string(),
            found: "string".to_string(),
        };
        let message = error.user_friendly_message();
        assert!(message.contains("Expected type 'int' but found 'string'"));
    }
    
    #[test]
    fn test_validation_error() {
        let error = ValidationError::RequiredFieldMissing {
            field_name: "player_id".to_string(),
        };
        let message = error.user_friendly_message();
        assert!(message.contains("Required field 'player_id' is missing"));
    }
    
    #[test]
    fn test_conversion_error_to_bridge_error() {
        let conv_error = ConversionError::NullValue;
        let bridge_error: PythonBridgeError = conv_error.into();
        assert!(matches!(bridge_error, PythonBridgeError::BsatnConversionError(_)));
    }
    
    #[test]
    fn test_validation_error_to_bridge_error() {
        let val_error = ValidationError::CustomValidationFailed {
            message: "Test validation".to_string(),
        };
        let bridge_error: PythonBridgeError = val_error.into();
        assert!(matches!(bridge_error, PythonBridgeError::InvalidArgument(_)));
    }
}
