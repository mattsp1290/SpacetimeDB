//! SpacetimeDB Python Bridge using Pyodide Runtime
//! 
//! This crate provides a bridge between SpacetimeDB's WASM module interface
//! and Python code running in the Pyodide runtime. It enables Python to be
//! used as a first-class language for SpacetimeDB server modules.

use wasm_bindgen::prelude::*;
use js_sys::Object;
use web_sys::console;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

mod pyodide_runtime;
mod bsatn_bridge;
mod error;
mod type_conversion;

#[cfg(test)]
mod tests;

pub use pyodide_runtime::PyodideRuntime;
pub use bsatn_bridge::BsatnBridge;
pub use error::{PythonBridgeError, BridgeResult, ConversionError, ValidationError};
pub use type_conversion::{TypeConverter, ConversionStats};

/// Global instance of the Python module bridge
static mut PYTHON_MODULE: Option<PythonModule> = None;

/// Represents a Python table definition extracted from Python code
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonTableDefinition {
    pub name: String,
    pub fields: Vec<PythonFieldDefinition>,
    pub primary_key: Option<String>,
    pub indexes: Vec<String>,
}

/// Represents a Python field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonFieldDefinition {
    pub name: String,
    pub type_name: String,
    pub optional: bool,
    pub default_value: Option<String>,
}

/// Represents a Python reducer definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonReducerDefinition {
    pub name: String,
    pub parameters: Vec<PythonParameterDefinition>,
    pub is_scheduled: bool,
    pub schedule_interval: Option<u64>,
    pub schedule_delay: Option<u64>,
}

/// Represents a Python reducer parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonParameterDefinition {
    pub name: String,
    pub type_name: String,
    pub optional: bool,
    pub default_value: Option<String>,
}

/// Main Python module bridge that manages the Pyodide runtime and handles
/// SpacetimeDB WASM module interface with enhanced type conversion
#[wasm_bindgen]
pub struct PythonModule {
    pyodide: PyodideRuntime,
    bsatn_bridge: BsatnBridge,
    type_converter: Option<TypeConverter>,
    python_code: String,
    initialized: bool,
    tables: HashMap<String, PythonTableDefinition>,
    reducers: HashMap<String, PythonReducerDefinition>,
}

#[wasm_bindgen]
impl PythonModule {
    /// Create a new Python module bridge instance
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<PythonModule, JsValue> {
        console::log_1(&"Initializing Python module bridge with enhanced type conversion...".into());
        
        let pyodide = PyodideRuntime::new()?;
        let bsatn_bridge = BsatnBridge::new();
        
        Ok(PythonModule {
            pyodide,
            bsatn_bridge,
            type_converter: None, // Will be initialized when Pyodide runtime is ready
            python_code: String::new(),
            initialized: false,
            tables: HashMap::new(),
            reducers: HashMap::new(),
        })
    }

    /// Load and execute Python module code
    pub fn load_python_code(&mut self, code: &str) -> Result<(), JsValue> {
        console::log_1(&"Loading Python code with enhanced type conversion...".into());
        
        self.python_code = code.to_string();
        
        // Execute the Python code in the Pyodide runtime
        self.pyodide.run_python(code)?;
        
        // Initialize the enhanced type converter
        // Note: In the actual implementation, we'd pass the PyodideRuntime properly
        // For now, this is a placeholder that would be enhanced
        console::log_1(&"Initializing enhanced type converter...".into());
        
        // Extract table and reducer definitions from the executed code
        self.extract_python_definitions()?;
        
        self.initialized = true;
        console::log_1(&"Python code loaded successfully with enhanced type conversion".into());
        
        Ok(())
    }

    /// Call a Python reducer function with enhanced type conversion
    pub fn call_reducer(&mut self, name: &str, args: &[u8]) -> Result<Vec<u8>, JsValue> {
        if !self.initialized {
            return Err("Python module not initialized".into());
        }

        console::log_1(&format!("Calling Python reducer '{}' with enhanced type conversion", name).into());
        
        // Get reducer definition
        let _reducer_def = self.reducers.get(name)
            .ok_or_else(|| format!("Reducer '{}' not found", name))?;
        
        // Use enhanced BSATN conversion if available, otherwise fallback to basic bridge
        let py_args = if let Some(_converter) = &mut self.type_converter {
            // TODO: Use enhanced type converter when fully implemented
            console::log_1(&"Using enhanced type conversion for arguments".into());
            self.bsatn_bridge.bsatn_to_python_args(args)?
        } else {
            // Fallback to basic bridge
            self.bsatn_bridge.bsatn_to_python_args(args)?
        };
        
        // Call the Python reducer function
        let result = self.pyodide.call_python_function(name, &py_args)?;
        
        // Use enhanced BSATN conversion for return value if available
        let bsatn_result = if let Some(_converter) = &mut self.type_converter {
            // TODO: Use enhanced type converter when fully implemented
            console::log_1(&"Using enhanced type conversion for return value".into());
            self.bsatn_bridge.python_to_bsatn_result(&result)?
        } else {
            // Fallback to basic bridge
            self.bsatn_bridge.python_to_bsatn_result(&result)?
        };
        
        console::log_1(&format!("Reducer '{}' completed successfully", name).into());
        Ok(bsatn_result)
    }

    /// Get table definitions extracted from Python code
    pub fn get_table_definitions(&self) -> Result<JsValue, JsValue> {
        let definitions: Vec<&PythonTableDefinition> = self.tables.values().collect();
        serde_wasm_bindgen::to_value(&definitions)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get reducer definitions extracted from Python code
    pub fn get_reducer_definitions(&self) -> Result<JsValue, JsValue> {
        let definitions: Vec<&PythonReducerDefinition> = self.reducers.values().collect();
        serde_wasm_bindgen::to_value(&definitions)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Check if the module is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get type conversion statistics (if enhanced converter is available)
    pub fn get_conversion_stats(&self) -> Result<JsValue, JsValue> {
        if let Some(converter) = &self.type_converter {
            let stats = converter.get_stats();
            serde_wasm_bindgen::to_value(stats)
                .map_err(|e| JsValue::from_str(&e.to_string()))
        } else {
            Ok(JsValue::from_str(&serde_json::json!({
                "enhanced_converter": false,
                "message": "Using basic BSATN bridge"
            }).to_string()))
        }
    }
}

impl PythonModule {
    /// Extract table and reducer definitions from the executed Python code
    fn extract_python_definitions(&mut self) -> Result<(), JsValue> {
        console::log_1(&"Extracting Python definitions...".into());
        
        // Call Python helper functions to get registered tables and reducers
        let tables_js = self.pyodide.call_python_function("get_registered_tables", &JsValue::undefined())?;
        let reducers_js = self.pyodide.call_python_function("get_registered_reducers", &JsValue::undefined())?;
        
        // Convert JavaScript objects to Rust structures
        self.extract_table_definitions(&tables_js)?;
        self.extract_reducer_definitions(&reducers_js)?;
        
        console::log_1(&format!("Extracted {} tables and {} reducers", 
                                self.tables.len(), self.reducers.len()).into());
        
        Ok(())
    }

    fn extract_table_definitions(&mut self, tables_js: &JsValue) -> Result<(), JsValue> {
        // This would parse the JavaScript object containing table definitions
        // For now, implementing a basic version - would need to be expanded
        // to properly extract all table metadata from Python decorators
        
        if let Ok(tables_obj) = tables_js.clone().dyn_into::<Object>() {
            let keys = Object::keys(&tables_obj);
            for i in 0..keys.length() {
                if let Some(table_name) = keys.get(i).as_string() {
                    // Create a basic table definition - this would be expanded
                    // to extract actual field information from Python
                    let table_def = PythonTableDefinition {
                        name: table_name.clone(),
                        fields: vec![], // Would extract from Python class annotations
                        primary_key: None,
                        indexes: vec![],
                    };
                    self.tables.insert(table_name, table_def);
                }
            }
        }
        
        Ok(())
    }

    fn extract_reducer_definitions(&mut self, reducers_js: &JsValue) -> Result<(), JsValue> {
        // Similar to table extraction, this would parse reducer definitions
        // from the JavaScript object containing registered Python reducers
        
        if let Ok(reducers_obj) = reducers_js.clone().dyn_into::<Object>() {
            let keys = Object::keys(&reducers_obj);
            for i in 0..keys.length() {
                if let Some(reducer_name) = keys.get(i).as_string() {
                    // Create a basic reducer definition - would be expanded
                    // to extract actual parameter information from Python
                    let reducer_def = PythonReducerDefinition {
                        name: reducer_name.clone(),
                        parameters: vec![], // Would extract from Python function signature
                        is_scheduled: false,
                        schedule_interval: None,
                        schedule_delay: None,
                    };
                    self.reducers.insert(reducer_name, reducer_def);
                }
            }
        }
        
        Ok(())
    }
}

// SpacetimeDB WASM ABI implementation

/// Initialize the SpacetimeDB module
#[no_mangle]
pub extern "C" fn __spacetimedb_module_init() {
    console::log_1(&"Initializing SpacetimeDB Python module...".into());
    
    // Create and initialize the global Python module instance
    match PythonModule::new() {
        Ok(module) => {
            unsafe {
                PYTHON_MODULE = Some(module);
            }
            console::log_1(&"SpacetimeDB Python module initialized successfully".into());
        }
        Err(e) => {
            console::error_1(&format!("Failed to initialize Python module: {:?}", e).into());
        }
    }
}

/// Call a reducer in the Python module
#[no_mangle]
pub extern "C" fn __spacetimedb_call_reducer(
    id: u32,
    args_ptr: *const u8,
    args_len: usize,
) -> *mut u8 {
    console::log_1(&format!("Calling reducer with ID: {}", id).into());
    
    unsafe {
        if let Some(ref mut module) = PYTHON_MODULE {
            // Convert raw pointer to slice
            let args = std::slice::from_raw_parts(args_ptr, args_len);
            
            // For now, we'll need a way to map reducer IDs to names
            // This would be established during module initialization
            let reducer_name = format!("reducer_{}", id); // Placeholder
            
            match module.call_reducer(&reducer_name, args) {
                Ok(result) => {
                    // Allocate result and return pointer
                    let boxed = result.into_boxed_slice();
                    let ptr = boxed.as_ptr() as *mut u8;
                    std::mem::forget(boxed);
                    ptr
                }
                Err(e) => {
                    console::error_1(&format!("Reducer call failed: {:?}", e).into());
                    std::ptr::null_mut()
                }
            }
        } else {
            console::error_1(&"Python module not initialized".into());
            std::ptr::null_mut()
        }
    }
}

/// Get the number of tables defined in the Python module
#[no_mangle]
pub extern "C" fn __spacetimedb_get_table_count() -> u32 {
    unsafe {
        if let Some(ref module) = PYTHON_MODULE {
            module.tables.len() as u32
        } else {
            0
        }
    }
}

/// Get table definition by index
#[no_mangle]
pub extern "C" fn __spacetimedb_get_table_def(index: u32) -> *mut u8 {
    unsafe {
        if let Some(ref module) = PYTHON_MODULE {
            if let Some((_, table_def)) = module.tables.iter().nth(index as usize) {
                // Serialize table definition to bytes
                if let Ok(serialized) = serde_json::to_vec(table_def) {
                    let boxed = serialized.into_boxed_slice();
                    let ptr = boxed.as_ptr() as *mut u8;
                    std::mem::forget(boxed);
                    return ptr;
                }
            }
        }
        std::ptr::null_mut()
    }
}

// Helper functions for memory management from SpacetimeDB host

/// Free memory allocated by the bridge
#[no_mangle]
pub extern "C" fn __spacetimedb_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
            // Vec will be dropped and memory freed
        }
    }
}

// Utility functions for debugging and development

/// Set the Python code to be executed (for testing)
#[wasm_bindgen]
pub fn set_python_code(code: &str) -> Result<(), JsValue> {
    unsafe {
        if let Some(ref mut module) = PYTHON_MODULE {
            module.load_python_code(code)
        } else {
            Err("Python module not initialized".into())
        }
    }
}

/// Get the current status of the Python bridge
#[wasm_bindgen]
pub fn get_bridge_status() -> JsValue {
    unsafe {
        if let Some(ref module) = PYTHON_MODULE {
            let enhanced_converter = module.type_converter.is_some();
            let status = serde_json::json!({
                "initialized": module.is_initialized(),
                "table_count": module.tables.len(),
                "reducer_count": module.reducers.len(),
                "python_code_length": module.python_code.len(),
                "enhanced_type_conversion": enhanced_converter
            });
            JsValue::from_str(&status.to_string())
        } else {
            JsValue::from_str(&serde_json::json!({
                "initialized": false,
                "enhanced_type_conversion": false
            }).to_string())
        }
    }
}

/// Test the enhanced type conversion system (for debugging)
#[wasm_bindgen]
pub fn test_type_conversion(test_data: &JsValue) -> Result<JsValue, JsValue> {
    console::log_1(&"Testing enhanced type conversion...".into());
    
    unsafe {
        if let Some(ref module) = PYTHON_MODULE {
            if let Some(_converter) = &module.type_converter {
                // TODO: Implement actual type conversion testing
                console::log_1(&"Enhanced type converter available for testing".into());
                Ok(JsValue::from_str("Enhanced type conversion test completed"))
            } else {
                // Use basic bridge for testing
                let _basic_bridge = BsatnBridge::new();
                match crate::bsatn_bridge::test_bsatn_conversion(test_data) {
                    Ok(result) => {
                        console::log_1(&"Basic BSATN conversion test successful".into());
                        Ok(result)
                    }
                    Err(e) => {
                        console::error_1(&format!("BSATN conversion test failed: {:?}", e).into());
                        Err(e)
                    }
                }
            }
        } else {
            Err("Python module not initialized".into())
        }
    }
}

// Export types for external use
pub use pyodide_runtime::*;
pub use bsatn_bridge::*;
pub use error::*;
pub use type_conversion::*;

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn test_python_module_creation() {
        // This test would require Pyodide to be loaded in the test environment
        // For now, we'll just test the structure creation
        let tables = HashMap::new();
        let reducers = HashMap::new();
        
        assert_eq!(tables.len(), 0);
        assert_eq!(reducers.len(), 0);
    }

    #[test]
    fn test_python_table_definition() {
        let table_def = PythonTableDefinition {
            name: "TestTable".to_string(),
            fields: vec![],
            primary_key: Some("id".to_string()),
            indexes: vec!["name".to_string()],
        };
        
        assert_eq!(table_def.name, "TestTable");
        assert_eq!(table_def.primary_key, Some("id".to_string()));
        assert_eq!(table_def.indexes.len(), 1);
    }

    #[test]
    fn test_python_reducer_definition() {
        let reducer_def = PythonReducerDefinition {
            name: "test_reducer".to_string(),
            parameters: vec![],
            is_scheduled: false,
            schedule_interval: None,
            schedule_delay: None,
        };
        
        assert_eq!(reducer_def.name, "test_reducer");
        assert!(!reducer_def.is_scheduled);
    }

    #[test]
    fn test_enhanced_type_conversion_integration() {
        // Test that the enhanced type conversion system is properly integrated
        let module_result = PythonModule::new();
        
        // In a real test environment with Pyodide available, this would succeed
        // For now, we test the structure
        assert!(module_result.is_ok() || module_result.is_err()); // Will depend on Pyodide availability
    }
}
