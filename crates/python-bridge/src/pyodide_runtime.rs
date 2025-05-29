//! Pyodide Runtime Integration
//! 
//! This module handles the integration with the Pyodide WebAssembly Python runtime,
//! providing methods to initialize Pyodide, execute Python code, and call Python functions.

use wasm_bindgen::prelude::*;
use js_sys::{Object, Reflect, Function};
use web_sys::console;

/// Pyodide runtime manager that handles the Python execution environment
pub struct PyodideRuntime {
    pyodide: Object,
    python_globals: Object,
    initialized: bool,
}

impl PyodideRuntime {
    /// Create a new Pyodide runtime instance
    pub fn new() -> Result<Self, JsValue> {
        console::log_1(&"Creating new Pyodide runtime...".into());
        
        // For now, we'll create a placeholder that assumes Pyodide is already loaded
        // In a real implementation, this would handle the async loading of Pyodide
        let pyodide = Self::get_or_load_pyodide()?;
        let python_globals = Self::get_python_globals(&pyodide)?;
        
        let mut runtime = Self {
            pyodide,
            python_globals,
            initialized: false,
        };
        
        // Initialize the SpacetimeDB Python environment
        runtime.initialize_spacetimedb_environment()?;
        runtime.initialized = true;
        
        console::log_1(&"Pyodide runtime created successfully".into());
        Ok(runtime)
    }

    /// Execute Python code in the Pyodide runtime
    pub fn run_python(&self, code: &str) -> Result<JsValue, JsValue> {
        if !self.initialized {
            return Err("Pyodide runtime not initialized".into());
        }

        console::log_1(&format!("Executing Python code: {} chars", code.len()).into());
        
        // Call pyodide.runPython(code)
        let run_python_func = Reflect::get(&self.pyodide, &"runPython".into())?;
        let run_python_func: Function = run_python_func.dyn_into()
            .map_err(|_| "runPython is not a function")?;
        
        let result = run_python_func.call1(&self.pyodide, &code.into())?;
        
        console::log_1(&"Python code executed successfully".into());
        Ok(result)
    }

    /// Call a Python function by name with arguments
    pub fn call_python_function(&self, func_name: &str, args: &JsValue) -> Result<JsValue, JsValue> {
        if !self.initialized {
            return Err("Pyodide runtime not initialized".into());
        }

        console::log_1(&format!("Calling Python function: {}", func_name).into());
        
        // Get the function from Python globals
        let func = Reflect::get(&self.python_globals, &func_name.into())?;
        
        if func.is_undefined() {
            return Err(format!("Python function '{}' not found", func_name).into());
        }
        
        let func: Function = func.dyn_into()
            .map_err(|_| format!("'{}' is not a function", func_name))?;
        
        // Call the function with the provided arguments
        let result = if args.is_undefined() {
            func.call0(&JsValue::undefined())?
        } else {
            func.call1(&JsValue::undefined(), args)?
        };
        
        console::log_1(&format!("Python function '{}' called successfully", func_name).into());
        Ok(result)
    }

    /// Install a Python package using micropip
    pub fn install_package(&self, package_name: &str) -> Result<(), JsValue> {
        console::log_1(&format!("Installing Python package: {}", package_name).into());
        
        let install_code = format!(
            "import micropip; await micropip.install('{}')", 
            package_name
        );
        
        self.run_python(&install_code)?;
        
        console::log_1(&format!("Package '{}' installed successfully", package_name).into());
        Ok(())
    }

    /// Get a value from Python globals
    pub fn get_python_global(&self, name: &str) -> Result<JsValue, JsValue> {
        Reflect::get(&self.python_globals, &name.into())
    }

    /// Set a value in Python globals
    pub fn set_python_global(&self, name: &str, value: &JsValue) -> Result<(), JsValue> {
        Reflect::set(&self.python_globals, &name.into(), value)?;
        Ok(())
    }

    /// Check if the runtime is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get the Python globals object
    pub fn globals(&self) -> &Object {
        &self.python_globals
    }

    /// Evaluate a Python expression and return the result
    pub fn eval_python(&self, expression: &str) -> Result<JsValue, JsValue> {
        console::log_1(&format!("Evaluating Python expression: {}", expression).into());
        
        // Wrap the expression in eval() to get the result
        let eval_code = format!("eval('{}')", expression.replace("'", "\\'"));
        self.run_python(&eval_code)
    }
}

impl PyodideRuntime {
    /// Get or load the Pyodide instance
    fn get_or_load_pyodide() -> Result<Object, JsValue> {
        // Check if Pyodide is already available in the global scope
        let global = js_sys::global();
        let pyodide = Reflect::get(&global, &"pyodide".into())?;
        
        if !pyodide.is_undefined() {
            console::log_1(&"Using existing Pyodide instance".into());
            return Ok(pyodide.dyn_into()?);
        }
        
        // If Pyodide is not available, we need to load it
        // For now, we'll return an error and assume it should be pre-loaded
        // In a real implementation, this would handle the async loading
        Err("Pyodide not found. Please ensure Pyodide is loaded before initializing the bridge.".into())
    }

    /// Get the Python globals object from Pyodide
    fn get_python_globals(pyodide: &Object) -> Result<Object, JsValue> {
        let globals = Reflect::get(pyodide, &"globals".into())?;
        globals.dyn_into()
            .map_err(|_| "Failed to get Python globals".into())
    }

    /// Initialize the SpacetimeDB Python environment
    fn initialize_spacetimedb_environment(&mut self) -> Result<(), JsValue> {
        console::log_1(&"Initializing SpacetimeDB Python environment...".into());
        
        // Install the spacetimedb-server package (this would be a real package in production)
        // For now, we'll inject the basic infrastructure code directly
        let init_code = r#"
# SpacetimeDB Server Environment Initialization

import sys
import json
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import inspect

# Global registries for tables and reducers
_registered_tables: Dict[str, Any] = {}
_registered_reducers: Dict[str, Callable] = {}
_registered_scheduled: Dict[str, Callable] = {}

# Mock SpacetimeDB types for initial development
class Identity:
    def __init__(self, value: str):
        self.value = value
    
    def __str__(self):
        return self.value

class ReducerContext:
    def __init__(self, sender: Identity, timestamp: int):
        self.sender = sender
        self.timestamp = timestamp
    
    def log(self, message: str, level: str = "info"):
        print(f"[{level.upper()}] {message}")

# Table decorator
def table(cls=None, *, name: Optional[str] = None, primary_key: Optional[str] = None, indexes: Optional[List[str]] = None):
    def decorator(cls):
        cls._spacetimedb_table = True
        cls._spacetimedb_name = name or cls.__name__
        cls._spacetimedb_primary_key = primary_key
        cls._spacetimedb_indexes = indexes or []
        
        # Register the table
        _registered_tables[cls._spacetimedb_name] = cls
        
        return cls
    
    if cls is None:
        return decorator
    else:
        return decorator(cls)

# Reducer decorator
def reducer(func: Callable) -> Callable:
    func._spacetimedb_reducer = True
    func._spacetimedb_name = func.__name__
    func._spacetimedb_signature = inspect.signature(func)
    
    # Register the reducer
    _registered_reducers[func.__name__] = func
    
    return func

# Scheduled reducer decorator
def scheduled(interval_ms: Optional[int] = None, delay_ms: Optional[int] = None, repeating: bool = True):
    def decorator(func: Callable) -> Callable:
        func._spacetimedb_scheduled = True
        func._spacetimedb_interval = interval_ms
        func._spacetimedb_delay = delay_ms
        func._spacetimedb_repeating = repeating
        func._spacetimedb_name = func.__name__
        
        # Register the scheduled reducer
        _registered_scheduled[func.__name__] = func
        _registered_reducers[func.__name__] = func  # Also register as regular reducer
        
        return func
    
    return decorator

# Functions to get registered items (called by the bridge)
def get_registered_tables() -> Dict[str, Any]:
    return _registered_tables.copy()

def get_registered_reducers() -> Dict[str, Callable]:
    return _registered_reducers.copy()

def get_registered_scheduled() -> Dict[str, Callable]:
    return _registered_scheduled.copy()

# Basic database interface placeholder
class Database:
    def __init__(self):
        pass
    
    def get_table(self, table_class):
        return TableHandle(table_class)

class TableHandle:
    def __init__(self, table_class):
        self.table_class = table_class
        self.table_name = table_class.__name__
    
    def insert(self, row):
        # Placeholder for table operations
        print(f"INSERT into {self.table_name}: {row}")
        return row
    
    def scan(self, **filters):
        # Placeholder for table operations
        print(f"SCAN {self.table_name} with filters: {filters}")
        return []
    
    def delete(self, **filters):
        # Placeholder for table operations
        print(f"DELETE from {self.table_name} with filters: {filters}")
        return 0

print("SpacetimeDB Python environment initialized")
"#;
        
        self.run_python(init_code)?;
        
        console::log_1(&"SpacetimeDB Python environment initialized successfully".into());
        Ok(())
    }
}

/// Helper function to check if Pyodide is available
#[wasm_bindgen]
pub fn is_pyodide_available() -> bool {
    let global = js_sys::global();
    let pyodide = Reflect::get(&global, &"pyodide".into()).unwrap_or(JsValue::undefined());
    !pyodide.is_undefined()
}

/// Helper function to get Pyodide version (for debugging)
#[wasm_bindgen]
pub fn get_pyodide_version() -> Result<String, JsValue> {
    let global = js_sys::global();
    let pyodide = Reflect::get(&global, &"pyodide".into())?;
    
    if pyodide.is_undefined() {
        return Err("Pyodide not available".into());
    }
    
    let version = Reflect::get(&pyodide, &"version".into())?;
    version.as_string().ok_or_else(|| "Could not get Pyodide version".into())
}

/// Load Pyodide asynchronously (placeholder for future implementation)
#[wasm_bindgen]
pub async fn load_pyodide() -> Result<JsValue, JsValue> {
    // This would be implemented to load Pyodide from CDN or local files
    // For now, it's a placeholder that assumes Pyodide is already available
    Err("Pyodide loading not yet implemented. Please ensure Pyodide is pre-loaded.".into())
}
