//! BSATN Serialization Bridge
//! 
//! This module provides bidirectional conversion between SpacetimeDB's BSATN format
//! and Python objects, enabling data exchange for reducer calls and table operations.

use wasm_bindgen::prelude::*;
use js_sys::{Object, Array, Reflect};
use web_sys::console;
use spacetimedb_lib::AlgebraicValue;
use spacetimedb_sats::{AlgebraicType, ArrayValue, SumValue};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Bridge for converting between BSATN and Python objects
pub struct BsatnBridge {
    /// Cache for type conversions to improve performance
    type_cache: HashMap<String, AlgebraicType>,
}

impl BsatnBridge {
    /// Create a new BSATN bridge
    pub fn new() -> Self {
        Self {
            type_cache: HashMap::new(),
        }
    }

    /// Convert BSATN AlgebraicValue to Python-compatible JsValue
    pub fn bsatn_to_python(&self, value: &AlgebraicValue) -> Result<JsValue, JsValue> {
        console::log_1(&"Converting BSATN to Python object".into());
        
        match value {
            AlgebraicValue::Bool(b) => Ok(JsValue::from(*b)),
            AlgebraicValue::I8(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U8(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I16(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U16(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I32(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U32(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I64(i) => Ok(JsValue::from(*i as f64)), // JavaScript limitation
            AlgebraicValue::U64(u) => Ok(JsValue::from(*u as f64)), // JavaScript limitation
            AlgebraicValue::I128(i) => {
                let i_copy = i.0;
                Ok(JsValue::from_str(&i_copy.to_string()))
            },
            AlgebraicValue::U128(u) => {
                let u_copy = u.0;
                Ok(JsValue::from_str(&u_copy.to_string()))
            },
            AlgebraicValue::F32(f) => Ok(JsValue::from(f.into_inner())),
            AlgebraicValue::F64(f) => Ok(JsValue::from(f.into_inner())),
            AlgebraicValue::String(s) => Ok(JsValue::from_str(s)),
            AlgebraicValue::Product(elements) => self.product_to_python(&elements.elements),
            AlgebraicValue::Sum(variant) => self.sum_to_python(variant),
            AlgebraicValue::Array(elements) => self.array_to_python_from_array_value(elements),
            _ => Err("Unsupported AlgebraicValue variant for Python conversion".into()),
        }
    }

    /// Convert Python JsValue to BSATN AlgebraicValue
    pub fn python_to_bsatn(&self, value: &JsValue) -> Result<AlgebraicValue, JsValue> {
        console::log_1(&"Converting Python object to BSATN".into());
        
        if value.is_null() || value.is_undefined() {
            return Err("Cannot convert null/undefined to BSATN".into());
        }
        
        if let Some(b) = value.as_bool() {
            return Ok(AlgebraicValue::Bool(b));
        }
        
        if let Some(s) = value.as_string() {
            return Ok(AlgebraicValue::String(s.into()));
        }
        
        if let Some(n) = value.as_f64() {
            // Try to determine the most appropriate numeric type
            if n.fract() == 0.0 {
                // It's an integer
                if n >= 0.0 && n <= u32::MAX as f64 {
                    return Ok(AlgebraicValue::U32(n as u32));
                } else if n >= i32::MIN as f64 && n <= i32::MAX as f64 {
                    return Ok(AlgebraicValue::I32(n as i32));
                } else {
                    return Ok(AlgebraicValue::I64(n as i64));
                }
            } else {
                // It's a float
                return Ok(AlgebraicValue::F64(n.into()));
            }
        }
        
        if Array::is_array(value) {
            return self.python_array_to_bsatn(value);
        }
        
        if value.is_object() {
            return self.python_object_to_bsatn(value);
        }
        
        Err("Unsupported Python type for BSATN conversion".into())
    }

    /// Convert BSATN arguments (byte array) to Python arguments
    pub fn bsatn_to_python_args(&self, args_bytes: &[u8]) -> Result<JsValue, JsValue> {
        console::log_1(&format!("Converting {} bytes of BSATN args to Python", args_bytes.len()).into());
        
        // For now, we'll create a simple JSON-based conversion
        // In a real implementation, this would use proper BSATN deserialization
        let args_json = String::from_utf8(args_bytes.to_vec())
            .map_err(|_| "Invalid UTF-8 in BSATN args")?;
        
        // Parse as JSON and convert to JavaScript value
        let parsed: JsonValue = serde_json::from_str(&args_json)
            .map_err(|e| format!("Failed to parse args JSON: {}", e))?;
        
        self.json_to_js_value(&parsed)
    }

    /// Convert Python result to BSATN bytes
    pub fn python_to_bsatn_result(&self, result: &JsValue) -> Result<Vec<u8>, JsValue> {
        console::log_1(&"Converting Python result to BSATN bytes".into());
        
        // Convert to BSATN value
        let bsatn_value = self.python_to_bsatn(result)?;
        
        // For now, serialize as JSON (in a real implementation, use proper BSATN serialization)
        let json_result = self.bsatn_to_json(&bsatn_value)?;
        let json_string = serde_json::to_string(&json_result)
            .map_err(|e| format!("Failed to serialize result: {}", e))?;
        
        Ok(json_string.into_bytes())
    }
}

impl BsatnBridge {
    /// Convert BSATN Product to Python object/dict
    fn product_to_python(&self, elements: &[AlgebraicValue]) -> Result<JsValue, JsValue> {
        let obj = Object::new();
        
        for (index, element) in elements.iter().enumerate() {
            let key = JsValue::from(index);
            let value = self.bsatn_to_python(element)?;
            Reflect::set(&obj, &key, &value)?;
        }
        
        Ok(obj.into())
    }

    /// Convert BSATN Sum (union type) to Python representation
    fn sum_to_python(&self, variant: &SumValue) -> Result<JsValue, JsValue> {
        let obj = Object::new();
        
        // Create a union-like structure: { "variant": tag, "value": data }
        let tag = JsValue::from(variant.tag as u32);
        let value = self.bsatn_to_python(&variant.value)?;
        
        Reflect::set(&obj, &"variant".into(), &tag)?;
        Reflect::set(&obj, &"value".into(), &value)?;
        
        Ok(obj.into())
    }

    /// Convert BSATN Array to Python list
    fn array_to_python(&self, elements: &[AlgebraicValue]) -> Result<JsValue, JsValue> {
        let array = Array::new();
        
        for element in elements {
            let py_element = self.bsatn_to_python(element)?;
            array.push(&py_element);
        }
        
        Ok(array.into())
    }

    /// Convert BSATN ArrayValue to Python list
    fn array_to_python_from_array_value(&self, array_val: &ArrayValue) -> Result<JsValue, JsValue> {
        let array = Array::new();
        
        // Convert ArrayValue elements to AlgebraicValues and then to Python
        // This is a simplified conversion - in reality we'd need to handle different array types
        match array_val {
            ArrayValue::Bool(bools) => {
                for b in bools.iter() {
                    array.push(&JsValue::from(*b));
                }
            }
            ArrayValue::I8(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::U8(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::I16(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::U16(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::I32(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::U32(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v));
                }
            }
            ArrayValue::I64(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v as f64));
                }
            }
            ArrayValue::U64(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(*v as f64));
                }
            }
            ArrayValue::F32(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(v.into_inner()));
                }
            }
            ArrayValue::F64(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from(v.into_inner()));
                }
            }
            ArrayValue::String(vals) => {
                for v in vals.iter() {
                    array.push(&JsValue::from_str(v));
                }
            }
            _ => {
                // For complex types, we'd need proper conversion
                return Err("Unsupported ArrayValue type for Python conversion".into());
            }
        }
        
        Ok(array.into())
    }

    /// Convert BSATN Map to Python dict
    fn map_to_python(&self, map: &[(AlgebraicValue, AlgebraicValue)]) -> Result<JsValue, JsValue> {
        let obj = Object::new();
        
        for (key, value) in map {
            // Convert key to string representation for JavaScript object
            let key_str = match key {
                AlgebraicValue::String(s) => s.to_string(),
                _ => format!("{:?}", key), // Fallback for non-string keys
            };
            
            let py_value = self.bsatn_to_python(value)?;
            Reflect::set(&obj, &key_str.into(), &py_value)?;
        }
        
        Ok(obj.into())
    }

    /// Convert Python array to BSATN Array
    fn python_array_to_bsatn(&self, array: &JsValue) -> Result<AlgebraicValue, JsValue> {
        let js_array: Array = array.clone().dyn_into()
            .map_err(|_| "Expected array")?;
        
        let mut elements = Vec::new();
        for i in 0..js_array.length() {
            let element = js_array.get(i);
            let bsatn_element = self.python_to_bsatn(&element)?;
            elements.push(bsatn_element);
        }
        
        // Convert Vec<AlgebraicValue> to ArrayValue
        // For simplicity, we'll handle the most common case - assume homogeneous arrays
        if elements.is_empty() {
            return Ok(AlgebraicValue::Array(ArrayValue::U8(Box::new([]))));
        }
        
        // Check the type of the first element to determine array type
        match &elements[0] {
            AlgebraicValue::Bool(_) => {
                let bools: Result<Vec<_>, _> = elements.iter().map(|e| 
                    if let AlgebraicValue::Bool(b) = e { Ok(*b) } else { Err("Mixed types in array") }).collect();
                match bools {
                    Ok(b) => Ok(AlgebraicValue::Array(ArrayValue::Bool(b.into_boxed_slice()))),
                    Err(_) => Err("Mixed types in boolean array".into()),
                }
            },
            AlgebraicValue::I32(_) => {
                let ints: Result<Vec<_>, _> = elements.iter().map(|e| 
                    if let AlgebraicValue::I32(i) = e { Ok(*i) } else { Err("Mixed types in array") }).collect();
                match ints {
                    Ok(i) => Ok(AlgebraicValue::Array(ArrayValue::I32(i.into_boxed_slice()))),
                    Err(_) => Err("Mixed types in i32 array".into()),
                }
            },
            AlgebraicValue::String(_) => {
                let strings: Result<Vec<_>, _> = elements.iter().map(|e| 
                    if let AlgebraicValue::String(s) = e { Ok(s.as_ref().into()) } else { Err("Mixed types in array") }).collect();
                match strings {
                    Ok(s) => Ok(AlgebraicValue::Array(ArrayValue::String(s.into_boxed_slice()))),
                    Err(_) => Err("Mixed types in string array".into()),
                }
            },
            _ => {
                // For other types, create a generic array
                // Convert elements to a compatible type or use Product
                Ok(AlgebraicValue::Product(elements.into()))
            }
        }
    }

    /// Convert Python object to BSATN Product or Map
    fn python_object_to_bsatn(&self, obj: &JsValue) -> Result<AlgebraicValue, JsValue> {
        let js_obj: Object = obj.clone().dyn_into()
            .map_err(|_| "Expected object")?;
        
        let keys = Object::keys(&js_obj);
        let mut elements = Vec::new();
        
        // Check if this looks like a numeric-indexed object (Product) or named object (Map)
        let mut is_numeric_indexed = true;
        for i in 0..keys.length() {
            if let Some(key) = keys.get(i).as_string() {
                if key.parse::<usize>().is_err() {
                    is_numeric_indexed = false;
                    break;
                }
            }
        }
        
        if is_numeric_indexed && keys.length() > 0 {
            // Treat as Product (indexed elements)
            for i in 0..keys.length() {
                let key = JsValue::from(i);
                let value = Reflect::get(&js_obj, &key)?;
                let bsatn_value = self.python_to_bsatn(&value)?;
                elements.push(bsatn_value);
            }
            Ok(AlgebraicValue::Product(elements.into()))
        } else {
            // Treat as Product with string keys converted to indices
            // In a real implementation, we'd need proper mapping or use a different approach
            for i in 0..keys.length() {
                let key = JsValue::from(i);
                let value = Reflect::get(&js_obj, &key)?;
                let bsatn_value = self.python_to_bsatn(&value)?;
                elements.push(bsatn_value);
            }
            Ok(AlgebraicValue::Product(elements.into()))
        }
    }

    /// Convert JSON Value to JavaScript Value
    fn json_to_js_value(&self, json: &JsonValue) -> Result<JsValue, JsValue> {
        match json {
            JsonValue::Null => Ok(JsValue::null()),
            JsonValue::Bool(b) => Ok(JsValue::from(*b)),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(JsValue::from(i as f64))
                } else if let Some(f) = n.as_f64() {
                    Ok(JsValue::from(f))
                } else {
                    Err("Invalid number in JSON".into())
                }
            }
            JsonValue::String(s) => Ok(JsValue::from_str(s)),
            JsonValue::Array(arr) => {
                let js_array = Array::new();
                for item in arr {
                    let js_item = self.json_to_js_value(item)?;
                    js_array.push(&js_item);
                }
                Ok(js_array.into())
            }
            JsonValue::Object(obj) => {
                let js_obj = Object::new();
                for (key, value) in obj {
                    let js_value = self.json_to_js_value(value)?;
                    Reflect::set(&js_obj, &key.into(), &js_value)?;
                }
                Ok(js_obj.into())
            }
        }
    }

    /// Convert BSATN value to JSON (for serialization)
    fn bsatn_to_json(&self, value: &AlgebraicValue) -> Result<JsonValue, JsValue> {
        match value {
            AlgebraicValue::Bool(b) => Ok(JsonValue::Bool(*b)),
            AlgebraicValue::I8(i) => Ok(JsonValue::Number((*i).into())),
            AlgebraicValue::U8(u) => Ok(JsonValue::Number((*u).into())),
            AlgebraicValue::I16(i) => Ok(JsonValue::Number((*i).into())),
            AlgebraicValue::U16(u) => Ok(JsonValue::Number((*u).into())),
            AlgebraicValue::I32(i) => Ok(JsonValue::Number((*i).into())),
            AlgebraicValue::U32(u) => Ok(JsonValue::Number((*u).into())),
            AlgebraicValue::I64(i) => Ok(JsonValue::Number(serde_json::Number::from(*i))),
            AlgebraicValue::U64(u) => Ok(JsonValue::Number(serde_json::Number::from(*u))),
            AlgebraicValue::I128(i) => {
                let i_copy = i.0;
                Ok(JsonValue::String(i_copy.to_string()))
            },
            AlgebraicValue::U128(u) => {
                let u_copy = u.0;
                Ok(JsonValue::String(u_copy.to_string()))
            },
            AlgebraicValue::F32(f) => Ok(JsonValue::Number(serde_json::Number::from_f64(f.into_inner() as f64).unwrap())),
            AlgebraicValue::F64(f) => Ok(JsonValue::Number(serde_json::Number::from_f64(f.into_inner()).unwrap())),
            AlgebraicValue::String(s) => Ok(JsonValue::String(s.to_string())),
            AlgebraicValue::Product(elements) => {
                let mut arr = Vec::new();
                for element in &elements.elements {
                    arr.push(self.bsatn_to_json(element)?);
                }
                Ok(JsonValue::Array(arr))
            }
            AlgebraicValue::Sum(variant) => {
                let mut obj = serde_json::Map::new();
                obj.insert("variant".to_string(), JsonValue::Number((variant.tag as u32).into()));
                obj.insert("value".to_string(), self.bsatn_to_json(&variant.value)?);
                Ok(JsonValue::Object(obj))
            }
            AlgebraicValue::Array(array_val) => {
                let mut arr = Vec::new();
                // Handle different ArrayValue types 
                match array_val {
                    ArrayValue::Bool(bools) => {
                        for b in bools.iter() {
                            arr.push(JsonValue::Bool(*b));
                        }
                    }
                    ArrayValue::I32(ints) => {
                        for i in ints.iter() {
                            arr.push(JsonValue::Number((*i).into()));
                        }
                    }
                    ArrayValue::String(strings) => {
                        for s in strings.iter() {
                            arr.push(JsonValue::String(s.to_string()));
                        }
                    }
                    _ => {
                        // For other types, create empty array for now
                        // In real implementation, handle all ArrayValue variants
                    }
                }
                Ok(JsonValue::Array(arr))
            }
            _ => Err("Unsupported AlgebraicValue variant for JSON conversion".into()),
        }
    }
}

/// Helper function to create a BSATN bridge instance
pub fn create_bsatn_bridge() -> BsatnBridge {
    BsatnBridge::new()
}

/// Test function for BSATN conversion (for debugging)
pub fn test_bsatn_conversion(test_data: &JsValue) -> Result<JsValue, JsValue> {
    let bridge = BsatnBridge::new();
    
    // Convert to BSATN and back
    let bsatn_value = bridge.python_to_bsatn(test_data)?;
    let back_to_python = bridge.bsatn_to_python(&bsatn_value)?;
    
    console::log_1(&"BSATN round-trip conversion successful".into());
    Ok(back_to_python)
}
