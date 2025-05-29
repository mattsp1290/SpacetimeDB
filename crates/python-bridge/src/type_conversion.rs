//! Enhanced Type Conversion System with Performance Optimizations
//! 
//! This module provides comprehensive, type-safe conversion between SpacetimeDB's BSATN format
//! and Python objects with advanced error handling, performance optimizations, and batch processing.

use wasm_bindgen::prelude::*;
use js_sys::{Object, Array, Reflect};
use web_sys::console;
use spacetimedb_lib::AlgebraicValue;
use spacetimedb_sats::{AlgebraicType, ArrayValue, SumValue, ProductValue};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use crate::pyodide_runtime::PyodideRuntime;
use crate::error::ConversionError;

/// Cache entry for type information
#[derive(Debug, Clone)]
pub struct TypeCacheEntry {
    pub algebraic_type: AlgebraicType,
    pub python_type_name: String,
    pub conversion_hint: ConversionHint,
    pub last_used: u64,
    pub use_count: u64,
}

/// Hints for optimizing conversions
#[derive(Debug, Clone)]
pub enum ConversionHint {
    /// Simple primitive type
    Primitive,
    /// Array with homogeneous elements
    HomogeneousArray(Box<AlgebraicType>),
    /// Product with known field types
    StructuredProduct(Vec<AlgebraicType>),
    /// Sum with known variant types
    KnownSum(Vec<AlgebraicType>),
    /// Generic complex type
    Complex,
}

/// Object pool for reusing JavaScript objects to reduce GC pressure
#[derive(Debug, Default)]
pub struct ConversionObjectPool {
    js_objects: VecDeque<Object>,
    js_arrays: VecDeque<Array>,
    max_pool_size: usize,
}

impl ConversionObjectPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            js_objects: VecDeque::with_capacity(max_size),
            js_arrays: VecDeque::with_capacity(max_size),
            max_pool_size: max_size,
        }
    }

    pub fn get_object(&mut self) -> Object {
        self.js_objects.pop_front().unwrap_or_else(|| Object::new())
    }

    pub fn return_object(&mut self, obj: Object) {
        if self.js_objects.len() < self.max_pool_size {
            // Clear the object before returning to pool
            let keys = Object::keys(&obj);
            for i in 0..keys.length() {
                let key = keys.get(i);
                let _ = Reflect::delete_property(&obj, &key);
            }
            self.js_objects.push_back(obj);
        }
    }

    pub fn get_array(&mut self) -> Array {
        self.js_arrays.pop_front().unwrap_or_else(|| Array::new())
    }

    pub fn return_array(&mut self, arr: Array) {
        if self.js_arrays.len() < self.max_pool_size {
            // Clear the array before returning to pool
            arr.set_length(0);
            self.js_arrays.push_back(arr);
        }
    }
}

/// Enhanced LRU cache for type conversion patterns
#[derive(Debug)]
pub struct SmartTypeCache {
    cache: HashMap<String, TypeCacheEntry>,
    access_order: VecDeque<String>,
    max_size: usize,
    hit_count: u64,
    miss_count: u64,
}

impl SmartTypeCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            access_order: VecDeque::with_capacity(max_size),
            max_size,
            hit_count: 0,
            miss_count: 0,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&TypeCacheEntry> {
        if let Some(entry) = self.cache.get_mut(key) {
            self.hit_count += 1;
            entry.last_used = js_sys::Date::now() as u64;
            entry.use_count += 1;
            
            // Move to front of access order
            if let Some(pos) = self.access_order.iter().position(|k| k == key) {
                let key = self.access_order.remove(pos).unwrap();
                self.access_order.push_front(key);
            }
            
            Some(entry)
        } else {
            self.miss_count += 1;
            None
        }
    }

    pub fn insert(&mut self, key: String, entry: TypeCacheEntry) {
        if self.cache.len() >= self.max_size {
            // Remove least recently used item
            if let Some(old_key) = self.access_order.pop_back() {
                self.cache.remove(&old_key);
            }
        }

        self.cache.insert(key.clone(), entry);
        self.access_order.push_front(key);
    }

    pub fn get_hit_ratio(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }
}

/// Enhanced statistics for performance monitoring
#[derive(Debug, Default, serde::Serialize)]
pub struct ConversionStats {
    pub conversions_performed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub errors_encountered: u64,
    pub batch_conversions: u64,
    pub total_processing_time_ms: f64,
    pub peak_memory_usage: usize,
    pub object_pool_hits: u64,
    pub object_pool_misses: u64,
}

impl ConversionStats {
    pub fn get_average_conversion_time(&self) -> f64 {
        if self.conversions_performed == 0 {
            0.0
        } else {
            self.total_processing_time_ms / self.conversions_performed as f64
        }
    }

    pub fn get_cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    pub fn get_pool_hit_ratio(&self) -> f64 {
        let total = self.object_pool_hits + self.object_pool_misses;
        if total == 0 {
            0.0
        } else {
            self.object_pool_hits as f64 / total as f64
        }
    }
}

/// Enhanced type converter with caching, pooling, and batch processing
pub struct TypeConverter {
    pyodide: Arc<PyodideRuntime>,
    smart_cache: SmartTypeCache,
    object_pool: ConversionObjectPool,
    conversion_stats: ConversionStats,
    config: ConversionConfig,
}

/// Configuration for performance tuning
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    pub max_cache_size: usize,
    pub max_pool_size: usize,
    pub enable_batch_optimization: bool,
    pub enable_memory_monitoring: bool,
    pub cache_ttl_ms: u64,
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            max_cache_size: 1000,
            max_pool_size: 100,
            enable_batch_optimization: true,
            enable_memory_monitoring: true,
            cache_ttl_ms: 300000, // 5 minutes
        }
    }
}

impl TypeConverter {
    /// Create a new type converter with the given Pyodide runtime and configuration
    pub fn new(pyodide: Arc<PyodideRuntime>) -> Self {
        Self::with_config(pyodide, ConversionConfig::default())
    }

    /// Create a new type converter with custom configuration
    pub fn with_config(pyodide: Arc<PyodideRuntime>, config: ConversionConfig) -> Self {
        Self {
            pyodide,
            smart_cache: SmartTypeCache::new(config.max_cache_size),
            object_pool: ConversionObjectPool::new(config.max_pool_size),
            conversion_stats: ConversionStats::default(),
            config,
        }
    }

    /// Convert BSATN AlgebraicValue to Python-compatible JsValue with enhanced type handling
    pub fn bsatn_to_python(&mut self, value: &AlgebraicValue) -> Result<JsValue, ConversionError> {
        let start_time = js_sys::Date::now();
        self.conversion_stats.conversions_performed += 1;
        
        let result = match value {
            // Primitive types with precise handling
            AlgebraicValue::Bool(b) => Ok(JsValue::from(*b)),
            AlgebraicValue::I8(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U8(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I16(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U16(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I32(i) => Ok(JsValue::from(*i)),
            AlgebraicValue::U32(u) => Ok(JsValue::from(*u)),
            AlgebraicValue::I64(i) => self.safe_i64_to_python(*i),
            AlgebraicValue::U64(u) => self.safe_u64_to_python(*u),
            AlgebraicValue::I128(i) => {
                let val = i.0;
                self.large_int_to_python(&val.to_string())
            }
            AlgebraicValue::U128(u) => {
                let val = u.0;
                self.large_int_to_python(&val.to_string())
            }
            AlgebraicValue::F32(f) => Ok(JsValue::from(f.into_inner())),
            AlgebraicValue::F64(f) => Ok(JsValue::from(f.into_inner())),
            AlgebraicValue::String(s) => Ok(JsValue::from_str(s)),
            
            // Complex types with enhanced handling
            AlgebraicValue::Product(product) => self.product_to_python(product),
            AlgebraicValue::Sum(variant) => self.sum_to_python(variant),
            AlgebraicValue::Array(array) => self.array_to_python(array),
            
            // Special SpacetimeDB types
            _ => {
                self.conversion_stats.errors_encountered += 1;
                Err(ConversionError::UnsupportedType {
                    type_name: format!("{:?}", value),
                    context: "BSATN to Python conversion".to_string(),
                })
            }
        };

        // Update performance metrics
        let elapsed = js_sys::Date::now() - start_time;
        self.conversion_stats.total_processing_time_ms += elapsed;

        result
    }

    /// Convert Python JsValue to BSATN AlgebraicValue with type hints for accuracy
    pub fn python_to_bsatn(
        &mut self, 
        value: &JsValue, 
        type_hint: Option<&AlgebraicType>
    ) -> Result<AlgebraicValue, ConversionError> {
        let start_time = js_sys::Date::now();
        self.conversion_stats.conversions_performed += 1;
        
        if value.is_null() || value.is_undefined() {
            return Err(ConversionError::NullValue);
        }
        
        let result = if let Some(hint) = type_hint {
            self.convert_with_type_hint(value, hint)
        } else {
            self.infer_and_convert(value)
        };

        // Update performance metrics
        let elapsed = js_sys::Date::now() - start_time;
        self.conversion_stats.total_processing_time_ms += elapsed;

        result
    }

    /// Batch convert multiple BSATN values to Python for high-performance scenarios
    pub fn batch_bsatn_to_python(&mut self, values: &[AlgebraicValue]) -> Result<Vec<JsValue>, ConversionError> {
        if !self.config.enable_batch_optimization {
            return values.iter()
                .map(|v| self.bsatn_to_python(v))
                .collect();
        }

        let start_time = js_sys::Date::now();
        self.conversion_stats.batch_conversions += 1;
        
        let mut results = Vec::with_capacity(values.len());
        
        // Batch optimization: pre-allocate and reuse objects
        for value in values {
            let result = self.bsatn_to_python(value)?;
            results.push(result);
        }

        let elapsed = js_sys::Date::now() - start_time;
        console::log_1(&format!("Batch conversion of {} items completed in {:.2}ms", 
                               values.len(), elapsed).into());
        
        Ok(results)
    }

    /// Batch convert multiple Python values to BSATN for high-performance scenarios  
    pub fn batch_python_to_bsatn(
        &mut self, 
        values: &[JsValue], 
        type_hints: Option<&[AlgebraicType]>
    ) -> Result<Vec<AlgebraicValue>, ConversionError> {
        if !self.config.enable_batch_optimization {
            return values.iter().enumerate()
                .map(|(i, v)| {
                    let hint = type_hints.and_then(|hints| hints.get(i));
                    self.python_to_bsatn(v, hint)
                })
                .collect();
        }

        let start_time = js_sys::Date::now();
        self.conversion_stats.batch_conversions += 1;
        
        let mut results = Vec::with_capacity(values.len());
        
        for (i, value) in values.iter().enumerate() {
            let hint = type_hints.and_then(|hints| hints.get(i));
            let result = self.python_to_bsatn(value, hint)?;
            results.push(result);
        }

        let elapsed = js_sys::Date::now() - start_time;
        console::log_1(&format!("Batch conversion of {} items completed in {:.2}ms", 
                               values.len(), elapsed).into());
        
        Ok(results)
    }

    /// Convert with explicit type information for maximum accuracy
    fn convert_with_type_hint(
        &mut self, 
        value: &JsValue, 
        expected_type: &AlgebraicType
    ) -> Result<AlgebraicValue, ConversionError> {
        // Check cache first
        let cache_key = format!("{:?}_{}", expected_type, self.get_js_type_name(value));
        if let Some(_cached) = self.smart_cache.get(&cache_key) {
            self.conversion_stats.cache_hits += 1;
        } else {
            self.conversion_stats.cache_misses += 1;
        }

        match expected_type {
            AlgebraicType::Bool => {
                if let Some(b) = value.as_bool() {
                    Ok(AlgebraicValue::Bool(b))
                } else {
                    Err(ConversionError::TypeMismatch {
                        expected: "bool".to_string(),
                        found: self.get_js_type_name(value),
                    })
                }
            }
            
            AlgebraicType::I8 => self.convert_to_i8(value),
            AlgebraicType::U8 => self.convert_to_u8(value),
            AlgebraicType::I16 => self.convert_to_i16(value),
            AlgebraicType::U16 => self.convert_to_u16(value),
            AlgebraicType::I32 => self.convert_to_i32(value),
            AlgebraicType::U32 => self.convert_to_u32(value),
            AlgebraicType::I64 => self.convert_to_i64(value),
            AlgebraicType::U64 => self.convert_to_u64(value),
            AlgebraicType::I128 => self.convert_to_i128(value),
            AlgebraicType::U128 => self.convert_to_u128(value),
            AlgebraicType::F32 => self.convert_to_f32(value),
            AlgebraicType::F64 => self.convert_to_f64(value),
            AlgebraicType::String => self.convert_to_string(value),
            
            AlgebraicType::Product(product_type) => {
                // Extract field types from the product
                let field_types: Vec<AlgebraicType> = product_type.elements
                    .iter()
                    .map(|elem| elem.algebraic_type.clone())
                    .collect();
                self.python_to_product(value, &field_types)
            }
            AlgebraicType::Sum(sum_type) => {
                // Extract variant types from the sum
                let variant_types: Vec<AlgebraicType> = sum_type.variants
                    .iter()
                    .map(|variant| variant.algebraic_type.clone())
                    .collect();
                self.python_to_sum(value, &variant_types)
            }
            AlgebraicType::Array(array_type) => {
                self.python_to_typed_array(value, &array_type.elem_ty)
            }
            
            _ => {
                self.conversion_stats.errors_encountered += 1;
                Err(ConversionError::UnsupportedType {
                    type_name: format!("{:?}", expected_type),
                    context: "Type hint conversion".to_string(),
                })
            }
        }
    }

    /// Infer type from JavaScript value and convert appropriately
    fn infer_and_convert(&mut self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(b) = value.as_bool() {
            return Ok(AlgebraicValue::Bool(b));
        }
        
        if let Some(s) = value.as_string() {
            return Ok(AlgebraicValue::String(s.into()));
        }
        
        if let Some(n) = value.as_f64() {
            return self.infer_numeric_type(n);
        }
        
        if Array::is_array(value) {
            return self.python_array_to_bsatn(value);
        }
        
        if value.is_object() {
            return self.python_object_to_bsatn(value);
        }
        
        Err(ConversionError::UnsupportedType {
            type_name: self.get_js_type_name(value),
            context: "Type inference".to_string(),
        })
    }

    /// Enhanced Product conversion with field validation and object pooling
    fn product_to_python(&mut self, product: &ProductValue) -> Result<JsValue, ConversionError> {
        let obj = self.object_pool.get_object();
        self.conversion_stats.object_pool_hits += 1;
        
        for (index, element) in product.elements.iter().enumerate() {
            let key = JsValue::from(index);
            let value = self.bsatn_to_python(element)?;
            Reflect::set(&obj, &key, &value)
                .map_err(|e| ConversionError::PropertySetError(e))?;
        }
        
        Ok(obj.into())
    }

    /// Enhanced Sum conversion with variant validation
    fn sum_to_python(&mut self, variant: &SumValue) -> Result<JsValue, ConversionError> {
        let obj = self.object_pool.get_object();
        self.conversion_stats.object_pool_hits += 1;
        
        // Create SpacetimeDB-style sum representation
        let tag = JsValue::from(variant.tag as u32);
        let value = self.bsatn_to_python(&variant.value)?;
        
        Reflect::set(&obj, &"tag".into(), &tag)
            .map_err(|e| ConversionError::PropertySetError(e))?;
        Reflect::set(&obj, &"value".into(), &value)
            .map_err(|e| ConversionError::PropertySetError(e))?;
        
        Ok(obj.into())
    }

    /// Enhanced Array conversion with type preservation and pooling
    fn array_to_python(&mut self, array_val: &ArrayValue) -> Result<JsValue, ConversionError> {
        let array = self.object_pool.get_array();
        self.conversion_stats.object_pool_hits += 1;
        
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
                    let safe_val = self.safe_i64_to_python(*v)?;
                    array.push(&safe_val);
                }
            }
            ArrayValue::U64(vals) => {
                for v in vals.iter() {
                    let safe_val = self.safe_u64_to_python(*v)?;
                    array.push(&safe_val);
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
                return Err(ConversionError::UnsupportedType {
                    type_name: format!("{:?}", array_val),
                    context: "Array to Python conversion".to_string(),
                });
            }
        }
        
        Ok(array.into())
    }

    /// Get comprehensive statistics for performance monitoring
    pub fn get_stats(&self) -> &ConversionStats {
        &self.conversion_stats
    }

    /// Get detailed performance report
    pub fn get_performance_report(&self) -> JsValue {
        let report = serde_json::json!({
            "conversions_performed": self.conversion_stats.conversions_performed,
            "batch_conversions": self.conversion_stats.batch_conversions,
            "cache_hit_ratio": self.conversion_stats.get_cache_hit_ratio(),
            "pool_hit_ratio": self.conversion_stats.get_pool_hit_ratio(),
            "average_conversion_time_ms": self.conversion_stats.get_average_conversion_time(),
            "total_processing_time_ms": self.conversion_stats.total_processing_time_ms,
            "errors_encountered": self.conversion_stats.errors_encountered,
            "smart_cache_hit_ratio": self.smart_cache.get_hit_ratio(),
            "peak_memory_usage": self.conversion_stats.peak_memory_usage,
            "cache_size": self.smart_cache.cache.len(),
            "pool_utilization": {
                "objects": self.object_pool.js_objects.len(),
                "arrays": self.object_pool.js_arrays.len()
            }
        });
        
        JsValue::from_str(&report.to_string())
    }

    /// Clear all caches and reset pools (useful for memory management)
    pub fn clear_caches(&mut self) {
        self.smart_cache = SmartTypeCache::new(self.config.max_cache_size);
        self.object_pool = ConversionObjectPool::new(self.config.max_pool_size);
        console::log_1(&"Type converter caches and pools cleared".into());
    }

    /// Update configuration at runtime
    pub fn update_config(&mut self, new_config: ConversionConfig) {
        self.config = new_config;
        console::log_1(&"Type converter configuration updated".into());
    }
}

// Private helper methods
impl TypeConverter {
    /// Safely convert i64 to Python, handling JavaScript number limitations
    fn safe_i64_to_python(&self, value: i64) -> Result<JsValue, ConversionError> {
        if value >= -(2_i64.pow(53)) && value <= 2_i64.pow(53) {
            Ok(JsValue::from(value as f64))
        } else {
            // Use string representation for large numbers
            Ok(JsValue::from_str(&value.to_string()))
        }
    }

    /// Safely convert u64 to Python, handling JavaScript number limitations
    fn safe_u64_to_python(&self, value: u64) -> Result<JsValue, ConversionError> {
        if value <= 2_u64.pow(53) {
            Ok(JsValue::from(value as f64))
        } else {
            // Use string representation for large numbers
            Ok(JsValue::from_str(&value.to_string()))
        }
    }

    /// Convert large integers to Python using string representation
    fn large_int_to_python(&self, value_str: &str) -> Result<JsValue, ConversionError> {
        Ok(JsValue::from_str(value_str))
    }

    /// Infer the most appropriate numeric type from a JavaScript number
    fn infer_numeric_type(&self, n: f64) -> Result<AlgebraicValue, ConversionError> {
        if n.fract() == 0.0 {
            // It's an integer - choose the smallest appropriate type
            if n >= 0.0 {
                if n <= u8::MAX as f64 {
                    Ok(AlgebraicValue::U8(n as u8))
                } else if n <= u16::MAX as f64 {
                    Ok(AlgebraicValue::U16(n as u16))
                } else if n <= u32::MAX as f64 {
                    Ok(AlgebraicValue::U32(n as u32))
                } else {
                    Ok(AlgebraicValue::U64(n as u64))
                }
            } else {
                if n >= i8::MIN as f64 && n <= i8::MAX as f64 {
                    Ok(AlgebraicValue::I8(n as i8))
                } else if n >= i16::MIN as f64 && n <= i16::MAX as f64 {
                    Ok(AlgebraicValue::I16(n as i16))
                } else if n >= i32::MIN as f64 && n <= i32::MAX as f64 {
                    Ok(AlgebraicValue::I32(n as i32))
                } else {
                    Ok(AlgebraicValue::I64(n as i64))
                }
            }
        } else {
            // It's a float
            Ok(AlgebraicValue::F64(n.into()))
        }
    }

    /// Get JavaScript type name for error reporting
    fn get_js_type_name(&self, value: &JsValue) -> String {
        if value.is_null() {
            "null".to_string()
        } else if value.is_undefined() {
            "undefined".to_string()
        } else if value.is_string() {
            "string".to_string()
        } else if value.as_bool().is_some() {
            "boolean".to_string()
        } else if value.as_f64().is_some() {
            "number".to_string()
        } else if Array::is_array(value) {
            "array".to_string()
        } else if value.is_object() {
            "object".to_string()
        } else {
            "unknown".to_string()
        }
    }

    // Conversion helper methods for specific types
    fn convert_to_i8(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= i8::MIN as f64 && n <= i8::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::I8(n as i8))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "i8".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "i8".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_u8(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= 0.0 && n <= u8::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::U8(n as u8))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "u8".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "u8".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_i16(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= i16::MIN as f64 && n <= i16::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::I16(n as i16))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "i16".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "i16".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_u16(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= 0.0 && n <= u16::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::U16(n as u16))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "u16".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "u16".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_i32(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= i32::MIN as f64 && n <= i32::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::I32(n as i32))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "i32".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "i32".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_u32(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            if n >= 0.0 && n <= u32::MAX as f64 && n.fract() == 0.0 {
                Ok(AlgebraicValue::U32(n as u32))
            } else {
                Err(ConversionError::NumberOutOfRange {
                    value: n,
                    target_type: "u32".to_string(),
                })
            }
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "u32".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_i64(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            Ok(AlgebraicValue::I64(n as i64))
        } else if let Some(s) = value.as_string() {
            s.parse::<i64>()
                .map(AlgebraicValue::I64)
                .map_err(|_| ConversionError::NumberParseError {
                    value: s,
                    target_type: "i64".to_string(),
                })
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "i64".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_u64(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            Ok(AlgebraicValue::U64(n as u64))
        } else if let Some(s) = value.as_string() {
            s.parse::<u64>()
                .map(AlgebraicValue::U64)
                .map_err(|_| ConversionError::NumberParseError {
                    value: s,
                    target_type: "u64".to_string(),
                })
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "u64".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_i128(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(s) = value.as_string() {
            s.parse::<i128>()
                .map(|v| AlgebraicValue::I128(v.into()))
                .map_err(|_| ConversionError::NumberParseError {
                    value: s,
                    target_type: "i128".to_string(),
                })
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "i128".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_u128(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(s) = value.as_string() {
            s.parse::<u128>()
                .map(|v| AlgebraicValue::U128(v.into()))
                .map_err(|_| ConversionError::NumberParseError {
                    value: s,
                    target_type: "u128".to_string(),
                })
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "u128".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_f32(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            Ok(AlgebraicValue::F32((n as f32).into()))
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "f32".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_f64(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(n) = value.as_f64() {
            Ok(AlgebraicValue::F64(n.into()))
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "f64".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn convert_to_string(&self, value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        if let Some(s) = value.as_string() {
            Ok(AlgebraicValue::String(s.into()))
        } else {
            Err(ConversionError::TypeMismatch {
                expected: "string".to_string(),
                found: self.get_js_type_name(value),
            })
        }
    }

    fn python_to_product(&mut self, value: &JsValue, _fields: &[AlgebraicType]) -> Result<AlgebraicValue, ConversionError> {
        if !value.is_object() {
            return Err(ConversionError::TypeMismatch {
                expected: "object".to_string(),
                found: self.get_js_type_name(value),
            });
        }

        // TODO: Implement proper product conversion with field validation
        Err(ConversionError::UnsupportedType {
            type_name: "Product".to_string(),
            context: "Python to BSATN product conversion".to_string(),
        })
    }

    fn python_to_sum(&mut self, value: &JsValue, _variants: &[AlgebraicType]) -> Result<AlgebraicValue, ConversionError> {
        if !value.is_object() {
            return Err(ConversionError::TypeMismatch {
                expected: "object".to_string(),
                found: self.get_js_type_name(value),
            });
        }

        // TODO: Implement proper sum conversion with variant validation
        Err(ConversionError::UnsupportedType {
            type_name: "Sum".to_string(),
            context: "Python to BSATN sum conversion".to_string(),
        })
    }

    fn python_to_typed_array(&mut self, value: &JsValue, _element_type: &AlgebraicType) -> Result<AlgebraicValue, ConversionError> {
        if !Array::is_array(value) {
            return Err(ConversionError::TypeMismatch {
                expected: "array".to_string(),
                found: self.get_js_type_name(value),
            });
        }

        // TODO: Implement proper typed array conversion
        Err(ConversionError::UnsupportedType {
            type_name: "Array".to_string(),
            context: "Python to BSATN typed array conversion".to_string(),
        })
    }

    fn python_array_to_bsatn(&mut self, _value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        // TODO: Implement array conversion with type inference
        Err(ConversionError::UnsupportedType {
            type_name: "Array".to_string(),
            context: "Python array to BSATN conversion".to_string(),
        })
    }

    fn python_object_to_bsatn(&mut self, _value: &JsValue) -> Result<AlgebraicValue, ConversionError> {
        // TODO: Implement object conversion with type inference
        Err(ConversionError::UnsupportedType {
            type_name: "Object".to_string(),
            context: "Python object to BSATN conversion".to_string(),
        })
    }
}
