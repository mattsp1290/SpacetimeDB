//! Comprehensive tests for type conversion functionality
//! 
//! Tests all supported type conversions between BSATN and Python objects,
//! including edge cases, error conditions, and round-trip conversions.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::type_conversion::{TypeConverter, ConversionStats};
    use crate::error::{ConversionError, ValidationError};
    use crate::pyodide_runtime::PyodideRuntime;
    use spacetimedb_lib::AlgebraicValue;
    use spacetimedb_sats::{AlgebraicType, ArrayValue, SumValue, ProductValue};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::*;
    use js_sys::{Object, Array, Reflect};
    use std::sync::Arc;
    
    wasm_bindgen_test_configure!(run_in_browser);

    // Mock PyodideRuntime for testing (since we can't easily initialize real Pyodide in tests)
    struct MockPyodideRuntime;
    
    impl MockPyodideRuntime {
        fn new() -> Arc<Self> {
            Arc::new(Self)
        }
    }

    fn create_test_converter() -> TypeConverter {
        // Note: In real implementation, we'd use actual PyodideRuntime
        // For tests, we'll need to mock it or use a test-specific implementation
        let mock_runtime = MockPyodideRuntime::new();
        // TypeConverter::new(mock_runtime) - This will need adjustment for real implementation
        TypeConverter {
            pyodide: mock_runtime as Arc<dyn std::any::Any>, // Placeholder
            type_cache: std::collections::HashMap::new(),
            conversion_stats: crate::type_conversion::ConversionStats::default(),
        }
    }

    #[wasm_bindgen_test]
    fn test_primitive_bool_conversion() {
        let mut converter = create_test_converter();
        
        // Test bool true
        let bool_value = AlgebraicValue::Bool(true);
        let js_result = converter.bsatn_to_python(&bool_value).unwrap();
        assert!(js_result.as_bool().unwrap());
        
        // Test bool false
        let bool_value = AlgebraicValue::Bool(false);
        let js_result = converter.bsatn_to_python(&bool_value).unwrap();
        assert!(!js_result.as_bool().unwrap());
        
        // Test round-trip conversion
        let original = AlgebraicValue::Bool(true);
        let js_val = converter.bsatn_to_python(&original).unwrap();
        let back_to_bsatn = converter.python_to_bsatn(&js_val, Some(&AlgebraicType::Bool)).unwrap();
        assert_eq!(original, back_to_bsatn);
    }

    #[wasm_bindgen_test]
    fn test_primitive_integer_conversions() {
        let mut converter = create_test_converter();
        
        // Test various integer types
        let test_cases = vec![
            (AlgebraicValue::I8(-42), AlgebraicType::I8),
            (AlgebraicValue::U8(255), AlgebraicType::U8),
            (AlgebraicValue::I16(-32000), AlgebraicType::I16),
            (AlgebraicValue::U16(65000), AlgebraicType::U16),
            (AlgebraicValue::I32(-2000000), AlgebraicType::I32),
            (AlgebraicValue::U32(4000000), AlgebraicType::U32),
        ];
        
        for (original_value, type_hint) in test_cases {
            let js_val = converter.bsatn_to_python(&original_value).unwrap();
            let back_to_bsatn = converter.python_to_bsatn(&js_val, Some(&type_hint)).unwrap();
            assert_eq!(original_value, back_to_bsatn);
        }
    }

    #[wasm_bindgen_test]
    fn test_large_integer_conversions() {
        let mut converter = create_test_converter();
        
        // Test i64/u64 that require special handling for JavaScript
        let large_i64 = AlgebraicValue::I64(9007199254740992); // Beyond JS safe integer
        let js_result = converter.bsatn_to_python(&large_i64).unwrap();
        
        // Should be converted to string for large numbers
        assert!(js_result.is_string());
        
        // Test conversion back
        let back_to_bsatn = converter.python_to_bsatn(&js_result, Some(&AlgebraicType::I64)).unwrap();
        assert_eq!(large_i64, back_to_bsatn);
    }

    #[wasm_bindgen_test]
    fn test_floating_point_conversions() {
        let mut converter = create_test_converter();
        
        // Test f32
        let f32_value = AlgebraicValue::F32(3.14159.into());
        let js_result = converter.bsatn_to_python(&f32_value).unwrap();
        let back_to_bsatn = converter.python_to_bsatn(&js_result, Some(&AlgebraicType::F32)).unwrap();
        
        // Note: May have slight precision differences due to f32 conversion
        if let (AlgebraicValue::F32(orig), AlgebraicValue::F32(converted)) = (&f32_value, &back_to_bsatn) {
            let diff = (orig.into_inner() - converted.into_inner()).abs();
            assert!(diff < 0.0001); // Allow for floating point precision
        }
        
        // Test f64
        let f64_value = AlgebraicValue::F64(2.718281828459045.into());
        let js_result = converter.bsatn_to_python(&f64_value).unwrap();
        let back_to_bsatn = converter.python_to_bsatn(&js_result, Some(&AlgebraicType::F64)).unwrap();
        assert_eq!(f64_value, back_to_bsatn);
    }

    #[wasm_bindgen_test]
    fn test_string_conversions() {
        let mut converter = create_test_converter();
        
        let test_strings = vec![
            "Hello, World!",
            "",
            "Unicode: 🦀 Rust 🐍 Python",
            "Special chars: \n\t\r\"'\\",
            "Numbers as string: 12345",
        ];
        
        for test_str in test_strings {
            let string_value = AlgebraicValue::String(test_str.into());
            let js_result = converter.bsatn_to_python(&string_value).unwrap();
            assert_eq!(js_result.as_string().unwrap(), test_str);
            
            let back_to_bsatn = converter.python_to_bsatn(&js_result, Some(&AlgebraicType::String)).unwrap();
            assert_eq!(string_value, back_to_bsatn);
        }
    }

    #[wasm_bindgen_test]
    fn test_array_conversions() {
        let mut converter = create_test_converter();
        
        // Test boolean array
        let bool_array = ArrayValue::Bool(vec![true, false, true].into_boxed_slice());
        let array_value = AlgebraicValue::Array(bool_array);
        let js_result = converter.bsatn_to_python(&array_value).unwrap();
        
        assert!(Array::is_array(&js_result));
        let js_array: Array = js_result.dyn_into().unwrap();
        assert_eq!(js_array.length(), 3);
        assert!(js_array.get(0).as_bool().unwrap());
        assert!(!js_array.get(1).as_bool().unwrap());
        assert!(js_array.get(2).as_bool().unwrap());
        
        // Test integer array
        let int_array = ArrayValue::I32(vec![1, 2, 3, 4, 5].into_boxed_slice());
        let array_value = AlgebraicValue::Array(int_array);
        let js_result = converter.bsatn_to_python(&array_value).unwrap();
        
        let js_array: Array = js_result.dyn_into().unwrap();
        assert_eq!(js_array.length(), 5);
        for i in 0..5 {
            assert_eq!(js_array.get(i).as_f64().unwrap() as i32, i as i32 + 1);
        }
        
        // Test string array
        let string_array = ArrayValue::String(vec!["hello".into(), "world".into()].into_boxed_slice());
        let array_value = AlgebraicValue::Array(string_array);
        let js_result = converter.bsatn_to_python(&array_value).unwrap();
        
        let js_array: Array = js_result.dyn_into().unwrap();
        assert_eq!(js_array.length(), 2);
        assert_eq!(js_array.get(0).as_string().unwrap(), "hello");
        assert_eq!(js_array.get(1).as_string().unwrap(), "world");
    }

    #[wasm_bindgen_test]
    fn test_product_conversions() {
        let mut converter = create_test_converter();
        
        // Create a simple product (like a struct)
        let product_elements = vec![
            AlgebraicValue::String("player1".into()),
            AlgebraicValue::I32(100),
            AlgebraicValue::Bool(true),
        ];
        let product = ProductValue { elements: product_elements };
        let product_value = AlgebraicValue::Product(product);
        
        let js_result = converter.bsatn_to_python(&product_value).unwrap();
        assert!(js_result.is_object());
        
        let js_obj: Object = js_result.dyn_into().unwrap();
        
        // Check that all fields are present and correct
        let field_0 = Reflect::get(&js_obj, &0.into()).unwrap();
        assert_eq!(field_0.as_string().unwrap(), "player1");
        
        let field_1 = Reflect::get(&js_obj, &1.into()).unwrap();
        assert_eq!(field_1.as_f64().unwrap() as i32, 100);
        
        let field_2 = Reflect::get(&js_obj, &2.into()).unwrap();
        assert!(field_2.as_bool().unwrap());
    }

    #[wasm_bindgen_test]
    fn test_sum_conversions() {
        let mut converter = create_test_converter();
        
        // Create a sum type (like an enum)
        let sum_value = SumValue {
            tag: 1,
            value: AlgebraicValue::String("variant_data".into()),
        };
        let algebraic_sum = AlgebraicValue::Sum(sum_value);
        
        let js_result = converter.bsatn_to_python(&algebraic_sum).unwrap();
        assert!(js_result.is_object());
        
        let js_obj: Object = js_result.dyn_into().unwrap();
        
        // Check sum structure
        let tag = Reflect::get(&js_obj, &"tag".into()).unwrap();
        assert_eq!(tag.as_f64().unwrap() as u32, 1);
        
        let value = Reflect::get(&js_obj, &"value".into()).unwrap();
        assert_eq!(value.as_string().unwrap(), "variant_data");
    }

    #[wasm_bindgen_test]
    fn test_type_inference() {
        let mut converter = create_test_converter();
        
        // Test inference for different JavaScript values
        let test_cases = vec![
            (JsValue::from(true), "Bool"),
            (JsValue::from("test"), "String"),
            (JsValue::from(42), "Integer"),
            (JsValue::from(3.14), "Float"),
        ];
        
        for (js_val, expected_type) in test_cases {
            let result = converter.python_to_bsatn(&js_val, None);
            assert!(result.is_ok(), "Failed to infer type for {}", expected_type);
        }
    }

    #[wasm_bindgen_test]
    fn test_error_conditions() {
        let mut converter = create_test_converter();
        
        // Test null/undefined
        let null_val = JsValue::null();
        let result = converter.python_to_bsatn(&null_val, None);
        assert!(matches!(result, Err(ConversionError::NullValue)));
        
        let undefined_val = JsValue::undefined();
        let result = converter.python_to_bsatn(&undefined_val, None);
        assert!(matches!(result, Err(ConversionError::NullValue)));
        
        // Test type mismatch
        let string_val = JsValue::from("not_a_number");
        let result = converter.python_to_bsatn(&string_val, Some(&AlgebraicType::I32));
        assert!(matches!(result, Err(ConversionError::TypeMismatch { .. })));
        
        // Test number out of range
        let large_number = JsValue::from(300.0); // Too large for u8
        let result = converter.python_to_bsatn(&large_number, Some(&AlgebraicType::U8));
        assert!(matches!(result, Err(ConversionError::NumberOutOfRange { .. })));
    }

    #[wasm_bindgen_test]
    fn test_complex_nested_structures() {
        let mut converter = create_test_converter();
        
        // Create a complex nested structure
        let inner_product = ProductValue {
            elements: vec![
                AlgebraicValue::String("nested".into()),
                AlgebraicValue::I32(42),
            ]
        };
        
        let array_with_products = ArrayValue::String(vec!["item1".into(), "item2".into()].into_boxed_slice());
        
        let outer_product = ProductValue {
            elements: vec![
                AlgebraicValue::Product(inner_product),
                AlgebraicValue::Array(array_with_products),
                AlgebraicValue::Bool(true),
            ]
        };
        
        let complex_value = AlgebraicValue::Product(outer_product);
        
        // Test conversion to Python
        let js_result = converter.bsatn_to_python(&complex_value);
        assert!(js_result.is_ok(), "Failed to convert complex nested structure");
        
        let js_obj = js_result.unwrap();
        assert!(js_obj.is_object());
    }

    #[wasm_bindgen_test]
    fn test_conversion_statistics() {
        let mut converter = create_test_converter();
        
        // Perform several conversions
        let values = vec![
            AlgebraicValue::Bool(true),
            AlgebraicValue::I32(42),
            AlgebraicValue::String("test".into()),
        ];
        
        for value in values {
            let _ = converter.bsatn_to_python(&value);
        }
        
        let stats = converter.get_stats();
        assert_eq!(stats.conversions_performed, 3);
        assert_eq!(stats.errors_encountered, 0);
    }

    #[wasm_bindgen_test]
    fn test_cache_functionality() {
        let mut converter = create_test_converter();
        
        // Test cache clearing
        converter.clear_cache();
        let stats = converter.get_stats();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
    }

    #[wasm_bindgen_test]
    fn test_user_friendly_error_messages() {
        // Test ConversionError messages
        let type_mismatch = ConversionError::TypeMismatch {
            expected: "int".to_string(),
            found: "string".to_string(),
        };
        let message = type_mismatch.user_friendly_message();
        assert!(message.contains("Expected type 'int' but found 'string'"));
        assert!(message.contains("Please check your Python object structure"));
        
        let null_error = ConversionError::NullValue;
        let message = null_error.user_friendly_message();
        assert!(message.contains("Null or undefined values are not allowed"));
        
        let range_error = ConversionError::NumberOutOfRange {
            value: 300.0,
            target_type: "u8".to_string(),
        };
        let message = range_error.user_friendly_message();
        assert!(message.contains("Number 300 is out of range for type u8"));
    }

    #[wasm_bindgen_test]
    fn test_edge_cases() {
        let mut converter = create_test_converter();
        
        // Test empty string
        let empty_string = AlgebraicValue::String("".into());
        let js_result = converter.bsatn_to_python(&empty_string).unwrap();
        assert_eq!(js_result.as_string().unwrap(), "");
        
        // Test zero values
        let zero_int = AlgebraicValue::I32(0);
        let js_result = converter.bsatn_to_python(&zero_int).unwrap();
        assert_eq!(js_result.as_f64().unwrap(), 0.0);
        
        // Test empty array
        let empty_array = ArrayValue::Bool(vec![].into_boxed_slice());
        let array_value = AlgebraicValue::Array(empty_array);
        let js_result = converter.bsatn_to_python(&array_value).unwrap();
        
        let js_array: Array = js_result.dyn_into().unwrap();
        assert_eq!(js_array.length(), 0);
        
        // Test single-element product
        let single_product = ProductValue {
            elements: vec![AlgebraicValue::String("solo".into())]
        };
        let product_value = AlgebraicValue::Product(single_product);
        let js_result = converter.bsatn_to_python(&product_value).unwrap();
        assert!(js_result.is_object());
    }

    #[wasm_bindgen_test]
    fn test_precision_preservation() {
        let mut converter = create_test_converter();
        
        // Test that we don't lose precision for numbers within safe range
        let precise_number = AlgebraicValue::F64(1.23456789.into());
        let js_result = converter.bsatn_to_python(&precise_number).unwrap();
        let back_to_bsatn = converter.python_to_bsatn(&js_result, Some(&AlgebraicType::F64)).unwrap();
        
        if let (AlgebraicValue::F64(orig), AlgebraicValue::F64(converted)) = (&precise_number, &back_to_bsatn) {
            let diff = (orig.into_inner() - converted.into_inner()).abs();
            assert!(diff < 1e-15); // Should preserve double precision
        }
    }

    #[wasm_bindgen_test]
    fn test_special_float_values() {
        let mut converter = create_test_converter();
        
        // Test NaN (Note: JavaScript NaN != NaN, so we need special handling)
        let nan_value = AlgebraicValue::F64(f64::NAN.into());
        let js_result = converter.bsatn_to_python(&nan_value).unwrap();
        assert!(js_result.as_f64().unwrap().is_nan());
        
        // Test Infinity
        let inf_value = AlgebraicValue::F64(f64::INFINITY.into());
        let js_result = converter.bsatn_to_python(&inf_value).unwrap();
        assert!(js_result.as_f64().unwrap().is_infinite());
        assert!(js_result.as_f64().unwrap().is_sign_positive());
        
        // Test Negative Infinity
        let neg_inf_value = AlgebraicValue::F64(f64::NEG_INFINITY.into());
        let js_result = converter.bsatn_to_python(&neg_inf_value).unwrap();
        assert!(js_result.as_f64().unwrap().is_infinite());
        assert!(js_result.as_f64().unwrap().is_sign_negative());
    }
}
