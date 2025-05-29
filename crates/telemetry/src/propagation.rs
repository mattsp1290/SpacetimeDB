//! Distributed tracing context propagation utilities for SpacetimeDB
//!
//! This module provides utilities for extracting and injecting trace context
//! across service boundaries, supporting W3C Trace Context, B3 propagation,
//! and SpacetimeDB-specific context propagation.

use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{global, Context, KeyValue};
use spacetimedb_lib::Identity;
use std::collections::HashMap;
use tracing::Span;

/// Key for SpacetimeDB database identity in trace context
pub const SPACETIMEDB_DATABASE_ID_KEY: &str = "spacetimedb.database_id";
/// Key for SpacetimeDB module hash in trace context
pub const SPACETIMEDB_MODULE_HASH_KEY: &str = "spacetimedb.module_hash";
/// Key for SpacetimeDB operation type in trace context
pub const SPACETIMEDB_OPERATION_TYPE_KEY: &str = "spacetimedb.operation_type";
/// Key for SpacetimeDB identity in trace context
pub const SPACETIMEDB_IDENTITY_KEY: &str = "spacetimedb.identity";

/// Wrapper for HTTP headers to implement OpenTelemetry Extractor trait
pub struct HeaderMapExtractor<'a>(&'a HeaderMap);

impl<'a> HeaderMapExtractor<'a> {
    pub fn new(headers: &'a HeaderMap) -> Self {
        Self(headers)
    }
}

impl<'a> Extractor for HeaderMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

/// Wrapper for HTTP headers to implement OpenTelemetry Injector trait
pub struct HeaderMapInjector<'a>(&'a mut HeaderMap);

impl<'a> HeaderMapInjector<'a> {
    pub fn new(headers: &'a mut HeaderMap) -> Self {
        Self(headers)
    }
}

impl<'a> Injector for HeaderMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (HeaderName::try_from(key), HeaderValue::try_from(value)) {
            self.0.insert(name, val);
        }
    }
}

/// Wrapper for HashMap to implement OpenTelemetry Extractor trait
pub struct HashMapExtractor<'a>(&'a HashMap<String, String>);

impl<'a> HashMapExtractor<'a> {
    pub fn new(map: &'a HashMap<String, String>) -> Self {
        Self(map)
    }
}

impl<'a> Extractor for HashMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
}

/// Wrapper for HashMap to implement OpenTelemetry Injector trait
pub struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> HashMapInjector<'a> {
    pub fn new(map: &'a mut HashMap<String, String>) -> Self {
        Self(map)
    }
}

impl<'a> Injector for HashMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// SpacetimeDB-specific context information for distributed tracing
#[derive(Debug, Clone)]
pub struct SpacetimeDBContext {
    pub database_id: Option<String>,
    pub module_hash: Option<String>,
    pub operation_type: Option<String>,
    pub identity: Option<Identity>,
}

impl SpacetimeDBContext {
    /// Create a new empty SpacetimeDB context
    pub fn new() -> Self {
        Self {
            database_id: None,
            module_hash: None,
            operation_type: None,
            identity: None,
        }
    }

    /// Set the database ID
    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = Some(database_id.into());
        self
    }

    /// Set the module hash
    pub fn with_module_hash(mut self, module_hash: impl Into<String>) -> Self {
        self.module_hash = Some(module_hash.into());
        self
    }

    /// Set the operation type
    pub fn with_operation_type(mut self, operation_type: impl Into<String>) -> Self {
        self.operation_type = Some(operation_type.into());
        self
    }

    /// Set the identity
    pub fn with_identity(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Convert to OpenTelemetry key-value pairs
    pub fn to_key_values(&self) -> Vec<KeyValue> {
        let mut kv = Vec::new();
        
        if let Some(ref db_id) = self.database_id {
            kv.push(KeyValue::new(SPACETIMEDB_DATABASE_ID_KEY, db_id.clone()));
        }
        
        if let Some(ref module_hash) = self.module_hash {
            kv.push(KeyValue::new(SPACETIMEDB_MODULE_HASH_KEY, module_hash.clone()));
        }
        
        if let Some(ref op_type) = self.operation_type {
            kv.push(KeyValue::new(SPACETIMEDB_OPERATION_TYPE_KEY, op_type.clone()));
        }
        
        if let Some(ref identity) = self.identity {
            kv.push(KeyValue::new(SPACETIMEDB_IDENTITY_KEY, identity.to_hex().to_string()));
        }
        
        kv
    }

    /// Record context attributes on a tracing span
    pub fn record_on_span(&self, span: &Span) {
        if let Some(ref db_id) = self.database_id {
            span.record(SPACETIMEDB_DATABASE_ID_KEY, db_id.as_str());
        }
        
        if let Some(ref module_hash) = self.module_hash {
            span.record(SPACETIMEDB_MODULE_HASH_KEY, module_hash.as_str());
        }
        
        if let Some(ref op_type) = self.operation_type {
            span.record(SPACETIMEDB_OPERATION_TYPE_KEY, op_type.as_str());
        }
        
        if let Some(ref identity) = self.identity {
            span.record(SPACETIMEDB_IDENTITY_KEY, identity.to_hex().as_str());
        }
    }
}

impl Default for SpacetimeDBContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract trace context from HTTP headers
pub fn extract_trace_context_from_headers(headers: &HeaderMap) -> Context {
    let extractor = HeaderMapExtractor::new(headers);
    global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// Inject trace context into HTTP headers
pub fn inject_trace_context_into_headers(context: &Context, headers: &mut HeaderMap) {
    let mut injector = HeaderMapInjector::new(headers);
    global::get_text_map_propagator(|propagator| propagator.inject_context(context, &mut injector));
}

/// Extract trace context from a HashMap (useful for WebSocket subprotocols)
pub fn extract_trace_context_from_map(map: &HashMap<String, String>) -> Context {
    let extractor = HashMapExtractor::new(map);
    global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

/// Inject trace context into a HashMap (useful for WebSocket subprotocols)
pub fn inject_trace_context_into_map(context: &Context, map: &mut HashMap<String, String>) {
    let mut injector = HashMapInjector::new(map);
    global::get_text_map_propagator(|propagator| propagator.inject_context(context, &mut injector));
}

/// Create a child context with SpacetimeDB-specific attributes
pub fn create_spacetimedb_context(
    parent_context: &Context,
    _spacetimedb_ctx: &SpacetimeDBContext,
) -> Context {
    // In the simplified implementation, we just return the parent context
    // In a full implementation, this would add SpacetimeDB-specific baggage
    parent_context.clone()
}

/// Check if a context has an active span
pub fn has_active_span(context: &Context) -> bool {
    let span = context.span();
    let span_context = span.span_context();
    span_context.is_valid()
}

/// Get the trace ID from a context as a hex string
pub fn get_trace_id_hex(context: &Context) -> Option<String> {
    let span = context.span();
    let span_context = span.span_context();
    if span_context.is_valid() {
        Some(span_context.trace_id().to_string())
    } else {
        None
    }
}

/// Get the span ID from a context as a hex string
pub fn get_span_id_hex(context: &Context) -> Option<String> {
    let span = context.span();
    let span_context = span.span_context();
    if span_context.is_valid() {
        Some(span_context.span_id().to_string())
    } else {
        None
    }
}

/// Extract SpacetimeDB-specific context from WebSocket upgrade headers
pub fn extract_websocket_trace_context(headers: &HeaderMap) -> Result<(Context, Option<SpacetimeDBContext>)> {
    // Extract standard trace context
    let trace_context = extract_trace_context_from_headers(headers);
    
    // Extract SpacetimeDB-specific context from custom headers
    let mut spacetimedb_ctx = SpacetimeDBContext::new();
    
    if let Some(db_id) = headers.get("x-spacetimedb-database-id")
        .and_then(|v| v.to_str().ok()) {
        spacetimedb_ctx = spacetimedb_ctx.with_database_id(db_id);
    }
    
    if let Some(module_hash) = headers.get("x-spacetimedb-module-hash")
        .and_then(|v| v.to_str().ok()) {
        spacetimedb_ctx = spacetimedb_ctx.with_module_hash(module_hash);
    }
    
    if let Some(op_type) = headers.get("x-spacetimedb-operation-type")
        .and_then(|v| v.to_str().ok()) {
        spacetimedb_ctx = spacetimedb_ctx.with_operation_type(op_type);
    }
    
    if let Some(identity_hex) = headers.get("x-spacetimedb-identity")
        .and_then(|v| v.to_str().ok()) {
        if let Ok(identity) = Identity::from_hex(identity_hex) {
            spacetimedb_ctx = spacetimedb_ctx.with_identity(identity);
        }
    }
    
    let has_spacetimedb_context = spacetimedb_ctx.database_id.is_some() ||
        spacetimedb_ctx.module_hash.is_some() ||
        spacetimedb_ctx.operation_type.is_some() ||
        spacetimedb_ctx.identity.is_some();
    
    Ok((
        trace_context,
        if has_spacetimedb_context { Some(spacetimedb_ctx) } else { None }
    ))
}

/// Inject SpacetimeDB-specific context into WebSocket response headers
pub fn inject_websocket_trace_context(
    headers: &mut HeaderMap,
    context: &Context,
    spacetimedb_ctx: Option<&SpacetimeDBContext>,
) -> Result<()> {
    // Inject standard trace context
    inject_trace_context_into_headers(context, headers);
    
    // Inject SpacetimeDB-specific context
    if let Some(ctx) = spacetimedb_ctx {
        if let Some(ref db_id) = ctx.database_id {
            if let Ok(value) = HeaderValue::try_from(db_id) {
                headers.insert("x-spacetimedb-database-id", value);
            }
        }
        
        if let Some(ref module_hash) = ctx.module_hash {
            if let Ok(value) = HeaderValue::try_from(module_hash) {
                headers.insert("x-spacetimedb-module-hash", value);
            }
        }
        
        if let Some(ref op_type) = ctx.operation_type {
            if let Ok(value) = HeaderValue::try_from(op_type) {
                headers.insert("x-spacetimedb-operation-type", value);
            }
        }
        
        if let Some(ref identity) = ctx.identity {
            if let Ok(value) = HeaderValue::try_from(identity.to_hex().to_string()) {
                headers.insert("x-spacetimedb-identity", value);
            }
        }
    }
    
    Ok(())
}

/// Macro for creating spans with distributed tracing context
#[macro_export]
macro_rules! trace_span_with_context {
    ($context:expr, $level:expr, $name:expr, $($field:tt)*) => {
        {
            let span = tracing::span!($level, $name, $($field)*);
            // Set the span as a child of the distributed trace context
            if $crate::propagation::has_active_span($context) {
                use opentelemetry::trace::TraceContextExt;
                let span_ref = $context.span();
                let otel_context = opentelemetry::Context::current_with_span(span_ref);
                tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(&span, otel_context);
            }
            span
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_map_extractor() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".parse().unwrap());
        
        let extractor = HeaderMapExtractor::new(&headers);
        assert_eq!(
            extractor.get("traceparent"),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(extractor.get("nonexistent"), None);
    }

    #[test]
    fn test_header_map_injector() {
        let mut headers = HeaderMap::new();
        let mut injector = HeaderMapInjector::new(&mut headers);
        
        injector.set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        
        assert_eq!(
            headers.get("traceparent").unwrap().to_str().unwrap(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn test_spacetimedb_context() {
        let identity = Identity::from_byte_array([1; 32]);
        let ctx = SpacetimeDBContext::new()
            .with_database_id("test-db")
            .with_module_hash("abc123")
            .with_operation_type("call_reducer")
            .with_identity(identity);

        let kv = ctx.to_key_values();
        assert_eq!(kv.len(), 4);
        
        assert_eq!(kv[0].key.as_str(), SPACETIMEDB_DATABASE_ID_KEY);
        assert_eq!(kv[1].key.as_str(), SPACETIMEDB_MODULE_HASH_KEY);
        assert_eq!(kv[2].key.as_str(), SPACETIMEDB_OPERATION_TYPE_KEY);
        assert_eq!(kv[3].key.as_str(), SPACETIMEDB_IDENTITY_KEY);
    }

    #[test]
    fn test_extract_websocket_trace_context() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".parse().unwrap());
        headers.insert("x-spacetimedb-database-id", "test-db".parse().unwrap());
        headers.insert("x-spacetimedb-operation-type", "websocket_connect".parse().unwrap());

        let (_context, spacetimedb_ctx) = extract_websocket_trace_context(&headers).unwrap();
        
        assert!(spacetimedb_ctx.is_some());
        
        let ctx = spacetimedb_ctx.unwrap();
        assert_eq!(ctx.database_id, Some("test-db".to_string()));
        assert_eq!(ctx.operation_type, Some("websocket_connect".to_string()));
    }

    #[test]
    fn test_context_utilities_basic() {
        // Test basic context operations without complex span creation
        let context = Context::new();
        
        // These should not panic and work with empty context
        let _ = has_active_span(&context);
        let _ = get_trace_id_hex(&context);
        let _ = get_span_id_hex(&context);
    }
}
