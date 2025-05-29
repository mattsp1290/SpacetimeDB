//! OpenTelemetry instrumentation for WASM module lifecycle and execution.
//!
//! This module provides comprehensive tracing and metrics for SpacetimeDB's WASM module system,
//! including module loading, compilation, reducer execution, and lifecycle events.

use crate::execution_context::ReducerContext;
use crate::hash::Hash;
use crate::identity::Identity;
use spacetimedb_lib::{ConnectionId, Timestamp};
use spacetimedb_primitives::TableId;
use std::time::{Duration, Instant};
use tracing::{debug_span, info_span, trace_span, Span};

/// Standard span attributes for SpacetimeDB WASM operations
pub struct WasmSpanAttrs;

impl WasmSpanAttrs {
    /// Database-level attributes
    pub const DB_SYSTEM: &'static str = "db.system";
    pub const DB_OPERATION: &'static str = "db.operation";
    
    /// Module-level attributes
    pub const SPACETIMEDB_MODULE_HASH: &'static str = "spacetimedb.module.hash";
    pub const SPACETIMEDB_MODULE_SIZE_BYTES: &'static str = "spacetimedb.module.size_bytes";
    pub const SPACETIMEDB_MODULE_VERSION: &'static str = "spacetimedb.module.version";
    pub const SPACETIMEDB_MODULE_COMPILATION_TIME_MS: &'static str = "spacetimedb.module.compilation_time_ms";
    pub const SPACETIMEDB_MODULE_INSTANCES_ACTIVE: &'static str = "spacetimedb.module.instances_active";
    pub const SPACETIMEDB_MODULE_INSTANCES_POOLED: &'static str = "spacetimedb.module.instances_pooled";
    
    /// Reducer-level attributes
    pub const SPACETIMEDB_REDUCER_NAME: &'static str = "spacetimedb.reducer.name";
    pub const SPACETIMEDB_REDUCER_ID: &'static str = "spacetimedb.reducer.id";
    pub const SPACETIMEDB_REDUCER_ARGS_SIZE_BYTES: &'static str = "spacetimedb.reducer.args_size_bytes";
    pub const SPACETIMEDB_REDUCER_EXECUTION_TIME_MS: &'static str = "spacetimedb.reducer.execution_time_ms";
    pub const SPACETIMEDB_REDUCER_CALLER_IDENTITY: &'static str = "spacetimedb.reducer.caller_identity";
    pub const SPACETIMEDB_REDUCER_CONNECTION_ID: &'static str = "spacetimedb.reducer.connection_id";
    pub const SPACETIMEDB_REDUCER_OUTCOME: &'static str = "spacetimedb.reducer.outcome";
    pub const SPACETIMEDB_REDUCER_TIMESTAMP: &'static str = "spacetimedb.reducer.timestamp";
    
    /// Lifecycle-level attributes
    pub const SPACETIMEDB_LIFECYCLE_EVENT: &'static str = "spacetimedb.lifecycle.event";
    pub const SPACETIMEDB_LIFECYCLE_DURATION_MS: &'static str = "spacetimedb.lifecycle.duration_ms";
    
    /// Error attributes
    pub const SPACETIMEDB_ERROR_TYPE: &'static str = "spacetimedb.error.type";
    pub const SPACETIMEDB_ERROR_MESSAGE: &'static str = "spacetimedb.error.message";
}

/// Create a span for module host operations
pub fn module_host_span(operation: &str, database_identity: &Identity) -> Span {
    info_span!(
        "spacetimedb.module_host",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = operation,
        spacetimedb.database_identity = %database_identity,
    )
}

/// Create a span for module loading operations
pub fn module_loading_span(module_hash: &Hash, size_bytes: Option<usize>) -> Span {
    let span = info_span!(
        "spacetimedb.module.load",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = "module_load",
        { WasmSpanAttrs::SPACETIMEDB_MODULE_HASH } = %module_hash,
        { WasmSpanAttrs::SPACETIMEDB_MODULE_COMPILATION_TIME_MS } = tracing::field::Empty,
    );
    
    if let Some(size) = size_bytes {
        span.record(WasmSpanAttrs::SPACETIMEDB_MODULE_SIZE_BYTES, size);
    }
    
    span
}

/// Create a simple span for reducer operations (used in module_host.rs)
pub fn reducer_span(reducer_name: &str, database_identity: &Identity) -> Span {
    info_span!(
        "spacetimedb.reducer",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = "reducer_call",
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_NAME } = reducer_name,
        spacetimedb.database_identity = %database_identity,
    )
}

/// Create a span for reducer execution
pub fn reducer_execution_span(
    reducer_name: &str,
    reducer_id: u32,
    caller_identity: &Identity,
    caller_connection_id: ConnectionId,
    timestamp: Timestamp,
) -> Span {
    info_span!(
        "spacetimedb.reducer.execute",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = "reducer_execute",
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_NAME } = reducer_name,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_ID } = reducer_id,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_CALLER_IDENTITY } = %caller_identity,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_CONNECTION_ID } = %caller_connection_id,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_TIMESTAMP } = %timestamp,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_ARGS_SIZE_BYTES } = tracing::field::Empty,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_EXECUTION_TIME_MS } = tracing::field::Empty,
        { WasmSpanAttrs::SPACETIMEDB_REDUCER_OUTCOME } = tracing::field::Empty,
    )
}

/// Create a span for module lifecycle events
pub fn module_lifecycle_span(event: &str, database_identity: &Identity) -> Span {
    info_span!(
        "spacetimedb.module.lifecycle",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = "module_lifecycle",
        { WasmSpanAttrs::SPACETIMEDB_LIFECYCLE_EVENT } = event,
        { WasmSpanAttrs::SPACETIMEDB_LIFECYCLE_DURATION_MS } = tracing::field::Empty,
        spacetimedb.database_identity = %database_identity,
    )
}

/// Create a span for module instance operations
pub fn module_instance_span(operation: &str) -> Span {
    debug_span!(
        "spacetimedb.module.instance",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = operation,
    )
}

/// Record compilation time on a module loading span
pub fn record_compilation_time(span: &Span, compilation_time: Duration) {
    span.record(
        WasmSpanAttrs::SPACETIMEDB_MODULE_COMPILATION_TIME_MS,
        compilation_time.as_millis() as u64,
    );
}

/// Record reducer execution details on a span
pub fn record_reducer_execution(
    span: &Span,
    args_size: usize,
    execution_time: Duration,
    outcome: &str,
) {
    span.record(WasmSpanAttrs::SPACETIMEDB_REDUCER_ARGS_SIZE_BYTES, args_size);
    span.record(
        WasmSpanAttrs::SPACETIMEDB_REDUCER_EXECUTION_TIME_MS,
        execution_time.as_millis() as u64,
    );
    span.record(WasmSpanAttrs::SPACETIMEDB_REDUCER_OUTCOME, outcome);
}

/// Record lifecycle event duration
pub fn record_lifecycle_duration(span: &Span, duration: Duration) {
    span.record(
        WasmSpanAttrs::SPACETIMEDB_LIFECYCLE_DURATION_MS,
        duration.as_millis() as u64,
    );
}

/// Record error information on a span
pub fn record_error(span: &Span, error_type: &str, error_message: &str) {
    span.record(WasmSpanAttrs::SPACETIMEDB_ERROR_TYPE, error_type);
    span.record(WasmSpanAttrs::SPACETIMEDB_ERROR_MESSAGE, error_message);
}

/// Create a span for WASM memory operations
pub fn wasm_memory_span(operation: &str) -> Span {
    trace_span!(
        "spacetimedb.wasm.memory",
        { WasmSpanAttrs::DB_SYSTEM } = "spacetimedb",
        { WasmSpanAttrs::DB_OPERATION } = operation,
    )
}

/// Utility to measure and record execution time for a closure
pub fn measure_execution<T, F>(span: &Span, f: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();
    
    span.record(
        WasmSpanAttrs::SPACETIMEDB_REDUCER_EXECUTION_TIME_MS,
        duration.as_millis() as u64,
    );
    
    result
}

/// Extract reducer context information for span attributes
pub fn reducer_context_attrs(ctx: &ReducerContext) -> Vec<(&'static str, String)> {
    vec![
        (WasmSpanAttrs::SPACETIMEDB_REDUCER_NAME, ctx.name.clone()),
        (WasmSpanAttrs::SPACETIMEDB_REDUCER_CALLER_IDENTITY, ctx.caller_identity.to_string()),
        (WasmSpanAttrs::SPACETIMEDB_REDUCER_CONNECTION_ID, ctx.caller_connection_id.to_string()),
        (WasmSpanAttrs::SPACETIMEDB_REDUCER_TIMESTAMP, ctx.timestamp.to_string()),
        (WasmSpanAttrs::SPACETIMEDB_REDUCER_ARGS_SIZE_BYTES, ctx.arg_bsatn.len().to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use spacetimedb_lib::Identity;
    
    #[test]
    fn test_span_creation() {
        let identity = Identity::from_byte_array([1; 32]);
        let span = module_host_span("test_operation", &identity);
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb.module_host");
    }
    
    #[test]
    fn test_reducer_span_creation() {
        let identity = Identity::from_byte_array([1; 32]);
        let span = reducer_execution_span(
            "test_reducer",
            42,
            &identity,
            ConnectionId::from(123),
            Timestamp::now(),
        );
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb.reducer.execute");
    }
    
    #[test]
    fn test_simple_reducer_span_creation() {
        let identity = Identity::from_byte_array([1; 32]);
        let span = reducer_span("test_reducer", &identity);
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb.reducer");
    }
}
