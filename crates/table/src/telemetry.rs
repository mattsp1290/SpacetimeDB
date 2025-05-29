//! Telemetry utilities for table operations

use spacetimedb_telemetry::spacetime_span;
use tracing::{info_span, debug_span, Level};

/// Create a span for table operations
#[macro_export]
macro_rules! table_op_span {
    ($op:expr, $table_name:expr, $table_id:expr $(, $field:tt)*) => {
        tracing::info_span!(
            concat!("db.table.", $op),
            db.system = "spacetimedb",
            db.operation = $op,
            db.table = %$table_name,
            spacetimedb.table_id = %$table_id,
            $($field)*
        )
    };
}

/// Create a span for index operations
#[macro_export]
macro_rules! index_op_span {
    ($op:expr, $table_name:expr, $index_id:expr $(, $field:tt)*) => {
        tracing::debug_span!(
            concat!("db.index.", $op),
            db.system = "spacetimedb",
            db.operation = $op,
            db.table = %$table_name,
            spacetimedb.index_id = %$index_id,
            $($field)*
        )
    };
}

/// Record table operation metrics
pub fn record_table_op_metrics(
    span: &tracing::Span,
    rows_affected: usize,
    bytes_written: usize,
    pages_accessed: usize,
) {
    span.record("spacetimedb.rows_affected", rows_affected);
    span.record("spacetimedb.bytes_written", bytes_written);
    span.record("spacetimedb.pages_accessed", pages_accessed);
}

/// Record index operation metrics
pub fn record_index_op_metrics(
    span: &tracing::Span,
    keys_indexed: usize,
    bytes_used: usize,
) {
    span.record("spacetimedb.keys_indexed", keys_indexed);
    span.record("spacetimedb.index_bytes_used", bytes_used);
}
