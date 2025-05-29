//! Telemetry utilities for SpacetimeDB client API endpoints
//! 
//! This module provides instrumentation for HTTP and WebSocket API endpoints,
//! including request/response tracing, performance metrics, error tracking,
//! and distributed tracing context propagation.

use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::Response;
use opentelemetry::Context;
use spacetimedb_lib::Identity;
use spacetimedb_telemetry::propagation::{
    extract_trace_context_from_headers, extract_websocket_trace_context,
    inject_trace_context_into_headers, inject_websocket_trace_context,
    SpacetimeDBContext, get_trace_id_hex, has_active_span
};
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{field, info_span, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// HTTP request attributes for OpenTelemetry spans
pub struct HttpRequestAttributes {
    pub method: Method,
    pub uri: Uri,
    pub user_agent: Option<String>,
    pub client_ip: Option<SocketAddr>,
    pub content_length: Option<u64>,
    pub identity: Option<Identity>,
    pub trace_context: Option<Context>,
    pub trace_id: Option<String>,
}

/// WebSocket connection attributes for OpenTelemetry spans
pub struct WebSocketAttributes {
    pub connection_id: String,
    pub client_ip: Option<SocketAddr>,
    pub user_agent: Option<String>,
    pub protocol: Option<String>,
    pub identity: Option<Identity>,
    pub trace_context: Option<Context>,
    pub spacetimedb_context: Option<SpacetimeDBContext>,
}

/// Creates a span for HTTP API requests with standard attributes and distributed tracing
pub fn create_http_request_span(attrs: &HttpRequestAttributes) -> Span {
    let span = info_span!(
        "spacetimedb_http_request",
        http.method = %attrs.method,
        http.url = %attrs.uri,
        http.user_agent = field::Empty,
        http.client_ip = field::Empty,
        http.request.body.size = field::Empty,
        spacetimedb.identity = field::Empty,
        spacetimedb.operation_type = "http_request",
        trace.trace_id = field::Empty,
        otel.name = format!("{} {}", attrs.method, attrs.uri.path()),
        otel.kind = "server"
    );

    // Set distributed tracing parent context if available
    if let Some(ref trace_context) = attrs.trace_context {
        if has_active_span(trace_context) {
            span.set_parent(trace_context.clone());
        }
    }

    // Record optional attributes
    if let Some(ref user_agent) = attrs.user_agent {
        span.record("http.user_agent", user_agent.as_str());
    }
    
    if let Some(client_ip) = attrs.client_ip {
        span.record("http.client_ip", client_ip.to_string().as_str());
    }
    
    if let Some(content_length) = attrs.content_length {
        span.record("http.request.body.size", content_length);
    }
    
    if let Some(ref identity) = attrs.identity {
        span.record("spacetimedb.identity", identity.to_hex().as_str());
    }
    
    if let Some(ref trace_id) = attrs.trace_id {
        span.record("trace.trace_id", trace_id.as_str());
    }

    span
}

/// Creates a span for WebSocket connections with standard attributes and distributed tracing
pub fn create_websocket_span(attrs: &WebSocketAttributes) -> Span {
    let span = info_span!(
        "spacetimedb_websocket_connection",
        websocket.connection_id = %attrs.connection_id,
        websocket.client_ip = field::Empty,
        websocket.user_agent = field::Empty,
        websocket.protocol = field::Empty,
        spacetimedb.identity = field::Empty,
        spacetimedb.operation_type = "websocket_connection",
        spacetimedb.database_id = field::Empty,
        trace.trace_id = field::Empty,
        otel.name = "websocket_connection",
        otel.kind = "server"
    );

    // Set distributed tracing parent context if available
    if let Some(ref trace_context) = attrs.trace_context {
        if has_active_span(trace_context) {
            span.set_parent(trace_context.clone());
        }
    }

    // Record optional attributes
    if let Some(client_ip) = attrs.client_ip {
        span.record("websocket.client_ip", client_ip.to_string().as_str());
    }
    
    if let Some(ref user_agent) = attrs.user_agent {
        span.record("websocket.user_agent", user_agent.as_str());
    }
    
    if let Some(ref protocol) = attrs.protocol {
        span.record("websocket.protocol", protocol.as_str());
    }
    
    if let Some(ref identity) = attrs.identity {
        span.record("spacetimedb.identity", identity.to_hex().as_str());
    }

    // Record SpacetimeDB-specific context if available
    if let Some(ref stdb_ctx) = attrs.spacetimedb_context {
        stdb_ctx.record_on_span(&span);
        if let Some(ref trace_context) = attrs.trace_context {
            if let Some(trace_id) = get_trace_id_hex(trace_context) {
                span.record("trace.trace_id", trace_id.as_str());
            }
        }
    }

    span
}

/// Creates a span for WebSocket message operations with distributed tracing
pub fn create_websocket_message_span(
    connection_id: &str,
    message_type: &str,
    message_size: Option<usize>,
    parent_context: Option<&Context>,
) -> Span {
    let span = info_span!(
        "spacetimedb_websocket_message",
        websocket.connection_id = %connection_id,
        websocket.message_type = %message_type,
        websocket.message.size = field::Empty,
        spacetimedb.operation_type = "websocket_message",
        trace.trace_id = field::Empty,
        otel.name = format!("websocket_{}", message_type),
        otel.kind = "server"
    );

    // Set distributed tracing parent context if available
    if let Some(trace_context) = parent_context {
        if has_active_span(trace_context) {
            span.set_parent(trace_context.clone());
            if let Some(trace_id) = get_trace_id_hex(trace_context) {
                span.record("trace.trace_id", trace_id.as_str());
            }
        }
    }

    if let Some(size) = message_size {
        span.record("websocket.message.size", size);
    }

    span
}

/// Creates a span for database API operations with distributed tracing
pub fn create_database_api_span(
    operation: &str,
    database_id: Option<&str>,
    table_name: Option<&str>,
) -> Span {
    let span = info_span!(
        "spacetimedb_database_api",
        spacetimedb.operation_type = %operation,
        spacetimedb.database_id = field::Empty,
        spacetimedb.table_name = field::Empty,
        trace.trace_id = field::Empty,
        otel.name = format!("database_{}", operation),
        otel.kind = "server"
    );

    if let Some(db_id) = database_id {
        span.record("spacetimedb.database_id", db_id);
    }
    
    if let Some(table) = table_name {
        span.record("spacetimedb.table_name", table);
    }

    span
}

/// Creates a span for database API operations with distributed tracing context
pub fn create_database_api_span_with_context(
    operation: &str,
    database_id: Option<&str>,
    table_name: Option<&str>,
    trace_context: Option<&Context>,
) -> Span {
    let span = create_database_api_span(operation, database_id, table_name);

    // Set distributed tracing parent context if available
    if let Some(context) = trace_context {
        if has_active_span(context) {
            span.set_parent(context.clone());
            if let Some(trace_id) = get_trace_id_hex(context) {
                span.record("trace.trace_id", trace_id.as_str());
            }
        }
    }

    span
}

/// Records HTTP response attributes on a span
pub fn record_http_response(span: &Span, response: &Response, duration: Duration) {
    span.record("http.status_code", response.status().as_u16());
    span.record("http.response_time_ms", duration.as_millis() as u64);
    
    // Record response body size if available
    if let Some(content_length) = response.headers().get("content-length") {
        if let Ok(length_str) = content_length.to_str() {
            if let Ok(length) = length_str.parse::<u64>() {
                span.record("http.response.body.size", length);
            }
        }
    }
}

/// Records an error on a span with context
pub fn record_error(span: &Span, error: &dyn std::error::Error) {
    span.record("error.type", error.to_string().as_str());
    span.record("error.message", error.to_string().as_str());
    
    // Record error chain if available
    let mut source = error.source();
    let mut depth = 1;
    while let Some(err) = source {
        span.record(
            format!("error.source.{}", depth).as_str(),
            err.to_string().as_str(),
        );
        source = err.source();
        depth += 1;
        if depth > 5 {
            break; // Limit error chain depth
        }
    }
}

/// Records authentication context on a span
pub fn record_auth_context(span: &Span, identity: Option<&Identity>) {
    if let Some(identity) = identity {
        span.record("spacetimedb.identity", identity.to_hex().as_str());
        span.record("spacetimedb.authenticated", true);
    } else {
        span.record("spacetimedb.authenticated", false);
    }
}

/// Extracts HTTP request attributes from Axum extractors with distributed tracing support
pub fn extract_http_attributes(
    method: Method,
    uri: Uri,
    headers: &HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    identity: Option<&Identity>,
) -> HttpRequestAttributes {
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let client_ip = connect_info.map(|ci| ci.0);

    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    // Extract distributed tracing context from headers
    let trace_context = extract_trace_context_from_headers(headers);
    let trace_id = get_trace_id_hex(&trace_context);

    HttpRequestAttributes {
        method,
        uri,
        user_agent,
        client_ip,
        content_length,
        identity: identity.cloned(),
        trace_context: if has_active_span(&trace_context) { Some(trace_context) } else { None },
        trace_id,
    }
}

/// Extracts WebSocket attributes from connection info with distributed tracing support
pub fn extract_websocket_attributes(
    connection_id: String,
    headers: &HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    identity: Option<&Identity>,
) -> WebSocketAttributes {
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let protocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let client_ip = connect_info.map(|ci| ci.0);

    // Extract distributed tracing context from WebSocket headers
    let (trace_context, spacetimedb_context) = extract_websocket_trace_context(headers)
        .unwrap_or_else(|_| (Context::new(), None));

    WebSocketAttributes {
        connection_id,
        client_ip,
        user_agent,
        protocol,
        identity: identity.cloned(),
        trace_context: if has_active_span(&trace_context) { Some(trace_context) } else { None },
        spacetimedb_context,
    }
}

/// Inject distributed tracing context into HTTP response headers
pub fn inject_http_trace_context(headers: &mut HeaderMap, context: &Context) {
    inject_trace_context_into_headers(context, headers);
}

/// Inject distributed tracing context into WebSocket response headers
pub fn inject_websocket_trace_context_headers(
    headers: &mut HeaderMap,
    context: &Context,
    spacetimedb_ctx: Option<&SpacetimeDBContext>,
) -> Result<(), anyhow::Error> {
    inject_websocket_trace_context(headers, context, spacetimedb_ctx)
}

/// Create SpacetimeDB context for distributed tracing
pub fn create_spacetimedb_trace_context(
    database_id: Option<&str>,
    module_hash: Option<&str>,
    operation_type: Option<&str>,
    identity: Option<&Identity>,
) -> SpacetimeDBContext {
    let mut ctx = SpacetimeDBContext::new();
    
    if let Some(db_id) = database_id {
        ctx = ctx.with_database_id(db_id);
    }
    
    if let Some(hash) = module_hash {
        ctx = ctx.with_module_hash(hash);
    }
    
    if let Some(op_type) = operation_type {
        ctx = ctx.with_operation_type(op_type);
    }
    
    if let Some(identity) = identity {
        ctx = ctx.with_identity(*identity);
    }
    
    ctx
}

/// Extract trace context from current span for cross-service calls
pub fn get_current_trace_context() -> Context {
    Context::current()
}

/// Macro for creating HTTP request spans in endpoint handlers with distributed tracing
#[macro_export]
macro_rules! http_span {
    ($method:expr, $uri:expr, $headers:expr) => {
        $crate::telemetry::create_http_request_span(
            &$crate::telemetry::extract_http_attributes(
                $method,
                $uri,
                $headers,
                None,
                None,
            )
        )
    };
    ($method:expr, $uri:expr, $headers:expr, $connect_info:expr) => {
        $crate::telemetry::create_http_request_span(
            &$crate::telemetry::extract_http_attributes(
                $method,
                $uri,
                $headers,
                Some($connect_info),
                None,
            )
        )
    };
    ($method:expr, $uri:expr, $headers:expr, $connect_info:expr, $identity:expr) => {
        $crate::telemetry::create_http_request_span(
            &$crate::telemetry::extract_http_attributes(
                $method,
                $uri,
                $headers,
                Some($connect_info),
                Some($identity),
            )
        )
    };
}

/// Macro for creating WebSocket spans with distributed tracing
#[macro_export]
macro_rules! websocket_span {
    ($connection_id:expr, $headers:expr) => {
        $crate::telemetry::create_websocket_span(
            &$crate::telemetry::extract_websocket_attributes(
                $connection_id,
                $headers,
                None,
                None,
            )
        )
    };
    ($connection_id:expr, $headers:expr, $connect_info:expr) => {
        $crate::telemetry::create_websocket_span(
            &$crate::telemetry::extract_websocket_attributes(
                $connection_id,
                $headers,
                Some($connect_info),
                None,
            )
        )
    };
    ($connection_id:expr, $headers:expr, $connect_info:expr, $identity:expr) => {
        $crate::telemetry::create_websocket_span(
            &$crate::telemetry::extract_websocket_attributes(
                $connection_id,
                $headers,
                Some($connect_info),
                Some($identity),
            )
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, Uri};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_http_span_creation() {
        let attrs = HttpRequestAttributes {
            method: Method::GET,
            uri: Uri::from_static("/database/test"),
            user_agent: Some("test-client/1.0".to_string()),
            client_ip: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
            content_length: Some(1024),
            identity: None,
            trace_context: None,
            trace_id: None,
        };

        let span = create_http_request_span(&attrs);
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb_http_request");
    }

    #[test]
    fn test_websocket_span_creation() {
        let attrs = WebSocketAttributes {
            connection_id: "test-connection-123".to_string(),
            client_ip: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
            user_agent: Some("test-client/1.0".to_string()),
            protocol: Some("spacetimedb-ws".to_string()),
            identity: None,
            trace_context: None,
            spacetimedb_context: None,
        };

        let span = create_websocket_span(&attrs);
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb_websocket_connection");
    }

    #[test]
    fn test_database_api_span() {
        let span = create_database_api_span("query", Some("test-db"), Some("users"));
        assert_eq!(span.metadata().unwrap().name(), "spacetimedb_database_api");
    }

    #[test]
    fn test_spacetimedb_context_creation() {
        let identity = Identity::from_byte_array([1; 32]);
        let ctx = create_spacetimedb_trace_context(
            Some("test-db"),
            Some("abc123"),
            Some("call_reducer"),
            Some(&identity),
        );

        assert_eq!(ctx.database_id, Some("test-db".to_string()));
        assert_eq!(ctx.module_hash, Some("abc123".to_string()));
        assert_eq!(ctx.operation_type, Some("call_reducer".to_string()));
        assert_eq!(ctx.identity, Some(identity));
    }
}
