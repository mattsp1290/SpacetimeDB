//! Custom tracing layer for SpacetimeDB-specific span enrichment

use tracing::{span, Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

/// Custom tracing layer that enriches spans with SpacetimeDB-specific attributes
pub struct SpacetimeDBLayer {
    // Future: Add fields for database context, module context, etc.
}

impl SpacetimeDBLayer {
    /// Create a new SpacetimeDB tracing layer
    pub fn new() -> Self {
        Self {}
    }
}

impl<S> Layer<S> for SpacetimeDBLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        
        // Add SpacetimeDB-specific metadata
        extensions.insert(SpacetimeDBSpanData {
            component: attrs
                .metadata()
                .module_path()
                .unwrap_or("unknown")
                .to_string(),
            created_at: std::time::SystemTime::now(),
        });
    }

    fn on_record(
        &self,
        _id: &span::Id,
        _values: &span::Record<'_>,
        _ctx: Context<'_, S>,
    ) {
        // Future: Process span field updates
    }

    fn on_event(&self, _event: &Event<'_>, ctx: Context<'_, S>) {
        // Add context from parent spans
        if let Some(span) = ctx.lookup_current() {
            if let Some(data) = span.extensions().get::<SpacetimeDBSpanData>() {
                // Future: Add span context to events
                tracing::trace!(
                    spacetimedb.component = %data.component,
                    "Event in SpacetimeDB component"
                );
            }
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(data) = span.extensions().get::<SpacetimeDBSpanData>() {
                // Future: Track active spans per component
                tracing::trace!(
                    spacetimedb.component = %data.component,
                    "Entering SpacetimeDB span"
                );
            }
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(data) = span.extensions().get::<SpacetimeDBSpanData>() {
                let duration = data.created_at.elapsed().unwrap_or_default();
                
                // Future: Record span duration metrics
                tracing::trace!(
                    spacetimedb.component = %data.component,
                    spacetimedb.duration_ms = duration.as_millis(),
                    "Exiting SpacetimeDB span"
                );
            }
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(data) = span.extensions().get::<SpacetimeDBSpanData>() {
                let duration = data.created_at.elapsed().unwrap_or_default();
                
                // Future: Final span metrics and cleanup
                tracing::trace!(
                    spacetimedb.component = %data.component,
                    spacetimedb.total_duration_ms = duration.as_millis(),
                    "Closing SpacetimeDB span"
                );
            }
        }
    }
}

/// Data stored in span extensions for SpacetimeDB context
struct SpacetimeDBSpanData {
    component: String,
    created_at: std::time::SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;
    use tracing_subscriber::prelude::*;

    #[test]
    fn test_layer_creation() {
        let layer = SpacetimeDBLayer::new();
        
        // Test that the layer can be added to a subscriber
        let _subscriber = tracing_subscriber::registry()
            .with(layer)
            .with(tracing_subscriber::fmt::layer());
    }

    #[test]
    fn test_span_enrichment() {
        let layer = SpacetimeDBLayer::new();
        
        let subscriber = tracing_subscriber::registry()
            .with(layer)
            .with(tracing_subscriber::fmt::layer().with_test_writer());
        
        let _guard = tracing::subscriber::set_default(subscriber);
        
        // Create a test span
        let span = tracing::span!(Level::INFO, "test_span");
        let _enter = span.enter();
        
        // The span should be enriched with SpacetimeDB data
        tracing::info!("Test event within span");
    }
}
