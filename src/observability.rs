use std::{collections::HashMap, sync::OnceLock};

use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::{
    Context, global,
    propagation::{Extractor, Injector},
    trace::{TraceContextExt as _, TracerProvider as _},
};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_observability(service_name: &str) -> Result<(), String> {
    let _ = prometheus_handle()?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let fmt_layer = tracing_subscriber::fmt::layer().json();

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|error| format!("Could not create OTLP trace exporter: {error}"))?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                Resource::builder()
                    .with_service_name(service_name.to_string())
                    .build(),
            )
            .build();
        let tracer = provider.tracer(service_name.to_string());
        global::set_tracer_provider(provider);
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .map_err(|error| format!("Could not initialize tracing subscriber: {error}"))?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()
            .map_err(|error| format!("Could not initialize tracing subscriber: {error}"))?;
    }
    Ok(())
}

pub fn prometheus_handle() -> Result<PrometheusHandle, String> {
    if let Some(handle) = PROMETHEUS.get() {
        return Ok(handle.clone());
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| format!("Could not install Prometheus recorder: {error}"))?;
    let _ = PROMETHEUS.set(handle.clone());
    Ok(PROMETHEUS.get().cloned().unwrap_or(handle))
}

pub fn render_metrics() -> Result<String, String> {
    Ok(prometheus_handle()?.render())
}

pub async fn request_context(request: Request<Body>, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let parent = extract_parent(
        request
            .headers()
            .get("traceparent")
            .and_then(|value| value.to_str().ok()),
    );
    let span = tracing::info_span!(
        "http_request",
        request_id = %request_id,
        http.method = %method,
        url.path = %path,
        http.status_code = tracing::field::Empty,
    );
    if let Some(parent) = parent {
        let _ = span.set_parent(parent);
    }

    async move {
        let mut response = next.run(request).await;
        let status = response.status().as_u16();
        tracing::Span::current().record("http.status_code", status);
        metrics::counter!("rustflow_http_requests_total", "method" => method, "path" => path, "status" => status.to_string()).increment(1);
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert(REQUEST_ID_HEADER, value);
        }
        response
    }
    .instrument(span)
    .await
}

pub fn current_traceparent() -> Option<String> {
    let mut carrier = TraceParentMap::default();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&tracing::Span::current().context(), &mut carrier)
    });
    carrier.0.remove("traceparent")
}

pub fn extract_parent(traceparent: Option<&str>) -> Option<Context> {
    let traceparent = traceparent?;
    global::get_text_map_propagator(|propagator| {
        let carrier = TraceParentCarrier { traceparent };
        let context = propagator.extract(&carrier);
        context.has_active_span().then_some(context)
    })
}

struct TraceParentCarrier<'a> {
    traceparent: &'a str,
}

impl Extractor for TraceParentCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        (key.eq_ignore_ascii_case("traceparent")).then_some(self.traceparent)
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

#[derive(Default)]
struct TraceParentMap(HashMap<String, String>);

impl Injector for TraceParentMap {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}
