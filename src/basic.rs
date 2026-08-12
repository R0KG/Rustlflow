use axum::{Json, extract::Path as RoutePath};

use crate::models::{HealthResponse, HelloResponse};

#[utoipa::path(
    get,
    path = "/hello/{name}",
    params(("name" = String, Path, description = "Name to include in the greeting")),
    responses((status = 200, description = "Greeting", body = HelloResponse)),
    tag = "utility"
)]
pub async fn hello(RoutePath(name): RoutePath<String>) -> Json<HelloResponse> {
    Json(HelloResponse {
        message: format!("Hello, {name}"),
    })
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, description = "Service is healthy", body = HealthResponse)),
    tag = "utility"
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
