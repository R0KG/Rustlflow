use axum::{Json, extract::Path as RoutePath};

use crate::models::{HealthResponse, HelloResponse};

pub async fn hello(RoutePath(name): RoutePath<String>) -> Json<HelloResponse> {
    Json(HelloResponse {
        message: format!("Hello, {name}"),
    })
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
