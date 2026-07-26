use axum::{
    Json, Router,
    extract::Path,
    routing::{get, post},
};
use serde::Serialize;

#[derive(serde::Deserialize)]
struct DocumementRequest {
    name: String,
}
#[derive(serde::Deserialize)]
struct CreateDatasetRequest {
    name: String,
}

#[derive(serde::Serialize)]
struct DatesetResponse {
    id: u64,
    name: String,
}
#[derive(Serialize)]
struct HelloResponse {
    message: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/hello/{name}", get(hello))
        .route("/datasets", post(create_dataset));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind address");

    println!("Server running on local host 3000");

    axum::serve(listener, app).await.expect("server failed");
}
async fn create_dataset(Json(payload): Json<CreateDatasetRequest>) -> Json<DatesetResponse> {
    Json(DatesetResponse {
        id: 1,
        name: payload.name,
    })
}
async fn hello(Path(name): Path<String>) -> Json<HelloResponse> {
    Json(HelloResponse {
        message: format!("Hello, {name}"),
    })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
