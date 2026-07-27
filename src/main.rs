use axum::{
    Json, Router,
    extract::{Multipart, Path as RoutePath},
    http::StatusCode,
    routing::{get, post},
};

use std::path::Path as FilePath;
use uuid::Uuid;
#[derive(serde::Deserialize)]
struct CreateDatasetRequest {
    name: String,
    format: String,
}

#[derive(serde::Serialize)]
struct DatasetResponse {
    id: u64,
    filename: String,
    format: String,
    status: String,
}
#[derive(serde::Serialize)]
struct HelloResponse {
    message: String,
}

#[derive(serde::Serialize)]
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

async fn save_file(path: &FilePath, contents: &[u8]) -> Result<(), std::io::Error> {
    tokio::fs::write(path, contents).await
}

async fn create_dataset(
    mut multipart: Multipart,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    while let Some(field) = multipart.next_field().await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid multipart request".to_string(),
        )
    })? {
        if field.name() != Some("file") {
            continue;
        }

        let filename = field
            .file_name()
            .map(str::to_owned)
            .ok_or((StatusCode::BAD_REQUEST, "Missing filename".to_string()))?;

        let bytes = field
            .bytes()
            .await
            .map_err(|_| (StatusCode::BAD_REQUEST, "Could not read file".to_string()))?;

        if bytes.is_empty() {
            return Err((StatusCode::BAD_REQUEST, "File is is_empty".to_string()));
        }

        return Ok((
            StatusCode::OK,
            format!(
                "Got the file with name {filename} and size: ({} bytes )",
                bytes.len()
            ),
        ));
    }
    Err((
        StatusCode::BAD_REQUEST,
        "No file field provided".to_string(),
    ))
}
async fn hello(RoutePath(name): RoutePath<String>) -> Json<HelloResponse> {
    Json(HelloResponse {
        message: format!("Hello, {name}"),
    })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
