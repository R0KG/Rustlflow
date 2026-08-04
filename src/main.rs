mod basic;
mod datasets;
mod models;
mod queries;
mod query_engine;
mod serialization;

use axum::{
    Router,
    routing::{get, post},
};
use basic::{health, hello};
use datasets::create_dataset;
use queries::query;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/hello/{name}", get(hello))
        .route("/datasets", post(create_dataset))
        .route("/queries", post(query));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind address");

    println!("Server running on local host 3000");

    axum::serve(listener, app).await.expect("server failed");
}
