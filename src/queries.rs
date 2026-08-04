use std::path::Path;

use axum::{Json, http::StatusCode};
use uuid::Uuid;

use crate::{
    models::{QueryRequest, QueryResponse},
    query_engine::execute_query,
    serialization::record_batches_to_json,
};

pub async fn query(
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    if payload.sql.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Problem with sql command is either empty or incorrect".to_string(),
        ));
    }

    let dataset_id = Uuid::parse_str(&payload.dataset_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Error with dataset id".to_string()))?;

    let dataset_path = Path::new("data/datasets").join(format!("{dataset_id}.csv"));

    if !dataset_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Dataset was not found".to_string()));
    }

    let batches = execute_query(&dataset_path, &payload.sql)
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "Could not execute sql query".to_string(),
            )
        })?;

    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    let rows = record_batches_to_json(&batches).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not serialize query result: {error}"),
        )
    })?;

    Ok(Json(QueryResponse {
        dataset_id: payload.dataset_id,
        row_count,
        rows,
    }))
}
