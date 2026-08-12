use crate::errors::{AppError, ErrorResponse};
use axum::Json;
use uuid::Uuid;

use crate::{
    app::AppState,
    models::{QueryRequest, QueryResponse},
    query_engine::execute_stored_query,
    serialization::record_batches_to_json,
};

#[utoipa::path(
    post,
    path = "/queries",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query result", body = QueryResponse),
        (status = 400, description = "Invalid dataset ID or SQL", body = ErrorResponse),
        (status = 404, description = "Dataset not found", body = ErrorResponse),
        (status = 500, description = "Query result serialization failed", body = ErrorResponse)
    ),
    tag = "queries"
)]
pub async fn query(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    if payload.sql.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Problem with sql command is either empty or incorrect".to_string(),
        ));
    }

    let dataset_id = Uuid::parse_str(&payload.dataset_id)
        .map_err(|_| AppError::BadRequest("Error with dataset id".to_string()))?;

    let dataset: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT storage_key, storage_path FROM datasets WHERE id =$1")
            .bind(dataset_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| {
                AppError::Internal("Could not get dataset storage metadata".to_string())
            })?;

    let (storage_key, storage_path) =
        dataset.ok_or(AppError::NotFound("Dataset was not found".to_string()))?;
    let batches = execute_stored_query(
        state.storage.as_ref(),
        storage_key.as_deref(),
        &storage_path,
        &payload.sql,
    )
    .await
    .map_err(|_| AppError::BadRequest("Could not execute sql query".to_string()))?;

    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    let rows = record_batches_to_json(&batches).map_err(|error| {
        AppError::Internal(format!("Could not serialize query result: {error}"))
    })?;

    Ok(Json(QueryResponse {
        dataset_id: payload.dataset_id,
        row_count,
        rows,
    }))
}
