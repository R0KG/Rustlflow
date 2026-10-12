use axum::{
    Json,
    extract::{Multipart, Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    app::AppState,
    errors::{AppError, ErrorResponse},
    models::{DatasetListResponse, DatasetResponse, DatasetSummary},
    storage::dataset_source_key,
};

const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;
const DEFAULT_DATASET_PAGE_SIZE: i64 = 20;
const MAX_DATASET_PAGE_SIZE: i64 = 100;

#[derive(Deserialize)]
pub struct DatasetListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/datasets",
    params(
        ("limit" = Option<i64>, Query, description = "Page size from 1 to 100; defaults to 20"),
        ("offset" = Option<i64>, Query, description = "Number of datasets to skip; defaults to 0")
    ),
    responses(
        (status = 200, description = "Datasets ordered from newest to oldest", body = DatasetListResponse),
        (status = 400, description = "Invalid pagination parameters", body = ErrorResponse),
        (status = 500, description = "Database failure", body = ErrorResponse)
    ),
    tag = "datasets"
)]
pub async fn list_datasets(
    State(state): State<AppState>,
    query: Result<Query<DatasetListQuery>, QueryRejection>,
) -> Result<Json<DatasetListResponse>, AppError> {
    let Query(query) = query
        .map_err(|_| AppError::BadRequest("Invalid dataset pagination parameters".to_string()))?;
    let limit = query.limit.unwrap_or(DEFAULT_DATASET_PAGE_SIZE);
    if !(1..=MAX_DATASET_PAGE_SIZE).contains(&limit) {
        return Err(AppError::BadRequest(
            "Dataset limit must be between 1 and 100".to_string(),
        ));
    }
    let offset = query.offset.unwrap_or(0);
    if offset < 0 {
        return Err(AppError::BadRequest(
            "Dataset offset cannot be negative".to_string(),
        ));
    }

    let rows: Vec<(Uuid, String, String, String, i64)> = sqlx::query_as(
        "SELECT id, filename, format, status, size_bytes
         FROM datasets
         ORDER BY created_at DESC, id DESC
         LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| AppError::Internal("Could not list datasets".to_string()))?;

    let datasets = rows
        .into_iter()
        .map(
            |(id, filename, format, status, size_bytes)| DatasetSummary {
                id: id.to_string(),
                filename,
                format,
                status,
                size_bytes,
            },
        )
        .collect();

    Ok(Json(DatasetListResponse {
        datasets,
        limit,
        offset,
    }))
}

#[utoipa::path(
    post,
    path = "/datasets",
    request_body(content = String, content_type = "multipart/form-data", description = "CSV file in the file form field"),
    responses(
        (status = 201, description = "Dataset stored", body = DatasetResponse),
        (status = 400, description = "Invalid upload", body = ErrorResponse),
        (status = 500, description = "Storage or database failure", body = ErrorResponse)
    ),
    tag = "datasets"
)]
pub async fn create_dataset(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<DatasetResponse>), AppError> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| AppError::BadRequest("Invalid multipart request".to_string()))?
    {
        if field.name() != Some("file") {
            continue;
        }

        let filename = field
            .file_name()
            .map(str::to_owned)
            .ok_or(AppError::BadRequest("Missing filename".to_string()))?;

        if !filename.to_ascii_lowercase().ends_with(".csv") {
            return Err(AppError::BadRequest(
                "Only CSV uploads are supported".to_string(),
            ));
        }

        let bytes = field
            .bytes()
            .await
            .map_err(|_| AppError::BadRequest("Could not read file".to_string()))?;

        if bytes.is_empty() {
            return Err(AppError::BadRequest("File is empty".to_string()));
        }
        if bytes.len() > MAX_UPLOAD_BYTES {
            return Err(AppError::BadRequest(
                "File exceeds the 50 MiB upload limit".to_string(),
            ));
        }

        let dataset_id = Uuid::new_v4();
        let storage_key = dataset_source_key(dataset_id);
        state
            .storage
            .put(&storage_key, &bytes)
            .await
            .map_err(|_| AppError::Internal("Dataset object could not be saved".to_string()))?;

        let saved = sqlx::query(
            "INSERT INTO datasets
     (id, filename, storage_path, storage_key, format, size_bytes, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(dataset_id)
        .bind(&filename)
        .bind(&storage_key)
        .bind(&storage_key)
        .bind("csv")
        .bind(bytes.len() as i64)
        .bind("stored")
        .execute(&state.pool)
        .await;
        if saved.is_err() {
            if let Err(error) = state.storage.delete(&storage_key).await {
                tracing::warn!(%error, %storage_key, "Could not remove orphaned dataset object");
            }
            return Err(AppError::Internal(
                "Could not save dataset metadata".to_string(),
            ));
        }
        return Ok((
            StatusCode::CREATED,
            Json(DatasetResponse {
                id: dataset_id.to_string(),
                filename,
                format: "csv".to_string(),
                status: "stored".to_string(),
            }),
        ));
    }

    Err(AppError::BadRequest("No file field provided".to_string()))
}
