use axum::{Json, extract::Multipart, http::StatusCode};
use uuid::Uuid;

use crate::{
    app::AppState,
    errors::{AppError, ErrorResponse},
    models::DatasetResponse,
    storage::dataset_source_key,
};

const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

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
