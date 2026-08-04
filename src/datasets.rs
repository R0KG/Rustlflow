use std::path::Path;

use axum::{Json, extract::Multipart, http::StatusCode};
use uuid::Uuid;

use crate::models::DatasetResponse;

async fn save_file(path: &Path, contents: &[u8]) -> Result<(), std::io::Error> {
    tokio::fs::write(path, contents).await
}

pub async fn create_dataset(
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<DatasetResponse>), (StatusCode, String)> {
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
            return Err((StatusCode::BAD_REQUEST, "File is empty".to_string()));
        }

        let dataset_id = Uuid::new_v4();
        let directory = Path::new("data/datasets");

        tokio::fs::create_dir_all(directory).await.map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Directory could not be created/opened".to_string(),
            )
        })?;

        let storage_path = directory.join(format!("{dataset_id}.csv"));

        save_file(&storage_path, &bytes).await.map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "File could not be saved".to_string(),
            )
        })?;

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

    Err((
        StatusCode::BAD_REQUEST,
        "No file field provided".to_string(),
    ))
}
