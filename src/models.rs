#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct DatasetResponse {
    pub id: String,
    pub filename: String,
    pub format: String,
    pub status: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct DatasetSummary {
    pub id: String,
    pub filename: String,
    pub format: String,
    pub status: String,
    pub size_bytes: i64,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct DatasetListResponse {
    pub datasets: Vec<DatasetSummary>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct QueryRequest {
    pub dataset_id: String,
    pub sql: String,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct CreateJobRequest {
    pub dataset_id: String,
    pub sql: String,
}
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct HelloResponse {
    pub message: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub status: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct JobResultResponse {
    pub job_id: String,
    pub result: serde_json::Value,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct CreateJobResponse {
    pub job_id: String,
    pub status: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct QueryResponse {
    pub dataset_id: String,
    pub row_count: usize,
    pub rows: Vec<serde_json::Value>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
}
