#[derive(serde::Serialize)]
pub struct DatasetResponse {
    pub id: String,
    pub filename: String,
    pub format: String,
    pub status: String,
}

#[derive(serde::Deserialize)]
pub struct QueryRequest {
    pub dataset_id: String,
    pub sql: String,
}

#[derive(serde::Serialize)]
pub struct HelloResponse {
    pub message: String,
}

#[derive(serde::Serialize)]
pub struct QueryResponse {
    pub dataset_id: String,
    pub row_count: usize,
    pub rows: Vec<serde_json::Value>,
}

#[derive(serde::Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}
