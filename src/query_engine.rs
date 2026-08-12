use std::path::Path;

use datafusion::{
    arrow::array::RecordBatch,
    execution::{context::SessionContext, options::CsvReadOptions},
};

use crate::storage::{BlobStorage, materialize_blob};

pub async fn execute_query(
    dataset_path: &Path,
    sql: &str,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let ctx = SessionContext::new();

    let path_string = dataset_path.to_string_lossy().to_string();

    ctx.register_csv("events", &path_string, CsvReadOptions::new())
        .await?;

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    Ok(results)
}

pub async fn execute_stored_query(
    storage: &dyn BlobStorage,
    storage_key: Option<&str>,
    legacy_storage_path: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let materialized = materialize_blob(storage, storage_key, legacy_storage_path)
        .await
        .map_err(|_| "Could not materialize dataset object".to_string())?;
    let result = execute_query(materialized.path(), sql)
        .await
        .map_err(|error| error.to_string());
    materialized.cleanup().await;
    result
}
