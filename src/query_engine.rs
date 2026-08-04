use std::path::Path;

use datafusion::{
    arrow::array::RecordBatch,
    execution::{context::SessionContext, options::CsvReadOptions},
};

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
