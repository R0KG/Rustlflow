use axum::{
    extract::State,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};

use crate::{app::AppState, observability::render_metrics};

#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Prometheus metrics", body = String, content_type = "text/plain"),
        (status = 500, description = "Metrics could not be rendered")
    ),
    tag = "observability"
)]
pub async fn metrics_handler(State(state): State<AppState>) -> Response {
    if let Err(error) = refresh_database_metrics(&state).await {
        tracing::error!(%error, "Could not refresh database-backed metrics");
    }
    match render_metrics() {
        Ok(body) => (
            StatusCode::OK,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4"),
            )],
            body,
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "Could not render Prometheus metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn refresh_database_metrics(state: &AppState) -> Result<(), sqlx::Error> {
    let queue_depth: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM job_tasks WHERE status IN ('queued', 'running')")
            .fetch_one(&state.pool)
            .await?;
    let active_workers: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT worker_id) FROM job_tasks
         WHERE status = 'running' AND worker_id IS NOT NULL",
    )
    .fetch_one(&state.pool)
    .await?;
    let scanned_rows: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(partition.row_count), 0)::bigint
         FROM job_tasks AS task
         INNER JOIN dataset_partitions AS partition ON partition.id = task.dataset_partition_id
         WHERE task.status = 'completed'",
    )
    .fetch_one(&state.pool)
    .await?;
    metrics::gauge!("rustflow_queue_depth").set(queue_depth as f64);
    metrics::gauge!("rustflow_active_workers").set(active_workers as f64);
    metrics::gauge!("rustflow_scanned_rows").set(scanned_rows as f64);
    Ok(())
}
