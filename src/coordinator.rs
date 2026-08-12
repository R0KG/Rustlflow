use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    distributed_query::{merge_count_results, parse_grouped_count},
    storage::{BlobStorage, job_result_key, put_or_verify},
};

pub async fn try_complete_job(
    pool: &PgPool,
    storage: &dyn BlobStorage,
    job_id: Uuid,
) -> Result<(), String> {
    let job: Option<(String, String)> =
        sqlx::query_as("SELECT status, sql FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(pool)
            .await
            .map_err(|_| "Could not read parent job".to_string())?;
    let Some((status, sql)) = job else {
        return Err("Parent job was not found".to_string());
    };
    if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
        return Ok(());
    }
    let task_statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM job_tasks WHERE job_id = $1")
            .bind(job_id)
            .fetch_all(pool)
            .await
            .map_err(|_| "Could not read task statuses".to_string())?;
    if task_statuses.iter().any(|status| status == "failed") {
        sqlx::query("UPDATE jobs SET status = 'failed', error = 'A task failed', completed_at = NOW() WHERE id = $1 AND status IN ('queued', 'running')")
            .bind(job_id).execute(pool).await.map_err(|_| "Could not fail parent job".to_string())?;
        return Ok(());
    }
    if task_statuses.is_empty() || task_statuses.iter().any(|status| status != "completed") {
        return Ok(());
    }
    let query = parse_grouped_count(&sql)?;
    let task_results: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT result_key, result::text
         FROM job_tasks
         WHERE job_id = $1
         ORDER BY id",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await
    .map_err(|_| "Could not read task results".to_string())?;
    let mut results = Vec::with_capacity(task_results.len());
    for (result_key, result_text) in task_results {
        let text = match result_key {
            Some(key) => String::from_utf8(
                storage
                    .get(&key)
                    .await
                    .map_err(|_| "Could not read task result object".to_string())?,
            )
            .map_err(|_| "Task result object was not UTF-8 JSON".to_string())?,
            None => result_text.ok_or("Completed task has no result".to_string())?,
        };
        results
            .push(serde_json::from_str(&text).map_err(|_| "Task stored invalid JSON".to_string())?);
    }
    let result = merge_count_results(&query, &results)?;
    let result_text = result.to_string();
    let result_key = job_result_key(job_id);
    put_or_verify(storage, &result_key, result_text.as_bytes())
        .await
        .map_err(|_| "Could not store parent job result object".to_string())?;
    sqlx::query("UPDATE jobs SET status = 'completed', result = $2::jsonb, result_key = $3, completed_at = NOW() WHERE id = $1 AND status = 'running'")
        .bind(job_id).bind(result_text).bind(result_key).execute(pool).await.map_err(|_| "Could not complete parent job".to_string())?;
    Ok(())
}
