use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    coordinator::try_complete_job,
    query_engine::execute_stored_query,
    reliability::{lease_seconds, task_timeout},
    serialization::record_batches_to_json,
    storage::{BlobStorage, put_or_verify, task_result_key},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskProcessOutcome {
    Completed,
    Failed,
    Cancelled,
    AlreadyTerminal,
    LeaseHeld,
}

async fn claim_task(
    pool: &PgPool,
    task_id: Uuid,
    worker_id: &str,
) -> Result<Result<(Uuid, String, Option<String>, String), TaskProcessOutcome>, String> {
    let claimed: Option<(Uuid, String, Option<String>, String)> = sqlx::query_as(
        "UPDATE job_tasks AS task
         SET status = 'running', started_at = COALESCE(task.started_at, NOW()),
             worker_id = $2, lease_expires_at = NOW() + ($3 * INTERVAL '1 second'),
             attempt_count = task.attempt_count + 1
         FROM jobs AS job, dataset_partitions AS partition
         WHERE task.id = $1
           AND task.job_id = job.id
           AND task.dataset_partition_id = partition.id
           AND job.status IN ('queued', 'running')
           AND (task.status = 'queued' OR (task.status = 'running' AND task.lease_expires_at < NOW()))
         RETURNING task.job_id, job.sql, partition.storage_key, partition.storage_path",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(lease_seconds())
    .fetch_optional(pool)
    .await
    .map_err(|_| "Could not claim task".to_string())?;

    if let Some(task) = claimed {
        return Ok(Ok(task));
    }

    let current: Option<(String, String, Option<bool>)> = sqlx::query_as(
        "SELECT task.status, job.status, task.lease_expires_at > NOW()
         FROM job_tasks AS task
         INNER JOIN jobs AS job ON job.id = task.job_id
         WHERE task.id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| "Could not read task after claim conflict".to_string())?;

    match current {
        Some((_, parent, _)) if matches!(parent.as_str(), "completed" | "failed" | "cancelled") => {
            Ok(Err(TaskProcessOutcome::AlreadyTerminal))
        }
        Some((status, _, _)) if matches!(status.as_str(), "completed" | "failed" | "cancelled") => {
            Ok(Err(TaskProcessOutcome::AlreadyTerminal))
        }
        Some((status, _, Some(true))) if status == "running" => {
            Ok(Err(TaskProcessOutcome::LeaseHeld))
        }
        Some((status, _, _)) => Err(format!("Task could not be claimed from status: {status}")),
        None => Err("Task was not found".to_string()),
    }
}

async fn fail_task(
    pool: &PgPool,
    task_id: Uuid,
    worker_id: &str,
    error: String,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE job_tasks
         SET status = 'failed', error = $3, completed_at = NOW(), worker_id = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND worker_id = $2",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(error)
    .execute(pool)
    .await
    .map_err(|_| "Could not mark task as failed".to_string())?;
    Ok(())
}

pub async fn process_task(
    pool: &PgPool,
    storage: &dyn BlobStorage,
    task_id: Uuid,
    worker_id: &str,
) -> Result<TaskProcessOutcome, String> {
    let (job_id, sql, storage_key, storage_path) =
        match claim_task(pool, task_id, worker_id).await? {
            Ok(task) => task,
            Err(outcome) => return Ok(outcome),
        };
    sqlx::query("UPDATE jobs SET status = 'running', started_at = COALESCE(started_at, NOW()) WHERE id = $1 AND status = 'queued'")
        .bind(job_id).execute(pool).await.map_err(|_| "Could not mark parent job as running".to_string())?;

    tracing::info!(%task_id, %job_id, %worker_id, "Worker claimed task");
    metrics::counter!("rustflow_tasks_total", "outcome" => "started").increment(1);
    let batches = match tokio::time::timeout(
        task_timeout(),
        execute_stored_query(storage, storage_key.as_deref(), &storage_path, &sql),
    )
    .await
    {
        Ok(Ok(batches)) => batches,
        Ok(Err(error)) => {
            fail_task(
                pool,
                task_id,
                worker_id,
                format!("Could not execute task query: {error}"),
            )
            .await?;
            try_complete_job(pool, storage, job_id).await?;
            metrics::counter!("rustflow_tasks_total", "outcome" => "failed").increment(1);
            return Ok(TaskProcessOutcome::Failed);
        }
        Err(_) => {
            fail_task(
                pool,
                task_id,
                worker_id,
                format!(
                    "Task exceeded the configured {} second execution timeout",
                    task_timeout().as_secs()
                ),
            )
            .await?;
            try_complete_job(pool, storage, job_id).await?;
            metrics::counter!("rustflow_tasks_total", "outcome" => "timed_out").increment(1);
            return Ok(TaskProcessOutcome::Failed);
        }
    };
    let rows = match record_batches_to_json(&batches) {
        Ok(rows) => rows,
        Err(error) => {
            fail_task(
                pool,
                task_id,
                worker_id,
                format!("Could not serialize task result: {error}"),
            )
            .await?;
            try_complete_job(pool, storage, job_id).await?;
            metrics::counter!("rustflow_tasks_total", "outcome" => "failed").increment(1);
            return Ok(TaskProcessOutcome::Failed);
        }
    };
    let result = serde_json::json!({"row_count": rows.len(), "rows": rows});
    let result_key = task_result_key(job_id, task_id);
    if let Err(error) = put_or_verify(storage, &result_key, result.to_string().as_bytes()).await {
        fail_task(
            pool,
            task_id,
            worker_id,
            format!("Could not store task result object: {error}"),
        )
        .await?;
        try_complete_job(pool, storage, job_id).await?;
        metrics::counter!("rustflow_tasks_total", "outcome" => "failed").increment(1);
        return Ok(TaskProcessOutcome::Failed);
    }
    let completed = sqlx::query(
        "UPDATE job_tasks
         SET status = 'completed', result = $3::jsonb, result_key = $4, completed_at = NOW(), worker_id = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND worker_id = $2",
    )
    .bind(task_id).bind(worker_id).bind(result.to_string()).bind(result_key).execute(pool).await
    .map_err(|_| "Could not save task result".to_string())?;
    if completed.rows_affected() == 0 {
        return Ok(TaskProcessOutcome::Cancelled);
    }
    try_complete_job(pool, storage, job_id).await?;
    tracing::info!(%task_id, %job_id, %worker_id, "Worker completed task");
    metrics::counter!("rustflow_tasks_total", "outcome" => "completed").increment(1);
    Ok(TaskProcessOutcome::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn process_task_saves_a_partial_result() {
        dotenvy::from_filename(".env.test").ok();
        let pool = crate::db::connect_database()
            .await
            .expect("database connection failed");
        let dataset_id = Uuid::new_v4();
        let partition_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let root = std::env::temp_dir().join(format!("rustflow-task-runner-{task_id}"));
        let storage = crate::storage::LocalBlobStorage::new(&root);
        let partition_key = format!("datasets/{dataset_id}/partitions/part-00000.csv");
        storage
            .put(&partition_key, b"endpoint\n/payments\n/login\n")
            .await
            .expect("could not store partition");

        sqlx::query("INSERT INTO datasets (id, filename, storage_path, format, size_bytes, status) VALUES ($1, $2, $3, 'csv', 0, 'stored')")
            .bind(dataset_id).bind("source.csv").bind("legacy-source.csv").execute(&pool).await.expect("could not insert dataset");
        sqlx::query("INSERT INTO dataset_partitions (id, dataset_id, partition_number, storage_path, storage_key, row_count) VALUES ($1, $2, 0, $3, $4, 2)")
            .bind(partition_id).bind(dataset_id).bind(&partition_key).bind(&partition_key).execute(&pool).await.expect("could not insert partition");
        sqlx::query("INSERT INTO jobs (id, dataset_id, sql, status) VALUES ($1, $2, $3, 'queued')")
            .bind(job_id)
            .bind(dataset_id)
            .bind("SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint")
            .execute(&pool)
            .await
            .expect("could not insert job");
        sqlx::query("INSERT INTO job_tasks (id, job_id, dataset_partition_id, status) VALUES ($1, $2, $3, 'queued')")
            .bind(task_id).bind(job_id).bind(partition_id).execute(&pool).await.expect("could not insert task");

        assert_eq!(
            process_task(&pool, &storage, task_id, "worker-a")
                .await
                .expect("could not process task"),
            TaskProcessOutcome::Completed
        );
        let task: (String, String, Option<String>) =
            sqlx::query_as("SELECT status, result::text, result_key FROM job_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("could not read task");
        assert_eq!(task.0, "completed");
        assert!(task.1.contains("/payments"));
        let task_result_key = task.2.expect("task result key should be recorded");
        assert!(
            storage
                .get(&task_result_key)
                .await
                .unwrap()
                .starts_with(b"{")
        );
        let parent: (String, Option<String>) =
            sqlx::query_as("SELECT status, result_key FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("could not read job");
        assert_eq!(parent.0, "completed");
        let parent_result_key = parent.1.expect("parent result key should be recorded");
        assert!(
            storage
                .get(&parent_result_key)
                .await
                .unwrap()
                .starts_with(b"{")
        );

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset");
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
