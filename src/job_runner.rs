use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    query_engine::execute_stored_query,
    reliability::lease_seconds,
    serialization::record_batches_to_json,
    storage::{BlobStorage, LocalBlobStorage, job_result_key, put_or_verify},
};

async fn mark_job_failed(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: &str,
    error: &str,
) -> Result<(), String> {
    sqlx::query(
        "UPDATE jobs
         SET status = 'failed', error = $3, completed_at = NOW(),
             worker_id = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND worker_id = $2",
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(error)
    .execute(pool)
    .await
    .map_err(|_| "Could not mark job as failed".to_string())?;

    Ok(())
}

async fn fail_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: &str,
    error: String,
) -> Result<(), String> {
    if let Err(update_error) = mark_job_failed(pool, job_id, worker_id, &error).await {
        return Err(format!("{error}; {update_error}"));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Completed,
    Failed,
    Cancelled,
    AlreadyTerminal,
    LeaseHeld,
}

async fn claim_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: &str,
) -> Result<Result<(Uuid, String), ProcessOutcome>, String> {
    let claimed: Option<(Uuid, String)> = sqlx::query_as(
        "UPDATE jobs
         SET status = 'running', started_at = COALESCE(started_at, NOW()),
             worker_id = $2,
             lease_expires_at = NOW() + ($3 * INTERVAL '1 second'),
             attempt_count = attempt_count + 1
         WHERE id = $1
           AND (
               status = 'queued'
               OR (status = 'running' AND lease_expires_at < NOW())
           )
         RETURNING dataset_id, sql",
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(lease_seconds())
    .fetch_optional(pool)
    .await
    .map_err(|_| "Could not claim job".to_string())?;

    if let Some(job) = claimed {
        return Ok(Ok(job));
    }

    let current: Option<(String, Option<bool>)> = sqlx::query_as(
        "SELECT status, lease_expires_at > NOW()
         FROM jobs
         WHERE id = $1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| "Could not read job after claim conflict".to_string())?;

    match current {
        Some((status, _)) if matches!(status.as_str(), "completed" | "failed" | "cancelled") => {
            Ok(Err(ProcessOutcome::AlreadyTerminal))
        }
        Some((status, Some(true))) if status == "running" => Ok(Err(ProcessOutcome::LeaseHeld)),
        Some((status, _)) => Err(format!("Job could not be claimed from status: {status}")),
        None => Err("Job was not found".to_string()),
    }
}

pub async fn process_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: &str,
) -> Result<ProcessOutcome, String> {
    let storage = LocalBlobStorage::new(std::env::temp_dir().join("rustflow-legacy-storage"));
    process_job_with_storage(pool, &storage, job_id, worker_id).await
}

pub async fn process_job_with_storage(
    pool: &PgPool,
    storage: &dyn BlobStorage,
    job_id: Uuid,
    worker_id: &str,
) -> Result<ProcessOutcome, String> {
    let (dataset_id, sql) = match claim_job(pool, job_id, worker_id).await? {
        Ok(job) => job,
        Err(outcome) => return Ok(outcome),
    };

    tracing::info!(%job_id, %worker_id, "Worker claimed job");

    let dataset: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT storage_key, storage_path FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .fetch_optional(pool)
            .await
            .map_err(|_| "Could not get dataset storage metadata".to_string())?;
    let (storage_key, storage_path) = match dataset {
        Some(dataset) => dataset,
        None => {
            fail_job(pool, job_id, worker_id, "Dataset was not found".to_string()).await?;
            return Ok(ProcessOutcome::Failed);
        }
    };

    let batches =
        match execute_stored_query(storage, storage_key.as_deref(), &storage_path, &sql).await {
            Ok(batches) => batches,
            Err(error) => {
                fail_job(
                    pool,
                    job_id,
                    worker_id,
                    format!("Could not execute job query: {error}"),
                )
                .await?;
                return Ok(ProcessOutcome::Failed);
            }
        };
    let rows = match record_batches_to_json(&batches) {
        Ok(rows) => rows,
        Err(error) => {
            fail_job(
                pool,
                job_id,
                worker_id,
                format!("Could not serialize job result: {error}"),
            )
            .await?;
            return Ok(ProcessOutcome::Failed);
        }
    };
    let result = serde_json::json!({
        "row_count": rows.len(),
        "rows": rows,
    });
    let result_text = result.to_string();
    let result_key = job_result_key(job_id);
    if let Err(error) = put_or_verify(storage, &result_key, result_text.as_bytes()).await {
        fail_job(
            pool,
            job_id,
            worker_id,
            format!("Could not store job result object: {error}"),
        )
        .await?;
        return Ok(ProcessOutcome::Failed);
    }

    let completed = sqlx::query(
        "UPDATE jobs
         SET status = 'completed', result = $3::jsonb, result_key = $4, completed_at = NOW(),
             worker_id = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND worker_id = $2",
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(result_text)
    .bind(result_key)
    .execute(pool)
    .await
    .map_err(|_| "Could not save job result".to_string())?;

    if completed.rows_affected() == 0 {
        let current_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_optional(pool)
                .await
                .map_err(|_| "Could not read job status after claim conflict".to_string())?;

        return match current_status.as_deref() {
            Some("completed") | Some("failed") | Some("cancelled") => {
                Ok(ProcessOutcome::AlreadyTerminal)
            }
            Some(status) => Err(format!("Job cannot be completed from status: {status}")),
            None => Err("Job was not found after claim conflict".to_string()),
        };
    }

    tracing::info!(%job_id, %worker_id, "Worker completed job");
    Ok(ProcessOutcome::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db_pool() -> PgPool {
        dotenvy::from_filename(".env.test").ok();

        crate::db::connect_database()
            .await
            .expect("database connection failed")
    }

    async fn create_dataset_fixture(pool: &PgPool) -> (Uuid, String) {
        let dataset_id = Uuid::new_v4();
        let filename = format!("runner-test-{}.csv", Uuid::new_v4());
        let storage_path = format!("data/datasets/{filename}");

        tokio::fs::create_dir_all("data/datasets")
            .await
            .expect("could not create dataset directory");
        tokio::fs::write(&storage_path, "id,name\n1,Alice\n")
            .await
            .expect("could not create dataset file");

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind(&filename)
        .bind(&storage_path)
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(pool)
        .await
        .expect("could not create dataset fixture");

        (dataset_id, storage_path)
    }

    #[tokio::test]
    async fn process_job_moves_queued_job_to_completed() {
        let pool = test_db_pool().await;
        let (dataset_id, storage_path) = create_dataset_fixture(&pool).await;
        let job_id = crate::jobs::JobStatus::create_job(&pool, dataset_id, "SELECT * FROM events")
            .await
            .expect("could not create job fixture");

        process_job(&pool, job_id, "worker-a")
            .await
            .expect("could not process queued job");

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("could not read job status");

        assert_eq!(status, "completed");

        let result_text: String = sqlx::query_scalar("SELECT result::text FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .expect("could not read job result");
        let result: serde_json::Value =
            serde_json::from_str(&result_text).expect("job result was not valid JSON");

        assert_eq!(result["row_count"], 1);
        assert_eq!(result["rows"][0]["name"], "Alice");

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn process_job_rejects_missing_job() {
        let pool = test_db_pool().await;
        let error = process_job(&pool, Uuid::new_v4(), "worker-a")
            .await
            .expect_err("missing job should return an error");

        assert_eq!(error, "Job was not found");
    }

    #[tokio::test]
    async fn process_job_rejects_non_queued_job() {
        let pool = test_db_pool().await;
        let (dataset_id, storage_path) = create_dataset_fixture(&pool).await;
        let job_id = crate::jobs::JobStatus::create_job(&pool, dataset_id, "SELECT * FROM events")
            .await
            .expect("could not create job fixture");

        sqlx::query("UPDATE jobs SET status = 'completed' WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not update job fixture");

        let outcome = process_job(&pool, job_id, "worker-a")
            .await
            .expect("completed job should be treated as terminal");

        assert_eq!(outcome, ProcessOutcome::AlreadyTerminal);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn active_lease_prevents_a_second_worker_from_claiming_a_job() {
        let pool = test_db_pool().await;
        let (dataset_id, storage_path) = create_dataset_fixture(&pool).await;
        let job_id = crate::jobs::JobStatus::create_job(&pool, dataset_id, "SELECT * FROM events")
            .await
            .expect("could not create job fixture");

        let first_claim = claim_job(&pool, job_id, "worker-a")
            .await
            .expect("first claim should reach the database");
        assert!(first_claim.is_ok());

        let second_claim = claim_job(&pool, job_id, "worker-b")
            .await
            .expect("second claim should reach the database");
        assert_eq!(second_claim, Err(ProcessOutcome::LeaseHeld));

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn expired_lease_can_be_reclaimed_by_another_worker() {
        let pool = test_db_pool().await;
        let (dataset_id, storage_path) = create_dataset_fixture(&pool).await;
        let job_id = crate::jobs::JobStatus::create_job(&pool, dataset_id, "SELECT * FROM events")
            .await
            .expect("could not create job fixture");

        let first_claim = claim_job(&pool, job_id, "worker-a")
            .await
            .expect("first claim should reach the database");
        assert!(first_claim.is_ok());

        sqlx::query(
            "UPDATE jobs
             SET lease_expires_at = NOW() - INTERVAL '1 second'
             WHERE id = $1",
        )
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("could not expire test lease");

        let second_claim = claim_job(&pool, job_id, "worker-b")
            .await
            .expect("second claim should reach the database");
        assert!(second_claim.is_ok());

        let worker_id: Option<String> =
            sqlx::query_scalar("SELECT worker_id FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("could not read reclaimed job");
        assert_eq!(worker_id.as_deref(), Some("worker-b"));

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn process_job_marks_invalid_sql_as_failed() {
        let pool = test_db_pool().await;
        let (dataset_id, storage_path) = create_dataset_fixture(&pool).await;
        let job_id =
            crate::jobs::JobStatus::create_job(&pool, dataset_id, "SELECT * FROM missing_table")
                .await
                .expect("could not create job fixture");

        let outcome = process_job(&pool, job_id, "worker-a")
            .await
            .expect("invalid SQL should be persisted as a failed job");

        assert_eq!(outcome, ProcessOutcome::Failed);

        let stored_job: (String, String) =
            sqlx::query_as("SELECT status, error FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("could not read failed job");

        assert_eq!(stored_job.0, "failed");
        assert!(stored_job.1.contains("Could not execute job query"));

        let retry_outcome = process_job(&pool, job_id, "worker-b")
            .await
            .expect("failed job should be treated as terminal on redelivery");
        assert_eq!(retry_outcome, ProcessOutcome::AlreadyTerminal);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }
}
