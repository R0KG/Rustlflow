use axum::{
    Json,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header::HeaderName},
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    app::AppState,
    distributed_query::parse_grouped_count,
    errors::{AppError, ErrorResponse},
    job_tasks::create_job_tasks,
    models::{CreateJobRequest, CreateJobResponse, JobResultResponse, JobStatusResponse},
    partitions::ensure_dataset_partitions,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobCreation {
    Created(Uuid),
    Reused(Uuid),
}

const IDEMPOTENCY_KEY_HEADER: HeaderName = HeaderName::from_static("idempotency-key");
type StoredJob = (String, Option<String>, Option<String>, Option<String>);

impl JobStatus {
    pub fn can_transition_to(self, next: JobStatus) -> bool {
        matches!(
            (self, next),
            (JobStatus::Queued, JobStatus::Running)
                | (JobStatus::Queued, JobStatus::Cancelled)
                | (JobStatus::Running, JobStatus::Completed)
                | (JobStatus::Running, JobStatus::Failed)
                | (JobStatus::Running, JobStatus::Cancelled)
        )
    }
    pub async fn create_job(
        pool: &PgPool,
        dataset_id: Uuid,
        sql: &str,
    ) -> Result<Uuid, sqlx::Error> {
        let job_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO jobs (id, dataset_id, sql, status)
VALUES ($1, $2, $3, $4)",
        )
        .bind(job_id)
        .bind(dataset_id)
        .bind(sql)
        .bind("queued")
        .execute(pool)
        .await?;
        Ok(job_id)
    }

    async fn create_or_reuse_job(
        pool: &PgPool,
        dataset_id: Uuid,
        sql: &str,
        idempotency_key: Option<&str>,
    ) -> Result<JobCreation, sqlx::Error> {
        let job_id = Uuid::new_v4();

        let inserted = match idempotency_key {
            Some(key) => {
                sqlx::query(
                    "INSERT INTO jobs (id, dataset_id, sql, status, idempotency_key)
                 VALUES ($1, $2, $3, 'queued', $4)
                 ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING",
                )
                .bind(job_id)
                .bind(dataset_id)
                .bind(sql)
                .bind(key)
                .execute(pool)
                .await?
            }
            None => {
                sqlx::query(
                    "INSERT INTO jobs (id, dataset_id, sql, status)
                 VALUES ($1, $2, $3, 'queued')",
                )
                .bind(job_id)
                .bind(dataset_id)
                .bind(sql)
                .execute(pool)
                .await?
            }
        };

        if inserted.rows_affected() == 1 {
            return Ok(JobCreation::Created(job_id));
        }

        let key = idempotency_key.expect("only an idempotent insert can conflict");
        let existing: (Uuid, Uuid, String) =
            sqlx::query_as("SELECT id, dataset_id, sql FROM jobs WHERE idempotency_key = $1")
                .bind(key)
                .fetch_one(pool)
                .await?;
        if existing.1 != dataset_id || existing.2 != sql {
            return Err(sqlx::Error::Protocol(
                "Idempotency key was already used for another job request".to_string(),
            ));
        }
        Ok(JobCreation::Reused(existing.0))
    }
}

#[utoipa::path(
    post,
    path = "/jobs",
    request_body = CreateJobRequest,
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional client key for safely retrying the same request")
    ),
    responses(
        (status = 200, description = "Existing job returned for an idempotent replay", body = CreateJobResponse),
        (status = 202, description = "Job accepted", body = CreateJobResponse),
        (status = 400, description = "Invalid dataset ID, SQL, or idempotency key", body = ErrorResponse),
        (status = 404, description = "Dataset not found", body = ErrorResponse),
        (status = 409, description = "Idempotency key conflicts with another request", body = ErrorResponse),
        (status = 500, description = "Job creation failed", body = ErrorResponse),
        (status = 503, description = "Task queue is unavailable", body = ErrorResponse)
    ),
    tag = "jobs"
)]
pub async fn create_job_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateJobRequest>,
) -> Result<(StatusCode, Json<CreateJobResponse>), AppError> {
    if payload.sql.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Problem with sql command is either empty or incorrect".to_string(),
        ));
    }
    parse_grouped_count(&payload.sql).map_err(AppError::BadRequest)?;

    let dataset_id = Uuid::parse_str(&payload.dataset_id)
        .map_err(|_| AppError::BadRequest("Error with dataset id".to_string()))?;

    let idempotency_key = headers
        .get(&IDEMPOTENCY_KEY_HEADER)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| {
            AppError::BadRequest("Idempotency-Key must contain visible ASCII text".to_string())
        })?;
    if let Some(key) = idempotency_key
        && (key.is_empty() || key.len() > 255)
    {
        return Err(AppError::BadRequest(
            "Idempotency-Key must be between 1 and 255 bytes".to_string(),
        ));
    }

    let dataset: Option<(Option<String>, String)> =
        sqlx::query_as("SELECT storage_key, storage_path FROM datasets WHERE id =$1")
            .bind(dataset_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| {
                AppError::Internal("Could not get dataset storage metadata".to_string())
            })?;
    let (storage_key, storage_path) =
        dataset.ok_or(AppError::NotFound("Dataset was not found".to_string()))?;
    let partitions = ensure_dataset_partitions(
        &state.pool,
        state.storage.as_ref(),
        dataset_id,
        storage_key.as_deref(),
        &storage_path,
    )
    .await
    .map_err(AppError::Internal)?;
    let creation =
        JobStatus::create_or_reuse_job(&state.pool, dataset_id, &payload.sql, idempotency_key)
            .await
            .map_err(|error| {
                if error.to_string().contains("Idempotency key") {
                    AppError::Conflict(error.to_string())
                } else {
                    AppError::Internal("Could not create job".to_string())
                }
            })?;
    let job_id = match creation {
        JobCreation::Created(job_id) => job_id,
        JobCreation::Reused(job_id) => {
            tracing::info!(%job_id, "Reused idempotent job submission");
            metrics::counter!("rustflow_jobs_total", "outcome" => "idempotent_replay").increment(1);
            return Ok((
                StatusCode::OK,
                Json(CreateJobResponse {
                    job_id: job_id.to_string(),
                    status: "queued".to_string(),
                }),
            ));
        }
    };
    let task_ids = create_job_tasks(&state.pool, job_id, &partitions)
        .await
        .map_err(AppError::Internal)?;

    tracing::info!(job_id = %job_id, task_count = task_ids.len(), "Created queued job tasks");
    metrics::counter!("rustflow_jobs_total", "outcome" => "created").increment(1);
    metrics::counter!("rustflow_tasks_total", "outcome" => "queued")
        .increment(task_ids.len() as u64);

    let task_ids = task_ids.iter().map(|task| task.id).collect::<Vec<_>>();
    if let Err(error) = state.job_publisher.publish_tasks(&task_ids).await {
        tracing::error!(job_id = %job_id, %error, "Could not dispatch job to JetStream");
        sqlx::query(
            "UPDATE jobs
             SET status = 'failed', error = $2, completed_at = NOW()
             WHERE id = $1 AND status = 'queued'",
        )
        .bind(job_id)
        .bind("Could not dispatch job to worker queue")
        .execute(&state.pool)
        .await
        .map_err(|_| AppError::Internal("Could not record job dispatch failure".to_string()))?;
        sqlx::query("UPDATE job_tasks SET status = 'cancelled', completed_at = NOW() WHERE job_id = $1 AND status = 'queued'")
            .bind(job_id).execute(&state.pool).await.map_err(|_| AppError::Internal("Could not cancel undispatched tasks".to_string()))?;
        return Err(AppError::ServiceUnavailable(
            "Job queue is unavailable".to_string(),
        ));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CreateJobResponse {
            job_id: job_id.to_string(),
            status: "queued".to_string(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}",
    params(("job_id" = String, Path, description = "Job UUID")),
    responses(
        (status = 200, description = "Current job state", body = JobStatusResponse),
        (status = 400, description = "Invalid job ID", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 500, description = "Job lookup failed", body = ErrorResponse)
    ),
    tag = "jobs"
)]
pub async fn get_job_status(
    State(state): State<AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, AppError> {
    let job_id = Uuid::parse_str(&job_id)
        .map_err(|_| AppError::BadRequest("Error with job id".to_string()))?;

    let job_status: Option<String> = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| AppError::Internal("Could not get job status".to_string()))?;

    let status = job_status.ok_or(AppError::NotFound("Job was not found".to_string()))?;

    Ok(Json(JobStatusResponse {
        job_id: job_id.to_string(),
        status,
    }))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/result",
    params(("job_id" = String, Path, description = "Job UUID")),
    responses(
        (status = 200, description = "Completed job result", body = JobResultResponse),
        (status = 400, description = "Invalid job ID", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job is not completed", body = ErrorResponse),
        (status = 500, description = "Stored result could not be read", body = ErrorResponse)
    ),
    tag = "jobs"
)]
pub async fn get_job_result(
    State(state): State<AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobResultResponse>, AppError> {
    let job_id = Uuid::parse_str(&job_id)
        .map_err(|_| AppError::BadRequest("Error with job id".to_string()))?;

    let job: Option<StoredJob> = sqlx::query_as(
        "SELECT status, result_key, result::text, error
         FROM jobs
         WHERE id = $1",
    )
    .bind(job_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| AppError::Internal("Could not get job result".to_string()))?;

    let Some((status, result_key, result_text, error)) = job else {
        return Err(AppError::NotFound("Job was not found".to_string()));
    };

    match status.as_str() {
        "completed" => {
            let result_text = match result_key {
                Some(key) => String::from_utf8(state.storage.get(&key).await.map_err(|_| {
                    AppError::Internal("Could not read stored job result".to_string())
                })?)
                .map_err(|_| AppError::Internal("Stored job result is not UTF-8".to_string()))?,
                None => result_text.ok_or(AppError::Internal(
                    "Completed job has no stored result".to_string(),
                ))?,
            };
            let result = serde_json::from_str(&result_text).map_err(|_| {
                AppError::Internal("Stored job result is not valid JSON".to_string())
            })?;
            Ok(Json(JobResultResponse {
                job_id: job_id.to_string(),
                result,
            }))
        }
        "failed" => {
            Err(AppError::Conflict(error.unwrap_or_else(|| {
                "Job failed without an error message".to_string()
            })))
        }
        "queued" | "running" => Err(AppError::Conflict("Job result is not ready".to_string())),
        "cancelled" => Err(AppError::Conflict("Job was cancelled".to_string())),
        _ => Err(AppError::Internal("Job has an unknown status".to_string())),
    }
}

#[utoipa::path(
    post,
    path = "/jobs/{job_id}/cancel",
    params(("job_id" = String, Path, description = "Job UUID")),
    responses(
        (status = 200, description = "Job cancelled", body = JobStatusResponse),
        (status = 400, description = "Invalid job ID", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job is already terminal", body = ErrorResponse),
        (status = 500, description = "Job cancellation failed", body = ErrorResponse)
    ),
    tag = "jobs"
)]
pub async fn cancel_job_handler(
    State(state): State<AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, AppError> {
    let job_id = Uuid::parse_str(&job_id)
        .map_err(|_| AppError::BadRequest("Error with job id".to_string()))?;

    let cancelled: Option<String> = sqlx::query_scalar(
        "UPDATE jobs
         SET status = 'cancelled', completed_at = NOW(),
             worker_id = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status IN ('queued', 'running')
         RETURNING status",
    )
    .bind(job_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| AppError::Internal("Could not cancel job".to_string()))?;

    if let Some(status) = cancelled {
        return Ok(Json(JobStatusResponse {
            job_id: job_id.to_string(),
            status,
        }));
    }

    let existing_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| AppError::Internal("Could not get job status".to_string()))?;

    match existing_status {
        None => Err(AppError::NotFound("Job was not found".to_string())),
        Some(status) => Err(AppError::Conflict(format!(
            "Job cannot be cancelled from status: {status}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_can_become_running() {
        assert!(JobStatus::Queued.can_transition_to(JobStatus::Running));
    }

    #[test]
    fn queued_can_be_cancelled() {
        assert!(JobStatus::Queued.can_transition_to(JobStatus::Cancelled));
    }

    #[test]
    fn running_can_finish_or_fail() {
        assert!(JobStatus::Running.can_transition_to(JobStatus::Completed));
        assert!(JobStatus::Running.can_transition_to(JobStatus::Failed));
        assert!(JobStatus::Running.can_transition_to(JobStatus::Cancelled));
    }

    #[test]
    fn completed_is_terminal() {
        assert!(!JobStatus::Completed.can_transition_to(JobStatus::Running));
    }

    #[test]
    fn failed_is_terminal() {
        assert!(!JobStatus::Failed.can_transition_to(JobStatus::Completed));
    }

    #[test]
    fn cancelled_is_terminal() {
        assert!(!JobStatus::Cancelled.can_transition_to(JobStatus::Running));
    }

    #[tokio::test]
    async fn create_job_inserts_queued_job() {
        dotenvy::from_filename(".env.test").ok();

        let pool = crate::db::connect_database()
            .await
            .expect("database connection failed");
        let dataset_id = Uuid::new_v4();
        let query_text = "SELECT * FROM events";

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind("job-test.csv")
        .bind("data/datasets/job-test.csv")
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(&pool)
        .await
        .expect("could not create dataset fixture");

        let job_id = JobStatus::create_job(&pool, dataset_id, query_text)
            .await
            .expect("could not create job");

        let stored_job: (Uuid, String, String) =
            sqlx::query_as("SELECT dataset_id, sql, status FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("could not read created job");

        assert_eq!(stored_job.0, dataset_id);
        assert_eq!(stored_job.1, query_text);
        assert_eq!(stored_job.2, "queued");

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM jobs WHERE dataset_id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
    }

    #[tokio::test]
    async fn idempotency_key_reuses_only_the_same_job_request() {
        dotenvy::from_filename(".env.test").ok();
        let pool = crate::db::connect_database()
            .await
            .expect("database connection failed");
        let dataset_id = Uuid::new_v4();
        let key = format!("job-request-{dataset_id}");
        sqlx::query(
            "INSERT INTO datasets (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, 'idempotency.csv', 'legacy-idempotency.csv', 'csv', 0, 'stored')",
        )
        .bind(dataset_id)
        .execute(&pool)
        .await
        .expect("could not create dataset fixture");

        let first = JobStatus::create_or_reuse_job(
            &pool,
            dataset_id,
            "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint",
            Some(&key),
        )
        .await
        .expect("could not create idempotent job");
        let second = JobStatus::create_or_reuse_job(
            &pool,
            dataset_id,
            "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint",
            Some(&key),
        )
        .await
        .expect("could not reuse idempotent job");
        assert!(matches!(first, JobCreation::Created(_)));
        assert_eq!(
            match first {
                JobCreation::Created(id) => id,
                JobCreation::Reused(_) => unreachable!(),
            },
            match second {
                JobCreation::Reused(id) => id,
                JobCreation::Created(_) => unreachable!(),
            }
        );

        let conflict = JobStatus::create_or_reuse_job(
            &pool,
            dataset_id,
            "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint ORDER BY endpoint",
            Some(&key),
        )
        .await;
        assert!(conflict.is_err());

        sqlx::query("DELETE FROM jobs WHERE dataset_id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up idempotency job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
    }
}
