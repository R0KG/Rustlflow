use async_nats::jetstream::{Context, consumer::pull};
use futures_util::StreamExt;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    job_runner::{ProcessOutcome, process_job_with_storage},
    nats::{JOBS_STREAM, JobExecuteMessage, TaskExecuteMessage, ensure_jobs_stream},
    observability::extract_parent,
    reliability::{max_deliveries, retry_backoff},
    storage::SharedBlobStorage,
    task_runner::{TaskProcessOutcome, process_task},
};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const WORKER_CONSUMER: &str = "rustflow-workers";

pub async fn run_worker(
    pool: PgPool,
    context: Context,
    storage: SharedBlobStorage,
    worker_id: String,
) -> Result<(), String> {
    ensure_jobs_stream(&context).await?;
    let stream = context
        .get_stream(JOBS_STREAM)
        .await
        .map_err(|error| format!("Could not get jobs stream: {error}"))?;
    let consumer = stream
        .get_or_create_consumer(
            WORKER_CONSUMER,
            pull::Config {
                durable_name: Some(WORKER_CONSUMER.to_string()),
                max_deliver: max_deliveries(),
                backoff: retry_backoff(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| format!("Could not create jobs consumer: {error}"))?;
    let mut messages = consumer
        .messages()
        .await
        .map_err(|error| format!("Could not receive jobs: {error}"))?;

    tracing::info!(
        consumer = WORKER_CONSUMER,
        worker_id = %worker_id,
        "RustFlow worker started"
    );
    metrics::counter!("rustflow_workers_started_total").increment(1);

    while let Some(message) = messages.next().await {
        let message = message.map_err(|error| format!("Could not receive job message: {error}"))?;
        if let Ok(event) = serde_json::from_slice::<TaskExecuteMessage>(&message.payload) {
            let span = tracing::info_span!(
                "nats_task",
                task_id = %event.task_id,
                worker_id = %worker_id,
            );
            if let Some(parent) = extract_parent(event.traceparent.as_deref()) {
                let _ = span.set_parent(parent);
            }
            let outcome =
                async { process_task(&pool, storage.as_ref(), event.task_id, &worker_id).await }
                    .instrument(span)
                    .await;
            match outcome {
                Ok(
                    TaskProcessOutcome::Completed
                    | TaskProcessOutcome::Failed
                    | TaskProcessOutcome::Cancelled
                    | TaskProcessOutcome::AlreadyTerminal,
                ) => message
                    .ack()
                    .await
                    .map_err(|error| format!("Could not acknowledge task message: {error}"))?,
                Ok(TaskProcessOutcome::LeaseHeld) => {
                    tracing::info!(task_id = %event.task_id, %worker_id, "Another worker holds the active task lease; leaving message for redelivery")
                }
                Err(error) => {
                    tracing::error!(task_id = %event.task_id, %error, "Task processing failed; leaving message unacknowledged for retry");
                    metrics::counter!("rustflow_task_retries_total").increment(1);
                }
            }
            continue;
        }
        let event: JobExecuteMessage = match serde_json::from_slice(&message.payload) {
            Ok(event) => event,
            Err(error) => {
                tracing::error!(%error, "Ignoring invalid job message");
                message.ack().await.map_err(|error| {
                    format!("Could not acknowledge invalid job message: {error}")
                })?;
                continue;
            }
        };

        let span = tracing::info_span!(
            "nats_job",
            job_id = %event.job_id,
            worker_id = %worker_id,
        );
        if let Some(parent) = extract_parent(event.traceparent.as_deref()) {
            let _ = span.set_parent(parent);
        }
        let outcome = async {
            process_job_with_storage(&pool, storage.as_ref(), event.job_id, &worker_id).await
        }
        .instrument(span)
        .await;
        match outcome {
            Ok(
                ProcessOutcome::Completed
                | ProcessOutcome::Failed
                | ProcessOutcome::Cancelled
                | ProcessOutcome::AlreadyTerminal,
            ) => {
                message
                    .ack()
                    .await
                    .map_err(|error| format!("Could not acknowledge completed job: {error}"))?;
            }
            Ok(ProcessOutcome::LeaseHeld) => {
                tracing::info!(job_id = %event.job_id, %worker_id, "Another worker holds the active job lease; leaving message for redelivery");
            }
            Err(error) => {
                tracing::error!(job_id = %event.job_id, %error, "Job processing failed; leaving message unacknowledged for retry");
                metrics::counter!("rustflow_job_retries_total").increment(1);
            }
        }
    }

    Err("JetStream consumer stopped unexpectedly".to_string())
}

pub fn worker_id_from_env() -> String {
    std::env::var("WORKER_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("worker-{}", std::process::id()))
}

pub fn parse_job_id(value: &str) -> Result<Uuid, String> {
    Uuid::parse_str(value).map_err(|_| "Invalid job id in message".to_string())
}
