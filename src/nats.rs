use async_nats::jetstream::{
    self, Context,
    stream::{Config, RetentionPolicy, StorageType},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::observability::current_traceparent;

pub const JOBS_STREAM: &str = "RUSTFLOW_JOBS";
pub const JOB_EXECUTE_SUBJECT: &str = "jobs.execute";
pub const TASK_EXECUTE_SUBJECT: &str = "tasks.execute";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobExecuteMessage {
    pub job_id: Uuid,
    pub traceparent: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TaskExecuteMessage {
    pub task_id: Uuid,
    pub traceparent: Option<String>,
}

#[derive(Clone)]
pub enum JobPublisher {
    JetStream(Box<Context>),
    Disabled,
}

impl JobPublisher {
    pub async fn publish(&self, job_id: Uuid) -> Result<(), String> {
        match self {
            Self::JetStream(context) => {
                let payload = serde_json::to_vec(&JobExecuteMessage {
                    job_id,
                    traceparent: current_traceparent(),
                })
                .map_err(|error| format!("Could not encode job message: {error}"))?;
                let publish_ack = context
                    .publish(JOB_EXECUTE_SUBJECT, payload.into())
                    .await
                    .map_err(|error| format!("Could not publish job message: {error}"))?;
                publish_ack.await.map_err(|error| {
                    format!("JetStream did not acknowledge job message: {error}")
                })?;
                Ok(())
            }
            Self::Disabled => Ok(()),
        }
    }

    pub async fn publish_tasks(&self, task_ids: &[Uuid]) -> Result<(), String> {
        match self {
            Self::JetStream(context) => {
                for task_id in task_ids {
                    let payload = serde_json::to_vec(&TaskExecuteMessage {
                        task_id: *task_id,
                        traceparent: current_traceparent(),
                    })
                    .map_err(|error| format!("Could not encode task message: {error}"))?;
                    let publish_ack = context
                        .publish(TASK_EXECUTE_SUBJECT, payload.into())
                        .await
                        .map_err(|error| format!("Could not publish task message: {error}"))?;
                    publish_ack.await.map_err(|error| {
                        format!("JetStream did not acknowledge task message: {error}")
                    })?;
                }
                Ok(())
            }
            Self::Disabled => Ok(()),
        }
    }
}

pub async fn connect_jetstream_from_env() -> Result<Context, String> {
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = async_nats::connect(&nats_url)
        .await
        .map_err(|error| format!("Could not connect to NATS at {nats_url}: {error}"))?;
    let context = jetstream::new(client);

    ensure_jobs_stream(&context).await?;
    Ok(context)
}

pub async fn ensure_jobs_stream(context: &Context) -> Result<(), String> {
    context
        .create_or_update_stream(Config {
            name: JOBS_STREAM.to_string(),
            subjects: vec![
                JOB_EXECUTE_SUBJECT.to_string(),
                TASK_EXECUTE_SUBJECT.to_string(),
            ],
            retention: RetentionPolicy::WorkQueue,
            storage: StorageType::File,
            max_messages: 10_000,
            ..Default::default()
        })
        .await
        .map_err(|error| format!("Could not create JetStream jobs stream: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_message_round_trips_as_json() {
        let task_id = Uuid::new_v4();
        let encoded = serde_json::to_vec(&TaskExecuteMessage {
            task_id,
            traceparent: None,
        })
        .expect("could not encode task message");
        let decoded: TaskExecuteMessage =
            serde_json::from_slice(&encoded).expect("could not decode task message");

        assert_eq!(decoded.task_id, task_id);
    }
}
