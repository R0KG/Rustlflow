use sqlx::PgPool;

use crate::{
    nats::JobPublisher,
    router::router_with_state,
    storage::{LocalBlobStorage, SharedBlobStorage},
};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub job_publisher: JobPublisher,
    pub storage: SharedBlobStorage,
}

impl AppState {
    pub fn new(pool: PgPool, job_publisher: JobPublisher, storage: SharedBlobStorage) -> Self {
        Self {
            pool,
            job_publisher,
            storage,
        }
    }

    pub fn for_tests(pool: PgPool) -> Self {
        Self::new(
            pool,
            JobPublisher::Disabled,
            std::sync::Arc::new(LocalBlobStorage::new("data/objects")),
        )
    }
}

pub async fn run_api(
    pool: PgPool,
    job_publisher: JobPublisher,
    storage: SharedBlobStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = router_with_state(AppState::new(pool, job_publisher, storage));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    tracing::info!(address = "0.0.0.0:3000", "RustFlow API started");
    axum::serve(listener, app).await?;
    Ok(())
}
