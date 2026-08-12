use rustflow::{
    app::run_api,
    db::connect_database,
    nats::{JobPublisher, connect_jetstream_from_env},
    observability::init_observability,
    storage::storage_from_env,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    init_observability("rustflow-api")?;

    let db_pool = connect_database().await?;
    let jetstream = connect_jetstream_from_env().await?;
    let storage = storage_from_env()?;
    run_api(
        db_pool,
        JobPublisher::JetStream(Box::new(jetstream)),
        storage,
    )
    .await
}
