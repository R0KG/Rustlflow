use rustflow::{
    db::connect_database,
    nats::connect_jetstream_from_env,
    observability::init_observability,
    storage::storage_from_env,
    worker::{run_worker, worker_id_from_env},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    init_observability("rustflow-worker")?;

    let pool = connect_database().await?;
    let context = connect_jetstream_from_env().await?;
    let storage = storage_from_env()?;
    run_worker(pool, context, storage, worker_id_from_env()).await?;
    Ok(())
}
