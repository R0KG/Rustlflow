use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn connect_database() -> Result<PgPool, sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|error| sqlx::Error::Configuration(Box::new(error)))?;

    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
}
