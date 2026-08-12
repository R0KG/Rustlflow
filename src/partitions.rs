use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    partitioning::split_csv,
    storage::{BlobStorage, put_or_verify, read_blob_or_legacy},
};

pub const DEFAULT_ROWS_PER_PARTITION: usize = 10_000;

fn rows_per_partition_from_env() -> usize {
    std::env::var("RUSTFLOW_ROWS_PER_PARTITION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(DEFAULT_ROWS_PER_PARTITION)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetPartition {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub partition_number: i32,
    pub storage_key: Option<String>,
    pub storage_path: String,
    pub row_count: i64,
}

pub async fn ensure_dataset_partitions(
    pool: &PgPool,
    storage: &dyn BlobStorage,
    dataset_id: Uuid,
    storage_key: Option<&str>,
    legacy_storage_path: &str,
) -> Result<Vec<DatasetPartition>, String> {
    ensure_dataset_partitions_with_size(
        pool,
        storage,
        dataset_id,
        storage_key,
        legacy_storage_path,
        rows_per_partition_from_env(),
    )
    .await
}

async fn ensure_dataset_partitions_with_size(
    pool: &PgPool,
    storage: &dyn BlobStorage,
    dataset_id: Uuid,
    storage_key: Option<&str>,
    legacy_storage_path: &str,
    rows_per_partition: usize,
) -> Result<Vec<DatasetPartition>, String> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| "Could not start partition transaction".to_string())?;

    let dataset_exists: Option<Uuid> = sqlx::query_scalar(
        "SELECT id
         FROM datasets
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(dataset_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| "Could not lock dataset for partitioning".to_string())?;
    if dataset_exists.is_none() {
        return Err("Dataset was not found".to_string());
    }

    let existing = read_dataset_partitions(&mut transaction, dataset_id).await?;
    if !existing.is_empty() {
        transaction
            .commit()
            .await
            .map_err(|_| "Could not finish partition transaction".to_string())?;
        return Ok(existing);
    }

    let source_bytes = read_blob_or_legacy(storage, storage_key, legacy_storage_path)
        .await
        .map_err(|_| "Could not read dataset for partitioning".to_string())?;
    let files = split_csv(dataset_id, source_bytes, rows_per_partition).await?;
    if files.is_empty() {
        return Err("Dataset has no data rows to partition".to_string());
    }

    let mut partitions = Vec::with_capacity(files.len());
    for file in files {
        put_or_verify(storage, &file.storage_key, &file.bytes)
            .await
            .map_err(|_| "Could not save CSV partition object".to_string())?;
        let partition = DatasetPartition {
            id: Uuid::new_v4(),
            dataset_id,
            partition_number: file.partition_number,
            storage_key: Some(file.storage_key.clone()),
            storage_path: file.storage_key,
            row_count: file.row_count,
        };

        sqlx::query(
            "INSERT INTO dataset_partitions
             (id, dataset_id, partition_number, storage_path, storage_key, row_count)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(partition.id)
        .bind(partition.dataset_id)
        .bind(partition.partition_number)
        .bind(&partition.storage_path)
        .bind(&partition.storage_key)
        .bind(partition.row_count)
        .execute(&mut *transaction)
        .await
        .map_err(|_| "Could not save dataset partition metadata".to_string())?;

        partitions.push(partition);
    }

    transaction
        .commit()
        .await
        .map_err(|_| "Could not save dataset partitions".to_string())?;

    Ok(partitions)
}

async fn read_dataset_partitions(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset_id: Uuid,
) -> Result<Vec<DatasetPartition>, String> {
    let rows: Vec<(Uuid, Uuid, i32, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT id, dataset_id, partition_number, storage_key, storage_path, row_count
         FROM dataset_partitions
         WHERE dataset_id = $1
         ORDER BY partition_number",
    )
    .bind(dataset_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| "Could not read dataset partitions".to_string())?;

    Ok(rows
        .into_iter()
        .map(
            |(id, dataset_id, partition_number, storage_key, storage_path, row_count)| {
                DatasetPartition {
                    id,
                    dataset_id,
                    partition_number,
                    storage_key,
                    storage_path,
                    row_count,
                }
            },
        )
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalBlobStorage;

    async fn test_db_pool() -> PgPool {
        dotenvy::from_filename(".env.test").ok();
        crate::db::connect_database()
            .await
            .expect("database connection failed")
    }

    #[tokio::test]
    async fn partition_metadata_is_saved_once_and_reused() {
        let pool = test_db_pool().await;
        let root = std::env::temp_dir().join(format!("rustflow-partitions-{}", Uuid::new_v4()));
        let storage = LocalBlobStorage::new(&root);
        let dataset_id = Uuid::new_v4();
        let source_key = format!("datasets/{dataset_id}/source.csv");
        let bytes = b"id,name\n1,Alice\n2,Bob\n3,Carol\n";
        storage.put(&source_key, bytes).await.unwrap();

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, storage_key, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(dataset_id)
        .bind("source.csv")
        .bind(&source_key)
        .bind(&source_key)
        .bind("csv")
        .bind(bytes.len() as i64)
        .bind("stored")
        .execute(&pool)
        .await
        .expect("could not create dataset fixture");

        let first = ensure_dataset_partitions_with_size(
            &pool,
            &storage,
            dataset_id,
            Some(&source_key),
            &source_key,
            2,
        )
        .await
        .expect("could not save partition metadata");
        let second = ensure_dataset_partitions_with_size(
            &pool,
            &storage,
            dataset_id,
            Some(&source_key),
            &source_key,
            2,
        )
        .await
        .expect("could not reuse partition metadata");

        assert_eq!(first.len(), 2);
        assert_eq!(first, second);
        assert_eq!(first[0].row_count, 2);
        assert_eq!(first[1].row_count, 1);
        assert!(
            first
                .iter()
                .all(|partition| partition.storage_key.is_some())
        );

        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
