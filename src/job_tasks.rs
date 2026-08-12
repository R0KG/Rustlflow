use sqlx::PgPool;
use uuid::Uuid;

use crate::partitions::DatasetPartition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTask {
    pub id: Uuid,
    pub job_id: Uuid,
    pub dataset_partition_id: Uuid,
    pub status: String,
}

pub async fn create_job_tasks(
    pool: &PgPool,
    job_id: Uuid,
    partitions: &[DatasetPartition],
) -> Result<Vec<JobTask>, String> {
    if partitions.is_empty() {
        return Err("Cannot create tasks without dataset partitions".to_string());
    }

    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| "Could not start task creation transaction".to_string())?;

    for partition in partitions {
        sqlx::query(
            "INSERT INTO job_tasks (id, job_id, dataset_partition_id, status)
             VALUES ($1, $2, $3, 'queued')
             ON CONFLICT (job_id, dataset_partition_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(job_id)
        .bind(partition.id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| "Could not create job task".to_string())?;
    }

    let rows: Vec<(Uuid, Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT task.id, task.job_id, task.dataset_partition_id, task.status
         FROM job_tasks AS task
         INNER JOIN dataset_partitions AS partition
             ON partition.id = task.dataset_partition_id
         WHERE task.job_id = $1
         ORDER BY partition.partition_number",
    )
    .bind(job_id)
    .fetch_all(&mut *transaction)
    .await
    .map_err(|_| "Could not read created job tasks".to_string())?;

    if rows.len() != partitions.len() {
        return Err("Job has an incomplete task set".to_string());
    }

    transaction
        .commit()
        .await
        .map_err(|_| "Could not save job tasks".to_string())?;

    Ok(rows
        .into_iter()
        .map(|(id, job_id, dataset_partition_id, status)| JobTask {
            id,
            job_id,
            dataset_partition_id,
            status,
        })
        .collect())
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

    #[tokio::test]
    async fn task_creation_is_idempotent_per_job_partition_pair() {
        let pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let partitions = vec![
            DatasetPartition {
                id: Uuid::new_v4(),
                dataset_id,
                partition_number: 0,
                storage_key: None,
                storage_path: "data/datasets/part-0.csv".to_string(),
                row_count: 2,
            },
            DatasetPartition {
                id: Uuid::new_v4(),
                dataset_id,
                partition_number: 1,
                storage_key: None,
                storage_path: "data/datasets/part-1.csv".to_string(),
                row_count: 1,
            },
        ];

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind("task-test.csv")
        .bind("data/datasets/task-test.csv")
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(&pool)
        .await
        .expect("could not create dataset fixture");

        for partition in &partitions {
            sqlx::query(
                "INSERT INTO dataset_partitions
                 (id, dataset_id, partition_number, storage_path, row_count)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(partition.id)
            .bind(partition.dataset_id)
            .bind(partition.partition_number)
            .bind(&partition.storage_path)
            .bind(partition.row_count)
            .execute(&pool)
            .await
            .expect("could not create partition fixture");
        }

        sqlx::query("INSERT INTO jobs (id, dataset_id, sql, status) VALUES ($1, $2, $3, 'queued')")
            .bind(job_id)
            .bind(dataset_id)
            .bind("SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint")
            .execute(&pool)
            .await
            .expect("could not create job fixture");

        let first = create_job_tasks(&pool, job_id, &partitions)
            .await
            .expect("could not create job tasks");
        let second = create_job_tasks(&pool, job_id, &partitions)
            .await
            .expect("could not reuse job tasks");

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|task| task.status == "queued"));

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
    }
}
