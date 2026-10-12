use crate::{
    app::AppState,
    basic::{health, hello},
    datasets::{create_dataset, list_datasets},
    jobs::{cancel_job_handler, create_job_handler, get_job_result, get_job_status},
    metrics::metrics_handler,
    observability::request_context,
    openapi::openapi,
    queries::query,
};
use axum::{
    Router, middleware,
    routing::{get, post},
};
use sqlx::PgPool;

pub fn router_init(db_pool: PgPool) -> Router {
    router_with_state(AppState::for_tests(db_pool))
}

pub fn router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/openapi.yaml", get(openapi))
        .route("/hello/{name}", get(hello))
        .route("/datasets", get(list_datasets).post(create_dataset))
        .route("/queries", post(query))
        .route("/jobs", post(create_job_handler))
        .route("/jobs/{job_id}", get(get_job_status))
        .route("/jobs/{job_id}/result", get(get_job_result))
        .route("/jobs/{job_id}/cancel", post(cancel_job_handler))
        .route("/metrics", get(metrics_handler))
        .layer(middleware::from_fn(request_context))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::*;

    async fn test_db_pool() -> PgPool {
        dotenvy::from_filename(".env.test").ok();

        crate::db::connect_database()
            .await
            .expect("database connection failed")
    }

    #[tokio::test]
    async fn health_test() {
        let db_pool = test_db_pool().await;

        let app = router_init(db_pool);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.expect("Request failed");

        assert_eq!(response.status(), StatusCode::OK)
    }

    #[tokio::test]
    async fn upload_query_and_persist_dataset() {
        let db_pool = test_db_pool().await;
        let boundary = format!("test-boundary-{}", Uuid::new_v4());
        let filename = format!("integration-{}.csv", Uuid::new_v4());
        let csv = "id,name\n1,Alice\n";
        let multipart_body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
             Content-Type: text/csv\r\n\r\n\
             {csv}\r\n\
             --{boundary}--\r\n"
        );

        let upload_request = Request::builder()
            .method("POST")
            .uri("/datasets")
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(multipart_body))
            .expect("could not build upload request");

        let state = AppState::for_tests(db_pool.clone());
        let upload_response = router_with_state(state.clone())
            .oneshot(upload_request)
            .await
            .expect("upload request failed");

        assert_eq!(upload_response.status(), StatusCode::CREATED);

        let upload_body = to_bytes(upload_response.into_body(), usize::MAX)
            .await
            .expect("could not read upload response");
        let upload_json: serde_json::Value =
            serde_json::from_slice(&upload_body).expect("upload response was not JSON");
        let dataset_id = Uuid::parse_str(
            upload_json["id"]
                .as_str()
                .expect("upload response has no dataset id"),
        )
        .expect("upload response contains an invalid dataset id");

        let stored_filename: String =
            sqlx::query_scalar("SELECT filename FROM datasets WHERE id = $1")
                .bind(dataset_id)
                .fetch_one(&db_pool)
                .await
                .expect("dataset metadata was not stored");
        assert_eq!(stored_filename, filename);

        let storage_key: Option<String> =
            sqlx::query_scalar("SELECT storage_key FROM datasets WHERE id = $1")
                .bind(dataset_id)
                .fetch_one(&db_pool)
                .await
                .expect("dataset object key was not stored");
        let storage_key = storage_key.expect("new dataset should have an object key");
        assert_eq!(
            state.storage.get(&storage_key).await.unwrap(),
            csv.as_bytes()
        );

        let query_body = serde_json::json!({
            "dataset_id": dataset_id.to_string(),
            "sql": "SELECT * FROM events"
        });
        let query_request = Request::builder()
            .method("POST")
            .uri("/queries")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(query_body.to_string()))
            .expect("could not build query request");

        let query_response = router_with_state(state.clone())
            .oneshot(query_request)
            .await
            .expect("query request failed");

        assert_eq!(query_response.status(), StatusCode::OK);
        let query_body = to_bytes(query_response.into_body(), usize::MAX)
            .await
            .expect("could not read query response");
        let query_json: serde_json::Value =
            serde_json::from_slice(&query_body).expect("query response was not JSON");
        assert_eq!(query_json["dataset_id"], dataset_id.to_string());
        assert_eq!(query_json["row_count"], 1);

        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up test metadata");
        state.storage.delete(&storage_key).await.unwrap();
    }

    #[tokio::test]
    async fn query_rejects_invalid_input() {
        let db_pool = test_db_pool().await;
        let request_body = serde_json::json!({
            "dataset_id": "not-a-uuid",
            "sql": "SELECT * FROM events"
        });
        let request = Request::builder()
            .method("POST")
            .uri("/queries")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(request_body.to_string()))
            .expect("could not build invalid query request");

        let response = router_init(db_pool)
            .oneshot(request)
            .await
            .expect("invalid query request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_job_queues_dataset_query() {
        let db_pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();
        let filename = format!("integration-job-{}.csv", Uuid::new_v4());
        let storage_path = format!("data/datasets/{filename}");
        let csv = b"id,name\n1,Alice\n";
        let query_text = "SELECT name, COUNT(*) AS requests FROM events GROUP BY name";
        let idempotency_key = format!("router-job-{dataset_id}");

        tokio::fs::create_dir_all("data/datasets")
            .await
            .expect("could not create dataset directory");
        tokio::fs::write(&storage_path, csv)
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
        .bind(csv.len() as i64)
        .bind("stored")
        .execute(&db_pool)
        .await
        .expect("could not create dataset fixture");

        let request_body = serde_json::json!({
            "dataset_id": dataset_id.to_string(),
            "sql": query_text
        });
        let request = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header(CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", &idempotency_key)
            .body(Body::from(request_body.to_string()))
            .expect("could not build job request");

        let response = router_init(db_pool.clone())
            .oneshot(request)
            .await
            .expect("job request failed");

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response_body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("could not read job response");
        let response_json: serde_json::Value =
            serde_json::from_slice(&response_body).expect("job response was not JSON");
        let job_id = Uuid::parse_str(
            response_json["job_id"]
                .as_str()
                .expect("job response has no job id"),
        )
        .expect("job response contains an invalid job id");

        assert_ne!(job_id, dataset_id);
        assert_eq!(response_json["status"], "queued");

        let replay_request = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header(CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", &idempotency_key)
            .body(Body::from(request_body.to_string()))
            .expect("could not build replay job request");
        let replay_response = router_init(db_pool.clone())
            .oneshot(replay_request)
            .await
            .expect("replay job request failed");
        assert_eq!(replay_response.status(), StatusCode::OK);
        let replay_body = to_bytes(replay_response.into_body(), usize::MAX)
            .await
            .expect("could not read replay job response");
        let replay_json: serde_json::Value =
            serde_json::from_slice(&replay_body).expect("replay response was not JSON");
        assert_eq!(replay_json["job_id"], job_id.to_string());

        let stored_job: (Uuid, String, String) =
            sqlx::query_as("SELECT dataset_id, sql, status FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&db_pool)
                .await
                .expect("created job was not stored");

        assert_eq!(stored_job.0, dataset_id);
        assert_eq!(stored_job.1, query_text);
        assert_eq!(stored_job.2, "queued");
        let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE dataset_id = $1")
            .bind(dataset_id)
            .fetch_one(&db_pool)
            .await
            .expect("could not count idempotent jobs");
        assert_eq!(job_count, 1);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn get_job_status_returns_existing_job() {
        let db_pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();
        let filename = format!("integration-status-{}.csv", Uuid::new_v4());
        let storage_path = format!("data/datasets/{filename}");
        let csv = b"id,name\n1,Alice\n";

        tokio::fs::create_dir_all("data/datasets")
            .await
            .expect("could not create dataset directory");
        tokio::fs::write(&storage_path, csv)
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
        .bind(csv.len() as i64)
        .bind("stored")
        .execute(&db_pool)
        .await
        .expect("could not create dataset fixture");

        let job_id =
            crate::jobs::JobStatus::create_job(&db_pool, dataset_id, "SELECT * FROM events")
                .await
                .expect("could not create job fixture");

        let request = Request::builder()
            .method("GET")
            .uri(format!("/jobs/{job_id}"))
            .body(Body::empty())
            .expect("could not build get job request");

        let response = router_init(db_pool.clone())
            .oneshot(request)
            .await
            .expect("get job request failed");

        assert_eq!(response.status(), StatusCode::OK);

        let response_body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("could not read get job response");
        let response_json: serde_json::Value =
            serde_json::from_slice(&response_body).expect("get job response was not JSON");

        assert_eq!(response_json["job_id"], job_id.to_string());
        assert_eq!(response_json["status"], "queued");

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up dataset fixture");
        tokio::fs::remove_file(storage_path)
            .await
            .expect("could not clean up dataset file");
    }

    #[tokio::test]
    async fn get_job_status_returns_not_found_for_missing_job() {
        let db_pool = test_db_pool().await;
        let missing_job_id = Uuid::new_v4();

        let request = Request::builder()
            .method("GET")
            .uri(format!("/jobs/{missing_job_id}"))
            .body(Body::empty())
            .expect("could not build missing job request");

        let response = router_init(db_pool)
            .oneshot(request)
            .await
            .expect("missing job request failed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_job_result_returns_completed_json() {
        let db_pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind("result-test.csv")
        .bind("data/datasets/result-test.csv")
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(&db_pool)
        .await
        .expect("could not create dataset fixture");

        let job_id =
            crate::jobs::JobStatus::create_job(&db_pool, dataset_id, "SELECT * FROM events")
                .await
                .expect("could not create job fixture");
        sqlx::query(
            "UPDATE jobs
             SET status = 'completed', result = $2::jsonb, completed_at = NOW()
             WHERE id = $1",
        )
        .bind(job_id)
        .bind(r#"{"row_count":1,"rows":[{"name":"Alice"}]}"#)
        .execute(&db_pool)
        .await
        .expect("could not complete job fixture");

        let request = Request::builder()
            .method("GET")
            .uri(format!("/jobs/{job_id}/result"))
            .body(Body::empty())
            .expect("could not build result request");
        let response = router_init(db_pool.clone())
            .oneshot(request)
            .await
            .expect("result request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("could not read result response");
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("result response was not JSON");
        assert_eq!(json["job_id"], job_id.to_string());
        assert_eq!(json["result"]["row_count"], 1);
        assert_eq!(json["result"]["rows"][0]["name"], "Alice");

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up dataset fixture");
    }

    #[tokio::test]
    async fn get_job_result_rejects_queued_job() {
        let db_pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind("queued-result-test.csv")
        .bind("data/datasets/queued-result-test.csv")
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(&db_pool)
        .await
        .expect("could not create dataset fixture");

        let job_id =
            crate::jobs::JobStatus::create_job(&db_pool, dataset_id, "SELECT * FROM events")
                .await
                .expect("could not create job fixture");
        let request = Request::builder()
            .method("GET")
            .uri(format!("/jobs/{job_id}/result"))
            .body(Body::empty())
            .expect("could not build queued result request");
        let response = router_init(db_pool.clone())
            .oneshot(request)
            .await
            .expect("queued result request failed");

        assert_eq!(response.status(), StatusCode::CONFLICT);

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up dataset fixture");
    }

    #[tokio::test]
    async fn cancel_job_changes_queued_status() {
        let db_pool = test_db_pool().await;
        let dataset_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO datasets
             (id, filename, storage_path, format, size_bytes, status)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dataset_id)
        .bind("cancel-test.csv")
        .bind("data/datasets/cancel-test.csv")
        .bind("csv")
        .bind(0_i64)
        .bind("stored")
        .execute(&db_pool)
        .await
        .expect("could not create dataset fixture");

        let job_id =
            crate::jobs::JobStatus::create_job(&db_pool, dataset_id, "SELECT * FROM events")
                .await
                .expect("could not create job fixture");
        let request = Request::builder()
            .method("POST")
            .uri(format!("/jobs/{job_id}/cancel"))
            .body(Body::empty())
            .expect("could not build cancel request");
        let response = router_init(db_pool.clone())
            .oneshot(request)
            .await
            .expect("cancel request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&db_pool)
            .await
            .expect("could not read cancelled job");
        assert_eq!(status, "cancelled");

        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up job fixture");
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&db_pool)
            .await
            .expect("could not clean up dataset fixture");
    }
}
