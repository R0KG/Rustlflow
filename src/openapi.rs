use axum::{
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use utoipa::OpenApi;

use crate::{
    basic::{__path_health, __path_hello},
    datasets::{__path_create_dataset, __path_list_datasets},
    errors::ErrorResponse,
    jobs::{
        __path_cancel_job_handler, __path_create_job_handler, __path_get_job_result,
        __path_get_job_status,
    },
    metrics::__path_metrics_handler,
    models::{
        CreateJobRequest, CreateJobResponse, DatasetListResponse, DatasetResponse, DatasetSummary,
        HealthResponse, HelloResponse, JobResultResponse, JobStatusResponse, QueryRequest,
        QueryResponse,
    },
    queries::__path_query,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "RustFlow API",
        version = env!("CARGO_PKG_VERSION"),
        description = "CSV upload and asynchronous partitioned DataFusion query jobs."
    ),
    paths(
        health,
        hello,
        create_dataset,
        list_datasets,
        query,
        create_job_handler,
        get_job_status,
        get_job_result,
        cancel_job_handler,
        metrics_handler
    ),
    components(schemas(
        CreateJobRequest,
        CreateJobResponse,
        DatasetListResponse,
        DatasetResponse,
        DatasetSummary,
        ErrorResponse,
        HealthResponse,
        HelloResponse,
        JobResultResponse,
        JobStatusResponse,
        QueryRequest,
        QueryResponse
    )),
    tags(
        (name = "utility", description = "Service information"),
        (name = "datasets", description = "Dataset upload"),
        (name = "queries", description = "Synchronous DataFusion queries"),
        (name = "jobs", description = "Asynchronous distributed query jobs"),
        (name = "observability", description = "Metrics")
    )
)]
pub struct ApiDoc;

pub async fn openapi() -> Response {
    match ApiDoc::openapi().to_yaml() {
        Ok(document) => ([(CONTENT_TYPE, "application/yaml")], document).into_response(),
        Err(error) => {
            tracing::error!(%error, "Could not serialize generated OpenAPI document");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::to_bytes,
        http::{StatusCode, header::CONTENT_TYPE},
    };

    use super::*;

    #[tokio::test]
    async fn generated_document_contains_public_routes() {
        let response = openapi().await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/yaml"
        );
        let document = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("generated OpenAPI body should be readable")
                .to_vec(),
        )
        .expect("generated OpenAPI document should be UTF-8");

        for path in [
            "/datasets",
            "/jobs",
            "/jobs/{job_id}",
            "/jobs/{job_id}/result",
            "/jobs/{job_id}/cancel",
            "/metrics",
        ] {
            assert!(
                document.contains(path),
                "generated document is missing {path}"
            );
        }

        assert!(document.contains("Idempotency-Key"));
        assert!(document.contains("CreateJobRequest"));
    }
}
