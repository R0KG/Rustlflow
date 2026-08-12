use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    ServiceUnavailable(String),
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            AppError::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            AppError::Conflict(message) => (StatusCode::CONFLICT, "conflict", message),
            AppError::ServiceUnavailable(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                message,
            ),
            AppError::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
            }
        };
        (status, Json(ErrorResponse { code, message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn not_found_404() {
        let response = AppError::NotFound("Dataset was not found".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    #[test]
    fn bad_request_becomes_400() {
        let response = AppError::BadRequest("Invalid request".to_string()).into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn internal_error_becomes_500() {
        let response = AppError::Internal("Database error".to_string()).into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn conflict_becomes_409() {
        let response = AppError::Conflict("Job is still running".to_string()).into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
