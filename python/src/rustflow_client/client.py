from __future__ import annotations

import time
from pathlib import Path
from uuid import UUID, uuid4

import httpx

from .models import (
    DatasetResponse,
    ErrorResponse,
    JobResponse,
    JobResultResponse,
    JobStatusResponse,
)


class RustFlowApiError(RuntimeError):
    """An HTTP error returned by the RustFlow API."""


class RustFlowClient:
    """Small synchronous client for the public RustFlow API."""

    def __init__(self, base_url: str = "http://127.0.0.1:3000", timeout_seconds: float = 30):
        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout_seconds)

    def __enter__(self) -> RustFlowClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def health(self) -> dict[str, object]:
        response = self._client.get("/health")
        self._raise_for_error(response)
        return response.json()

    def upload_dataset(self, csv_path: Path) -> DatasetResponse:
        with csv_path.open("rb") as file:
            response = self._client.post(
                "/datasets",
                files={"file": (csv_path.name, file, "text/csv")},
            )
        self._raise_for_error(response)
        return DatasetResponse.model_validate(response.json())

    def create_job(
        self,
        dataset_id: UUID,
        sql: str,
        idempotency_key: str | None = None,
    ) -> JobResponse:
        response = self._client.post(
            "/jobs",
            headers={"Idempotency-Key": idempotency_key or str(uuid4())},
            json={"dataset_id": str(dataset_id), "sql": sql},
        )
        self._raise_for_error(response)
        return JobResponse.model_validate(response.json())

    def get_job(self, job_id: UUID) -> JobStatusResponse:
        response = self._client.get(f"/jobs/{job_id}")
        self._raise_for_error(response)
        return JobStatusResponse.model_validate(response.json())

    def get_result(self, job_id: UUID) -> JobResultResponse:
        response = self._client.get(f"/jobs/{job_id}/result")
        self._raise_for_error(response)
        return JobResultResponse.model_validate(response.json())

    def wait_for_result(
        self,
        job_id: UUID,
        timeout_seconds: float = 60,
        poll_interval_seconds: float = 0.25,
    ) -> JobResultResponse:
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            job = self.get_job(job_id)

            if job.status == "completed":
                return self.get_result(job_id)
            if job.status in {"failed", "cancelled"}:
                raise RustFlowApiError(f"Job {job_id} ended with status {job.status}: {job.error}")

            time.sleep(poll_interval_seconds)

        raise TimeoutError(f"Job {job_id} did not finish within {timeout_seconds} seconds")

    @staticmethod
    def _raise_for_error(response: httpx.Response) -> None:
        if not response.is_error:
            return

        try:
            error = ErrorResponse.model_validate(response.json())
            detail = f"{error.code}: {error.message}"
        except (ValueError, TypeError):
            detail = response.text

        raise RustFlowApiError(
            f"{response.request.method} {response.request.url.path} returned "
            f"{response.status_code}: {detail}"
        )
