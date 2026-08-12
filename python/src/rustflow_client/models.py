from typing import Any
from uuid import UUID

from pydantic import BaseModel


class DatasetResponse(BaseModel):
    id: UUID
    filename: str
    format: str
    status: str


class JobResponse(BaseModel):
    job_id: UUID
    status: str


class JobStatusResponse(BaseModel):
    job_id: UUID
    status: str


class JobResultResponse(BaseModel):
    job_id: UUID
    result: dict[str, Any]


class ErrorResponse(BaseModel):
    code: str
    message: str
