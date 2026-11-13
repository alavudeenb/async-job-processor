from typing import Any, Literal
from datetime import datetime

from pydantic import BaseModel, Field


class JobCreateRequest(BaseModel):
    job_type: str = Field(min_length=1, max_length=25)
    payload: dict[str, Any]

    priority: Literal["HIGH", "MEDIUM", "LOW"] = "LOW"
    max_retries: int = Field(ge=0, le=10)
    timeout_seconds: int = Field(gt=0, le=3600)


class JobCreateResponse(BaseModel):
    job_id: str
    status: str
    created_at: datetime
