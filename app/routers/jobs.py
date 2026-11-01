from uuid import uuid4

from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Job, OutboxEvent
from app.schemas import JobCreateRequest, JobCreateResponse
from app.config import REGISTERED_JOB_TYPES

router = APIRouter(prefix="/api/jobs")


@router.post("", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED)
def create_job(request: JobCreateRequest, db: Session = Depends(get_db)):

    if request.job_type not in REGISTERED_JOB_TYPES:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Unsupported Job Type",
                "message": f"Unsupported job_type {request.job_type}",
            },
        )

    job = Job(
        id=str(uuid4()),
        job_type=request.job_type,
        payload=request.payload,
        priority=request.priority,
        max_retries=request.max_retries,
        timeout_seconds=request.timeout_seconds,
        status="QUEUED",
    )

    outbox_event = OutboxEvent(id=str(uuid4()), job_id=job.id)

    db.add(job)
    db.add(outbox_event)
    db.commit()
    db.refresh(job)
    return JobCreateResponse(
        job_id=job.id, status=job.status, created_at=job.created_at
    )


@router.get("/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"job {job_id} not found"
        )

    return {
        "status": job.status,
        "attempt_count": job.attempt_count,
        "last_error": job.last_error,
        "result": job.result,
    }
