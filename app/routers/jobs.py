from uuid import uuid4
from sqlalchemy import func

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


@router.post("/drain")
def drain_jobs(db: Session = Depends(get_db)):

    result = (
        db.query(Job)
        .filter(Job.status == "QUEUED")
        .update(
            {
                Job.status: "CANCELLED",
                Job.updated_at: func.now(),
            },
            synchronize_session=False,
        )
    )

    db.commit()

    return {
        "cancelled_count": result,
        "message": "Pending jobs drained successfully",
    }


@router.get("/queue-depth")
def get_queue_depth(db: Session = Depends(get_db)):
    from sqlalchemy import func

    results = (
        db.query(
            Job.status,
            Job.priority,
            func.count(Job.id).label("count"),
        )
        .filter(Job.status.in_(["QUEUED", "RUNNING"]))
        .group_by(Job.status, Job.priority)
        .all()
    )

    depth = {
        "total_pending": 0,
        "by_status": {},
        "by_priority": {},
    }

    for status_value, priority, count in results:
        depth["by_status"][status_value] = (
            depth["by_status"].get(status_value, 0) + count
        )

        depth["by_priority"][priority] = depth["by_priority"].get(priority, 0) + count

        if status_value == "QUEUED":
            depth["total_pending"] += count

    return depth


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


@router.post("/{job_id}/cancel")
def cancel_job(job_id: str, db: Session = Depends(get_db)):

    job = db.query(Job).filter(Job.id == job_id).with_for_update().first()

    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found",
        )

    # Make cancellation idempotent.
    if job.status == "CANCELLED":
        return {"job_id": job.id, "status": job.status}

    if job.status != "QUEUED":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job in {job.status} state",
        )

    job.status = "CANCELLED"
    db.commit()

    return {"job_id": job.id, "status": job.status}
