from uuid import uuid4
import logging


from sqlalchemy import func

from app.storagelayer.schemas import JobCreateRequest, JobCreateResponse
from app.storagelayer.models import Job, OutboxEvent
from app.storagelayer.database import SessionLocal
from app.exceptions import JobConflictError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_job(request: JobCreateRequest) -> JobCreateResponse:
    db = SessionLocal()
    try:
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
    except Exception as exc:
        db.rollback()
        logger.exception("Failed to create job for job_type=%s", request.job_type)
        raise Exception("Unable to process the request") from exc
    finally:
        db.close()


def get_job(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()

        if job is None:
            return None

        return {
            "status": job.status,
            "attempt_count": job.attempt_count,
            "last_error": job.last_error,
            "result": job.result,
        }
    except Exception as exc:
        logger.exception("Failed while fetching job_id=%s", job_id)
        raise Exception("Unable to process the request") from exc
    finally:
        db.close()


def get_queue_depth():
    db = SessionLocal()
    try:
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

            depth["by_priority"][priority] = (
                depth["by_priority"].get(priority, 0) + count
            )

            if status_value == "QUEUED":
                depth["total_pending"] += count

        return depth
    except Exception as exc:
        logger.exception("Failed while fetching queue depth")
        raise Exception("Unable to process the request") from exc
    finally:
        db.close()


def cancel_job(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).with_for_update().first()

        if job is None:
            return None

        # Make cancellation idempotent.
        if job.status == "CANCELLED":
            return {"job_id": job.id, "status": job.status}

        if job.status != "QUEUED":
            raise JobConflictError(f"Cannot cancel job in {job.status} state")

        job.status = "CANCELLED"
        db.commit()

        return {"job_id": job.id, "status": job.status}
    except JobConflictError:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("Failed while cancelling job_id=%s", job_id)
        raise Exception("Unable to process the request") from exc
    finally:
        db.close()


def drain_jobs():
    db = SessionLocal()
    try:
        # 1. Select and lock all queued jobs
        jobs = db.query(Job).filter(Job.status == "QUEUED").with_for_update().all()

        # 2. Update the locked rows
        for job in jobs:
            job.status = "CANCELLED"
            job.updated_at = func.now()

        # 3. Commit and release locks
        db.commit()

        return {
            "cancelled_count": len(jobs),
            "message": "Pending jobs drained successfully",
        }
    except Exception as exc:
        db.rollback()
        logger.exception("Failed while draining jobs")
        raise Exception("Unable to process the request") from exc
    finally:
        db.close()
