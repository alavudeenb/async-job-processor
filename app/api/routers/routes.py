from fastapi import APIRouter, status, HTTPException

from app.storagelayer.schemas import JobCreateRequest, JobCreateResponse
from app.config import REGISTERED_JOB_TYPES
from app.api.businesslayer import jobprocessor
from app.exceptions import JobConflictError

apirouter = APIRouter()


@apirouter.get("/")
def Welcome():
    return {"Response": "Hello world"}


@apirouter.get("/health")
def health():
    return {"status": "health"}


@apirouter.post(
    "/api/jobs", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED
)
def create_job(request: JobCreateRequest):
    try:
        if request.job_type not in REGISTERED_JOB_TYPES:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "Unsupported Job Type",
                    "message": f"Unsupported job_type {request.job_type}",
                },
            )

        return jobprocessor.create_job(request)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        ) from exc


@apirouter.get("/api/jobs/queue-depth")
def get_queue_depth():
    try:
        depth = jobprocessor.get_queue_depth()
        return depth
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        )


@apirouter.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    try:
        job = jobprocessor.cancel_job(job_id)

        if job is None:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found",
            )

        return job
    except JobConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        )


@apirouter.post("/api/jobs/drain")
def drain_jobs():
    try:
        result = jobprocessor.drain_jobs()

        return result
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        )


@apirouter.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    try:
        job = jobprocessor.get_job(job_id)

        if job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"job {job_id} not found"
            )

        return job
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        )
