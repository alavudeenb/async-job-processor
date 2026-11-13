from importlib import import_module

from app.config import REGISTERED_JOB_TYPES


def resolve_handler(job_type: str):
    if job_type not in REGISTERED_JOB_TYPES:
        raise ValueError(f"Unsupported job type:{job_type}")

    module = import_module(f"app.worker.handlers.jobs.{job_type}")

    handler = getattr(module, "handle", None)

    if not callable(handler):
        raise ValueError(f"Handler module for {job_type} must expose handle")

    return handler
