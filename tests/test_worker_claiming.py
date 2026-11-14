from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from uuid import uuid4

from app.worker.worker import claim_job
from app.storagelayer.models import Job
from app.worker.worker import complete_job, renew_lease


def create_queued_job(client):
    response = client.post(
        "/api/jobs",
        json={
            "job_type": "generate_report",
            "payload": {"report_id": str(uuid4())},
            "priority": "HIGH",
            "max_retries": 3,
            "timeout_seconds": 30,
        },
    )

    assert response.status_code == 202
    return response.json()["job_id"]


def test_only_one_worker_can_claim_same_job(client):
    job_id = create_queued_job(client)

    barrier = Barrier(2)

    def worker_claim(worker_id):
        barrier.wait()
        return claim_job(job_id, worker_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(worker_claim, "worker-A")
        future_b = executor.submit(worker_claim, "worker-B")

        result_a = future_a.result()
        result_b = future_b.result()

    successful_claims = [
        result for result in (result_a, result_b) if result is not None
    ]

    failed_claims = [result for result in (result_a, result_b) if result is None]

    assert len(successful_claims) == 1
    assert len(failed_claims) == 1

    claimed_job = successful_claims[0]

    assert claimed_job["id"] == job_id
    assert claimed_job["lock_version"] == 1
    assert claimed_job["attempt_count"] == 1

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()

        assert job.status == "RUNNING"
        assert job.locked_by in ("worker-A", "worker-B")
        assert job.lock_version == 1
        assert job.attempt_count == 1


def test_worker_cannot_claim_job_with_active_lease(client):
    job_id = create_queued_job(client)

    first_claim = claim_job(job_id, "worker-A")

    assert first_claim is not None

    second_claim = claim_job(job_id, "worker-B")

    assert second_claim is None

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()

        assert job.status == "RUNNING"
        assert job.locked_by == "worker-A"
        assert job.lock_version == 1
        assert job.attempt_count == 1


def test_worker_can_reclaim_job_after_lease_expiry(client):
    job_id = create_queued_job(client)

    first_claim = claim_job(job_id, "worker-A")

    assert first_claim is not None
    assert first_claim["lock_version"] == 1

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()

        job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)

        db.commit()

    second_claim = claim_job(job_id, "worker-B")

    assert second_claim is not None
    assert second_claim["lock_version"] == 2
    assert second_claim["attempt_count"] == 2

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()

        assert job.status == "RUNNING"
        assert job.locked_by == "worker-B"
        assert job.lock_version == 2
        assert job.attempt_count == 2


def test_stale_worker_cannot_renew_lease(client):
    job_id = create_queued_job(client)

    first_claim = claim_job(job_id, "worker-A")

    assert first_claim is not None

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        db.commit()

    second_claim = claim_job(job_id, "worker-B")

    assert second_claim is not None

    renewed = renew_lease(
        job_id,
        "worker-A",
        first_claim["lock_version"],
    )

    assert renewed is False


def test_stale_worker_cannot_complete_reclaimed_job(client):
    job_id = create_queued_job(client)

    first_claim = claim_job(job_id, "worker-A")

    assert first_claim is not None

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        db.commit()

    second_claim = claim_job(job_id, "worker-B")

    assert second_claim is not None

    completed = complete_job(
        job_id=job_id,
        worker_id="worker-A",
        lock_version=first_claim["lock_version"],
        result_payload={"result": "stale"},
    )

    assert completed is False

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()

        assert job.status == "RUNNING"
        assert job.locked_by == "worker-B"
        assert job.lock_version == 2
        assert job.result is None
