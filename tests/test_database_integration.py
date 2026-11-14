import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.storagelayer.database import Base
from app.storagelayer.models import Job, OutboxEvent
from app.worker import worker

pytestmark = pytest.mark.integration


@pytest.fixture
def postgres_db(monkeypatch):
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        pytest.skip("DATABASE_URL is not configured")

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )

    TestingSessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
    )

    # This test requires a dedicated database.
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    monkeypatch.setattr(worker, "SessionLocal", TestingSessionLocal)

    yield TestingSessionLocal

    Base.metadata.drop_all(bind=engine)
    engine.dispose()


def test_job_and_outbox_event_rollback_together(postgres_db):
    job_id = str(uuid4())
    event_id = str(uuid4())

    db = postgres_db()

    try:
        db.add(
            Job(
                id=job_id,
                job_type="send_email",
                payload={"recipient": "test@example.com"},
                priority="HIGH",
                status="QUEUED",
                max_retries=3,
                timeout_seconds=30,
                attempt_count=0,
            )
        )

        db.add(
            OutboxEvent(
                id=event_id,
                job_id=job_id,
            )
        )

        # Simulate a failure before the transaction commits.
        db.rollback()

    finally:
        db.close()

    # Neither record should have been persisted.
    with postgres_db() as verification_db:
        job = verification_db.get(Job, job_id)
        event = verification_db.get(OutboxEvent, event_id)

        assert job is None
        assert event is None


def test_only_one_worker_can_claim_a_job(postgres_db):
    job_id = str(uuid4())

    with postgres_db() as db:
        db.add(
            Job(
                id=job_id,
                job_type="send_email",
                payload={"recipient": "test@example.com"},
                priority="HIGH",
                status="QUEUED",
                max_retries=3,
                timeout_seconds=30,
                attempt_count=0,
                lock_version=0,
            )
        )
        db.commit()

    barrier = Barrier(2)

    def claim(worker_id):
        barrier.wait()
        return worker.claim_job(job_id, worker_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(claim, "worker-A"),
            executor.submit(claim, "worker-B"),
        ]
        results = [future.result() for future in futures]

    successful_claims = [result for result in results if result is not None]

    assert len(successful_claims) == 1

    with postgres_db() as db:
        job = db.get(Job, job_id)

        assert job.status == "RUNNING"
        assert job.locked_by in {"worker-A", "worker-B"}
        assert job.attempt_count == 1
        assert job.lock_version == 1
