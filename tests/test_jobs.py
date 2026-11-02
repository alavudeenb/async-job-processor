from sqlalchemy.orm import Session

from app.models import Job, OutboxEvent


def test_create_job_persists_job_and_outbox_event(client):
    request_payload = {
        "job_type": "generate_report",
        "payload": {"report_id": "RPT-1001"},
        "priority": "HIGH",
        "max_retries": 3,
        "timeout_seconds": 30,
    }

    response = client.post("/api/jobs", json=request_payload)

    assert response.status_code == 202

    response_data = response.json()

    assert response_data["job_id"]
    assert response_data["status"] == "QUEUED"

    # Verify both records were persisted.
    from app.database import get_db

    db_generator = get_db()
    db = next(db_generator)

    try:
        with client.db_session() as db:
            job = db.query(Job).filter(Job.id == response_data["job_id"]).first()

            event = (
                db.query(OutboxEvent)
                .filter(OutboxEvent.job_id == response_data["job_id"])
                .first()
            )

            assert job is not None
            assert event is not None
            assert job.job_type == "generate_report"
            assert job.payload == {"report_id": "RPT-1001"}
            assert job.status == "QUEUED"

    finally:
        db.close()


def test_get_job_status(client):
    # Create a job first.
    payload = {
        "job_type": "generate_report",
        "payload": {"report_id": "RPT-1002"},
        "priority": "HIGH",
        "max_retries": 3,
        "timeout_seconds": 30,
    }

    create_response = client.post("/api/jobs", json=payload)
    job_id = create_response.json()["job_id"]

    response = client.get(f"/api/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json() == {
        "status": "QUEUED",
        "attempt_count": 0,
        "last_error": None,
        "result": None,
    }


def test_get_unknown_job_returns_404(client):
    response = client.get("/api/jobs/unknown-job-id")

    assert response.status_code == 404


def create_test_job(client, report_id):
    payload = {
        "job_type": "generate_report",
        "payload": {"report_id": report_id},
        "priority": "HIGH",
        "max_retries": 3,
        "timeout_seconds": 30,
    }

    response = client.post("/api/jobs", json=payload)
    assert response.status_code == 202

    return response.json()["job_id"]


def test_cancel_queued_job(client):
    job_id = create_test_job(client, "RPT-CANCEL-1")

    response = client.post(f"/api/jobs/{job_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"

    # Cancellation should be idempotent.
    response = client.post(f"/api/jobs/{job_id}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"


def test_cannot_cancel_running_job(client):
    job_id = create_test_job(client, "RPT-CANCEL-2")

    with client.db_session() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        job.status = "RUNNING"
        db.commit()

    response = client.post(f"/api/jobs/{job_id}/cancel")

    assert response.status_code == 409


def test_drain_cancels_only_queued_jobs(client):
    queued_job_id = create_test_job(client, "RPT-DRAIN-1")
    running_job_id = create_test_job(client, "RPT-DRAIN-2")

    with client.db_session() as db:
        running_job = db.query(Job).filter(Job.id == running_job_id).first()

        running_job.status = "RUNNING"
        db.commit()

    response = client.post("/api/jobs/drain")

    assert response.status_code == 200
    assert response.json()["cancelled_count"] == 1

    with client.db_session() as db:
        queued_job = db.query(Job).filter(Job.id == queued_job_id).first()

        running_job = db.query(Job).filter(Job.id == running_job_id).first()

        assert queued_job.status == "CANCELLED"
        assert running_job.status == "RUNNING"
