import os
import time

import httpx
import pytest

from dotenv import load_dotenv

pytestmark = pytest.mark.e2e

load_dotenv(override=False)


@pytest.fixture
def api_client():
    api_url = os.getenv("E2E_API_URL")

    if not api_url:
        pytest.skip("E2E_API_URL is not configured")

    with httpx.Client(base_url=api_url, timeout=30) as client:
        yield client


def wait_for_terminal_status(client, job_id, timeout=120):
    """Poll until the job reaches a terminal state."""

    terminal_statuses = {
        "SUCCEEDED",
        "FAILED",
        "DEAD-LETTERED",
        "CANCELLED",
    }

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200, response.text

        job = response.json()

        if job["status"] in terminal_statuses:
            return job

        time.sleep(2)

    raise AssertionError(
        f"Job {job_id} did not reach a terminal state " f"within {timeout} seconds"
    )


def wait_for_status(client, job_id, expected_status, timeout=15):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200, response.text

        job = response.json()

        if job["status"] == expected_status:
            return job

        if job["status"] in {"SUCCEEDED", "FAILED", "DEAD-LETTERED", "CANCELLED"}:
            raise AssertionError(
                f"Job reached {job['status']} instead of {expected_status}"
            )

        time.sleep(0.5)

    raise AssertionError(
        f"Job did not reach {expected_status} within {timeout} seconds"
    )


def wait_for_retry_queued(client, job_id, timeout=90):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200, response.text

        job = response.json()

        if job["status"] == "QUEUED" and job["attempt_count"] >= 1:
            return job

        if job["status"] in {
            "SUCCEEDED",
            "FAILED",
            "DEAD-LETTERED",
            "CANCELLED",
        }:
            raise AssertionError(f"Unexpected terminal status: {job['status']}")

        time.sleep(0.5)

    raise AssertionError(
        f"Job {job_id} did not return to QUEUED after retry "
        f"within {timeout} seconds"
    )


def test_job_submission_to_success(api_client):
    # 1. Submit job through the real API.
    response = api_client.post(
        "/api/jobs",
        json={
            "job_type": "send_email",
            "payload": {"recipient": "test@example.com"},
            "priority": "HIGH",
            "max_retries": 3,
            "timeout_seconds": 30,
        },
    )

    assert response.status_code == 202, response.text

    job_id = response.json()["job_id"]

    # 2. Wait for dispatcher and worker to process the job.
    job = wait_for_terminal_status(api_client, job_id)

    # 3. Verify the final result.
    assert job["status"] == "SUCCEEDED"
    assert job["result"]["recipient"] == "test@example.com"


def test_permanent_failure_marks_job_failed(api_client):
    # Missing recipient causes PermanentJobError in send_email handler.
    response = api_client.post(
        "/api/jobs",
        json={
            "job_type": "send_email",
            "payload": {},
            "priority": "HIGH",
            "max_retries": 3,
            "timeout_seconds": 30,
        },
    )

    assert response.status_code == 202, response.text

    job_id = response.json()["job_id"]

    # Wait for the worker to process the permanent failure.
    job = wait_for_terminal_status(api_client, job_id)

    assert job["status"] == "FAILED"
    assert job["last_error"] is not None
    assert "recipient is required" in job["last_error"]
    assert job["attempt_count"] == 1


def test_cancel_job_during_retry(api_client):

    response = api_client.post(
        "/api/jobs",
        json={
            "job_type": "generate_report",
            "payload": {"report_id": "123"},
            "priority": "LOW",
            "max_retries": 3,
            "timeout_seconds": 10,
        },
    )

    assert response.status_code == 202, response.text

    job_id = response.json()["job_id"]

    # Wait for first attempt to fail and retry to be scheduled.
    job = wait_for_retry_queued(api_client, job_id)

    assert job["attempt_count"] >= 1

    # Cancel while waiting for retry.
    cancel_response = api_client.post(f"/api/jobs/{job_id}/cancel")

    assert cancel_response.status_code in (200, 202), cancel_response.text

    # Verify cancellation.
    job = wait_for_terminal_status(api_client, job_id)

    assert job["status"] == "CANCELLED"
    assert job["attempt_count"] >= 1


def test_job_timeout(api_client):
    response = api_client.post(
        "/api/jobs",
        json={
            "job_type": "generate_report",
            "payload": {"report_id": "RPT-TIMEOUT-001"},
            "priority": "HIGH",
            "max_retries": 0,
            "timeout_seconds": 10,
        },
    )

    assert response.status_code == 202, response.text

    job_id = response.json()["job_id"]

    job = wait_for_terminal_status(
        api_client,
        job_id,
        timeout=120,
    )

    assert job["status"] == "DEAD-LETTERED"
    assert job["last_error"] is not None


def test_queue_depth(api_client):
    response = api_client.get("/api/jobs/queue-depth")

    assert response.status_code == 200, response.text

    queue_data = response.json()

    assert "total_pending" in queue_data
    assert "by_status" in queue_data
    assert "by_priority" in queue_data

    total_pending = queue_data["total_pending"]
    by_status = queue_data["by_status"]
    by_priority = queue_data["by_priority"]

    assert isinstance(total_pending, int)
    assert total_pending >= 0

    assert isinstance(by_status, dict)
    assert isinstance(by_priority, dict)

    # All reported counts must be non-negative integers.
    assert all(isinstance(count, int) and count >= 0 for count in by_status.values())

    assert all(isinstance(count, int) and count >= 0 for count in by_priority.values())

    # QUEUED jobs should match total_pending.
    assert by_status.get("QUEUED", 0) == total_pending


def test_drain_pending_jobs(api_client):

    response = api_client.post(
        "/api/jobs",
        json={
            "job_type": "generate_report",
            "payload": {"report_id": "123"},
            "priority": "LOW",
            "max_retries": 3,
            "timeout_seconds": 10,
        },
    )

    assert response.status_code == 202, response.text

    job_id = response.json()["job_id"]

    # Wait until the failed job is queued for retry.
    job = wait_for_retry_queued(api_client, job_id)

    assert job["attempt_count"] >= 1

    # Drain pending jobs.
    drain_response = api_client.post("/api/jobs/drain")

    assert drain_response.status_code == 200, drain_response.text

    drain_data = drain_response.json()

    assert drain_data["cancelled_count"] >= 1
    assert drain_data["message"] == "Pending jobs drained successfully"

    # Verify that our particular job was cancelled.
    job_response = api_client.get(f"/api/jobs/{job_id}")

    assert job_response.status_code == 200, job_response.text

    job = job_response.json()

    assert job["status"] == "CANCELLED"
    assert job["attempt_count"] >= 1
