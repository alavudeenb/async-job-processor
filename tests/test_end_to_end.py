import os
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e


@pytest.fixture
def api_client():
    api_url = os.getenv("E2E_API_URL")

    if not api_url:
        pytest.skip("E2E_API_URL is not configured")

    with httpx.Client(base_url=api_url, timeout=10) as client:
        yield client


def wait_for_terminal_status(client, job_id, timeout=120):
    """Poll the API until the job reaches a terminal state."""

    terminal_statuses = {"SUCCEEDED", "FAILED", "DEAD-LETTERED"}
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        response = client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200, response.text

        job = response.json()
        status = job["status"]

        if status in terminal_statuses:
            return job

        time.sleep(2)

    raise AssertionError(
        f"Job {job_id} did not reach a terminal state within {timeout} seconds"
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
