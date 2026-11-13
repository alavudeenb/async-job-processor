import multiprocessing
import threading

from app.worker.worker import run_handler_with_timeout


def test_handler_execution_success():
    ownership_lost = threading.Event()

    status, result = run_handler_with_timeout(
        job_type="send_email",
        payload={"recipient": "abc@xyz.com"},
        timeout_seconds=10,
        ownership_lost=ownership_lost,
    )

    assert status == "success"
    assert result == {
        "recipient": "abc@xyz.com",
        "message": "Email sent successfully",
    }


def test_handler_execution_timeout():
    ownership_lost = threading.Event()

    status, error = run_handler_with_timeout(
        job_type="generate_report",
        payload={"report_id": "RPT-001"},
        timeout_seconds=2,
        ownership_lost=ownership_lost,
    )

    assert status == "timeout"
    assert error == "Handler timed out after 2 seconds"


def test_handler_execution_stops_when_ownership_lost():
    ownership_lost = threading.Event()
    ownership_lost.set()

    status, result = run_handler_with_timeout(
        job_type="generate_report",
        payload={"report_id": "RPT-001"},
        timeout_seconds=10,
        ownership_lost=ownership_lost,
    )

    assert status == "ownership_lost"
    assert result is None
