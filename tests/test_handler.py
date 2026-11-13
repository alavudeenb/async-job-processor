import pytest

from app.worker.handler_resolver import resolve_handler
from app.exceptions import PermanentJobError, RetryableJobError


def test_resolve_send_email_handler():
    handler = resolve_handler("send_email")

    result = handler({"recipient": "abc@xyz.com"})

    assert result == {
        "recipient": "abc@xyz.com",
        "message": "Email sent successfully",
    }


def test_send_email_retryable_failure():
    handler = resolve_handler("send_email")

    with pytest.raises(RetryableJobError):
        handler({"recipient": "abc@xyz.com", "simulate_timeout": True})


def test_resolve_generate_report_handler():
    handler = resolve_handler("generate_report")

    result = handler({"report_id": "RPT-001"})

    assert result == {
        "report_id": "RPT-001",
        "message": "report generated successfully",
    }


def test_generate_report_rejects_missing_report_id():
    handler = resolve_handler("generate_report")

    with pytest.raises(PermanentJobError, match="report_id is required"):
        handler({})


def test_unsupported_job_type():
    with pytest.raises((ValueError, ImportError, ModuleNotFoundError)):
        resolve_handler("unsupported_job")
