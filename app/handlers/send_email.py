import time
from app.exceptions import PermanentJobError, RetryableJobError


def handle(payload: dict) -> dict:
    recipient = payload.get("recipient")

    if not recipient:
        raise PermanentJobError("recipient is required")

    try:
        # Simulate actual email operation
        time.sleep(1)

        # Simulate a temporary failure
        raise TimeoutError("Email provider timed out")

    except TimeoutError as exc:
        raise RetryableJobError("Temporary email delivery failure") from exc

    return {"recipient": recipient, "message": "Email sent successfully"}
