import time


def handle(payload: dict) -> dict:
    recipient = payload.get("recipient")

    if not recipient:
        raise ValueError("recipient is required")

    # actual work
    time.sleep(1)

    return {"recipient": recipient, "message": "Email sent successfully"}
