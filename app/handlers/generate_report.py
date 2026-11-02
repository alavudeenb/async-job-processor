import time


def handle(payload: dict) -> dict:
    report_id = payload.get("report_id")

    if not report_id:
        raise ValueError("report_id is required")

    # actual work
    time.sleep(20)

    return {"report_id": report_id, "message": "report generated successfully"}
