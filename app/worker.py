import uuid
import json
import logging
import time

import pika

from sqlalchemy import text
from app.database import SessionLocal
from app.config import RABBITMQ_URL
from app.handler_resolver import resolve_handler
from app.models import OutboxEvent

import threading
import multiprocessing

from concurrent.futures import ThreadPoolExecutor

from datetime import datetime, timedelta, timezone
from uuid import uuid4

LEASE_DURATION_SECONDS = 60
LEASE_RENEW_INTERVAL_SECONDS = 20

QUEUE_NAME = "jobs"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def claim_job(job_id: str, worker_id: str):
    db = SessionLocal()

    try:
        result = (
            db.execute(
                text("""
                UPDATE jobs
                SET
                    status = 'RUNNING',
                    locked_by = :worker_id,
                    lease_expires_at =
                        NOW() + (:lease_seconds * INTERVAL '1 second'),
                    lock_version = lock_version + 1,
                    attempt_count = attempt_count + 1,
                    updated_at = NOW()
                WHERE id = :job_id
                  AND (
                      status = 'QUEUED'
                      OR (
                          status = 'RUNNING'
                          AND lease_expires_at <= NOW()
                      )
                  )
                RETURNING
                    id,
                    job_type,
                    payload,
                    attempt_count,
                    max_retries,
                    timeout_seconds,
                    lock_version
            """),
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "lease_seconds": LEASE_DURATION_SECONDS,
                },
            )
            .mappings()
            .first()
        )

        db.commit()
        return dict(result) if result else None

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


def renew_lease(job_id: str, worker_id: str, lock_version: int) -> bool:
    db = SessionLocal()

    try:
        result = db.execute(
            text("""
                UPDATE jobs
                SET
                    lease_expires_at =
                        NOW() + (:lease_seconds * INTERVAL '1 second'),
                    updated_at = NOW()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                  AND lock_version = :lock_version
                  AND lease_expires_at > NOW()
            """),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "lock_version": lock_version,
                "lease_seconds": LEASE_DURATION_SECONDS,
            },
        )

        db.commit()
        return result.rowcount == 1
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def complete_job(
    job_id: str, worker_id: str, lock_version: int, result_payload: dict
) -> bool:
    db = SessionLocal()

    try:
        result = db.execute(
            text("""
                UPDATE jobs
                SET
                    status = 'SUCCEEDED',
                    result = CAST(:result AS JSON),
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                  AND lock_version = :lock_version
                  AND lease_expires_at > NOW()
            """),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "lock_version": lock_version,
                "result": json.dumps(result_payload),
            },
        )

        db.commit()
        return result.rowcount == 1

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def fail_job(
    job_id: str, worker_id: str, lock_version: int, error_message: str
) -> bool:
    db = SessionLocal()

    try:
        result = db.execute(
            text("""
                UPDATE jobs
                SET
                    status = 'FAILED',
                    last_error = :error_message,
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                  AND lock_version = :lock_version
                  AND lease_expires_at > NOW()
            """),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "lock_version": lock_version,
                "error_message": error_message[:4000],
            },
        )

        db.commit()
        return result.rowcount == 1

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


def maintain_lease(
    job_id: str,
    worker_id: str,
    lock_version: int,
    stop_event: threading.Event,
    ownership_lost: threading.Event,
):
    while not stop_event.wait(LEASE_RENEW_INTERVAL_SECONDS):
        try:
            renewed = renew_lease(job_id, worker_id, lock_version)

            if not renewed:
                logger.warning("Lease lost for job_id=%s", job_id)
                ownership_lost.set()
                return

        except Exception:
            logger.exception("Lease renewal failed for job_id=%s", job_id)
            ownership_lost.set()
            return


def _run_handler_process(job_type, payload, send_conn):
    """
    Runs inside the child process.
    """
    try:
        handler = resolve_handler(job_type)
        result = handler(payload)

        send_conn.send(("success", result))

    except Exception as exc:
        send_conn.send(("error", f"{type(exc).__name__}: {exc}"))

    finally:
        send_conn.close()


def run_handler_with_timeout(job_type, payload, timeout_seconds, ownership_lost):
    """
    Starts a child process and waits for its result.
    Terminates it if the timeout expires or lease ownership is lost.
    """

    context = multiprocessing.get_context("spawn")
    recv_conn, send_conn = context.Pipe(duplex=False)

    process = context.Process(
        target=_run_handler_process, args=(job_type, payload, send_conn)
    )

    try:
        process.start()
        send_conn.close()

        deadline = time.monotonic() + timeout_seconds

        while True:

            if ownership_lost.is_set():
                return ("ownership_lost", None)

            if recv_conn.poll(0.2):
                try:
                    status, result = recv_conn.recv()
                except EOFError:
                    return ("error", "Handler process exited unexpectedly")

                process.join(timeout=2)

                return status, result

            if not process.is_alive():
                process.join()
                return (
                    "error",
                    f"Handler process exited unexpectedly. "
                    f"Exit code: {process.exitcode}",
                )

            if time.monotonic() >= deadline:
                return ("timeout", f"Handler timed out after {timeout_seconds} seconds")

    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)

            if process.is_alive():
                process.kill()
                process.join()

        recv_conn.close()
        send_conn.close()


def execute_job(connection, channel, delivery_tag, body):
    try:
        message = json.loads(body)
        job_id = message["job_id"]

    except (json.JSONDecodeError, KeyError, TypeError):
        logger.exception("Invalid RabbitMQ message")

        connection.add_callback_threadsafe(
            lambda: channel.basic_ack(delivery_tag=delivery_tag)
        )
        return

    worker_id = f"worker-{uuid.uuid4()}"

    try:
        # 1. Claim job from PostgreSQL.
        job = claim_job(job_id, worker_id)

        if not job:
            logger.info("Job already claimed or completed: %s", job_id)

            connection.add_callback_threadsafe(
                lambda: channel.basic_ack(delivery_tag=delivery_tag)
            )
            return

        lock_version = job["lock_version"]

        # 2. Start heartbeat in parent process.
        stop_event = threading.Event()
        ownership_lost = threading.Event()

        heartbeat = threading.Thread(
            target=maintain_lease,
            args=(job_id, worker_id, lock_version, stop_event, ownership_lost),
            daemon=True,
        )

        heartbeat.start()

        handler_error = None
        result_payload = None

        try:
            # 3. Execute handler in child process with timeout.
            logger.info(
                "Executing job_id=%s type=%s attempt=%s timeout=%s",
                job_id,
                job["job_type"],
                job["attempt_count"],
                job["timeout_seconds"],
            )

            status, output = run_handler_with_timeout(
                job_type=job["job_type"],
                payload=job["payload"],
                timeout_seconds=job["timeout_seconds"],
                ownership_lost=ownership_lost,
            )

            if status == "success":
                result_payload = output

            elif status in ("timeout", "error"):
                handler_error = output

            elif status == "ownership_lost":
                ownership_lost.set()

        finally:
            # 4. Stop heartbeat after handler execution.
            stop_event.set()
            heartbeat.join()

        # 5. Verify that we still own the job.
        if ownership_lost.is_set():
            logger.warning("Skipping job update because ownership was lost: %s", job_id)

            connection.add_callback_threadsafe(
                lambda: channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            )
            return

        # 6. Handle handler exception or timeout.
        if handler_error:

            logger.error("Job attempt failed for job_id=%s: %s", job_id, handler_error)

            outcome = schedule_retry_or_dead_letter(
                job_id, worker_id, lock_version, handler_error
            )

            if outcome in ("RETRY_SCHEDULED", "DEAD-LETTERED"):
                connection.add_callback_threadsafe(
                    lambda: channel.basic_ack(delivery_tag=delivery_tag)
                )
            else:
                connection.add_callback_threadsafe(
                    lambda: channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                )

            return

        # 7. Complete successfully.
        completed = complete_job(job_id, worker_id, lock_version, result_payload)

        if completed:
            logger.info("Job succeeded: %s", job_id)

            connection.add_callback_threadsafe(
                lambda: channel.basic_ack(delivery_tag=delivery_tag)
            )

        else:
            logger.warning("Fenced completion rejected for job_id=%s", job_id)

            connection.add_callback_threadsafe(
                lambda: channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            )

    except Exception:
        logger.exception("Worker failed for job_id=%s", job_id)

        connection.add_callback_threadsafe(
            lambda: channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        )


def schedule_retry_or_dead_letter(
    job_id: str, worker_id: str, lock_version: int, error_message: str
) -> str:
    db = SessionLocal()

    try:
        # Update only if this worker still owns a valid lease.
        updated_job = (
            db.execute(
                text("""
                UPDATE jobs
                SET
                    status = CASE
                        WHEN attempt_count <= max_retries
                        THEN 'QUEUED'
                        ELSE 'DEAD-LETTERED'
                    END,
                    last_error = :error_message,
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                  AND lock_version = :lock_version
                  AND lease_expires_at > NOW()
                RETURNING attempt_count, max_retries, status
            """),
                {
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "lock_version": lock_version,
                    "error_message": error_message[:4000],
                },
            )
            .mappings()
            .first()
        )

        if not updated_job:
            db.rollback()
            return "ownership_lost"

        if updated_job["status"] == "DEAD-LETTERED":
            db.commit()
            logger.error("Job moved to dead-letter state: %s", job_id)
            return "DEAD-LETTERED"

        # Calculate exponential backoff.
        attempt_count = updated_job["attempt_count"]

        delay_seconds = min(5 * (2 ** (attempt_count - 1)), 300)

        available_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Schedule the next delivery using the transactional outbox.
        retry_event = OutboxEvent(
            id=str(uuid4()), job_id=job_id, available_at=available_at
        )

        db.add(retry_event)
        db.commit()

        logger.info(
            "Retry scheduled for job_id=%s in %s seconds", job_id, delay_seconds
        )

        return "RETRY_SCHEDULED"

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


def main():
    if not RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL is not configured")

    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)

    executor = ThreadPoolExecutor(max_workers=1)

    def on_message(channel, method, properties, body):
        executor.submit(execute_job, connection, channel, method.delivery_tag, body)

    channel.basic_consume(
        queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False
    )

    logger.info("Worker listening on queue: %s", QUEUE_NAME)

    try:
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Worker stopping")

    finally:
        if channel.is_open:
            channel.stop_consuming()

        executor.shutdown(wait=True)

        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
