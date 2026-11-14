import json
import logging
import os
import time
from uuid import uuid4

import pika

from datetime import datetime, timezone
from sqlalchemy import select, func

from app.config import RABBITMQ_URL
from app.storagelayer.database import SessionLocal
from app.storagelayer.models import OutboxEvent, Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


QUEUE_NAME = "jobs_priority"
BATCH_SIZE = 20
POLL_INTERVAL_SECONDS = 2
PRIORITY_MAP = {"HIGH": 9, "MEDIUM": 5, "LOW": 1}


def recover_expired_jobs():
    db = SessionLocal()
    recovered_count = 0

    try:
        expired_jobs = (
            db.execute(
                select(Job)
                .where(
                    Job.status == "RUNNING",
                    Job.lease_expires_at <= func.now(),
                )
                .order_by(Job.lease_expires_at)
                .limit(BATCH_SIZE)
                .with_for_update(of=Job, skip_locked=True)
            )
            .scalars()
            .all()
        )

        for job in expired_jobs:
            if job.attempt_count > job.max_retries:
                job.status = "DEAD-LETTERED"
                job.last_error = (
                    job.last_error
                    or "Worker lease expired after retry limit was exhausted"
                )
                job.locked_by = None
                job.lease_expires_at = None
                continue

            job.status = "QUEUED"
            job.locked_by = None
            job.lease_expires_at = None

            db.add(
                OutboxEvent(
                    id=str(uuid4()),
                    job_id=job.id,
                    available_at=datetime.now(timezone.utc),
                )
            )

            recovered_count += 1

        db.commit()

        if recovered_count:
            logger.info(
                "Recovered %s expired jobs",
                recovered_count,
            )

        return recovered_count

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


def dispatch_pending_events(channel):
    db = SessionLocal()

    try:
        events = db.execute(
            select(OutboxEvent, Job.priority)
            .join(Job, Job.id == OutboxEvent.job_id)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.available_at <= func.now(),
                Job.status == "QUEUED",
            )
            .order_by(OutboxEvent.available_at, OutboxEvent.created_at)
            .with_for_update(of=OutboxEvent, skip_locked=True)
            .limit(BATCH_SIZE)
        ).all()

        if not events:
            return 0

        for event, job_priority in events:
            message = {"job_id": event.job_id}

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    message_id=event.id,
                    priority=PRIORITY_MAP[job_priority],
                ),
                mandatory=True,
            )

            event.published_at = datetime.now(timezone.utc)

            logger.info("Published job id=%s, event id=%s", event.job_id, event.id)

        db.commit()

        return len(events)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main():
    if not RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL is not configured")

    while True:
        connection = None

        try:
            parameters = pika.URLParameters(RABBITMQ_URL)

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(
                queue=QUEUE_NAME, durable=True, arguments={"x-max-priority": 10}
            )

            channel.confirm_delivery()

            logger.info("Connected to RabbitMQ")

            while True:
                recover_expired_jobs()

                dispatched = dispatch_pending_events(channel)

                if dispatched == 0:
                    time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Dispatcher stopped")
            break

        except Exception:
            logger.exception("Dispatcer error: Retrying in 5 seconds")
            time.sleep(5)

        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
