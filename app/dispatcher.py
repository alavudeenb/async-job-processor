import json
import logging
import os
import time

import pika


from datetime import datetime, timezone
from sqlalchemy import select, func


from app.config import RABBITMQ_URL
from app.database import SessionLocal
from app.models import OutboxEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


QUEUE_NAME = "jobs"
BATCH_SIZE = 20
POLL_INTERVAL_SECONDS = 2


def dispatch_pending_events(channel):
    db = SessionLocal()

    try:
        events = (
            db.execute(
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                    OutboxEvent.available_at <= func.now(),
                )
                .order_by(OutboxEvent.available_at, OutboxEvent.created_at)
                .limit(BATCH_SIZE)
            )
            .scalars()
            .all()
        )

        if not events:
            return 0

        for event in events:
            message = {"job_id": event.job_id}

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    message_id=event.id,
                ),
                mandatory=True,
            )

            event.published_at = datetime.now(timezone.utc)

            db.commit()

            logger.info("Published job id=%s, event id=%s", event.job_id, event.id)

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

            channel.queue_declare(queue=QUEUE_NAME, durable=True)

            channel.confirm_delivery()

            logger.info("Connected to RabbitMQ")

            while True:
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
            if connection is connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
