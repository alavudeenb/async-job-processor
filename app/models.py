from datetime import datetime, timezone

from sqlalchemy import Column, String, Integer, DateTime, JSON, Text

from app.database import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(String(36), primary_key=True)
    job_type = Column(String(25), nullable=False)
    payload = Column(JSON, nullable=False)

    priority = Column(String(10), nullable=False, default="LOW")
    status = Column(String(30), nullable=False, default="QUEUED")

    max_retries = Column(Integer, nullable=False)
    timeout_seconds = Column(Integer, nullable=False)
    attempt_count = Column(Integer, nullable=False, default=0)

    last_error = Column(String(100), nullable=True)
    result = Column(JSON, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    locked_by = Column(String(100), nullable=True)
    lease_expires_at = Column(DateTime(timezone=True), nullable=True)
    lock_version = Column(Integer, nullable=False, default=0)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id = Column(String(36), primary_key=True)
    job_id = Column(String(36), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    published_at = Column(DateTime(timezone=True), nullable=True)

    available_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
