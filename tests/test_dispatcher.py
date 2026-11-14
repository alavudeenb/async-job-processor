import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

import pytest

from app.dispatcher import dispatcher


def create_event(event_id, job_id):
    return SimpleNamespace(
        id=event_id,
        job_id=job_id,
        published_at=None,
        available_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        created_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_db():
    db = MagicMock()

    with patch.object(
        dispatcher,
        "SessionLocal",
        return_value=db,
    ):
        yield db


@pytest.fixture
def mock_channel():
    return MagicMock()


def configure_events(db, events):
    db.execute.return_value.all.return_value = [(event, "HIGH") for event in events]


def test_dispatch_returns_zero_when_no_pending_events(
    mock_db,
    mock_channel,
):
    configure_events(mock_db, [])

    result = dispatcher.dispatch_pending_events(mock_channel)

    assert result == 0
    mock_channel.basic_publish.assert_not_called()
    mock_db.commit.assert_not_called()
    mock_db.close.assert_called_once()


def test_dispatch_publishes_single_event(
    mock_db,
    mock_channel,
):
    event = create_event("event-1", "job-1")
    configure_events(mock_db, [event])

    result = dispatcher.dispatch_pending_events(mock_channel)

    assert result == 1

    mock_channel.basic_publish.assert_called_once()

    call_args = mock_channel.basic_publish.call_args

    assert call_args.kwargs["exchange"] == ""
    assert call_args.kwargs["routing_key"] == dispatcher.QUEUE_NAME
    assert json.loads(call_args.kwargs["body"]) == {"job_id": "job-1"}

    properties = call_args.kwargs["properties"]

    assert properties.delivery_mode == 2
    assert properties.content_type == "application/json"
    assert properties.message_id == "event-1"

    assert event.published_at is not None
    mock_db.commit.assert_called_once()
    mock_db.close.assert_called_once()


def test_dispatch_publishes_all_events_in_batch(
    mock_db,
    mock_channel,
):
    events = [
        create_event("event-1", "job-1"),
        create_event("event-2", "job-2"),
        create_event("event-3", "job-3"),
    ]

    configure_events(mock_db, events)

    result = dispatcher.dispatch_pending_events(mock_channel)

    assert result == 3
    assert mock_channel.basic_publish.call_count == 3
    assert mock_db.commit.call_count == 1

    for event in events:
        assert event.published_at is not None


def test_dispatch_rolls_back_when_rabbitmq_publish_fails(
    mock_db,
    mock_channel,
):
    event = create_event("event-1", "job-1")
    configure_events(mock_db, [event])

    mock_channel.basic_publish.side_effect = RuntimeError("RabbitMQ unavailable")

    with pytest.raises(RuntimeError, match="RabbitMQ unavailable"):
        dispatcher.dispatch_pending_events(mock_channel)

    assert event.published_at is None
    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()


def test_dispatch_rolls_back_when_database_commit_fails(
    mock_db,
    mock_channel,
):
    event = create_event("event-1", "job-1")
    configure_events(mock_db, [event])

    mock_db.commit.side_effect = RuntimeError("Database commit failed")

    with pytest.raises(RuntimeError, match="Database commit failed"):
        dispatcher.dispatch_pending_events(mock_channel)

    mock_channel.basic_publish.assert_called_once()
    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()


def test_dispatch_closes_session_when_query_fails(
    mock_db,
    mock_channel,
):
    mock_db.execute.side_effect = RuntimeError("Database unavailable")

    with pytest.raises(RuntimeError, match="Database unavailable"):
        dispatcher.dispatch_pending_events(mock_channel)

    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()
