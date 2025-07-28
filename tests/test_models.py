# tests/test_models.py
from datetime import datetime, timezone
from pydantic import HttpUrl
from src.models import Event


def test_event_roundtrip():
    start = datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    e = Event(source="T", source_id="1", title="Test",
              start=start, end=end,
              url=HttpUrl("https://example.com"))
    assert e.title == "Test"


def test_event_has_time_info():
    """Test that events with specific times return has_time_info=True."""
    start = datetime(2024, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

    event = Event(
        source="GSP",
        source_id="test-123",
        title="Test Event",
        start=start,
        end=end,
        venue="Test Park",
        url=HttpUrl("https://example.com/event/123")
    )

    assert event.has_time_info() is True
    assert event.is_date_only() is False


def test_event_date_only():
    """Test that events with zero duration at midnight are treated as date-only."""
    # Zero-duration event at midnight UTC (date-only)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)

    event = Event(
        source="GSP",
        source_id="test-123",
        title="Test Event",
        start=start,
        end=end,
        venue="Test Park",
        url=HttpUrl("https://example.com/event/123")
    )

    assert event.has_time_info() is False
    assert event.is_date_only() is True
