# tests/test_database.py
from src.database import Database


def test_database_has_events():
    """Basic test to verify we can connect to the database and it has events."""
    with Database(compress_on_exit=False) as db:
        count = db.get_source_events_count()
        assert count > 0, f"Expected database to have events, but found {count}"


def test_can_list_events():
    """Test that we can retrieve events from the database."""
    with Database(compress_on_exit=False) as db:
        events = db.get_source_events()
        assert len(events) > 0, "Expected to retrieve events from database"

        # Verify the first event has required fields
        first_event = events[0]
        assert first_event.source is not None
        assert first_event.source_id is not None
        assert first_event.title is not None
        assert first_event.start is not None
        assert first_event.end is not None
        assert first_event.url is not None
