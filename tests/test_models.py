# tests/test_models.py
from src.models import Event
def test_event_roundtrip():
    e = Event(source="T", source_id="1", title="Test",
              start="2025-01-01T09:00:00", end="2025-01-01T11:00:00",
              url="https://example.com")
    assert e.title == "Test"