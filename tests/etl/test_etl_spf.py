# tests/test_etl_spf.py
from pathlib import Path
from src.etl.spf import SPFExtractor

data_path = Path(__file__).parent / "data"


def test_parse_fixture():
    html = (data_path / "spf_events.html").read_text()
    extractor = SPFExtractor(html)
    events = extractor.extract()

    # sanity check - expect at least a few events
    assert len(events) >= 3
    assert all(e.source == "SPF" for e in events)

    # Check that we can parse the first event from the JSON-LD data
    first_event = events[0]
    assert first_event.title == "Pigeon Point Park Restoration Event"
    assert first_event.start.year == 2025
    assert first_event.start.month == 7
    assert first_event.start.day == 29
    assert "Pigeon Point" in first_event.title
    assert first_event.url

    # Ideally we'd like to get the registration link, but it's not present in the JSON-LD data

    # Check that venue and address extraction works for events that have it
    events_with_venue = [e for e in events if e.venue]
    assert len(
        events_with_venue) > 0, "Should have some events with venue information"

    events_with_address = [e for e in events if e.address]
    assert len(
        events_with_address) > 0, "Should have some events with address information"
