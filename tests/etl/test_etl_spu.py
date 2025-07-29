from datetime import timezone
from pathlib import Path

from src.etl.spu import SPUExtractor


def test_spu_extractor():
    """Test SPU extractor with real HTML data."""
    # Load the test fixture
    fixture_path = Path(__file__).parent / "data" / \
        "seattle_utilities_cleanup.html"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Create extractor with the fixture data
    extractor = SPUExtractor(html_content)
    events = extractor.extract()

    # Basic validation
    assert len(events) > 0, "Should extract at least one event"

    # Check specific event we know exists (Saturday, August 9 - Othello)
    othello_events = [e for e in events if "othello" in e.source_id.lower()]
    assert len(
        othello_events) == 1, f"Should find exactly one Othello event, found {len(othello_events)}"

    othello_event = othello_events[0]
    assert othello_event.source == "SPU"
    assert "Othello" in othello_event.title
    assert "All Hands Neighborhood Cleanup" in othello_event.title
    assert othello_event.venue == "Othello Park"
    assert str(
        othello_event.url) == "https://www.seattle.gov/utilities/volunteer/all-hands-neighborhood-cleanup"
    assert othello_event.tags is not None
    assert "cleanup" in othello_event.tags
    assert "neighborhood" in othello_event.tags
    assert "utilities" in othello_event.tags

    # Check that start and end times are parsed correctly
    assert othello_event.start < othello_event.end, "Start should be before end"
    assert othello_event.start.tzinfo == timezone.utc, "Times should be in UTC"
    assert othello_event.end.tzinfo == timezone.utc, "Times should be in UTC"

    # Check source_id format (should be like "2025-08-09-othello")
    assert "-othello" in othello_event.source_id
    assert othello_event.source_id.count("-") == 3  # YYYY-MM-DD-neighborhood

    # Test event with address in parentheses (Lake City event)
    lake_city_events = [e for e in events if "lakecity" in e.source_id.lower()]
    if lake_city_events:
        lake_city_event = lake_city_events[0]
        assert lake_city_event.address == "12360 Lake City Way NE"
        assert lake_city_event.venue == "Akin Building"

    print(f"Successfully extracted {len(events)} events")
    for event in events:
        print(
            f"  - {event.title} at {event.venue} on {event.start.strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    test_spu_extractor()
