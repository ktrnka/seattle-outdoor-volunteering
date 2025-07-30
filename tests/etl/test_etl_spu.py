import json
from datetime import timezone
from pathlib import Path

from src.etl.spu import SPUExtractor
from src.etl.spu import SPUSourceEvent


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


def test_source_dict_structure():
    """Test that source_dict contains properly structured SPU data"""
    fixture_path = Path(__file__).parent / "data" / \
        "seattle_utilities_cleanup.html"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    extractor = SPUExtractor(html_content)
    events = extractor.extract()

    # Check the Othello event's source_dict
    othello_event = None
    for event in events:
        if "othello" in event.source_id.lower():
            othello_event = event
            break

    assert othello_event is not None, "Should find Othello event"
    assert othello_event.source_dict is not None, "Event should have source_dict"

    # Parse the JSON source_dict
    source_data = json.loads(othello_event.source_dict)

    # Verify it's a valid SPUSourceEvent structure
    spu_data = SPUSourceEvent(**source_data)

    # Check key fields are populated
    assert spu_data.date == "Saturday, August 9"
    assert spu_data.neighborhood == "Othello"
    assert spu_data.location == "Othello Park"
    assert spu_data.google_maps_link == "https://maps.app.goo.gl/YZyjvpu73oYC2hHj8"
    assert spu_data.start_time == "10 am"
    assert spu_data.end_time == "12 pm"


def test_source_dict_event_with_address():
    """Test source_dict for an event that has address information"""
    fixture_path = Path(__file__).parent / "data" / \
        "seattle_utilities_cleanup.html"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    extractor = SPUExtractor(html_content)
    events = extractor.extract()

    # Find the Lake City event which should have address info
    lake_city_event = None
    for event in events:
        if "lakecity" in event.source_id.lower():
            lake_city_event = event
            break

    assert lake_city_event is not None, "Should find Lake City event"
    assert lake_city_event.source_dict is not None, "Event should have source_dict"

    # Check the structured source data
    source_data = json.loads(lake_city_event.source_dict)
    spu_data = SPUSourceEvent(**source_data)

    # Should have date, neighborhood, location with address info
    assert spu_data.date == "Sunday, June 22"
    assert spu_data.neighborhood == "Lake City"
    assert "Akin Building" in spu_data.location
    assert "12360 Lake City Way NE" in spu_data.location
    assert spu_data.google_maps_link == "https://maps.app.goo.gl/dqunyY8FyiahfkFo8"

    # Check that the Event model has the correct parsed venue and address
    assert lake_city_event.venue == "Akin Building"
    assert lake_city_event.address == "12360 Lake City Way NE"


def test_extract_spu_source_event_directly():
    """Test the _extract_spu_source_event method directly"""
    fixture_path = Path(__file__).parent / "data" / \
        "seattle_utilities_cleanup.html"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    extractor = SPUExtractor(html_content)

    # We can't easily test the private method without parsing the HTML,
    # but we can verify that all events have proper source_dict
    events = extractor.extract()

    for event in events:
        assert event.source_dict is not None, f"Event {event.title} should have source_dict"

        # Verify source_dict can be parsed back to SPUSourceEvent
        source_data = json.loads(event.source_dict)
        spu_data = SPUSourceEvent(**source_data)

        # Basic validation of required fields
        assert spu_data.date, "Should have date"
        assert spu_data.neighborhood, "Should have neighborhood"
        assert spu_data.location, "Should have location"
        assert spu_data.start_time, "Should have start_time"


if __name__ == "__main__":
    test_spu_extractor()
    test_source_dict_structure()
    test_source_dict_event_with_address()
    test_extract_spu_source_event_directly()
