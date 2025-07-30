import json
from pathlib import Path
from pydantic import HttpUrl

from src.etl.dnda import DNDAExtractor
from src.models import SEATTLE_TZ, DNDASourceEvent

data_path = Path(__file__).parent / "data"


def test_parse_volunteer_events_basic():
    """Test basic parsing of DNDA volunteer events from JSON fixture."""
    json_data = (data_path / "dnda_volunteer_events.json").read_text()
    extractor = DNDAExtractor(json_data)
    events = extractor.extract()

    # Should get all 4 volunteer events
    assert len(events) == 4
    assert all(e.source == "DNDA" for e in events)

    # Check first event - Volunteer Wetland Restoration
    first_event = events[0]
    assert first_event.title == "Volunteer Wetland Restoration"
    assert first_event.source_id == "3400"
    assert first_event.url == HttpUrl(
        "https://dnda.org/calendar/volunteer-wetland-restoration")
    assert first_event.address == "5601 23rd Ave SW, Seattle, WA 98106"

    # Check that times are properly converted to UTC
    # Original: 2025-07-29T10:00:00-07:00 (Pacific) -> should be 17:00 UTC
    local_start = first_event.start.astimezone(SEATTLE_TZ)
    assert local_start.year == 2025
    assert local_start.month == 7
    assert local_start.day == 29
    assert local_start.hour == 10  # 10am Pacific
    assert local_start.minute == 0

    # Event duration: 3 hours (10am-1pm Pacific)
    local_end = first_event.end.astimezone(SEATTLE_TZ)
    assert local_end.hour == 13  # 1pm Pacific
    assert local_end.minute == 0


def test_source_dict_structure():
    """Test that source_dict contains properly structured DNDA data"""
    json_data = (data_path / "dnda_volunteer_events.json").read_text()
    extractor = DNDAExtractor(json_data)
    events = extractor.extract()

    # Check the first event's source_dict
    first_event = events[0]
    assert first_event.source_dict is not None

    # Parse the JSON source_dict
    source_data = json.loads(first_event.source_dict)

    # Verify it's a valid DNDASourceEvent structure
    dnda_data = DNDASourceEvent(**source_data)

    # Check key fields are populated
    assert dnda_data.id == 3400
    assert dnda_data.title == "Volunteer Wetland Restoration"
    assert dnda_data.start == "2025-07-29T10:00:00-07:00"
    assert dnda_data.end == "2025-07-29T13:00:00-07:00"
    assert dnda_data.url == "https://dnda.org/calendar/volunteer-wetland-restoration/"
    assert dnda_data.location == "5601 23rd Ave SW, Seattle, WA 98106"
    assert dnda_data.start_date == "July 29, 2025"
    assert dnda_data.start_time == "10:00 am"
    assert dnda_data.end_time == "1:00 pm"
    assert dnda_data.start_day == "Tuesday"

    # Check that rich data is preserved
    assert dnda_data.image == "https://dnda.org/wp-content/uploads/2025/05/Delridge-Wetlands-Park-Image23.jpg"
    assert dnda_data.background_color == "#a6c968"
    assert dnda_data.description is not None
    assert "restoration work" in dnda_data.description
    assert "Location:" in dnda_data.description


def test_source_dict_all_events_have_structure():
    """Test that all parsed events have valid source_dict structure"""
    json_data = (data_path / "dnda_volunteer_events.json").read_text()
    extractor = DNDAExtractor(json_data)
    events = extractor.extract()

    for event in events:
        # Every event should have a source_dict
        assert event.source_dict is not None

        # Verify source_dict can be parsed back to DNDASourceEvent
        source_data = json.loads(event.source_dict)
        dnda_data = DNDASourceEvent(**source_data)

        # Basic validation of required fields
        assert dnda_data.id > 0, "Should have valid event ID"
        assert dnda_data.title, "Should have title"
        assert dnda_data.start, "Should have start time"
        assert dnda_data.end, "Should have end time"
        assert dnda_data.url, "Should have URL"


def test_extract_dnda_source_event_directly():
    """Test the _extract_dnda_source_event method with specific data"""
    test_event_data = {
        "id": 7956,
        "title": "Forest Restoration at Pigeon Point Park",
        "start": "2025-08-09T10:00:00-07:00",
        "end": "2025-08-09T13:00:00-07:00",
        "startStr": 1754733600,
        "endStr": 1754744400,
        "image": "https://dnda.org/wp-content/uploads/2024/05/Volunteer-Event-Banner.png",
        "url": "https://dnda.org/calendar/pigeon-point-park-462-788/",
        "backgroundColor": "#a6c968",
        "borderColor": "#a6c968",
        "location": "1901 SW Genesee St, Seattle, WA, 98122",
        "start_date": "August 9, 2025",
        "start_time": "10:00 am",
        "end_date": "August 9, 2025",
        "end_time": "1:00 pm",
        "startDay": "Saturday"
    }

    # Empty data, we're testing the method directly
    extractor = DNDAExtractor("{}")
    dnda_event = extractor._extract_dnda_source_event(test_event_data)

    assert dnda_event is not None
    assert dnda_event.id == 7956
    assert dnda_event.title == "Forest Restoration at Pigeon Point Park"
    assert dnda_event.location == "1901 SW Genesee St, Seattle, WA, 98122"
    assert dnda_event.start_day == "Saturday"
    assert dnda_event.background_color == "#a6c968"


def test_volunteer_event_filtering():
    """Test that only volunteer-related events are extracted."""
    # Create test data with mixed event types
    test_data = [
        {
            "id": 1,
            "title": "Volunteer Wetland Restoration",
            "start": "2025-07-29T10:00:00-07:00",
            "end": "2025-07-29T13:00:00-07:00",
            "url": "https://dnda.org/calendar/volunteer-wetland-restoration/",
            "location": "Some Park"
        },
        {
            "id": 2,
            "title": "Dance Fitness Class",
            "start": "2025-07-29T18:00:00-07:00",
            "end": "2025-07-29T19:00:00-07:00",
            "url": "https://dnda.org/calendar/dance-fitness/",
            "location": "Studio"
        },
        {
            "id": 3,
            "title": "Forest Restoration at Camp Long",
            "start": "2025-08-05T10:00:00-07:00",
            "end": "2025-08-05T13:00:00-07:00",
            "url": "https://dnda.org/calendar/forest-restoration/",
            "location": "Camp Long"
        }
    ]

    extractor = DNDAExtractor(json.dumps(test_data))
    events = extractor.extract()

    # Should only get the 2 volunteer events, not the dance class
    assert len(events) == 2
    titles = [e.title for e in events]
    assert "Volunteer Wetland Restoration" in titles
    assert "Forest Restoration at Camp Long" in titles
    assert "Dance Fitness Class" not in titles


def test_venue_extraction():
    """Test venue extraction from descriptions and locations."""
    # Test data with venue information in description
    test_data = [
        {
            "id": 1,
            "title": "Park Cleanup",
            "start": "2025-07-29T10:00:00-07:00",
            "end": "2025-07-29T13:00:00-07:00",
            "url": "https://dnda.org/calendar/cleanup/",
            "location": "5601 23rd Ave SW, Seattle, WA 98106",
            "description": "<p><strong>Location: </strong><a href=\"https://maps.app.goo.gl/2CTqPGMiUiN1F7uT9\" target=\"_blank\" rel=\"noopener\">Delridge Wetland Park</a></p>"
        },
        {
            "id": 2,
            "title": "Forest Restoration",
            "start": "2025-08-05T10:00:00-07:00",
            "end": "2025-08-05T13:00:00-07:00",
            "url": "https://dnda.org/calendar/restoration/",
            "location": "1901 SW Genesee St, Seattle, WA, 98122",
            "description": "<p>Work at Pigeon Point Park</p>"
        }
    ]

    extractor = DNDAExtractor(json.dumps(test_data))
    events = extractor.extract()

    assert len(events) == 2

    # First event should extract venue from description link
    assert events[0].venue == "Delridge Wetland Park"

    # Second event should extract venue from description text
    assert events[1].venue == "Pigeon Point Park"
