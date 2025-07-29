import json
from pathlib import Path
from pydantic import HttpUrl

from src.etl.dnda import DNDAExtractor
from src.models import SEATTLE_TZ

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
