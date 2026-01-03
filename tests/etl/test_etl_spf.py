# tests/test_etl_spf.py
import json
from pathlib import Path

from pydantic import HttpUrl

from src.etl.spf import SPFExtractor, SPFSourceEvent
from src.models import SEATTLE_TZ

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
    assert first_event.url == HttpUrl("https://www.seattleparksfoundation.org/event/pigeon-point-park-restoration-event-41")

    # "startDate":"2025-07-29T10:00:00-07:00","endDate":"2025-07-29T13:00:00-07:00"

    local_start = first_event.start.astimezone(SEATTLE_TZ)
    assert local_start.year == 2025
    assert local_start.month == 7
    assert local_start.day == 29
    assert local_start.hour == 10  # 10am local time
    assert local_start.minute == 0

    # Ideally we'd like to get the registration link, but it's not present in the JSON-LD data

    # Check that venue and address extraction works for events that have it
    events_with_venue = [e for e in events if e.venue]
    assert len(events_with_venue) > 0, "Should have some events with venue information"

    events_with_address = [e for e in events if e.address]
    assert len(events_with_address) > 0, "Should have some events with address information"


def test_source_dict_structure():
    """Test that source_dict contains properly structured SPF data"""
    html = (data_path / "spf_events.html").read_text()
    extractor = SPFExtractor(html)
    events = extractor.extract()

    # Check the first event's source_dict
    first_event = events[0]
    assert first_event.source_dict is not None

    # Parse the JSON source_dict
    source_data = json.loads(first_event.source_dict)

    # Verify it's a valid SPFSourceEvent structure
    spf_data = SPFSourceEvent(**source_data)

    # Check key fields are populated
    assert spf_data.name == "Pigeon Point Park Restoration Event"
    assert spf_data.url == "https://www.seattleparksfoundation.org/event/pigeon-point-park-restoration-event-41/"
    assert spf_data.start_date == "2025-07-29T10:00:00-07:00"
    assert spf_data.end_date == "2025-07-29T13:00:00-07:00"
    assert spf_data.event_attendance_mode == "https://schema.org/OfflineEventAttendanceMode"
    assert spf_data.event_status == "https://schema.org/EventScheduled"

    # Check organizer information
    assert spf_data.organizer is not None
    assert spf_data.organizer.name == "Green Seattle Partnership"
    assert spf_data.organizer.url == "https://greenseattle.org"

    # Check that description contains content
    assert spf_data.description is not None
    assert "restoration work party" in spf_data.description


def test_source_dict_event_with_location():
    """Test source_dict for an event that has location information"""
    html = (data_path / "spf_events.html").read_text()
    extractor = SPFExtractor(html)
    events = extractor.extract()

    # Find an event with location data
    location_event = None
    for event in events:
        if event.source_dict:
            source_data = json.loads(event.source_dict)
            spf_data = SPFSourceEvent(**source_data)
            if spf_data.location:
                location_event = spf_data
                break

    assert location_event is not None, "Should have at least one event with location data"
    assert location_event.location is not None, "Event should have location data"

    # Check location structure
    assert location_event.location.name is not None

    # Check that address is now a structured SPFAddress object, not a dict
    if location_event.location.address:
        # Should be an SPFAddress object with proper attributes
        address = location_event.location.address
        assert hasattr(address, "street_address")
        assert hasattr(address, "address_locality")
        assert hasattr(address, "address_region")
        assert hasattr(address, "postal_code")

        # Check that at least some address fields are populated
        address_fields = [address.street_address, address.address_locality, address.address_region, address.postal_code]
        assert any(field for field in address_fields), "At least one address field should be populated"


def test_address_parsing_specific_event():
    """Test that address parsing works correctly for a specific event"""
    html = (data_path / "spf_events.html").read_text()
    extractor = SPFExtractor(html)
    events = extractor.extract()

    # Look for the "2nd Saturday Work Party in Volunteer Park" event
    # which should have address: "1247 15th Ave E, Seattle, WA, 98112"
    volunteer_park_event = None
    for event in events:
        if "Volunteer Park" in event.title and "Work Party" in event.title:
            volunteer_park_event = event
            break

    assert volunteer_park_event is not None, "Should find the Volunteer Park work party event"
    assert volunteer_park_event.source_dict is not None, "Event should have source_dict"

    # Check the structured source data
    source_data = json.loads(volunteer_park_event.source_dict)
    spf_data = SPFSourceEvent(**source_data)

    assert spf_data.location is not None
    assert spf_data.location.address is not None

    address = spf_data.location.address
    assert address.street_address == "1247 15th Ave E"
    assert address.address_locality == "Seattle"
    assert address.address_region == "WA"
    assert address.postal_code == "98112"
    assert address.address_country == "United States"

    # Check that the Event model has the correct formatted address
    assert volunteer_park_event.address == "1247 15th Ave E, Seattle, WA"


def test_source_dict_all_events_have_structure():
    """Test that all parsed events have valid source_dict structure"""
    html = (data_path / "spf_events.html").read_text()
    extractor = SPFExtractor(html)
    events = extractor.extract()

    for event in events:
        # Every event should have a source_dict
        assert event.source_dict is not None

        # It should be valid JSON
        source_data = json.loads(event.source_dict)

        # It should be a valid SPFSourceEvent structure
        spf_data = SPFSourceEvent(**source_data)

        # Basic required fields should be present
        assert spf_data.name is not None
        assert spf_data.name != ""
        assert spf_data.url is not None
        assert spf_data.url != ""
        assert spf_data.start_date is not None
        assert spf_data.end_date is not None


def test_extract_spf_source_event_directly():
    """Test that we can extract SPFSourceEvent directly from JSON-LD data"""
    html = (data_path / "spf_events.html").read_text()
    import json

    from bs4 import BeautifulSoup

    extractor = SPFExtractor(html)
    soup = BeautifulSoup(html, "html.parser")

    # Find the first JSON-LD script with event data
    json_scripts = soup.find_all("script", type="application/ld+json")
    event_data = None

    for script in json_scripts:
        try:
            data = json.loads(script.get_text())
            if isinstance(data, list):
                event_list = data
            else:
                event_list = [data]

            for item in event_list:
                if item.get("@type") == "Event":
                    event_data = item
                    break

            if event_data:
                break
        except json.JSONDecodeError:
            continue

    assert event_data is not None, "Should find at least one event in JSON-LD data"

    # Test the direct extraction method
    spf_event = extractor._extract_spf_source_event(event_data)

    # Verify it's a valid SPFSourceEvent instance
    assert isinstance(spf_event, SPFSourceEvent)
    assert spf_event.name == "Pigeon Point Park Restoration Event"
    assert spf_event.url == "https://www.seattleparksfoundation.org/event/pigeon-point-park-restoration-event-41/"
    assert spf_event.start_date == "2025-07-29T10:00:00-07:00"


def test_detail_page_extractor():
    """Test extraction of enrichment data from SPF detail page using CSS selectors"""
    from src.etl.spf import SPFDetailEnrichment, SPFDetailExtractor

    html = (data_path / "spf_detail_page.html").read_text()
    detail_url = "https://www.seattleparksfoundation.org/event/scotch-broom-patrol-4"

    extractor = SPFDetailExtractor(detail_url, html)
    enrichment_data = extractor.extract()

    # Should return SPFDetailEnrichment model
    assert isinstance(enrichment_data, SPFDetailEnrichment)

    # Should extract the website URL from span.tribe-events-event-url > a
    assert enrichment_data.website_url is not None
    # URL should be normalized (http->https, trailing slash removed)
    assert enrichment_data.website_url == "https://seattle.greencitypartnerships.org/event/42741"
