# tests/test_etl_spr.py
import json
from pathlib import Path

from pydantic import HttpUrl

from src.etl.spr import SPRExtractor, SPRSourceData
from src.models import SEATTLE_TZ

data_path = Path(__file__).parent / "data"


def test_parse_fixture():
    """Test parsing the RSS fixture file"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # sanity check - expect at least a few events
    assert len(events) >= 3
    assert all(e.source == "SPR" for e in events)

    # Check that we get the expected fields
    first_event = events[0]
    assert first_event.title
    assert first_event.source_id
    assert first_event.start
    assert first_event.end
    assert first_event.url

    # Verify the first event matches our RSS fixture
    assert first_event.title == "Green Lake Litter Patrol"
    assert first_event.address == "7312 West Green Lake Dr N, Seattle, WA 98103"
    assert first_event.same_as is None  # No x-trumba:weblink for this event

    # Sunday, April 19, 2026, 8:45–11am
    local_start = first_event.start.astimezone(SEATTLE_TZ)
    assert local_start.year == 2026
    assert local_start.month == 4
    assert local_start.day == 19
    assert local_start.hour == 8  # 8am local time
    assert local_start.minute == 45

    local_end = first_event.end.astimezone(SEATTLE_TZ)
    assert local_end.hour == 11  # 11am local time
    assert local_end.minute == 0

    # Verify URL format
    assert "trumbaEmbed" in str(first_event.url)
    assert "180079866" in str(first_event.url)  # source_id should be in URL

    # Check tags/categories
    assert first_event.tags is not None
    assert any("Volunteer" in tag for tag in first_event.tags) or any("Work Party" in tag for tag in first_event.tags)
    assert "Green Lake Litter patrol" in first_event.tags

    # Verify venue extraction
    assert first_event.venue is not None
    assert "Green Lake Park" in first_event.venue


def test_parse_multiple_events():
    """Test that we parse multiple events correctly"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Should have multiple events
    assert len(events) >= 5

    # All should have SPR source
    assert all(e.source == "SPR" for e in events)

    # All should have unique source_ids
    source_ids = [e.source_id for e in events]
    assert len(source_ids) == len(set(source_ids))

    # Check a different event (Green Lake Litter Patrol)
    green_lake_event = next((e for e in events if "Green Lake Litter Patrol" in e.title), None)
    assert green_lake_event is not None
    assert green_lake_event.venue == "Green Lake Park"
    assert green_lake_event.address is not None
    assert "7312 West Green Lake Dr N, Seattle, WA 98103" in green_lake_event.address
    assert green_lake_event.cost == "Free"


def test_datetime_parsing():
    """Test that date and time parsing works correctly"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Check that start times are before end times
    for event in events:
        assert event.start < event.end

    # Check that all events have reasonable times (not in the distant past or future)
    for event in events:
        assert event.start.year >= 2025
        assert event.start.year <= 2026


def test_empty_rss():
    """Test handling of empty RSS content"""
    extractor = SPRExtractor("<?xml version='1.0'?><rss><channel></channel></rss>")
    events = extractor.extract()
    assert events == []


def test_malformed_rss():
    """Test handling of malformed RSS content"""
    extractor = SPRExtractor("not xml at all")
    events = extractor.extract()
    assert events == []


def test_source_dict_structure():
    """Test that source_dict contains properly structured SPR data"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Check the first event's source_dict
    first_event = events[0]
    assert first_event.source_dict is not None

    # Parse the JSON source_dict
    source_data = json.loads(first_event.source_dict)

    # Verify it's a valid SPRSourceData structure
    spr_data = SPRSourceData(**source_data)

    # Check key fields are populated
    assert spr_data.title == "Green Lake Litter Patrol"
    assert spr_data.location == "7312 West Green Lake Dr N, Seattle, WA 98103"
    assert spr_data.event_types == "Volunteer/Work Party"
    assert spr_data.parks == "Green Lake Park"
    assert spr_data.sponsoring_organization == "Green Lake Litter patrol"
    assert spr_data.contact == "G Todd Young"
    assert spr_data.contact_phone is not None
    assert spr_data.contact_email is not None
    assert spr_data.audience is not None
    assert "All" in spr_data.audience
    assert spr_data.pre_register == "No"
    assert spr_data.cost == "Free"

    # Check that description contains the main event text
    assert "Bath House Theater" in spr_data.description


def test_source_dict_green_lake_event():
    """Test source_dict for the Green Lake Litter Patrol event"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Find the Green Lake event
    green_lake_event = next((e for e in events if "Green Lake Litter Patrol" in e.title), None)
    assert green_lake_event is not None
    assert green_lake_event.source_dict is not None

    # Parse the JSON source_dict
    source_data = json.loads(green_lake_event.source_dict)
    spr_data = SPRSourceData(**source_data)

    # Check specific fields for this event
    assert spr_data.title == "Green Lake Litter Patrol"
    assert spr_data.location == "7312 West Green Lake Dr N, Seattle, WA 98103"
    assert spr_data.parks == "Green Lake Park"
    assert spr_data.contact == "G Todd Young"
    assert spr_data.contact_phone is not None
    assert spr_data.contact_email is not None
    assert spr_data.cost == "Free"
    assert spr_data.audience is not None
    assert "Adults, All, Children, Family, Pets, Senior, Special Needs, Teen" in spr_data.audience


def test_source_dict_all_events_have_structure():
    """Test that all parsed events have valid source_dict structure"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    for event in events:
        # Every event should have a source_dict
        assert event.source_dict is not None

        # It should be valid JSON
        source_data = json.loads(event.source_dict)

        # It should be a valid SPRSourceData structure
        spr_data = SPRSourceData(**source_data)

        # Basic required fields should be present
        assert spr_data.title is not None
        assert spr_data.title != ""


def test_extract_spr_source_data_directly():
    """Test that we can extract SPRSourceData directly from RSS items"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    import xml.etree.ElementTree as ET

    extractor = SPRExtractor(rss_content)
    root = ET.fromstring(rss_content)
    first_item = root.findall(".//item")[0]

    # Test the direct extraction method
    spr_data = extractor._extract_spr_source_data(first_item)

    # Verify it's a valid SPRSourceData instance
    assert isinstance(spr_data, SPRSourceData)
    assert spr_data.title == "Green Lake Litter Patrol"
    assert spr_data.location == "7312 West Green Lake Dr N, Seattle, WA 98103"
    assert spr_data.event_types == "Volunteer/Work Party"
    assert spr_data.sponsoring_organization == "Green Lake Litter patrol"
    assert spr_data.contact == "G Todd Young"
    assert spr_data.contact_phone is not None
    assert spr_data.link is None  # No 'more info' link for this event


def test_address_parsing_different_formats():
    """Test that address parsing works correctly for different RSS formats"""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Find events with different address formats

    # Format 1: Single line street address (standard format)
    standard_format_event = next((e for e in events if "Green Lake Litter Patrol" in e.title), None)
    assert standard_format_event is not None
    assert standard_format_event.address == "7312 West Green Lake Dr N, Seattle, WA 98103"

    # Format 2: Date-first format (no address in description, date is first line)
    date_first_event = next((e for e in events if "Sturtevant Ravine" in e.title), None)
    assert date_first_event is not None
    assert date_first_event.address is not None

    # Verify that both events have valid dates/times
    from src.models import SEATTLE_TZ

    standard_local = standard_format_event.start.astimezone(SEATTLE_TZ)
    date_first_local = date_first_event.start.astimezone(SEATTLE_TZ)

    assert standard_local.hour == 8  # 8:45am rounds to hour 8
    assert standard_local.minute == 45
    assert date_first_local.hour == 9  # 9am


def test_skips_events_without_parseable_time():
    """Test that events with no parseable time (e.g. date-only descriptions) are skipped gracefully."""
    rss_content = (data_path / "spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Syttende Mai Parade has a date-only line (no time), so it should be skipped
    syttende_mai = next((e for e in events if "Syttende" in e.title), None)
    assert syttende_mai is None, "Event with unparseable datetime should be skipped"

    # Other events should still be extracted
    assert len(events) >= 3


def test_clean_html():
    """Test the _clean_html static method directly"""
    # Test HTML tag removal
    assert SPRExtractor._clean_html("<b>Bold text</b>") == "Bold text"
    assert SPRExtractor._clean_html('<a href="mailto:test@example.com">Email</a>') == "Email"

    # Test HTML entity decoding
    assert SPRExtractor._clean_html("&amp;") == "&"
    assert SPRExtractor._clean_html("&lt;script&gt;") == "<script>"
    assert SPRExtractor._clean_html("Hello&nbsp;World") == "Hello World"  # Test nbsp in context
    assert SPRExtractor._clean_html("&ndash;") == "–"

    # Test complex example (similar to RSS content)
    complex_html = "Sunday, July 27, 2025, 8&amp;nbsp;&amp;ndash;&amp;nbsp;11am"
    expected = "Sunday, July 27, 2025, 8 – 11am"
    assert SPRExtractor._clean_html(complex_html) == expected

    # Test stripping whitespace
    assert SPRExtractor._clean_html("  <p>Text</p>  ") == "Text"


def test_find_datetime_line():
    """Test the _find_datetime_line static method directly"""
    # Test standard datetime format
    lines = ["5921 Aurora Ave N, Seattle, WA 98103", "Sunday, July 27, 2025, 8 – 11am", "Join us for a restoration work party..."]
    result = SPRExtractor._find_datetime_line(lines)
    assert result is not None
    datetime_line, index = result
    assert index == 1
    assert "2025" in datetime_line
    assert "11am" in datetime_line

    # Test multi-line address format
    lines = [
        "Ballard Corners Park",
        "1702 NW 62nd St",
        "Seattle, WA 98107",
        "Saturday, August 2, 2025, 10am – 2pm",
        "We'll be weeding, grooming plants...",
    ]
    result = SPRExtractor._find_datetime_line(lines)
    assert result is not None
    datetime_line, index = result
    assert index == 3
    assert "2025" in datetime_line
    assert "10am" in datetime_line

    # Test no datetime line found
    lines = ["Some address", "Just description text", "No date or time here"]
    result = SPRExtractor._find_datetime_line(lines)
    assert result is None

    # Test line with time but no year (should not match)
    lines = ["Some address", "Meeting at 9am", "More description"]
    result = SPRExtractor._find_datetime_line(lines)
    assert result is None
