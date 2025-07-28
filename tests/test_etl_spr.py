# tests/test_etl_spr.py
from pathlib import Path
from src.etl.spr import SPRExtractor


def test_parse_fixture():
    """Test parsing the RSS fixture file"""
    rss_content = Path("tests/fixtures/spr_volunteer.rss").read_text()
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
    assert first_event.title == "Preparing for Fall Planting"
    assert first_event.address == "5921 Aurora Ave N, Seattle, WA 98103"
    assert first_event.start.year == 2025
    assert first_event.start.month == 7
    assert first_event.start.day == 27
    assert first_event.start.hour == 8  # 8am local time
    assert first_event.end.hour == 11  # 11am local time
    assert first_event.same_as == "http://seattle.greencitypartnerships.org/event/42030"

    # Verify URL format
    assert "trumbaEmbed" in str(first_event.url)
    assert "187593769" in str(first_event.url)  # source_id should be in URL

    # Check tags/categories
    assert first_event.tags is not None
    assert any("Volunteer" in tag for tag in first_event.tags) or any(
        "Work Party" in tag for tag in first_event.tags)
    assert "Green Seattle Partnership" in first_event.tags

    # Verify venue extraction
    assert first_event.venue is not None
    assert "Woodland Park" in first_event.venue


def test_parse_multiple_events():
    """Test that we parse multiple events correctly"""
    rss_content = Path("tests/fixtures/spr_volunteer.rss").read_text()
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
    green_lake_event = next(
        (e for e in events if "Green Lake Litter Patrol" in e.title), None)
    assert green_lake_event is not None
    assert green_lake_event.venue == "Green Lake Park"
    assert green_lake_event.address is not None
    assert "7312 West Green Lake Dr N, Seattle, WA 98103" in green_lake_event.address
    assert green_lake_event.cost == "Free"


def test_datetime_parsing():
    """Test that date and time parsing works correctly"""
    rss_content = Path("tests/fixtures/spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Check that start times are before end times
    for event in events:
        assert event.start < event.end

    # Check that all events have reasonable times (not in the distant past or future)
    for event in events:
        assert event.start.year >= 2025
        assert event.start.year <= 2026


def test_contact_info_extraction():
    """Test that contact information is extracted properly"""
    rss_content = Path("tests/fixtures/spr_volunteer.rss").read_text()
    extractor = SPRExtractor(rss_content)
    events = extractor.extract()

    # Find an event with contact info
    contact_event = next(
        (e for e in events if "Greg Netols" in str(e.tags)), None)
    assert contact_event is not None
    assert "gregnetols@gmail.com" in str(contact_event.tags)
    assert "2243889145" in str(contact_event.tags)


def test_empty_rss():
    """Test handling of empty RSS content"""
    extractor = SPRExtractor(
        "<?xml version='1.0'?><rss><channel></channel></rss>")
    events = extractor.extract()
    assert events == []


def test_malformed_rss():
    """Test handling of malformed RSS content"""
    extractor = SPRExtractor("not xml at all")
    events = extractor.extract()
    assert events == []
