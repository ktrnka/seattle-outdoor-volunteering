# tests/test_etl_gsp.py
from pathlib import Path

from pydantic import HttpUrl
from src.etl.gsp import GSPExtractor, GSPCalendarExtractor, GSPAPIExtractor
from src.models import SEATTLE_TZ

data_path = Path(__file__).parent / "data"


def test_parse_calendar_fixture():
    """Test parsing the calendar HTML page fixture."""
    html = (data_path / "gsp_calendar.html").read_text()
    extractor = GSPCalendarExtractor(html)
    events = extractor.extract()

    # sanity check - expect at least a few events
    assert len(events) >= 3
    assert all(e.source == "GSP" for e in events)

    # Basic checks on the first event
    first_event = events[0]
    assert first_event.title == "Weeding south of 70th St. again"
    assert first_event.url == HttpUrl(
        "https://seattle.greencitypartnerships.org/event/42093")

    # July 28, 9am-12:30pm @ Burke-Gilman Trail in local time
    local_start = first_event.start.astimezone(SEATTLE_TZ)
    assert local_start.year == 2025
    assert local_start.month == 7
    assert local_start.day == 28
    assert local_start.hour == 9  # 9am local time
    assert local_start.minute == 0

    local_end = first_event.end.astimezone(SEATTLE_TZ)
    assert local_end.hour == 12  # 12:30pm local time
    assert local_end.minute == 30


def test_parse_api_fixture():
    """Test parsing the new API endpoint response with 100 events."""
    json_data = (data_path / "gsp_api_100.json").read_text()
    extractor = GSPAPIExtractor(json_data)
    events = extractor.extract()

    # Should get many more events from the API
    assert len(events) >= 50  # API returned 66 events
    assert all(e.source == "GSP" for e in events)

    # Check first event from API response
    first_event = events[0]
    assert first_event.title == "Weeding south of 70th St. again"
    assert first_event.venue == "Burke-Gilman Trail"
    assert first_event.start.year == 2025
    assert first_event.start.month == 7
    assert first_event.start.day == 28
    assert first_event.source_id == "42093"
    assert first_event.url == HttpUrl(
        "https://seattle.greencitypartnerships.org/event/42093")


def test_main_extractor_delegates_to_api():
    """Test that the main GSPExtractor properly delegates to API extractor when given JSON."""
    json_data = (data_path / "gsp_api_100.json").read_text()
    extractor = GSPExtractor(json_data)
    events = extractor.extract()

    # Should behave the same as GSPAPIExtractor
    assert len(events) >= 50
    assert all(e.source == "GSP" for e in events)


def test_main_extractor_delegates_to_calendar():
    """Test that the main GSPExtractor properly delegates to calendar extractor when given HTML."""
    html = (data_path / "gsp_calendar.html").read_text()
    extractor = GSPExtractor(html)
    events = extractor.extract()

    # Should behave the same as GSPCalendarExtractor
    assert len(events) >= 3
    assert all(e.source == "GSP" for e in events)
