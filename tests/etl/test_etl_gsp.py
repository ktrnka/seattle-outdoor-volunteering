# tests/test_etl_gsp.py
from pathlib import Path

from pydantic import HttpUrl
from src.etl.gsp import GSPDetailEvent, GSPDetailPageExtractor, GSPExtractor, GSPCalendarExtractor, GSPAPIExtractor
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


def test_detail_extractor():
    """Test that the GSPDetailPageExtractor can extract event details."""
    html = (data_path / "gsp_detail_page.html").read_text()
    url = "https://seattle.greencitypartnerships.org/event/42093"

    extractor = GSPDetailPageExtractor(HttpUrl(url), html)
    detailed_event = extractor.extract_detail_event()

    assert detailed_event == GSPDetailEvent(
        title="Schmitz Preserve Park - Schmitz Park / Sprucing …08/01/2025",
        url=HttpUrl(url),
        source_id="42093",
        datetimes="August 1, 2025 9:30am - 11:30am",
        description="This is a newly adopted forest steward parcel, just down the trail from the Stevens St. trailhead along the Adams Highway section of the park. This will be our third visit since adopting the parcel. So far we've removed ivy, blackberry, and other invasives and continue to remove legacy trash that we uncover. This trip we'll dig into an area that is overgrown with Himalayan blackberry and make piles to decompose on site.",
        contact_name='Erik Bell',
        contact_email='erik.belltribe@gmail.com'
    )

    event = detailed_event.to_source_event()
    assert event.title == "Schmitz Preserve Park - Schmitz Park / Sprucing …08/01/2025"
    assert event.url == HttpUrl(url)
    assert event.source_id == "42093"

    assert event.start_local.year == 2025
    assert event.start_local.month == 8
    assert event.start_local.day == 1
    assert event.start_local.hour == 9
    assert event.start_local.minute == 30

    assert event.end_local.year == 2025
    assert event.end_local.month == 8
    assert event.end_local.day == 1
    assert event.end_local.hour == 11
    assert event.end_local.minute == 30
