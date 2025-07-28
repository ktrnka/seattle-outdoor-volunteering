# tests/test_etl_gsp.py
from pathlib import Path

from pydantic import HttpUrl
from src.etl.gsp import GSPExtractor


def test_parse_fixture():
    html = Path("tests/fixtures/gsp_calendar.html").read_text()
    extractor = GSPExtractor(html)
    events = extractor.extract()

    # sanity check - expect at least a few events
    assert len(events) >= 3
    assert all(e.source == "GSP" for e in events)

    # Basic checks on the first event
    first_event = events[0]
    assert first_event.title == "Weeding south of 70th St. again"
    assert first_event.start.year == 2025
    assert first_event.start.month == 7
    assert first_event.start.day == 28
    assert first_event.url == HttpUrl(
        "https://seattle.greencitypartnerships.org/event/42093")
