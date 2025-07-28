# tests/test_etl_gsp.py
from pathlib import Path
from src.etl.gsp import GSPExtractor


def test_parse_fixture():
    html = Path("tests/fixtures/gsp_calendar.html").read_text()
    events = GSPExtractor(session=None).fetch(html=html)

    # sanity check - expect at least a few events
    assert len(events) >= 3
    assert all(e.source == "GSP" for e in events)
