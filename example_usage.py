#!/usr/bin/env python3
"""
Example of how to use the refactored extractors.

Shows both live fetching and parsing from saved files.
"""

from pathlib import Path
from src.etl.gsp import GSPExtractor
from src.etl.spf import SPFExtractor
from src.etl.spr import SPRExtractor


def example_live_fetch():
    """Example of live fetching from websites."""
    print("=== Live Fetching Example ===")

    # For GSP: fetch from live website and extract events
    try:
        gsp_extractor = GSPExtractor.fetch()
        gsp_events = gsp_extractor.extract()
        print(f"GSP: Found {len(gsp_events)} events from live website")
        if gsp_events:
            print(f"  First event: {gsp_events[0].title}")
    except Exception as e:
        print(f"GSP fetch failed: {e}")

    # For SPF: fetch from live website and extract events
    try:
        spf_extractor = SPFExtractor.fetch()
        spf_events = spf_extractor.extract()
        print(f"SPF: Found {len(spf_events)} events from live website")
        if spf_events:
            print(f"  First event: {spf_events[0].title}")
    except Exception as e:
        print(f"SPF fetch failed: {e}")

    # For SPR: fetch from live RSS feed and extract events
    try:
        spr_extractor = SPRExtractor.fetch()
        spr_events = spr_extractor.extract()
        print(f"SPR: Found {len(spr_events)} events from live RSS")
        if spr_events:
            print(f"  First event: {spr_events[0].title}")
    except Exception as e:
        print(f"SPR fetch failed: {e}")


def example_file_parsing():
    """Example of parsing from saved test fixture files."""
    print("\n=== File Parsing Example ===")

    # GSP: parse from saved HTML file
    try:
        html_content = Path("tests/fixtures/gsp_calendar.html").read_text()
        gsp_extractor = GSPExtractor(html_content)
        gsp_events = gsp_extractor.extract()
        print(f"GSP: Found {len(gsp_events)} events from saved HTML file")
        if gsp_events:
            print(f"  First event: {gsp_events[0].title}")
    except Exception as e:
        print(f"GSP file parsing failed: {e}")

    # SPF: parse from saved HTML file
    try:
        html_content = Path("tests/fixtures/spf_events.html").read_text()
        spf_extractor = SPFExtractor(html_content)
        spf_events = spf_extractor.extract()
        print(f"SPF: Found {len(spf_events)} events from saved HTML file")
        if spf_events:
            print(f"  First event: {spf_events[0].title}")
    except Exception as e:
        print(f"SPF file parsing failed: {e}")

    # SPR: parse from saved RSS file
    try:
        rss_content = Path("tests/fixtures/spr_volunteer.rss").read_text()
        spr_extractor = SPRExtractor(rss_content)
        spr_events = spr_extractor.extract()
        print(f"SPR: Found {len(spr_events)} events from saved RSS file")
        if spr_events:
            print(f"  First event: {spr_events[0].title}")
    except Exception as e:
        print(f"SPR file parsing failed: {e}")


def main():
    # Show file parsing (always works with test fixtures)
    example_file_parsing()

    # Show live fetching (may fail if network is down)
    example_live_fetch()


if __name__ == "__main__":
    main()
