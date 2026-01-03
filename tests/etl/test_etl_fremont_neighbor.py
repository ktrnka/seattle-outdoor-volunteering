import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from time import tzset

import pytest
from dotenv import load_dotenv

from src.etl.fremont_neighbor import FremontNeighborExtractor, generate_source_id, strip_html_tags
from src.llm.blog_event_extractor import extract_articles


def test_fremont_neighbor_parse_rss_structure():
    """Test parsing RSS structure without LLM calls."""
    test_data_path = Path(__file__).parent / "data" / "fremont_neighbor_rss.xml"
    with open(test_data_path, "r", encoding="utf-8") as f:
        rss_content = f.read()

    extractor = FremontNeighborExtractor(rss_content)

    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    assert len(items) == 10

    # Test parsing the first few items to see the variety
    for i, item in enumerate(items[:5]):  # Check first 5 items
        article = extractor._parse_rss_item(item)
        assert article is not None
        print(f"\nArticle {i + 1}: {article.title}")
        print(f"  Categories: {article.categories}")
        print(f"  Has 'Volunteering' category: {'Volunteering' in article.categories}")

        # Show description with character count
        desc_preview = article.description[:200]
        desc_remaining = len(article.description) - len(desc_preview)
        print(f"  Description ({len(article.description)} chars): {desc_preview}{'...' if desc_remaining > 0 else ''}")
        if desc_remaining > 0:
            print(f"    ({desc_remaining} more chars)")

        # Show content with character count
        content_preview = article.content[:200]
        content_remaining = len(article.content) - len(content_preview)
        print(f"  Content ({len(article.content)} chars): {content_preview}{'...' if content_remaining > 0 else ''}")
        if content_remaining > 0:
            print(f"    ({content_remaining} more chars)")

    # Test parsing the first item specifically
    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert article.link
    assert article.pub_date

    # Check if the first article is about the A.B. Ernst Park Cleanup
    assert "A.B. Ernst Park Cleanup" in article.title
    assert "Volunteering" in article.categories


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction():
    """Test LLM extraction on the first article (A.B. Ernst Park Cleanup)."""
    # Load LLM key
    load_dotenv()

    test_data_path = Path(__file__).parent / "data" / "fremont_neighbor_rss.xml"
    with open(test_data_path, "r", encoding="utf-8") as f:
        rss_content = f.read()

    extractor = FremontNeighborExtractor(rss_content)

    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert "A.B. Ernst Park Cleanup" in article.title
    assert "Volunteering" in article.categories

    print(f"\nTesting LLM extraction on: {article.title}")
    print(f"Description: {article.description}")

    print(f"Cleaned content preview: {article.content[:500]}...")

    # Extract events using LLM
    extracted_events = extract_articles(article.title, str(article.pub_date), strip_html_tags(article.content))

    print(f"\nExtracted {len(extracted_events)} events:")
    for i, event in enumerate(extracted_events, 1):
        print(f"\nEvent {i}:")
        print(event.model_dump_json(indent=2))

    # Based on the RSS content, we expect Aug 10 and Aug 16
    assert len(extracted_events) == 2

    # Check that we have both dates
    event_dates = [event.event_date for event in extracted_events]
    print(f"\nEvent dates found: {event_dates}")

    extracted_events = sorted(extracted_events, key=lambda e: e.event_date)

    assert extracted_events[0].event_date == date(2025, 8, 10)
    assert extracted_events[0].start_datetime.date() == date(2025, 8, 10)
    assert extracted_events[1].event_date == date(2025, 8, 16)
    assert extracted_events[1].start_datetime.date() == date(2025, 8, 16)

    assert generate_source_id(article.guid, extracted_events[0].event_date) == "683_2025-08-10"
    assert generate_source_id(article.guid, extracted_events[1].event_date) == "683_2025-08-16"


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction_no_events():
    """Test LLM extraction on a non-volunteer article (should return zero events)."""
    load_dotenv()

    test_data_path = Path(__file__).parent / "data" / "fremont_neighbor_rss.xml"
    with open(test_data_path, "r", encoding="utf-8") as f:
        rss_content = f.read()

    # Create extractor instance and get the third article (food and drink business news)
    extractor = FremontNeighborExtractor(rss_content)

    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    # Get the third article (index 2) - "Fremont food and drink: Closures, openings"
    third_item = items[2]
    article = extractor._parse_rss_item(third_item)

    assert article is not None
    assert "food and drink" in article.title.lower()
    assert "Volunteering" not in article.categories
    assert "Business" in article.categories

    print(f"\nTesting LLM extraction on: {article.title}")
    print(f"Categories: {article.categories}")
    print(f"Description: {article.description}")

    print(f"Cleaned content preview: {article.content[:500]}...")

    # Extract events using LLM
    extracted_events = extract_articles(article.title, str(article.pub_date), strip_html_tags(article.content))

    print(f"\nExtracted {len(extracted_events)} events:")
    for i, event in enumerate(extracted_events, 1):
        print(f"\nEvent {i}:")
        print(event.model_dump_json(indent=2))

    # This article is about restaurant business news, should return 0 events
    assert len(extracted_events) == 0


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction_utc_environment():
    """Test LLM extraction with UTC environment to ensure LLM extracts timezone from content, not process."""
    # Override the timezone for this process to UTC (like GitHub Actions)
    os.environ["TZ"] = "UTC"
    tzset()

    # Verify the process timezone is UTC
    local_tzinfo = datetime.now().astimezone().tzinfo
    assert local_tzinfo and str(local_tzinfo) == "UTC"

    # Load environment variables
    load_dotenv()

    # Read the downloaded RSS data
    test_data_path = Path(__file__).parent / "data" / "fremont_neighbor_rss.xml"
    with open(test_data_path, "r", encoding="utf-8") as f:
        rss_content = f.read()

    # Create extractor instance and get the first article
    extractor = FremontNeighborExtractor(rss_content)

    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert "A.B. Ernst Park Cleanup" in article.title

    # Extract events using LLM
    extracted_events = extract_articles(article.title, str(article.pub_date), strip_html_tags(article.content))

    # Should have at least one event
    assert len(extracted_events) == 2

    # Test that the LLM correctly extracted Pacific Time from content, not UTC from process
    first_event = extracted_events[0]

    # The start_datetime should have timezone info (Pacific Time)
    assert first_event.start_datetime is not None
    assert first_event.start_datetime.tzinfo is not None

    # Should be Pacific Time (UTC-7 or UTC-8), not UTC
    # In August, Pacific Time is UTC-7 (PDT)
    timezone_offset = first_event.start_datetime.utcoffset()
    assert timezone_offset is not None

    # Pacific Daylight Time is UTC-7 (negative 7 hours)
    expected_offset = timedelta(hours=-7)
    assert timezone_offset == expected_offset
