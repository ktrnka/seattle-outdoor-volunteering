from pathlib import Path
import pytest
import os
from time import tzset

from src.etl.fremont_neighbor import FremontNeighborExtractor


def test_fremont_neighbor_parse_rss_structure():
    """Test parsing RSS structure without LLM calls."""
    # Read the downloaded RSS data
    test_data_path = Path(__file__).parent / "data" / \
        "fremont_neighbor_rss.xml"
    with open(test_data_path, 'r', encoding='utf-8') as f:
        rss_content = f.read()

    # Create extractor instance
    extractor = FremontNeighborExtractor(rss_content)

    # Test RSS parsing by checking the first few articles
    import xml.etree.ElementTree as ET
    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    # Should have multiple items
    assert len(items) > 0
    print(f"Found {len(items)} RSS items")

    # Test parsing the first few items to see the variety
    for i, item in enumerate(items[:5]):  # Check first 5 items
        article = extractor._parse_rss_item(item)
        if article:
            print(f"\nArticle {i+1}: {article.title}")
            print(f"  Categories: {article.categories}")
            print(
                f"  Has 'Volunteering' category: {'Volunteering' in article.categories}")

            # Show description with character count
            desc_preview = article.description[:200]
            desc_remaining = len(article.description) - len(desc_preview)
            print(
                f"  Description ({len(article.description)} chars): {desc_preview}{'...' if desc_remaining > 0 else ''}")
            if desc_remaining > 0:
                print(f"    ({desc_remaining} more chars)")

            # Show content with character count
            content_preview = article.content[:200]
            content_remaining = len(article.content) - len(content_preview)
            print(
                f"  Content ({len(article.content)} chars): {content_preview}{'...' if content_remaining > 0 else ''}")
            if content_remaining > 0:
                print(f"    ({content_remaining} more chars)")

    # Test parsing the first item specifically
    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert article.title
    assert article.link
    assert article.pub_date
    assert isinstance(article.categories, list)
    # Check if the first article is about the A.B. Ernst Park Cleanup
    assert "A.B. Ernst Park Cleanup" in article.title
    assert "Volunteering" in article.categories


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction():
    """Test LLM extraction on the first article (A.B. Ernst Park Cleanup)."""
    from dotenv import load_dotenv
    from src.llm.blog_event_extractor import extract_articles

    # Load environment variables
    load_dotenv()
    # Read the downloaded RSS data
    test_data_path = Path(__file__).parent / "data" / \
        "fremont_neighbor_rss.xml"
    with open(test_data_path, 'r', encoding='utf-8') as f:
        rss_content = f.read()

    # Create extractor instance and get the first article
    extractor = FremontNeighborExtractor(rss_content)

    import xml.etree.ElementTree as ET
    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert "A.B. Ernst Park Cleanup" in article.title
    assert "Volunteering" in article.categories

    print(f"\nTesting LLM extraction on: {article.title}")
    print(f"Description: {article.description}")

    # Prepare content for LLM (remove HTML tags like the extractor does)
    import re
    content = re.sub(r'<[^>]+>', ' ', article.content)
    content = re.sub(r'\s+', ' ', content).strip()

    print(f"Cleaned content preview: {content[:500]}...")

    # Extract events using LLM
    extracted_events = extract_articles(
        article.title, str(article.pub_date), content)

    print(f"\nExtracted {len(extracted_events)} events:")
    for i, event in enumerate(extracted_events, 1):
        print(f"\nEvent {i}:")
        print(f"  Title: {event.title}")
        print(f"  Date: {event.event_date}")
        print(f"  Start time: {event.start_time}")
        print(f"  End time: {event.end_time}")
        print(f"  Venue: {event.venue}")
        print(f"  Description: {event.description}")
        print(f"  Contact: {event.contact_info}")

    # Based on the RSS content, we expect at least 1 event, possibly 2 (Aug 10 and Aug 16)
    assert len(
        extracted_events) >= 1, f"Expected at least 1 event, got {len(extracted_events)}"

    # Check that we have both dates
    event_dates = [event.event_date for event in extracted_events]
    print(f"\nEvent dates found: {event_dates}")

    # Should have August 10 and possibly August 16 (2025)
    from datetime import date
    expected_dates = [date(2025, 8, 10)]  # At minimum should have the 10th

    # The first event should be August 10th
    first_event = extracted_events[0]
    assert first_event.event_date == expected_dates[
        0], f"Expected first event on 2025-08-10, got {first_event.event_date}"

    # If we have 2 events, the second should be August 16th
    if len(extracted_events) == 2:
        assert extracted_events[1].event_date == date(
            2025, 8, 16), f"Expected second event on 2025-08-16, got {extracted_events[1].event_date}"

    # Test that we now have start_datetime and end_datetime
    for event in extracted_events:
        assert hasattr(
            event, 'start_datetime'), f"Event missing start_datetime: {event}"
        assert hasattr(
            event, 'end_datetime'), f"Event missing end_datetime: {event}"

        # These should be datetime objects
        from datetime import datetime
        if event.start_datetime:
            assert isinstance(
                event.start_datetime, datetime), f"start_datetime should be datetime, got {type(event.start_datetime)}"
        if event.end_datetime:
            assert isinstance(
                event.end_datetime, datetime), f"end_datetime should be datetime, got {type(event.end_datetime)}"

        print(f"  Start datetime: {event.start_datetime}")
        print(f"  End datetime: {event.end_datetime}")

    # Test source_id generation
    from src.llm.blog_event_extractor import generate_source_id
    for event in extracted_events:
        source_id = generate_source_id(article.guid, event.event_date)
        print(f"  Generated source_id: {source_id}")
        # Should be in format "683_2025-08-10" not the full URL
        assert "_" in source_id, f"source_id should contain underscore: {source_id}"
        assert str(
            event.event_date) in source_id, f"source_id should contain event date: {source_id}"
        # Should be much shorter now (not contain the full URL)
        assert "https://" not in source_id, f"source_id should not contain full URL: {source_id}"

    print("\n✓ LLM extraction test passed!")


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction_no_events():
    """Test LLM extraction on a non-volunteer article (should return zero events)."""
    from dotenv import load_dotenv
    from src.llm.blog_event_extractor import extract_articles

    # Load environment variables
    load_dotenv()
    # Read the downloaded RSS data
    test_data_path = Path(__file__).parent / "data" / \
        "fremont_neighbor_rss.xml"
    with open(test_data_path, 'r', encoding='utf-8') as f:
        rss_content = f.read()

    # Create extractor instance and get the third article (food and drink business news)
    extractor = FremontNeighborExtractor(rss_content)

    import xml.etree.ElementTree as ET
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

    # Prepare content for LLM (remove HTML tags like the extractor does)
    import re
    content = re.sub(r'<[^>]+>', ' ', article.content)
    content = re.sub(r'\s+', ' ', content).strip()

    print(f"Cleaned content preview: {content[:500]}...")

    # Extract events using LLM
    extracted_events = extract_articles(
        article.title, str(article.pub_date), content)

    print(f"\nExtracted {len(extracted_events)} events:")
    for i, event in enumerate(extracted_events, 1):
        print(f"\nEvent {i}:")
        print(f"  Title: {event.title}")
        print(f"  Date: {event.event_date}")
        print(f"  Start time: {event.start_time}")
        print(f"  End time: {event.end_time}")
        print(f"  Venue: {event.venue}")
        print(f"  Description: {event.description}")
        print(f"  Contact: {event.contact_info}")

    # This article is about restaurant business news, should return 0 events
    assert len(
        extracted_events) == 0, f"Expected 0 events for business news article, got {len(extracted_events)}"

    print("\n✓ LLM no-events extraction test passed!")


@pytest.mark.llm
def test_fremont_neighbor_llm_extraction_utc_environment():
    """Test LLM extraction with UTC environment to ensure LLM extracts timezone from content, not process."""
    from dotenv import load_dotenv
    from src.llm.blog_event_extractor import extract_articles
    from datetime import datetime

    # Override the timezone for this process to UTC (like GitHub Actions)
    os.environ["TZ"] = "UTC"
    tzset()

    # Verify the process timezone is UTC
    local_tzinfo = datetime.now().astimezone().tzinfo
    assert local_tzinfo and str(
        local_tzinfo) == "UTC", f"Expected UTC timezone, got {local_tzinfo}"

    # Load environment variables
    load_dotenv()

    # Read the downloaded RSS data
    test_data_path = Path(__file__).parent / "data" / \
        "fremont_neighbor_rss.xml"
    with open(test_data_path, 'r', encoding='utf-8') as f:
        rss_content = f.read()

    # Create extractor instance and get the first article
    extractor = FremontNeighborExtractor(rss_content)

    import xml.etree.ElementTree as ET
    root = ET.fromstring(rss_content)
    items = root.findall(".//item")

    first_item = items[0]
    article = extractor._parse_rss_item(first_item)

    assert article is not None
    assert "A.B. Ernst Park Cleanup" in article.title

    # Prepare content for LLM
    import re
    content = re.sub(r'<[^>]+>', ' ', article.content)
    content = re.sub(r'\s+', ' ', content).strip()

    # Extract events using LLM
    extracted_events = extract_articles(
        article.title, str(article.pub_date), content)

    # Should have at least one event
    assert len(
        extracted_events) >= 1, f"Expected at least 1 event, got {len(extracted_events)}"

    # Test that the LLM correctly extracted Pacific Time from content, not UTC from process
    first_event = extracted_events[0]

    # The start_datetime should have timezone info (Pacific Time)
    assert first_event.start_datetime is not None, "start_datetime should not be None"
    assert first_event.start_datetime.tzinfo is not None, "start_datetime should have timezone info"

    # Should be Pacific Time (UTC-7 or UTC-8), not UTC
    # In August, Pacific Time is UTC-7 (PDT)
    timezone_offset = first_event.start_datetime.utcoffset()
    assert timezone_offset is not None, "Should have timezone offset"

    # Pacific Daylight Time is UTC-7 (negative 7 hours)
    from datetime import timedelta
    expected_offset = timedelta(hours=-7)
    assert timezone_offset == expected_offset, f"Expected Pacific Time offset {expected_offset}, got {timezone_offset}"

    print(
        f"\n✓ LLM correctly extracted Pacific Time ({timezone_offset}) despite UTC process environment")
    print(f"  Event time: {first_event.start_datetime}")
    print(f"  Process timezone: {local_tzinfo}")

    print("\n✓ LLM timezone extraction test passed!")
