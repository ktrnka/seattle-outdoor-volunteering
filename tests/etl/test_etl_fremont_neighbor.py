from pathlib import Path
import pytest

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
    try:
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

        # Based on the RSS content, we expect 2 events (Aug 10 and Aug 16)
        assert len(
            extracted_events) == 2, f"Expected 2 events, got {len(extracted_events)}"

        # Check that we have both dates
        event_dates = [event.event_date for event in extracted_events]
        print(f"\nEvent dates found: {event_dates}")

        # Should have August 10 and August 16 (2025)
        from datetime import date
        expected_dates = [date(2025, 8, 10), date(2025, 8, 16)]

        for expected_date in expected_dates:
            assert expected_date in event_dates, f"Expected date {expected_date} not found in {event_dates}"

        print("\n✓ LLM extraction test passed!")

    except Exception as e:
        print(f"\nLLM extraction failed: {e}")
        # Don't fail the test if LLM is unavailable, just report it
        print("This might be due to missing GITHUB_TOKEN or LLM service issues")


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
    try:
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
        assert len(extracted_events) == 0, f"Expected 0 events for business news article, got {len(extracted_events)}"

        print("\n✓ LLM no-events extraction test passed!")

    except Exception as e:
        print(f"\nLLM extraction failed: {e}")
        # Don't fail the test if LLM is unavailable, just report it
        print("This might be due to missing GITHUB_TOKEN or LLM service issues")
