from pathlib import Path

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
