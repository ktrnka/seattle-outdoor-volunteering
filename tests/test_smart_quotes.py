"""Test for smart quotes and HTML entity handling in title normalization."""

from src.etl.deduplication import normalize_title


def test_smart_quotes_normalization():
    """Test that different quote types in titles normalize to the same string."""

    # All these variations should normalize to the same string
    titles = [
        "Heron s Nest Event",           # No quotes - baseline
        "Heron's Nest Event",         # Regular apostrophe
        "Heron's Nest Event",         # Smart quote/curly apostrophe (U+2019)
        # HTML entity for smart quote - this is the problematic one from SPF
        "Heron&#8217;s Nest Event",
    ]

    normalized_titles = [normalize_title(title) for title in titles]

    print("Title normalization results:")
    for original, normalized in zip(titles, normalized_titles):
        print(f"  {original!r} -> {normalized!r}")

    # All normalized titles should be identical
    expected = "heron s nest event"
    for i, normalized in enumerate(normalized_titles):
        assert normalized == expected, f"Title {i} ({titles[i]!r}) normalized to {normalized!r}, expected {expected!r}"

    # All should be the same
    assert len(set(normalized_titles)
               ) == 1, f"All titles should normalize the same, got: {set(normalized_titles)}"


def test_html_entity_decoding():
    """Test HTML entity decoding specifically."""
    import html

    # Test the HTML entity decoding step directly
    html_entity_title = "Heron&#8217;s Nest Event"
    decoded = html.unescape(html_entity_title)
    print(f"HTML unescape: {html_entity_title!r} -> {decoded!r}")

    assert "\u2019" in decoded, "Expected smart quote in decoded title"
    assert "\u2019" not in normalize_title(html_entity_title), \
        "normalize_title should not contain smart quotes after normalization"
