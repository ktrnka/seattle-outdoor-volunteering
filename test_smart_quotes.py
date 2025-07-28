"""Test for smart quotes normalization issue."""

from datetime import datetime, date
from zoneinfo import ZoneInfo
from pydantic import HttpUrl

from src.models import Event
from src.etl.new_deduplication import normalize_title, group_events_by_title_and_date, deduplicate_events_new

UTC = ZoneInfo('UTC')


def test_smart_quotes_normalization():
    """Test that smart quotes and HTML entities are normalized properly."""

    # Test cases with different quote styles
    test_cases = [
        # Basic smart quote normalization
        ("Heron's Nest Event", "herons nest event"),  # Regular apostrophe
        ("Heron's Nest Event", "herons nest event"),  # Smart quote (right single)
        ("Heron's Nest Event", "herons nest event"),  # Smart quote (left single)

        # HTML entity normalization
        # HTML entity for right single quote
        ("Heron&#8217;s Nest Event", "herons nest event"),
        # HTML entity for left single quote
        ("Heron&#8216;s Nest Event", "herons nest event"),
        # HTML entity for regular apostrophe
        ("Heron&#39;s Nest Event", "herons nest event"),

        # Double quotes
        ('"Smart quotes"', "smart quotes"),  # Regular double quotes
        # HTML entities for smart double quotes
        ("&#8220;Smart quotes&#8221;", "smart quotes"),

        # Mixed cases that should all normalize to the same thing
        ("Heron's Nest", "herons nest"),
        ("Heron's Nest", "herons nest"),
        ("Heron&#8217;s Nest", "herons nest"),
        ("Heron&#39;s Nest", "herons nest"),
    ]

    for original, expected in test_cases:
        result = normalize_title(original)
        print(f"'{original}' -> '{result}' (expected: '{expected}')")
        assert result == expected, f"Expected '{expected}', got '{result}' for input '{original}'"

    print("All smart quote normalization tests passed!")


def test_real_world_heron_nest_case():
    """Test the actual Heron's Nest case from August 8th data."""

    # Create events matching the real data
    events = [
        Event(
            source="GSP",
            source_id="42008",
            title="Heron's Nest  Healing the Forest",  # Regular apostrophe
            start=datetime(2025, 8, 8, 0, 0, tzinfo=UTC),
            end=datetime(2025, 8, 8, 0, 0, tzinfo=UTC),
            url=HttpUrl(
                "https://seattle.greencitypartnerships.org/event/42008"),
            venue="West Duwamish Greenbelt: Alaska"
        ),
        Event(
            source="SPR",
            source_id="187593764",
            title="Heron's Nest Healing the Forest",  # Regular apostrophe, single space
            start=datetime(2025, 8, 8, 17, 0, tzinfo=UTC),
            end=datetime(2025, 8, 8, 21, 0, tzinfo=UTC),
            url=HttpUrl("https://www.seattle.gov/parks/volunteer/event"),
            address="4700 14th Ave SW, Seattle, WA 98106"
        ),
        Event(
            source="SPF",
            source_id="herons-nest-healing-the-forest",
            title="Heron&#8217;s Nest  Healing the Forest",  # HTML entity smart quote
            start=datetime(2025, 8, 8, 17, 0, tzinfo=UTC),
            end=datetime(2025, 8, 8, 21, 0, tzinfo=UTC),
            url=HttpUrl(
                "https://www.seattleparksfoundation.org/event/herons-nest"),
            tags=["Green Seattle Partnership"]
        ),
    ]

    # Test current normalization
    normalized_titles = [normalize_title(event.title) for event in events]
    print("Current normalized titles:")
    for i, (event, normalized) in enumerate(zip(events, normalized_titles)):
        print(f"  {i+1}. '{event.title}' -> '{normalized}'")

    # Group events
    groups = group_events_by_title_and_date(events)

    print(f"\nGrouping results: {len(groups)} groups")
    for key, group in groups.items():
        print(f"  Group '{key[0]}' on {key[1]}: {len(group)} events")
        for event in group:
            print(f"    - {event.source}: '{event.title}'")

    # This should create 1 group, not 2!
    # All three events should be grouped together since they're the same event
    expected_groups = 1
    actual_groups = len(groups)

    if actual_groups != expected_groups:
        print(
            f"\nFAILED: Expected {expected_groups} groups, got {actual_groups}")
        print("The smart quote normalization is not working properly!")
        return False
    else:
        print(
            f"\nSUCCESS: All events grouped into {actual_groups} group as expected")
        return True


if __name__ == "__main__":
    print("Testing smart quotes normalization...")
    try:
        test_smart_quotes_normalization()
    except AssertionError as e:
        print(f"Smart quotes test failed: {e}")

    print("\nTesting real-world Heron's Nest case...")
    success = test_real_world_heron_nest_case()

    if not success:
        print("\nThe test confirms the bug exists. Need to fix normalize_title() function.")
    else:
        print("\nAll tests passed!")
