"""Tests for the new deduplication system."""

from datetime import datetime, date
from zoneinfo import ZoneInfo

import pytest
from pydantic import HttpUrl

from src.models import Event
from src.etl.deduplication import (
    normalize_title,
    get_event_date,
    group_events_by_title_and_date,
    select_most_common_title,
    select_most_common_venue,
    select_event_with_time_info,
    is_gsp_url,
    select_preferred_url,
    generate_canonical_id,
    create_canonical_event,
    deduplicate_events,
)

# Test timezone
SEATTLE_TZ = ZoneInfo('America/Los_Angeles')
UTC = ZoneInfo('UTC')


def test_normalize_title():
    """Test title normalization."""
    # Basic normalization
    assert normalize_title(
        "Volunteer Event at Green Lake") == "volunteer event at green lake"

    # Remove punctuation
    assert normalize_title(
        "Work-Party: Restoration!") == "work party restoration"

    # Multiple spaces
    assert normalize_title(
        "Event   with    extra   spaces") == "event with extra spaces"

    # Strip whitespace
    assert normalize_title("  Event with padding  ") == "event with padding"

    # Mixed case and punctuation
    assert normalize_title(
        "Lincoln Park: Tree-Planting & Invasive Removal") == "lincoln park tree planting invasive removal"


def test_get_event_date():
    """Test extracting date from event."""
    event = Event(
        source="SPR",
        source_id="123",
        title="Test Event",
        start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
        end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
        url=HttpUrl("https://example.com/event")
    )

    assert get_event_date(event) == date(2025, 7, 28)


def test_group_events_by_title_and_date():
    """Test grouping events by normalized title and date."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Lincoln Park Work Party",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://spr.example.com/1")
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Lincoln Park: Work Party",  # Different punctuation
            # Same date, different time
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://gsp.example.com/2")
        ),
        Event(
            source="SPR",
            source_id="3",
            title="Green Lake Restoration",
            start=datetime(2025, 7, 29, 9, 0, tzinfo=UTC),  # Different date
            end=datetime(2025, 7, 29, 11, 0, tzinfo=UTC),
            url=HttpUrl("https://spr.example.com/3")
        ),
    ]

    groups = group_events_by_title_and_date(events)

    # Should have 2 groups
    assert len(groups) == 2

    # Lincoln Park events should be grouped together
    lincoln_key = ("lincoln park work party", date(2025, 7, 28))
    assert lincoln_key in groups
    assert len(groups[lincoln_key]) == 2

    # Green Lake event should be in its own group
    green_lake_key = ("green lake restoration", date(2025, 7, 29))
    assert green_lake_key in groups
    assert len(groups[green_lake_key]) == 1


def test_select_most_common_title():
    """Test selecting most common title."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Lincoln Park Work Party",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/1")
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Lincoln Park Work Party",  # Same title
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/2")
        ),
        Event(
            source="SPF",
            source_id="3",
            title="Lincoln Park: Work Party",  # Different punctuation
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/3")
        ),
    ]

    # Should pick the most common exact title
    assert select_most_common_title(events) == "Lincoln Park Work Party"


def test_select_most_common_venue():
    """Test selecting most common venue."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Test Event",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/1"),
            venue="Lincoln Park"
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Test Event",
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/2"),
            venue="Lincoln Park"  # Same venue
        ),
        Event(
            source="SPF",
            source_id="3",
            title="Test Event",
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/3"),
            venue=None  # No venue
        ),
    ]

    assert select_most_common_venue(events) == "Lincoln Park"

    # Test all None venues
    events_no_venue = [
        Event(
            source="SPR",
            source_id="1",
            title="Test Event",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://example.com/1"),
            venue=None
        ),
    ]

    assert select_most_common_venue(events_no_venue) is None


def test_select_event_with_time_info():
    """Test selecting event with time information."""
    # Date-only event (midnight UTC with zero duration)
    date_only_event = Event(
        source="GSP",
        source_id="1",
        title="Test Event",
        start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
        end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
        url=HttpUrl("https://example.com/1")
    )

    # Event with time info
    timed_event = Event(
        source="SPR",
        source_id="2",
        title="Test Event",
        start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
        end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
        url=HttpUrl("https://example.com/2")
    )

    events = [date_only_event, timed_event]

    # Should pick the event with time info
    selected = select_event_with_time_info(events)
    assert selected == timed_event

    # Test with no events having time info
    date_only_events = [date_only_event]
    assert select_event_with_time_info(date_only_events) is None


def test_is_gsp_url():
    """Test GSP URL detection."""
    assert is_gsp_url("https://greenseattle.org/event/123")
    assert is_gsp_url("https://www.greenseattle.org/volunteers")
    assert not is_gsp_url("https://seattle.gov/parks")
    assert not is_gsp_url("https://seattle-parks-foundation.org")


def test_select_preferred_url():
    """Test URL preference selection."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Test Event",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://seattle.gov/parks/event/1")
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Test Event",
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://greenseattle.org/event/2")  # GSP URL
        ),
        Event(
            source="SPF",
            source_id="3",
            title="Test Event",
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://seattle.gov/parks/event/1")  # Same as first
        ),
    ]

    # Should prefer GSP URL
    preferred = select_preferred_url(events)
    assert preferred == "https://greenseattle.org/event/2"

    # Test without GSP URL - should pick most common
    non_gsp_events = [events[0], events[2]]  # Two seattle.gov URLs
    preferred_non_gsp = select_preferred_url(non_gsp_events)
    assert preferred_non_gsp == "https://seattle.gov/parks/event/1"


def test_generate_canonical_id():
    """Test canonical ID generation."""
    # Should be deterministic
    id1 = generate_canonical_id("lincoln park work party", date(2025, 7, 28))
    id2 = generate_canonical_id("lincoln park work party", date(2025, 7, 28))
    assert id1 == id2

    # Should be different for different inputs
    id3 = generate_canonical_id("green lake restoration", date(2025, 7, 28))
    assert id1 != id3

    id4 = generate_canonical_id("lincoln park work party", date(2025, 7, 29))
    assert id1 != id4

    # Should be reasonable length
    assert len(id1) == 16


def test_create_canonical_event():
    """Test creating canonical event from group."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Lincoln Park Work Party",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Lincoln Park",
            cost="Free"
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Lincoln Park Work Party",
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),  # Date-only
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://greenseattle.org/event/2"),
            venue="Lincoln Park"
        ),
    ]

    normalized_title = "lincoln park work party"
    event_date = date(2025, 7, 28)

    canonical = create_canonical_event(events, normalized_title, event_date)

    # Check basic properties
    assert canonical.title == "Lincoln Park Work Party"
    assert canonical.venue == "Lincoln Park"
    # Should prefer GSP
    assert str(canonical.url) == "https://greenseattle.org/event/2"
    assert canonical.cost == "Free"  # From SPR event

    # Should use timing from SPR event (has time info)
    assert canonical.start == datetime(2025, 7, 28, 10, 0, tzinfo=UTC)
    assert canonical.end == datetime(2025, 7, 28, 12, 0, tzinfo=UTC)

    # Check source events tracking
    assert set(canonical.source_events) == {"SPR:1", "GSP:2"}

    # Check canonical ID generation
    expected_id = generate_canonical_id(normalized_title, event_date)
    assert canonical.canonical_id == expected_id


def test_deduplicate_events_new():
    """Test the complete new deduplication system."""
    events = [
        Event(
            source="SPR",
            source_id="1",
            title="Lincoln Park Work Party",
            start=datetime(2025, 7, 28, 10, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 12, 0, tzinfo=UTC),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Lincoln Park"
        ),
        Event(
            source="GSP",
            source_id="2",
            title="Lincoln Park: Work Party",  # Similar title
            start=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            end=datetime(2025, 7, 28, 0, 0, tzinfo=UTC),
            url=HttpUrl("https://greenseattle.org/event/2"),
            venue="Lincoln Park"
        ),
        Event(
            source="SPR",
            source_id="3",
            title="Green Lake Restoration",
            start=datetime(2025, 7, 29, 9, 0, tzinfo=UTC),
            end=datetime(2025, 7, 29, 11, 0, tzinfo=UTC),
            url=HttpUrl("https://seattle.gov/parks/event/3"),
            venue="Green Lake Park"
        ),
    ]

    canonical_events, membership_map = deduplicate_events(events)

    # Should have 2 canonical events
    assert len(canonical_events) == 2

    # Check membership map
    assert len(membership_map) == 3
    assert ("SPR", "1") in membership_map
    assert ("GSP", "2") in membership_map
    assert ("SPR", "3") in membership_map

    # SPR:1 and GSP:2 should be in the same group
    assert membership_map[("SPR", "1")] == membership_map[("GSP", "2")]

    # SPR:3 should be in a different group
    assert membership_map[("SPR", "3")] != membership_map[("SPR", "1")]

    # Find the Lincoln Park canonical event
    lincoln_canonical = None
    for canonical in canonical_events:
        if "lincoln park" in canonical.title.lower():
            lincoln_canonical = canonical
            break

    assert lincoln_canonical is not None
    assert set(lincoln_canonical.source_events) == {"SPR:1", "GSP:2"}
    # Should prefer GSP
    assert str(lincoln_canonical.url) == "https://greenseattle.org/event/2"


def test_empty_event_group():
    """Test error handling for empty event groups."""
    with pytest.raises(ValueError, match="Cannot create canonical event from empty group"):
        create_canonical_event([], "test", date(2025, 7, 28))
