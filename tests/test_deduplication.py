"""Tests for deduplication utilities."""

from datetime import datetime
from zoneinfo import ZoneInfo
from pydantic import HttpUrl

from src.etl.deduplication import deduplicate_events, _group_similar_events, _select_canonical_event
from src.models import Event

UTC = ZoneInfo('UTC')


def test_source_precedence():
    """Test that SPR has highest precedence, followed by GSP, then SPF."""
    # Create similar events from different sources
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    events = [
        Event(
            source="SPF",
            source_id="spf-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattleparksfoundation.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Discovery Park"
        )
    ]

    result = deduplicate_events(events)

    # SPR should be canonical (no same_as)
    spr_event = next(e for e in result if e.source == "SPR")
    assert spr_event.same_as is None

    # GSP and SPF should point to SPR
    gsp_event = next(e for e in result if e.source == "GSP")
    spf_event = next(e for e in result if e.source == "SPF")

    assert gsp_event.same_as == spr_event.url
    assert spf_event.same_as == spr_event.url


def test_no_duplicates():
    """Test that events with different titles/venues/times are not deduplicated."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    events = [
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Lincoln Park Cleanup",  # Different title
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Lincoln Park"  # Different venue
        )
    ]

    result = deduplicate_events(events)

    # Both events should be canonical (no same_as)
    for event in result:
        assert event.same_as is None


def test_time_difference_tolerance():
    """Test that events within 2 hours are considered duplicates."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    # Events 1 hour apart should be considered duplicates
    events = [
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Discovery Park Restoration",
            start=base_time.replace(hour=11),  # 1 hour later
            end=base_time.replace(hour=13),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Discovery Park"
        )
    ]

    result = deduplicate_events(events)

    gsp_event = next(e for e in result if e.source == "GSP")
    spr_event = next(e for e in result if e.source == "SPR")

    assert spr_event.same_as is None  # SPR is now canonical
    assert gsp_event.same_as == spr_event.url


def test_time_difference_too_large():
    """Test that events more than 2 hours apart are not considered duplicates."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    # Events 3 hours apart should NOT be considered duplicates
    events = [
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Discovery Park Restoration",
            start=base_time.replace(hour=13),  # 3 hours later
            end=base_time.replace(hour=15),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Discovery Park"
        )
    ]

    result = deduplicate_events(events)

    # Both should be canonical since they're too far apart in time
    for event in result:
        assert event.same_as is None


def test_spr_over_spf_precedence():
    """Test that SPR takes precedence over SPF when GSP is not present."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    events = [
        Event(
            source="SPF",
            source_id="spf-1",
            title="Lincoln Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattleparksfoundation.org/event/1"),
            venue="Lincoln Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Lincoln Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Lincoln Park"
        )
    ]

    result = deduplicate_events(events)

    # SPR should be canonical
    spr_event = next(e for e in result if e.source == "SPR")
    spf_event = next(e for e in result if e.source == "SPF")

    assert spr_event.same_as is None
    assert spf_event.same_as == spr_event.url


def test_group_similar_events():
    """Test the event grouping logic."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    events = [
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Discovery Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
            venue="Discovery Park"
        ),
        Event(
            source="SPF",
            source_id="spf-1",
            title="Lincoln Park Cleanup",  # Different event
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattleparksfoundation.org/event/2"),
            venue="Lincoln Park"
        )
    ]

    groups = _group_similar_events(events)

    # Should have 2 groups: one with 2 events (Discovery Park), one with 1 event (Lincoln Park)
    assert len(groups) == 2

    discovery_group = next(g for g in groups if len(g) == 2)
    lincoln_group = next(g for g in groups if len(g) == 1)

    assert len(discovery_group) == 2
    assert len(lincoln_group) == 1
    assert lincoln_group[0].venue == "Lincoln Park"


def test_select_canonical_event():
    """Test canonical event selection based on precedence."""
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)

    events = [
        Event(
            source="SPF",
            source_id="spf-1",
            title="Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattleparksfoundation.org/event/1"),
        ),
        Event(
            source="GSP",
            source_id="gsp-1",
            title="Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.greencitypartnerships.org/event/1"),
        ),
        Event(
            source="SPR",
            source_id="spr-1",
            title="Park Restoration",
            start=base_time,
            end=base_time.replace(hour=12),
            url=HttpUrl("https://seattle.gov/parks/event/1"),
        )
    ]

    canonical = _select_canonical_event(events)
    assert canonical.source == "SPR"  # Should pick SPR as highest precedence


def test_spr_same_as_url_becomes_canonical_url():
    """Test that SPR events with same_as URLs use the same_as URL as canonical."""
    from pydantic import HttpUrl
    
    base_time = datetime(2024, 6, 15, 10, 0, tzinfo=UTC)
    
    # SPR event with same_as pointing to GSP registration
    spr_event = Event(
        source="SPR",
        source_id="spr-1",
        title="Park Restoration",
        start=base_time,
        end=base_time.replace(hour=12),
        url=HttpUrl("https://seattle.gov/parks/event/1"),
        same_as=HttpUrl("https://seattle.greencitypartnerships.org/event/123")
    )
    
    canonical = _select_canonical_event([spr_event])
    
    # Should use the GSP URL from same_as field
    assert canonical.url == HttpUrl("https://seattle.greencitypartnerships.org/event/123")
    assert canonical.same_as is None  # Should be cleared
    assert canonical.source == "SPR"  # Should still be SPR source for timing info
