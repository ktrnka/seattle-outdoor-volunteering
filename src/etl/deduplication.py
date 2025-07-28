"""Deduplication utilities for events from multiple sources."""

from typing import List
from ..models import Event

# Source precedence order (lower number = higher precedence)
# SPR preferred over GSP because SPR has time-of-day info while GSP often only has dates
SOURCE_PRECEDENCE = {
    "SPR": 1,  # Seattle Parks & Rec - has time info and often links to GSP for registration
    "GSP": 2,  # Green Seattle Partnership - often only has date info, not times
    "SPF": 3,  # Seattle Parks Foundation - messiest data source
}


def deduplicate_events(events: List[Event]) -> List[Event]:
    """
    Identify and mark duplicate events across sources using precedence-based matching.

    Strategy:
    1. Group events by similarity (title, venue, time)
    2. Within each group, pick the canonical event based on source precedence
    3. Mark lower-precedence events with same_as pointing to canonical event

    Source precedence (lower number = higher precedence):
    - GSP: 1 (source of truth for registration)
    - SPR: 2 (clean data source)
    - SPF: 3 (messiest data source)

    Args:
        events: List of events from all sources

    Returns:
        List of events with same_as field set appropriately
    """
    # Group similar events together
    event_groups = _group_similar_events(events)

    # For each group, determine the canonical event and mark duplicates
    for group in event_groups:
        if len(group) > 1:
            canonical_event = _select_canonical_event(group)

            # Mark all other events in group as duplicates
            for event in group:
                if event != canonical_event:
                    event.same_as = canonical_event.url

    return events


def _group_similar_events(events: List[Event]) -> List[List[Event]]:
    """
    Group events that are likely the same event from different sources.

    Returns:
        List of event groups, where each group contains events that are likely duplicates
    """
    groups = []
    ungrouped_events = events.copy()

    while ungrouped_events:
        current_event = ungrouped_events.pop(0)
        group = [current_event]

        # Find all events similar to current_event
        i = 0
        while i < len(ungrouped_events):
            if _events_likely_same(current_event, ungrouped_events[i]):
                group.append(ungrouped_events.pop(i))
            else:
                i += 1

        groups.append(group)

    return groups


def _select_canonical_event(events: List[Event]) -> Event:
    """
    Select the canonical event from a group of similar events based on source precedence.

    Special handling: If SPR event is selected and has a same_as link (GSP registration),
    use that URL instead of the SPR URL to provide better registration experience.

    Args:
        events: List of similar events from potentially different sources

    Returns:
        The event that should be considered canonical
    """
    if not events:
        raise ValueError("Cannot select canonical event from empty list")

    if len(events) == 1:
        canonical = events[0]
    else:
        # Sort by source precedence (lower number = higher precedence)
        def get_precedence(event: Event) -> int:
            # Unknown sources get lowest precedence
            return SOURCE_PRECEDENCE.get(event.source, 999)

        canonical = min(events, key=get_precedence)

    # Special handling for SPR events: if they have a same_as link (usually GSP registration),
    # use that URL instead of the SPR URL for better user experience
    if canonical.source == "SPR" and canonical.same_as:
        # Create a copy of the event with the GSP URL but keep all other SPR data
        canonical = canonical.model_copy()
        # We already checked that same_as is not None above
        canonical.url = canonical.same_as  # type: ignore
        canonical.same_as = None  # Clear same_as since this is now the canonical URL

    return canonical


def _events_likely_same(event1: Event, event2: Event) -> bool:
    """
    Check if two events are likely the same event based on title, venue, and time.
    """
    # Check title similarity
    if not _titles_match(event1.title, event2.title):
        return False

    # Check venue similarity (allow None matches)
    if not _venues_match(event1.venue, event2.venue):
        return False

    # Check time similarity (within 2 hours to account for different sources)
    time_diff = abs((event1.start - event2.start).total_seconds())
    if time_diff > 7200:  # 2 hours in seconds
        return False

    return True


def _titles_match(title1: str, title2: str) -> bool:
    """Check if two event titles are similar enough to be considered the same event."""
    if not title1 or not title2:
        return False

    # Normalize titles for comparison
    t1 = title1.lower().strip()
    t2 = title2.lower().strip()

    # Exact match
    if t1 == t2:
        return True

    # Remove common variations and check again
    t1_clean = _normalize_title(t1)
    t2_clean = _normalize_title(t2)

    if t1_clean == t2_clean:
        return True

    # Check if one title contains the other (for cases like "Event" vs "Event at Location")
    if t1_clean in t2_clean or t2_clean in t1_clean:
        # Make sure the shorter title is at least 10 characters to avoid false positives
        min_len = min(len(t1_clean), len(t2_clean))
        return min_len >= 10

    return False


def _normalize_title(title: str) -> str:
    """Normalize a title for comparison by removing common variations."""
    # Remove common suffixes/prefixes that might differ between sources
    title = title.replace("restoration event", "")
    title = title.replace("work party", "")
    title = title.replace("volunteer event", "")
    title = title.replace(" event", "")
    title = title.replace("park restoration", "park")

    # Remove extra whitespace
    title = " ".join(title.split())

    return title.strip()


def _venues_match(venue1: str | None, venue2: str | None) -> bool:
    """Check if two venue names are similar enough to be considered the same location."""
    if not venue1 and not venue2:
        return True  # Both null is considered a match

    if not venue1 or not venue2:
        return True  # Allow one to be None (handle "Location TBD" cases)

    v1 = venue1.lower().strip()
    v2 = venue2.lower().strip()

    # Exact match
    if v1 == v2:
        return True

    # Check if one venue contains the other
    if v1 in v2 or v2 in v1:
        return True

    return False
