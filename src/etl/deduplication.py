"""New deduplication system using canonical events and grouping by normalized title/date."""

import re
import hashlib
import html
from collections import Counter, defaultdict
from datetime import date
from typing import Iterable, List, Dict, Optional, Tuple, TypeVar
from urllib.parse import urlparse
from pydantic import HttpUrl

from ..models import Event, CanonicalEvent


def normalize_title(title: str) -> str:
    """
    Normalize a title for grouping by removing non-word characters and lowercasing.

    Handles HTML entities, smart quotes, and other unicode characters.

    Args:
        title: Original event title

    Returns:
        Normalized title for grouping
    """
    # Convert to lowercase
    normalized = title.lower()

    # Decode HTML entities (like &#8217; for smart quotes)
    normalized = html.unescape(normalized)

    # Remove all other non-word characters (keep only letters, numbers, spaces)
    normalized = re.sub(r'[^\w\s]', ' ', normalized)

    # Collapse multiple spaces into single spaces
    normalized = re.sub(r'\s+', ' ', normalized)

    # Strip leading/trailing whitespace
    return normalized.strip()


def group_events_by_title_and_date(events: List[Event]) -> Dict[Tuple[str, date], List[Event]]:
    """
    Group events by normalized title and date.

    Args:
        events: List of events to group

    Returns:
        Dictionary mapping (normalized_title, date) to list of events
    """
    groups = defaultdict(list)

    for event in events:
        normalized_title = normalize_title(event.title)
        event_date = event.start.date()
        key = (normalized_title, event_date)
        groups[key].append(event)

    return dict(groups)


T = TypeVar('T')


def mode(values: Iterable[Optional[T]]) -> Optional[T]:
    "Get the most common, non-null value from an iterable"
    counts = Counter(value for value in values if value is not None)
    if not counts:
        return None
    return counts.most_common(1)[0][0]


def select_event_with_time_info(events: List[Event]) -> Event | None:
    """
    Select the first event that has actual time-of-day information.

    Args:
        events: List of events in the same group

    Returns:
        First event with time info, or None if none have time info
    """
    for event in events:
        if event.has_time_info():
            return event
    return None


def is_gsp_url(url: str) -> bool:
    """
    Check if a URL is from Green Seattle Partnership.

    Args:
        url: URL to check

    Returns:
        True if URL is from GSP
    """
    parsed = urlparse(str(url))
    return 'greenseattle' in parsed.netloc.lower()


def select_preferred_url(events: List[Event]) -> str:
    """
    Select the preferred registration URL from a group of events.
    Prefers GSP links over others, otherwise picks the most common.

    Args:
        events: List of events in the same group

    Returns:
        Preferred URL string
    """
    # First, try to find a GSP URL
    for event in events:
        if is_gsp_url(str(event.url)):
            return str(event.url)

    # If no GSP URL, pick the most common URL
    url_counts = Counter(str(event.url) for event in events)
    return url_counts.most_common(1)[0][0]


def generate_canonical_id(normalized_title: str, event_date: date) -> str:
    """
    Generate a unique ID for a canonical event based on title and date.

    Args:
        normalized_title: Normalized title for grouping
        event_date: Date of the event

    Returns:
        Unique canonical ID string
    """
    # Create a deterministic hash from title and date
    content = f"{normalized_title}:{event_date.isoformat()}"
    hash_obj = hashlib.sha256(content.encode('utf-8'))
    return hash_obj.hexdigest()[:16]  # Use first 16 chars for shorter IDs


def create_canonical_event(event_group: List[Event], normalized_title: str, event_date: date) -> CanonicalEvent:
    """
    Create a canonical event from a group of similar events.

    Args:
        event_group: List of similar events to merge
        normalized_title: Normalized title used for grouping
        event_date: Date used for grouping

    Returns:
        Canonical event representing the group
    """
    if not event_group:
        raise ValueError("Cannot create canonical event from empty group")

    # Generate canonical ID
    canonical_id = generate_canonical_id(normalized_title, event_date)

    # Select best attributes from the group
    title = mode(event.title for event in event_group) or ""
    venue = mode(event.venue for event in event_group)
    url = select_preferred_url(event_group)

    # Get timing info from an event with time data if available
    timing_event = select_event_with_time_info(event_group)
    if timing_event:
        start = timing_event.start
        end = timing_event.end
    else:
        # Fall back to first event's timing
        start = event_group[0].start
        end = event_group[0].end

    # Collect other attributes (pick first non-null value)
    address = next((e.address for e in event_group if e.address), None)
    cost = next((e.cost for e in event_group if e.cost), None)
    latitude = next((e.latitude for e in event_group if e.latitude), None)
    longitude = next((e.longitude for e in event_group if e.longitude), None)

    # Merge tags from all events
    all_tags = set()
    for event in event_group:
        if event.tags:
            all_tags.update(event.tags)
    tags = sorted(list(all_tags))

    # Create source events list
    source_events = [
        f"{event.source}:{event.source_id}" for event in event_group]

    return CanonicalEvent(
        canonical_id=canonical_id,
        title=title,
        start=start,
        end=end,
        venue=venue,
        address=address,
        url=HttpUrl(url),
        cost=cost,
        latitude=latitude,
        longitude=longitude,
        tags=tags,
        source_events=source_events
    )


def deduplicate_events(events: List[Event]) -> List[CanonicalEvent]:
    """
    New deduplication system that groups events by normalized title and date.

    Args:
        events: List of all events from various sources

    Returns:
        Tuple of:
        - List of canonical events
        - Dictionary mapping (source, source_id) to canonical_id for tracking membership
    """
    # Group events by normalized title and date
    event_groups = group_events_by_title_and_date(events)

    canonical_events = []

    for (normalized_title, event_date), event_group in event_groups.items():
        # Create canonical event from this group
        canonical_event = create_canonical_event(
            event_group, normalized_title, event_date)
        canonical_events.append(canonical_event)

    return canonical_events
