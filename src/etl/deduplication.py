"""Deduplication utilities for events from multiple sources."""

from typing import List, Dict, Set
from .url_utils import urls_match, normalize_url
from ..models import Event


def deduplicate_events(events: List[Event]) -> List[Event]:
    """
    Identify and mark duplicate events across sources using URL matching.
    
    Strategy:
    1. Build a map of normalized URLs to events
    2. For events with same_as URLs, mark them as duplicates
    3. For events without same_as, check if their URL matches another event's same_as
    4. Prefer GSP as canonical source (since it's the primary registration)
    
    Args:
        events: List of events from all sources
        
    Returns:
        List of events with same_as field set appropriately
    """
    # Build URL to event mapping for canonical events
    url_to_event: Dict[str, Event] = {}
    
    # First pass: collect all events that could be canonical (no same_as)
    for event in events:
        if not event.same_as:
            normalized_url = normalize_url(event.url)
            url_to_event[normalized_url] = event
    
    # Second pass: find duplicates
    for event in events:
        if event.same_as:
            # This event already points to a canonical version
            continue
            
        # Check if this event's URL matches any same_as URLs from other events
        normalized_url = normalize_url(event.url)
        
        # Look for other events that point to this one as canonical
        for other_event in events:
            if (other_event.same_as and 
                other_event != event and
                urls_match(normalized_url, other_event.same_as)):
                
                # This event is the canonical version that others point to
                # No need to mark it as duplicate
                break
        else:
            # Check if there's a canonical event this should point to
            # Look through all events with same_as to find the canonical URL
            canonical_url = _find_canonical_url_for_event(event, events)
            if canonical_url:
                event.same_as = canonical_url
    
    return events


def _find_canonical_url_for_event(event: Event, all_events: List[Event]) -> str | None:
    """
    Find the canonical URL that this event should point to as same_as.
    
    For example, if we have:
    - SPF event about "Pigeon Point" from greenseattle.org organizer
    - GSP event at URL "https://seattle.greencitypartnerships.org/event/41845"  
    - SPR event with same_as="https://seattle.greencitypartnerships.org/event/41845"
    
    Then the SPF event should also point to the GSP URL as canonical.
    """
    event_url = normalize_url(event.url)
    
    # Look for events that have same_as URLs
    for other_event in all_events:
        if not other_event.same_as or other_event == event:
            continue
            
        # Check if this event could be a duplicate of the canonical event
        canonical_url = normalize_url(other_event.same_as)
        
        if _events_likely_same(event, other_event):
            return canonical_url
            
        # Also check if there's a canonical event with this URL
        for potential_canonical in all_events:
            if (not potential_canonical.same_as and 
                urls_match(canonical_url, potential_canonical.url) and
                _events_likely_same(event, potential_canonical)):
                return canonical_url
    
    return None


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
