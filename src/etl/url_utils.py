"""URL normalization utilities for event deduplication."""

from urllib.parse import urlparse, urlunparse
from pydantic import HttpUrl


def normalize_url(url: str | HttpUrl) -> str:
    """
    Normalize a URL for comparison purposes.
    
    - Ensures https scheme if no scheme provided
    - Removes trailing slashes
    - Converts relative URLs to absolute for GSP domain
    - Lowercases domain
    
    Args:
        url: The URL to normalize
        
    Returns:
        Normalized URL string
    """
    url_str = str(url) if isinstance(url, HttpUrl) else url
    
    # Handle relative URLs from GSP (like "/event/42093")
    if url_str.startswith('/'):
        url_str = f"https://seattle.greencitypartnerships.org{url_str}"
    
    # Parse the URL
    parsed = urlparse(url_str)
    
    # Default to https if no scheme
    scheme = parsed.scheme or 'https'
    
    # Lowercase the netloc (domain)
    netloc = parsed.netloc.lower()
    
    # Remove trailing slash from path
    path = parsed.path.rstrip('/')
    
    # Reconstruct the URL
    normalized = urlunparse((
        scheme,
        netloc, 
        path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))
    
    return normalized


def urls_match(url1: str | HttpUrl, url2: str | HttpUrl) -> bool:
    """
    Check if two URLs refer to the same resource after normalization.
    
    Args:
        url1: First URL to compare
        url2: Second URL to compare
        
    Returns:
        True if URLs match after normalization
    """
    if not url1 or not url2:
        return False
        
    norm1 = normalize_url(url1)
    norm2 = normalize_url(url2)
    
    return norm1 == norm2


def extract_event_id_from_url(url: str | HttpUrl) -> str | None:
    """
    Extract event ID from common URL patterns.
    
    Examples:
    - https://seattle.greencitypartnerships.org/event/42093 -> "42093"
    - https://www.seattleparksfoundation.org/event/pigeon-point-park-restoration-event-41/ -> "pigeon-point-park-restoration-event-41"
    
    Args:
        url: URL to extract ID from
        
    Returns:
        Event ID if found, None otherwise
    """
    url_str = str(url) if isinstance(url, HttpUrl) else url
    parsed = urlparse(url_str)
    
    # Handle /event/ID patterns
    if '/event/' in parsed.path:
        # Split by /event/ and get the part after it
        event_part = parsed.path.split('/event/')[-1]
        # Remove trailing slash and get first segment
        event_id = event_part.rstrip('/').split('/')[0]
        return event_id if event_id else None
    
    return None
