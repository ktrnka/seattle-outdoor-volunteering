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
    if url_str.startswith("/"):
        url_str = f"https://seattle.greencitypartnerships.org{url_str}"

    # Parse the URL
    parsed = urlparse(url_str)

    # Default to https and replace http with https
    scheme = parsed.scheme or "https"
    if scheme == "http":
        scheme = "https"

    # Lowercase the netloc (domain)
    netloc = parsed.netloc.lower()

    # Remove trailing slash from path
    path = parsed.path.rstrip("/")

    # Reconstruct the URL
    normalized = urlunparse((scheme, netloc, path, parsed.params, parsed.query, parsed.fragment))

    return normalized
