"""Request throttling to respect rate limits and be gentle on external servers."""

import time
from typing import Dict
from urllib.parse import urlparse


class RequestThrottle:
    """Per-domain request throttling. Assumes single-threaded usage."""

    def __init__(self):
        self._last_request_time: Dict[str, float] = {}

    def wait_if_needed(self, url: str, delay_seconds: float = 2.0) -> None:
        """Sleep if needed to respect rate limit for this domain.
        
        Args:
            url: Full URL to extract domain from
            delay_seconds: Minimum seconds between requests to same domain (default: 2.0)
        """
        domain = self._extract_domain(url)
        
        now = time.time()
        last_request = self._last_request_time.get(domain, 0)
        time_since_last = now - last_request
        
        if time_since_last < delay_seconds:
            sleep_time = delay_seconds - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time[domain] = time.time()

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Cannot extract domain from URL: {url}")
        return parsed.netloc


# Global instance for single-threaded usage
_throttle = RequestThrottle()


def throttled_get(url: str, delay_seconds: float = 2.0, **kwargs) -> any:
    """Make a throttled HTTP GET request.
    
    Args:
        url: URL to fetch
        delay_seconds: Minimum seconds between requests to same domain
        **kwargs: Additional arguments passed to requests.get()
    
    Returns:
        Response object from requests.get()
    """
    import requests
    
    _throttle.wait_if_needed(url, delay_seconds)
    return requests.get(url, **kwargs)
