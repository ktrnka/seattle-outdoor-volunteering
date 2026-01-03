"""Tests for request throttling."""

import time

import pytest

from src.etl.request_throttle import RequestThrottle


def test_throttle_enforces_delay():
    """Test that throttle enforces minimum delay between requests."""
    throttle = RequestThrottle()
    
    url = "https://example.com/page1"
    
    # First request should be immediate
    start = time.time()
    throttle.wait_if_needed(url, delay_seconds=0.1)
    first_duration = time.time() - start
    assert first_duration < 0.05, "First request should be immediate"
    
    # Second request to same domain should be delayed
    start = time.time()
    throttle.wait_if_needed(url, delay_seconds=0.1)
    second_duration = time.time() - start
    assert second_duration >= 0.09, f"Second request should be delayed by ~0.1s, was {second_duration}"
    
    # Request to different domain should be immediate
    start = time.time()
    throttle.wait_if_needed("https://different.com/page", delay_seconds=0.1)
    third_duration = time.time() - start
    assert third_duration < 0.05, f"Different domain should be immediate, was {third_duration}"


def test_throttle_invalid_url():
    """Test that invalid URLs raise an error."""
    throttle = RequestThrottle()
    
    with pytest.raises(ValueError, match="Cannot extract domain"):
        throttle._extract_domain("not-a-url")
