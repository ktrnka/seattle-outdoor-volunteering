import json
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser
from pydantic import HttpUrl

from ..models import SEATTLE_TZ, Event
from .base import BaseListExtractor
from .url_utils import normalize_url


class EarthCorpsCalendarExtractor(BaseListExtractor):
    """Extractor for EarthCorps volunteer events."""

    source = "EC"

    def extract(self) -> List[Event]:
        """Extract volunteer events from EarthCorps calendar HTML."""
        soup = BeautifulSoup(self.raw_data, "html.parser")

        # Extract month/year from navigation links
        year, month = self._extract_year_month(soup)

        # Find the JavaScript with event data
        events_data = self._extract_events_data(soup)
        if not events_data:
            return []

        events = []
        for day, day_data in events_data.items():
            for event_data in day_data.get("events", []):
                event = self._parse_event(event_data, year, month, int(day))
                if event:
                    events.append(event)

        return events

    @classmethod
    def raise_for_missing_content(cls, response) -> None:
        """Check response for CloudFlare protection and validate expected content."""
        # Check for Cloudflare protection
        if "Just a moment" in response.text and "cloudflare" in response.text.lower():
            raise Exception(f"Cloudflare protection detected when fetching EarthCorps calendar: {response.url}")

        # Check for the specific JavaScript variable that contains event data
        if "var events_by_date" not in response.text:
            raise Exception(f"Invalid response from EarthCorps calendar - missing events data: {response.url}")

    @classmethod
    def fetch(cls) -> "EarthCorpsCalendarExtractor":
        """Fetch events from EarthCorps calendar for two weeks from now."""
        # Get current date and add two weeks to find future events
        from datetime import datetime, timedelta

        target_date = datetime.now() + timedelta(weeks=2)
        url = f"https://www.earthcorps.org/volunteer/calendar/{target_date.year}/{target_date.month}/"

        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.earthcorps.org/",
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        cls.raise_for_missing_content(response)

        return cls(response.text)

    def _extract_year_month(self, soup: BeautifulSoup) -> tuple[int, int]:
        """Extract year and month from navigation links."""
        # Look for previous/next month navigation links
        prev_link = soup.select_one("div.month-nav a[href*='/volunteer/calendar/']")
        if prev_link and isinstance(prev_link, Tag):
            href = prev_link.get("href")
            if href:
                match = re.search(r"/calendar/(\d{4})/(\d{1,2})/", str(href))
                if match:
                    year = int(match.group(1))
                    prev_month = int(match.group(2))
                    # Current month is next month from previous
                    month = prev_month + 1
                    return year, month

        # Fallback to current date
        today = datetime.now()
        return today.year, today.month

    def _extract_events_data(self, soup: BeautifulSoup) -> Optional[dict]:
        """Extract the JavaScript events_by_date object."""
        # Find script tag containing events_by_date
        for script in soup.find_all("script"):
            assert isinstance(script, Tag)
            if script.string and "events_by_date" in script.string:
                script_content = script.string

                # Extract the JSON from the JavaScript variable
                # Look for: var events_by_date = {...};
                match = re.search(r"var events_by_date = ({.*?});", script_content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass

        return None

    def _parse_event(self, event_data: dict, year: int, month: int, day: int) -> Optional[Event]:
        """Parse a single event from the events_by_date JavaScript object."""
        try:
            # Extract basic fields
            event_id = event_data.get("Id", "")
            title = event_data.get("Name", "").strip()

            if not event_id or not title:
                return None

            # Create source_id from event ID
            source_id = event_id

            # Parse datetime
            start_datetime_str = event_data.get("StartDateTime", "")
            duration_hours = float(event_data.get("Duration", "0.0"))

            # Parse start time (format like "8/9/2025 10:00 AM") or raise error if not found
            start_dt = parser.parse(start_datetime_str)

            # Convert to UTC (assuming Pacific time)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=SEATTLE_TZ)
            start = start_dt.astimezone(timezone.utc)

            # Calculate end time
            end = start + timedelta(hours=duration_hours)

            # Extract venue/location information
            venue = self._extract_venue(event_data)

            # Build event URL
            url = f"https://www.earthcorps.org/volunteer/event/{event_id}"

            # TODO: Refactor into an EarthCorpCalendarEvent -> Event conversion method
            return Event(
                source=self.source,
                source_id=source_id,
                title=title,
                start=start,
                end=end,
                venue=venue,
                address=None,  # Not provided in the data
                url=HttpUrl(normalize_url(url)),
                cost=None,  # EarthCorps events appear to be free
                latitude=None,
                longitude=None,
                tags=[],
                same_as=None,
            )

        # Handle exceptions in the caller
        except Exception as e:
            print(f"Error parsing EarthCorps event {event_data.get('Id', 'unknown')}: {e}")
            return None

    def _extract_venue(self, event_data: dict) -> str:
        """Extract venue name from event data."""

        return event_data.get("SubRegion", "Unknown")
