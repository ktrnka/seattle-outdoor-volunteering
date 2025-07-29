import json
import re
from datetime import timezone, datetime, timedelta
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pydantic import HttpUrl

from .base import BaseExtractor
from .url_utils import normalize_url
from ..models import Event, SEATTLE_TZ


class EarthCorpsExtractor(BaseExtractor):
    """Extractor for EarthCorps volunteer events."""
    source = "EC"

    def extract(self) -> List[Event]:
        """Extract volunteer events from EarthCorps calendar HTML."""
        soup = BeautifulSoup(self.raw_data, 'html.parser')

        # Extract month/year from navigation links
        year, month = self._extract_year_month(soup)

        # Find the JavaScript with event data
        events_data = self._extract_events_data(soup)
        if not events_data:
            return []

        events = []
        for day, day_data in events_data.items():
            for event_data in day_data.get('events', []):
                event = self._parse_event(event_data, year, month, int(day))
                if event:
                    events.append(event)

        return events

    @classmethod
    def fetch(cls) -> 'EarthCorpsExtractor':
        """Fetch events from EarthCorps calendar for two weeks from now."""
        # Get current date and add two weeks to find future events
        from datetime import datetime, timedelta
        target_date = datetime.now() + timedelta(weeks=2)
        url = f"https://www.earthcorps.org/volunteer/calendar/{target_date.year}/{target_date.month}/"

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.earthcorps.org/',
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Check for Cloudflare protection
        if 'Just a moment' in response.text and 'cloudflare' in response.text.lower():
            raise Exception(
                f"Cloudflare protection detected when fetching EarthCorps calendar: {url}")

        # Check for minimal expected content to ensure we got a real calendar page
        if 'calendar' not in response.text.lower() or 'earthcorps' not in response.text.lower():
            raise Exception(
                f"Invalid response from EarthCorps calendar - missing expected content: {url}")

        return cls(response.text)

    def _extract_year_month(self, soup: BeautifulSoup) -> tuple[int, int]:
        """Extract year and month from navigation links."""
        # Look for previous/next month navigation links
        nav = soup.find('div', class_='month-nav')
        if nav:
            # Extract from "Previous" link like "/volunteer/calendar/2025/7/"
            prev_link = nav.find('a')
            if prev_link and hasattr(prev_link, 'get'):
                href = prev_link.get('href')
                if href:
                    match = re.search(
                        r'/calendar/(\d{4})/(\d{1,2})/', str(href))
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
        for script in soup.find_all('script'):
            if hasattr(script, 'string') and script.string and 'events_by_date' in script.string:
                script_content = script.string

                # Extract the JSON from the JavaScript variable
                # Look for: var events_by_date = {...};
                match = re.search(
                    r'var events_by_date = ({.*?});', script_content, re.DOTALL)
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
            event_id = event_data.get('Id', '')
            title = event_data.get('Name', '').strip()

            if not event_id or not title:
                return None

            # Create source_id from event ID
            source_id = event_id

            # Parse datetime
            start_datetime_str = event_data.get('StartDateTime', '')
            duration_hours = float(event_data.get('Duration', '3.0'))

            # Parse start time (format like "8/9/2025 10:00 AM")
            try:
                start_dt = parser.parse(start_datetime_str)
            except Exception:
                # Fallback: construct from year/month/day and time
                start_time = event_data.get('startTime', '10am')
                time_match = re.search(r'(\d{1,2})(am|pm)', start_time.lower())
                if time_match:
                    hour = int(time_match.group(1))
                    if time_match.group(2) == 'pm' and hour != 12:
                        hour += 12
                    elif time_match.group(2) == 'am' and hour == 12:
                        hour = 0
                else:
                    hour = 10  # Default to 10am

                start_dt = datetime(year, month, day, hour, 0)

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
                same_as=None
            )

        except Exception as e:
            print(
                f"Error parsing EarthCorps event {event_data.get('Id', 'unknown')}: {e}")
            return None

    def _extract_venue(self, event_data: dict) -> str:
        """Extract venue name from event data."""
        # Try to use the title as venue info since it often contains location
        title = event_data.get('Name', '')

        # Look for common patterns like "Location: Description" or just use title
        if ':' in title:
            # Take the part before the colon as location
            parts = title.split(':', 1)
            location_part = parts[0].strip()
            # Clean up common prefixes
            location_part = re.sub(
                r'^(Seattle|Tacoma|Everett|Bellevue|Kirkland|Redmond|Issaquah|Federal Way|Burien|Kent|Tukwila):\s*', '', location_part)
            if location_part:
                return location_part
        else:
            # No colon, use the full title as venue if it looks like a location
            if title:
                return title

        # If we have region/subregion info, use that as fallback
        region = event_data.get('Region', '')
        subregion = event_data.get('SubRegion', '')

        if subregion and subregion != region:
            return f"{subregion}, {region}"
        elif region:
            return region

        # Final fallback
        return "EarthCorps Event Location"
