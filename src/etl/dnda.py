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
from ..models import Event

# DNDA API endpoint for events
# Base URL with dynamic date range - we'll build this in fetch()
API_BASE_URL = "https://dnda.org/wp-json/mec/v1/events"


class DNDAExtractor(BaseExtractor):
    """Extractor for Delridge Neighborhoods Development Association (DNDA) events."""
    source = "DNDA"

    def extract(self) -> List[Event]:
        """Extract volunteer events from DNDA JSON API response."""
        try:
            events_data = json.loads(self.raw_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}")

        events = []
        for event_data in events_data:
            # Only extract volunteer/restoration/cleanup related events
            title = event_data.get('title', '')
            if not self._is_volunteer_event(title):
                continue

            event = self._parse_event(event_data)
            if event:
                events.append(event)

        return events

    @classmethod
    def fetch(cls) -> 'DNDAExtractor':
        """Fetch events from DNDA API."""
        # Build the API URL with appropriate date range
        # Get events from today to 3 months from now

        today = datetime.now()
        end_date = today + timedelta(days=30)

        # Format dates for API (YYYY-MM-DDTHH:MM:SS format)
        start_param = today.strftime("%Y-%m-%dT00:00:00")
        end_param = end_date.strftime("%Y-%m-%dT23:59:59")

        # Build query parameters
        params = {
            'show_past_events': '0',
            'show_only_past_events': '0',
            'show_only_one_occurrence': '0',
            'categories': '',
            'multiCategories': '',
            'location': 'undefined',
            'organizer': 'undefined',
            'speaker': 'undefined',
            'tag': 'undefined',
            'label': 'undefined',
            'cost_min': 'undefined',
            'cost_max': 'undefined',
            'display_label': '1',
            'reason_for_cancellation': '',
            'is_category_page': '',
            'cat_id': '',
            'local_time': '',
            'filter_category': '',
            'filter_location': '',
            'filter_organizer': '',
            'filter_label': '',
            'filter_tag': '',
            'filter_author': '',
            'locale': 'en',
            'lang': 'en',
            'startParam': start_param,
            'endParam': end_param,
            'timeZone': '-7'  # Pacific timezone offset
        }

        # Make the request with appropriate headers
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(
            API_BASE_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        return cls(response.text)

    def _is_volunteer_event(self, title: str) -> bool:
        """Check if the event title indicates a volunteer activity."""
        volunteer_keywords = [
            'volunteer', 'restoration', 'cleanup', 'stewardship',
            'weeding', 'planting', 'invasive', 'forest'
        ]
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in volunteer_keywords)

    def _parse_event(self, event_data: dict) -> Optional[Event]:
        """Parse a single event from DNDA JSON data."""
        try:
            # Extract basic fields
            event_id = event_data.get('id')
            title = event_data.get('title', '').strip()
            url = event_data.get('url', '')

            # Create source_id from event ID
            source_id = str(
                event_id) if event_id else self._create_fallback_source_id(title)

            # Parse start and end times (already in Pacific timezone)
            start_str = event_data.get('start', '')
            end_str = event_data.get('end', '')

            start = parser.parse(start_str).astimezone(timezone.utc)
            end = parser.parse(end_str).astimezone(timezone.utc)

            # Extract venue/location information
            venue = self._extract_venue(event_data)
            address = event_data.get('location', '').strip() or None

            return Event(
                source=self.source,
                source_id=source_id,
                title=title,
                start=start,
                end=end,
                venue=venue,
                address=address,
                url=HttpUrl(normalize_url(url)),
                cost=None,  # DNDA events appear to be free
                latitude=None,
                longitude=None,
                tags=[],
                same_as=None
            )

        except Exception as e:
            # Log the error but continue processing other events
            print(
                f"Error parsing DNDA event {event_data.get('id', 'unknown')}: {e}")
            return None

    def _extract_venue(self, event_data: dict) -> str:
        """Extract venue name from event data."""
        # First try to extract from description
        description = event_data.get('description', '')
        if description:
            venue = self._extract_venue_from_description(description)
            if venue:
                return venue

        # Fallback to location field or a default
        location = event_data.get('location', '').strip()
        if location:
            # Try to extract park/venue name from address
            venue = self._extract_venue_from_address(location)
            if venue:
                return venue

        return "DNDA Event Location"

    def _extract_venue_from_description(self, description: str) -> Optional[str]:
        """Extract venue name from HTML description."""
        soup = BeautifulSoup(description, 'html.parser')

        # Look for "Location:" followed by a link or text
        location_pattern = r'<strong>Location:\s*</strong>\s*<a[^>]*>([^<]+)</a>'
        match = re.search(location_pattern, description, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Look for common park names in the text
        text = soup.get_text()
        park_patterns = [
            # "at ParkName" pattern
            r'\bat\s+(\w+(?:\s+\w+)*\s+(?:Park|Wetland|Bog))\b',
            # Just park names with word boundaries
            r'\b(\w+(?:\s+\w+)*\s+(?:Park|Wetland|Bog))\b',
            r'\b(Camp\s+\w+)\b',
        ]

        for pattern in park_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def _extract_venue_from_address(self, address: str) -> Optional[str]:
        """Extract venue name from address string."""
        # Common venue patterns in addresses
        venue_patterns = [
            # Optional "at" prefix, multi-word park names
            r'(?:at\s+)?(\w+(?:\s+\w+)*\s+(?:Park|Wetland|Bog))',
            r'(Camp\s+\w+)',
        ]

        for pattern in venue_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def _create_fallback_source_id(self, title: str) -> str:
        """Create a fallback source_id from title."""
        return re.sub(r'[^a-z0-9]+', '-', title.lower())[:64].strip('-')
