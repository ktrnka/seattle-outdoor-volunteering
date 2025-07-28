import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List
import requests
from dateutil import parser
from pydantic import HttpUrl

from .base import BaseExtractor
from .url_utils import normalize_url
from ..models import Event

RSS_URL = "https://www.trumba.com/calendars/volunteer-1.rss"


class SPRExtractor(BaseExtractor):
    """Seattle Parks & Recreation extractor for RSS feed."""
    source = "SPR"

    @classmethod
    def fetch(cls):
        """Fetch raw RSS content from SPR volunteer feed."""
        response = requests.get(RSS_URL, timeout=30)
        return cls(response.text)

    def extract(self) -> List[Event]:
        """Extract events from SPR volunteer RSS feed."""
        try:
            root = ET.fromstring(self.raw_data)
        except ET.ParseError:
            # Return empty list for malformed XML
            return []

        events = []

        # Find all item elements in the RSS feed
        for item in root.findall(".//item"):
            try:
                event = self._parse_rss_item(item)
                if event:
                    events.append(event)
            except Exception:
                # Skip events that can't be parsed
                continue

        return events

    def _parse_rss_item(self, item) -> Event:
        """Parse a single RSS item into an Event."""
        # Define namespace map for XML parsing
        namespaces = {
            'x-trumba': 'http://schemas.trumba.com/rss/x-trumba'
        }

        # Extract basic fields
        title_elem = item.find("title")
        title = title_elem.text if title_elem is not None else ""

        link_elem = item.find("link")
        link = link_elem.text if link_elem is not None else ""

        description_elem = item.find("description")
        description = description_elem.text if description_elem is not None else ""

        guid_elem = item.find("guid")
        guid = guid_elem.text if guid_elem is not None else ""

        # Extract x-trumba:weblink (GSP URL for Green Seattle Partnership events)
        weblink_elem = item.find(".//x-trumba:weblink", namespaces)
        same_as_url = normalize_url(
            weblink_elem.text) if weblink_elem is not None else ""

        # Extract source_id from GUID (format: http://uid.trumba.com/event/187593769)
        source_id = ""
        if guid:
            match = re.search(r'/event/(\d+)', guid)
            if match:
                source_id = match.group(1)

        # Parse description for structured data
        address, venue, cost, start_dt, end_dt, tags = self._parse_description(
            description)

        # Ensure we have valid datetime values
        if start_dt is None:
            start_dt = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
        if end_dt is None:
            end_dt = start_dt.replace(hour=12)

        # Ensure we have a valid URL
        if not link or not link.startswith('http'):
            event_url = RSS_URL
        else:
            event_url = link

        return Event(
            source=self.source,
            source_id=source_id,
            title=title,
            start=start_dt,
            end=end_dt,
            venue=venue,
            address=address,
            url=HttpUrl(normalize_url(event_url)),
            cost=cost,
            tags=tags,
            same_as=same_as_url
        )

    def _parse_description(self, description: str):
        """Parse the description field to extract structured information."""
        # Initialize defaults
        address = ""
        venue = None
        cost = None
        start_dt = None
        end_dt = None
        tags = []

        # Parse the HTML-like description
        # Format: "Address <br/>Date, Time <br/><br/>Description <br/><br/><b>Field</b>: Value"

        lines = description.replace(
            '<br/>', '\n').replace('<br>', '\n').split('\n')
        lines = [line.strip() for line in lines if line.strip()]

        if len(lines) >= 2:
            # First line is usually the address
            address = self._clean_html(lines[0])

            # Second line is usually date and time
            datetime_line = self._clean_html(lines[1])
            start_dt, end_dt = self._parse_datetime(datetime_line)

        # Parse structured fields (format: <b>Field</b>: Value)
        for line in lines:
            line = self._clean_html(line)

            # Extract structured fields
            if ':' in line:
                field, value = line.split(':', 1)
                field = field.strip().lower()
                value = value.strip()

                if field == "cost" and value:
                    cost = value
                elif field == "parks" and value and not venue:
                    # Extract park name from parks field, handle links like "Green Lake Park"
                    parks_match = re.search(r'>([^<]+)<', value)
                    if parks_match:
                        venue = parks_match.group(1).strip()
                    else:
                        venue = value
                elif field in ["event types", "neighborhoods", "sponsoring organization",
                               "contact", "contact email", "contact phone", "audience"]:
                    if value:
                        tags.append(value)

            # Extract venue from description text as fallback
            elif not venue and ("Join us for a restoration work party at" in line or "park" in line.lower()):
                venue_match = re.search(r'at ([^-]+?)(?:\s*-|$)', line)
                if venue_match:
                    venue = venue_match.group(1).strip()

        return address, venue, cost, start_dt, end_dt, tags

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and decode entities."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode common HTML entities
        text = text.replace('&amp;', '&').replace(
            '&lt;', '<').replace('&gt;', '>')
        text = text.replace('&nbsp;', ' ').replace('&ndash;', '–')
        return text.strip()

    def _parse_datetime(self, datetime_line: str):
        """Parse datetime information from a line like 'Sunday, July 27, 2025, 8–11am'."""
        # Default fallback
        current_year = datetime.now().year
        start_dt = datetime(current_year, 7, 28, 9, 0)  # fallback
        end_dt = datetime(current_year, 7, 28, 12, 0)   # fallback

        try:
            # Handle various formats like:
            # "Sunday, July 27, 2025, 8–11am"
            # "Sunday, July 27, 2025, 8:45–11am"
            # "Sunday, July 27, 2025, 10:30am–2:30pm"

            # Extract date part and time part
            if ',' in datetime_line:
                parts = datetime_line.split(',')
                if len(parts) >= 4:
                    # Format: "Sunday, July 27, 2025, 8–11am"
                    # "July 27, 2025"
                    date_part = f"{parts[1].strip()}, {parts[2].strip()}"
                    time_part = parts[3].strip()  # "8–11am"
                elif len(parts) >= 3:
                    # Format might be "July 27, 2025, 8–11am"
                    date_part = f"{parts[0].strip()}, {parts[1].strip()}"
                    time_part = parts[2].strip()
                else:
                    return start_dt, end_dt
            else:
                return start_dt, end_dt

            # Parse time range (e.g., "8–11am", "10:30am–2:30pm")
            time_match = re.search(
                r'(\d{1,2}(?::\d{2})?(?:am|pm)?)\s*[–-]\s*(\d{1,2}(?::\d{2})?(?:am|pm)?)', time_part)
            if time_match:
                start_time_str = time_match.group(1)
                end_time_str = time_match.group(2)

                # Handle case where start time doesn't have am/pm but end time does
                if not re.search(r'(am|pm)', start_time_str):
                    end_period = re.search(r'(am|pm)', end_time_str)
                    if end_period:
                        start_time_str += end_period.group(1)

                # Parse full datetime
                start_dt = parser.parse(f"{date_part} {start_time_str}")
                end_dt = parser.parse(f"{date_part} {end_time_str}")

        except Exception:
            # Keep fallback values
            pass

        return start_dt, end_dt
