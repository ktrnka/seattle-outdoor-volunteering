import re
import xml.etree.ElementTree as ET
import json
from datetime import timezone
from typing import List, Optional, Tuple
import requests
from pydantic import BaseModel, ConfigDict, HttpUrl

from src.etl.date_utils import parse_range_single_string

from .base import BaseListExtractor
from .url_utils import normalize_url
from ..models import Event, SEATTLE_TZ

RSS_URL = "https://www.trumba.com/calendars/volunteer-1.rss"


class SPRSourceData(BaseModel):
    """Structured data extracted from SPR RSS feed."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    description: str
    location: Optional[str] = None
    event_types: Optional[str] = None
    neighborhoods: Optional[str] = None
    parks: Optional[str] = None
    sponsoring_organization: Optional[str] = None
    contact: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    audience: Optional[str] = None
    pre_register: Optional[str] = None
    cost: Optional[str] = None
    link: Optional[str] = None


class SPRExtractor(BaseListExtractor):
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
            event = self._parse_rss_item(item)
            if event:
                events.append(event)

        return events

    def _parse_rss_item(self, item) -> Optional[Event]:
        """Parse a single RSS item into an Event."""
        # Step 1: Extract SPRSourceData from RSS item
        spr_data = self._extract_spr_source_data(item)

        # Step 2: Convert SPRSourceData to Event
        return self._convert_to_event(item, spr_data)

    def _extract_spr_source_data(self, item) -> SPRSourceData:
        """Extract structured SPRSourceData from RSS item."""
        # Extract basic fields
        title_elem = item.find("title")
        title = title_elem.text if title_elem is not None else ""

        description_elem = item.find("description")
        description = description_elem.text if description_elem is not None else ""

        # Initialize all source data fields
        source_data = {
            'title': title,
            'description': self._extract_description_text(description),
            'location': None,
            'event_types': None,
            'neighborhoods': None,
            'parks': None,
            'sponsoring_organization': None,
            'contact': None,
            'contact_phone': None,
            'contact_email': None,
            'audience': None,
            'pre_register': None,
            'cost': None,
            'link': None
        }

        # Parse structured fields from description
        lines = description.replace(
            '<br/>', '\n').replace('<br>', '\n').split('\n')
        lines = [line.strip() for line in lines if line.strip()]

        # First line is usually the address/location
        if lines:
            source_data['location'] = self._clean_html(lines[0])

        # Parse structured fields
        for line in lines:
            original_line = line  # Keep the original before cleaning
            line = self._clean_html(line)

            # Extract structured fields
            if ':' in line:
                field, value = line.split(':', 1)
                field = field.strip().lower()
                value = value.strip()

                if field == "event types":
                    source_data['event_types'] = value
                elif field == "neighborhoods":
                    source_data['neighborhoods'] = value
                elif field == "parks":
                    source_data['parks'] = value
                elif field == "sponsoring organization":
                    source_data['sponsoring_organization'] = value
                elif field == "contact":
                    source_data['contact'] = value
                elif field == "contact phone":
                    source_data['contact_phone'] = value
                elif field == "contact email":
                    # Extract email from potential HTML link
                    email_match = re.search(
                        r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', value)
                    if email_match:
                        source_data['contact_email'] = email_match.group(1)
                    else:
                        source_data['contact_email'] = value
                elif field == "audience":
                    source_data['audience'] = value
                elif field == "pre-register":
                    source_data['pre_register'] = value
                elif field == "cost":
                    source_data['cost'] = value
                elif field == "more info":
                    # Extract the raw URL from the "More info" field without normalization
                    # Use the original line to get the raw href value
                    url_match = re.search(r'href="([^"]+)"', original_line)
                    if url_match:
                        source_data['link'] = url_match.group(1)

        return SPRSourceData(**source_data)

    def _convert_to_event(self, item, spr_data: SPRSourceData) -> Event:
        """Convert SPRSourceData and RSS item metadata to Event."""
        # Define namespace map for XML parsing
        namespaces = {
            'x-trumba': 'http://schemas.trumba.com/rss/x-trumba'
        }

        description_elem = item.find("description")
        description = description_elem.text if description_elem is not None else ""

        link_elem = item.find("link")
        link = normalize_url(
            link_elem.text) if link_elem is not None else RSS_URL

        guid_elem = item.find("guid")
        guid = guid_elem.text if guid_elem is not None else ""

        # Extract x-trumba:weblink (GSP URL for Green Seattle Partnership events)
        weblink_elem = item.find(".//x-trumba:weblink", namespaces)
        same_as_url = HttpUrl(normalize_url(
            weblink_elem.text)) if weblink_elem is not None else None

        # Extract source_id from GUID (format: http://uid.trumba.com/event/187593769)
        source_id = ""
        if guid:
            match = re.search(r'/event/(\d+)', guid)
            if match:
                source_id = match.group(1)

        # Parse datetime from description for Event fields
        address, venue, cost, start_dt, end_dt, tags = self._parse_description(
            description)

        # Skip events that don't have valid datetime information
        if start_dt is None or end_dt is None:
            return None

        # Convert to UTC (assume Pacific time if timezone-naive)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(
                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=SEATTLE_TZ).astimezone(timezone.utc)

        return Event(
            source=self.source,
            source_id=source_id,
            title=spr_data.title,
            start=start_dt,
            end=end_dt,
            venue=venue,
            address=address,
            url=HttpUrl(link),
            cost=cost,
            tags=tags,
            same_as=same_as_url,
            source_dict=json.dumps(spr_data.model_dump())
        )

    @staticmethod
    def _find_datetime_line(lines: List[str]) -> Optional[Tuple[str, int]]:
        """Find the line containing date/time information and return it with its index.

        Returns:
            Tuple of (datetime_line, index) if found, None otherwise
        """
        for i, line in enumerate(lines):
            cleaned_line = SPRExtractor._clean_html(line)
            # Look for a line that contains a year (20XX) and time (am/pm)
            # This distinguishes actual datetime lines from description text that mentions times
            if re.search(r'20\d{2}', cleaned_line) and re.search(r'[^A-Za-z](am|pm)\b', cleaned_line, re.IGNORECASE):
                return cleaned_line, i
        return None

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
        # Format 1: "Address <br/>Date, Time <br/><br/>Description <br/><br/><b>Field</b>: Value"
        # Format 2: "Park Name<br/>Address Line 1<br/>City, State Zip <br/>Date, Time <br/><br/>Description..."

        lines = description.replace(
            '<br/>', '\n').replace('<br>', '\n').split('\n')
        lines = [line.strip() for line in lines if line.strip()]

        if lines:
            # Find the line with date/time information
            if datetime_result := self._find_datetime_line(lines):
                datetime_line, datetime_line_index = datetime_result
                try:
                    # Parse the date and time range
                    start_dt, end_dt = parse_range_single_string(
                        datetime_line, SEATTLE_TZ)
                except ValueError as e:
                    print(f"Error parsing date/time '{datetime_line}': {e}")
                    start_dt = None
                    end_dt = None

                # Build address from lines before the datetime line
                address_lines = [self._clean_html(
                    line) for line in lines[:datetime_line_index]]
                address = ', '.join(address_lines) if address_lines else ""
            else:
                # Fallback: use first line if no datetime found
                address = self._clean_html(lines[0]) if lines else ""

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
                # Only add tags from event types, neighborhoods, parks, and sponsoring organization
                elif field in ["event types", "neighborhoods", "parks", "sponsoring organization"]:
                    if value:
                        tags.append(value.strip())

            # Extract venue from description text as fallback
            elif not venue and ("Join us for a restoration work party at" in line or "park" in line.lower()):
                venue_match = re.search(r'at ([^-]+?)(?:\s*-|$)', line)
                if venue_match:
                    venue = venue_match.group(1).strip()

        return address, venue, cost, start_dt, end_dt, tags

    @staticmethod
    def _clean_html(text: str) -> str:
        """Remove HTML tags and decode entities."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode common HTML entities
        text = text.replace('&amp;', '&').replace(
            '&lt;', '<').replace('&gt;', '>')
        text = text.replace('&nbsp;', ' ').replace('&ndash;', '–')
        return text.strip()

    def _extract_description_text(self, description: str) -> str:
        """Extract the main description text, skipping address/date and structured fields."""
        lines = description.replace(
            '<br/>', '\n').replace('<br>', '\n').split('\n')
        lines = [self._clean_html(line.strip())
                 for line in lines if line.strip()]

        # Skip first two lines (address and date/time)
        # Find lines that are plain text description (not structured fields)
        description_lines = []
        for i, line in enumerate(lines):
            if i < 2:  # Skip address and date lines
                continue
            if ':' in line and any(field in line.lower() for field in
                                   ['event types', 'neighborhoods', 'parks', 'sponsoring organization',
                                   'contact', 'audience', 'pre-register', 'cost', 'more info']):
                # This is a structured field, stop collecting description
                break
            if line and not line.startswith('<'):  # Skip HTML tags
                description_lines.append(line)

        return ' '.join(description_lines).strip()
