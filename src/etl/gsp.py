from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Tuple
import json
import re
import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from dateutil import parser
from pydantic import BaseModel, ConfigDict, HttpUrl

from .base import BaseExtractor, BaseDetailExtractor
from .url_utils import normalize_url
from ..models import Event, SEATTLE_TZ

# New API endpoint that returns JSON with up to 100 events
API_URL = "https://seattle.greencitypartnerships.org/event/map/?sEcho=2&iColumns=1&sColumns=&iDisplayStart=0&iDisplayLength=100&sNames=&sort=date"

# Keep the old calendar URL as fallback
CAL_URL = "https://seattle.greencitypartnerships.org/event/calendar/"


def _extract_source_id_from_url(event_url: str) -> str | None:
    """Extract source_id from GSP event URL."""
    if event_url and '/event/' in event_url:
        try:
            return event_url.split('/event/')[-1].split('/')[0]
        except Exception:
            pass
    return None


def parse_time(time_str: str) -> datetime:
    """Parse a time like '9am' or '12:30pm' into a datetime object."""

    try:
        return datetime.strptime(time_str, "%I:%M%p")
    except ValueError:
        return datetime.strptime(time_str, "%I%p")


def parse_gsp_range(event_datetime_str: str, after: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Parse a date like July 28, 9am-12:30pm"""

    date_str, time_range_str = event_datetime_str.split(', ')

    start_str, end_str = time_range_str.split('-')

    partial_date = datetime.strptime(
        date_str + " " + str(date.today().year), "%B %d %Y").date()
    partial_start_time = parse_time(start_str.strip())
    partial_end_time = parse_time(end_str.strip())

    if after and partial_date < after.date():
        # If the date is before the 'after' date, adjust to next year
        partial_date = partial_date.replace(year=after.year + 1)

    start_dt = datetime.combine(partial_date, partial_start_time.time())
    end_dt = datetime.combine(partial_date, partial_end_time.time())

    return start_dt, end_dt


class GSPBaseExtractor(BaseExtractor):
    """Base class for GSP extractors with shared utility methods."""
    source = "GSP"

    @staticmethod
    def _create_fallback_source_id(title):
        """Create a fallback source_id from title."""
        return title.lower().replace(" ", "-")[:64]

    @staticmethod
    def _normalize_event_url(event_url, base_url):
        """Normalize a GSP event URL."""
        if event_url and event_url.startswith('/'):
            event_url = "https://seattle.greencitypartnerships.org" + event_url
        return normalize_url(event_url or base_url)

    @staticmethod
    def _create_date_only_times(year, month, day):
        """Create zero-duration start/end times for date-only events."""
        # Create midnight in Seattle time first, then convert to UTC
        # Date-only events have zero duration (same start/end time)
        start_seattle = datetime(
            year, month, day, 0, 0, 0, tzinfo=SEATTLE_TZ)
        start_utc = start_seattle.astimezone(timezone.utc)
        end_utc = start_utc  # Zero duration indicates this is a date-only event
        return start_utc, end_utc

    @staticmethod
    def _create_event(source_id, title, start, end, venue, url, **extra_fields):
        """Create an Event object with standard GSP formatting."""

        # TODO: Refactor into a GSPSourceEvent -> Shared SourceEvent conversion
        return Event(
            source="GSP",
            source_id=source_id,
            title=title,
            start=start,
            end=end,
            venue=venue,
            address=None,
            url=HttpUrl(url),
            source_dict=json.dumps(extra_fields) if extra_fields else None
        )


class GSPAPIExtractor(GSPBaseExtractor):
    """Extractor for GSP API endpoint (returns up to 100 events)."""

    @classmethod
    def fetch(cls):
        """Fetch raw JSON from the GSP API endpoint."""
        response = requests.get(API_URL, timeout=30, headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        response.raise_for_status()
        return cls(response.text)

    def extract(self):
        """Extract events from the API JSON response."""
        data = json.loads(self.raw_data)
        events = []

        for row in data['aaData']:
            if not row or not row[0]:
                continue

            html_content = row[0]

            # Skip header rows
            if 'class="header"' in html_content:
                continue

            # Parse the HTML content in each row
            try:
                soup = BeautifulSoup(html_content, "html.parser")
                event_div = soup.select_one("div.event")
                if not event_div:
                    continue

                # Extract title and URL
                title_link = event_div.select_one("div.col1 a")
                if not title_link:
                    continue

                title = title_link.get_text(strip=True)
                event_url = str(title_link.get('href', ''))

                # Extract source_id from URL
                source_id = _extract_source_id_from_url(event_url)
                if not source_id:
                    source_id = self._create_fallback_source_id(title)

                # Extract date - format like "07/28/2025"
                date_elem = event_div.select_one("div.col2")
                if not date_elem:
                    continue
                date_text = date_elem.get_text(strip=True)

                # Extract venue
                venue_elem = event_div.select_one("div.col3")
                venue = venue_elem.get_text(strip=True) if venue_elem else None

                # Parse date
                try:
                    # Convert MM/DD/YYYY to datetime
                    date_parts = date_text.split('/')
                    if len(date_parts) == 3:
                        month, day, year = map(int, date_parts)
                        # API only provides date, not time - create date-only event
                        start, end = self._create_date_only_times(
                            year, month, day)
                    else:
                        raise ValueError("Invalid date format")
                except Exception:
                    # Fallback to current date if parsing fails
                    current_date = datetime.now()
                    start, end = self._create_date_only_times(
                        current_date.year, current_date.month, current_date.day)

                normalized_url = self._normalize_event_url(event_url, API_URL)
                evt = self._create_event(
                    source_id, title, start, end, venue, normalized_url)
                events.append(evt)

            except Exception:
                # Skip events that can't be parsed
                continue

        return events


class GSPCalendarExtractor(GSPBaseExtractor):
    """Extractor for GSP calendar HTML page (fallback, limited events)."""

    @classmethod
    def build_url(cls, start_date: date, end_date: date) -> str:
        """Build the calendar URL with date range."""
        url = CAL_URL
        if start_date and end_date:
            url += f"?start={start_date}&end={end_date}"
        return url

    @classmethod
    def fetch(cls):
        """Fetch raw HTML from the GSP calendar."""

        html = requests.get(cls.build_url(
            date.today(), date.today() + timedelta(days=30)), timeout=30).text
        return cls(html)

    def extract(self) -> List[Event]:
        """Extract events from HTML content."""
        soup = BeautifulSoup(self.raw_data, "html.parser")
        events = []

        # Look for event divs in the table
        event_divs = soup.select("div.event")
        for event_div in event_divs:
            try:
                # Extract title from h4 > a
                title_link = event_div.select_one("h4 a")
                if not title_link:
                    continue
                title = title_link.get_text(strip=True)

                # Note: These are relative URLs
                event_url = str(title_link.get('href', ''))

                # Extract source_id from URL
                source_id = _extract_source_id_from_url(event_url)
                if not source_id:
                    source_id = self._create_fallback_source_id(title)

                # Extract date/time info from em tag
                date_info = event_div.select_one("p em")
                if not date_info:
                    continue
                date_text = date_info.get_text(strip=True)

                # Parse date - format like "July 28, 9am-12:30pm @ Burke-Gilman Trail"
                # Extract the date and time part before @
                if '@' in date_text:
                    date_part, venue_part = date_text.split('@', 1)
                    venue = venue_part.strip()
                else:
                    date_part = date_text
                    venue = None

                description_element = event_div.select("p")[1]
                if description_element:
                    description = description_element.find(
                        string=True, recursive=False).strip()  # type: ignore
                else:
                    description = None

                # Parse the date and time range
                start, end = parse_gsp_range(date_part.strip())

                normalized_url = self._normalize_event_url(event_url, CAL_URL)
                evt = self._create_event(
                    source_id, title, start, end, venue, normalized_url, description=description)
                events.append(evt)
            except Exception:
                # Skip events that can't be parsed
                continue

        return events


class GSPDetailEvent(BaseModel):
    """Definition of a recurring manual event."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    source_id: str
    url: HttpUrl
    datetimes: str

    description: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None

    def to_source_event(self) -> Event:
        """Convert to a source event."""
        # Parse a date and time string like "August 1, 2025 9:30am - 11:30am"
        start, end = self.datetimes.split('-')
        start = parser.parse(start.strip()).replace(
            tzinfo=SEATTLE_TZ).astimezone(timezone.utc)

        # Get the date from start and the time from end.strip
        end_time = parser.parse(end.strip()).replace(
            tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
        end = start.replace(
            hour=end_time.hour,
            minute=end_time.minute,
        )

        return Event(
            source=GSPDetailPageExtractor.source,
            source_id=self.source_id,
            title=self.title,
            start=start,
            end=end,
            venue=None,  # No venue info in detail page
            url=HttpUrl(normalize_url(str(self.url))),
            source_dict=self.model_dump_json()
        )


def extract_immediate_text(element):
    """Extract text from an element, handling nested tags."""
    if not element:
        return ""
    immediate_text = ''.join(
        s for s in element.contents if isinstance(s, NavigableString)
    ).strip()
    return immediate_text


class GSPDetailPageExtractor(BaseDetailExtractor):
    """Extractor for GSP detail HTML page."""
    source = "GSP_DETAIL"

    def extract_detail_event(self) -> GSPDetailEvent:
        """Extract event details from the HTML page."""
        soup = BeautifulSoup(self.raw_data, 'html.parser')

        # All the details are under <section class="whitebox panel">
        main_section = soup.select_one("#main")
        assert main_section, "No event details found in the page"

        # The title is in <h2 class="green">
        title_elem = main_section.select_one("h2.green")
        assert title_elem, "No event title found in the details"
        title = title_elem.get_text(strip=True)

        detail_section = main_section.select_one("div.non-map-content")
        assert detail_section, "No event detail section found in the page"

        # Process div.column.left and extract the paragraph tags: activites, ages, num_registered, what_to_bring, where_to_meet, where_to_park
        left_column = detail_section.select_one("div.column.left")
        assert left_column, "No left column found in the details"
        left_paragraphs = left_column.find_all("p")

        # Process div.column.right and extract the paragraph tags: datetimes, contact_line1, contact_line2
        right_column = detail_section.select_one("div.column.right")
        assert right_column, "No right column found in the details"
        right_paragraphs = right_column.find_all("p")

        source_id = _extract_source_id_from_url(self.url)
        assert source_id, "No source_id found in the URL"

        # Pull out the activities section and simplify whitespacing
        description = left_paragraphs[0].get_text(strip=True)
        description = re.sub(r'\s+', ' ', description)

        # Pull out contact info
        contact_name = extract_immediate_text(right_paragraphs[1])
        contact_email = right_paragraphs[1].select_one("a[href^='mailto:']")
        contact_email = contact_email.get_text(
            strip=True) if contact_email else None

        return GSPDetailEvent(
            title=title,
            url=HttpUrl(self.url),
            source_id=source_id,
            datetimes=right_paragraphs[0].get_text(strip=True),
            description=description,
            contact_name=contact_name,
            contact_email=contact_email,
        )

    def extract(self) -> Event:
        """Extract a single event from the detail page."""
        detail_event = self.extract_detail_event()
        return detail_event.to_source_event()

    @classmethod
    def fetch(cls, url: str) -> 'GSPDetailPageExtractor':
        """Fetch raw HTML from the detail page URL."""

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return cls(url, response.text)
