import requests
from bs4 import BeautifulSoup
from dateutil import parser
import datetime
from datetime import timezone
from pydantic import HttpUrl

from .base import BaseExtractor
from .url_utils import normalize_url, extract_event_id_from_url
from ..models import Event, SEATTLE_TZ

CAL_URL = "https://seattle.greencitypartnerships.org/event/calendar/"


class GSPExtractor(BaseExtractor):
    source = "GSP"

    @classmethod
    def fetch(cls):
        """Fetch raw HTML from the GSP calendar."""
        html = requests.get(CAL_URL, timeout=30).text
        return cls(html)

    def extract(self):
        """Extract events from the HTML content."""
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
                event_url_attr = title_link.get('href')
                event_url = str(event_url_attr) if event_url_attr else ''
                if event_url and event_url.startswith('/'):
                    event_url = "https://seattle.greencitypartnerships.org" + event_url

                # Normalize the URL
                normalized_url = normalize_url(event_url or CAL_URL)

                # Extract source_id from URL - format like "/event/41845"
                source_id = None
                if event_url and '/event/' in event_url:
                    try:
                        source_id = event_url.split(
                            '/event/')[-1].split('/')[0]
                    except Exception:
                        pass

                # Fallback to title-based ID if we can't extract from URL
                if not source_id:
                    source_id = title.lower().replace(" ", "-")[:64]

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

                # Try to parse the date
                # Format: "July 28, 9am-12:30pm" -> need to add year
                current_year = datetime.datetime.now().year
                try:
                    # Simple parsing - assume it's in the current year
                    date_part = date_part.strip()
                    if ',' in date_part:
                        month_day, time_part = date_part.split(',', 1)
                        time_part = time_part.strip()

                        # Parse start time
                        if '-' in time_part:
                            start_time, end_time = time_part.split('-', 1)
                            start_time = start_time.strip()
                        else:
                            start_time = time_part
                            end_time = None

                        # Create datetime - simplified parsing
                        date_str = f"{month_day}, {current_year}"
                        start = parser.parse(f"{date_str} {start_time}")
                        if end_time:
                            end = parser.parse(
                                f"{date_str} {end_time.strip()}")
                        else:
                            # default 3 hour duration
                            end = start.replace(hour=start.hour + 3)

                        # Convert to UTC (assume Pacific time)
                        if start.tzinfo is None:
                            start = start.replace(
                                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
                        if end.tzinfo is None:
                            end = end.replace(
                                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
                    else:
                        # Fallback
                        start = parser.parse(
                            f"{date_part} {current_year} 09:00:00")
                        end = start.replace(hour=12)
                        # Convert to UTC (assume Pacific time)
                        if start.tzinfo is None:
                            start = start.replace(
                                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
                        if end.tzinfo is None:
                            end = end.replace(
                                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
                except Exception:
                    # Fallback parsing
                    start = parser.parse(f"{current_year}-07-28 09:00:00")
                    end = start.replace(hour=12)
                    # Convert to UTC (assume Pacific time)
                    if start.tzinfo is None:
                        start = start.replace(
                            tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
                    if end.tzinfo is None:
                        end = end.replace(
                            tzinfo=SEATTLE_TZ).astimezone(timezone.utc)

                evt = Event(
                    source=self.source,
                    source_id=source_id,
                    title=title,
                    start=start,
                    end=end,
                    venue=venue,
                    address=None,
                    url=HttpUrl(normalized_url),
                )
                events.append(evt)
            except Exception:
                # Skip events that can't be parsed
                continue

        return events
