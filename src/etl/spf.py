import json
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pydantic import HttpUrl

from .base import BaseExtractor
from .url_utils import normalize_url
from ..models import Event

SPF_EVENTS_URL = "https://www.seattleparksfoundation.org/events/"


class SPFExtractor(BaseExtractor):
    """Seattle Parks Foundation extractor - parses schema.org JSON-LD data."""
    source = "SPF"

    @classmethod
    def fetch(cls):
        """Fetch raw HTML from the SPF events page."""
        html = requests.get(SPF_EVENTS_URL, timeout=30).text
        return cls(html)

    def extract(self) -> List[Event]:
        """Extract events from SPF website JSON-LD schema.org data."""
        soup = BeautifulSoup(self.raw_data, "html.parser")
        events = []

        # Find the JSON-LD script tag containing event data
        json_scripts = soup.find_all("script", type="application/ld+json")

        for script in json_scripts:
            try:
                script_content = script.get_text()
                if not script_content:
                    continue

                data = json.loads(script_content)
                # The data might be a list of events or a single event
                if isinstance(data, list):
                    event_list = data
                else:
                    event_list = [data]

                for event_data in event_list:
                    if event_data.get("@type") == "Event":
                        event = self._parse_event(event_data)
                        if event:
                            events.append(event)

            except (json.JSONDecodeError, KeyError):
                # Skip malformed JSON or missing keys
                continue

        return events

        return events

    def _parse_event(self, event_data: dict) -> Optional[Event]:
        """Parse a single event from schema.org JSON-LD data."""
        try:
            # Required fields
            title = event_data.get("name", "").strip()
            if not title:
                return None

            url = event_data.get("url", "").strip()
            if not url:
                return None

            start_date_str = event_data.get("startDate", "")
            end_date_str = event_data.get("endDate", "")

            if not start_date_str or not end_date_str:
                return None

            # Parse dates
            start_date = parser.isoparse(start_date_str)
            end_date = parser.isoparse(end_date_str)

            # Optional fields
            venue = None
            address = None

            location = event_data.get("location")
            if location and isinstance(location, dict):
                venue = location.get("name", "").strip()
                address_obj = location.get("address")
                if address_obj and isinstance(address_obj, dict):
                    street = address_obj.get("streetAddress", "")
                    locality = address_obj.get("addressLocality", "")
                    region = address_obj.get("addressRegion", "")

                    address_parts = [part for part in [
                        street, locality, region] if part]
                    address = ", ".join(
                        address_parts) if address_parts else None

            # Check if this is a Green Seattle Partnership event
            organizer = event_data.get("organizer")
            is_gsp_event = False
            if organizer and isinstance(organizer, dict):
                organizer_name = organizer.get("name", "")
                organizer_url = organizer.get("sameAs", "") or organizer.get("url", "")
                if "Green Seattle Partnership" in organizer_name or "greenseattle.org" in organizer_url:
                    is_gsp_event = True

            # Generate a source_id from URL
            source_id = url.split(
                "/")[-2] if url.endswith("/") else url.split("/")[-1]
            if not source_id:
                source_id = str(hash(url))

            return Event(
                source=self.source,
                source_id=source_id,
                title=title,
                start=start_date,
                end=end_date,
                venue=venue,
                address=address,
                url=HttpUrl(normalize_url(url)),
                cost=None,  # Not available in schema.org data
                latitude=None,  # Not available in schema.org data
                longitude=None,  # Not available in schema.org data
                tags=["Green Seattle Partnership"] if is_gsp_event else []  # Tag GSP events
            )

        except (ValueError, TypeError, KeyError):
            # Skip events with parsing errors
            return None
