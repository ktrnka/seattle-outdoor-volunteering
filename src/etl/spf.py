import json
from datetime import timezone
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pydantic import BaseModel, ConfigDict, HttpUrl

from ..models import Event
from .base import BaseDetailExtractor, BaseListExtractor
from .url_utils import normalize_url

SPF_EVENTS_URL = "https://www.seattleparksfoundation.org/events/"


class SPFOrganizer(BaseModel):
    """Organizer information from SPF schema.org JSON-LD."""

    model_config = ConfigDict(from_attributes=True)

    name: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    telephone: Optional[str] = None
    email: Optional[str] = None
    same_as: Optional[str] = None


class SPFAddress(BaseModel):
    """Address information from SPF schema.org JSON-LD PostalAddress."""

    model_config = ConfigDict(from_attributes=True)

    type: Optional[str] = None
    street_address: Optional[str] = None
    address_locality: Optional[str] = None
    address_region: Optional[str] = None
    postal_code: Optional[str] = None
    address_country: Optional[str] = None


class SPFLocation(BaseModel):
    """Location information from SPF schema.org JSON-LD."""

    model_config = ConfigDict(from_attributes=True)

    name: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    address: Optional[SPFAddress] = None
    telephone: Optional[str] = None
    same_as: Optional[str] = None


class SPFSourceEvent(BaseModel):
    """Structured data extracted from SPF schema.org JSON-LD."""

    model_config = ConfigDict(from_attributes=True)

    name: str
    description: Optional[str] = None
    image: Optional[str] = None
    url: str
    event_attendance_mode: Optional[str] = None
    event_status: Optional[str] = None
    start_date: str
    end_date: str
    location: Optional[SPFLocation] = None
    organizer: Optional[SPFOrganizer] = None
    performer: Optional[str] = None


class SPFDetailEnrichment(BaseModel):
    """Enrichment data extracted from SPF detail page."""

    model_config = ConfigDict(from_attributes=True)

    website_url: Optional[str] = None  # External website link (e.g., GSP event page)


class SPFExtractor(BaseListExtractor):
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
                        # Step 1: Extract SPFSourceEvent from JSON-LD
                        spf_event = self._extract_spf_source_event(event_data)
                        if spf_event:
                            # Step 2: Convert SPFSourceEvent to Event
                            event = self._convert_to_event(spf_event)
                            if event:
                                events.append(event)

            except (json.JSONDecodeError, KeyError):
                # Skip malformed JSON or missing keys
                continue

        return events

    def _extract_spf_source_event(self, event_data: dict) -> Optional[SPFSourceEvent]:
        """Extract structured SPFSourceEvent from schema.org JSON-LD data."""
        try:
            # Required fields
            name = event_data.get("name", "").strip()
            if not name:
                return None

            url = event_data.get("url", "").strip()
            if not url:
                return None

            start_date = event_data.get("startDate", "")
            end_date = event_data.get("endDate", "")

            if not start_date or not end_date:
                return None

            # Extract optional fields with minimal processing
            description = event_data.get("description", "")
            image = event_data.get("image", "")
            event_attendance_mode = event_data.get("eventAttendanceMode", "")
            event_status = event_data.get("eventStatus", "")
            performer = event_data.get("performer", "")

            # Extract location data
            location_data = None
            location_raw = event_data.get("location")
            if location_raw and isinstance(location_raw, dict):
                # Parse address if present
                address_data = None
                address_raw = location_raw.get("address")
                if address_raw and isinstance(address_raw, dict):
                    address_data = SPFAddress(
                        type=address_raw.get("@type"),
                        street_address=address_raw.get("streetAddress"),
                        address_locality=address_raw.get("addressLocality"),
                        address_region=address_raw.get("addressRegion"),
                        postal_code=address_raw.get("postalCode"),
                        address_country=address_raw.get("addressCountry"),
                    )

                location_data = SPFLocation(
                    name=location_raw.get("name"),
                    description=location_raw.get("description"),
                    url=location_raw.get("url"),
                    address=address_data,
                    telephone=location_raw.get("telephone"),
                    same_as=location_raw.get("sameAs"),
                )

            # Extract organizer data
            organizer_data = None
            organizer_raw = event_data.get("organizer")
            if organizer_raw and isinstance(organizer_raw, dict):
                organizer_data = SPFOrganizer(
                    name=organizer_raw.get("name"),
                    description=organizer_raw.get("description"),
                    url=organizer_raw.get("url"),
                    telephone=organizer_raw.get("telephone"),
                    email=organizer_raw.get("email"),
                    same_as=organizer_raw.get("sameAs"),
                )

            return SPFSourceEvent(
                name=name,
                description=description,
                image=image,
                url=url,
                event_attendance_mode=event_attendance_mode,
                event_status=event_status,
                start_date=start_date,
                end_date=end_date,
                location=location_data,
                organizer=organizer_data,
                performer=performer,
            )

        except (ValueError, TypeError, KeyError):
            return None

    def _convert_to_event(self, spf_event: SPFSourceEvent) -> Optional[Event]:
        """Convert SPFSourceEvent to Event model."""
        try:
            # Parse dates - these should already be in proper format
            start_date = parser.isoparse(spf_event.start_date).astimezone(timezone.utc)
            end_date = parser.isoparse(spf_event.end_date).astimezone(timezone.utc)

            # Extract venue and address from location
            venue = None
            address = None
            if spf_event.location:
                venue = spf_event.location.name
                if spf_event.location.address:
                    address_obj = spf_event.location.address
                    street = address_obj.street_address or ""
                    locality = address_obj.address_locality or ""
                    region = address_obj.address_region or ""

                    address_parts = [part for part in [street, locality, region] if part]
                    address = ", ".join(address_parts) if address_parts else None

            # Check if this is a Green Seattle Partnership event
            is_gsp_event = False
            if spf_event.organizer:
                organizer_name = spf_event.organizer.name or ""
                organizer_url = spf_event.organizer.same_as or spf_event.organizer.url or ""
                if "Green Seattle Partnership" in organizer_name or "greenseattle.org" in organizer_url:
                    is_gsp_event = True

            # Generate a source_id from URL
            source_id = spf_event.url.split("/")[-2] if spf_event.url.endswith("/") else spf_event.url.split("/")[-1]
            if not source_id:
                source_id = str(hash(spf_event.url))

            return Event(
                source=self.source,
                source_id=source_id,
                title=spf_event.name,
                start=start_date,
                end=end_date,
                venue=venue,
                address=address,
                url=HttpUrl(normalize_url(spf_event.url)),
                tags=["Green Seattle Partnership"] if is_gsp_event else [],
                source_dict=json.dumps(spf_event.model_dump()),
            )

        except (ValueError, TypeError, KeyError):
            return None


class SPFDetailExtractor(BaseDetailExtractor):
    """Extractor for SPF event detail pages to get enrichment data."""

    source = "SPF"

    @classmethod
    def fetch(cls, url: str) -> "SPFDetailExtractor":
        """Fetch raw HTML from the detail page URL."""
        # TODO: Handle requests like the other ones
        html = requests.get(url, timeout=30).text
        return cls(url, html)

    def extract(self) -> SPFDetailEnrichment:
        """Extract enrichment data from the detail page using CSS selectors.

        Returns:
            SPFDetailEnrichment with:
            - website_url: The external website link (e.g., GSP event page)
        """
        soup = BeautifulSoup(self.raw_data, "html.parser")

        website_url = None
        # Extract website URL from span.tribe-events-event-url > a
        website_elem = soup.select_one("span.tribe-events-event-url > a")
        if website_elem:
            href = website_elem.get("href")
            if href:
                # normalize_url removes trailing slashes and converts http->https
                website_url = normalize_url(str(href))

        return SPFDetailEnrichment(website_url=website_url)
