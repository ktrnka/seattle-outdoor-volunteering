import re
from typing import List, Optional
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pydantic import BaseModel, ConfigDict, HttpUrl

from src.etl.date_utils import parse_range

from .base import BaseListExtractor
from ..models import Event, SEATTLE_TZ

SPU_CLEANUP_URL = "https://www.seattle.gov/utilities/volunteer/all-hands-neighborhood-cleanup"


class SPUSourceEvent(BaseModel):
    """Structured data extracted from SPU All Hands Neighborhood Cleanup table."""
    model_config = ConfigDict(from_attributes=True)

    date: str  # Raw date string like "Saturday, August 9"
    neighborhood: str
    location: str  # Full location text from the cell
    google_maps_link: Optional[str] = None
    start_time: str  # Raw time string like "10 am – 12 pm"
    end_time: Optional[str] = None  # Parsed from start_time if available


class SPUExtractor(BaseListExtractor):
    """Seattle Public Utilities All Hands Neighborhood Cleanup extractor."""
    source = "SPU"

    @classmethod
    def fetch(cls):
        """Fetch raw HTML from the SPU cleanup events page."""
        html = requests.get(SPU_CLEANUP_URL, timeout=30).text
        return cls(html)

    def extract(self) -> List[Event]:
        """Extract cleanup events from the SPU website table."""
        soup = BeautifulSoup(self.raw_data, "html.parser")
        events = []

        # Find the table containing events
        # The table has headers: Date, Neighborhood, Meeting Location, Time
        table = soup.find("table")
        if not table:
            return events

        # Find all table rows (skip the header row)
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
        else:
            all_rows = table.find_all("tr")
            rows = all_rows[1:] if len(all_rows) > 1 else []

        for row in rows:
            cells = row.find_all(["th", "td"])
            if len(cells) >= 4:
                # Step 1: Extract SPUSourceEvent from table row
                spu_event = self._extract_spu_source_event(cells)
                if spu_event:
                    # Step 2: Convert SPUSourceEvent to Event
                    event = self._convert_to_event(spu_event)
                    if event:
                        events.append(event)

        return events

    def _extract_spu_source_event(self, cells) -> Optional[SPUSourceEvent]:
        """Extract structured SPUSourceEvent from table row cells."""
        try:
            # Extract raw data from cells
            date = cells[0].get_text(strip=True)
            neighborhood = cells[1].get_text(strip=True)

            # Extract location information from the location cell
            location_cell = cells[2]
            location = location_cell.get_text(strip=True)
            # Clean up location text (remove extra whitespace, line breaks)
            location = re.sub(r'\s+', ' ', location).strip()

            # Extract Google Maps link if present
            google_maps_link = None
            location_link = location_cell.find("a")
            if location_link and "maps.app.goo.gl" in location_link.get("href", ""):
                google_maps_link = location_link.get("href")

            # Extract time information
            time_text = cells[3].get_text(strip=True)

            # Parse start and end times from the time text
            start_time = time_text
            end_time = None
            time_parts = re.split(r'[–\-]', time_text)
            if len(time_parts) == 2:
                start_time = time_parts[0].strip()
                end_time = time_parts[1].strip()

            return SPUSourceEvent(
                date=date,
                neighborhood=neighborhood,
                location=location,
                google_maps_link=google_maps_link,
                start_time=start_time,
                end_time=end_time
            )

        except Exception:
            return None

    def _convert_to_event(self, spu_event: SPUSourceEvent) -> Optional[Event]:
        """Convert SPUSourceEvent to Event model."""
        try:
            try:
                start_datetime, end_datetime = parse_range(
                    spu_event.date, f"{spu_event.start_time} - {spu_event.end_time}", SEATTLE_TZ)
                start_datetime = start_datetime.astimezone(timezone.utc)
                end_datetime = end_datetime.astimezone(timezone.utc)
            except Exception:
                print(
                    f"Failed to parse date/time for event: {spu_event.date}, {spu_event.start_time} - {spu_event.end_time}")
                return None

            # Parse venue and address from location string
            venue, address = self._parse_location_and_address(
                spu_event.location)

            # Generate source_id from date and neighborhood
            # Format: "2025-08-09-othello"
            date_str = start_datetime.strftime("%Y-%m-%d")
            neighborhood_clean = re.sub(
                r'[^a-z0-9]', '', spu_event.neighborhood.lower())
            source_id = f"{date_str}-{neighborhood_clean}"

            # Create title
            title = f"All Hands Neighborhood Cleanup - {spu_event.neighborhood}"

            return Event(
                source=self.source,
                source_id=source_id,
                title=title,
                start=start_datetime,
                end=end_datetime,
                venue=venue,
                address=address,
                url=HttpUrl(SPU_CLEANUP_URL),
                source_dict=spu_event.model_dump_json()
            )

        except Exception:
            return None

    def _parse_location_and_address(self, location_text: str) -> tuple[Optional[str], Optional[str]]:
        """Parse venue and address from location text."""
        # Location text patterns:
        # "Mt Baker Lightrail Station"
        # "Akin Building\n(12360 Lake City Way NE)"
        # "Fresh Flours\n(9410 Delridge Wy SW)"
        # "Othello Park"
        # "Hoa Mai Park\n(1224 S King St)"
        # "Pratt Park"

        venue = location_text
        address = None

        # Check if there's an address in parentheses
        paren_match = re.search(r'\(([^)]+)\)', location_text)
        if paren_match:
            address = paren_match.group(1).strip()
            # Remove the parentheses part to get the venue name
            venue = re.sub(r'\s*\([^)]+\)', '', location_text).strip()

        # Clean up venue name (remove line breaks, extra spaces)
        venue = re.sub(r'\s+', ' ', venue).strip()

        return venue, address
