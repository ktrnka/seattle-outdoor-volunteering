import re
from typing import List, Optional
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser
from pydantic import HttpUrl

from .base import BaseExtractor
from ..models import Event, SEATTLE_TZ

SPU_CLEANUP_URL = "https://www.seattle.gov/utilities/volunteer/all-hands-neighborhood-cleanup"


class SPUExtractor(BaseExtractor):
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
                event = self._parse_row(cells)
                if event:
                    events.append(event)

        return events

    def _parse_row(self, cells) -> Optional[Event]:
        """Parse a single table row into an Event."""
        try:
            # Extract data from cells
            date_text = cells[0].get_text(strip=True)
            neighborhood = cells[1].get_text(strip=True)
            location_cell = cells[2]
            time_text = cells[3].get_text(strip=True)

            # Parse location - could have links or plain text
            venue = location_cell.get_text(strip=True)
            # Clean up venue text (remove extra whitespace, line breaks)
            venue = re.sub(r'\s+', ' ', venue).strip()

            # Extract address if there's a link
            address = None
            location_link = location_cell.find("a")
            if location_link and "maps.app.goo.gl" in location_link.get("href", ""):
                link_text = location_link.get_text(strip=True)

                # Check if the link text looks like an address
                # If it contains numbers and street-like words, it's probably an address
                if re.search(r'\d+\s+.*(?:Way|St|Ave|Rd|Blvd|Dr|Ln|Ct|Pl)', link_text, re.IGNORECASE):
                    address = link_text
                    # Extract venue name from text before the link
                    # Look for text before <br> or before parentheses
                    full_html = str(location_cell)
                    # Try to find venue name before <br> tag
                    br_match = re.search(
                        r'>([^<]+)<br', full_html, re.IGNORECASE)
                    if br_match:
                        venue = br_match.group(1).strip()
                    else:
                        # Fallback: look for text before parentheses in the text content
                        paren_match = re.search(r'^([^(]+)', venue)
                        if paren_match:
                            venue = paren_match.group(1).strip()
                else:
                    # Link text is probably the venue name
                    venue = link_text
                    # Look for address in parentheses in the full text
                    full_text = location_cell.get_text()
                    address_match = re.search(r'\(([^)]+)\)', full_text)
                    if address_match:
                        address = address_match.group(1).strip()

            # Parse date - convert from formats like "Saturday, August 9" to full date
            start_datetime, end_datetime = self._parse_date_and_time(
                date_text, time_text)
            if not start_datetime or not end_datetime:
                return None

            # Generate source_id from date and neighborhood
            # Format: "2025-08-09-othello"
            date_str = start_datetime.strftime("%Y-%m-%d")
            neighborhood_clean = re.sub(r'[^a-z0-9]', '', neighborhood.lower())
            source_id = f"{date_str}-{neighborhood_clean}"

            # Create title
            title = f"All Hands Neighborhood Cleanup - {neighborhood}"

            return Event(
                source=self.source,
                source_id=source_id,
                title=title,
                start=start_datetime,
                end=end_datetime,
                venue=venue,
                address=address,
                url=HttpUrl(SPU_CLEANUP_URL),
                tags=["cleanup", "neighborhood", "utilities"]
            )

        except Exception as e:
            # Skip malformed rows
            return None

    def _parse_date_and_time(self, date_text: str, time_text: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """Parse date and time strings into datetime objects."""
        try:
            # Parse date string like "Saturday, August 9"
            # We need to add the year since it's not provided
            current_year = datetime.now().year

            # Try to parse with current year first
            try:
                date_with_year = f"{date_text}, {current_year}"
                parsed_date = parser.parse(date_with_year, fuzzy=True)
            except:
                # If that fails, try next year (events might be for next year)
                date_with_year = f"{date_text}, {current_year + 1}"
                parsed_date = parser.parse(date_with_year, fuzzy=True)

            # Parse time string like "10 am – 12 pm" or "9 am – 12 pm"
            time_parts = re.split(r'[–\-]', time_text)
            if len(time_parts) != 2:
                return None, None

            start_time_str = time_parts[0].strip()
            end_time_str = time_parts[1].strip()

            # Parse start and end times
            start_time = parser.parse(start_time_str, fuzzy=True).time()
            end_time = parser.parse(end_time_str, fuzzy=True).time()

            # Combine date and times
            start_datetime = datetime.combine(parsed_date.date(), start_time)
            end_datetime = datetime.combine(parsed_date.date(), end_time)

            # Assume Seattle local time, convert to UTC
            start_datetime = start_datetime.replace(
                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)
            end_datetime = end_datetime.replace(
                tzinfo=SEATTLE_TZ).astimezone(timezone.utc)

            return start_datetime, end_datetime

        except Exception as e:
            return None, None
