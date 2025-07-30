from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo
from enum import Enum

from pydantic import BaseModel, ConfigDict, HttpUrl

# Seattle timezone
SEATTLE_TZ = ZoneInfo('America/Los_Angeles')


class RecurringPattern(str, Enum):
    """Supported recurring patterns for manual events."""
    FIRST_SATURDAY = "first_saturday"
    FIRST_SUNDAY = "first_sunday"
    SECOND_SATURDAY = "second_saturday"
    SECOND_SUNDAY = "second_sunday"
    THIRD_SATURDAY = "third_saturday"
    THIRD_SUNDAY = "third_sunday"
    FOURTH_SATURDAY = "fourth_saturday"
    FOURTH_SUNDAY = "fourth_sunday"


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source:   str
    source_id: str
    title:    str
    start:    datetime  # Should be timezone-aware (UTC)
    end:      datetime  # Should be timezone-aware (UTC)
    venue:    Optional[str] = None
    address:  Optional[str] = None
    url:      HttpUrl
    cost:     Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags:     Optional[List[str]] = []
    # URL of the canonical/primary version of this event (from raw source data)
    same_as: Optional[HttpUrl] = None
    # Source-specific structured data as JSON dict
    source_dict: Optional[str] = None

    def has_time_info(self) -> bool:
        """
        Check if this event has actual time information or is date-only.

        Returns True if the event has specific time information,
        False if it's a date-only event (zero duration).
        """
        # Events with zero duration are date-only events
        return self.start != self.end

    def is_date_only(self) -> bool:
        """Check if this is a date-only event (time unknown/not specified)."""
        return not self.has_time_info()


class CanonicalEvent(BaseModel):
    """Canonical event created by merging duplicate events from multiple sources."""
    model_config = ConfigDict(from_attributes=True)

    canonical_id: str  # Generated unique ID for the canonical event
    title: str
    start: datetime  # Should be timezone-aware (UTC)
    end: datetime  # Should be timezone-aware (UTC)
    venue: Optional[str] = None
    address: Optional[str] = None
    url: HttpUrl  # Preferred registration URL
    cost: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags: Optional[List[str]] = []
    source_events: List[str] = []  # List of (source, source_id) pairs

    def has_time_info(self) -> bool:
        """
        Check if this event has actual time information or is date-only.

        Returns True if the event has specific time information,
        False if it's a date-only event (zero duration).
        """
        # Events with zero duration are date-only events
        return self.start != self.end

    def is_date_only(self) -> bool:
        """Check if this is a date-only event (time unknown/not specified)."""
        return not self.has_time_info()

    def get_event_type(self) -> str:
        """
        Determine the event type based on tags, URL, and title.

        Returns:
            str: One of 'parks', 'cleanup', or 'other'
        """
        # Check URL first for Green Seattle Partnership
        if self.url and 'seattle.greencitypartnerships.org' in str(self.url):
            return 'parks'

        # Check title for specific keywords
        title_lower = self.title.lower()
        if 'cleanup' in title_lower:
            return 'cleanup'
        if 'forest restoration' in title_lower:
            return 'parks'

        # Check tags
        if self.tags:
            tags_lower = [tag.lower() for tag in self.tags]

            # Check for cleanup events
            cleanup_indicators = ['cleanup', 'litter patrol']
            if any(indicator in tag for tag in tags_lower for indicator in cleanup_indicators):
                return 'cleanup'

            # Check for parks/restoration events
            parks_indicators = [
                'green seattle partnership', 'volunteer/work party']
            if any(indicator in tag for tag in tags_lower for indicator in parks_indicators):
                return 'parks'

        # Everything else
        return 'other'


class EventGroupMembership(BaseModel):
    """Represents membership of a source event in a canonical event group."""
    model_config = ConfigDict(from_attributes=True)

    canonical_id: str
    source: str
    source_id: str


class ETLRun(BaseModel):
    """Tracks ETL runs for each data source."""
    model_config = ConfigDict(from_attributes=True)

    source: str  # Data source identifier (e.g., "GSP", "SPR", "SPF")
    run_datetime: datetime  # When the ETL run occurred (UTC timezone-aware)
    status: str  # "success" or "failure"
    num_rows: int = 0  # Number of events retrieved
