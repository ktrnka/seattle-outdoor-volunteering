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
