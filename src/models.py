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


class ManualEventDefinition(BaseModel):
    """Definition of a recurring manual event."""
    model_config = ConfigDict(from_attributes=True)

    id: str  # Unique identifier for this recurring event definition
    title: str
    description: Optional[str] = None
    recurring_pattern: RecurringPattern
    venue: Optional[str] = None
    address: Optional[str] = None
    url: HttpUrl
    cost: Optional[str] = None
    tags: Optional[List[str]] = []


class ManualEventsConfig(BaseModel):
    """Configuration file structure for manual events."""
    model_config = ConfigDict(from_attributes=True)

    recurring_events: List[ManualEventDefinition] = []


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

    def has_time_info(self) -> bool:
        """
        Check if this event has actual time information or is date-only.

        Returns True if the event has specific time information,
        False if it's a date-only event (zero duration at midnight in Seattle time).
        """
        # Convert UTC time to Seattle time to check if it's midnight
        start_seattle = self.start.astimezone(SEATTLE_TZ)

        # Check if it's a zero-duration event at midnight Seattle time
        # This indicates a date-only event
        return not (self.start == self.end and
                    start_seattle.hour == 0 and
                    start_seattle.minute == 0 and
                    start_seattle.second == 0)

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
