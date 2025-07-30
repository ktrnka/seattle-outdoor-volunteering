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


class SPUSourceEvent(BaseModel):
    """Structured data extracted from SPU All Hands Neighborhood Cleanup table."""
    model_config = ConfigDict(from_attributes=True)

    date: str  # Raw date string like "Saturday, August 9"
    neighborhood: str
    location: str  # Full location text from the cell
    google_maps_link: Optional[str] = None
    start_time: str  # Raw time string like "10 am – 12 pm"
    end_time: Optional[str] = None  # Parsed from start_time if available


class DNDASourceEvent(BaseModel):
    """Structured data extracted from DNDA JSON API."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    start: str  # ISO datetime string like "2025-08-09T10:00:00-07:00"
    end: str  # ISO datetime string like "2025-08-09T13:00:00-07:00"
    start_str: Optional[int] = None  # Unix timestamp
    end_str: Optional[int] = None  # Unix timestamp
    image: Optional[str] = None
    url: str
    background_color: Optional[str] = None
    border_color: Optional[str] = None
    description: Optional[str] = None
    localtime: Optional[bool] = None
    location: Optional[str] = None  # Address string
    start_date: Optional[str] = None  # Formatted date like "August 9, 2025"
    start_time: Optional[str] = None  # Formatted time like "10:00 am"
    end_date: Optional[str] = None  # Formatted date like "August 9, 2025"
    end_time: Optional[str] = None  # Formatted time like "1:00 pm"
    start_date_str: Optional[int] = None  # Unix timestamp for date
    end_date_str: Optional[int] = None  # Unix timestamp for date
    start_day: Optional[str] = None  # Day of week like "Saturday"
    labels: Optional[str] = None
    reason_for_cancellation: Optional[str] = None
    loca_time_html: Optional[str] = None
    gridsquare: Optional[str] = None  # HTML img tag


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
