from datetime import datetime
from typing import Generator, List, Optional
from zoneinfo import ZoneInfo
from enum import Enum

from pydantic import BaseModel, ConfigDict, HttpUrl

# Seattle timezone
SEATTLE_TZ = ZoneInfo("America/Los_Angeles")


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


class EventCategory(str, Enum):
    """Supported event categories for LLM classification."""
    
    VOLUNTEER_PARKS = "volunteer/parks"
    VOLUNTEER_LITTER = "volunteer/litter"
    SOCIAL_EVENT = "social_event"
    CONCERT = "concert"
    OTHER = "other"


class LLMEventCategorization(BaseModel):
    """Result of LLM-based event categorization."""
    
    model_config = ConfigDict(from_attributes=True)
    
    category: EventCategory
    reasoning: Optional[str] = None  # Optional explanation of the categorization


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source: str
    source_id: str
    title: str
    start: datetime  # Should be timezone-aware (UTC)
    end: datetime  # Should be timezone-aware (UTC)
    venue: Optional[str] = None
    address: Optional[str] = None
    url: HttpUrl
    cost: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags: Optional[List[str]] = []
    # URL of the canonical/primary version of this event (from raw source data)
    same_as: Optional[HttpUrl] = None
    # Source-specific structured data as JSON dict
    source_dict: Optional[str] = None
    # LLM-based categorization (populated when joining with enrichment table)
    llm_categorization: Optional[LLMEventCategorization] = None

    @property
    def start_local(self, tz: ZoneInfo = SEATTLE_TZ) -> datetime:
        return self.start.astimezone(tz)

    @property
    def end_local(self, tz: ZoneInfo = SEATTLE_TZ) -> datetime:
        return self.end.astimezone(tz)

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
    source_events: List[str] = []  # List of source:source_id pairs

    def iter_source_events(self) -> Generator[tuple[str, str], None, None]:
        """
        Iterate over source events that contributed to this canonical event.

        Yields tuples of (source, source_id) for each source event.
        """
        for source_event in self.source_events:
            source, source_id = source_event.split(":", 1)
            yield source, source_id

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
        Determine the event type based on LLM categorization (via tags), URL, and title.
        
        Priority order:
        1. LLM categorization (from tags with llm: prefix)
        2. Hardcoded rules based on URL, title, and other tags

        Returns:
            str: One of 'parks', 'cleanup', 'social_event', 'concert', or 'other'
        """
        # Check for LLM categorization in tags first
        if self.tags:
            for tag in self.tags:
                if tag.startswith("llm:"):
                    llm_category = tag[4:]  # Remove "llm:" prefix
                    # Map LLM categories to frontend event types
                    if llm_category == "volunteer/parks":
                        return "parks"
                    elif llm_category == "volunteer/litter":
                        return "cleanup"
                    elif llm_category == "social_event":
                        return "social_event"
                    elif llm_category == "concert":
                        return "concert"
                    elif llm_category == "other":
                        return "other"
        
        # Fall back to existing hardcoded rules
        # Check URL first for Green Seattle Partnership
        if self.url and "seattle.greencitypartnerships.org" in str(self.url):
            return "parks"

        # Check title for specific keywords
        title_lower = self.title.lower()
        if "cleanup" in title_lower:
            return "cleanup"
        if "forest restoration" in title_lower:
            return "parks"

        # Check tags
        if self.tags:
            tags_lower = [tag.lower() for tag in self.tags]

            # Check for cleanup events
            cleanup_indicators = ["cleanup", "litter patrol"]
            if any(indicator in tag for tag in tags_lower for indicator in cleanup_indicators):
                return "cleanup"

            # Check for parks/restoration events
            parks_indicators = ["green seattle partnership", "volunteer/work party"]
            if any(indicator in tag for tag in tags_lower for indicator in parks_indicators):
                return "parks"

        # Everything else
        return "other"


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
