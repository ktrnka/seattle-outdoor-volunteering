from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, HttpUrl

# Seattle timezone
SEATTLE_TZ = ZoneInfo('America/Los_Angeles')


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
    # URL of the canonical/primary version of this event
    same_as: Optional[HttpUrl] = None

    def has_time_info(self) -> bool:
        """
        Check if this event has actual time information or is date-only.

        Returns True if the event has specific time information,
        False if it's a date-only event (zero duration at midnight).
        """
        # Check if it's a zero-duration event at midnight UTC
        # This indicates a date-only event
        return not (self.start == self.end and
                    self.start.hour == 0 and
                    self.start.minute == 0 and
                    self.start.second == 0)

    def is_date_only(self) -> bool:
        """Check if this is a date-only event (time unknown/not specified)."""
        return not self.has_time_info()
