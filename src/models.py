from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, HttpUrl


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source:   str
    source_id: str
    title:    str
    start:    datetime
    end:      datetime
    venue:    Optional[str] = None
    address:  Optional[str] = None
    url:      HttpUrl
    cost:     Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags:     Optional[List[str]] = []
    # URL of the canonical/primary version of this event
    same_as: Optional[HttpUrl] = None
