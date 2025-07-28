from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, HttpUrl

class Event(BaseModel):
    source:   str
    source_id: str
    title:    str
    start:    datetime
    end:      datetime
    venue:    Optional[str]
    address:  Optional[str]
    url:      HttpUrl
    cost:     Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags:     Optional[List[str]] = []

    class Config:
        from_attributes = True
