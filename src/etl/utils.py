from typing import List

import sqlite_utils

from ..config import DB_PATH
from ..models import Event


def upsert_events(events: List[Event]) -> None:
    """Insert or update events in the database."""
    db = sqlite_utils.Database(DB_PATH)
    
    # Ensure the events table exists with the correct schema
    table = db["events"]
    table.create({
        "source": str, 
        "source_id": str, 
        "title": str,
        "start": str, 
        "end": str, 
        "venue": str, 
        "address": str,
        "url": str, 
        "cost": str, 
        "latitude": float, 
        "longitude": float, 
        "tags": str
    }, pk=("source", "source_id"), if_not_exists=True)
    
    # Convert events to dict format for sqlite-utils
    event_dicts = []
    for event in events:
        event_dict = {
            "source": event.source,
            "source_id": event.source_id,
            "title": event.title,
            "start": event.start.isoformat(),
            "end": event.end.isoformat(),
            "venue": event.venue,
            "address": event.address,
            "url": str(event.url),
            "cost": event.cost,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "tags": ",".join(event.tags) if event.tags else ""
        }
        event_dicts.append(event_dict)
    
    # Upsert events (insert or replace on conflict)
    table.upsert_all(event_dicts)
