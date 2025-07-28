"""
DEPRECATED: This module is deprecated. Use src.database instead.

This module used sqlite_utils and has been replaced with SQLAlchemy-based
database operations in src.database module.
"""

import warnings
from typing import List

import sqlite_utils

from ..config import DB_PATH
from ..models import Event


def upsert_events(events: List[Event]) -> None:
    """
    DEPRECATED: Use src.database.upsert_events instead.

    Insert or update events in the database.
    """
    warnings.warn(
        "etl.utils.upsert_events is deprecated. Use src.database.upsert_events instead.",
        DeprecationWarning,
        stacklevel=2
    )

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
