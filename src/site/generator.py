from pathlib import Path
from datetime import timezone
from urllib.parse import quote_plus
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itertools import groupby
from ..database import get_canonical_events_future
from ..models import SEATTLE_TZ


def build(output_dir: Path):
    # Get future canonical events (already deduplicated)
    canonical_events = get_canonical_events_future()

    # Convert events to dict format for template compatibility
    event_dicts = []
    for event in canonical_events:
        # Convert UTC times to Pacific time for display
        start_utc = event.start.replace(
            tzinfo=timezone.utc) if event.start.tzinfo is None else event.start.astimezone(timezone.utc)
        end_utc = event.end.replace(
            tzinfo=timezone.utc) if event.end.tzinfo is None else event.end.astimezone(timezone.utc)

        # Convert to Pacific time for display
        start_pacific = start_utc.astimezone(SEATTLE_TZ)
        end_pacific = end_utc.astimezone(SEATTLE_TZ)

        # Check if this is a date-only event (midnight UTC with zero duration)
        is_date_only = (event.start.hour == 0 and event.start.minute == 0 and
                        event.start.second == 0 and event.start == event.end)

        event_dict = {
            "canonical_id": event.canonical_id,
            "title": event.title,
            "start": start_pacific,  # Display time in Pacific timezone
            "end": end_pacific,      # Display time in Pacific timezone
            "start_utc": start_utc,  # UTC time for Google Calendar
            "end_utc": end_utc,      # UTC time for Google Calendar
            "venue": event.venue,
            "address": event.address,
            "url": str(event.url),
            "cost": event.cost,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "tags": ",".join(event.tags) if event.tags else "",
            "is_date_only": is_date_only,  # Whether this is a date-only event
            "source_events": event.source_events,  # List of source events that were merged
            # Add Google Maps URL for addresses
            "maps_url": f"https://www.google.com/maps/search/{quote_plus(event.address)}" if event.address and event.address.lower() != 'none' else None
        }
        event_dicts.append(event_dict)

    # Sort events by start time
    event_dicts.sort(key=lambda e: e['start'])

    # Group events by date
    events_by_date = []
    for date, events in groupby(event_dicts, key=lambda e: e['start'].date()):
        events_by_date.append({
            'date': date,
            'events': list(events)
        })

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape()
    )
    tmpl = env.get_template("index.html.j2")
    html = tmpl.render(events_by_date=events_by_date)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html, encoding="utf-8")
