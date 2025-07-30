from pathlib import Path
from datetime import timezone
from urllib.parse import quote_plus
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itertools import groupby
from ..database import get_future_canonical_events, get_source_updated_stats, get_events_by_canonical_id
from ..models import SEATTLE_TZ


def build(output_dir: Path):
    # Get future canonical events (already deduplicated)
    canonical_events = get_future_canonical_events()

    # Convert events to dict format for template compatibility
    event_dicts = []
    for event in canonical_events:
        # Convert UTC times to Pacific time for display
        start_utc = event.start
        end_utc = event.end

        # Convert to Pacific time for display
        start_pacific = start_utc.astimezone(SEATTLE_TZ)
        end_pacific = end_utc.astimezone(SEATTLE_TZ)

        # Use the Event model's is_date_only method for proper timezone handling
        is_date_only = event.is_date_only()

        # Get detailed source events for debug info
        source_events_detail = get_events_by_canonical_id(event.canonical_id)
        debug_source_events = []
        for se in source_events_detail:
            debug_source_events.append({
                "source": se.source,
                "source_id": se.source_id,
                "title": se.title,
                # Convert to Pacific and serialize
                "start": se.start.astimezone(SEATTLE_TZ).isoformat(),
                # Convert to Pacific and serialize
                "end": se.end.astimezone(SEATTLE_TZ).isoformat(),
                "venue": se.venue,
                "address": se.address,
                "url": str(se.url),
                "tags": se.tags,
                "same_as": str(se.same_as) if se.same_as else None,
                "source_dict": se.source_dict
            })

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
            # Count of source events for display
            "source_events_count": len(debug_source_events),
            # Detailed source event data for debug
            "debug_source_events": debug_source_events,
            # Add Google Maps URL for addresses
            "maps_url": f"https://www.google.com/maps/search/{quote_plus(event.address)}" if event.address and event.address.lower() != 'none' else None
        }
        event_dicts.append(event_dict)

    # Sort events by start time, then by title for events with the same start time
    event_dicts.sort(key=lambda e: (e['start'], e['title']))

    # Prepare debug data dictionary
    debug_data = {}
    for event_dict in event_dicts:
        debug_data[event_dict['canonical_id']] = {
            'title': event_dict['title'],
            'source_events': event_dict['debug_source_events']
        }

    # Group events by date
    events_by_date = []
    for date, events in groupby(event_dicts, key=lambda e: e['start'].date()):
        events_by_date.append({
            'date': date,
            'events': list(events)
        })

    # Get source update statistics
    source_stats = get_source_updated_stats()

    # Convert UTC times to Pacific time for display
    source_stats_pacific = {}
    for source, utc_datetime in source_stats.items():
        pacific_datetime = utc_datetime.astimezone(SEATTLE_TZ)
        source_stats_pacific[source] = pacific_datetime

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape()
    )
    tmpl = env.get_template("index.html.j2")
    html = tmpl.render(
        events_by_date=events_by_date,
        source_stats=source_stats_pacific,
        debug_data=debug_data
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html, encoding="utf-8")
