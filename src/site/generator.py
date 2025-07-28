from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus
from jinja2 import Environment, FileSystemLoader, select_autoescape
from ..database import get_all_events_sorted
from ..models import SEATTLE_TZ
from ..models import SEATTLE_TZ


def build(output_dir: Path):
    # Get all events and filter out past events
    all_events = get_all_events_sorted()
    now_utc = datetime.now(timezone.utc)

    # Filter out past events (events that have already ended)
    future_events = [event for event in all_events if event.end.replace(
        tzinfo=timezone.utc) >= now_utc]

    # Filter out duplicate events
    canonical_events = [event for event in future_events if not event.same_as]

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

        event_dict = {
            "source": event.source,
            "source_id": event.source_id,
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
            # Add Google Maps URL for addresses
            "maps_url": f"https://www.google.com/maps/search/{quote_plus(event.address)}" if event.address and event.address.lower() != 'none' else None
        }
        event_dicts.append(event_dict)

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape()
    )
    tmpl = env.get_template("index.html.j2")
    html = tmpl.render(events=event_dicts)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html, encoding="utf-8")
