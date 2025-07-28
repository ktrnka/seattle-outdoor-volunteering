from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape
from ..database import get_all_events_sorted


def build(output_dir: Path):
    events = get_all_events_sorted()
    # Convert events to dict format for template compatibility
    event_dicts = []
    for event in events:
        event_dict = {
            "source": event.source,
            "source_id": event.source_id,
            "title": event.title,
            "start": event.start,  # Keep as datetime object for template
            "end": event.end,      # Keep as datetime object for template
            "venue": event.venue,
            "address": event.address,
            "url": str(event.url),
            "cost": event.cost,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "tags": ",".join(event.tags) if event.tags else ""
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
