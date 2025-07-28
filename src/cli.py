import click
import gzip
import shutil
from pathlib import Path

from .config import DB_PATH, DB_GZ
from .etl.gsp import GSPExtractor
from .etl.spf import SPFExtractor
from .etl.spr import SPRExtractor
from .site import generator
from . import database


@click.group()
def cli(): ...


@cli.command()
def init_db():
    """Initialize the database by creating tables."""
    database.init_database()
    click.echo(f"Database initialized at {DB_PATH}")


@cli.command()
def etl():
    """Run all extractors and build/compact DB."""
    for extractor_class in [GSPExtractor, SPRExtractor, SPFExtractor]:
        # Fetch raw data and extract events
        extractor = extractor_class.fetch()
        events = extractor.extract()

        # Save events to database
        database.upsert_events(events)

        click.echo(f"{extractor_class.__name__}: {len(events)} events")

    # gzip-compress for committing
    with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
        shutil.copyfileobj(src, dst)


@cli.command()
def list_events():
    """List all events sorted by date for quality checking."""
    events = database.get_all_events_sorted()
    total_count = len(events)

    if total_count == 0:
        click.echo("No events found in database.")
        return

    click.echo(f"Found {total_count} events:\n")

    for event in events:
        # Format the event display
        start_str = event.start.strftime("%Y-%m-%d %H:%M")
        end_str = event.end.strftime("%Y-%m-%d %H:%M")
        venue_str = f" at {event.venue}" if event.venue else ""
        cost_str = f" (Cost: {event.cost})" if event.cost else ""
        tags_str = f" [Tags: {', '.join(event.tags)}]" if event.tags else ""

        click.echo(f"• {event.title}")
        click.echo(f"  Source: {event.source} | ID: {event.source_id}")
        click.echo(f"  Time: {start_str} → {end_str}")
        click.echo(f"  Location: {event.address}{venue_str}")
        click.echo(f"  URL: {event.url}{cost_str}{tags_str}")
        click.echo("")  # Empty line for readability


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


if __name__ == "__main__":
    cli()
