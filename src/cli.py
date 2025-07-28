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
@click.option('--all-future', is_flag=True, help='Show all future events')
@click.option('--all-past', is_flag=True, help='Show all past events')
def list_events(all_future, all_past):
    """List events. By default shows upcoming events in the next month."""
    if all_future:
        events = database.get_all_future_events()
        title = "All future events"
        show_year = True
    elif all_past:
        events = database.get_all_past_events()
        title = "All past events"
        show_year = True
    else:
        events = database.get_upcoming_events(days_ahead=30)
        title = "Upcoming events (next 30 days)"
        show_year = False

    total_count = len(events)

    if total_count == 0:
        click.echo(f"No {title.lower()} found in database.")
        return

    click.echo(f"{title}: {total_count} events\n")

    for event in events:
        # Format date and time with day of week
        day_of_week = event.start.strftime("%a")  # Mon, Tue, etc.
        if show_year:
            date_str = event.start.strftime("%-m/%-d/%Y")
        else:
            date_str = event.start.strftime("%-m/%-d")
        date_with_day = f"{day_of_week} {date_str}"

        start_time = event.start.strftime("%-I:%M%p").lower()
        end_time = event.end.strftime("%-I:%M%p").lower()

        # Check if the event spans multiple days
        if event.start.date() != event.end.date():
            end_day_of_week = event.end.strftime("%a")
            if show_year:
                end_date_str = event.end.strftime("%-m/%-d/%Y")
            else:
                end_date_str = event.end.strftime("%-m/%-d")
            end_date_with_day = f"{end_day_of_week} {end_date_str}"
            time_str = f"{date_with_day} {start_time} - {end_date_with_day} {end_time}"
        else:
            time_str = f"{date_with_day} from {start_time} - {end_time}"

        venue_str = f" at {event.venue}" if event.venue else ""
        cost_str = f" (Cost: {event.cost})" if event.cost else ""
        tags_str = f" [Tags: {', '.join(event.tags)}]" if event.tags else ""

        # Handle address display - don't show "None"
        if event.address and event.address.lower() != "none":
            address_str = event.address
        else:
            address_str = "Location TBD"

        click.echo(f"• {event.title}")
        click.echo(f"  {time_str}")
        click.echo(f"  {address_str}{venue_str}")
        click.echo(f"  Source: {event.source}{cost_str}{tags_str}")
        click.echo(f"  {event.url}")
        click.echo("")  # Empty line for readability


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


if __name__ == "__main__":
    cli()
