import click
import gzip
import shutil
from pathlib import Path

from .config import DB_PATH, DB_GZ
from .etl.gsp import GSPExtractor
from .etl.spf import SPFExtractor
from .etl.spr import SPRExtractor
from .etl.deduplication import deduplicate_events
from .etl.new_deduplication import deduplicate_events_new, normalize_title
from .site import generator
from . import database


@click.group()
def cli(): ...


@cli.group()
def dev():
    """Development and debugging commands."""
    pass


@cli.command()
def init_db():
    """Initialize the database by creating tables."""
    database.init_database()
    click.echo(f"Database initialized at {DB_PATH}")


@cli.command()
@click.option('--deduplicate-only', is_flag=True, help='Only run deduplication on existing data, skip fetching from sources')
def etl(deduplicate_only):
    """Run all extractors, deduplicate, and build/compact DB."""
    if deduplicate_only:
        # Load existing events from database and re-run deduplication
        click.echo("Loading events from database...")
        all_events = database.get_all_events_sorted()

        if not all_events:
            click.echo(
                "No events found in database. Run 'etl' without --deduplicate-only first.")
            return

        click.echo(f"Loaded {len(all_events)} events from database")
    else:
        # Fetch fresh data from all sources
        all_events = []

        for extractor_class in [GSPExtractor, SPRExtractor, SPFExtractor]:
            # Fetch raw data and extract events
            extractor = extractor_class.fetch()
            events = extractor.extract()
            all_events.extend(events)

            click.echo(f"{extractor_class.__name__}: {len(events)} events")

    # Run deduplication
    deduplicated_events = deduplicate_events(all_events)

    # Count duplicates
    duplicate_count = sum(1 for event in deduplicated_events if event.same_as)
    unique_count = len(deduplicated_events) - duplicate_count

    click.echo(
        f"Deduplication: {len(deduplicated_events)} total events, {unique_count} unique, {duplicate_count} duplicates")

    # Save events to database
    database.upsert_events(deduplicated_events)

    # gzip-compress for committing
    with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
        shutil.copyfileobj(src, dst)


@cli.command()
def deduplicate():
    """Run deduplication on existing events in the database."""
    click.echo("Loading events from database...")
    events = database.get_all_events_sorted()

    if not events:
        click.echo("No events found in database. Run 'etl' first.")
        return

    click.echo(f"Loaded {len(events)} events")
    click.echo("Running deduplication...")

    # Run deduplication
    deduplicated_events = deduplicate_events(events)

    # Count duplicates
    duplicate_count = sum(1 for event in deduplicated_events if event.same_as)
    click.echo(f"Found {duplicate_count} duplicate events")

    # Update database with deduplication results
    database.upsert_events(deduplicated_events)
    click.echo("Database updated with deduplication results")


@cli.command()
@click.option('--show-examples', is_flag=True, help='Show examples of how events are being merged')
@click.option('--verbose', is_flag=True, help='Show detailed logging of the deduplication process')
def new_deduplicate(show_examples, verbose):
    """Run the new deduplication system using canonical events."""
    click.echo("Loading events from database...")
    events = database.get_all_events_sorted()

    if not events:
        click.echo("No events found in database. Run 'etl' first.")
        return

    click.echo(f"Loaded {len(events)} source events")

    if verbose:
        click.echo("\n--- Detailed deduplication process ---")
        # Show some statistics about the input events
        source_counts = {}
        for event in events:
            source_counts[event.source] = source_counts.get(
                event.source, 0) + 1

        click.echo("Source event counts:")
        for source, count in sorted(source_counts.items()):
            click.echo(f"  {source}: {count} events")

        # Show grouping process
        from .etl.new_deduplication import group_events_by_title_and_date
        groups = group_events_by_title_and_date(events)

        click.echo(f"\nGrouped into {len(groups)} title/date combinations:")
        click.echo(
            f"  Single-event groups: {sum(1 for g in groups.values() if len(g) == 1)}")
        click.echo(
            f"  Multi-event groups: {sum(1 for g in groups.values() if len(g) > 1)}")

        # Show some examples of grouping keys
        click.echo("\nExample grouping keys:")
        for i, ((normalized_title, event_date), group) in enumerate(list(groups.items())[:5]):
            click.echo(
                f"  '{normalized_title}' on {event_date}: {len(group)} events")

    click.echo("Running new deduplication system...")

    # Run new deduplication
    canonical_events, membership_map = deduplicate_events_new(events)

    # Show statistics
    total_source_events = len(events)
    total_canonical_events = len(canonical_events)
    total_groups_with_duplicates = sum(
        1 for canonical in canonical_events if len(canonical.source_events) > 1)

    click.echo(
        f"Created {total_canonical_events} canonical events from {total_source_events} source events")
    click.echo(
        f"Found {total_groups_with_duplicates} groups with multiple source events")

    if verbose:
        # Show size distribution of groups
        group_sizes = [len(canonical.source_events)
                       for canonical in canonical_events]
        from collections import Counter
        size_counts = Counter(group_sizes)

        click.echo("\nGroup size distribution:")
        for size in sorted(size_counts.keys()):
            count = size_counts[size]
            click.echo(f"  {size} events: {count} groups")

    if show_examples:
        click.echo("\n--- Examples of event merging ---")

        examples_shown = 0
        for canonical in canonical_events:
            if len(canonical.source_events) > 1 and examples_shown < 5:  # Show up to 5 examples
                click.echo(f"\nCanonical Event: {canonical.title}")
                click.echo(
                    f"  Date: {canonical.start.strftime('%Y-%m-%d %H:%M UTC')}")
                click.echo(f"  Venue: {canonical.venue or 'Unknown'}")
                click.echo(f"  URL: {canonical.url}")
                click.echo(
                    f"  Merged from {len(canonical.source_events)} sources:")

                # Find the source events from the original events list using the membership map
                source_events_for_canonical = []
                for event in events:
                    event_key = (event.source, event.source_id)
                    if event_key in membership_map and membership_map[event_key] == canonical.canonical_id:
                        source_events_for_canonical.append(event)

                for source_event in source_events_for_canonical:
                    time_info = " (date-only)" if source_event.is_date_only() else ""
                    click.echo(
                        f"    - {source_event.source}: '{source_event.title}' ({source_event.start.strftime('%Y-%m-%d %H:%M UTC')}{time_info})")
                    click.echo(f"      URL: {source_event.url}")

                examples_shown += 1

        if examples_shown == 0:
            click.echo("No duplicate groups found to show as examples.")

    # Save to database
    click.echo("Saving canonical events to database...")
    database.upsert_canonical_events(canonical_events)
    database.upsert_event_group_memberships(membership_map)
    click.echo("New deduplication results saved to database")


@cli.command()
@click.option('--future-only', is_flag=True, help='Show only future canonical events')
def list_canonical(future_only):
    """List canonical events from the new deduplication system."""
    if future_only:
        canonical_events = database.get_canonical_events_future()
        title = "Future canonical events"
    else:
        canonical_events = database.get_canonical_events()
        title = "All canonical events"

    click.echo(f"{title}: {len(canonical_events)} events\n")

    for event in canonical_events:
        # Format date/time
        if event.start.hour == 0 and event.start.minute == 0 and event.start == event.end:
            time_str = event.start.strftime('%a %-m/%-d/%Y (date only)')
        else:
            time_str = event.start.strftime('%a %-m/%-d/%Y from %-I:%M%p')
            if event.end != event.start:
                time_str += event.end.strftime(' - %-I:%M%p')

        # Show the event info
        click.echo(f"• {event.title}")
        click.echo(f"  {time_str}")
        if event.venue:
            click.echo(f"  {event.venue}")
        click.echo(f"  Sources: {', '.join(event.source_events)}")
        click.echo(f"  {event.url}")
        click.echo()


@cli.command()
@click.option('--all-future', is_flag=True, help='Show all future events')
@click.option('--all-past', is_flag=True, help='Show all past events')
@click.option('--show-duplicates', is_flag=True, help='Show duplicate events instead of canonical ones')
def list_events(all_future, all_past, show_duplicates):
    """List events. By default shows upcoming events in the next month."""
    if show_duplicates:
        events = database.get_duplicate_events()
        title = "Duplicate events"
        show_year = True
    elif all_future:
        events = database.get_all_future_events(
        ) if not show_duplicates else database.get_all_future_events()
        if not show_duplicates:
            events = [e for e in events if not e.same_as]
        title = "All future events" + \
            (" (including duplicates)" if show_duplicates else " (canonical only)")
        show_year = True
    elif all_past:
        events = database.get_all_past_events()
        if not show_duplicates:
            events = [e for e in events if not e.same_as]
        title = "All past events" + \
            (" (including duplicates)" if show_duplicates else " (canonical only)")
        show_year = True
    else:
        events = database.get_upcoming_events(days_ahead=30)
        if not show_duplicates:
            events = [e for e in events if not e.same_as]
        title = "Upcoming events (next 30 days)" + (
            " (including duplicates)" if show_duplicates else " (canonical only)")
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
        duplicate_str = f" [DUPLICATE of {event.same_as}]" if event.same_as else ""

        # Handle address display - don't show "None"
        if event.address and event.address.lower() != "none":
            address_str = event.address
        else:
            address_str = "Location TBD"

        click.echo(f"• {event.title}")
        click.echo(f"  {time_str}")
        click.echo(f"  {address_str}{venue_str}")
        click.echo(
            f"  Source: {event.source}{cost_str}{tags_str}{duplicate_str}")
        click.echo(f"  {event.url}")
        click.echo("")  # Empty line for readability


@dev.command()
@click.argument('date', required=True)
def show_source_events(date):
    """Show all source events for a specified date (YYYY-MM-DD) with raw data dump."""
    from datetime import datetime

    try:
        target_date = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        click.echo("Error: Date must be in YYYY-MM-DD format")
        return

    click.echo(f"Loading all source events for {target_date}...")
    all_events = database.get_all_events_sorted()

    # Filter events for the specified date
    matching_events = [
        event for event in all_events
        if event.start.date() == target_date
    ]

    if not matching_events:
        click.echo(f"No source events found for {target_date}")
        return

    click.echo(
        f"Found {len(matching_events)} source events for {target_date}:")
    click.echo("=" * 80)

    for i, event in enumerate(matching_events, 1):
        click.echo(f"\nEvent #{i}:")
        click.echo(f"  source: {event.source}")
        click.echo(f"  source_id: {event.source_id}")
        click.echo(f"  title: {repr(event.title)}")
        click.echo(f"  start: {event.start}")
        click.echo(f"  end: {event.end}")
        click.echo(f"  venue: {repr(event.venue)}")
        click.echo(f"  address: {repr(event.address)}")
        click.echo(f"  url: {event.url}")
        click.echo(f"  cost: {repr(event.cost)}")
        click.echo(f"  latitude: {event.latitude}")
        click.echo(f"  longitude: {event.longitude}")
        click.echo(f"  tags: {event.tags}")
        click.echo(f"  same_as: {event.same_as}")
        click.echo(f"  has_time_info(): {event.has_time_info()}")
        click.echo(f"  is_date_only(): {event.is_date_only()}")


@dev.command()
@click.argument('date', required=True)
def show_canonical_events(date):
    """Show all canonical events for a specified date (YYYY-MM-DD) with raw data dump."""
    from datetime import datetime

    try:
        target_date = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        click.echo("Error: Date must be in YYYY-MM-DD format")
        return

    click.echo(f"Loading all canonical events for {target_date}...")
    all_canonical = database.get_canonical_events()

    # Filter canonical events for the specified date
    matching_canonical = [
        event for event in all_canonical
        if event.start.date() == target_date
    ]

    if not matching_canonical:
        click.echo(f"No canonical events found for {target_date}")
        return

    click.echo(
        f"Found {len(matching_canonical)} canonical events for {target_date}:")
    click.echo("=" * 80)

    for i, canonical in enumerate(matching_canonical, 1):
        click.echo(f"\nCanonical Event #{i}:")
        click.echo(f"  canonical_id: {canonical.canonical_id}")
        click.echo(f"  title: {repr(canonical.title)}")
        click.echo(f"  start: {canonical.start}")
        click.echo(f"  end: {canonical.end}")
        click.echo(f"  venue: {repr(canonical.venue)}")
        click.echo(f"  address: {repr(canonical.address)}")
        click.echo(f"  url: {canonical.url}")
        click.echo(f"  cost: {repr(canonical.cost)}")
        click.echo(f"  latitude: {canonical.latitude}")
        click.echo(f"  longitude: {canonical.longitude}")
        click.echo(f"  tags: {canonical.tags}")
        click.echo(f"  source_events: {canonical.source_events}")

        # Show the actual source events that belong to this canonical event
        click.echo("  Source event details:")
        source_events = database.get_events_by_canonical_id(
            canonical.canonical_id)
        if source_events:
            for j, source_event in enumerate(source_events, 1):
                click.echo(
                    f"    #{j}: {source_event.source}:{source_event.source_id} - '{source_event.title}'")
                click.echo(f"        start: {source_event.start}")
                click.echo(f"        url: {source_event.url}")
        else:
            click.echo(
                "    (No source events found - may need to rerun new-deduplicate)")


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


# Add the dev group to the main CLI
cli.add_command(dev)


if __name__ == "__main__":
    cli()
