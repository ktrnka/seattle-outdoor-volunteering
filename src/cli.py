import click
import gzip
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.etl.dnda import DNDAExtractor

from .config import DB_PATH, DB_GZ
from .etl.gsp import GSPExtractor
from .etl.spf import SPFExtractor
from .etl.spr import SPRExtractor
from .etl.spu import SPUExtractor
from .etl.earthcorps import EarthCorpsExtractor
from .etl.deduplication import deduplicate_events
from .site import generator
from .models import SEATTLE_TZ
from . import database


@click.group()
def cli(): ...


@cli.group()
def dev():
    """Development and debugging commands."""
    pass


@cli.command()
@click.option("--reset", is_flag=True, help="Reset the database before initializing")
def init_db(reset: bool = False):
    """Initialize the database by creating tables."""
    database.init_database(reset=reset)
    click.echo(f"Database initialized at {DB_PATH}")


@cli.command()
@click.option("--only-run", type=str, help="Run only the specified extractor (e.g., SPU, GSP, SPR, SPF, DNDA, EarthCorps)")
def etl(only_run: Optional[str] = None):
    """Run all extractors, deduplication, and build/compact DB."""
    # Map extractor names to classes
    extractor_map = {
        "GSP": GSPExtractor,
        "SPR": SPRExtractor,
        "SPF": SPFExtractor,
        "SPU": SPUExtractor,
        "DNDA": DNDAExtractor,
        "EarthCorps": EarthCorpsExtractor,
    }

    # Determine which extractors to run
    if only_run:
        if only_run not in extractor_map:
            click.echo(
                f"Error: Unknown extractor '{only_run}'. Available: {', '.join(extractor_map.keys())}")
            return
        extractors_to_run = [extractor_map[only_run]]
        click.echo(f"Running only {only_run} extractor...")
    else:
        extractors_to_run = list(extractor_map.values())
        click.echo("Running all extractors...")

    # Fetch source events from specified extractors
    source_events = []

    for extractor_class in extractors_to_run:
        try:
            extractor = extractor_class.fetch()
            events = extractor.extract()
            source_events.extend(events)
            click.echo(f"{extractor_class.__name__}: {len(events)} events")

            # Record successful ETL run
            database.record_etl_run(
                source=extractor.source,
                status="success",
                num_rows=len(events)
            )
        except Exception as e:
            click.echo(f"{extractor_class.__name__}: ERROR - {str(e)}")
            # Record failed ETL run
            database.record_etl_run(
                source=extractor_class.source,  # Use class attribute since instance may not exist
                status="failure",
                num_rows=0
            )
            # Continue in case the next extractor can still run
            continue

    # Save newly extracted events to database
    click.echo("Saving new events to database...")
    database.upsert_source_events(source_events)

    # Now run deduplication on ALL events in the database
    click.echo("Loading all source events from database...")
    all_source_events = database.get_source_events()

    if not all_source_events:
        click.echo(
            "No source events found in database after upsert. Something went wrong.")
        return

    click.echo(
        f"Running deduplication on {len(all_source_events)} total events...")
    canonical_events, membership_map = deduplicate_events(all_source_events)

    # Show summary
    total_groups_with_duplicates = sum(
        1 for canonical in canonical_events if len(canonical.source_events) > 1)
    click.echo(
        f"Created {len(canonical_events)} canonical events from {len(all_source_events)} source events")
    click.echo(
        f"Found {total_groups_with_duplicates} groups with multiple sources")

    # Save canonical events and memberships to database
    click.echo("Updating canonical events in database...")
    database.overwrite_canonical_events(canonical_events)
    database.overwrite_event_group_memberships(membership_map)

    # Compress database for git
    with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
        shutil.copyfileobj(src, dst)

    click.echo("ETL complete!")


@cli.command()
@click.option('--show-examples', is_flag=True, help='Show examples of how events are being merged')
@click.option('--verbose', is_flag=True, help='Show detailed logging of the deduplication process')
def deduplicate(show_examples, verbose):
    """Run deduplication on existing source events in the database."""
    click.echo("Loading source events from database...")
    source_events = database.get_source_events()

    if not source_events:
        click.echo("No source events found in database. Run 'etl' first.")
        return

    click.echo(f"Loaded {len(source_events)} source events")

    if verbose:
        click.echo("\n--- Detailed deduplication process ---")
        # Show source event counts
        source_counts = Counter(event.source for event in source_events)
        click.echo("Source event counts:")
        for source, count in sorted(source_counts.items()):
            click.echo(f"  {source}: {count} events")

    # Run deduplication
    click.echo("Running deduplication...")
    canonical_events, membership_map = deduplicate_events(source_events)

    # Show summary
    total_groups_with_duplicates = sum(
        1 for canonical in canonical_events if len(canonical.source_events) > 1)
    click.echo(
        f"Created {len(canonical_events)} canonical events from {len(source_events)} source events")
    click.echo(
        f"Found {total_groups_with_duplicates} groups with multiple sources")

    if verbose:
        # Show group size distribution
        group_sizes = [len(canonical.source_events)
                       for canonical in canonical_events]
        size_counts = Counter(group_sizes)
        click.echo("\nGroup size distribution:")
        for size in sorted(size_counts.keys()):
            count = size_counts[size]
            click.echo(f"  {size} events: {count} groups")

    if show_examples:
        click.echo("\n--- Examples of event merging ---")
        examples_shown = 0
        for canonical in canonical_events:
            if len(canonical.source_events) > 1 and examples_shown < 5:
                click.echo(f"\nCanonical Event: {canonical.title}")
                click.echo(
                    f"  Date: {canonical.start.strftime('%Y-%m-%d %H:%M UTC')}")
                click.echo(f"  Venue: {canonical.venue or 'Unknown'}")
                click.echo(f"  URL: {canonical.url}")
                click.echo(
                    f"  Merged from {len(canonical.source_events)} sources:")

                # Find the source events for this canonical event
                source_events_for_canonical = []
                for event in source_events:
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

    # Save canonical events to database
    click.echo("Saving canonical events to database...")
    database.overwrite_canonical_events(canonical_events)
    database.overwrite_event_group_memberships(membership_map)
    click.echo("Deduplication complete!")


@cli.command()
@click.option('--all', is_flag=True, help='Show all canonical events')
def list_canonical_events(all: bool = False):
    """List canonical events from the new deduplication system."""
    if not all:
        canonical_events = database.get_future_canonical_events()
    else:
        canonical_events = database.get_canonical_events()
    click.echo(f"{len(canonical_events)} events\n")

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


@dev.command()
@click.option('--days', default=7, help='Number of days to look back (default: 7)')
def etl_stats(days: int):
    """Show ETL run success and error rates by source."""
    stats = database.get_etl_run_stats(days)

    if not stats:
        click.echo(f"No ETL runs found in the last {days} days.")
        return

    click.echo(f"ETL Run Statistics (Last {days} days)")
    click.echo("=" * 50)

    # Sort sources alphabetically
    for source in sorted(stats.keys()):
        source_stats = stats[source]
        total = source_stats['total']
        success = source_stats['success']
        failure = source_stats['failure']

        if total > 0:
            success_rate = (success / total) * 100
            click.echo(f"\n{source}:")
            click.echo(f"  Total runs: {total}")
            click.echo(f"  Successful: {success} ({success_rate:.1f}%)")
            click.echo(f"  Failed: {failure} ({100-success_rate:.1f}%)")
        else:
            click.echo(f"\n{source}: No runs found")

    # Show overall stats
    total_runs = sum(s['total'] for s in stats.values())
    total_success = sum(s['success'] for s in stats.values())

    if total_runs > 0:
        overall_success_rate = (total_success / total_runs) * 100
        click.echo("\nOverall:")
        click.echo(f"  Total runs: {total_runs}")
        click.echo(f"  Success rate: {overall_success_rate:.1f}%")


@dev.command()
@click.argument('date', required=True)
def debug_date(date):
    target_date = datetime.strptime(date, '%Y-%m-%d').date()

    # Convert UTC times to Pacific time for date comparison
    source_events = [e for e in database.get_source_events()
                     if e.start.astimezone(SEATTLE_TZ).date() == target_date]

    canonical_events = [e for e in database.get_canonical_events()
                        if e.start.astimezone(SEATTLE_TZ).date() == target_date]

    click.echo(f"# Source events for {target_date}: {len(source_events)}")
    for event in source_events:
        utc_start = event.start
        local_start = utc_start.astimezone(SEATTLE_TZ)

        click.echo(
            f"\n## {event.title} (UTC {utc_start}, Local {local_start}, TZ={event.start.tzinfo})")
        click.echo(f"{event.source} - {event.url})")

    click.echo(
        f"\n# Canonical events for {target_date}: {len(canonical_events)}")
    for event in canonical_events:
        utc_start = event.start
        local_start = utc_start.astimezone(SEATTLE_TZ)

        click.echo(
            f"\n## {event.title} (UTC {utc_start}, Local {local_start}, TZ={event.start.tzinfo})")
        click.echo(f"Sources: {', '.join(event.source_events)}")
        click.echo(f"URL: {event.url}")


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


# Add the dev group to the main CLI
cli.add_command(dev)


if __name__ == "__main__":
    cli()
