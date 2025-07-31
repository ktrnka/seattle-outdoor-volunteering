import gzip
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

import click


from . import database
from .config import DB_GZ, DB_PATH
from .etl.deduplication import deduplicate_events
from .etl.dnda import DNDAExtractor
from .etl.earthcorps import EarthCorpsExtractor
from .etl.gsp import GSPCalendarExtractor
from .etl.manual import ManualExtractor
from .etl.spf import SPFExtractor
from .etl.spr import SPRExtractor
from .etl.spu import SPUExtractor
from .models import SEATTLE_TZ
from .site import generator


@click.group()
def cli(): ...


@cli.command()
@click.option('--days', default=7, help='Number of days to include in stats')
def stats(days):
    """Show ETL run statistics for each source over the last N days."""
    stats = database.get_etl_run_stats(days)
    if not stats:
        click.echo(f"No ETL runs found in the last {days} days.")
        return

    click.echo(f"ETL Run Statistics (Last {days} days)")
    click.echo("=" * 50)

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

    total_runs = sum(s['total'] for s in stats.values())
    total_success = sum(s['success'] for s in stats.values())

    if total_runs > 0:
        overall_success_rate = (total_success / total_runs) * 100
        click.echo("\nOverall:")
        click.echo(f"  Total runs: {total_runs}")
        click.echo(f"  Success rate: {overall_success_rate:.1f}%")


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
        "GSP": GSPCalendarExtractor,
        "SPR": SPRExtractor,
        "SPF": SPFExtractor,
        "SPU": SPUExtractor,
        "DNDA": DNDAExtractor,
        "EarthCorps": EarthCorpsExtractor,
        "Manual": ManualExtractor,
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
    canonical_events = deduplicate_events(all_source_events)

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

    # Compress database for git
    with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
        shutil.copyfileobj(src, dst)

    click.echo("ETL complete!")


@cli.command()
@click.option('--verbose', is_flag=True, help='Show detailed logging of the deduplication process')
@click.option('--dry-run', is_flag=True, help='Run deduplication without saving changes')
def deduplicate(verbose: bool = False, dry_run: bool = False):
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
    canonical_events = deduplicate_events(source_events)

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

    # Save canonical events to database
    if not dry_run:
        click.echo("Saving canonical events to database...")
        database.overwrite_canonical_events(canonical_events)
        click.echo("Deduplication complete!")
    else:
        click.echo("Dry run enabled. No changes were made.")


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
@click.option('--source-events', is_flag=True, help='Analyze tags from source events instead of canonical events')
@click.option('--min-count', default=1, help='Only show tags with at least this many occurrences (default: 1)')
def tag_stats(source_events: bool = False, min_count: int = 1):
    """Analyze tag frequency in the database."""
    if source_events:
        events = database.get_source_events()
        click.echo(f"Analyzing tags from {len(events)} source events")
    else:
        events = database.get_canonical_events()
        click.echo(f"Analyzing tags from {len(events)} canonical events")

    # Count individual tags
    tag_counter = Counter()
    events_with_tags = 0

    for event in events:
        if event.tags:
            events_with_tags += 1
            for tag in event.tags:
                tag_counter[tag] += 1

    # Display results
    click.echo(
        f"Events with tags: {events_with_tags}/{len(events)} ({events_with_tags/len(events)*100:.1f}%)")
    click.echo(f"Total unique tags: {len(tag_counter)}")

    if tag_counter:
        click.echo(
            f"\nTag frequency (showing tags with {min_count}+ occurrences):")
        click.echo("=" * 50)

        # Sort by frequency (descending) then alphabetically
        filtered_tags = [(tag, count)
                         for tag, count in tag_counter.items() if count >= min_count]
        sorted_tags = sorted(filtered_tags, key=lambda x: (-x[1], x[0]))

        for tag, count in sorted_tags:
            percentage = (count / len(events)) * 100
            click.echo(f"{tag:<30} {count:>4} ({percentage:>5.1f}%)")
    else:
        click.echo("No tags found in any events.")


@dev.command()
def event_type_stats():
    """Analyze event type distribution based on current classification logic."""
    events = database.get_canonical_events()
    click.echo(f"Analyzing event types from {len(events)} canonical events")

    # Count event types
    type_counter = Counter()
    for event in events:
        event_type = event.get_event_type()
        type_counter[event_type] += 1

    # Display results
    click.echo("\nEvent type distribution:")
    click.echo("=" * 40)

    for event_type in ['parks', 'cleanup', 'other']:
        count = type_counter[event_type]
        percentage = (count / len(events)) * 100 if events else 0
        click.echo(f"{event_type:<10} {count:>4} ({percentage:>5.1f}%)")

    # Show some examples for each type
    click.echo("\nExample events by type:")
    click.echo("=" * 40)

    examples_per_type = 3
    type_examples = {'parks': [], 'cleanup': [], 'other': []}

    for event in events:
        event_type = event.get_event_type()
        if len(type_examples[event_type]) < examples_per_type:
            type_examples[event_type].append(event)

    for event_type in ['parks', 'cleanup', 'other']:
        click.echo(f"\n{event_type.upper()}:")
        if type_examples[event_type]:
            for event in type_examples[event_type]:
                click.echo(f"  • {event.title}")
                click.echo(f"    URL: {event.url}")
                if event.tags:
                    click.echo(
                        f"    Tags: {', '.join(event.tags[:3])}{'...' if len(event.tags) > 3 else ''}")
        else:
            click.echo("  No examples found")


@dev.command()
def migrate():
    """Run database migrations for development."""
    click.echo("Running database migrations...")

    # Add source_dict column to events table
    database.migrate_add_source_dict_column()

    click.echo("Migrations completed successfully!")


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


@dev.command()
def test_splink():
    """Test out Splink for event deduplication."""
    from .etl.splink_dedupe import run_splink_deduplication

    run_splink_deduplication()


@dev.command()
@click.argument("event_title")
def test_llm_canonicalization(event_title: str):
    """Test LLM-based canonicalization."""
    from .llm_canonicalization import run_llm_canonicalization

    run_llm_canonicalization(
        *database.find_canonical_event_with_sources(event_title)
    )


# Add the dev group to the main CLI
cli.add_command(dev)


if __name__ == "__main__":
    cli()
