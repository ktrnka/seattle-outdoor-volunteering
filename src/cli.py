import traceback
from collections import Counter
from pathlib import Path
from typing import Optional

import click

from . import database
from .etl.deduplication import deduplicate_events
from .etl.dnda import DNDAExtractor
from .etl.earthcorps import EarthCorpsCalendarExtractor
from .etl.fremont_neighbor import FremontNeighborExtractor
from .etl.gsp import GSPCalendarExtractor
from .etl.manual import ManualExtractor
from .etl.spf import SPFExtractor
from .etl.splink_dedupe import run_splink_deduplication
from .etl.spr import SPRExtractor
from .etl.spu import SPUExtractor
from .site import generator


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
    with database.Database() as db:
        db.init_database(reset=reset)


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
        "EarthCorps": EarthCorpsCalendarExtractor,
        "FremontNeighbor": FremontNeighborExtractor,
        "Manual": ManualExtractor,
    }

    # Determine which extractors to run
    if only_run:
        if only_run not in extractor_map:
            click.echo(f"Error: Unknown extractor '{only_run}'. Available: {', '.join(extractor_map.keys())}")
            return
        extractors_to_run = [extractor_map[only_run]]
        click.echo(f"Running only {only_run} extractor...")
    else:
        extractors_to_run = list(extractor_map.values())
        click.echo("Running all extractors...")

    # Fetch source events from specified extractors
    source_events = []

    with database.Database() as db:
        for extractor_class in extractors_to_run:
            try:
                extractor = extractor_class.fetch()
                events = extractor.extract()
                source_events.extend(events)
                click.echo(f"{extractor_class.__name__}: {len(events)} events")

                # Record successful ETL run
                db.record_etl_run(source=extractor.source, status="success", num_rows=len(events))
            except Exception as e:
                click.echo(f"{extractor_class.__name__}: ERROR - {str(e)}")
                traceback.print_exception(e)

                # Record failed ETL run
                db.record_etl_run(
                    source=extractor_class.source,  # Use class attribute since instance may not exist
                    status="failure",
                    num_rows=0,
                )
                # Continue in case the next extractor can still run
                continue

        # Save newly extracted events to database
        click.echo("Saving new events to database...")
        db.upsert_source_events(source_events)

        click.echo("ETL complete!")


@cli.command()
@click.option("--verbose", is_flag=True, help="Show detailed logging of the deduplication process")
@click.option("--dry-run", is_flag=True, help="Run deduplication without saving changes")
@click.option("--method", type=click.Choice(["splink", "old"]), default="splink")
def deduplicate(verbose: bool = False, dry_run: bool = False, method: str = "splink"):
    """Run deduplication on existing source events in the database."""
    if method == "splink":
        click.echo("Running Splink deduplication...")
        canonical_events = run_splink_deduplication(show_examples=verbose)
        if not dry_run:
            with database.Database() as db:
                db.overwrite_canonical_events(canonical_events)
    elif method == "old":
        click.echo("Running old deduplication method...")
        deduplicate_old(verbose=verbose, dry_run=dry_run)
    else:
        click.echo(f"Unknown deduplication method: {method}")


def deduplicate_old(verbose: bool = False, dry_run: bool = False):
    """Run deduplication on existing source events in the database."""
    with database.Database() as db:
        click.echo("Loading source events from database...")
        source_events = db.get_source_events()

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
        total_groups_with_duplicates = sum(1 for canonical in canonical_events if len(canonical.source_events) > 1)
        click.echo(f"Created {len(canonical_events)} canonical events from {len(source_events)} source events")
        click.echo(f"Found {total_groups_with_duplicates} groups with multiple sources")

        if verbose:
            # Show group size distribution
            group_sizes = [len(canonical.source_events) for canonical in canonical_events]
            size_counts = Counter(group_sizes)
            click.echo("\nGroup size distribution:")
            for size in sorted(size_counts.keys()):
                count = size_counts[size]
                click.echo(f"  {size} events: {count} groups")

        # Save canonical events to database
        if not dry_run:
            click.echo("Saving canonical events to database...")
            db.overwrite_canonical_events(canonical_events)
            click.echo("Deduplication complete!")
        else:
            click.echo("Dry run enabled. No changes were made.")


@dev.command()
@click.option("--days", default=7, help="Number of days to look back (default: 7)")
def etl_stats(days: int):
    """Show ETL run success and error rates by source."""
    with database.Database() as db:
        stats = db.get_etl_run_stats(days)

    if not stats:
        click.echo(f"No ETL runs found in the last {days} days.")
        return

    click.echo(f"ETL Run Statistics (Last {days} days)")
    click.echo("=" * 50)

    # Sort sources alphabetically
    for source in sorted(stats.keys()):
        source_stats = stats[source]
        total = source_stats["total"]
        success = source_stats["success"]
        failure = source_stats["failure"]

        if total > 0:
            success_rate = (success / total) * 100
            click.echo(f"\n{source}:")
            click.echo(f"  Total runs: {total}")
            click.echo(f"  Successful: {success} ({success_rate:.1f}%)")
            click.echo(f"  Failed: {failure} ({100 - success_rate:.1f}%)")
        else:
            click.echo(f"\n{source}: No runs found")

    # Show overall stats
    total_runs = sum(s["total"] for s in stats.values())
    total_success = sum(s["success"] for s in stats.values())

    if total_runs > 0:
        overall_success_rate = (total_success / total_runs) * 100
        click.echo("\nOverall:")
        click.echo(f"  Total runs: {total_runs}")
        click.echo(f"  Success rate: {overall_success_rate:.1f}%")


@dev.command()
@click.option("--source-events", is_flag=True, help="Analyze tags from source events instead of canonical events")
@click.option("--min-count", default=1, help="Only show tags with at least this many occurrences (default: 1)")
def tag_stats(source_events: bool = False, min_count: int = 1):
    """Analyze tag frequency in the database."""
    with database.Database() as db:
        if source_events:
            events = db.get_source_events()
            click.echo(f"Analyzing tags from {len(events)} source events")
        else:
            events = db.get_canonical_events()
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
        click.echo(f"Events with tags: {events_with_tags}/{len(events)} ({events_with_tags / len(events) * 100:.1f}%)")
        click.echo(f"Total unique tags: {len(tag_counter)}")

        if tag_counter:
            click.echo(f"\nTag frequency (showing tags with {min_count}+ occurrences):")
            click.echo("=" * 50)

            # Sort by frequency (descending) then alphabetically
            filtered_tags = [(tag, count) for tag, count in tag_counter.items() if count >= min_count]
            sorted_tags = sorted(filtered_tags, key=lambda x: (-x[1], x[0]))

            for tag, count in sorted_tags:
                percentage = (count / len(events)) * 100
                click.echo(f"{tag:<30} {count:>4} ({percentage:>5.1f}%)")
        else:
            click.echo("No tags found in any events.")


@dev.command()
def event_type_stats():
    """Analyze event type distribution based on current classification logic."""

    with database.Database() as db:
        events = db.get_canonical_events()
        click.echo(f"Analyzing event types from {len(events)} canonical events")

        # Count event types
        type_counter = Counter()
        for event in events:
            event_type = event.get_event_type()
            type_counter[event_type] += 1

        # Display results
        click.echo("\nEvent type distribution:")
        click.echo("=" * 40)

        for event_type in ["parks", "cleanup", "other"]:
            count = type_counter[event_type]
            percentage = (count / len(events)) * 100 if events else 0
            click.echo(f"{event_type:<10} {count:>4} ({percentage:>5.1f}%)")

        # Show some examples for each type
        click.echo("\nExample events by type:")
        click.echo("=" * 40)

        examples_per_type = 3
        type_examples = {"parks": [], "cleanup": [], "other": []}

        for event in events:
            event_type = event.get_event_type()
            if len(type_examples[event_type]) < examples_per_type:
                type_examples[event_type].append(event)

        for event_type in ["parks", "cleanup", "other"]:
            click.echo(f"\n{event_type.upper()}:")
            if type_examples[event_type]:
                for event in type_examples[event_type]:
                    click.echo(f"  • {event.title}")
                    click.echo(f"    URL: {event.url}")
                    if event.tags:
                        click.echo(f"    Tags: {', '.join(event.tags[:3])}{'...' if len(event.tags) > 3 else ''}")
            else:
                click.echo("  No examples found")


@dev.command()
@click.argument("source", required=True)
@click.option("--canonical", is_flag=True, help="Show canonical events instead of source events")
@click.option("--limit", default=20, help="Maximum number of events to show (default: 20)")
def show_events(source: str, canonical: bool = False, limit: int = 20):
    """Show events from a specific source (e.g., GSP, SPR, FRE)."""
    with database.Database() as db:
        if canonical:
            events = db.get_canonical_events()
            # Filter canonical events that have the specified source
            filtered_events = [e for e in events if any(source in unique_id for unique_id in e.source_events)]
            event_type = "canonical events"
        else:
            events = db.get_source_events()
            # Filter source events by source
            filtered_events = [e for e in events if e.source == source]
            event_type = "source events"

        if not filtered_events:
            click.echo(f"No {event_type} found for source '{source}'")
            return

        # Sort by start date
        filtered_events.sort(key=lambda e: e.start)

        # Limit results
        display_events = filtered_events[:limit]

        click.echo(f"Showing {len(display_events)} of {len(filtered_events)} {event_type} for source '{source}':")
        click.echo("=" * 60)

        for event in display_events:
            # Format date/time
            if event.start.hour == 0 and event.start.minute == 0 and event.start == event.end:
                time_str = event.start.strftime("%a %-m/%-d/%Y (date only)")
            else:
                time_str = event.start.strftime("%a %-m/%-d/%Y from %-I:%M%p")
                if event.end != event.start:
                    time_str += event.end.strftime(" - %-I:%M%p")

            click.echo(f"\n• {event.title}")
            click.echo(f"  {time_str}")
            if event.venue:
                click.echo(f"  {event.venue}")
            if canonical:
                click.echo(f"  Sources: {', '.join(event.source_events)}")
            else:
                click.echo(f"  Source ID: {event.source_id}")
            if event.tags:
                click.echo(f"  Tags: {', '.join(event.tags)}")
            click.echo(f"  {event.url}")


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


@dev.command()
@click.argument("event_title")
def test_llm_canonicalization(event_title: str):
    """Test LLM-based canonicalization."""
    from .llm.llm_canonicalization import run_llm_canonicalization

    with database.Database() as db:
        data = db.find_canonical_event_with_sources(event_title)
        assert data, f"No canonical event found with title '{event_title}'"
        run_llm_canonicalization(*data)


@dev.command()
@click.argument("source", required=True)
@click.argument("source_id", required=True)
def categorize_event(source: str, source_id: str):
    """Categorize a source event with LLM and store the result."""
    from .llm.event_categorization import categorize_event as llm_categorize
    
    with database.Database() as db:
        # Find the source event (will include enrichment if it exists)
        target_event = db.get_source_event(source, source_id)

        if not target_event:
            click.echo(f"Error: No source event found with source='{source}' and source_id='{source_id}'")
            return
            
        # Display event info
        click.echo("Event to categorize:")
        click.echo("=" * 50)
        click.echo(f"Source: {target_event.source}")
        click.echo(f"Source ID: {target_event.source_id}")
        click.echo(f"Title: {target_event.title}")
        if target_event.venue:
            click.echo(f"Venue: {target_event.venue}")
        click.echo(f"URL: {target_event.url}")
        if target_event.tags:
            click.echo(f"Existing tags: {', '.join(target_event.tags)}")
        
        # Check if already categorized
        if target_event.llm_categorization:
            click.echo("\nExisting LLM Categorization:")
            click.echo("=" * 50)
            click.echo(f"Category: {target_event.llm_categorization.category.value}")
            if target_event.llm_categorization.reasoning:
                click.echo(f"Reasoning: {target_event.llm_categorization.reasoning}")
            click.echo("(Already categorized - showing existing result)")
        else:
            click.echo("\nCategorizing with LLM...")
            
            try:
                categorization = llm_categorize(target_event)
                
                # Store the result in the database
                db.store_event_enrichment(target_event.source, target_event.source_id, categorization)
                
                click.echo("\nLLM Categorization Result:")
                click.echo("=" * 50)
                click.echo(f"Category: {categorization.category.value}")
                if categorization.reasoning:
                    click.echo(f"Reasoning: {categorization.reasoning}")
                click.echo("(Stored in database)")
                    
            except Exception as e:
                click.echo(f"\nError during categorization: {e}")
                import traceback
                traceback.print_exc()


@dev.command()
def create_enrichment_table():
    """Create the enriched_source_events table for LLM categorization."""
    from .database import EnrichedSourceEvent
    
    with database.Database() as db:
        # Create just the enrichment table
        EnrichedSourceEvent.__table__.create(db.engine, checkfirst=True)
        click.echo("Created enriched_source_events table successfully!")


@dev.command()
@click.option("--limit", default=10, help="Maximum number of enriched events to show (default: 10)")
def show_enriched_events(limit: int = 10):
    """Show source events with their LLM enrichment data."""
    with database.Database() as db:
        enriched_events = db.get_enriched_source_events(limit=limit)
        
        if not enriched_events:
            click.echo("No enriched events found in database.")
            return
            
        click.echo(f"Showing {len(enriched_events)} enriched events:")
        click.echo("=" * 60)
        
        for event in enriched_events:
            # Format date/time
            if event.start.hour == 0 and event.start.minute == 0 and event.start == event.end:
                time_str = event.start.strftime("%a %-m/%-d/%Y (date only)")
            else:
                time_str = event.start.strftime("%a %-m/%-d/%Y from %-I:%M%p")
                if event.end != event.start:
                    time_str += event.end.strftime(" - %-I:%M%p")

            click.echo(f"\n• {event.title}")
            click.echo(f"  {time_str}")
            if event.venue:
                click.echo(f"  {event.venue}")
            click.echo(f"  Source: {event.source}:{event.source_id}")
            
            # Show LLM categorization
            if event.llm_categorization:
                click.echo(f"  LLM Category: {event.llm_categorization.category.value}")
                if event.llm_categorization.reasoning:
                    click.echo(f"  LLM Reasoning: {event.llm_categorization.reasoning}")
            else:
                click.echo("  LLM Category: Not available")
                
            click.echo(f"  {event.url}")


# Add the dev group to the main CLI
cli.add_command(dev)


if __name__ == "__main__":
    cli()
