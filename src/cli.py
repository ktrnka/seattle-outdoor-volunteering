import traceback
from collections import Counter, namedtuple
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


FetchResult = namedtuple("FetchResult", ["success", "error"])


def _fetch_spf_detail_pages(db, max_events: int = 5) -> FetchResult:
    """Fetch detail pages for SPF events to get additional data like GSP URLs."""
    from .etl.spf import SPFDetailExtractor

    unenriched = db.get_unenriched_detail_page_events(source="SPF", limit=max_events)
    if not unenriched:
        click.echo("No unenriched SPF events found")
        return FetchResult(0, 0)

    click.echo(f"Found {len(unenriched)} unenriched SPF events. Processing up to {max_events}...")
    success_count = 0
    error_count = 0

    for event in unenriched:
        try:
            detail_extractor = SPFDetailExtractor.fetch(event.url)
            enrichment = detail_extractor.extract()

            db.store_detail_page_enrichment(
                source=event.source,
                source_id=event.source_id,
                detail_page_url=event.url,
                enrichment_data=enrichment.model_dump(exclude_none=True),
                status="success",
            )
            success_count += 1
            click.echo(f"  ✓ Enriched: {event.title}")
        except Exception as e:
            db.store_detail_page_enrichment(
                source=event.source,
                source_id=event.source_id,
                detail_page_url=event.url,
                enrichment_data={},
                status="failed",
                error_message=str(e),
            )
            error_count += 1
            click.echo(f"  ✗ Failed: {event.title} - {str(e)}")

    # Record ETL run for observability
    status = "success" if error_count == 0 else "failure"
    db.record_etl_run(source="SPF_DETAILS", status=status, num_rows=success_count)
    
    return FetchResult(success_count, error_count)


def _fetch_listings_impl(db, source: Optional[str] = None) -> int:
    """Fetch event listings from all sources or a specific source."""
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
    if source:
        if source not in extractor_map:
            click.echo(f"Error: Unknown source '{source}'. Available: {', '.join(extractor_map.keys())}")
            return 0
        extractors_to_run = [extractor_map[source]]
        click.echo(f"Running only {source} extractor...")
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
    
    return len(source_events)


def _fetch_categorizations_impl(db, max_events: int = 50) -> FetchResult:
    """Categorize uncategorized events using LLM."""
    from .llm.event_categorization import categorize_event as llm_categorize

    # Get uncategorized events
    uncategorized_events = db.get_uncategorized_source_events(limit=max_events)

    if not uncategorized_events:
        click.echo("No uncategorized events found.")
        return FetchResult(0, 0)

    click.echo(f"Found {len(uncategorized_events)} uncategorized events. Processing up to {max_events}...")

    success_count = 0
    error_count = 0

    for i, event in enumerate(uncategorized_events, 1):
        click.echo(f"\n[{i}/{len(uncategorized_events)}] Processing: {event.title}")
        click.echo(f"  Source: {event.source}:{event.source_id}")

        try:
            categorization = llm_categorize(event)

            # Store the result in the database
            db.store_event_enrichment(event.source, event.source_id, categorization)

            click.echo(f"  ✓ Categorized as: {categorization.category.value}")
            if categorization.reasoning:
                # Truncate reasoning for display
                reasoning_preview = categorization.reasoning[:60] + "..." if len(categorization.reasoning) > 60 else categorization.reasoning
                click.echo(f"  Reasoning: {reasoning_preview}")

            success_count += 1

        except Exception as e:
            click.echo(f"  ✗ Error: {str(e)}")
            error_count += 1
            # Continue processing other events

    # Record ETL run for observability
    status = "success" if error_count == 0 else "failure"
    db.record_etl_run(source="LLM_CATEGORIZATION", status=status, num_rows=success_count)
    
    return FetchResult(success_count, error_count)


@cli.command()
@click.option("--source", type=str, help="Run only the specified source (e.g., SPU, GSP, SPR, SPF, DNDA, EarthCorps)")
def fetch_listings(source: Optional[str] = None):
    """Fetch event listings from all sources - makes network requests."""
    with database.Database() as db:
        total_events = _fetch_listings_impl(db, source)
        click.echo(f"\nFetched {total_events} total events")


@cli.command()
@click.option("--max-events", type=int, default=5, help="Maximum events to process (default: 5)")
def fetch_details(max_events: int = 5):
    """Fetch SPF detail pages - makes network requests."""
    with database.Database() as db:
        result = _fetch_spf_detail_pages(db, max_events=max_events)
        click.echo(f"\nProcessing complete:")
        click.echo(f"  Successfully fetched: {result.success}")
        click.echo(f"  Errors: {result.error}")


@cli.command()
@click.option("--max-events", type=int, default=50, help="Maximum events to categorize (default: 50)")
def fetch_categorizations(max_events: int = 50):
    """Categorize uncategorized events using LLM - makes network requests and uses LLM budget."""
    with database.Database() as db:
        result = _fetch_categorizations_impl(db, max_events)
        click.echo("\nProcessing complete:")
        click.echo(f"  Successfully categorized: {result.success}")
        click.echo(f"  Errors: {result.error}")


@cli.command()
def pipeline():
    """Run complete ETL pipeline: fetch listings → fetch details → categorize → dedupe → build site."""
    click.echo("=" * 60)
    click.echo("Running ETL Pipeline")
    click.echo("=" * 60)
    
    with database.Database() as db:
        # Stage 1: Fetch event listings
        click.echo("\n[1/5] Fetching event listings...")
        total_events = _fetch_listings_impl(db, source=None)
        click.echo(f"✓ Fetched {total_events} events from all sources")
        
        # Stage 2: Fetch detail pages
        click.echo("\n[2/5] Fetching SPF detail pages...")
        detail_result = _fetch_spf_detail_pages(db, max_events=5)
        click.echo(f"✓ Fetched {detail_result.success} detail pages ({detail_result.error} errors)")
        
        # Stage 3: LLM categorization
        click.echo("\n[3/5] Categorizing events with LLM...")
        cat_result = _fetch_categorizations_impl(db, max_events=50)
        click.echo(f"✓ Categorized {cat_result.success} events ({cat_result.error} errors)")
    
    # Stage 4: Deduplication
    click.echo("\n[4/5] Running deduplication...")
    canonical_events = run_splink_deduplication(show_examples=False)
    with database.Database() as db:
        db.overwrite_canonical_events(canonical_events)
    click.echo(f"✓ Created {len(canonical_events)} canonical events")
    
    # Stage 5: Build site
    click.echo("\n[5/5] Building site...")
    generator.build(Path("docs"))
    click.echo("✓ Site built → docs/index.html")
    
    click.echo("\n" + "=" * 60)
    click.echo("Pipeline complete!")
    click.echo("=" * 60)


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

        # Show all event types found, sorted by count
        for event_type, count in type_counter.most_common():
            percentage = (count / len(events)) * 100 if events else 0
            click.echo(f"{event_type:<20} {count:>4} ({percentage:>5.1f}%)")

        # Show some examples for each type
        click.echo("\nExample events by type:")
        click.echo("=" * 40)

        examples_per_type = 3
        type_examples = {}

        for event in events:
            event_type = event.get_event_type()
            if event_type not in type_examples:
                type_examples[event_type] = []
            if len(type_examples[event_type]) < examples_per_type:
                type_examples[event_type].append(event)

        for event_type in sorted(type_examples.keys()):
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
@click.option("--source", type=str, default=None, help="Filter by source (e.g., GSP, SPR, SPF)")
@click.option("--canonical", is_flag=True, help="Show canonical events instead of source events")
@click.option("--limit", default=20, help="Maximum number of events to show (default: 20)")
@click.option("--future", is_flag=True, help="Only show upcoming events")
def show_events(source: Optional[str] = None, canonical: bool = False, limit: int = 20, future: bool = False):
    """Show events from a specific source or all sources."""
    from datetime import datetime, timezone

    with database.Database() as db:
        if canonical:
            events = db.get_canonical_events()
            # Filter canonical events by source if specified
            if source:
                filtered_events = [e for e in events if any(source in unique_id for unique_id in e.source_events)]
            else:
                filtered_events = events
            event_type = "canonical events"
        else:
            events = db.get_source_events()
            # Filter source events by source if specified
            if source:
                filtered_events = [e for e in events if e.source == source]
            else:
                filtered_events = events
            event_type = "source events"

        # Filter for future events if requested
        if future:
            now = datetime.now(timezone.utc)
            filtered_events = [e for e in filtered_events if e.start >= now]
            event_type = f"upcoming {event_type}"

        if not filtered_events:
            source_msg = f" for source '{source}'" if source else ""
            click.echo(f"No {event_type} found{source_msg}")
            return

        # Sort by start date
        filtered_events.sort(key=lambda e: e.start)

        # Limit results
        display_events = filtered_events[:limit]

        source_msg = f" for source '{source}'" if source else ""
        click.echo(f"Showing {len(display_events)} of {len(filtered_events)} {event_type}{source_msg}:")
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


def _display_event_info(event) -> None:
    """Helper to display basic event information."""
    click.echo(f"Source: {event.source}")
    click.echo(f"Source ID: {event.source_id}")
    click.echo(f"Title: {event.title}")
    if event.venue:
        click.echo(f"Venue: {event.venue}")
    click.echo(f"URL: {event.url}")
    if event.tags:
        click.echo(f"Existing tags: {', '.join(event.tags)}")


def _display_llm_categorization(categorization, status_note: str = "") -> None:
    """Helper to display LLM categorization results."""
    click.echo("=" * 50)
    click.echo(f"Category: {categorization.category.value}")
    if categorization.reasoning:
        click.echo(f"Reasoning: {categorization.reasoning}")
    if status_note:
        click.echo(f"({status_note})")


@dev.command()
@click.argument("max_events", type=int, required=True)
def enrich_source_events(max_events: int):
    """Categorize uncategorized source events with LLM (deprecated - use 'fetch-categorizations' instead)."""
    with database.Database() as db:
        result = _fetch_categorizations_impl(db, max_events)
        click.echo("\nProcessing complete:")
        click.echo(f"  Successfully categorized: {result.success}")
        click.echo(f"  Errors: {result.error}")


@dev.command()
@click.option("--max", "max_events", type=int, default=1, help="Maximum events to process (default: 1)")
def enrich_detail_pages(max_events: int):
    """Fetch and parse detail pages (deprecated - use 'fetch-details' instead)."""
    with database.Database() as db:
        result = _fetch_spf_detail_pages(db, max_events=max_events)

        click.echo("\nProcessing complete:")
        click.echo(f"  Successfully enriched: {result.success}")
        click.echo(f"  Errors: {result.error}")


# Add the dev group to the main CLI
cli.add_command(dev)


if __name__ == "__main__":
    cli()
