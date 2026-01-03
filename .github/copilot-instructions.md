# Seattle Outdoor Volunteering - Copilot Instructions

## Project Overview
An ETL pipeline that scrapes Seattle-area outdoor volunteer events into a SQLite database and generates a static website with PicoCSS. Events are fetched nightly via GitHub Actions and served through GitHub Pages.

## Interaction Guidelines

**Goal**: Enable smooth, reliable development by catching misunderstandings early and making changes incrementally.

**Clarify before building:**
- When requirements are ambiguous, ask targeted questions to understand intent before implementing
- Ask questions **one at a time** - this catches misconceptions early rather than after reading 10+ questions
- If infeasible or conflicting, stop and ask rather than making assumptions

**Work incrementally:**
- Make changes in small, logical steps that can be independently accepted/reverted
- Test after each significant change to maintain confidence
- For bug fixes: Start with TDD (failing test first)
- For new features: Ask whether to use TDD based on complexity

**Learn and improve:**
- After major features/refactors: Reflect on what worked, propose small process improvements
- Update these instructions collaboratively to reduce repeated guidance across sessions

## Architecture & Data Flow
```
Sources (GSP/SPR/SPF) → BaseExtractor → Event models → LLM Enrichment → Detail Page Enrichment → Deduplication → SQLite → Site Generator → HTML
```

- **ETL Sources**: Each has a dedicated extractor inheriting from `BaseExtractor` in `src/etl/`
- **LLM Enrichment**: Event categorization stored in `enriched_source_events` table, integrated via `llm_categorization` field
- **Detail Page Enrichment**: Additional data extracted from detail pages stored in `detail_page_enrichments` table (e.g., SPF → GSP URLs for deduplication)
- **Deduplication**: Splink-based probabilistic matching using title similarity, date/time, address, and URL array intersection
- **Database**: SQLAlchemy with both Pydantic models (`src/models.py`) and SQLAlchemy models (`src/database.py`)
- **Site Generation**: Jinja2 templates in `src/site/templates/` with timezone-aware datetime handling

## Key Patterns & Conventions

### Timezone Handling
All datetimes are stored in UTC. We convert from local time to UTC immediately during extraction, then do all our processing in UTC, then convert to local time only when generating the site. This ensures consistency and avoids timezone-related bugs. If the local time is not specified, we assume the event is in Seattle's timezone (PST/PDT).

### Extractor Pattern
New data sources should:
1. Inherit from `BaseExtractor` with `fetch()` classmethod and `extract()` instance method
2. Set a unique `source` class attribute (3-letter code like "GSP", "SPR")
3. Generate deterministic `source_id` values for stable database keys
4. Use `normalize_url()` and handle timezone conversion to UTC

**For Complex Sources with Multiple Data Formats:**
- Create a base extractor class with shared utility methods (following DRY principle)
- Create separate specialized extractors for each data format (API, HTML, RSS, etc.)

**Detail Page Enrichment Pattern:**
- Separate `BaseDetailExtractor` class for fetching additional data from event detail pages
- Enrichment data stored in `detail_page_enrichments` table (separate from daily ETL upserts)
- `Event.to_pydantic()` accepts optional `detail_page_enrichment` parameter to populate fields like `same_as`
- Incremental enrichment: Process small batches during each ETL run (e.g., 2 pages/day)
- Use case: SPF detail pages contain GSP URLs that enable URL-based deduplication matching

### Database Strategy
- Compressed SQLite checked into git at `data/events.sqlite.gz`
- Auto-extracted to `data/events.sqlite` when missing via `config.ensure_database_exists()`
- Composite primary key: `(source, source_id)`
- Upsert operations handle both new and updated events

### Deduplication Logic
Events are deduplicated using Splink (probabilistic record linkage) with multiple comparison layers:
- Title similarity using Jaro-Winkler distance (threshold ≥0.7)
- Exact date and time matching
- Address matching with fuzzy Jaro distance (threshold ≥0.75)
- URL array intersection (includes `url`, `same_as`, and `website_url` from detail page enrichment)
- Splink trained on blocking rules: exact title match and exact date match

Enrichment data (detail pages, LLM categorization) improves matching accuracy by providing additional URLs and standardized fields.

## Development Guidelines

### Test-Driven Development (TDD) - Primary Approach
**When fixing bugs, prefer TDD:**
1. **Write a failing test first** that exposes the problem or demonstrates the desired behavior
2. **Create minimal test data if needed** in `tests/etl/data/` that reproduces the issue
3. **Run the test** to confirm it fails for the right reasons
4. **Implement the minimal fix** to make the test pass
5. **Verify** that existing tests still pass

**When implementing new features:**
1. Write a minimal implementation, being careful to separate unit-testable logic from network calls
2. Write tests (optionally with fixture data) that minimally cover the new functionality
3. Run tests to ensure they pass
4. As we make the implementation more complete and robust, use the tests to catch bugs early and add to the tests

**TDD Examples:**
- Bug reports: Create a test with data that reproduces the bug, then fix it
- New extractors: Write the basic extractor logic against fixture data first, then implement network requests
- Date/time parsing issues: Test edge cases with malformed input data
- Data integrity: Test that invalid data gets dropped rather than creating hallucinated events

**When TDD applies:**
- Fixing parsing errors or data quality issues (always use TDD)
- Adding new data sources (create tests with fixture data first)
- Modifying extraction logic (test edge cases first)
- Any change that could break existing functionality

**When TDD may not be needed:**
- Simple documentation updates or configuration changes
- Minor refactoring with existing comprehensive test coverage
- Exploratory work or prototyping (but add tests before committing)

### Data Integrity Principles
- **Never create hallucinated data**: Drop events with unparseable dates/times rather than using defaults like `datetime.now()`
- **Log errors clearly**: When dropping invalid data, log what was dropped and why
- **Test edge cases**: Always test with malformed/missing data to ensure robust error handling

### Code Quality
- Follow single responsibility principle: separate classes for different data formats
- Use static methods for utility functions that don't need instance state
- Add tests for at least one success and one failure case for each new feature

### Documentation & Clarity
**Philosophy**: Code should be self-documenting through clear naming and typing. Documentation is code that must be maintained—keep it minimal and valuable.

**When to add documentation:**
- **Discoverability**: When common search terms differ from the chosen names (e.g., "rate limit" vs "throttle")
- **Non-obvious intentions**: When the implementation strategy or design choice isn't clear from the code alone
- **Subtle interactions**: When components interact in ways that aren't obvious from reading either in isolation
- **Complex algorithms**: When the logic requires understanding context not visible in the function

**When to avoid documentation:**
- Restating what the code clearly shows (e.g., "Increments counter by 1" for `counter += 1`)
- Describing types already in the signature
- Documenting obvious parameter meanings
- Repeating function names in different words

**Example of good documentation:**
```python
def wait_if_needed(self, url: str, delay_seconds: float = 2.0) -> None:
    """Sleep if needed to respect per-domain rate limits."""
    # Note: "rate limits" helps discoverability for those searching that term
```

**Example of unnecessary documentation:**
```python
def _extract_domain(url: str) -> str:
    """Extract domain from URL."""  # ← Restates the function name
```

## Development Workflows

### Local Development
```bash
# Setup
cp .env.example .env
uv sync --all-extras

# Initialize fresh database
uv run seattle-volunteering init-db

# Run full ETL pipeline
uv run seattle-volunteering pipeline

# Generate site only
uv run seattle-volunteering build-site
```

### Testing
Tests use data in `tests/etl/data/` with real HTML/XML samples. Run with `uv run pytest`.

**Test Data Management:**
- Save real examples: `curl` actual API responses/HTML pages to `tests/etl/data/`
- Create edge case fixtures: Invalid dates, malformed HTML, empty responses
- Use descriptive filenames: `gsp_api_invalid_date.json`, `spr_rss_missing_datetime.xml`

**Integration Testing:**
- Use `uv run seattle-volunteering pipeline` to test the full pipeline with live data
- Use `uv run seattle-volunteering fetch-listings --source X` to test a single source
- Compare output before/after changes to ensure improvements work as expected

### Adding New Data Sources
1. Download example data with curl to `tests/etl/data/` for inspection and unit testing
2. Create extractor in `src/etl/new_source.py` inheriting from `BaseExtractor`
3. Write tests using fixture data until extraction works correctly
4. Implement network request in extractor
5. Add to `extractor_map` in `src/cli.py:_fetch_listings_impl()`
6. Test with `uv run seattle-volunteering fetch-listings --source new_source`
7. Update `DATA_SOURCES.md` status table

## File Structure Notes
- CLI entry point: `src/cli.py` with Click commands
- Manual events (rare): `data/manual_events.yaml` (currently empty)
- Site output: Generated to `docs/index.html` for GitHub Pages
- Templates: Single Jinja2 template in `src/site/templates/index.html.j2`

## Dependencies & External Integrations
- **Web Scraping**: BeautifulSoup4 + requests (no browser automation)
- **Database**: SQLAlchemy with SQLite (no migrations - recreate if schema changes)
- **Scheduling**: GitHub Actions cron at 7AM UTC daily
- **Hosting**: GitHub Pages serves `docs/index.html`
- **Styling**: PicoCSS via CDN, minimal custom CSS for date headers

## Long-Term TODO

See `TODO.md` for a roadmap to consider. When we write new code, try to keep it compatible with future plans. Likewise, when we refactor existing code, consider how it will fit into the long-term vision.