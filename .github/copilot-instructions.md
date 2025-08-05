# Seattle Outdoor Volunteering - Copilot Instructions

## Project Overview
An ETL pipeline that scrapes Seattle-area outdoor volunteer events into a SQLite database and generates a static website with PicoCSS. Events are fetched nightly via GitHub Actions and served through GitHub Pages.

## Interaction Guidelines
- When making design decisions that will significantly affect future development, consider how it will fit into the long-term vision and ask for feedback on the approach.
- If instructions don't seem feasible (e.g., extracting data that doesn't exist in the source, conflicting requirements), stop development and ask for guidance rather than making assumptions. Clear communication prevents wasted effort and ensures the right solution.
- **For bug reports and data quality issues**: Always start with TDD - create a failing test that reproduces the problem before attempting any fixes
- **For new features**: Ask whether to use TDD or implement directly, depending on complexity and risk
- After completing a major feature or refactor, do a small post-mortem. Reflect on what went well and what could be improved. Then consider some small incremental changes to our development process that might improve our workflow. Then review them with me and collaborate to update the copilot instructions.

## Architecture & Data Flow
```
Sources (GSP/SPR/SPF) → BaseExtractor → Event models → Deduplication → SQLite → Site Generator → HTML
```

- **ETL Sources**: Each has a dedicated extractor inheriting from `BaseExtractor` in `src/etl/`
- **Deduplication**: Smart precedence-based matching in `src/etl/deduplication.py` - GSP events are canonical
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

### Database Strategy
- Compressed SQLite checked into git at `data/events.sqlite.gz`
- Auto-extracted to `data/events.sqlite` when missing via `config.ensure_database_exists()`
- Composite primary key: `(source, source_id)`
- Upsert operations handle both new and updated events

### Deduplication Logic
Events are deduplicated based on date and title, then a canonical version is created from the data.

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

## Development Workflows

### Local Development
```bash
# Setup
cp .env.example .env
uv sync --all-extras

# Initialize fresh database
uv run seattle-volunteering init-db

# Run full ETL pipeline
uv run seattle-volunteering etl

# Generate site only
uv run seattle-volunteering build-site
```

### Testing
Tests use data in `tests/etl/data/` with real HTML/XML samples. Run with `uv run pytest`.

**Test-Driven Development Workflow:**
- **Bug fixes**: Start by creating a test that reproduces the bug, then fix it
- **Feature development**: Write tests with mock/fixture data before implementing network calls
- **Use realistic test data**: Save actual API responses, HTML pages, etc. to `tests/etl/data/`
- **Test both success and failure**: Include malformed data to test error handling

**Integration Testing:**
- Use `uv run seattle-volunteering etl` to test the full pipeline with live data
- This verifies all extractors work together and shows actual event counts
- Compare output before/after changes to ensure improvements work as expected

**Test Data Management:**
- Save real examples: `curl` actual API responses/HTML pages to `tests/etl/data/`
- Create edge case fixtures: Invalid dates, malformed HTML, empty responses
- Use descriptive filenames: `gsp_api_invalid_date.json`, `spr_rss_missing_datetime.xml`

### Adding New Data Sources
1. **Download example data**: If possible, download an example file (HTML, XML, JSON) with curl to `tests/etl/data/` for LLM inspection and unit testing
2. Create extractor in `src/etl/new_source.py` inheriting from `BaseExtractor`
3. **Test-driven development**: Write and iterate on tests using the fixture data until extraction rules work correctly - this avoids slow/flaky network requests and changing data
4. After the extraction is working, then implement the actual network request in the extractor based on any CURL examples or API documentation
5. Add to extractor list in `src/cli.py:etl()`
6. Test by running the ETL with `uv run seattle-volunteering etl --only-run new_source`
7. Update `DATA_SOURCES.md` status table

### Bug Fixing Protocol
**Always start with a failing test when fixing bugs:**
1. **Reproduce first**: Create test data that reproduces the reported issue
2. **Verify the bug**: Write a test that fails in the expected way
3. **Understand the root cause**: Use the test to understand why it's failing
4. **Fix minimally**: Make the smallest change possible to make the test pass
5. **Verify comprehensively**: Run all tests to ensure no regressions
6. **Log appropriately**: Add clear error messages for dropped/invalid data

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