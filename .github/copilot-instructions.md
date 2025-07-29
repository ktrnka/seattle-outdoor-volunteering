# Seattle Outdoor Volunteering - Copilot Instructions

## Project Overview
An ETL pipeline that scrapes Seattle-area outdoor volunteer events into a SQLite database and generates a static website with PicoCSS. Events are fetched nightly via GitHub Actions and served through GitHub Pages.

## Interaction Guidelines
- When making design desisions that will significantly affect future development, consider how it will fit into the long-term vision and ask for feedback on the approach.
- If instructions don't seem feasible (e.g., extracting data that doesn't exist in the source, conflicting requirements), stop development and ask for guidance rather than making assumptions. Clear communication prevents wasted effort and ensures the right solution.
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

- Use test-driven development when appropriate
- Follow single responsibility principle: separate classes for different data formats

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

**Integration Testing:**
- Use `uv run seattle-volunteering etl` to test the full pipeline with live data
- This verifies all extractors work together and shows actual event counts
- Compare output before/after changes to ensure improvements work as expected

### Adding New Data Sources
1. **Download example data**: If possible, download an example file (HTML, XML, JSON) with curl to `tests/etl/data/` for LLM inspection and unit testing
2. Create extractor in `src/etl/new_source.py` inheriting from `BaseExtractor`
3. **Test-driven development**: Write and iterate on tests using the fixture data until extraction rules work correctly - this avoids slow/flaky network requests and changing data
4. After the extraction is working, then implement the actual network request in the extractor based on any CURL examples or API documentation
5. Add to extractor list in `src/cli.py:etl()`
6. Test by running the ETL with `uv run seattle-volunteering etl --only-run new_source`
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