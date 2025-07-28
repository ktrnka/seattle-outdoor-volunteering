# Seattle Outdoor Volunteering - Copilot Instructions

## Project Overview
An ETL pipeline that scrapes Seattle-area outdoor volunteer events into a SQLite database and generates a static website with PicoCSS. Events are fetched nightly via GitHub Actions and served through GitHub Pages.

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
All events are stored in UTC but displayed in Pacific time (`SEATTLE_TZ`). The site generator creates both `start`/`end` (Pacific) and `start_utc`/`end_utc` (for Google Calendar) fields.

### Extractor Pattern
New data sources should:
1. Inherit from `BaseExtractor` with `fetch()` classmethod and `extract()` instance method
2. Set a unique `source` class attribute (3-letter code like "GSP", "SPR")
3. Generate deterministic `source_id` values for stable database keys
4. Use `normalize_url()` and handle timezone conversion to UTC

**For Complex Sources with Multiple Data Formats:**
- Create a base extractor class with shared utility methods (following DRY principle)
- Create separate specialized extractors for each data format (API, HTML, RSS, etc.)

**Testing Strategy:**
- Use `curl` to download real API responses to `tests/fixtures/` for comprehensive testing
- Include proper headers (Accept, X-Requested-With, User-Agent) when testing AJAX endpoints

### Database Strategy
- Compressed SQLite checked into git at `data/events.sqlite.gz`
- Auto-extracted to `data/events.sqlite` when missing via `config.ensure_database_exists()`
- Composite primary key: `(source, source_id)`
- Upsert operations handle both new and updated events

### Deduplication Logic
Events are deduplicated using a precedence-based approach that groups similar events by title, venue, and time, then selects the canonical version based on source precedence:

**Source Precedence (lower number = higher precedence):**
1. **GSP**: Preferred as canonical (registration URL and source of truth)
2. **SPR**: Clean data source
3. **SPF**: Messiest data source

When duplicate events are found across sources:
- The highest-precedence source becomes canonical (no `same_as` field)
- Lower-precedence duplicates get `same_as` field pointing to canonical event URL
- Site generation filters to canonical events only (`WHERE same_as IS NULL`)
- Events are considered duplicates if they have similar titles, venues, and start times within 2 hours

## Development Guidelines

### Test-Driven Development for Data Sources
When adding new data sources, prefer downloading example files to `tests/fixtures/` and developing extraction rules against static data. This approach:
- Avoids slow and flaky network requests during development
- Prevents issues with changing data underneath us
- Allows rapid iteration on parsing logic
- Creates reliable test fixtures for future regression testing

**API Discovery and Testing:**
- Test API endpoints with `curl` using appropriate headers before implementing extractors

### Code Organization and Maintainability
- Follow single responsibility principle: separate classes for different data formats
- Use base classes with static methods for shared utility functions (DRY principle)

### Asking for Clarification
If instructions don't seem feasible (e.g., extracting data that doesn't exist in the source, conflicting requirements), stop development and ask for guidance rather than making assumptions. Clear communication prevents wasted effort and ensures the right solution.

## Development Workflows

### Local Development
```bash
# Setup
cp .env.example .env
uv sync

# Run full ETL pipeline
uv run seattle-volunteering etl

# Generate site only
uv run seattle-volunteering build-site

# Initialize fresh database
uv run seattle-volunteering init-db
```

### Testing
Tests use fixtures in `tests/fixtures/` with real HTML/XML samples. Run with `uv run pytest`.

**Integration Testing:**
- Use `uv run seattle-volunteering etl` to test the full pipeline with live data
- This verifies all extractors work together and shows actual event counts
- Compare output before/after changes to ensure improvements work as expected

### Adding New Sources
1. **Download example data**: If possible, download an example file (HTML, XML, JSON-LD) to `tests/fixtures/` for unit testing
2. **Test-driven development**: Write and iterate on tests using the fixture data until extraction rules work correctly - this avoids slow/flaky network requests and changing data
3. Create extractor in `src/etl/new_source.py` inheriting from `BaseExtractor`
4. Add to extractor list in `src/cli.py:etl()`
5. Update `DATA_SOURCES.md` status table

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

### Data Quality & Collection
- **Filter SPF events**: Use GitHub Models LLM to classify and filter out non-volunteer events from Seattle Parks Foundation
- ~~**Expand GSP data**: Currently only fetching first 5 events, but many more are available~~ ✅ **COMPLETED**: Now fetching 66 events via API endpoint
- **Validate SPR parsing**: More thorough checking of parsed data from Seattle Parks & Rec
- **RSS/Blog support**: Once LLM integration is ready, add support for RSS feeds and event calendars mentioned in `DATA_SOURCES.md`

### Event Enhancement
- **Standardize titles**: Use LLM to create more informative titles like "Park restoration at Woodland Park with Greg"
- **Event categorization**: Support both parks work and litter cleanup work with user filtering options
- **Enhanced details**: Standardize event information to cover who/what/why/how/when (explaining "why" invasive species removal matters will be challenging)

### User Experience
- **Geographic filtering**: Add distance-based filtering with user location (cookie storage vs neighborhood selector)
- **Site freshness**: Show when site was last updated and data limitations
- **Interactive details**: Click on rows to expand with more event information
- **Mobile optimization**: Ensure responsive design works well on all devices

### Development & Quality
- **PR automation**: Run unit tests on pull requests
- **UI documentation**: Attach desktop and mobile screenshots to GitHub PRs
- **LLM integration**: Foundation for classification, title standardization, and content enhancement
