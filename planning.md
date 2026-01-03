# ETL Pipeline Refactoring Plan

## Status: Phase 1 Complete ✅

Phase 1 (CLI Refactoring) has been implemented and tested. All tests pass.

---

## Assessment / Problems / Opportunities

### Current State Overview

The ETL pipeline currently has multiple stages that run at different times and places:

**In `etl` CLI command:**
1. Fetch event listings from all sources (GSP, SPR, SPF, SPU, DNDA, EarthCorps, FremontNeighbor, Manual)
2. Upsert source events to database
3. Fetch SPF detail pages (2 per day) - embedded within ETL
4. Record ETL run stats

**In GitHub Actions workflow:**
1. Run `etl` command (listings + SPF detail pages)
2. Run `dev enrich-source-events 50` (LLM categorization) - separate command
3. Run `deduplicate` (Splink deduplication)
4. Run `build-site` (generate static HTML)

### Problem 1: Confusing "Enrichment" Terminology

**Two different enrichment processes:**

1. **LLM Enrichment** (`enriched_source_events` table)
   - Purpose: AI-powered event categorization
   - Storage: `llm_categorization` JSON field with category, reasoning, confidence
   - Triggered: Manually via `dev enrich-source-events N` command
   - Current rate: 50 events per day in production

2. **Detail Page Enrichment** (`detail_page_enrichments` table)
   - Purpose: Scrape additional data from event detail pages (e.g., SPF → GSP URLs)
   - Storage: `enrichment_data` JSON field with source-specific data (e.g., `website_url`)
   - Triggered: Automatically within `etl` command
   - Current rate: 2 SPF pages per day, embedded in main ETL

**Why this is confusing:**
- Both use the word "enrichment" but serve different purposes
- One is automatic (detail pages), one is manual (LLM)
- One is embedded in ETL, one is a separate command
- Database tables have similar structures but different semantics
- When discussing "enrichment" in conversation, it's unclear which one we mean

**Impact areas:**
- CLI commands: `enrich-source-events` vs `_enrich_spf_detail_pages()` helper
- Database tables: `enriched_source_events` vs `detail_page_enrichments`
- Code discussions: "enrichment" is ambiguous without qualification

### Problem 2: Multiple Discrete Steps in Pipeline

**Current workflow requires 4 separate commands:**
```bash
seattle-volunteering etl                      # Extract listings + detail pages
seattle-volunteering dev enrich-source-events 50  # LLM categorization
seattle-volunteering deduplicate              # Merge duplicates
seattle-volunteering build-site               # Generate HTML
```

**Why this is problematic:**
- Easy to forget a step when running locally
- GitHub Actions has 4 separate workflow steps
- No single "run the whole pipeline" command
- Inconsistent: detail page enrichment is auto (in `etl`), but LLM enrichment is manual
- Harder to reason about the overall flow

**Current ETL stages in code:**
1. **Fetch listings**: Each extractor's `fetch()` + `extract()` methods
2. **Fetch detail pages**: `_enrich_spf_detail_pages()` helper function (private, inside `etl`)
3. **LLM categorization**: `dev enrich-source-events` command
4. **Deduplication**: `deduplicate` command
5. **Site generation**: `build-site` command

### Problem 3: Lack of Observability for Enrichment Processes

**What we CAN see:**
- ✅ ETL run stats per source (success/failure, row counts)
- ✅ Data freshness grid (5-day history of successful runs per source)
- ✅ Source events in database queries
- ✅ Canonical events on frontend
- ✅ Debug modal shows merged source events

**What we CANNOT see:**
- ❌ How many events have LLM categorization vs not
- ❌ How many events have detail page enrichment vs not
- ❌ Success/failure rates for enrichment processes
- ❌ Enrichment data in the frontend UI (can't see LLM categories or reasoning)
- ❌ Quick summary: "X% of SPF events have detail page data"
- ❌ Historical enrichment progress over time

**Why this matters:**
- Can't easily tell if enrichment is keeping up with new events
- Can't debug enrichment issues without manual DB queries
- Can't showcase LLM categorization work to users
- No visibility into what's happening during development

### Problem 4: SPF Detail Page Fetch Rate

**Current state:**
- Fetches 2 SPF detail pages per day
- ~60 total SPF events → 30 days to fully enrich
- No throttling between requests (could hit rate limits)
- Hardcoded to SPF source only

**Why this needs improvement:**
- 30-day catchup might be too slow (events are posted weeks ahead, but unclear how far)
- No throttling could cause issues if we increase rate
- When we add detail page enrichment for other sources (GSP, SPR), we'll need reusable throttling
- Throttling logic should be encapsulated in the fetcher, not scattered in callers

**Opportunities:**
- Implement domain-based throttling that's reusable
- Adaptive rate: faster catchup (5/day), slower maintenance (2/day)
- Make it easy to add detail page enrichment for other sources in the future

### Problem 5: Database Layer Concerns (Lower Priority)

**Similar patterns found:**
- `get_uncategorized_source_events()` - left anti-join with `enriched_source_events`
- `get_unenriched_detail_page_events()` - left anti-join with `detail_page_enrichments`
- `store_event_enrichment()` - upsert pattern for LLM data
- `store_detail_page_enrichment()` - upsert pattern for detail page data

**Decision:** Accept some duplication for now (only 2 cases). If we add a 3rd enrichment type, revisit with helper methods.

---

## Plan / Solutions / Implementation

### Phase 1: CLI Refactoring - Separate Stages with Unified Command

**Goal:** Make ETL stages explicit and composable, while providing a single "run everything" command.

#### 1.1 Create Individual Stage Commands

Extract existing functionality into separate CLI commands:

```python
@cli.command()
@click.option("--only-run", type=str, help="Run only the specified extractor")
def fetch_listings(only_run: Optional[str] = None):
    """Fetch event listings from all sources (GSP, SPR, SPF, etc.) - makes network requests"""
    # Current logic from etl() command, minus detail page enrichment
    
@cli.command()
@click.option("--source", type=str, default="SPF", help="Source to enrich")
@click.option("--max-events", type=int, default=5, help="Max events to process")
def fetch_details(source: str, max_events: int):
    """Fetch detail pages for events - makes network requests"""
    # Current _enrich_spf_detail_pages() logic, generalized
    
@cli.command()
@click.option("--max-events", type=int, default=50, help="Max events to categorize")
def fetch_categorizations(max_events: int):
    """Categorize uncategorized events using LLM - makes network requests and uses LLM budget"""
    # Current dev enrich-source-events logic, renamed for clarity
```

**Decision**: Use "fetch-" prefix for all network-requesting commands to make it clear they'll do I/O.
Rate: 5 SPF detail pages per day (no adaptive rate needed).

#### 1.2 Create Unified Pipeline Command

```python
@cli.command()
@click.option("--skip-llm", is_flag=True, help="Skip LLM categorization (faster)")
def pipeline(skip_llm: bool = False):
    """Run complete ETL pipeline: fetch listings → fetch details → categorize → dedupe → build site"""
    # Call each stage in sequence
    # Provide summary at end
```

#### 1.3 Benefits

- Clear stage separation for development and debugging
- Single command for production use
- Easier to test individual stages
- Better error messages (know which stage failed)
- Easier to add new stages in the future

### Phase 2: Request Throttling Infrastructure

**Goal:** Implement reusable, per-domain request throttling for detail page fetchers.

#### 2.1 Create Throttle Manager

```python
# src/etl/request_throttle.py
class RequestThrottle:
    """Per-domain request throttling with configurable delays."""
    
    def __init__(self, delay_seconds: float = 2.0):
        self._last_request_time: Dict[str, float] = {}
        self._delay_seconds = delay_seconds
    
    def wait_if_needed(self, domain: str):
        """Sleep if needed to respect rate limit for this domain."""
        # Implementation: check last request time, sleep if needed
```

#### 2.2 Integrate into BaseDetailExtractor

```python
# src/etl/base.py
class BaseDetailExtractor(abc.ABC):
    _throttle = RequestThrottle(delay_seconds=2.0)
    
    @classmethod
    def fetch(cls, url: str) -> "BaseDetailExtractor":
        domain = extract_domain(url)
        cls._throttle.wait_if_needed(domain)
        # ... existing fetch logic
```

#### 2.3 SPF Fetch Rate Configuration

Update `fetch_details` to use 5 events per day (increased from 2).
No adaptive rate needed - will naturally slow down when queue is empty.

### Phase 3: Observability - Enrichment Stats

**Goal:** Make enrichment progress visible via CLI command, then extend to UI.

#### 3.1 Create Enrichment Stats CLI Command

```python
@dev.command()
@click.option("--source", type=str, help="Filter by source")
def enrichment_stats(source: Optional[str] = None):
    """Show enrichment progress and statistics."""
    # Display:
    # - LLM categorization: X/Y events (Z%)
    # - Detail pages: X/Y SPF events (Z%)
    # - Recent success/failure rates
```

#### 3.2 Add Database Helper Methods

```python
# src/database.py
def get_enrichment_stats(self) -> Dict[str, Dict[str, int]]:
    """Get counts of enriched vs unenriched events by type."""
    # Return structure:
    # {
    #   "llm_categorization": {"enriched": 523, "total": 600},
    #   "detail_pages": {"SPF": {"enriched": 45, "total": 60}}
    # }
```

#### 3.3 Add to Pipeline Output

Show enrichment stats at the end of `pipeline` command:
```
Pipeline complete!
✓ Fetched 56 events from 8 sources
✓ Enriched 5 SPF detail pages (8 remaining)
✓ Categorized 50 events with LLM (127 remaining)
✓ Merged into 423 canonical events
✓ Generated site with 312 upcoming events
```

#### 3.4 Future: Add to Frontend (Optional)

Consider adding enrichment metadata to event debug modal:
- Show LLM category and reasoning
- Show detail page data (website URL, etc.)
- Add enrichment progress indicator to data freshness section

### Phase 4: Naming Improvements

**Goal:** Reduce confusion between the two enrichment types.

#### 4.1 CLI Naming (Immediate - Easy to Change)

Current names → New names:
- `dev enrich-source-events` → `categorize-events` (top-level command)
- `_enrich_spf_detail_pages()` →fetch-categorizations` (top-level command)
- `_enrich_spf_detail_pages()` → `_fetch_detail_pages()` (internal helper)
- `dev enrich-detail-pages` → `fetch-details` (top-level command)

Rationale:
- "fetch-" prefix makes it clear these commands do network requests
- "fetch-categorizations" signals both network I/O and LLM budget usagepages)
- No more overloaded "enrich" term in user-facing commands

#### 4.2 Database Naming (Future - Requires Migration)

Consider for future schema changes:
- `enriched_source_events` → `llm_categorizations`
- `detail_page_enrichments` → `detail_page_data` or keep as-is

Decision: Document current naming clearly, defer DB changes until next schema migration.

#### 4.3 Code Comments and Documentation

Add clear docstrings distinguishing:
- **LLM Categorization**: AI-powered event categorization (category, reasoning, confidence)
- **Detail Page Data**: Additional data scraped from event detail pages (URLs, descriptions, etc.)

---

## Implementation Order

### ✅ Phase 1 - COMPLETED

**Changes made:**
1. **Extracted stage functions** into reusable implementations:
   - `_fetch_listings_impl()` - Fetch event listings from sources
   - `_fetch_spf_detail_pages()` - Fetch SPF detail pages specifically
   - `_fetch_categorizations_impl()` - LLM categorization

2. **Added FetchResult named tuple** for clearer return values (success, error counts)

3. **Created new top-level CLI commands**:
   - `fetch-listings --source <SOURCE>` - Fetch event listings (optional source filter)
   - `fetch-details` - Fetch SPF detail pages (5 per day)
   - `fetch-categorizations` - LLM categorization (50 per day)
   - `pipeline` - Unified command that runs all stages

4. **Removed legacy/redundant commands**:
   - Removed `etl` command (replaced by fetch-listings + fetch-details + pipeline)
   - Kept deprecated `dev enrich-*` commands for backward compatibility

5. **Consistency improvements**:
   - All commands use `--source` parameter (not `--only-run`)
   - Removed fake genericity (detail pages are SPF-specific, not generic)
   - Removed verbose docstrings that added little value
   - Removed `--skip-llm` from pipeline (use individual commands instead)

6. **Net result**: 580 lines (down from 657) - 77 fewer lines while adding new functionality

7. **Verified**:
   - All 69 tests pass ✅
   - CLI help shows all commands correctly ✅
   - Backward compatibility maintained ✅

### 🔲 Phase 2 - TODO (Request Throttling)

---

## Testing Strategy

For each phase:
1. **Unit tests**: Test new helper functions with mocked data
2. **Integration tests**: Run full pipeline locally against test data
3. **Manual verification**: Check GitHub Actions logs after deployment
4. **Data quality**: Verify enrichment stats match expectations

Consider adding a test that loads the actual committed database to validate enrichment stats:
```python
def test_enrichment_stats_on_real_db():
    """Test enrichment stats against committed database."""
    with Database() as db:
        stats = db.get_enrichment_stats()
        # Validate structure, ensure counts are reasonable
```
