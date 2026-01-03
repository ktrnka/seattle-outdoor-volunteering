# SPF Detail Page Enrichment - Implementation Plan

## Problem Statement
Example of the problem:
1/7 "Scotch Broom Patrol"
- 3 events (GSP, seattle.gov, SPF) but the merging process didn't merge SPF with the others because the SPF record is lacking detail
- SPF detail page does have the correct information linking to the GSP URL

Event 1:
- https://seattle.greencitypartnerships.org/event/42741
- https://www.seattle.gov/parks/volunteer/volunteer-calendar?trumbaEmbed=view%3devent%26eventid%3d194367045

Event 2 (duplicate that was not merged with Splink)
https://www.seattleparksfoundation.org/event/scotch-broom-patrol-4

## Current State Survey

### 1. Detail Page Extractor Pattern - ✅ EXISTS
- **Base class**: `BaseDetailExtractor` in `src/etl/base.py`
  - Takes `url` and `raw_data` in constructor
  - Has `fetch(url)` classmethod to fetch and create extractor instance
  - Has `extract()` method to extract a single Event
- **Example implementation**: `GSPDetailExtractor` in `src/etl/gsp.py` (lines 297-352)
  - Demonstrates the pattern for fetching and extracting from detail pages
- **Status**: Pattern is well-established, ready to implement for SPF

### 2. SPF List Extractor - ✅ EXISTS
- **Location**: `src/etl/spf.py`
- **Current capabilities**:
  - Extracts events from listing page JSON-LD structured data
  - Creates `SPFSourceEvent` with organizer and location info
  - Stores structured data in `source_dict` field as JSON
  - Already captures `organizer.same_as` and `location.same_as` fields from schema.org data
- **Gap**: Listing page JSON-LD may not include `same_as` links that appear on detail pages
- **Status**: Working, but missing detail page data

### 3. Enrichment Storage Pattern - ✅ EXISTS
- **Table**: `enriched_source_events` in database
  - Primary key: `(source, source_id)`
  - Fields: `llm_categorization`, `llm_request_metadata`, `created_at`, `processing_status`, `error_message`
  - Used for LLM-based event categorization
- **Access pattern**:
  - `db.get_uncategorized_source_events(limit)` - find events needing enrichment
  - `db.store_event_enrichment(source, source_id, data)` - store enrichment
  - Events auto-join with enrichment when retrieved via `get_source_event()`
- **CLI command**: `dev enrich-source-events <max_events>`
  - Processes uncategorized events in batches
  - Stores LLM results in enrichment table
- **Status**: Pattern exists and is working for LLM categorization

### 4. same_as Field - ✅ EXISTS
- **In Event model** (`src/models.py`):
  - Field: `same_as: Optional[HttpUrl]` - URL of canonical/primary version
  - Used to link to alternate source for the same event
- **In SPR extractor**: Already extracts `same_as` from RSS feed (links to GSP)
- **In deduplication** (`src/etl/splink_dedupe.py`):
  - Creates `urls` list combining `url` and `same_as`
  - Uses `ArrayIntersectAtSizes` comparison on URLs for matching
  - Enables cross-source matching when URLs overlap
- **Status**: Field exists and is used in deduplication, ready to populate from SPF detail pages

### 5. Test Data - ⚠️ PARTIAL
- **Exists**: `tests/etl/data/spf_events.html` - listing page with multiple events
- **Missing**: SPF detail page HTML for testing detail extractor
- **Action needed**: Download example detail page (e.g., scotch-broom-patrol-4)

## Implementation Plan

### Phase 1: Detail Page Extractor (TDD approach)
1. **Download test data**: Save SPF detail page HTML to `tests/etl/data/spf_detail_page.html`
2. **Create test**: Write test in `tests/etl/test_etl_spf.py` for detail page extraction
3. **Implement extractor**: Create `SPFDetailExtractor(BaseDetailExtractor)` in `src/etl/spf.py`
   - Parse JSON-LD from detail page (likely has more complete `organizer.sameAs` data)
   - Extract registration URL if available
   - Return single Event with populated `same_as` field
4. **Verify**: Run test to ensure extraction works

### Phase 2: Detail Page Enrichment Table
1. **Create new table**: `detail_page_enrichments`
   - Primary key: `(source, source_id)`
   - Fields:
     - `detail_page_url` - URL that was fetched
     - `same_as_url` - Extracted sameAs link (e.g., to GSP)
     - `registration_url` - Registration/signup URL if different from main URL
     - `detail_page_json` - Full JSON-LD data from detail page
     - `fetched_at` - When the detail page was crawled
     - `processing_status` - "success", "failed", "pending"
     - `error_message` - Only set if failed
   - Rationale: Separate table prevents daily ETL upserts from overwriting enrichment data

2. **Update database.py**:
   - Add `DetailPageEnrichment` SQLAlchemy model
   - Add query methods: `get_unenriched_detail_pages()`, `store_detail_page_enrichment()`
   - Update `get_source_events()` to optionally join with enrichment for deduplication

3. **Implement CLI command**: `seattle-volunteering dev enrich-spf-detail-pages [--max N]`
   - Query for SPF events without detail page enrichment
   - Fetch detail pages (rate-limited, default 1 per run to be nice to servers)
   - Parse and store enrichment data
   - Add to GitHub Actions workflow (run after main ETL)

### Phase 3: Merge Integration
1. **Update deduplication**: Modify `load_source_events()` in splink_dedupe.py
   - Join with `detail_page_enrichments` table
   - Include `same_as_url` from enrichment in the `urls` array for matching
   - This allows SPF events to match with GSP events via shared URLs
2. **Test**: Verify that Scotch Broom Patrol events now merge correctly

## Design Decision: ✅ DECIDED

**Use separate enrichment table for detail page data**

Why this approach:
- **Persistence**: Daily ETL upserts won't overwrite enrichment data
- **Efficiency**: Only crawl detail pages once (or weekly), not daily
- **Separation**: Keeps list-page extraction separate from detail-page enrichment
- **Flexibility**: Can store full JSON-LD data for future use
- **Pattern match**: Similar to existing `enriched_source_events` table

The enrichment data will be joined during deduplication to provide `same_as` URLs for matching.