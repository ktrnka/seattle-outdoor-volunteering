# Seattle Outdoor Volunteering

Aggregates Seattle-area outdoor volunteer events and publishes them at [seattlevolunteering.com](https://seattlevolunteering.com).

## Quick Start

```bash
# Setup
cp .env.example .env
uv sync

# Run full ETL pipeline
uv run seattle-volunteering etl

# Generate site only  
uv run seattle-volunteering build-site
```

## Data Sources

Currently scraping 3 sources nightly via GitHub Actions:
- Green Seattle Partnership (56 events)
- Seattle Parks & Rec (6 events) 
- Seattle Parks Foundation (11 events)

See `DATA_SOURCES.md` for implementation details.
