# Seattle Outdoor Volunteering

Aggregates Seattle-area outdoor volunteer events and publishes them at [ktrnka.github.io/seattle-outdoor-volunteering](https://ktrnka.github.io/seattle-outdoor-volunteering/).

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

## Maintenance

### Updating & pinning GitHub Actions

Actions in `.github/workflows/` are pinned to commit SHAs (with `# vX.Y.Z` comments) to defend against tag-retargeting attacks. To bump them to the latest releases and re-pin, run from the repo root:

```bash
npx actions-up            # interactive: pick which to update, re-pins to SHA
npx actions-up --dry-run  # preview available updates without changing files
npx actions-up --yes      # apply all updates non-interactively
```

[`actions-up`](https://github.com/azat-io/actions-up) scans the workflows, finds newer releases, and rewrites each `uses:` to `@<sha> # <tag>`. Review every SHA change before committing — a changed SHA is exactly what a retargeting attack looks like. Re-run when you see Node-version deprecation warnings in Actions logs.
