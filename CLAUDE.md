# CLAUDE.md — seattle-outdoor-volunteering

Context for AI-assisted development on this repo.

## Project Overview

ETL pipeline that scrapes Seattle-area outdoor volunteer events nightly and publishes a static site at [ktrnka.github.io/seattle-outdoor-volunteering](https://ktrnka.github.io/seattle-outdoor-volunteering/).

Public GitHub repo. Deployed via GitHub Actions.

## Quick Start

From this repo's root:

```bash
echo "GITHUB_TOKEN=..." > .env   # first time only; LLM steps need it (loaded via python-dotenv)
uv sync --locked
uv run seattle-volunteering --help
```

## Key Architecture

| What | Where |
|------|-------|
| CLI entry point | `seattle-volunteering` (installed via `uv sync`) |
| Pipeline source | `src/` |
| SQLite database + outputs | `data/` |
| Generated static site | `docs/` (served as GitHub Pages) |
| Tests | `tests/` |
| Data source details | `DATA_SOURCES.md` |
| Deduplication (Splink) model, tuning, evaluation | `DEDUPLICATION.md`; check changes with `uv run seattle-volunteering dev dedupe-eval` |

## Common Commands

```bash
# Run the full pipeline (what the nightly GitHub Actions job runs)
uv run seattle-volunteering pipeline

# Generate site only (skip scraping)
uv run seattle-volunteering build-site

# Run tests
uv run pytest
```

## Deployment

GitHub Actions runs the full ETL nightly and pushes updated `docs/` to `main`, which triggers GitHub Pages rebuild. Manual pushes to `main` also trigger a redeploy.

## Environment Variables

| Var | Purpose |
|-----|---------|
| `GITHUB_TOKEN` | GitHub Models API access for LLM steps (needed locally too, read from `.env`); in CI, also commit push |

That's the only variable the code reads.

## Data Sources

Currently scraping 3 sources: Green Seattle Partnership, Seattle Parks & Rec, Seattle Parks Foundation. See `DATA_SOURCES.md` for scraper implementation details and source-specific quirks.
