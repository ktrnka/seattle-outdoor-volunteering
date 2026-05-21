# CLAUDE.md — seattle-outdoor-volunteering

Context for AI-assisted development on this repo.

## Project Overview

ETL pipeline that scrapes Seattle-area outdoor volunteer events nightly and publishes a static site at [ktrnka.github.io/seattle-outdoor-volunteering](https://ktrnka.github.io/seattle-outdoor-volunteering/).

Public GitHub repo. Deployed via GitHub Actions.

## Quick Start

```bash
cd public/seattle-outdoor-volunteering
cp .env.example .env   # first time only
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

## Common Commands

```bash
# Run full ETL + site generation
uv run seattle-volunteering etl

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
| `OPENAI_API_KEY` | LLM enrichment of event descriptions |
| `GITHUB_TOKEN` | CI: GitHub Models API access + commit push |

See `.env.example` for the full list with descriptions.

## Data Sources

Currently scraping 3 sources: Green Seattle Partnership, Seattle Parks & Rec, Seattle Parks Foundation. See `DATA_SOURCES.md` for scraper implementation details and source-specific quirks.
