# Seattle-Outdoor-Volunteering

Aggregates Seattle-area outdoor volunteer events into a compressed SQLite
database (checked into the repo) and publishes a minimal static website styled
with [PicoCSS](https://picocss.com).

* ETL runs nightly via GitHub Actions, fetching:
  1. Green Seattle Partnership event calendar [56]
  2. Seattle Parks & Rec volunteer calendar [6]
  3. Seattle Parks Foundation events [11]

* Static-site generation then converts the cleaned tables to
  `docs/index.html`, ready for GitHub Pages hosting.

* A Click CLI lets you run everything locally for debugging.

* Manual or hard-to-scrape events live in `data/manual_events.yaml`.

See DATA_SOURCES.md for implementation status.

cp .env.example .env   # then fill in any keys you have
