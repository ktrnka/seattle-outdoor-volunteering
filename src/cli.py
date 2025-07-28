import click
import gzip
import shutil
from pathlib import Path

from .config import DB_PATH, DB_GZ
from .etl.gsp import GSPExtractor
from .etl.spf import SPFExtractor
from .etl.spr import SPRExtractor
from .site import generator


@click.group()
def cli(): ...


@cli.command()
def etl():
    """Run all extractors and build/compact DB."""
    from .etl import utils

    for extractor_class in [GSPExtractor, SPRExtractor, SPFExtractor]:
        # Fetch raw data and extract events
        extractor = extractor_class.fetch()
        events = extractor.extract()

        # Save events to database
        utils.upsert_events(events)

        click.echo(f"{extractor_class.__name__}: {len(events)} events")

    # gzip-compress for committing
    with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
        shutil.copyfileobj(src, dst)


@cli.command()
def build_site():
    """Generate static site into docs/."""
    generator.build(Path("docs"))
    click.echo("Site built → docs/index.html")


if __name__ == "__main__":
    cli()
