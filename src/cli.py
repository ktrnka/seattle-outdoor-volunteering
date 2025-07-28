import click
import gzip
import shutil
import sqlite_utils
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
    db = sqlite_utils.Database(DB_PATH)
    db["events"].create({
        "source": str, "source_id": str, "title": str,
        "start": str, "end": str, "venue": str, "address": str,
        "url": str, "cost": str, "lat": float, "lon": float, "tags": str
    }, pk=("source","source_id"), if_not_exists=True)
    session = None
    for extractor in [GSPExtractor, SPRExtractor, SPFExtractor]:
        n = extractor(session).run()
        click.echo(f"{extractor.__name__}: {n} events")
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
