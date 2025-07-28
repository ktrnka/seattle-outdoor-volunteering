from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlite_utils import Database
from ..config import DB_PATH

def build(output_dir: Path):
    db = Database(DB_PATH)
    rows = list(db["events"].rows)
    env  = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape()
    )
    tmpl = env.get_template("index.html.j2")
    html = tmpl.render(events=rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html, encoding="utf-8")
