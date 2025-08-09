from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "events.sqlite"
DB_GZ = DATA_DIR / "events.sqlite.gz"
