from pathlib import Path
import os
import gzip
import shutil
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "events.sqlite"
DB_GZ = DATA_DIR / "events.sqlite.gz"

# ── Load .env when present ────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env", override=False)

# Optional keys (tests and prod run fine if missing)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GSP_API_KEY = os.getenv("GSP_API_KEY")        # future-proofing


def ensure_database_exists() -> None:
    """Ensure the uncompressed database exists by extracting from gzipped version if needed."""
    if not DB_PATH.exists() and DB_GZ.exists():
        print(f"Extracting {DB_GZ} to {DB_PATH}")
        with gzip.open(DB_GZ, 'rb') as f_in:
            with open(DB_PATH, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
