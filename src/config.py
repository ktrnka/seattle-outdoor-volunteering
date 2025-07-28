from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "events.sqlite"
DB_GZ    = DATA_DIR / "events.sqlite.gz"

# ── Load .env when present ────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env", override=False)

# Optional keys (tests and prod run fine if missing)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GSP_API_KEY    = os.getenv("GSP_API_KEY")        # future-proofing
