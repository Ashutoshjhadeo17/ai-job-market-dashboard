from pathlib import Path
import os

# ======================================================
# PROJECT ROOT
# ======================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ======================================================
# DATA PATHS
# ======================================================
DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DATA_DIR = RAW_DATA_DIR / "bronze"   # ⭐ NEW

PROCESSED_DATA_DIR = DATA_DIR / "processed"

OUTPUT_DIR = PROJECT_ROOT / "output"

# ======================================================
# DASHBOARD SETTINGS
# ======================================================
DEFAULT_TOP_N = int(os.getenv("TOP_N_RESULTS", 20))

# Auto refresh interval (seconds)
DASHBOARD_REFRESH_SECONDS = int(
    os.getenv("DASHBOARD_REFRESH_SECONDS", 300)
)

# ======================================================
# INGESTION SETTINGS
# ======================================================
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "AI-Job-Market-Intelligence/1.0"
)

# ======================================================
# ENVIRONMENT
# ======================================================
ENV = os.getenv("APP_ENV", "dev")
