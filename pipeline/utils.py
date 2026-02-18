# ======================================================
# Pipeline Utilities
# ======================================================

from pathlib import Path
import hashlib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def generate_job_id(row):
    """
    Stable unique ID for deduplication.
    """
    key = f"{row.get('job_title','')}_{row.get('company','')}_{row.get('location','')}"
    return hashlib.sha256(key.encode()).hexdigest()

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def safe_read_parquet(path: Path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
