# ======================================================
# Central Metrics Loader (Robust & Validated)
# ======================================================

from pathlib import Path
import pandas as pd
import os   # ⭐ FIXED: missing import

# ------------------------------------------------------
# Project Paths
# ------------------------------------------------------
from config.settings import OUTPUT_DIR, PROJECT_ROOT

# ------------------------------------------------------
# Expected Schemas (lowercase enforced)
# ------------------------------------------------------
EXPECTED_SCHEMAS = {
    "skill_demand": {"skill", "demand"},
    "location_demand": {"location", "job_count"},
    "job_title_demand": {"job_title", "openings"},
}

# ------------------------------------------------------
# Safe CSV Loader
# ------------------------------------------------------
def _load_csv_safe(name: str) -> pd.DataFrame:
    path = OUTPUT_DIR / f"{name}.csv"

    if not path.exists():
        print(f"⚠️ Missing file: {path.name}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"❌ Failed to load {path.name}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠️ Empty dataset: {path.name}")
        return df

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Schema validation
    expected = EXPECTED_SCHEMAS.get(name)
    if expected:
        missing = expected - set(df.columns)
        if missing:
            print(f"⚠️ Schema mismatch in {name}, missing columns: {missing}")
            return pd.DataFrame()

    return df

# ------------------------------------------------------
# LOAD CLEAN DATASET
# ------------------------------------------------------
def load_clean_dataset():

    # ⭐ FIXED: consistent Path usage
    path = PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.csv"

    if not path.exists():
        print("⚠️ cleaned_jobs.csv missing")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    except Exception as e:
        print("Failed loading cleaned dataset:", e)
        return pd.DataFrame()

# ------------------------------------------------------
# Public API
# ------------------------------------------------------
def load_all_metrics() -> dict:
    """
    Load all dashboard metrics safely.
    Returns empty DataFrames if anything is missing or invalid.
    """
    return {
        "skills": _load_csv_safe("skill_demand"),      # ⭐ FIXED
        "locations": _load_csv_safe("location_demand"),# ⭐ FIXED
        "jobs": _load_csv_safe("job_title_demand"),    # ⭐ FIXED
        "clean": load_clean_dataset(),
    }
