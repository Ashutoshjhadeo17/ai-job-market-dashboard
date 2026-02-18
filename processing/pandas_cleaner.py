import pandas as pd
import re
import logging
from pathlib import Path
from datetime import datetime

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
RAW_INPUT = Path("data/raw/jobs_big.csv")
OUTPUT_PATH = Path("data/processed/cleaned_jobs.csv")

REQUIRED_COLUMNS = [
    "job_title",
    "company",
    "location",
    "skills",
    "salary_raw",
    "source",
    "job_type",
    "date_posted"
]

DEFAULT_STRING = "Unknown"
DEFAULT_INT = 0

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# SAFE SALARY PARSER
# --------------------------------------------------
def parse_salary(value):
    """
    Extract salary_min and salary_max from messy text.
    Handles worst-case garbage safely.
    """
    if pd.isna(value):
        return DEFAULT_INT, DEFAULT_INT

    text = str(value).lower()

    nums = re.findall(r"\d+", text)
    try:
        if len(nums) >= 2:
            return int(nums[0]) * 1000, int(nums[1]) * 1000
        elif len(nums) == 1:
            val = int(nums[0]) * 1000
            return val, val
    except Exception:
        pass

    return DEFAULT_INT, DEFAULT_INT

# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------
def main():
    logger.info("Starting pandas data cleaning pipeline")

    if not RAW_INPUT.exists():
        raise FileNotFoundError(f"Input file not found: {RAW_INPUT}")

    df = pd.read_csv(RAW_INPUT)
    logger.info(f"Loaded raw data: {df.shape}")

    # --------------------------------------------------
    # ENSURE REQUIRED COLUMNS EXIST
    # --------------------------------------------------
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            logger.warning(f"Missing column detected: {col}. Filling with default.")
            df[col] = DEFAULT_STRING

    # --------------------------------------------------
    # TEXT NORMALIZATION
    # --------------------------------------------------
    for col in ["job_title", "company", "source", "job_type"]:
        df[col] = df[col].astype(str).fillna(DEFAULT_STRING).str.strip()

    df["location"] = (
        df["location"]
        .astype(str)
        .fillna(DEFAULT_STRING)
        .str.strip()
        .str.title()
    )

    df["skills"] = (
        df["skills"]
        .astype(str)
        .fillna(DEFAULT_STRING)
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )

    # --------------------------------------------------
    # DATE NORMALIZATION
    # --------------------------------------------------
    df["date_posted"] = pd.to_datetime(
        df["date_posted"],
        errors="coerce"
    )

    # --------------------------------------------------
    # SALARY EXTRACTION
    # --------------------------------------------------
    salary_df = df["salary_raw"].apply(
        lambda x: pd.Series(parse_salary(x), index=["salary_min", "salary_max"])
    )

    df = pd.concat([df, salary_df], axis=1)
    df.drop(columns=["salary_raw"], inplace=True)

    # --------------------------------------------------
    # FINAL SAFETY CLEAN
    # --------------------------------------------------
    df["salary_min"] = df["salary_min"].fillna(DEFAULT_INT).astype(int)
    df["salary_max"] = df["salary_max"].fillna(DEFAULT_INT).astype(int)

    df["ingested_at"] = datetime.utcnow()

    # --------------------------------------------------
    # SAVE OUTPUT
    # --------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    logger.info(f"Cleaned data saved successfully → {OUTPUT_PATH}")
    logger.info(f"Final dataset shape: {df.shape}")

# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    main()
