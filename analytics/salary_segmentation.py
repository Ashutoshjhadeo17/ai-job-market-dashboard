# ======================================================
# Salary Segmentation Analysis
# ======================================================

from pathlib import Path
import pandas as pd

# ------------------------------------------------------
# Paths
# ------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "cleaned_jobs.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"

OUTPUT_DIR.mkdir(exist_ok=True)

# ------------------------------------------------------
# Load Data
# ------------------------------------------------------

try:
    df = pd.read_csv(INPUT_FILE)
except Exception as e:
    raise RuntimeError(f"Failed to load cleaned data: {e}")

required_cols = {"salary_min", "salary_max", "job_type", "location"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# ------------------------------------------------------
# Salary Cleaning
# ------------------------------------------------------

df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")

df = df.dropna(subset=["salary_min", "salary_max"])
df = df[(df["salary_min"] > 0) & (df["salary_max"] > 0)]

df["avg_salary"] = (df["salary_min"] + df["salary_max"]) / 2

# ------------------------------------------------------
# Job Type Salary Analysis
# ------------------------------------------------------

job_type_salary = (
    df.groupby("job_type", as_index=False)
      .agg(
          avg_salary=("avg_salary", "mean"),
          job_count=("job_type", "count")
      )
)

job_type_salary = job_type_salary[job_type_salary["job_count"] >= 10]
job_type_salary = job_type_salary.sort_values("avg_salary", ascending=False)

job_type_salary.to_csv(
    OUTPUT_DIR / "salary_by_job_type.csv",
    index=False
)

# ------------------------------------------------------
# Location Salary Analysis
# ------------------------------------------------------

df["location"] = df["location"].fillna("Unknown").str.title()

location_salary = (
    df.groupby("location", as_index=False)
      .agg(
          avg_salary=("avg_salary", "mean"),
          job_count=("location", "count")
      )
)

location_salary = location_salary[location_salary["job_count"] >= 10]
location_salary = location_salary.sort_values("avg_salary", ascending=False)

location_salary.to_csv(
    OUTPUT_DIR / "salary_by_location.csv",
    index=False
)

print("✅ Salary segmentation analysis completed")
