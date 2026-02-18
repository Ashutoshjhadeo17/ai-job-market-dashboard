# ======================================================
# Salary Analytics
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
# Load Data Safely
# ------------------------------------------------------

try:
    df = pd.read_csv(INPUT_FILE)
except Exception as e:
    raise RuntimeError(f"Failed to load cleaned data: {e}")

required_cols = {"job_title", "salary_min", "salary_max"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# ------------------------------------------------------
# Clean Salary Data
# ------------------------------------------------------

df = df.copy()

df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")

df = df.dropna(subset=["salary_min", "salary_max"])
df = df[(df["salary_min"] > 0) & (df["salary_max"] > 0)]

df["avg_salary"] = (df["salary_min"] + df["salary_max"]) / 2

# ------------------------------------------------------
# Salary by Job Title
# ------------------------------------------------------

salary_by_title = (
    df.groupby("job_title", as_index=False)
      .agg(
          avg_salary=("avg_salary", "mean"),
          postings=("job_title", "count")
      )
      .sort_values("avg_salary", ascending=False)
)

# ------------------------------------------------------
# Salary Summary Stats
# ------------------------------------------------------

salary_summary = pd.DataFrame({
    "metric": ["min", "median", "mean", "max"],
    "salary": [
        df["avg_salary"].min(),
        df["avg_salary"].median(),
        df["avg_salary"].mean(),
        df["avg_salary"].max()
    ]
})

# ------------------------------------------------------
# Save Outputs
# ------------------------------------------------------

salary_by_title.to_csv(
    OUTPUT_DIR / "salary_by_title.csv",
    index=False
)

salary_summary.to_csv(
    OUTPUT_DIR / "salary_summary.csv",
    index=False
)

print("✅ Salary analytics generated successfully")
