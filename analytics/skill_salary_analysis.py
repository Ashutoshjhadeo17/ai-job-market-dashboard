# ======================================================
# Skill vs Salary Analysis
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

required_cols = {"skills", "salary_min", "salary_max"}
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
# Skill Explosion
# ------------------------------------------------------

df["skills"] = df["skills"].fillna("")

df = df.assign(
    skill=df["skills"]
    .str.lower()
    .str.split(",")
).explode("skill")

df["skill"] = df["skill"].str.strip()

df = df[df["skill"] != ""]

# ------------------------------------------------------
# Aggregate Skill ↔ Salary
# ------------------------------------------------------

skill_salary = (
    df.groupby("skill", as_index=False)
      .agg(
          avg_salary=("avg_salary", "mean"),
          job_count=("skill", "count")
      )
)

# Remove noisy skills (too rare)
skill_salary = skill_salary[skill_salary["job_count"] >= 10]

skill_salary = skill_salary.sort_values(
    "avg_salary", ascending=False
)

# ------------------------------------------------------
# Save Output
# ------------------------------------------------------

skill_salary.to_csv(
    OUTPUT_DIR / "skill_salary.csv",
    index=False
)

print("✅ Skill vs Salary analysis completed")
