import pandas as pd
import os
import sys

# -----------------------------
# Configuration
# -----------------------------
INPUT_PATH = "output/spark/skill_demand.csv"
OUTPUT_DIR = "features"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "skill_features.csv")

REQUIRED_COLUMNS = {"skill", "demand"}

# -----------------------------
# Helper Functions
# -----------------------------
def fail(msg: str):
    print(f"❌ ERROR: {msg}")
    sys.exit(1)


def categorize_skill(percentile: float) -> str:
    """
    Categorize skills based on demand percentile.
    """
    if percentile >= 0.90:
        return "core"
    elif percentile >= 0.70:
        return "high-demand"
    elif percentile >= 0.40:
        return "mid-demand"
    else:
        return "niche"


# -----------------------------
# Load Data
# -----------------------------
if not os.path.exists(INPUT_PATH):
    fail(f"Input file not found: {INPUT_PATH}")

try:
    df = pd.read_csv(INPUT_PATH)
except Exception as e:
    fail(f"Failed to read CSV: {e}")

# -----------------------------
# Validate Schema
# -----------------------------
missing_cols = REQUIRED_COLUMNS - set(df.columns)
if missing_cols:
    fail(f"Missing required columns: {missing_cols}")

if df.empty:
    fail("Skill demand file is empty")

# -----------------------------
# Cleaning & Normalization
# -----------------------------
df["skill"] = (
    df["skill"]
    .astype(str)
    .str.strip()
    .str.lower()
)

df = df[df["skill"] != ""]              # drop empty skills
df = df[df["skill"] != "unknown"]       # drop placeholders
df = df[df["demand"] > 0]               # drop invalid counts

# -----------------------------
# Feature Engineering
# -----------------------------
# Total demand share
total_demand = df["demand"].sum()
df["demand_pct"] = (df["demand"] / total_demand).round(4)

# Percentile ranking
df["percentile"] = df["demand"].rank(pct=True).round(3)

# Skill category
df["category"] = df["percentile"].apply(categorize_skill)

# Core skill flag (useful for dashboards)
df["is_core_skill"] = df["category"].isin(["core", "high-demand"])

# -----------------------------
# Sorting (dashboard-friendly)
# -----------------------------
df = df.sort_values(
    by=["category", "demand"],
    ascending=[True, False]
)

# -----------------------------
# Save Output
# -----------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    df.to_csv(OUTPUT_PATH, index=False)
except Exception as e:
    fail(f"Failed to write output CSV: {e}")

print("✅ Skill feature engineering completed successfully")
print(f"📁 Saved to: {OUTPUT_PATH}")
print(f"🔢 Total skills processed: {len(df)}")
