import pandas as pd
import os
import sys
import re

# -----------------------------
# Configuration
# -----------------------------
INPUT_PATH = "output/job_title_demand.csv"
OUTPUT_DIR = "features"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "job_title_features.csv")

REQUIRED_COLUMNS = {"job_title", "openings"}

# -----------------------------
# Fail Fast Helper
# -----------------------------
def fail(msg: str):
    print(f"❌ ERROR: {msg}")
    sys.exit(1)

# -----------------------------
# Normalization Rules
# -----------------------------
SENIORITY_PATTERNS = {
    "junior": r"\b(junior|jr|entry|intern)\b",
    "mid": r"\b(mid|associate|intermediate)\b",
    "senior": r"\b(senior|sr|lead|principal|staff)\b",
    "manager": r"\b(manager|head|director)\b",
}

TECH_KEYWORDS = [
    "engineer", "developer", "scientist", "analyst",
    "architect", "programmer", "ml", "ai", "data",
    "cloud", "devops", "backend", "frontend"
]

ROLE_FAMILIES = {
    "engineering": r"\b(engineer|developer|architect|programmer)\b",
    "data": r"\b(data|ml|ai|scientist|analytics)\b",
    "product": r"\b(product|owner|manager)\b",
    "design": r"\b(design|ui|ux)\b",
    "sales": r"\b(sales|business development|account)\b",
    "marketing": r"\b(marketing|growth|seo|content)\b",
    "operations": r"\b(operations|ops|support|admin)\b",
}

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
missing = REQUIRED_COLUMNS - set(df.columns)
if missing:
    fail(f"Missing required columns: {missing}")

if df.empty:
    fail("Job title demand file is empty")

# -----------------------------
# Cleaning
# -----------------------------
df["job_title"] = (
    df["job_title"]
    .astype(str)
    .str.strip()
    .str.lower()
)

df = df[df["job_title"] != ""]
df = df[df["job_title"] != "unknown"]
df = df[df["openings"] > 0]

# -----------------------------
# Feature Engineering
# -----------------------------
def detect_seniority(title: str) -> str:
    for level, pattern in SENIORITY_PATTERNS.items():
        if re.search(pattern, title):
            return level
    return "unspecified"


def detect_role_family(title: str) -> str:
    for family, pattern in ROLE_FAMILIES.items():
        if re.search(pattern, title):
            return family
    return "other"


def is_tech_role(title: str) -> bool:
    return any(keyword in title for keyword in TECH_KEYWORDS)


df["seniority"] = df["job_title"].apply(detect_seniority)
df["role_family"] = df["job_title"].apply(detect_role_family)
df["is_tech"] = df["job_title"].apply(is_tech_role)

# Demand share
total_openings = df["openings"].sum()
df["demand_pct"] = (df["openings"] / total_openings).round(4)

# -----------------------------
# Normalized Display Title
# -----------------------------
df["normalized_title"] = (
    df["job_title"]
    .str.replace(r"[^a-zA-Z\s]", "", regex=True)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

# -----------------------------
# Sorting (Dashboard Friendly)
# -----------------------------
df = df.sort_values(
    by=["openings"],
    ascending=False
)

# -----------------------------
# Save Output
# -----------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    df.to_csv(OUTPUT_PATH, index=False)
except Exception as e:
    fail(f"Failed to write output CSV: {e}")

print("✅ Job title feature engineering completed")
print(f"📁 Saved to: {OUTPUT_PATH}")
print(f"🔢 Total titles processed: {len(df)}")
