import pandas as pd
import os
import sys
import re

# -----------------------------
# Configuration
# -----------------------------
INPUT_PATH = "data/processed/cleaned_jobs.csv"
OUTPUT_DIR = "features"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "location_features.csv")

REQUIRED_COLUMNS = {"location"}

# -----------------------------
# Fail Fast
# -----------------------------
def fail(msg: str):
    print(f"❌ ERROR: {msg}")
    sys.exit(1)

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
    fail("Cleaned jobs dataset is empty")

# -----------------------------
# Normalize Location Text
# -----------------------------
df["location_raw"] = (
    df["location"]
    .astype(str)
    .str.strip()
    .str.lower()
)

# -----------------------------
# Work Mode Detection
# -----------------------------
def detect_work_mode(loc: str) -> str:
    if re.search(r"\b(remote|work from home|wfh|anywhere)\b", loc):
        return "remote"
    if re.search(r"\b(hybrid)\b", loc):
        return "hybrid"
    return "onsite"

df["work_mode"] = df["location_raw"].apply(detect_work_mode)

# -----------------------------
# City Extraction
# -----------------------------
def extract_city(loc: str) -> str:
    if loc in {"", "unknown"}:
        return "unknown"
    if "remote" in loc:
        return "remote"
    return loc.split(",")[0].strip()

df["city"] = df["location_raw"].apply(extract_city)

# -----------------------------
# Country Detection (India-first, global-safe)
# -----------------------------
INDIA_CITIES = {
    "bangalore", "bengaluru", "hyderabad", "pune", "chennai",
    "mumbai", "delhi", "gurgaon", "noida", "kolkata", "ahmedabad"
}

def detect_country(city: str) -> str:
    if city in INDIA_CITIES:
        return "india"
    if city == "remote":
        return "global"
    return "unknown"

df["country"] = df["city"].apply(detect_country)

# -----------------------------
# Region Bucketing
# -----------------------------
def assign_region(country: str) -> str:
    if country == "india":
        return "asia"
    if country == "global":
        return "global"
    return "unknown"

df["region"] = df["country"].apply(assign_region)

# -----------------------------
# Final Cleanup
# -----------------------------
final_cols = [
    "location",
    "city",
    "country",
    "region",
    "work_mode"
]

df_final = df[final_cols].drop_duplicates()

# -----------------------------
# Save Output
# -----------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    df_final.to_csv(OUTPUT_PATH, index=False)
except Exception as e:
    fail(f"Failed to write output CSV: {e}")

print("✅ Location feature engineering completed")
print(f"📁 Saved to: {OUTPUT_PATH}")
print(f"📍 Unique locations processed: {len(df_final)}")
