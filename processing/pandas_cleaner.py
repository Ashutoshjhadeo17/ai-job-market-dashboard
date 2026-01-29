import pandas as pd
import re

# -----------------------------
# STEP 4.2: Load Data
# -----------------------------
df = pd.read_csv("data/raw/jobs_big.csv")

print("Initial shape:", df.shape)
print(df.head())

# -----------------------------
# STEP 4.3: Handle Missing Values
# -----------------------------
text_columns = df.select_dtypes(include="object").columns
numeric_columns = df.select_dtypes(include=["int64", "float64"]).columns

df[text_columns] = df[text_columns].fillna("Unknown")
df[numeric_columns] = df[numeric_columns].fillna(-1)

# -----------------------------
# STEP 4.4: Normalize Skills
# -----------------------------
df["skills"] = (
    df["skills"]
    .str.lower()
    .str.replace(" ", "")
)

# -----------------------------
# STEP 4.5: Normalize Location
# -----------------------------
df["location"] = df["location"].str.strip().str.title()

# -----------------------------
# STEP 4.6: Parse Salary
# -----------------------------
def extract_salary(s):
    if s == "Unknown":
        return -1, -1
    nums = re.findall(r"\d+", str(s))
    if len(nums) >= 2:
        return int(nums[0]) * 1000, int(nums[1]) * 1000
    elif len(nums) == 1:
        val = int(nums[0]) * 1000
        return val, val
    else:
        return -1, -1

df[["salary_min", "salary_max"]] = df["salary_raw"].apply(
    lambda x: pd.Series(extract_salary(x))
)

# -----------------------------
# STEP 4.7: Drop Raw Salary Column
# -----------------------------
df.drop(columns=["salary_raw"], inplace=True)

# -----------------------------
# STEP 4.8: Final Validation
# -----------------------------
print(df.info())
print(df.head())

# -----------------------------
# STEP 4.9: Save Clean Data
# -----------------------------
df.to_csv("data/processed/cleaned_jobs.csv", index=False)

print("✅ Cleaned data saved successfully")
