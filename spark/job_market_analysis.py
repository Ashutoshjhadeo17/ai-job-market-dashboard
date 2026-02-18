from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, lower, trim, count, desc
)
import os
import pandas as pd

# -------------------------------------------------
# 1. Spark Session (Windows-safe configuration)
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AI Job Market Analysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Started")

# -------------------------------------------------
# 2. Load Cleaned Data (STRICT PATH CHECK)
# -------------------------------------------------
INPUT_PATH = "data/processed/cleaned_jobs.csv"

if not os.path.exists(INPUT_PATH):
    raise FileNotFoundError(
        f"❌ Cleaned data not found at {INPUT_PATH}. "
        "Run pandas_cleaner.py first."
    )

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

print(f"✅ Loaded {df.count()} job records")
df.printSchema()

# -------------------------------------------------
# 3. Skill Demand Analysis
# -------------------------------------------------
skills_df = (
    df
    .filter(col("skills").isNotNull())
    .withColumn(
        "skill",
        explode(split(lower(col("skills")), ","))
    )
    .withColumn("skill", trim(col("skill")))
    .filter(col("skill") != "")
)

skill_demand = (
    skills_df
    .groupBy("skill")
    .agg(count("*").alias("demand"))
    .orderBy(desc("demand"))
)

print("🔥 Top Skills")
skill_demand.show(10, truncate=False)

# -------------------------------------------------
# 4. Location Demand Analysis
# -------------------------------------------------
location_demand = (
    df
    .filter(col("location").isNotNull())
    .groupBy("location")
    .agg(count("*").alias("job_count"))
    .orderBy(desc("job_count"))
)

print("📍 Top Locations")
location_demand.show(10, truncate=False)

# -------------------------------------------------
# 5. Job Title Demand
# -------------------------------------------------
job_title_demand = (
    df
    .filter(col("job_title").isNotNull())
    .groupBy("job_title")
    .agg(count("*").alias("openings"))
    .orderBy(desc("openings"))
)

print("💼 Top Job Titles")
job_title_demand.show(10, truncate=False)

# -------------------------------------------------
# 6. Save Outputs (Spark → Pandas → CSV)
# -------------------------------------------------
# -------------------------------------------------
# 6. Save Outputs (ABSOLUTE, PROJECT-ROOT SAFE)
# -------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output", "spark")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_df(spark_df, filename):
    pdf = spark_df.toPandas()
    path = os.path.join(OUTPUT_DIR, filename)
    pdf.to_csv(path, index=False)
    print(f"✅ Saved: {path}")

save_df(skill_demand, "skill_demand.csv")
save_df(location_demand, "location_demand.csv")
save_df(job_title_demand, "job_title_demand.csv")

# -------------------------------------------------
# 7. Cleanup
# -------------------------------------------------
spark.stop()
print("🛑 Spark Session Stopped")
print("🎉 PHASE 2 COMPLETED SUCCESSFULLY")
