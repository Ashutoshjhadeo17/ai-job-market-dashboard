# ==========================================================
# AI Job Market Spark Analytics (Production Optimized)
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, lower, trim, count, desc
)
from pathlib import Path
import os

# ==========================================================
# 1. Optimized Spark Session (Docker-Safe + Memory Bound)
# ==========================================================

spark = (
    SparkSession.builder
    .appName("AI Job Market Intelligence")

    # ---- Execution Mode ----
    .master("local[*]")

    # ---- MEMORY CONTROL ----
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "1")

    # ---- PERFORMANCE TUNING ----
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")

    # ---- JVM STABILITY ----
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

    # ---- Disable Spark UI (saves memory) ----
    .config("spark.ui.enabled", "false")

    # ---- Avoid Native Hadoop Warning ----
    .config("spark.hadoop.io.native.lib.available", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Started (Memory Optimized)")

# ==========================================================
# 2. Load Cleaned Data (Container-Safe Path)
# ==========================================================

INPUT_PATH = Path("/app/data/processed/cleaned_jobs.csv")

if not INPUT_PATH.exists():
    raise FileNotFoundError(
        f"❌ Cleaned data not found at {INPUT_PATH}. "
        "Run cleaning stage first."
    )

df = spark.read.csv(str(INPUT_PATH), header=True, inferSchema=True)

row_count = df.count()
print(f"✅ Loaded {row_count:,} job records")
df.printSchema()

# ==========================================================
# 3. Skill Demand Analysis
# ==========================================================

skills_df = (
    df
    .filter(col("skills").isNotNull())
    .withColumn("skill", explode(split(lower(col("skills")), ",")))
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

# ==========================================================
# 4. Location Demand Analysis
# ==========================================================

location_demand = (
    df
    .filter(col("location").isNotNull())
    .groupBy("location")
    .agg(count("*").alias("job_count"))
    .orderBy(desc("job_count"))
)

print("📍 Top Locations")
location_demand.show(10, truncate=False)

# ==========================================================
# 5. Job Title Demand
# ==========================================================

job_title_demand = (
    df
    .filter(col("job_title").isNotNull())
    .groupBy("job_title")
    .agg(count("*").alias("openings"))
    .orderBy(desc("openings"))
)

print("💼 Top Job Titles")
job_title_demand.show(10, truncate=False)

# ==========================================================
# 6. Save Outputs (Safe for Small/Medium Data)
# ==========================================================

OUTPUT_DIR = Path("/app/output/spark")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_df(spark_df, filename):
    """
    Convert small Spark DF to Pandas safely.
    For large-scale production, this should write parquet instead.
    """
    pdf = spark_df.toPandas()
    output_path = OUTPUT_DIR / filename
    pdf.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")


save_df(skill_demand, "skill_demand.csv")
save_df(location_demand, "location_demand.csv")
save_df(job_title_demand, "job_title_demand.csv")

# ==========================================================
# 7. Cleanup
# ==========================================================

spark.stop()
print("🛑 Spark Session Stopped")
print("🎉 Spark Analytics Completed Successfully")
