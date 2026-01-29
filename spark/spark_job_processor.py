from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
from pyspark.sql.functions import col, when, length

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("AI Job Market Dashboard") \
    .getOrCreate()

# -----------------------------
# Schema (STRICT TYPING)
# -----------------------------
schema = StructType([
    StructField("Job Title", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Tags", StringType(), True),
    StructField("Date Posted", StringType(), True),
    StructField("Salary", StringType(), True)
])

# -----------------------------
# Load Cleaned Data
# -----------------------------
df = spark.read.csv(
    "data/processed/cleaned_jobs.csv",
    header=True,
    schema=schema
)

print("Initial Record Count:", df.count())

# -----------------------------
# Basic Transformations
# -----------------------------
df = df.withColumn(
    "job_title_length",
    length(col("Job Title"))
)

df = df.withColumn(
    "remote_flag",
    when(col("Location").contains("Remote"), 1).otherwise(0)
)

# -----------------------------
# Filter Low Quality Rows
# -----------------------------
df = df.filter(col("Job Title") != "Unknown")

# -----------------------------
# Save Spark Output
# -----------------------------
df.write.mode("overwrite").parquet(
    "data/processed/spark_jobs.parquet"
)

print("✅ Spark processing completed successfully")

spark.stop()
