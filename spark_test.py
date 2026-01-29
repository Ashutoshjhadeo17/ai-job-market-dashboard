from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("spark-test") \
    .getOrCreate()

print("SPARK STARTED SUCCESSFULLY")

spark.stop()
