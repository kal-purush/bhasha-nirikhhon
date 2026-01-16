from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("parquet_example").getOrCreate()

# Read the parquet file into a DataFrame
df = spark.read.parquet("MT cars.parquet")

# Create a temporary view for running SQL queries
df.createOrReplaceTempView("parquet_table")

# Perform SQL operations on the DataFrame
results = spark.sql("SELECT count(mpg), model FROM parquet_table group by model")
results.show()