# Databricks notebook source
# MAGIC %sql
# MAGIC select * from json.`dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

df.write.saveAsTable("Gayatri.Constructor")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df.drop("url")

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

# MAGIC %fs ls