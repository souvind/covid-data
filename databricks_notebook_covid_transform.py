# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Data Transform
# MAGIC
# MAGIC This notebook reads COVID-19 summary JSON data, flattens the countries array, and writes out Parquet for downstream consumption.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date

# COMMAND ----------

# DBTITLE 1,Parameters
# You can set these with dbutils.widgets for production use
input_path = "dbfs:/FileStore/sample_data/covid_summary.json"
output_path = "dbfs:/FileStore/output/parquet"

# COMMAND ----------

# DBTITLE 1,Read JSON
df = spark.read.json(input_path)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Explode Countries Array
countries = df.select(explode(col("Countries")).alias("country")).select("country.*")
display(countries.limit(5))

# COMMAND ----------

# DBTITLE 1,Clean & Rename Columns
cleaned = countries.withColumn("ReportDate", to_date(col("Date"))) \
    .withColumnRenamed("Country", "CountryName") \
    .withColumnRenamed("CountryCode", "CountryISO")

final = cleaned.select(
    "CountryName", "CountryISO", "ReportDate",
    "NewConfirmed", "TotalConfirmed", "NewDeaths", "TotalDeaths",
    "NewRecovered", "TotalRecovered"
)

display(final.limit(5))

# COMMAND ----------

# DBTITLE 1,Write Parquet Output
final.write.mode("overwrite").parquet(output_path)
print(f"Parquet written to: {output_path}")
