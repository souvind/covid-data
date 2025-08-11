# transform.py
# PySpark job (script) to read COVID-19 summary JSON, flatten and write Parquet output.
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date

def main(input_path, output_path):
    spark = SparkSession.builder             .appName("covid-transform")             .getOrCreate()

    print(f"Reading input from {input_path}")
    df = spark.read.json(input_path)

    # The API structure has 'Countries' as an array of country records.
    countries = df.select(explode(col("Countries")).alias("country")).select("country.*")

    # Basic cleaning / type casting
    cleaned = countries.withColumn("ReportDate", to_date(col("Date")))             .withColumnRenamed("Country", "CountryName")             .withColumnRenamed("CountryCode", "CountryISO")

    # Select useful columns
    final = cleaned.select(
        "CountryName", "CountryISO", "ReportDate",
        "NewConfirmed", "TotalConfirmed", "NewDeaths", "TotalDeaths",
        "NewRecovered", "TotalRecovered"
    )

    print("Writing Parquet output to", output_path)
    final.write.mode("overwrite").parquet(output_path)
    print("Done.")
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="sample_data/covid_summary.json")
    parser.add_argument("--output", default="output/parquet")
    args = parser.parse_args()
    main(args.input, args.output)
