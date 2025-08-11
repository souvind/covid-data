# COVID-19 Data Pipeline (Azure + Databricks friendly)

**Author:** Souvind N K  
**Role:** Data Engineer | Azure & Databricks

## Project Overview
This is a compact end-to-end data engineering project that demonstrates extracting COVID-19 case data from a public API,
transforming it with PySpark, and loading it to a cloud-friendly format (Parquet / Delta-ready). It is designed to be
easy to run locally for evaluation and straightforward to adapt for Azure Databricks + Azure Data Lake + Synapse.

**Key Components**
- `extract.py` : Fetches public COVID-19 summary data and saves a sample JSON.
- `transform.py` : PySpark script to read the JSON, clean/transform, and write Parquet output.
- `load.py` : Example script showing how to upload Parquet to Azure Data Lake Storage Gen2 (uses azure-storage-blob).
- `sql_scripts/create_reporting_tables.sql` : Example SQL to create aggregated reporting tables (Synapse / SQL).
- `sample_data/` : Small sample JSON for local testing.
- `requirements.txt` : Python dependencies.
- `.gitignore` : Files to ignore in Git.
- `pipeline_diagram.png` : Placeholder image filename (add your diagram here).

---

## Quick start (run locally)
1. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

2. Extract (fetch sample data):
```bash
python extract.py
# This will create sample_data/covid_summary.json
```

3. Transform (run PySpark locally):
```bash
# If you have pyspark installed, run:
python transform.py --input sample_data/covid_summary.json --output output/parquet
```

4. Load (upload to ADLS / Blob) — configure credentials in `load.py` or use environment variables:
```bash
python load.py --local-path output/parquet --container-name my-container --storage-account myaccount
```

---

## Notes for Azure Databricks / ADLS / Synapse
- Upload `transform.py` into a Databricks notebook or convert it into a notebook. Use cluster with PySpark.
- For ADLS Gen2 upload, set `AZURE_STORAGE_CONNECTION_STRING` as an environment variable or use Managed Identity.
- Use `Delta Lake` for ACID streaming / incremental updates.

---

## Contact
Souvind N K — souvind.souvi@gmail.com  
LinkedIn: https://www.linkedin.com/in/souvind-sajeev/

