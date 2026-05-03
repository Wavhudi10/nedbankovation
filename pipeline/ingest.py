"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DecimalType
from pyspark.sql.functions import lit
from datetime import datetime
import yaml

def run_ingestion(spark):
  # TODO: Implement Bronze layer ingestion.
  #
  # Suggested steps:
  #   1. Load pipeline_config.yaml to get input/output paths.
  #   2. Initialise a SparkSession with Delta Lake support (local[2]).
  #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
  #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
  #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.
  with open("../config/pipeline_config.yaml") as pc:
    config = yaml.safe_load(pc)

  accounts_schema = StructType([
      StructField("account_id", StringType(), False),
      StructField("customer_ref", StringType(), False),
      StructField("account_type", StringType(), False),
      StructField("account_status", StringType(), False),
      StructField("open_date", StringType(), False),
      StructField("product_tier", StringType(), False),
      StructField("mobile_number", StringType(), True),
      StructField("digital_channel", StringType(), False),
      StructField("credit_limit", DecimalType(), True),
      StructField("current_balance", DecimalType(), False),
      StructField("last_activity_date", StringType(), True)])

  customers_schema = StructType([
      StructField("customer_id", StringType(), False),
      StructField("id_number", StringType(), False),
      StructField("first_name", StringType(), False),
      StructField("last_name", StringType(), False),
      StructField("dob", StringType(), False),
      StructField("gender", StringType(), False),
      StructField("province", StringType(), False),
      StructField("income_band", StringType(), False),
      StructField("segment", StringType(), False),
      StructField("risk_score", IntegerType(), False),
      StructField("kyc_status", StringType(), False),
      StructField("product_flags", StringType(), False)
    ])

  trasactions_schema = StructType([
      StructField("transaction_id", StringType(), False),
      StructField("account_id", StringType(), False),
      StructField("transaction_date", StringType(), False),
      StructField("transaction_time", StringType(), False),
      StructField("transaction_type", StringType(), False),
      StructField("merchant_category", StringType(), True),
      StructField("merchant_subcategory", StringType(), True),
      StructField("amount", DoubleType(), False),
      StructField("currency", StringType(), False),
      StructField("channel", StringType(), False),
      StructField("location", StringType([
          StructField("province", StringType(), True),
          StructField("city", StringType(), True),
          StructField("coordinates", StringType(), True)
      ]), True),
      StructField("metadata", StringType([
          StructField("device_id", StringType(), True),
          StructField("session_id", StringType(), True),
          StructField("retry_flag", BooleanType(), True),
      ]), True)
    ])  

  schemas = {
      "accounts": {
        "schema" : accounts_schema,
        "extension" : config["input"]["accounts_path"].split(".")[-1]
      },
      "customers": {
        "schema" : customers_schema,
        "extension" : config["input"]["customers_path"].split(".")[-1]
      },
      "transactions": {
        "schema" : trasactions_schema,
        "extension" : config["input"]["transactions_path"].split(".")[-1]
      }
  }

  run_timestamp = datetime.now()

  for source, props in schemas.items():
    if props["extension"] == "csv":
      df = spark.read.csv(config["input"][f"{source}_path"], header=True, schema=props["schema"])
    elif props["extension"] == "jsonl" :
      df = spark.read.json(config["input"][f"{source}_path"], schema=props["schema"])
    df = df.withColumn("ingestion_time", lit(run_timestamp))
    df.write\
      .format("delta")\
      .mode("append")\
      .option("overwriteSchema", "true")\
      .save(f"{config["output"]["bronze_path"]}/{source}")
