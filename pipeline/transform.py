"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import DecimalType, TimeType, DateType
import yaml
from delta.tables import DeltaTable

def run_transformation(spark):
  # TODO: Implement Silver layer transformation.
  #
  # Suggested steps:
  #   1. Load pipeline_config.yaml to get input/output paths.
  #   2. Initialise (or reuse) SparkSession.
  #   3. Read each Bronze table.
  #   4. Deduplicate, type-cast, and standardise each table.
  #   5. Apply DQ flagging to the transactions table.
  #   6. Write cleaned tables to silver/.

  with open("../config/pipeline_config.yaml") as pc:
    config = yaml.safe_load(pc)

  # accounts
  df_acc = spark.read.table("nedbank_ovation.bronze.accounts")
  df_acc = df_acc.dropDuplicates(["account_id"])
  df_acc = df_acc.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))
  df_acc = df_acc.withColumn("last_activity_date", to_date(col("last_activity_date"), "yyyy-MM-dd"))
  df_acc = df_acc.withColumn("credit_limit", col("credit_limit").cast(DecimalType(18,2)))
  df_acc = df_acc.withColumn("current_balance", col("current_balance").cast(DecimalType(18,2)))

  # customers
  df_cust = spark.read.table("nedbank_ovation.bronze.customers")
  df_cust = df_cust.dropDuplicates(["customer_id"])
  df_cust = df_cust.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))

  # Transactions
  df_trans = spark.read.table("nedbank_ovation.bronze.transactions")
  df_trans = df_trans.dropDuplicates(["transaction_id"])
  df_trans = df_trans.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
  df_trans = df_trans.withColumn("amount", col("amount").cast(DecimalType(18,2)))
  #Location flattening
  df_trans = df_trans.withColumn("location_province", col("location").getItem("province"))
  df_trans = df_trans.withColumn("location_city", col("location").getItem("city"))
  df_trans = df_trans.withColumn("location_coordinates", col("location").getItem("coordinates"))
  df_trans = df_trans.withColumn("device_id", col("metadata").getItem("device_id"))
  #Metadata flattening
  df_trans = df_trans.withColumn("session_id", col("metadata").getItem("session_id"))
  df_trans = df_trans.withColumn("retry_flag", col("metadata").getItem("retry_flag").cast("boolean"))
  df_trans = df_trans.withColumn("ingestion_time", col("ingestion_time").cast(DateType()))
  #Dropping the object columns that are now flattened
  df_trans = df_trans.drop("location", "metadata")

  data_frames = {
      "accounts": {
          "key" : "account_id",
          "df": df_acc
      }, 
      "customers": {
          "key" : "customer_id",
          "df": df_cust
      }, 
      "transactions": {
          "key" : "transaction_id",
          "df": df_trans
      }
  }

  #Writting dataframes to the silver layer
  for tblname, details in data_frames.items():
      if DeltaTable.isDeltaTable(spark, f"{config["output"]["silver_path"]}/{tblname}"):
          delta_tbl = DeltaTable.forPath(spark, f"{config["output"]["silver_path"]}/{tblname}")
          delta_tbl.alias("t").merge(
              details["df"].alias("s"),
              f"t.{details["key"]} = s.{details["key"]}"
          ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      else:
          details["df"].write.format("delta").mode("overwrite").save(f"{config["output"]["silver_path"]}/{tblname}")
