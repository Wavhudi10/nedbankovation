"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""
import yaml

def run_provisioning(spark):
    # TODO: Implement Gold layer provisioning.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read Silver tables.
    #   4. Build dim_customers with surrogate keys and derived age_band.
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    #   7. Write all three Gold tables as Delta Parquet.
    #   8. (Stage 2+) Write dq_report.json to /data/output/.
    with open("../config/pipeline_config.yaml") as pc:
        config = yaml.safe_load(pc)

    #dim_accounts
    df_accounts = spark.sql(
        f"""
        SELECT
            row_number() OVER(ORDER BY account_id) AS account_sk,
            *
        FROM delta.`{config["output"]["silver_path"]}/accounts`
        """
    )
    df_accounts = df_accounts.withColumnRenamed("customer_ref", "customer_id")

    #dim_customers
    df_customers = spark.sql(
        """
            WITH customer_age AS (
                SELECT
                row_number() OVER(ORDER BY customer_id) AS customer_sk,
                floor((date_diff(CAST(ingestion_time AS DATE), dob)) / 365.25) AS age,
                * EXCEPT (dob)
                FROM delta.`{config["output"]["silver_path"]}/customers`
            )

            SELECT
            *,
            CASE
                WHEN age >= 65 THEN "65+"
                WHEN age >= 56 THEN "56-65"
                WHEN age >= 46 THEN "46-55"
                WHEN age >= 36 THEN "36-45"
                WHEN age >= 26 THEN "26-35"
                WHEN age >= 18 THEN "18-25"
            ELSE NULL
            END AS age_band
            FROM customer_age
        """
    )

    #transactions
    df_transactions = spark.sql(
        f"""
            SELECT
                row_number() OVER(ORDER BY transaction_id) AS transaction_sk,
                acc.account_sk,
                cust.customer_sk,
                TO_TIMESTAMP(concat(transaction_date, " ", transaction_time), "yyyy-MM-dd HH:mm:ss") AS transaction_timestamp,
                * EXCEPT (transaction_time)
            FROM delta.`{config["output"]["silver_path"]}/transactions`
            JOIN delta.`{config["output"]["gold_path"]}/dim_accounts` AS acc
                ON transactions.account_id = acc.account_id
            JOIN {config["output"]["gold_path"]}/dim_customers AS cust
                ON acc.customer_id = cust.customer_id
        """
    )

    data_frame = {
        "dim_accounts" : df_accounts,
        "dim_customers" : df_customers,
        "fact_transactions" : df_transactions
    }

    for table_name, df in data_frame.items():
        df.write\
            .mode("overwrite")\
            .format("delta")\
            .option("overwriteSchema", "true")\
            .save(f"{config["output"]["gold_path"]}/{table_name}")