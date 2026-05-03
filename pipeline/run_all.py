"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""
from pyspark.sql import SparkSession
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("nedbank-de-pipeline")\
        .master("local[2]")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()
    run_ingestion(spark)
    run_transformation(spark)
    run_provisioning(spark)

    spark.stop()
