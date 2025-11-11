# Databricks notebook source
# ============================================================
# 02_transformation_silver_enrichment.py
# ============================================================
# Purpose:
#   Transform Bronze data by cleaning, deduplicating,
#   and enriching with CRM details.
#
# Layers:
#   - Silver: Curated, enriched, clean data
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, monotonically_increasing_id

spark = SparkSession.builder.appName("SilverLayerTransformation").getOrCreate()

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
bronze_path = "/mnt/bronze/call_transcripts/"
silver_path = "/mnt/silver/enriched_calls/"
crm_path = "/mnt/raw/crm_customers.csv"

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
df_bronze = spark.read.format("delta").load(bronze_path)
df_crm = spark.read.option("header", True).csv(crm_path)

# ------------------------------------------------------------
# CLEAN & STANDARDIZE
# ------------------------------------------------------------
df_bronze_clean = (
    df_bronze
    .dropDuplicates(["call_id"])  # remove duplicates
    .withColumn("transcript", trim(col("transcript")))
    .withColumn("transcript_lower", lower(col("transcript")))
)

# ------------------------------------------------------------
# ENRICH WITH CRM DATA
# ------------------------------------------------------------
df_silver = (
    df_bronze_clean.join(df_crm, on="caller_id", how="left")
    .withColumn("record_id", monotonically_increasing_id())
)

# ------------------------------------------------------------
# WRITE TO SILVER DELTA TABLE
# ------------------------------------------------------------
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)

print("✅ Silver layer created successfully at:", silver_path)

# ------------------------------------------------------------
# DEMO: MERGE Example (Schema Evolution + Upsert)
# ------------------------------------------------------------
# Simulate incremental load
from delta.tables import DeltaTable

silver_table = DeltaTable.forPath(spark, silver_path)

new_records = spark.createDataFrame([
    ("C125", "U569", "2025-11-08T11:00:00Z", "New order delayed", "Arjun Mehta", "South", "Appliances")
], ["call_id", "caller_id", "timestamp", "transcript", "customer_name", "region", "product"])

silver_table.alias("tgt").merge(
    new_records.alias("src"),
    "tgt.call_id = src.call_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("✅ Merge completed successfully!")
