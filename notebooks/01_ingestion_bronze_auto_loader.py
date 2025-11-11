# Databricks notebook source
# ============================================================
# 01_ingestion_bronze_auto_loader.py
# ============================================================
# Purpose:
#   Ingest raw call center transcripts into the Bronze Delta layer
#   using Databricks Auto Loader.
#
# Layers:
#   - Bronze: Raw incremental data
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

spark = SparkSession.builder.appName("BronzeLayerIngestion").getOrCreate()

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
raw_path = "/mnt/raw/call_transcripts/"
bronze_path = "/mnt/bronze/call_transcripts/"
checkpoint_path = "/mnt/checkpoints/bronze/"

# ------------------------------------------------------------
# STREAMING INGESTION
# ------------------------------------------------------------
df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(raw_path)
)

# Add metadata columns
df_stream = df_stream.withColumn("ingest_time", current_timestamp()) \
                     .withColumn("source_file", input_file_name())

# ------------------------------------------------------------
# WRITE TO BRONZE DELTA TABLE
# ------------------------------------------------------------
(df_stream.writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint_path)
 .option("mergeSchema", "true")
 .outputMode("append")
 .start(bronze_path)
)

# ============================================================
# To test locally:
# 1. Place sample JSON files in /mnt/raw/call_transcripts/
# 2. Watch /mnt/bronze/call_transcripts/ update automatically
# ============================================================