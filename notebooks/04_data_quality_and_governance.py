# Databricks notebook source
# ============================================================
# 04_data_quality_and_governance.py
# ============================================================
# Purpose:
#   Implement Unity Catalog-based governance, role-based
#   access control, and data quality validation using
#   Delta expectations.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("GovernanceAndQuality").getOrCreate()

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
gold_path = "/mnt/gold/nlp_insights/"
catalog_name = "call_intel_catalog"
schema_name = "production"
table_name = "nlp_insights"

# ------------------------------------------------------------
# STEP 1: Create Catalog, Schema, and Table in Unity Catalog
# ------------------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE {schema_name}")

# Register the Gold table under Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name}
USING DELTA
LOCATION '{gold_path}'
""")

print(f"✅ Registered table: {catalog_name}.{schema_name}.{table_name}")

# ------------------------------------------------------------
# STEP 2: Assign Permissions (RBAC Simulation)
# ------------------------------------------------------------
# These commands require account admin privileges in production.
# Adjust groups as per your AAD setup.

spark.sql(f"GRANT SELECT ON TABLE {table_name} TO `call_reviewers`")
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {table_name} TO `call_admins`")

print("✅ Assigned RBAC permissions via Unity Catalog")

# ------------------------------------------------------------
# STEP 3: Data Quality Validation (Delta Expectations)
# ------------------------------------------------------------
df_gold = spark.read.format("delta").load(gold_path)

# Expectation 1: sentiment is not null
null_sentiment_count = df_gold.filter(col("sentiment").isNull()).count()
if null_sentiment_count > 0:
    print(f"⚠️ Found {null_sentiment_count} records with NULL sentiment")

# Expectation 2: positive, negative, neutral sum <= 1.1 (tolerance)
invalid_confidence = df_gold.filter(
    (col("positive") + col("negative") + col("neutral")) > 1.1
).count()

if invalid_confidence > 0:
    print(f"⚠️ Found {invalid_confidence} records with invalid confidence scores")

# ------------------------------------------------------------
# STEP 4: Enforce Expectations via Delta Constraint
# ------------------------------------------------------------
# Example of declarative constraint enforcement
spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.{table_name}
ADD CONSTRAINT valid_sentiment CHECK (sentiment IS NOT NULL)
""")

print("✅ Added Delta table constraint: valid_sentiment")

# ------------------------------------------------------------
# STEP 5: Audit Table Metadata
# ------------------------------------------------------------
table_details = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{table_name}")
# display(table_details)

print("✅ Governance setup and quality checks complete.")
