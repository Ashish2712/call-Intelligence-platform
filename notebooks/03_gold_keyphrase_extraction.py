# Databricks notebook source
# ============================================================
# 03_gold_keyphrase_extraction.py
# ============================================================
# Purpose:
#   Apply NLP enrichment using Azure Cognitive Services
#   to extract key phrases and sentiment from transcripts.
#
# Layers:
#   - Gold: Analytical insights (NLP features)
# ============================================================

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, explode
from pyspark.sql.types import ArrayType, StringType, FloatType, StructType, StructField

spark = SparkSession.builder.appName("GoldLayerNLP").getOrCreate()

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
silver_path = "/mnt/silver/enriched_calls/"
gold_path = "/mnt/gold/nlp_insights/"

# Secrets can be read from Key Vault or JSON for simplicity
config = {
    "azure_text_analytics_endpoint": "https://<your-region>.api.cognitive.microsoft.com/",
    "azure_text_analytics_key": "<your-api-key>"
}

endpoint = config["azure_text_analytics_endpoint"]
subscription_key = config["azure_text_analytics_key"]

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
df_silver = spark.read.format("delta").load(silver_path)
df_silver = df_silver.select("call_id", "caller_id", "transcript", "customer_name", "region", "product")

# ------------------------------------------------------------
# DEFINE NLP FUNCTIONS (Key Phrases & Sentiment)
# ------------------------------------------------------------
def extract_key_phrases(text):
    if not text:
        return []
    url = endpoint + "text/analytics/v3.1/keyPhrases"
    headers = {"Ocp-Apim-Subscription-Key": subscription_key}
    body = {"documents": [{"id": "1", "language": "en", "text": text}]}
    try:
        response = requests.post(url, headers=headers, json=body)
        result = response.json()
        return result["documents"][0].get("keyPhrases", [])
    except Exception as e:
        return []

def extract_sentiment(text):
    if not text:
        return None
    url = endpoint + "text/analytics/v3.1/sentiment"
    headers = {"Ocp-Apim-Subscription-Key": subscription_key}
    body = {"documents": [{"id": "1", "language": "en", "text": text}]}
    try:
        response = requests.post(url, headers=headers, json=body)
        result = response.json()
        sentiment = result["documents"][0]["sentiment"]
        confidence = result["documents"][0]["confidenceScores"]
        return (sentiment, confidence["positive"], confidence["negative"], confidence["neutral"])
    except Exception as e:
        return ("unknown", 0.0, 0.0, 0.0)

# ------------------------------------------------------------
# REGISTER UDFs
# ------------------------------------------------------------
extract_key_phrases_udf = udf(extract_key_phrases, ArrayType(StringType()))
extract_sentiment_udf = udf(extract_sentiment, StructType([
    StructField("sentiment", StringType()),
    StructField("positive", FloatType()),
    StructField("negative", FloatType()),
    StructField("neutral", FloatType())
]))

# ------------------------------------------------------------
# APPLY NLP ENRICHMENT
# ------------------------------------------------------------
df_gold = (
    df_silver
    .withColumn("key_phrases", extract_key_phrases_udf(col("transcript")))
    .withColumn("sentiment_struct", extract_sentiment_udf(col("transcript")))
    .withColumn("sentiment", col("sentiment_struct.sentiment"))
    .withColumn("positive", col("sentiment_struct.positive"))
    .withColumn("negative", col("sentiment_struct.negative"))
    .withColumn("neutral", col("sentiment_struct.neutral"))
    .drop("sentiment_struct")
)

# ------------------------------------------------------------
# WRITE TO GOLD DELTA TABLE
# ------------------------------------------------------------
df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

print("✅ Gold layer created successfully at:", gold_path)

# ------------------------------------------------------------
# OPTIONAL: Track metrics using MLflow
# ------------------------------------------------------------
import mlflow

mlflow.set_experiment("/Shared/Call_Intelligence_Tracking")

with mlflow.start_run(run_name="KeyPhraseExtraction"):
    mlflow.log_param("source", "Azure Cognitive Services")
    mlflow.log_metric("records_processed", df_gold.count())
    mlflow.log_artifact("/mnt/gold/nlp_insights/")

print("✅ MLflow tracking completed successfully.")
