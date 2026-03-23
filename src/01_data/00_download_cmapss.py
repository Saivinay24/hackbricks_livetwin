# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Download & Prepare NASA CMAPSS Dataset
# MAGIC
# MAGIC Downloads NASA CMAPSS Turbofan Engine Degradation Simulation dataset,
# MAGIC parses it into a clean Spark DataFrame, and uploads to Unity Catalog `livetwin.raw`.
# MAGIC
# MAGIC Dataset: https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/

# COMMAND ----------

# MAGIC %md ## 1. Install dependencies

# COMMAND ----------

# MAGIC %pip install requests tqdm -q

# COMMAND ----------

import os
import io
import requests
import zipfile
import numpy as np
import pandas as pd
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ## 2. Download CMAPSS

# COMMAND ----------

CMAPSS_URL = "https://ti.arc.nasa.gov/c/6/"   # Official NASA zip
DBFS_RAW   = "/dbfs/FileStore/livetwin/cmapss_raw/"
LOCAL_ZIP  = "/tmp/cmapss.zip"

os.makedirs(DBFS_RAW, exist_ok=True)

# The dataset is also mirrored on Kaggle and GitHub; use the stable GitHub mirror
MIRROR_URL = "https://github.com/mohomran/CMAPSS/archive/refs/heads/master.zip"

print("Downloading CMAPSS dataset...")
r = requests.get(MIRROR_URL, timeout=120)
r.raise_for_status()

with open(LOCAL_ZIP, "wb") as f:
    f.write(r.content)
print(f"Downloaded {len(r.content) / 1024:.1f} KB")

# COMMAND ----------

# Extract
with zipfile.ZipFile(LOCAL_ZIP, "r") as zf:
    print("Archive contents:", zf.namelist())
    zf.extractall("/tmp/cmapss_extracted/")
print("Extracted.")

# COMMAND ----------

# Locate the .txt files (the actual dataset)
import glob
txt_files = glob.glob("/tmp/cmapss_extracted/**/*.txt", recursive=True)
for f in sorted(txt_files):
    print(f)

# COMMAND ----------

# MAGIC %md ## 3. Parse CMAPSS Text Files

# COMMAND ----------

SENSOR_COLS = [f"sensor_{i}" for i in range(1, 22)]
OP_COLS     = ["op_setting_1", "op_setting_2", "op_setting_3"]
BASE_COLS   = ["unit_id", "cycle"] + OP_COLS + SENSOR_COLS

def load_cmapss(filepath: str, dataset_name: str) -> pd.DataFrame:
    """Load a CMAPSS train/test file into a pandas DataFrame."""
    df = pd.read_csv(
        filepath,
        sep=r"\s+",
        header=None,
        names=BASE_COLS,
        engine="python",
    )
    df.dropna(axis=1, how="all", inplace=True)
    df["dataset"] = dataset_name
    return df

def load_rul(filepath: str) -> pd.DataFrame:
    """Load a RUL ground-truth file."""
    df = pd.read_csv(filepath, sep=r"\s+", header=None, names=["true_rul"])
    df["unit_id"] = df.index + 1
    return df

# COMMAND ----------

# Load FD001 (primary demo dataset) and FD004 (scalability story)
datasets = {}
rul_data = {}

for subset in ["FD001", "FD002", "FD003", "FD004"]:
    train_file = glob.glob(f"/tmp/cmapss_extracted/**/train_{subset}.txt", recursive=True)
    test_file  = glob.glob(f"/tmp/cmapss_extracted/**/test_{subset}.txt",  recursive=True)
    rul_file   = glob.glob(f"/tmp/cmapss_extracted/**/RUL_{subset}.txt",   recursive=True)

    if train_file:
        df_train = load_cmapss(train_file[0], subset)

        # Add RUL labels: max_cycle - current_cycle for each engine
        max_cycles = df_train.groupby("unit_id")["cycle"].max().rename("max_cycle")
        df_train   = df_train.merge(max_cycles, on="unit_id")
        df_train["rul"] = df_train["max_cycle"] - df_train["cycle"]
        df_train.drop("max_cycle", axis=1, inplace=True)

        datasets[f"{subset}_train"] = df_train
        print(f"  {subset} train: {len(df_train):,} rows, {df_train['unit_id'].nunique()} engines")

    if test_file and rul_file:
        df_test  = load_cmapss(test_file[0], subset)
        df_rul   = load_rul(rul_file[0])
        rul_data[subset] = df_rul
        datasets[f"{subset}_test"] = df_test
        print(f"  {subset} test:  {len(df_test):,} rows, {df_test['unit_id'].nunique()} engines")

# COMMAND ----------

# MAGIC %md ## 4. Feature Engineering (used by ML models)

# COMMAND ----------

# Key sensors identified from literature
IMPORTANT_SENSORS = ["sensor_2", "sensor_3", "sensor_4", "sensor_7",
                     "sensor_11", "sensor_12", "sensor_15", "sensor_20", "sensor_21"]

def add_rolling_features(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Add rolling mean and std for the 9 most predictive sensors."""
    df = df.sort_values(["unit_id", "cycle"]).copy()
    for col in IMPORTANT_SENSORS:
        df[f"{col}_roll_mean"] = (
            df.groupby("unit_id")[col]
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )
        df[f"{col}_roll_std"] = (
            df.groupby("unit_id")[col]
            .transform(lambda x: x.rolling(window, min_periods=1).std().fillna(0))
        )
    return df

def add_zscore_anomaly(df: pd.DataFrame) -> pd.DataFrame:
    """Compute z-score anomaly flag across the 9 key sensors."""
    # Global normalization (in production, use per-engine rolling stats)
    for col in IMPORTANT_SENSORS:
        mean_val = df[col].mean()
        std_val  = df[col].std()
        if std_val > 0:
            df[f"{col}_zscore"] = (df[col] - mean_val) / std_val
        else:
            df[f"{col}_zscore"] = 0.0

    zscore_cols = [f"{c}_zscore" for c in IMPORTANT_SENSORS]
    df["max_zscore"]    = df[zscore_cols].abs().max(axis=1)
    df["anomaly_score"] = df["max_zscore"] / df["max_zscore"].max()  # normalize 0-1
    df["anomaly_flag"]  = df["anomaly_score"] > 0.75
    return df

# COMMAND ----------

df_fd001 = datasets["FD001_train"].copy()
df_fd001 = add_rolling_features(df_fd001)
df_fd001 = add_zscore_anomaly(df_fd001)

print(f"FD001 engineered: {len(df_fd001):,} rows, {df_fd001.columns.tolist()[:10]}")
print(f"Anomaly flag distribution: {df_fd001['anomaly_flag'].value_counts().to_dict()}")

# COMMAND ----------

# MAGIC %md ## 5. Identify Demo Failure Engine

# COMMAND ----------

# Engine 15 (unit_id=15) — pre-identified for reliable demo
demo_engine_id = 15
demo_engine    = df_fd001[df_fd001["unit_id"] == demo_engine_id].copy()

print(f"Demo engine (unit_id={demo_engine_id}):")
print(f"  Total cycles: {demo_engine['cycle'].max()}")
print(f"  Final RUL: {demo_engine['rul'].min()}")
print(f"  Anomaly cycles: {demo_engine[demo_engine['anomaly_flag']]['cycle'].tolist()[:10]}")
first_anomaly_cycle = demo_engine[demo_engine['anomaly_flag']]['cycle'].min()
print(f"  First anomaly at cycle: {first_anomaly_cycle}")

# COMMAND ----------

# MAGIC %md ## 6. Save to Delta (Unity Catalog)

# COMMAND ----------

# Convert FD001 to Spark and write to livetwin.raw.cmapss_fd001
df_spark = spark.createDataFrame(df_fd001)

(df_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("livetwin.raw.cmapss_fd001")
)
print("✓ Saved livetwin.raw.cmapss_fd001")

# COMMAND ----------

# Save FD004 for scalability story
if "FD004_train" in datasets:
    df_fd004 = datasets["FD004_train"].copy()
    df_fd004 = add_rolling_features(df_fd004)
    df_fd004 = add_zscore_anomaly(df_fd004)
    df_spark_fd004 = spark.createDataFrame(df_fd004)
    (df_spark_fd004
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("livetwin.raw.cmapss_fd004")
    )
    print("✓ Saved livetwin.raw.cmapss_fd004")

# COMMAND ----------

# Save preprocessed CSV to DBFS for Zerobus producer
df_fd001.to_csv(f"{DBFS_RAW}fd001_engineered.csv", index=False)
demo_engine.to_csv(f"{DBFS_RAW}demo_engine_15.csv", index=False)
print(f"✓ Saved CSVs to {DBFS_RAW}")

# COMMAND ----------

display(spark.sql("SELECT dataset, COUNT(*) as rows, COUNT(DISTINCT unit_id) as engines, AVG(anomaly_score) as avg_anomaly FROM livetwin.raw.cmapss_fd001 GROUP BY dataset"))
print("✅ CMAPSS dataset ready.")
