# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Zerobus Streaming Producer
# MAGIC
# MAGIC Replays NASA CMAPSS data as a live stream using Databricks Zerobus.
# MAGIC Zerobus GA February 2026 — no Kafka cluster required.
# MAGIC
# MAGIC Modes:
# MAGIC - **Normal:** Replay all 100 engines at 10x speed (continuous background stream)
# MAGIC - **Demo:** Fast-forward Engine 15 final 50 cycles to guarantee anomaly within 30s
# MAGIC
# MAGIC Run this notebook in a Databricks Job (continuous) or interactively for demo.

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.24.0 -q

# COMMAND ----------

import json
import time
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster

# COMMAND ----------

# Configuration
ZEROBUS_TOPIC      = "livetwin_sensors"
REPLAY_SPEED       = 0.05          # seconds between messages; 0.05s = 20x faster
DEMO_SPEED         = 0.01          # 100x faster for demo mode
CMAPSS_PATH        = "/dbfs/FileStore/livetwin/cmapss_raw/fd001_engineered.csv"
DEMO_ENGINE_PATH   = "/dbfs/FileStore/livetwin/cmapss_raw/demo_engine_15.csv"
DEMO_FINAL_CYCLES  = 50            # replay last N cycles of demo engine at start

# COMMAND ----------

# MAGIC %md ## 1. Initialize Zerobus Client

# COMMAND ----------

w = WorkspaceClient()

# Create the Zerobus topic if it doesn't exist
try:
    w.zerobus.create_topic(
        name=ZEROBUS_TOPIC,
        retention_ms=86_400_000,    # 24 hours
        partitions=4,
        comment="LiveTwin sensor stream — NASA CMAPSS turbofan readings"
    )
    print(f"✓ Created Zerobus topic: {ZEROBUS_TOPIC}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Topic {ZEROBUS_TOPIC} already exists — using existing topic")
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 2. Message Builder

# COMMAND ----------

def build_sensor_message(row: pd.Series, timestamp_override=None) -> dict:
    """Convert a CMAPSS DataFrame row to a Zerobus message payload."""
    ts = timestamp_override or datetime.now(timezone.utc).isoformat()

    sensors = {
        f"s{i}": float(row.get(f"sensor_{i}", 0.0))
        for i in range(1, 22)
    }

    return {
        "asset_id":      int(row["unit_id"]),
        "timestamp":     ts,
        "cycle":         int(row["cycle"]),
        "sensors":       sensors,
        "op_settings": {
            "op1": float(row.get("op_setting_1", 0.0)),
            "op2": float(row.get("op_setting_2", 0.0)),
            "op3": float(row.get("op_setting_3", 100.0)),
        },
        "anomaly_score": float(row.get("anomaly_score", 0.0)),
        "anomaly_flag":  bool(row.get("anomaly_flag", False)),
        "dataset":       str(row.get("dataset", "FD001")),
        "source":        "cmapss_replay",
    }

# COMMAND ----------

# MAGIC %md ## 3. Normal Streaming Mode

# COMMAND ----------

def run_normal_stream(df: pd.DataFrame, speed: float = REPLAY_SPEED, max_messages: int = None):
    """
    Replay all engines continuously via Zerobus.
    Streams rows in order, cycling back to start when dataset exhausted.
    """
    print(f"Starting normal stream: {len(df):,} rows, speed={speed}s/msg")
    count = 0
    producer = w.zerobus.create_producer(topic=ZEROBUS_TOPIC)

    try:
        while True:
            for _, row in df.iterrows():
                msg = build_sensor_message(row)
                producer.produce(
                    key=str(msg["asset_id"]),
                    value=json.dumps(msg).encode("utf-8"),
                )
                count += 1

                if count % 500 == 0:
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] Produced {count:,} messages")

                if max_messages and count >= max_messages:
                    print(f"Stopping after {max_messages} messages.")
                    return

                time.sleep(speed)

    except KeyboardInterrupt:
        print(f"Stream stopped. Total messages: {count:,}")
    finally:
        producer.flush()
        producer.close()

# COMMAND ----------

# MAGIC %md ## 4. Demo Mode — Fast Engine 15 Failure

# COMMAND ----------

def run_demo_stream(full_df: pd.DataFrame, demo_engine_id: int = 15, speed: float = DEMO_SPEED):
    """
    Demo mode: First stream the final 50 cycles of Engine 15 at high speed
    to guarantee anomaly detection fires within 30 seconds, then switch to
    normal background stream for remaining engines.
    """
    print(f"=== DEMO MODE: Replaying Engine {demo_engine_id} failure first ===")

    producer = w.zerobus.create_producer(topic=ZEROBUS_TOPIC)

    # Phase 1: Stream Engine 15 final cycles (anomaly guaranteed)
    demo_engine = full_df[full_df["unit_id"] == demo_engine_id].copy()
    # Take last DEMO_FINAL_CYCLES cycles (most degraded)
    last_cycles = demo_engine.nlargest(DEMO_FINAL_CYCLES, "cycle")

    print(f"  Phase 1: Streaming Engine {demo_engine_id} last {DEMO_FINAL_CYCLES} cycles")
    print(f"  Anomaly cycles in this batch: {last_cycles[last_cycles['anomaly_flag']]['cycle'].tolist()}")

    for _, row in last_cycles.sort_values("cycle").iterrows():
        msg = build_sensor_message(row)
        producer.produce(
            key=str(msg["asset_id"]),
            value=json.dumps(msg).encode("utf-8"),
        )
        if msg["anomaly_flag"]:
            print(f"  🚨 ANOMALY sent: Engine {msg['asset_id']} cycle {msg['cycle']} score={msg['anomaly_score']:.3f}")
        time.sleep(speed)

    print(f"  Phase 1 complete — Engine {demo_engine_id} failure sequence streamed")

    # Phase 2: Background stream all other engines at normal speed
    print("  Phase 2: Starting background stream for all engines...")
    other_engines = full_df[full_df["unit_id"] != demo_engine_id]

    def background_stream():
        for _, row in other_engines.iterrows():
            msg = build_sensor_message(row)
            producer.produce(
                key=str(msg["asset_id"]),
                value=json.dumps(msg).encode("utf-8"),
            )
            time.sleep(REPLAY_SPEED)

    bg_thread = threading.Thread(target=background_stream, daemon=True)
    bg_thread.start()
    print("  Background stream started in separate thread.")

    producer.flush()
    return producer  # caller can call producer.close() when done

# COMMAND ----------

# MAGIC %md ## 5. Load Data and Run

# COMMAND ----------

# Load preprocessed CMAPSS dataset
try:
    df_cmapss = pd.read_csv(CMAPSS_PATH)
    print(f"Loaded {len(df_cmapss):,} rows from {CMAPSS_PATH}")
except FileNotFoundError:
    print("CSV not found on DBFS — reading from Unity Catalog instead")
    df_cmapss = spark.table("livetwin.raw.cmapss_fd001").toPandas()
    print(f"Loaded {len(df_cmapss):,} rows from Unity Catalog")

print(f"Engines: {sorted(df_cmapss['unit_id'].unique())}")

# COMMAND ----------

# Select run mode via widget
dbutils.widgets.dropdown("mode", "demo", ["demo", "normal", "test"])
mode = dbutils.widgets.get("mode")
print(f"Run mode: {mode}")

# COMMAND ----------

if mode == "test":
    # Quick test: produce 50 messages and stop
    print("TEST MODE: Producing 50 messages then stopping")
    run_normal_stream(df_cmapss, speed=0.01, max_messages=50)
    print("✓ Test complete")

elif mode == "demo":
    print("DEMO MODE: Engine 15 failure first, then background stream")
    producer = run_demo_stream(df_cmapss, demo_engine_id=15)
    print("✅ Demo stream running. Monitor Agent should detect anomaly within 30 seconds.")

elif mode == "normal":
    print("NORMAL MODE: Continuous stream, Ctrl+C to stop")
    run_normal_stream(df_cmapss, speed=REPLAY_SPEED)

# COMMAND ----------

# MAGIC %md ## 6. Verify Messages Arriving in Delta

# COMMAND ----------

# Zerobus writes directly to Delta — check the landing zone
import time
time.sleep(10)  # give Zerobus 10s to flush

try:
    df_check = spark.sql("""
        SELECT asset_id, COUNT(*) as msg_count, MAX(timestamp) as latest, AVG(anomaly_score) as avg_score
        FROM livetwin.raw.bronze_sensor_readings
        GROUP BY asset_id
        ORDER BY asset_id
        LIMIT 20
    """)
    display(df_check)
    print("✅ Messages arriving in Delta Bronze layer")
except Exception as e:
    print(f"Bronze table not yet created — run DLT pipeline first: {e}")
