# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Silver → Lakebase Sync Job
# MAGIC
# MAGIC Reads from Silver Delta table and writes to Lakebase `sensor_readings` (OLTP).
# MAGIC Run this as a Databricks Structured Streaming job (continuous micro-batch).
# MAGIC
# MAGIC Why this exists: DLT cannot write directly to Lakebase (different sink type).
# MAGIC This job bridges the gap — Silver Delta → Lakebase OLTP hot table.
# MAGIC Latency target: < 5 seconds from Silver to Lakebase.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk -q

# COMMAND ----------

import os
import json
import psycopg2
import psycopg2.extras
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

# Lakebase connection parameters — store in Databricks Secrets
# databricks secrets put --scope livetwin --key lakebase_host
# databricks secrets put --scope livetwin --key lakebase_password

LAKEBASE_HOST     = dbutils.secrets.get("livetwin", "lakebase_host")
LAKEBASE_PORT     = 5432
LAKEBASE_DB       = "livetwin_ops"
LAKEBASE_USER     = "livetwin_agent"
LAKEBASE_PASSWORD = dbutils.secrets.get("livetwin", "lakebase_password")

SILVER_TABLE      = "livetwin.processed.silver_sensor_readings"
CHECKPOINT_PATH   = "/dbfs/FileStore/livetwin/checkpoints/silver_to_lakebase"
BATCH_SIZE        = 100   # rows per Lakebase INSERT batch
TTL_HOURS         = 1     # keep only last N hours in sensor_readings

# COMMAND ----------

# MAGIC %md ## Lakebase Connection Pool

# COMMAND ----------

def get_lakebase_conn():
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=LAKEBASE_PASSWORD,
        connect_timeout=5,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
    )

def test_connection():
    conn = get_lakebase_conn()
    cur  = conn.cursor()
    cur.execute("SELECT version(), NOW()")
    row = cur.fetchone()
    conn.close()
    print(f"✓ Lakebase connection OK: {row[0][:40]} at {row[1]}")

test_connection()

# COMMAND ----------

# MAGIC %md ## Batch Write Function (called by foreachBatch)

# COMMAND ----------

SENSOR_WRITE_SQL = """
INSERT INTO sensor_readings
    (asset_id, timestamp, cycle, sensors, op_settings, anomaly_score, anomaly_flag)
VALUES %s
ON CONFLICT DO NOTHING
"""

def write_batch_to_lakebase(batch_df, batch_id):
    """
    foreachBatch sink: write each micro-batch to Lakebase via psycopg2.
    Also runs TTL cleanup to keep the hot table small.
    """
    rows = batch_df.collect()
    if not rows:
        return

    conn = get_lakebase_conn()
    cur  = conn.cursor()

    try:
        records = []
        for row in rows:
            # Build sensors JSONB dict
            sensors_dict = {f"s{i}": getattr(row, f"s{i}", None) for i in range(1, 22)}
            op_dict      = {
                "op1": row.op_setting_1,
                "op2": row.op_setting_2,
                "op3": row.op_setting_3,
            }
            records.append((
                row.asset_id,
                row.event_ts,
                row.cycle,
                json.dumps(sensors_dict),
                json.dumps(op_dict),
                float(row.anomaly_score or 0.0),
                bool(row.anomaly_flag or False),
            ))

        # Batch insert via execute_values (much faster than executemany)
        psycopg2.extras.execute_values(cur, SENSOR_WRITE_SQL, records, page_size=BATCH_SIZE)

        # TTL cleanup: delete rows older than TTL_HOURS
        cur.execute("""
            DELETE FROM sensor_readings
            WHERE timestamp < NOW() - INTERVAL '%s hours'
        """, (TTL_HOURS,))

        # Update asset last_reading_ts
        cur.execute("""
            UPDATE assets SET last_reading_ts = NOW()
            WHERE id = ANY(%s)
        """, ([r[0] for r in records],))

        conn.commit()
        print(f"  Batch {batch_id}: wrote {len(records)} rows to Lakebase")

    except Exception as e:
        conn.rollback()
        print(f"  ERROR in batch {batch_id}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

# COMMAND ----------

# MAGIC %md ## Start Streaming Job

# COMMAND ----------

silver_stream = (
    spark.readStream
        .format("delta")
        .table(SILVER_TABLE)
        .select(
            "asset_id", "event_ts", "cycle",
            *[f"s{i}" for i in range(1, 22)],
            "op_setting_1", "op_setting_2", "op_setting_3",
            "anomaly_score", "anomaly_flag",
        )
)

query: StreamingQuery = (
    silver_stream
        .writeStream
        .foreachBatch(write_batch_to_lakebase)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="2 seconds")   # 2-second micro-batch = ~3-5s Silver → Lakebase
        .start()
)

print(f"✓ Lakebase sync stream started. Query ID: {query.id}")
print("  Streaming Silver → Lakebase. Monitor in Spark UI.")
query.awaitTermination()
