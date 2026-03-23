# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Delta Live Tables Pipeline
# MAGIC
# MAGIC Declarative Bronze → Silver → Gold medallion pipeline.
# MAGIC Powered by Lakeflow Declarative Pipelines (DLT).
# MAGIC
# MAGIC **Deploy as a DLT Pipeline in the Databricks UI:**
# MAGIC - Pipeline name: `livetwin_sensor_pipeline`
# MAGIC - Target catalog: `livetwin`
# MAGIC - Mode: Continuous (for live streaming)
# MAGIC - Cluster: Single node with DBR 15+ (for Zerobus source)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

# ─── CONSTANTS ─────────────────────────────────────────────────────────────
ZEROBUS_TOPIC        = "livetwin_sensors"
ANOMALY_THRESHOLD    = 0.75      # z-score normalized score above which anomaly_flag = TRUE
ROLLING_WINDOW_ROWS  = 50        # per-engine rolling window for z-score calculations
IMPORTANT_SENSORS    = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]

# ──────────────────────────────────────────────────────────────────────────────
# BRONZE LAYER
# Raw, immutable, append-only. Schema enforced. No transformations.
# ──────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_sensor_readings",
    comment="Raw sensor readings from Zerobus. Immutable, append-only. Schema enforced.",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "pipelines.reset.allowed": "false",
    },
    partition_cols=["asset_id"],
)
def bronze_sensor_readings():
    """
    Ingest from Zerobus topic. Zerobus writes directly to Delta, eliminating
    the Kafka message bus layer. Sub-5-second latency from sensor to Bronze.
    """
    return (
        spark.readStream
            .format("zerobus")
            .option("topic", ZEROBUS_TOPIC)
            .option("startingOffsets", "latest")
            .load()
            .select(
                F.col("key").cast("string").alias("message_key"),
                F.from_json(
                    F.col("value").cast("string"),
                    schema=StructType([
                        StructField("asset_id",      IntegerType(),  False),
                        StructField("timestamp",     StringType(),   False),
                        StructField("cycle",         IntegerType(),  True),
                        StructField("sensors",       MapType(StringType(), DoubleType()), True),
                        StructField("op_settings",   MapType(StringType(), DoubleType()), True),
                        StructField("anomaly_score", DoubleType(),   True),
                        StructField("anomaly_flag",  BooleanType(),  True),
                        StructField("dataset",       StringType(),   True),
                        StructField("source",        StringType(),   True),
                    ])
                ).alias("data"),
                F.col("offset").alias("zerobus_offset"),
                F.col("timestamp").alias("ingestion_ts"),
            )
            .select(
                "data.*",
                "zerobus_offset",
                "ingestion_ts",
                F.to_timestamp("data.timestamp").alias("event_ts"),
            )
    )

# Dead-letter table: malformed or rejected records
@dlt.table(
    name="bronze_sensor_rejected",
    comment="Malformed sensor records that failed schema validation",
    table_properties={"quality": "bronze"},
)
@dlt.expect_all_or_drop({
    "valid_asset_id":  "asset_id IS NOT NULL AND asset_id > 0",
    "valid_timestamp": "timestamp IS NOT NULL",
})
def bronze_sensor_rejected():
    return dlt.read_stream("bronze_sensor_readings").filter(
        F.col("asset_id").isNull() | F.col("timestamp").isNull()
    )


# ──────────────────────────────────────────────────────────────────────────────
# SILVER LAYER
# Cleaned, validated, enriched with z-score anomaly detection.
# ──────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_sensor_readings",
    comment=(
        "Cleaned sensor readings with z-score anomaly detection and rolling statistics. "
        "Queried by Diagnosis Agent for 5-year historical health context."
    ),
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["asset_id"],
)
@dlt.expect_or_drop("valid_asset_id",  "asset_id IS NOT NULL AND asset_id > 0")
@dlt.expect_or_drop("valid_timestamp", "event_ts IS NOT NULL")
@dlt.expect_or_drop("valid_cycle",     "cycle IS NULL OR cycle > 0")
def silver_sensor_readings():
    """
    Transform Bronze → Silver:
    1. Explode JSONB sensors map into individual columns
    2. Compute per-engine rolling mean and std for the 9 key sensors
    3. Compute z-score anomaly score (normalized 0–1)
    4. Set anomaly_flag where score > ANOMALY_THRESHOLD
    5. Add data quality metadata
    """
    bronze = dlt.read_stream("bronze_sensor_readings")

    # Explode sensors map into individual float columns
    sensor_cols = [
        F.col("sensors")[f"s{i}"].cast("double").alias(f"s{i}")
        for i in range(1, 22)
    ]
    op_cols = [
        F.col("op_settings")["op1"].cast("double").alias("op_setting_1"),
        F.col("op_settings")["op2"].cast("double").alias("op_setting_2"),
        F.col("op_settings")["op3"].cast("double").alias("op_setting_3"),
    ]

    df = bronze.select(
        "asset_id",
        "event_ts",
        "cycle",
        "dataset",
        "source",
        "zerobus_offset",
        *sensor_cols,
        *op_cols,
    )

    # Z-score anomaly detection using rolling window per engine
    # Uses unbounded window (all history for this micro-batch) — sufficient for anomaly detection
    window_spec = (
        Window.partitionBy("asset_id")
              .orderBy("event_ts")
              .rowsBetween(-ROLLING_WINDOW_ROWS, 0)
    )

    for sensor in IMPORTANT_SENSORS:
        df = df.withColumn(f"{sensor}_roll_mean", F.avg(sensor).over(window_spec))
        df = df.withColumn(f"{sensor}_roll_std",  F.stddev(sensor).over(window_spec))
        df = df.withColumn(
            f"{sensor}_zscore",
            F.when(
                F.col(f"{sensor}_roll_std") > 0,
                F.abs((F.col(sensor) - F.col(f"{sensor}_roll_mean")) / F.col(f"{sensor}_roll_std"))
            ).otherwise(F.lit(0.0))
        )

    # Composite anomaly score: max z-score across all key sensors, normalized 0–1
    zscore_cols = [f"{s}_zscore" for s in IMPORTANT_SENSORS]
    df = df.withColumn(
        "max_zscore",
        F.greatest(*[F.col(c) for c in zscore_cols])
    )
    df = df.withColumn(
        "anomaly_score",
        F.least(F.col("max_zscore") / F.lit(5.0), F.lit(1.0))  # normalize: z=5 → score=1.0
    )
    df = df.withColumn(
        "anomaly_flag",
        F.col("anomaly_score") > ANOMALY_THRESHOLD
    )
    df = df.withColumn("processed_ts", F.current_timestamp())

    return df


# ──────────────────────────────────────────────────────────────────────────────
# GOLD LAYER
# Per-engine health KPIs aggregated for agents, Genie, and dashboards.
# ──────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_engine_health",
    comment=(
        "Per-engine health KPIs: health score, anomaly counts, sensor trends. "
        "Primary query target for Genie AI/BI and SQL Dashboard."
    ),
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def gold_engine_health():
    """
    Aggregate Silver → Gold:
    - health_score: composite 0–100 (weighted sensor deviations, inverted)
    - anomaly_count_1h: rolling anomaly count last 1 hour
    - anomaly_count_24h: rolling anomaly count last 24 hours
    - max_anomaly_score_1h: worst anomaly score in last hour
    - sensor trend deltas: rate of change for key sensors
    - total_cycles: max cycle seen for this engine
    - last_seen_ts: most recent reading timestamp
    """
    silver = dlt.read("silver_sensor_readings")

    now_expr = F.current_timestamp()

    # Aggregate per engine
    df = silver.groupBy("asset_id").agg(
        # Health score: 100 - (mean anomaly score * 100), clipped 0–100
        (F.lit(100) - F.least(
            F.avg("anomaly_score") * F.lit(100),
            F.lit(100)
        )).alias("health_score"),

        # Anomaly counts (approximated via conditional sum in batch)
        F.sum(
            F.when(
                F.col("event_ts") >= F.date_sub(now_expr, 1) & F.col("anomaly_flag"),
                1
            ).otherwise(0)
        ).alias("anomaly_count_24h"),

        # Sensor statistics
        F.max("anomaly_score").alias("max_anomaly_score_ever"),
        F.avg("anomaly_score").alias("avg_anomaly_score"),

        # Key sensor averages (last readings)
        *[F.avg(f"s{i}").alias(f"s{i}_avg") for i in range(1, 22)],

        # Operational metadata
        F.max("cycle").alias("total_cycles"),
        F.max("event_ts").alias("last_seen_ts"),
        F.min("event_ts").alias("first_seen_ts"),
        F.count("*").alias("total_readings"),
        F.countDistinct("dataset").alias("num_datasets"),
    )

    # Categorize health status
    df = df.withColumn(
        "health_status",
        F.when(F.col("health_score") >= 80, "healthy")
         .when(F.col("health_score") >= 50, "degrading")
         .when(F.col("health_score") >= 20, "critical")
         .otherwise("failure_imminent")
    )

    df = df.withColumn("updated_ts", F.current_timestamp())
    return df


# ──────────────────────────────────────────────────────────────────────────────
# GOLD LAYER — FLEET SUMMARY (for Genie and Dashboard)
# ──────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_fleet_summary",
    comment="Fleet-level summary: count by health status, top anomalous engines.",
    table_properties={"quality": "gold"},
)
def gold_fleet_summary():
    """Aggregate all engines into a single fleet health row. Genie-friendly."""
    gold = dlt.read("gold_engine_health")

    return gold.agg(
        F.count("*").alias("total_engines"),
        F.sum(F.when(F.col("health_status") == "healthy",           1).otherwise(0)).alias("healthy_count"),
        F.sum(F.when(F.col("health_status") == "degrading",         1).otherwise(0)).alias("degrading_count"),
        F.sum(F.when(F.col("health_status") == "critical",          1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("health_status") == "failure_imminent",  1).otherwise(0)).alias("failure_imminent_count"),
        F.avg("health_score").alias("fleet_avg_health_score"),
        F.min("health_score").alias("fleet_min_health_score"),
        F.sum("anomaly_count_24h").alias("fleet_anomaly_count_24h"),
        F.current_timestamp().alias("summary_ts"),
    )


# ──────────────────────────────────────────────────────────────────────────────
# LAKEBASE SYNC — Silver hot table → Lakebase (OLTP)
# Runs as a separate micro-batch job writing to Lakebase via JDBC
# Configure as a DLT Post-Pipeline Hook or separate Databricks Job
# ──────────────────────────────────────────────────────────────────────────────

# Note: The Lakebase sync is NOT declared as a DLT table (Lakebase is OLTP, not Delta).
# It runs via a Databricks Job that reads from silver_sensor_readings (streaming)
# and writes to Lakebase via psycopg2. See 05_agents/utils/lakebase_client.py.

# ──────────────────────────────────────────────────────────────────────────────
# LAKEHOUSE MONITORING (auto-configured at pipeline level)
# ──────────────────────────────────────────────────────────────────────────────

# Lakehouse Monitoring tracks:
# - Null rates per column per batch
# - Schema drift (new columns, type changes)
# - Statistical distribution drift (anomaly_score mean, variance)
# - Alert triggers for > 5% null rate on any sensor column

# Configure via Databricks UI: Pipeline Settings → Monitoring → Enable
# Or via REST API:
# POST /api/2.0/pipelines/{pipeline_id}/monitoring
# {
#   "enabled": true,
#   "alert_threshold": {"null_rate": 0.05, "drift_pvalue": 0.01}
# }
