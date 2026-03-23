# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Monitor Agent
# MAGIC
# MAGIC Continuously scans the fleet for anomalies and triggers the Diagnosis Agent.
# MAGIC
# MAGIC - **Trigger:** Continuous loop (every 100ms)
# MAGIC - **Input:** Lakebase `sensor_readings` (last 30 seconds)
# MAGIC - **Output:** Triggers `diagnosis_agent` for each new anomaly
# MAGIC - **Mode:** Real-Time Spark Mode (<300ms fleet scan p99)
# MAGIC - **Authorization:** Runs under OBO — inherits triggering user's permissions
# MAGIC
# MAGIC Run this notebook as a long-running Databricks Job.

# COMMAND ----------

import sys
import time
import json
import logging
import threading
import concurrent.futures
from datetime import datetime, timezone

# Add utils to path (when running in Databricks notebook)
sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

from utils.lakebase_client import (
    get_fleet_anomalies, get_config, mark_in_flight, is_in_flight,
    clear_in_flight, update_in_flight_stage,
)
from utils.mlflow_client import AgentTrace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("monitor_agent")

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

POLL_INTERVAL_MS   = int(get_config("monitor_poll_interval_ms", "100")) / 1000
ANOMALY_WINDOW_SEC = 30
MAX_PARALLEL_CHAINS= 5    # max concurrent diagnosis chains
DEMO_ENGINE_ID     = int(get_config("demo_failure_engine_id", "15"))

# Stats
_stats = {
    "scans":              0,
    "anomalies_detected": 0,
    "chains_triggered":   0,
    "chains_failed":      0,
}

# COMMAND ----------

# MAGIC %md ## Real-Time Spark Fleet Scan
# MAGIC
# MAGIC Uses Real-Time Spark Mode (<300ms p99) to scan all sensor readings in the last 30s.
# MAGIC Fleet-wide scan via a single SQL query — no per-engine polling.

# COMMAND ----------

def scan_fleet_realtime() -> list[dict]:
    """
    Fleet anomaly scan using Real-Time Spark Mode.
    Returns list of anomalous engine readings not already in-flight.

    Note: Real-Time Spark Mode is enabled at cluster level via:
        spark.conf.set("spark.databricks.realtime.enabled", "true")
    """
    try:
        # This query runs via Real-Time Spark Mode when cluster is configured
        df = spark.sql(f"""
            SELECT
                sr.asset_id,
                sr.timestamp,
                sr.anomaly_score,
                sr.anomaly_flag,
                sr.sensors,
                sr.cycle,
                a.name    AS asset_name,
                a.plant,
                a.location
            FROM livetwin.operational.sensor_readings sr
            JOIN livetwin.operational.assets a ON a.id = sr.asset_id
            WHERE sr.timestamp > NOW() - INTERVAL {ANOMALY_WINDOW_SEC} SECONDS
              AND sr.anomaly_flag = TRUE
            ORDER BY sr.anomaly_score DESC
            LIMIT 10
        """)
        rows = [row.asDict() for row in df.collect()]
        return rows
    except Exception as e:
        logger.warning(f"Spark fleet scan failed, falling back to Lakebase direct: {e}")
        # Fallback: direct Lakebase query (same result, slightly higher latency)
        return get_fleet_anomalies(window_seconds=ANOMALY_WINDOW_SEC)


# COMMAND ----------

# MAGIC %md ## Chain Trigger (async)

# COMMAND ----------

def trigger_diagnosis_chain(anomaly: dict, user_context: dict):
    """
    Trigger the full agent chain for a detected anomaly.
    Runs asynchronously — Monitor Agent doesn't wait for chain completion.

    Imports diagnosis_agent lazily to avoid circular imports.
    """
    asset_id = anomaly["asset_id"]

    try:
        update_in_flight_stage(asset_id, "diagnosis")
        logger.info(f"[Monitor] Chain triggered for asset_id={asset_id} score={anomaly['anomaly_score']:.3f}")

        # Lazy import to allow running each agent independently
        from diagnosis_agent import run_diagnosis_agent
        diagnosis = run_diagnosis_agent(anomaly, user_context)

        update_in_flight_stage(asset_id, "plan")
        from plan_agent import run_plan_agent
        plan = run_plan_agent(diagnosis, user_context)

        update_in_flight_stage(asset_id, "guardian")
        from guardian_agent import run_guardian_agent
        result = run_guardian_agent(plan, diagnosis, user_context)

        _stats["chains_triggered"] += 1
        logger.info(
            f"[Monitor] Chain complete for asset_id={asset_id}: "
            f"verdict={result.get('verdict')} order_id={result.get('committed_order_id')}"
        )

    except Exception as e:
        _stats["chains_failed"] += 1
        logger.error(f"[Monitor] Chain failed for asset_id={asset_id}: {e}", exc_info=True)
    finally:
        clear_in_flight(asset_id)


# COMMAND ----------

# MAGIC %md ## Main Monitor Loop

# COMMAND ----------

def run_monitor_agent(user_context: dict = None, max_scans: int = None):
    """
    Main monitoring loop. Runs indefinitely (or until max_scans for tests).

    Args:
        user_context: OBO user info — {'user_id': 'plant_a_operator@company.com', 'groups': [...]}
        max_scans: For testing — stop after N scans
    """
    if user_context is None:
        user_context = {"user_id": "system", "groups": ["maintenance_supervisors"]}

    logger.info(f"Monitor Agent started | user={user_context['user_id']} | poll={POLL_INTERVAL_MS*1000:.0f}ms")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_CHAINS)
    scan_count = 0

    with AgentTrace("monitor_agent") as trace:
        while True:
            scan_start = time.time()
            anomalies  = []

            try:
                anomalies = scan_fleet_realtime()
                _stats["scans"] += 1
                scan_count += 1

                new_anomalies = [
                    a for a in anomalies
                    if not is_in_flight(a["asset_id"])
                ]

                if new_anomalies:
                    _stats["anomalies_detected"] += len(new_anomalies)
                    for anomaly in new_anomalies:
                        if mark_in_flight(anomaly["asset_id"], stage="monitor"):
                            logger.info(
                                f"[Monitor] NEW ANOMALY: asset={anomaly['asset_id']} "
                                f"name={anomaly.get('asset_name','')} "
                                f"score={anomaly['anomaly_score']:.3f} "
                                f"cycle={anomaly.get('cycle', '?')}"
                            )
                            executor.submit(trigger_diagnosis_chain, anomaly, user_context)

            except Exception as e:
                logger.error(f"[Monitor] Scan error: {e}")

            scan_ms = int((time.time() - scan_start) * 1000)

            if scan_count % 50 == 0:
                logger.info(
                    f"[Monitor] Stats: scans={_stats['scans']} "
                    f"anomalies={_stats['anomalies_detected']} "
                    f"chains={_stats['chains_triggered']} "
                    f"last_scan={scan_ms}ms"
                )
                trace.log(
                    "fleet_scan_50",
                    inputs={"fleet_size": 20, "window_sec": ANOMALY_WINDOW_SEC},
                    outputs={**_stats, "last_scan_ms": scan_ms},
                    latency_ms=scan_ms,
                )

            if max_scans and scan_count >= max_scans:
                logger.info(f"[Monitor] Stopping after {max_scans} scans (test mode)")
                break

            # Sleep for remainder of poll interval
            elapsed = time.time() - scan_start
            sleep_time = max(0, POLL_INTERVAL_MS - elapsed)
            time.sleep(sleep_time)


# COMMAND ----------

# MAGIC %md ## OBO Authorization Context

# COMMAND ----------

def get_obo_user_context() -> dict:
    """
    Get the OBO user context from Databricks notebook context.
    When running under Databricks Agent Framework, OBO is automatic.
    When running as a job, use the job owner's permissions.
    """
    try:
        from databricks.sdk.runtime import dbutils
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        user_email = ctx.userName().get()
        return {
            "user_id":   user_email,
            "groups":    [],  # populated by Unity Catalog ABAC at query time
            "source":    "obo",
        }
    except Exception:
        return {"user_id": "system", "groups": ["maintenance_supervisors"], "source": "system"}


# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

if __name__ == "__main__" or True:  # Always run in notebook context
    dbutils.widgets.dropdown("mode", "run", ["run", "test"])
    mode = dbutils.widgets.get("mode")

    user_ctx = get_obo_user_context()
    logger.info(f"Starting Monitor Agent as: {user_ctx['user_id']}")

    if mode == "test":
        # Run 5 scan cycles and check for anomalies (non-blocking)
        run_monitor_agent(user_context=user_ctx, max_scans=5)
        print(f"Test complete. Stats: {_stats}")
    else:
        # Production: run indefinitely
        run_monitor_agent(user_context=user_ctx)
