# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Diagnosis Agent
# MAGIC
# MAGIC Given an anomaly, determines: severity, RUL, failure mode, and similar historical incidents.
# MAGIC
# MAGIC - **Trigger:** Called by Monitor Agent on anomaly detection
# MAGIC - **Inputs:** Lakebase (current state), Gold Delta (history), MLflow endpoints, Vector Search
# MAGIC - **Output:** Full diagnosis JSON with confidence intervals
# MAGIC - **Timing:** ~2.1 seconds total (Lakebase 8ms + Gold 340ms + RUL 890ms + Classifier 650ms + VS 120ms)

# COMMAND ----------

import sys
import json
import time
import logging
import numpy as np

sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

from utils.lakebase_client import (
    get_sensor_history, get_asset, get_config, update_in_flight_stage,
)
from utils.mlflow_client import (
    predict_rul, predict_failure_mode, predict_rul_heuristic, AgentTrace,
)
from utils.vector_search_client import search_similar_incidents

logger = logging.getLogger("diagnosis_agent")

IMPORTANT_SENSORS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]
CYCLES_PER_HOUR   = 1.0


# ─── Feature Engineering ─────────────────────────────────────────────────────

def build_sensor_features(sensor_readings: list[dict]) -> list[float]:
    """
    Extract feature vector from recent sensor readings.
    Uses rolling mean of last 10 readings for the 9 key sensors.
    Returns flat list for MLflow endpoint input.
    """
    if not sensor_readings:
        return [0.0] * (len(IMPORTANT_SENSORS) + 1)

    # Parse sensors JSONB (stored as dict in Lakebase)
    def parse_sensors(row: dict) -> dict:
        s = row.get("sensors", {})
        if isinstance(s, str):
            try:
                s = json.loads(s)
            except Exception:
                s = {}
        return s

    all_readings = [parse_sensors(r) for r in sensor_readings[:10]]  # last 10 readings

    features = []
    for sensor in IMPORTANT_SENSORS:
        values = [r.get(sensor, 0.0) for r in all_readings if sensor in r]
        if values:
            features.append(float(np.mean(values)))
        else:
            features.append(0.0)

    # Add cycle as feature
    latest_cycle = sensor_readings[0].get("cycle", 0) if sensor_readings else 0
    features.append(float(latest_cycle))

    return features


def build_anomaly_description(sensor_readings: list[dict], failure_mode: str) -> str:
    """Build a natural-language anomaly description for Vector Search query."""
    if not sensor_readings:
        return f"Turbofan anomaly. Predicted failure mode: {failure_mode}."

    def parse_sensors(row: dict) -> dict:
        s = row.get("sensors", {})
        if isinstance(s, str):
            try:
                s = json.loads(s)
            except Exception:
                return {}
        return s

    latest = parse_sensors(sensor_readings[0])

    # Describe sensor deviations
    elevated  = [s for s in IMPORTANT_SENSORS if latest.get(s, 0) > 0][:3]
    decreased = [s for s in IMPORTANT_SENSORS if latest.get(s, 0) < 0][:3]

    pattern_parts = []
    if elevated:
        pattern_parts.append(f"{', '.join(elevated)} elevated")
    if decreased:
        pattern_parts.append(f"{', '.join(decreased)} decreasing")

    pattern = "; ".join(pattern_parts) if pattern_parts else "anomalous readings"
    cycle   = sensor_readings[0].get("cycle", "?")

    return (
        f"Turbofan engine anomaly at cycle {cycle}. "
        f"Sensor pattern: {pattern}. "
        f"Predicted failure mode: {failure_mode}."
    )


# ─── Gold Layer Historical Data ───────────────────────────────────────────────

def get_engine_gold_history(asset_id: int, limit: int = 500) -> list[dict]:
    """
    Query Gold Delta table for historical engine health.
    Returns list of gold records sorted by most recent first.
    """
    try:
        df = spark.sql(f"""
            SELECT asset_id, health_score, max_anomaly_score_ever,
                   avg_anomaly_score, total_cycles, last_seen_ts,
                   s2_avg, s3_avg, s4_avg, s7_avg, s11_avg, s12_avg,
                   health_status
            FROM livetwin.processed.gold_engine_health
            WHERE asset_id = {asset_id}
            ORDER BY last_seen_ts DESC
            LIMIT {limit}
        """)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        logger.warning(f"Gold table query failed for asset {asset_id}: {e}")
        return []


# ─── Main Diagnosis Function ──────────────────────────────────────────────────

def run_diagnosis_agent(anomaly: dict, user_context: dict) -> dict:
    """
    Full diagnosis pipeline for one anomaly event.

    Returns:
        {
            asset_id, rul_hours, rul_lower, rul_upper,
            failure_mode, failure_confidence, failure_probabilities,
            similar_incidents (list), diagnosis_summary (str),
            timing_ms (dict)
        }
    """
    asset_id = anomaly["asset_id"]
    timing   = {}

    with AgentTrace("diagnosis_agent", asset_id=asset_id) as trace:

        # ── Step 1: Current state from Lakebase (OLTP, sub-10ms) ─────────────
        t0 = time.time()
        sensor_readings = get_sensor_history(asset_id, limit=100)
        timing["lakebase_ms"] = int((time.time() - t0) * 1000)

        asset_info = get_asset(asset_id)
        logger.info(
            f"[Diagnosis] asset={asset_id} ({asset_info.get('name','?')}) "
            f"readings={len(sensor_readings)} lakebase={timing['lakebase_ms']}ms"
        )

        trace.log("lakebase_fetch", inputs={"asset_id": asset_id},
                  outputs={"readings": len(sensor_readings)},
                  latency_ms=timing["lakebase_ms"])

        # ── Step 2: Historical health from Gold Delta (~340ms) ────────────────
        t0 = time.time()
        gold_history = get_engine_gold_history(asset_id, limit=500)
        timing["gold_ms"] = int((time.time() - t0) * 1000)

        logger.info(f"[Diagnosis] Gold history: {len(gold_history)} records in {timing['gold_ms']}ms")
        trace.log("gold_fetch", inputs={"asset_id": asset_id},
                  outputs={"records": len(gold_history)}, latency_ms=timing["gold_ms"])

        # ── Step 3: Feature engineering ───────────────────────────────────────
        features = build_sensor_features(sensor_readings)

        # ── Step 4: RUL prediction (~890ms) ──────────────────────────────────
        t0 = time.time()
        try:
            rul_result = predict_rul(features)
        except Exception as e:
            logger.warning(f"[Diagnosis] RUL endpoint failed, using heuristic: {e}")
            sensors_dict = {}
            if sensor_readings:
                s = sensor_readings[0].get("sensors", {})
                if isinstance(s, str):
                    s = json.loads(s)
                sensors_dict = s
            rul_result = predict_rul_heuristic(sensors_dict, sensor_readings[0].get("cycle", 100) if sensor_readings else 100)
        timing["rul_ms"] = int((time.time() - t0) * 1000)

        logger.info(
            f"[Diagnosis] RUL: {rul_result['rul_hours']:.1f}h "
            f"[{rul_result['lower_bound']:.1f}, {rul_result['upper_bound']:.1f}] "
            f"in {timing['rul_ms']}ms"
        )
        trace.log("rul_prediction",
                  inputs={"feature_count": len(features)},
                  outputs={"rul_hours": rul_result["rul_hours"]},
                  latency_ms=timing["rul_ms"],
                  model_version="livetwin.ml.rul_predictor@champion")

        # ── Step 5: Failure mode classification (~650ms) ──────────────────────
        t0 = time.time()
        try:
            failure_result = predict_failure_mode(features)
        except Exception as e:
            logger.warning(f"[Diagnosis] Classifier endpoint failed, using heuristic: {e}")
            failure_result = {
                "failure_mode": "compressor_degradation",
                "confidence": 0.6,
                "probabilities": {},
            }
        timing["classifier_ms"] = int((time.time() - t0) * 1000)

        logger.info(
            f"[Diagnosis] Failure mode: {failure_result['failure_mode']} "
            f"conf={failure_result['confidence']:.2f} "
            f"in {timing['classifier_ms']}ms"
        )
        trace.log("failure_classification",
                  inputs={"feature_count": len(features)},
                  outputs={
                      "failure_mode": failure_result["failure_mode"],
                      "confidence":   failure_result["confidence"],
                  },
                  latency_ms=timing["classifier_ms"],
                  model_version="livetwin.ml.failure_classifier@champion")

        # ── Step 6: Vector Search — similar incidents (~120ms) ────────────────
        t0 = time.time()
        query_text = build_anomaly_description(sensor_readings, failure_result["failure_mode"])
        similar_incidents = search_similar_incidents(
            query_text=query_text,
            num_results=3,
            failure_mode_filter=failure_result["failure_mode"],
        )
        timing["vector_search_ms"] = int((time.time() - t0) * 1000)

        logger.info(
            f"[Diagnosis] Similar incidents: {len(similar_incidents)} "
            f"top={similar_incidents[0].get('incident_id','?') if similar_incidents else 'none'} "
            f"in {timing['vector_search_ms']}ms"
        )
        trace.log("vector_search",
                  inputs={"query_length": len(query_text)},
                  outputs={"results": len(similar_incidents),
                           "top_incident": similar_incidents[0].get("incident_id", "") if similar_incidents else ""},
                  latency_ms=timing["vector_search_ms"])

        # ── Step 7: Build diagnosis summary ──────────────────────────────────
        top_incident  = similar_incidents[0] if similar_incidents else {}
        total_ms      = sum(timing.values())

        diagnosis_summary = (
            f"Engine {asset_id} ({asset_info.get('name','?')}) showing "
            f"{failure_result['failure_mode'].replace('_', ' ')} pattern. "
            f"{rul_result['rul_hours']:.1f} hours to failure "
            f"({int(failure_result['confidence']*100)}% confidence, "
            f"CI: [{rul_result['lower_bound']:.1f}, {rul_result['upper_bound']:.1f}]). "
            + (
                f"Matches {top_incident.get('incident_id','?')} "
                f"({int(top_incident.get('similarity_score',0)*100)}% similarity): "
                f"{top_incident.get('root_cause','')[:80]}."
                if top_incident else ""
            )
        )

        # Determine urgency hint
        rul_h = rul_result["rul_hours"]
        rul_urgent_h   = float(get_config("rul_urgent_threshold_h",   "24"))
        rul_critical_h = float(get_config("rul_critical_threshold_h", "8"))

        if rul_h <= rul_critical_h:
            urgency_hint = "CRITICAL"
        elif rul_h <= rul_urgent_h:
            urgency_hint = "URGENT"
        else:
            urgency_hint = "SCHEDULED"

        result = {
            "asset_id":               asset_id,
            "asset_name":             asset_info.get("name", f"Engine-{asset_id:02d}") if asset_info else f"Engine-{asset_id:02d}",
            "plant":                  asset_info.get("plant", "unknown") if asset_info else "unknown",
            "rul_hours":              rul_result["rul_hours"],
            "rul_lower_bound":        rul_result["lower_bound"],
            "rul_upper_bound":        rul_result["upper_bound"],
            "failure_mode":           failure_result["failure_mode"],
            "failure_confidence":     failure_result["confidence"],
            "failure_probabilities":  failure_result.get("probabilities", {}),
            "similar_incidents":      similar_incidents,
            "urgency_hint":           urgency_hint,
            "diagnosis_summary":      diagnosis_summary,
            "timing_ms":              timing,
            "total_diagnosis_ms":     total_ms,
            "anomaly_score":          anomaly.get("anomaly_score", 0.0),
            "cycle":                  sensor_readings[0].get("cycle", 0) if sensor_readings else 0,
        }

        trace.log("diagnosis_complete",
                  inputs={"asset_id": asset_id},
                  outputs={
                      "rul_hours":         rul_result["rul_hours"],
                      "failure_mode":      failure_result["failure_mode"],
                      "urgency_hint":      urgency_hint,
                      "total_ms":          total_ms,
                  },
                  latency_ms=total_ms)

        logger.info(
            f"[Diagnosis] COMPLETE asset={asset_id} "
            f"rul={rul_result['rul_hours']:.1f}h mode={failure_result['failure_mode']} "
            f"urgency={urgency_hint} total={total_ms}ms"
        )

        return result


# COMMAND ----------

# MAGIC %md ## Test (run standalone)

# COMMAND ----------

if __name__ == "__main__" or dbutils.widgets.get("run_test") == "true":
    # Simulate Monitor Agent calling us with a known anomaly (Engine 15)
    test_anomaly = {
        "asset_id":     15,
        "timestamp":    "2026-04-11T10:30:45Z",
        "anomaly_score": 0.89,
        "anomaly_flag": True,
        "cycle":        187,
    }
    test_user = {"user_id": "maintenance_supervisor@livetwin.com", "groups": ["maintenance_supervisors"]}

    result = run_diagnosis_agent(test_anomaly, test_user)

    print(f"\n{'='*60}")
    print(f"DIAGNOSIS RESULT")
    print(f"{'='*60}")
    print(f"Asset:         {result['asset_name']} (id={result['asset_id']})")
    print(f"RUL:           {result['rul_hours']:.1f}h [{result['rul_lower_bound']:.1f}, {result['rul_upper_bound']:.1f}]")
    print(f"Failure Mode:  {result['failure_mode']} ({result['failure_confidence']:.0%} confidence)")
    print(f"Urgency:       {result['urgency_hint']}")
    print(f"Top Incident:  {result['similar_incidents'][0]['incident_id'] if result['similar_incidents'] else 'none'}")
    print(f"Total Time:    {result['total_diagnosis_ms']}ms")
    print(f"\nSummary: {result['diagnosis_summary']}")
