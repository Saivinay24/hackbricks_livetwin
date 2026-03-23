"""
LiveTwin Dashboard API Server
FastAPI backend bridging the HTML dashboard to Lakebase (OLTP) and Delta tables.

Data sources:
  Lakebase (PostgreSQL, sub-10ms):
    assets, sensor_readings, maintenance_orders,
    agent_decisions, agent_inflight, config, operational_schedule
  Delta / Spark (DLT output):
    livetwin.processed.gold_engine_health  (health_score, health_status, anomaly counts)
    livetwin.processed.gold_fleet_summary  (aggregate KPIs)
    livetwin.processed.silver_sensor_readings (fallback sensors)

Run locally:
    pip install -r requirements.txt
    export LAKEBASE_HOST=<host> LAKEBASE_PASSWORD=<pw>
    python api_server.py

Run on Databricks:
    See run_dashboard.py notebook
"""

import os
import json
import time
import logging
import functools
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("livetwin.api")

# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="LiveTwin Operational Dashboard API",
    version="1.0.0",
    description="Turbofan fleet monitoring — Monitor·Diagnosis·Plan·Guardian agents on Databricks Lakehouse",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/", include_in_schema=False)
def serve_dashboard():
    path = os.path.join(_DASHBOARD_DIR, "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    raise HTTPException(404, "dashboard.html not found")


# ─────────────────────────────────────────────────────────────
# Lakebase connection pool (thread-safe for FastAPI workers)
# ─────────────────────────────────────────────────────────────

_pool = None

def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    try:
        from psycopg2 import pool as pgpool
        host     = os.environ.get("LAKEBASE_HOST", "")
        user     = os.environ.get("LAKEBASE_USER", "token")
        password = os.environ.get("LAKEBASE_PASSWORD", _get_secret("livetwin", "lakebase_password"))
        database = os.environ.get("LAKEBASE_DB", "livetwin")
        port     = int(os.environ.get("LAKEBASE_PORT", "5432"))
        _pool = pgpool.ThreadedConnectionPool(
            minconn=2, maxconn=10,
            host=host, port=port, user=user,
            password=password, database=database,
            connect_timeout=5, application_name="livetwin_dashboard",
        )
        log.info("Lakebase connection pool ready (min=2 max=10)")
    except Exception as e:
        log.warning(f"Lakebase pool init failed: {e}")
        _pool = None
    return _pool


def lakebase_query(sql: str, params: tuple = ()) -> List[Dict]:
    """Execute SELECT on Lakebase, return list of dicts. Never raises — returns []."""
    pool = _get_pool()
    if pool is None:
        return []
    conn = None
    try:
        conn = pool.getconn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        log.warning(f"Lakebase query error: {e}\nSQL: {sql[:200]}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return []
    finally:
        if conn and pool:
            try:
                pool.putconn(conn)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# Spark / Delta queries
# ─────────────────────────────────────────────────────────────

_spark = None

def _get_spark():
    global _spark
    if _spark is None:
        try:
            from pyspark.sql import SparkSession
            _spark = SparkSession.getActiveSession()
        except Exception:
            _spark = None
    return _spark


def delta_query(sql: str) -> List[Dict]:
    """Execute SQL via Spark Delta. Returns []  on any failure."""
    spark = _get_spark()
    if spark is None:
        return []
    try:
        df = spark.sql(sql)
        return [_jsonify(row.asDict()) for row in df.collect()]
    except Exception as e:
        log.warning(f"Delta query error: {e}\nSQL: {sql[:200]}")
        return []


# ─────────────────────────────────────────────────────────────
# Simple TTL cache for expensive Delta reads
# ─────────────────────────────────────────────────────────────

_cache: Dict[str, Tuple[float, Any]] = {}

def _cached(key: str, ttl_s: float, fn):
    now = time.monotonic()
    if key in _cache:
        ts, val = _cache[key]
        if now - ts < ttl_s:
            return val
    val = fn()
    _cache[key] = (now, val)
    return val


# ─────────────────────────────────────────────────────────────
# Databricks Secrets
# ─────────────────────────────────────────────────────────────

def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient().secrets.get_secret(scope=scope, key=key).value
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────
# Serialization helpers
# ─────────────────────────────────────────────────────────────

def _jsonify(obj):
    """Recursively make an object JSON-safe."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


def _parse_ts(val) -> datetime:
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def health_status_from_score(score: Optional[float]) -> str:
    if score is None:
        return "unknown"
    if score >= 80:
        return "healthy"
    if score >= 50:
        return "degrading"
    if score >= 20:
        return "critical"
    return "failure_imminent"


# ─────────────────────────────────────────────────────────────
# GET /health
# ─────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    lakebase_ok = len(lakebase_query("SELECT 1 AS ok")) > 0
    spark_ok    = _get_spark() is not None
    agents_in_flight = lakebase_query("SELECT asset_id, stage FROM agent_inflight")
    return {
        "status":          "ok" if lakebase_ok else "degraded",
        "lakebase":        lakebase_ok,
        "spark":           spark_ok,
        "agents_in_flight": len(agents_in_flight),
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# GET /api/fleet
# ─────────────────────────────────────────────────────────────

@app.get("/api/fleet")
def get_fleet():
    # ── 1. Assets from Lakebase ──
    asset_rows = lakebase_query(
        "SELECT id AS asset_id, name, plant, status, total_cycles, last_reading_ts FROM assets ORDER BY id"
    )

    # ── 2. Health scores from Delta gold_engine_health (cached 10s) ──
    health_rows = _cached("gold_fleet", 10.0, lambda: delta_query(
        "SELECT asset_id, health_score, health_status, anomaly_count_24h, "
        "max_anomaly_score_ever, last_seen_ts, updated_ts "
        "FROM livetwin.processed.gold_engine_health"
    ))
    health_map = {r["asset_id"]: r for r in health_rows}

    # ── 3. Latest RUL per engine from last approved maintenance order ──
    rul_rows = lakebase_query("""
        SELECT DISTINCT ON (asset_id)
               asset_id, rul_at_detection AS rul_hours,
               rul_lower_bound, rul_upper_bound,
               failure_mode, failure_confidence,
               created_at
        FROM   maintenance_orders
        WHERE  guardian_approved = TRUE
        ORDER  BY asset_id, created_at DESC
    """)
    rul_map = {r["asset_id"]: r for r in rul_rows}

    # ── 4. Currently in-flight asset IDs ──
    inflight_rows = lakebase_query("SELECT asset_id, stage FROM agent_inflight")
    inflight_map  = {r["asset_id"]: r["stage"] for r in inflight_rows}

    # ── 5. Pending orders count ──
    pending_rows = lakebase_query(
        "SELECT COUNT(*) AS cnt FROM maintenance_orders WHERE status = 'pending' AND guardian_approved = TRUE"
    )
    pending_count = pending_rows[0]["cnt"] if pending_rows else 0

    # ── 6. Merge all sources ──
    engines = []
    existing_ids = {a["asset_id"] for a in asset_rows}
    for i in range(1, 21):
        if i not in existing_ids:
            asset_rows.append({
                "asset_id": i, "name": f"Engine-{i:02d}",
                "plant": "A" if i <= 10 else "B", "status": "unknown",
            })
    asset_rows.sort(key=lambda r: r["asset_id"])

    for a in asset_rows:
        aid = a["asset_id"]
        h   = health_map.get(aid, {})
        r   = rul_map.get(aid, {})
        status = h.get("health_status") or health_status_from_score(h.get("health_score"))
        engines.append({
            "asset_id":         aid,
            "name":             a.get("name") or f"Engine-{aid:02d}",
            "plant":            a.get("plant") or ("A" if aid <= 10 else "B"),
            "health_score":     h.get("health_score"),
            "health_status":    status,
            "anomaly_count_24h": h.get("anomaly_count_24h", 0),
            "rul_hours":        r.get("rul_hours"),
            "failure_mode":     r.get("failure_mode"),
            "in_flight_stage":  inflight_map.get(aid),
            "last_seen_ts":     _jsonify(h.get("last_seen_ts") or a.get("last_reading_ts")),
        })

    # ── 7. Fleet KPIs ──
    healthy_statuses = {"healthy", "degrading"}
    alert_statuses   = {"critical", "failure_imminent"}
    online  = sum(1 for e in engines if e["health_status"] in healthy_statuses)
    alerts  = sum(1 for e in engines if e["health_status"] in alert_statuses)
    scores  = [e["health_score"] for e in engines if e["health_score"] is not None]
    avg_h   = round(sum(scores) / len(scores), 1) if scores else 0.0

    return {
        "engines": engines,
        "kpis": {
            "engines_online":    online,
            "active_alerts":     alerts,
            "pending_orders":    pending_count,
            "avg_health":        avg_h,
            "agents_in_flight":  len(inflight_rows),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# GET /api/engine/{engine_id}
# ─────────────────────────────────────────────────────────────

@app.get("/api/engine/{engine_id}")
def get_engine(engine_id: int):
    if not 1 <= engine_id <= 20:
        raise HTTPException(400, f"engine_id must be 1–20, got {engine_id}")

    # ── Asset metadata ──
    asset_rows = lakebase_query(
        "SELECT id AS asset_id, name, plant, status, total_cycles, last_reading_ts "
        "FROM assets WHERE id = %s", (engine_id,)
    )
    asset = asset_rows[0] if asset_rows else {
        "asset_id": engine_id,
        "plant": "A" if engine_id <= 10 else "B",
    }

    # ── Health from Delta (cached 10s per engine) ──
    health_rows = _cached(f"gold_{engine_id}", 10.0, lambda: delta_query(
        f"SELECT asset_id, health_score, health_status, anomaly_count_24h, "
        f"max_anomaly_score_ever, avg_anomaly_score, total_cycles, "
        f"last_seen_ts, updated_ts "
        f"FROM livetwin.processed.gold_engine_health WHERE asset_id = {engine_id}"
    ))
    health = health_rows[0] if health_rows else {}

    # ── Latest maintenance order (for RUL + ML predictions) ──
    order_rows = lakebase_query("""
        SELECT rul_at_detection, rul_lower_bound, rul_upper_bound,
               failure_mode, failure_confidence,
               urgency, estimated_cost, maintenance_action,
               similar_incident_id, similar_incident_similarity,
               recommended_window_start, recommended_window_end,
               guardian_checks, branch_id, approved_by_user, created_at
        FROM   maintenance_orders
        WHERE  asset_id = %s AND guardian_approved = TRUE
        ORDER  BY created_at DESC LIMIT 1
    """, (engine_id,))
    latest_order = order_rows[0] if order_rows else {}

    # ── Sensor readings from Lakebase (last 50 cycles, sub-10ms) ──
    sensor_rows = lakebase_query("""
        SELECT cycle, sensors, op_settings, anomaly_score, anomaly_flag, timestamp
        FROM   sensor_readings
        WHERE  asset_id = %s
        ORDER  BY cycle DESC
        LIMIT  50
    """, (engine_id,))
    if not sensor_rows:
        # Fallback to Delta Silver if Lakebase not yet synced
        sensor_rows = delta_query(
            f"SELECT cycle, sensors, anomaly_score, anomaly_flag, event_ts AS timestamp "
            f"FROM livetwin.processed.silver_sensor_readings "
            f"WHERE asset_id = {engine_id} ORDER BY cycle DESC LIMIT 50"
        )

    # Parse sensors JSONB → dict if stored as string
    for r in sensor_rows:
        if isinstance(r.get("sensors"), str):
            try:
                r["sensors"] = json.loads(r["sensors"])
            except Exception:
                r["sensors"] = {}
        if isinstance(r.get("op_settings"), str):
            try:
                r["op_settings"] = json.loads(r["op_settings"])
            except Exception:
                r["op_settings"] = {}

    # ── Health history for sparkline: rolling anomaly-based health score ──
    health_history = _compute_health_history(sensor_rows, health.get("health_score"))

    # ── ML predictions ──
    predictions = {
        "rul_hours":          latest_order.get("rul_at_detection"),
        "rul_lower":          latest_order.get("rul_lower_bound"),
        "rul_upper":          latest_order.get("rul_upper_bound"),
        "failure_mode":       latest_order.get("failure_mode"),
        "failure_confidence": latest_order.get("failure_confidence"),
        "urgency":            latest_order.get("urgency"),
    }

    # ── Similar incidents ──
    similar_incidents = _resolve_similar_incidents(
        engine_id,
        latest_order.get("similar_incident_id"),
        latest_order.get("similar_incident_similarity"),
    )

    engine_out = {
        "asset_id":          asset.get("asset_id", engine_id),
        "name":              asset.get("name") or f"Engine-{engine_id:02d}",
        "plant":             asset.get("plant") or ("A" if engine_id <= 10 else "B"),
        "health_score":      health.get("health_score"),
        "health_status":     health.get("health_status") or health_status_from_score(health.get("health_score")),
        "anomaly_count_24h": health.get("anomaly_count_24h", 0),
        "current_cycle":     asset.get("total_cycles") or (sensor_rows[0]["cycle"] if sensor_rows else None),
        "last_seen":         _jsonify(health.get("last_seen_ts") or asset.get("last_reading_ts")),
    }

    return {
        "engine":            _jsonify(engine_out),
        "sensor_readings":   [_jsonify(r) for r in sensor_rows],
        "health_history":    health_history,
        "predictions":       _jsonify(predictions),
        "similar_incidents": similar_incidents,
        "latest_order":      _jsonify(latest_order),
    }


def _compute_health_history(sensor_rows: List[Dict], current_health: Optional[float]) -> List[float]:
    """
    Derive health trend from Silver sensor readings (desc by cycle → reverse to chronological).
    Uses rolling 10-cycle anomaly rate: health = (1 - anomaly_rate) × 100.
    Falls back to [current_health] if no sensor data.
    """
    if not sensor_rows:
        return [current_health] if current_health is not None else []
    ordered = list(reversed(sensor_rows))  # oldest → newest
    window  = 10
    history = []
    for i in range(len(ordered)):
        sl = ordered[max(0, i - window + 1): i + 1]
        anomaly_cnt = sum(1 for r in sl if r.get("anomaly_flag"))
        history.append(round((1 - anomaly_cnt / len(sl)) * 100, 1))
    return history


def _resolve_similar_incidents(
    engine_id: int,
    incident_id: Optional[str],
    similarity: Optional[float],
) -> List[Dict]:
    """
    Return similar incidents for display. Primary: last diagnosis agent output_action.
    Fallback: incident_id/similarity from maintenance_orders.
    """
    # Try to parse similar_incidents from the last diagnosis decision output_action
    diag_rows = lakebase_query("""
        SELECT output_action FROM agent_decisions
        WHERE  asset_id = %s AND agent_name = 'diagnosis_agent'
        ORDER  BY timestamp DESC LIMIT 1
    """, (engine_id,))
    if diag_rows and diag_rows[0].get("output_action"):
        try:
            text = diag_rows[0]["output_action"]
            # The output_action is plain text; similar_incidents stored via plan agent
            # Try: look for plan agent's JSON-embedded incidents
            pass
        except Exception:
            pass

    # Fallback: show the one stored in the maintenance order
    if incident_id:
        return [{
            "incident_id":       incident_id,
            "failure_mode":      None,
            "similarity_score":  similarity,
            "cost":              None,
            "resolution":        "See incident database",
        }]
    return []


# ─────────────────────────────────────────────────────────────
# GET /api/agents/status
# ─────────────────────────────────────────────────────────────

@app.get("/api/agents/status")
def get_agents_status():
    # ── Latest decision per agent from agent_decisions ──
    last_decisions = lakebase_query("""
        SELECT DISTINCT ON (agent_name)
               agent_name, asset_id, timestamp, output_action,
               guardian_verdict, guardian_reason, latency_ms,
               triggered_by_user, branch_id
        FROM   agent_decisions
        ORDER  BY agent_name, timestamp DESC
    """)
    last_by_agent = {r["agent_name"]: r for r in last_decisions}

    # ── Currently in-flight (which engine is being processed right now) ──
    inflight_rows = lakebase_query("SELECT asset_id, stage, started_at FROM agent_inflight")
    inflight_by_stage = {r["stage"]: r for r in inflight_rows}

    AGENTS = [
        ("monitor_agent",   "monitor"),
        ("diagnosis_agent", "diagnosis"),
        ("plan_agent",      "plan"),
        ("guardian_agent",  "guardian"),
    ]

    agents = []
    for agent_name, stage in AGENTS:
        last  = last_by_agent.get(agent_name, {})
        inf   = inflight_by_stage.get(stage)

        # Status: in-flight beats last-known
        if inf:
            status = "running"
            asset_id = inf["asset_id"]
        else:
            verdict = last.get("guardian_verdict")
            status  = (
                "blocked"  if verdict == "BLOCKED"  else
                "complete" if verdict == "APPROVED" else
                "complete" if last else
                "idle"
            )
            asset_id = last.get("asset_id")

        # Build last_output summary
        if last:
            if last.get("guardian_verdict"):
                last_output = f"{last['guardian_verdict']}: {(last.get('guardian_reason') or '')[:100]}"
            else:
                last_output = (last.get("output_action") or "—")[:120]
        else:
            last_output = "—"

        agents.append({
            "agent_name":  agent_name,
            "status":      status,
            "last_output": last_output,
            "last_run":    _jsonify(last.get("timestamp")),
            "asset_id":    asset_id,
            "latency_ms":  last.get("latency_ms"),
            "branch_id":   last.get("branch_id") if agent_name == "guardian_agent" else None,
        })

    # ── Last 20 decisions for the decision log ──
    decisions = lakebase_query("""
        SELECT id, agent_name, asset_id, timestamp,
               output_action, guardian_verdict, latency_ms, triggered_by_user
        FROM   agent_decisions
        ORDER  BY timestamp DESC LIMIT 20
    """)

    return {
        "agents":    [_jsonify(a) for a in agents],
        "decisions": [_jsonify(d) for d in decisions],
        "inflight":  [_jsonify(r) for r in inflight_rows],
    }


# ─────────────────────────────────────────────────────────────
# GET /api/orders
# ─────────────────────────────────────────────────────────────

@app.get("/api/orders")
def get_orders():
    rows = lakebase_query("""
        SELECT id, asset_id, failure_mode, urgency, status,
               estimated_cost,
               recommended_window_start  AS window_start,
               recommended_window_end    AS window_end,
               rul_at_detection,
               rul_lower_bound, rul_upper_bound,
               failure_confidence,
               maintenance_action,
               guardian_approved, guardian_reason,
               guardian_checks,
               approved_by_user,
               similar_incident_id, similar_incident_similarity,
               branch_id, created_at, updated_at
        FROM   maintenance_orders
        ORDER  BY created_at DESC
        LIMIT  200
    """)
    if not rows:
        rows = delta_query("""
            SELECT id, asset_id, failure_mode, urgency, status,
                   estimated_cost,
                   recommended_window_start AS window_start,
                   recommended_window_end AS window_end,
                   approved_by_user, maintenance_action, created_at
            FROM livetwin.operational.maintenance_orders
            ORDER BY created_at DESC LIMIT 200
        """)

    # Parse guardian_checks JSONB if stored as string
    for r in rows:
        if isinstance(r.get("guardian_checks"), str):
            try:
                r["guardian_checks"] = json.loads(r["guardian_checks"])
            except Exception:
                r["guardian_checks"] = []

    # ── Summary stats ──
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    this_month  = [r for r in rows if _parse_ts(r.get("created_at")) >= month_start]
    total_cost  = sum(r.get("estimated_cost") or 0 for r in this_month)
    savings     = total_cost * 2  # unplanned failure ≈ 3× planned; net savings = 2×

    # Urgency breakdown
    critical_count = sum(1 for r in rows if r.get("urgency") == "CRITICAL" and r.get("status") == "pending")
    urgent_count   = sum(1 for r in rows if r.get("urgency") == "URGENT"   and r.get("status") == "pending")

    return {
        "orders": [_jsonify(r) for r in rows],
        "summary": {
            "total_this_month":    len(this_month),
            "total_cost":          total_cost,
            "estimated_savings":   savings,
            "critical_pending":    critical_count,
            "urgent_pending":      urgent_count,
        },
    }


# ─────────────────────────────────────────────────────────────
# GET /api/governance
# ─────────────────────────────────────────────────────────────

@app.get("/api/governance")
def get_governance(request: Request):
    # ── Config (cost limits) from Lakebase ──
    config_rows = lakebase_query("SELECT key, value FROM config")
    config = {r["key"]: r["value"] for r in config_rows}

    cost_limits = {
        "operator":   int(config.get("cost_limit_operator",   10000)),
        "supervisor": int(config.get("cost_limit_supervisor", 50000)),
        "admin":      None,
    }

    # ── OBO context: read from request header, fall back to demo default ──
    obo_user   = request.headers.get("X-OBO-User",   config.get("demo_user", "plant_a_supervisor@livetwin.com"))
    obo_groups_raw = request.headers.get("X-OBO-Groups", "plant_a_operators,maintenance_supervisors")
    obo_groups = [g.strip() for g in obo_groups_raw.split(",")]

    plant = "A" if any("plant_a" in g for g in obo_groups) else \
            "B" if any("plant_b" in g for g in obo_groups) else "ALL"
    role  = ("admin" if any("admin" in g for g in obo_groups) else
             "supervisor" if any("supervisor" in g for g in obo_groups) else
             "operator")

    obo_context = {
        "user_id": obo_user,
        "groups":  obo_groups,
        "plant":   plant,
        "role":    role,
    }
    visible_engines = (
        list(range(1, 11))  if plant == "A"   else
        list(range(11, 21)) if plant == "B"   else
        list(range(1, 21))
    )

    # ── Guardian branch log from agent_decisions (branch_id is a direct column) ──
    branch_rows = lakebase_query("""
        SELECT id, asset_id, guardian_verdict, guardian_reason,
               branch_id, latency_ms, timestamp, triggered_by_user
        FROM   agent_decisions
        WHERE  agent_name = 'guardian_agent'
          AND  branch_id IS NOT NULL
        ORDER  BY timestamp DESC LIMIT 20
    """)
    branch_log = [
        {
            "branch_id":  r["branch_id"],
            "asset_id":   r["asset_id"],
            "action":     "CREATE → TEST → " + ("MERGE" if r["guardian_verdict"] == "APPROVED" else "DROP"),
            "outcome":    "MERGED"      if r["guardian_verdict"] == "APPROVED" else
                          "DROPPED"     if r["guardian_verdict"] == "BLOCKED"  else
                          "IN_PROGRESS",
            "duration_ms": r.get("latency_ms"),
            "created_at":  _jsonify(r.get("timestamp")),
            "triggered_by": r.get("triggered_by_user"),
        }
        for r in branch_rows
    ]

    # ── Guardian decision audit (last 10) ──
    guard_rows = lakebase_query("""
        SELECT ad.id, ad.asset_id, ad.guardian_verdict, ad.guardian_reason,
               ad.branch_id, ad.latency_ms, ad.timestamp, ad.triggered_by_user,
               mo.estimated_cost
        FROM   agent_decisions ad
        LEFT JOIN maintenance_orders mo ON mo.agent_decision_id = ad.id
        WHERE  ad.agent_name = 'guardian_agent'
        ORDER  BY ad.timestamp DESC LIMIT 10
    """)
    guardian_decisions = [
        {
            "asset_id":       r["asset_id"],
            "verdict":        r["guardian_verdict"],
            "reason":         r.get("guardian_reason", "")[:100],
            "estimated_cost": r.get("estimated_cost"),
            "triggered_by":   r.get("triggered_by_user"),
            "created_at":     _jsonify(r.get("timestamp")),
        }
        for r in guard_rows
    ]

    # ── Fleet summary from Delta (cached 30s) ──
    fleet_summary = _cached("fleet_summary", 30.0, lambda: delta_query(
        "SELECT * FROM livetwin.processed.gold_fleet_summary LIMIT 1"
    ))
    fs = fleet_summary[0] if fleet_summary else {}

    return {
        "obo_context":        obo_context,
        "visible_engines":    visible_engines,
        "branch_log":         branch_log,
        "guardian_decisions": guardian_decisions,
        "cost_limits":        cost_limits,
        "fleet_summary":      _jsonify(fs),
    }


# ─────────────────────────────────────────────────────────────
# POST /api/demo/trigger
# ─────────────────────────────────────────────────────────────

@app.post("/api/demo/trigger")
def trigger_demo():
    """
    Starts the Engine 15 demo chain asynchronously.
    Returns 202 immediately; poll /api/agents/status to track progress.
    """
    import threading

    def _run():
        try:
            import sys
            agents_path = os.path.join(os.path.dirname(__file__), "..", "05_agents")
            if os.path.abspath(agents_path) not in sys.path:
                sys.path.insert(0, os.path.abspath(agents_path))
            from agent_orchestrator import run_full_chain
            result = run_full_chain(asset_id=15)
            log.info(f"Demo chain done for engine 15: verdict={result.get('guardian', {}).get('verdict')}")
        except Exception as e:
            log.error(f"Demo chain failed: {e}", exc_info=True)

    t = threading.Thread(target=_run, daemon=True, name="demo-chain-15")
    t.start()

    return JSONResponse(
        status_code=202,
        content={
            "status":    "accepted",
            "asset_id":  15,
            "message":   "Engine 15 demo chain started. Poll /api/agents/status for live progress.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )


# ─────────────────────────────────────────────────────────────
# POST /api/stream/control  — start/stop Zerobus demo stream
# ─────────────────────────────────────────────────────────────

@app.post("/api/stream/control")
def control_stream(request: Request):
    """
    Signal the Zerobus producer to switch between demo and normal mode.
    Updates the 'stream_mode' config key which producer.py reads on each batch.
    """
    import asyncio

    async def _read_body():
        body = await request.body()
        return json.loads(body) if body else {}

    try:
        import asyncio
        body = asyncio.get_event_loop().run_until_complete(_read_body())
    except Exception:
        body = {}

    mode = body.get("mode", "demo")
    if mode not in ("demo", "normal", "stop"):
        raise HTTPException(400, "mode must be demo | normal | stop")

    pool = _get_pool()
    if pool:
        conn = None
        try:
            conn = pool.getconn()
            conn.autocommit = True
            conn.cursor().execute(
                "UPDATE config SET value = %s, updated_at = NOW() WHERE key = 'stream_mode'",
                (mode,)
            )
        except Exception as e:
            log.warning(f"stream control update failed: {e}")
        finally:
            if conn and pool:
                pool.putconn(conn)

    return {"status": "ok", "mode": mode, "timestamp": datetime.now(timezone.utc).isoformat()}


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    log.info("=" * 60)
    log.info("  LiveTwin Operational Dashboard API")
    log.info(f"  Dashboard : http://localhost:{port}")
    log.info(f"  API docs  : http://localhost:{port}/docs")
    log.info(f"  Lakebase  : {os.environ.get('LAKEBASE_HOST', '(not set)')}")
    log.info("=" * 60)
    uvicorn.run("api_server:app", host="0.0.0.0", port=port, reload=False, workers=1)
