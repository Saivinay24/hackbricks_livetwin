# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Agent Orchestrator
# MAGIC
# MAGIC Entry point that wires all four agents together and provides:
# MAGIC - End-to-end chain test (Monitor → Diagnosis → Plan → Guardian)
# MAGIC - Demo mode: inject Engine 15 anomaly and watch full chain execute
# MAGIC - Status dashboard: show current agent chain state
# MAGIC
# MAGIC Run this notebook to start the full system.

# COMMAND ----------

import sys
import time
import json
import logging
from datetime import datetime, timezone
from IPython.display import display, HTML

sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s"
)
logger = logging.getLogger("orchestrator")

# COMMAND ----------

# MAGIC %md ## Mode Selection

# COMMAND ----------

dbutils.widgets.dropdown("mode", "demo", ["demo", "full_run", "test_chain", "status"])
mode = dbutils.widgets.get("mode")
print(f"Mode: {mode}")

# COMMAND ----------

# MAGIC %md ## OBO Context

# COMMAND ----------

def get_user_context() -> dict:
    """Get OBO user context from Databricks notebook."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        user_email = ctx.userName().get()
        logger.info(f"OBO user: {user_email}")
        return {
            "user_id": user_email,
            "groups":  [],  # Unity Catalog ABAC handles this transparently
            "source":  "obo",
        }
    except Exception:
        return {"user_id": "demo_supervisor@livetwin.com", "groups": ["maintenance_supervisors"], "source": "system"}

user_ctx = get_user_context()
print(f"Running as: {user_ctx['user_id']}")

# COMMAND ----------

# MAGIC %md ## Full Chain Test (single asset)

# COMMAND ----------

def run_full_chain(asset_id: int, anomaly_score: float = 0.89, cycle: int = 187) -> dict:
    """
    Run the full Monitor → Diagnosis → Plan → Guardian chain for one asset.
    Used for demo and integration testing.
    """
    from diagnosis_agent import run_diagnosis_agent
    from plan_agent      import run_plan_agent
    from guardian_agent  import run_guardian_agent
    from utils.lakebase_client import mark_in_flight, clear_in_flight

    chain_start = time.time()

    print(f"\n{'='*60}")
    print(f"LIVETWIN AGENT CHAIN — Asset {asset_id}")
    print(f"User: {user_ctx['user_id']}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # Simulate Monitor Agent detecting anomaly
    anomaly = {
        "asset_id":     asset_id,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "anomaly_score": anomaly_score,
        "anomaly_flag": True,
        "cycle":        cycle,
    }
    print(f"\n[t=0s] MONITOR: Anomaly detected on Engine {asset_id}")
    print(f"       Score={anomaly_score:.3f}, Cycle={cycle}")
    mark_in_flight(asset_id, stage="diagnosis")

    try:
        # Diagnosis
        t1 = time.time()
        print(f"\n[t=0s] DIAGNOSIS: Starting...")
        diagnosis = run_diagnosis_agent(anomaly, user_ctx)
        t1_done = time.time() - t1

        print(f"       RUL: {diagnosis['rul_hours']:.1f}h [{diagnosis['rul_lower_bound']:.1f}–{diagnosis['rul_upper_bound']:.1f}]")
        print(f"       Mode: {diagnosis['failure_mode']} ({diagnosis['failure_confidence']:.0%})")
        print(f"       Urgency: {diagnosis['urgency_hint']}")
        if diagnosis["similar_incidents"]:
            top = diagnosis["similar_incidents"][0]
            print(f"       Best match: {top.get('incident_id','?')} ({top.get('similarity_score',0):.0%})")
        print(f"       Time: {t1_done:.1f}s")

        # Plan
        t2 = time.time()
        print(f"\n[t={time.time()-chain_start:.1f}s] PLAN: Building maintenance plan...")
        plan = run_plan_agent(diagnosis, user_ctx)
        t2_done = time.time() - t2

        print(f"       Urgency: {plan['urgency']}")
        print(f"       Window: {plan['recommended_window']['start'][:19]}")
        print(f"       Cost: ₹{plan['estimated_cost']:,.0f}")
        print(f"       Action: {plan['maintenance_action'][:60]}...")
        print(f"       Time: {t2_done:.1f}s")

        # Guardian
        t3 = time.time()
        print(f"\n[t={time.time()-chain_start:.1f}s] GUARDIAN: Validating + branching DB...")
        result = run_guardian_agent(plan, diagnosis, user_ctx)
        t3_done = time.time() - t3

        print(f"\n[t={time.time()-chain_start:.1f}s] GUARDIAN VERDICT: {result['verdict']}")
        for check in result.get("checks", []):
            icon = "✓" if check["passed"] else "✗"
            print(f"       {icon} {check['check']}: {check['detail'][:60]}")

        if result["verdict"] == "APPROVED":
            print(f"\n  ✅ MAINTENANCE ORDER #{result.get('committed_order_id','?')} COMMITTED")
            print(f"     Branch: {result.get('branch_id','?')}")
            print(f"     Approved by: {result.get('approved_by','?')}")
        else:
            print(f"\n  ⛔ BLOCKED: {result.get('block_reasons','')}")
        print(f"       Time: {t3_done:.1f}s")

        total_s = time.time() - chain_start
        print(f"\n{'='*60}")
        print(f"CHAIN COMPLETE: {total_s:.1f}s end-to-end")
        print(f"(vs human process: 2-6 hours of inspection + paperwork)")
        print(f"{'='*60}")

        return {
            "diagnosis": diagnosis,
            "plan":      plan,
            "guardian":  result,
            "total_s":   total_s,
        }

    finally:
        clear_in_flight(asset_id)

# COMMAND ----------

# MAGIC %md ## Demo Mode (Engine 15 Failure)

# COMMAND ----------

if mode == "demo":
    print("🎬 DEMO MODE: Replaying Engine 15 failure sequence...")

    # First start the Zerobus stream (in background)
    import threading
    def start_stream():
        try:
            dbutils.notebook.run(
                "/Workspace/Repos/livetwin/src/01_data/01_zerobus_producer",
                timeout_seconds=300,
                arguments={"mode": "demo"}
            )
        except Exception as e:
            logger.warning(f"Stream notebook error (expected for demo): {e}")

    stream_thread = threading.Thread(target=start_stream, daemon=True)
    stream_thread.start()
    print("  Zerobus stream started (background)...")
    time.sleep(5)

    # Run full chain for Engine 15
    print("\n  Engine 15 anomaly injected — running agent chain...\n")
    chain_result = run_full_chain(asset_id=15, anomaly_score=0.89, cycle=187)

    print("\n📊 Live Query — Maintenance Orders:")
    display(spark.sql("""
        SELECT id, asset_id, failure_mode, rul_at_detection, urgency,
               estimated_cost, guardian_approved, approved_by_user, created_at
        FROM livetwin.operational.maintenance_orders
        ORDER BY created_at DESC LIMIT 5
    """))

# COMMAND ----------

elif mode == "test_chain":
    print("🧪 TEST MODE: Running chain against Engine 15 (direct, no stream)")
    result = run_full_chain(asset_id=15, anomaly_score=0.89, cycle=187)

# COMMAND ----------

elif mode == "full_run":
    print("🚀 FULL RUN MODE: Starting Monitor Agent (continuous)")
    from monitor_agent import run_monitor_agent
    run_monitor_agent(user_context=user_ctx)

# COMMAND ----------

elif mode == "status":
    print("📊 SYSTEM STATUS")

    # Active chains
    display(spark.sql("""
        SELECT ai.asset_id, a.name, ai.stage, ai.started_at,
               TIMESTAMPDIFF(SECOND, ai.started_at, NOW()) as seconds_running
        FROM livetwin.operational.agent_inflight ai
        JOIN livetwin.operational.assets a ON a.id = ai.asset_id
        ORDER BY ai.started_at DESC
    """))

    print("\nRecent Maintenance Orders:")
    display(spark.sql("""
        SELECT id, asset_id, failure_mode, rul_at_detection, urgency,
               estimated_cost, guardian_approved, status, created_at
        FROM livetwin.operational.maintenance_orders
        ORDER BY created_at DESC LIMIT 10
    """))

    print("\nFleet Health:")
    display(spark.sql("""
        SELECT asset_id, health_score, health_status, total_cycles,
               anomaly_count_24h, last_seen_ts
        FROM livetwin.processed.gold_engine_health
        ORDER BY health_score ASC LIMIT 20
    """))

# COMMAND ----------

print(f"\n✅ LiveTwin Orchestrator — Mode={mode} complete.")
print(f"   MLflow traces at: /Shared/livetwin/agent_traces")
print(f"   Unity Catalog audit: SHOW AUDIT LOG FOR CATALOG livetwin")
