# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — System Verification
# MAGIC
# MAGIC Final health check after bootstrap workflow completes.
# MAGIC Verifies: Lakebase, MLflow endpoints, Vector Search, Delta tables.

# COMMAND ----------

import sys, os
REPO_PATH = "/Workspace/Repos/livetwin"
sys.path.insert(0, f"{REPO_PATH}/src/05_agents")
sys.path.insert(0, f"{REPO_PATH}/src/08_dashboard")

# Read secrets
lakebase_host     = dbutils.secrets.get("livetwin", "lakebase_host")
lakebase_password = dbutils.secrets.get("livetwin", "lakebase_password")
os.environ["LAKEBASE_HOST"]     = lakebase_host
os.environ["LAKEBASE_PASSWORD"] = lakebase_password
os.environ["LAKEBASE_USER"]     = "token"
os.environ["LAKEBASE_DB"]       = "livetwin"

# COMMAND ----------

results = {}

# 1. Lakebase connectivity
try:
    from utils.lakebase_client import LakebaseClient
    lb = LakebaseClient()
    rows = lb.query("SELECT COUNT(*) AS n FROM assets")
    asset_count = rows[0]["n"] if rows else 0
    results["lakebase"] = f"✓ Connected — {asset_count} assets loaded"
except Exception as e:
    results["lakebase"] = f"✗ FAILED: {e}"

# COMMAND ----------

# 2. Delta tables
try:
    df = spark.sql("SELECT COUNT(*) AS n FROM livetwin.processed.gold_engine_health")
    n = df.collect()[0]["n"]
    results["delta_gold"] = f"✓ gold_engine_health — {n} rows"
except Exception as e:
    results["delta_gold"] = f"✗ FAILED: {e}"

try:
    df2 = spark.sql("SELECT COUNT(*) AS n FROM livetwin.silver.sensor_readings")
    n2 = df2.collect()[0]["n"]
    results["delta_silver"] = f"✓ silver.sensor_readings — {n2} rows"
except Exception as e:
    results["delta_silver"] = f"✗ FAILED: {e}"

# COMMAND ----------

# 3. MLflow endpoints
try:
    import mlflow
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    endpoints = [e.name for e in w.serving_endpoints.list()]
    livetwin_eps = [e for e in endpoints if "livetwin" in e.lower() or "anomaly" in e.lower() or "rul" in e.lower()]
    results["mlflow_endpoints"] = f"✓ Serving endpoints: {livetwin_eps}" if livetwin_eps else "⚠ No LiveTwin endpoints found (models may not be deployed yet)"
except Exception as e:
    results["mlflow_endpoints"] = f"✗ FAILED: {e}"

# COMMAND ----------

# 4. Vector Search
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    indexes = [i.name for i in w.vector_search_indexes.list_indexes("livetwin_incidents")]
    results["vector_search"] = f"✓ Vector Search indexes: {indexes}" if indexes else "⚠ No indexes found"
except Exception as e:
    results["vector_search"] = f"✗ FAILED: {e}"

# COMMAND ----------

# 5. Lakebase table row counts
try:
    rows = lb.query("SELECT COUNT(*) AS n FROM sensor_readings")
    sr_count = rows[0]["n"] if rows else 0
    rows2 = lb.query("SELECT COUNT(*) AS n FROM maintenance_orders")
    mo_count = rows2[0]["n"] if rows2 else 0
    rows3 = lb.query("SELECT COUNT(*) AS n FROM agent_decisions")
    ad_count = rows3[0]["n"] if rows3 else 0
    results["lakebase_tables"] = f"✓ sensor_readings={sr_count}, maintenance_orders={mo_count}, agent_decisions={ad_count}"
except Exception as e:
    results["lakebase_tables"] = f"✗ FAILED: {e}"

# COMMAND ----------

# Print summary
print("=" * 60)
print("  LIVETWIN SYSTEM VERIFICATION REPORT")
print("=" * 60)
all_ok = True
for check, status in results.items():
    icon = "✓" if "✓" in status else ("⚠" if "⚠" in status else "✗")
    print(f"  {check:<25} {status}")
    if "✗" in status:
        all_ok = False

print("=" * 60)
if all_ok:
    print("  ✓ ALL SYSTEMS GO — LiveTwin is ready")
    print()
    print("  Next steps:")
    print("  1. Start the Zerobus producer:  src/01_data/01_zerobus_producer.py")
    print("  2. Start the agent loop:         src/05_agents/agent_orchestrator.py")
    print("  3. Start the dashboard server:   src/08_dashboard/run_dashboard.py")
else:
    print("  ✗ Some checks failed — review above before proceeding")
    raise Exception("System verification failed — check results above")
print("=" * 60)
