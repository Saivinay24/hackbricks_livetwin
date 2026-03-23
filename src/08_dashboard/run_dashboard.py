# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Dashboard API Server
# MAGIC
# MAGIC Starts the FastAPI server on the Databricks driver node.
# MAGIC
# MAGIC **Usage:**
# MAGIC 1. Run this notebook as a long-running Databricks Job (not interactive)
# MAGIC 2. Note the proxy URL shown in the output
# MAGIC 3. Open dashboard at: `{workspace_url}/driver-proxy/o/{org_id}/{cluster_id}/8000/`
# MAGIC
# MAGIC Or for local dev:
# MAGIC ```
# MAGIC cd src/08_dashboard
# MAGIC LAKEBASE_HOST=<host> LAKEBASE_PASSWORD=<pw> python api_server.py
# MAGIC ```

# COMMAND ----------

import subprocess
import sys
import os

# Install deps (only needed once per cluster)
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "fastapi", "uvicorn[standard]", "psycopg2-binary", "--quiet"])

# COMMAND ----------

# Set up path
REPO_PATH = "/Workspace/Repos/livetwin"
sys.path.insert(0, f"{REPO_PATH}/src/08_dashboard")
sys.path.insert(0, f"{REPO_PATH}/src/05_agents")

# Read Lakebase credentials from Databricks Secrets
try:
    lakebase_host     = dbutils.secrets.get("livetwin", "lakebase_host")
    lakebase_password = dbutils.secrets.get("livetwin", "lakebase_password")
except Exception:
    lakebase_host = ""
    lakebase_password = ""
    print("  Warning: Lakebase secrets not found. DB calls will fail.")

os.environ["LAKEBASE_HOST"]     = lakebase_host
os.environ["LAKEBASE_PASSWORD"] = lakebase_password
os.environ["LAKEBASE_USER"]     = "token"
os.environ["LAKEBASE_DB"]       = "livetwin"
os.environ["PORT"]              = "8000"

# COMMAND ----------

# MAGIC %md ## Start FastAPI Server

# COMMAND ----------

import uvicorn
import threading
from api_server import app

def run_server():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

server_thread = threading.Thread(target=run_server, daemon=True)
server_thread.start()

import time
time.sleep(3)
print("✓ LiveTwin Dashboard API started on port 8000")
print()

# COMMAND ----------

# Print the dashboard URL
try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    workspace_url = ctx.apiUrl().getOrElse(None)
    cluster_id    = ctx.clusterId().getOrElse(None)
    org_id        = ctx.workspaceId().getOrElse(None)
    if workspace_url and cluster_id and org_id:
        proxy_url = f"{workspace_url}/driver-proxy/o/{org_id}/{cluster_id}/8000/"
        print(f"Dashboard URL: {proxy_url}")
    else:
        print("Dashboard URL: http://localhost:8000")
except Exception:
    print("Dashboard URL: http://localhost:8000")

# COMMAND ----------

# Health check
import requests
try:
    r = requests.get("http://localhost:8000/health", timeout=5)
    print(f"Health check: {r.json()}")
except Exception as e:
    print(f"Health check failed: {e}")

# COMMAND ----------

# Keep alive — this cell blocks the notebook so the server stays running
print("Server running. Keep this cell alive to maintain the API.")
print("Cancel this cell to stop the server.")
server_thread.join()
