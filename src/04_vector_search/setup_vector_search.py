# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Vector Search Index Setup
# MAGIC
# MAGIC Creates two Vector Search indexes:
# MAGIC 1. `maintenance_reports_index` — 50 maintenance incident reports (used by Diagnosis Agent)
# MAGIC 2. `safety_protocols_index` — 5 safety protocols (used by Guardian Agent)
# MAGIC
# MAGIC Uses Databricks Vector Search with `databricks-gte-large-en` embedding model.
# MAGIC Storage-optimized for billion-scale indices (overkill for 50 docs, shows platform depth).

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch databricks-sdk mlflow -q

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()
vsc = VectorSearchClient()

# COMMAND ----------

# MAGIC %md ## 1. Create Vector Search Endpoint

# COMMAND ----------

VS_ENDPOINT    = "livetwin-vs-endpoint"
EMBEDDING_MODEL = "databricks-gte-large-en"

try:
    vsc.create_endpoint(
        name=VS_ENDPOINT,
        endpoint_type="STANDARD",
    )
    print(f"✓ Created Vector Search endpoint: {VS_ENDPOINT}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Endpoint {VS_ENDPOINT} already exists")
    else:
        raise

# Wait for endpoint to be ready
print("Waiting for endpoint to be ONLINE...")
for i in range(30):
    status = vsc.get_endpoint(VS_ENDPOINT)
    if status["endpoint_status"]["state"] == "ONLINE":
        print(f"✓ Endpoint ONLINE after {i*10}s")
        break
    print(f"  [{i*10}s] Status: {status['endpoint_status']['state']}")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md ## 2. Maintenance Reports Index (Diagnosis Agent)

# COMMAND ----------

INCIDENTS_INDEX = "livetwin.incidents.maintenance_reports_index"
INCIDENTS_TABLE = "livetwin.incidents.maintenance_reports"

# Enable Change Data Feed on source table (required for Delta Sync index)
spark.sql(f"ALTER TABLE {INCIDENTS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"✓ CDF enabled on {INCIDENTS_TABLE}")

try:
    vsc.create_delta_sync_index(
        endpoint_name       = VS_ENDPOINT,
        index_name          = INCIDENTS_INDEX,
        source_table_name   = INCIDENTS_TABLE,
        pipeline_type       = "TRIGGERED",         # sync manually (or on schedule)
        primary_key         = "incident_id",
        embedding_source_column = "full_text",
        embedding_model_endpoint_name = EMBEDDING_MODEL,
    )
    print(f"✓ Created index: {INCIDENTS_INDEX}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Index {INCIDENTS_INDEX} already exists")
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 3. Safety Protocols Index (Guardian Agent)

# COMMAND ----------

SAFETY_INDEX = "livetwin.incidents.safety_protocols_index"
SAFETY_TABLE = "livetwin.incidents.safety_protocols"

spark.sql(f"ALTER TABLE {SAFETY_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

try:
    vsc.create_delta_sync_index(
        endpoint_name       = VS_ENDPOINT,
        index_name          = SAFETY_INDEX,
        source_table_name   = SAFETY_TABLE,
        pipeline_type       = "TRIGGERED",
        primary_key         = "protocol_id",
        embedding_source_column = "full_text",
        embedding_model_endpoint_name = EMBEDDING_MODEL,
    )
    print(f"✓ Created index: {SAFETY_INDEX}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Index {SAFETY_INDEX} already exists")
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 4. Sync Indexes (populate embeddings)

# COMMAND ----------

for index_name in [INCIDENTS_INDEX, SAFETY_INDEX]:
    try:
        vsc.get_index(VS_ENDPOINT, index_name).sync()
        print(f"✓ Sync triggered for: {index_name}")
    except Exception as e:
        print(f"  Sync error on {index_name}: {e}")

# Wait for initial sync
print("Waiting for indexes to sync...")
for i in range(20):
    try:
        i_status = vsc.get_index(VS_ENDPOINT, INCIDENTS_INDEX)
        s_status = vsc.get_index(VS_ENDPOINT, SAFETY_INDEX)
        i_ready = i_status.describe().get("status", {}).get("indexed_row_count", 0) > 0
        s_ready = s_status.describe().get("status", {}).get("indexed_row_count", 0) > 0
        if i_ready and s_ready:
            print(f"✓ Both indexes synced after {i*15}s")
            break
        print(f"  [{i*15}s] Incidents indexed: {i_ready}, Safety indexed: {s_ready}")
    except Exception as e:
        print(f"  [{i*15}s] Checking sync: {e}")
    time.sleep(15)

# COMMAND ----------

# MAGIC %md ## 5. Verify with Sample Queries

# COMMAND ----------

incidents_idx = vsc.get_index(VS_ENDPOINT, INCIDENTS_INDEX)

# Test 1: bearing degradation query (should match INC-2019-0042)
results = incidents_idx.similarity_search(
    query_text="Turbofan engine showing elevated s2 temperature and decreasing s11 pressure. Bearing degradation pattern.",
    num_results=3,
    filters={"asset_type": "turbofan"},
)
print("=== Test 1: Bearing Degradation Query ===")
for r in results.get("result", {}).get("data_array", []):
    print(f"  {r[0]}: similarity={r[-1]:.4f}")

# COMMAND ----------

# Test 2: fuel system query
results2 = incidents_idx.similarity_search(
    query_text="Decreasing fuel flow ratio s12 with elevated s4 temperature. Fuel efficiency dropping.",
    num_results=3,
)
print("\n=== Test 2: Fuel System Query ===")
for r in results2.get("result", {}).get("data_array", []):
    print(f"  {r[0]}: similarity={r[-1]:.4f}")

# COMMAND ----------

# Test 3: safety protocol
safety_idx = vsc.get_index(VS_ENDPOINT, SAFETY_INDEX)
results3 = safety_idx.similarity_search(
    query_text="Is it safe to replace HPT bearing with 23 hours remaining until failure?",
    num_results=2,
)
print("\n=== Test 3: Safety Protocol Query ===")
for r in results3.get("result", {}).get("data_array", []):
    print(f"  {r[0]}: {r[1][:60]}... similarity={r[-1]:.4f}")

# COMMAND ----------

print(f"""
✅ Vector Search Setup Complete:

  Endpoint: {VS_ENDPOINT}
  Maintenance Incidents Index: {INCIDENTS_INDEX}
  Safety Protocols Index:      {SAFETY_INDEX}
  Embedding Model: {EMBEDDING_MODEL}

Diagnosis Agent config:
  incidents_index = "{INCIDENTS_INDEX}"
  similarity_search(query, num_results=3, filters={{"asset_type": "turbofan"}})

Guardian Agent config:
  safety_index = "{SAFETY_INDEX}"
  similarity_search(query, num_results=2, filters={{"doc_type": "safety_protocol"}})
""")
