# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Unity Catalog + ABAC Setup
# MAGIC
# MAGIC Provisions:
# MAGIC - `livetwin` catalog with 5 schemas
# MAGIC - ABAC roles: plant_a_operator, plant_b_operator, maintenance_supervisor, read_only_analyst
# MAGIC - Row-level security tags on assets
# MAGIC - Grants for each role

# COMMAND ----------

# MAGIC %md ## 1. Create Catalog and Schemas

# COMMAND ----------

# DBTITLE 1,Create livetwin catalog
spark.sql("CREATE CATALOG IF NOT EXISTS livetwin COMMENT 'LiveTwin operational AI platform — all data, models, and vectors'")
spark.sql("USE CATALOG livetwin")

# COMMAND ----------

schemas = ["raw", "processed", "operational", "ml", "vectors", "incidents"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS livetwin.{schema}")
    print(f"✓ Schema created: livetwin.{schema}")

# COMMAND ----------

# MAGIC %md ## 2. Create Unity Catalog Groups (ABAC roles)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

groups = [
    "plant_a_operators",
    "plant_b_operators",
    "maintenance_supervisors",
    "read_only_analysts",
]

for group_name in groups:
    try:
        w.groups.create(display_name=group_name)
        print(f"✓ Group created: {group_name}")
    except Exception as e:
        print(f"  Group {group_name} already exists or error: {e}")

# COMMAND ----------

# MAGIC %md ## 3. Grants per Role

# COMMAND ----------

# plant_a_operators — can read processed layer, Plant A assets only (row filter applied later)
spark.sql("""
    GRANT USE CATALOG ON CATALOG livetwin TO `plant_a_operators`
""")
spark.sql("""
    GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.processed TO `plant_a_operators`
""")
spark.sql("""
    GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.operational TO `plant_a_operators`
""")

# COMMAND ----------

# plant_b_operators — same as plant_a but B side
spark.sql("""
    GRANT USE CATALOG ON CATALOG livetwin TO `plant_b_operators`
""")
spark.sql("""
    GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.processed TO `plant_b_operators`
""")
spark.sql("""
    GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.operational TO `plant_b_operators`
""")

# COMMAND ----------

# maintenance_supervisors — all assets, can execute maintenance writes
spark.sql("""
    GRANT USE CATALOG ON CATALOG livetwin TO `maintenance_supervisors`
""")
for schema in ["raw", "processed", "operational", "ml", "vectors", "incidents"]:
    spark.sql(f"""
        GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.{schema} TO `maintenance_supervisors`
    """)
spark.sql("""
    GRANT MODIFY ON SCHEMA livetwin.operational TO `maintenance_supervisors`
""")

# COMMAND ----------

# read_only_analysts — gold tables only, no writes
spark.sql("""
    GRANT USE CATALOG ON CATALOG livetwin TO `read_only_analysts`
""")
spark.sql("""
    GRANT USE SCHEMA, SELECT ON SCHEMA livetwin.processed TO `read_only_analysts`
""")

# COMMAND ----------

# MAGIC %md ## 4. Row-Level Security — Plant A vs Plant B

# COMMAND ----------

# Row filters ensure asset_id 1–10 = Plant A, 11–20 = Plant B
# Unity Catalog applies these transparently — no code in agents

spark.sql("""
    CREATE OR REPLACE FUNCTION livetwin.processed.plant_a_row_filter(asset_id BIGINT)
    RETURNS BOOLEAN
    RETURN
        IS_MEMBER('maintenance_supervisors') OR
        (IS_MEMBER('plant_a_operators') AND asset_id BETWEEN 1 AND 10) OR
        (IS_MEMBER('plant_b_operators') AND asset_id BETWEEN 11 AND 20)
""")
print("✓ Row filter function created")

# COMMAND ----------

# Apply the row filter to gold_engine_health once the table exists
# (Run this cell AFTER 02_pipeline completes)
try:
    spark.sql("""
        ALTER TABLE livetwin.processed.gold_engine_health
        SET ROW FILTER livetwin.processed.plant_a_row_filter ON (asset_id)
    """)
    print("✓ Row filter applied to gold_engine_health")
except Exception as e:
    print(f"  Table not yet created — re-run after pipeline: {e}")

try:
    spark.sql("""
        ALTER TABLE livetwin.processed.silver_sensor_readings
        SET ROW FILTER livetwin.processed.plant_a_row_filter ON (asset_id)
    """)
    print("✓ Row filter applied to silver_sensor_readings")
except Exception as e:
    print(f"  Table not yet created — re-run after pipeline: {e}")

# COMMAND ----------

# MAGIC %md ## 5. Verify Setup

# COMMAND ----------

display(spark.sql("SHOW SCHEMAS IN livetwin"))

# COMMAND ----------

display(spark.sql("SHOW GRANTS ON CATALOG livetwin"))

# COMMAND ----------

print("✅ Unity Catalog setup complete.")
print("Next step: run 01_lakebase_schema.sql in your Lakebase connection.")
