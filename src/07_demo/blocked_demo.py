# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Guardian BLOCKED Demo
# MAGIC
# MAGIC Shows the Guardian blocking an invalid maintenance order.
# MAGIC
# MAGIC Demo scenario:
# MAGIC - Same Engine 15 failure
# MAGIC - Plant A Operator (low cost limit: ₹50,000) tries to approve ₹95,000 repair
# MAGIC - Guardian blocks: cost exceeds authorization limit
# MAGIC - No write to production DB
# MAGIC - Escalation path shown
# MAGIC
# MAGIC Use this after the APPROVED demo to show the "what-if" safety case.

# COMMAND ----------

import sys
from datetime import datetime, timezone, timedelta
sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

from guardian_agent import run_guardian_agent
from utils.lakebase_client import execute

# Temporarily set operator cost limit low for demo
execute("UPDATE config SET value = '10000' WHERE key = 'cost_limit_operator'")
print("  Config: operator cost limit temporarily set to ₹10,000 for blocked demo")

# COMMAND ----------

now = datetime.now(timezone.utc)

# Use a high-cost plan that exceeds operator limit
BLOCKED_PLAN = {
    "asset_id":           15,
    "failure_mode":       "bearing_degradation",
    "recommended_window": {
        "start": (now + timedelta(hours=2)).isoformat(),
        "end":   (now + timedelta(hours=6)).isoformat(),
        "priority_override": False,
        "schedule_id": 1,
    },
    "estimated_cost": 95000.0,     # Exceeds ₹10,000 operator limit
    "urgency": "URGENT",
    "maintenance_action": "Full bearing replacement + lubrication system overhaul.",
    "similar_incidents": [{"incident_id": "INC-2024-0033", "cost": 95000, "similarity_score": 0.88}],
    "timing_ms": {},
}

BLOCKED_DIAGNOSIS = {
    "asset_id": 15, "rul_hours": 23.4,
    "rul_lower_bound": 18.2, "rul_upper_bound": 28.6,
    "failure_confidence": 0.87,
}

# Operator user (lower cost limit)
OPERATOR_USER = {
    "user_id": "plant_b_operator@livetwin.com",
    "groups":  ["plant_b_operators"],
    "source":  "obo",
}

print("\n=== Guardian BLOCKED Demo ===")
print(f"User: {OPERATOR_USER['user_id']}")
print(f"Estimated cost: ₹{BLOCKED_PLAN['estimated_cost']:,.0f}")
print(f"Operator cost limit: ₹10,000")
print()

result = run_guardian_agent(BLOCKED_PLAN, BLOCKED_DIAGNOSIS, OPERATOR_USER)

print(f"\nVERDICT: {result['verdict']}")
if result["verdict"] == "BLOCKED":
    print(f"Block reason: {result.get('block_reasons', 'N/A')}")
    print("\n✓ Production database unchanged — no maintenance order written")
    print("→ Escalation path: Supervisor receives notification to approve this order")
else:
    print("Unexpected: order was approved despite cost violation")

# COMMAND ----------

# Restore operator cost limit to real value
execute("UPDATE config SET value = '50000' WHERE key = 'cost_limit_operator'")
print("\n  Config: operator cost limit restored to ₹50,000")

# COMMAND ----------

# Show that production was NOT modified
import pyspark.sql.functions as F
df = spark.sql("""
    SELECT id, asset_id, estimated_cost, approved_by_user, created_at
    FROM livetwin.operational.maintenance_orders
    ORDER BY created_at DESC LIMIT 3
""")
print("\nLatest maintenance orders (should NOT contain ₹95,000 record):")
display(df)
