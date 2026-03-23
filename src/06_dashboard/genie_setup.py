# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Genie AI/BI Setup
# MAGIC
# MAGIC Configures a Genie Space with the LiveTwin operational tables.
# MAGIC Genie learns your schema and answers natural-language questions about operations.
# MAGIC
# MAGIC Sample questions Genie must answer:
# MAGIC - "Why did Machine 15 fail?"
# MAGIC - "How much did LiveTwin save this month?"
# MAGIC - "Which engines need attention in the next 48 hours?"
# MAGIC - "What is the most common failure mode?"
# MAGIC - "Show me all CRITICAL maintenance orders"

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md ## 1. Create Genie Space

# COMMAND ----------

GENIE_SPACE_NAME = "LiveTwin Operations"
GENIE_DESCRIPTION = (
    "Operational AI platform for turbofan engine fleet. "
    "Contains sensor readings, maintenance orders, agent decisions, and fleet health KPIs. "
    "20 engines across Plant A (1-10) and Plant B (11-20). "
    "Failure modes: bearing_degradation, thermal_degradation, vibration, fuel_system, compressor_degradation."
)

# Tables to expose to Genie
GENIE_TABLES = [
    "livetwin.processed.gold_engine_health",
    "livetwin.processed.gold_fleet_summary",
    "livetwin.processed.silver_sensor_readings",
    "livetwin.operational.maintenance_orders",
    "livetwin.operational.agent_decisions",
    "livetwin.operational.assets",
    "livetwin.operational.operational_schedule",
]

# Create Genie Space via API
try:
    response = w.genie.create_space(
        title=GENIE_SPACE_NAME,
        description=GENIE_DESCRIPTION,
        table_identifiers=GENIE_TABLES,
    )
    space_id = response.id
    print(f"✓ Genie Space created: {GENIE_SPACE_NAME} (id={space_id})")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Genie Space already exists. Proceed with adding context.")
        # Get existing space
        spaces = w.genie.list_spaces()
        for sp in spaces:
            if sp.title == GENIE_SPACE_NAME:
                space_id = sp.id
                break
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 2. Add Business Context Instructions

# COMMAND ----------

INSTRUCTIONS = """
You are the LiveTwin operational AI assistant. You help maintenance engineers and plant operators
understand the health of their turbofan engine fleet and take action.

## Key Business Context

- **Fleet**: 20 turbofan engines. Plant A: engines 1-10. Plant B: engines 11-20.
- **Health Score**: 0-100. Above 80 = healthy, 50-80 = degrading, 20-50 = critical, below 20 = failure imminent.
- **RUL**: Remaining Useful Life in hours. Below 24h = URGENT. Below 8h = CRITICAL.
- **Failure Modes**: bearing_degradation, thermal_degradation, vibration, fuel_system, compressor_degradation
- **Cost**: All costs in Indian Rupees (INR). Supervisor approval required above ₹50,000.
- **Guardian**: The AI agent that writes maintenance orders. It branches the database to test before committing.
- **OBO**: On-Behalf-Of authorization — agents run with the triggering user's exact permissions.

## Common Queries

### "How much did LiveTwin save this month?"
JOIN maintenance_orders (guardian_approved=TRUE) with failure mode cost estimates.
Unplanned failure cost ≈ 3x planned maintenance cost (industry benchmark).
Net savings = (unplanned_cost - planned_cost) × orders_completed.

### "Which engines need attention?"
Query gold_engine_health WHERE health_status IN ('critical', 'failure_imminent')
OR maintenance_orders WHERE status = 'pending' AND urgency IN ('URGENT', 'CRITICAL').

### "Why did Machine 15 fail?"
Combine: agent_decisions (Guardian's reasoning), maintenance_orders (failure_mode, rul_at_detection),
gold_engine_health (health trend), silver_sensor_readings (sensor anomaly history).

### "Show Guardian decisions"
agent_decisions WHERE agent_name = 'guardian_agent', showing verdict, checks, and branch_id.

## Important Notes
- All monetary values are in INR (Indian Rupees)
- Cycle counts ≈ operational hours (1 cycle ≈ 1 hour for turbofan engines in CMAPSS dataset)
- Plant A operators can only see assets 1-10; Plant B operators can only see 11-20 (Unity Catalog ABAC)
- The `sensors` column in sensor_readings contains a JSON map: {"s1": value, "s2": value, ..., "s21": value}
- s2 = LPC outlet temperature, s4 = LPT outlet temperature, s7 = HPC outlet pressure
"""

try:
    w.genie.update_space(
        space_id=space_id,
        instructions=INSTRUCTIONS,
    )
    print(f"✓ Business context instructions added to Genie Space")
except Exception as e:
    print(f"  Error adding instructions: {e}")

# COMMAND ----------

# MAGIC %md ## 3. Add Sample Questions (Genie Curated Answers)

# COMMAND ----------

SAMPLE_QUESTIONS = [
    {
        "question": "How much did LiveTwin save this month?",
        "hint": "Join maintenance_orders with estimated_cost, multiply by 2x for unplanned savings."
    },
    {
        "question": "Which engines need attention in the next 48 hours?",
        "hint": "Filter gold_engine_health where health_status in ('critical','failure_imminent') or maintenance_orders pending."
    },
    {
        "question": "What is the current fleet health?",
        "hint": "Query gold_fleet_summary for aggregate counts and average health score."
    },
    {
        "question": "Show me all CRITICAL maintenance orders",
        "hint": "Filter maintenance_orders where urgency = 'CRITICAL' and status = 'pending'."
    },
    {
        "question": "What happened with Engine 15?",
        "hint": "Join maintenance_orders, agent_decisions, and gold_engine_health for Engine 15 (asset_id=15)."
    },
    {
        "question": "What is the most common failure mode?",
        "hint": "GROUP BY failure_mode in maintenance_orders, COUNT."
    },
    {
        "question": "Show all Guardian decisions in the last 24 hours",
        "hint": "Query agent_decisions where agent_name='guardian_agent' and timestamp > now - 24h."
    },
    {
        "question": "Which plant has more anomalies today?",
        "hint": "Join assets with sensor_readings or gold_engine_health, group by plant."
    },
]

for sq in SAMPLE_QUESTIONS:
    try:
        w.genie.add_sample_question(
            space_id=space_id,
            question=sq["question"],
            hint=sq["hint"],
        )
        print(f"  ✓ Added: {sq['question'][:60]}")
    except Exception as e:
        print(f"  Error adding question '{sq['question'][:40]}': {e}")

# COMMAND ----------

# MAGIC %md ## 4. Verify Genie Space

# COMMAND ----------

try:
    space = w.genie.get_space(space_id)
    print(f"\n✓ Genie Space verified:")
    print(f"  Name:   {space.title}")
    print(f"  Tables: {len(GENIE_TABLES)}")
    print(f"  ID:     {space_id}")
    print(f"\n  URL: https://{{workspace}}/genie/spaces/{space_id}")
except Exception as e:
    print(f"  Error verifying space: {e}")

# COMMAND ----------

# MAGIC %md ## 5. Test Genie Queries

# COMMAND ----------

TEST_QUERIES = [
    "What is the current fleet health?",
    "How many engines are in critical condition?",
    "Show me the most recent maintenance orders",
]

for query in TEST_QUERIES:
    print(f"\nGenie: '{query}'")
    try:
        # Start a Genie conversation
        conv = w.genie.start_conversation(space_id=space_id, content=query)
        print(f"  Conversation started: {conv.conversation_id}")
        # Wait for response (in real demo, UI shows this in real-time)
        import time; time.sleep(3)
        messages = w.genie.get_messages(space_id=space_id, conversation_id=conv.conversation_id)
        for msg in messages.messages or []:
            if msg.role == "ASSISTANT":
                print(f"  Genie: {msg.content[:150]}...")
    except Exception as e:
        print(f"  Error: {e}")

# COMMAND ----------

print(f"""
✅ Genie AI/BI Setup Complete

Space: {GENIE_SPACE_NAME}
Tables: {len(GENIE_TABLES)} tables exposed

Demo questions for judges:
1. "How much did LiveTwin save this month?"
2. "Which engines need attention in the next 48 hours?"
3. "Why did Machine 15 fail?"
4. "Show all Guardian APPROVED decisions today"
5. "What is the current fleet health?"
""")
