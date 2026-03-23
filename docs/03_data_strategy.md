# LiveTwin — Data Strategy

---

## Primary Dataset: NASA CMAPSS Turbofan Engine Degradation

**Source:** NASA Ames Prognostics Data Repository (public domain)
**URL:** https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/
**Size:** ~30MB total across 4 sub-datasets

### Why This Dataset

1. **Purpose-built for RUL:** Ground truth labels for Remaining Useful Life included
2. **Realistic sensor profiles:** 21 sensors mimic real industrial equipment
3. **Run-to-failure sequences:** Engines are tracked from healthy → failure
4. **Well-validated:** Published in hundreds of research papers — judges who know ML will recognize it
5. **Public domain:** No data rights issues for hackathon submission
6. **Clean:** Minimal preprocessing needed — spend time on architecture, not data wrangling

### Dataset Structure

| File | Contents | Rows |
|---|---|---|
| train_FD001.txt | Training sequences, 100 engines, 1 operating condition, 1 fault mode | ~20,631 |
| train_FD002.txt | 260 engines, 6 operating conditions, 1 fault mode | ~53,759 |
| train_FD003.txt | 100 engines, 1 operating condition, 2 fault modes | ~24,720 |
| train_FD004.txt | 249 engines, 6 operating conditions, 2 fault modes | ~61,249 |
| test_FD001.txt | Test sequences (truncated — RUL unknown) | ~13,096 |
| RUL_FD001.txt | Ground truth RUL for test sequences | 100 values |

**Use FD001 for core demo** (simplest: 1 operating condition, 1 fault mode).
Use FD004 for scalability story (6 conditions, 2 fault modes, 249 engines).

### Column Schema

```
unit_id  | cycle | op_setting_1 | op_setting_2 | op_setting_3 |
sensor_1 | sensor_2 | ... | sensor_21
```

**Sensors with most predictive power (from literature):**
- sensor_2: LPC outlet temperature
- sensor_3: HPC outlet temperature
- sensor_4: LPT outlet temperature
- sensor_7: HPC outlet pressure
- sensor_11: HPC outlet static pressure
- sensor_12: Ratio of fuel flow to pressure ratio
- sensor_15: Bleed enthalpy
- sensor_20: Total temperature at fan inlet
- sensor_21: Total pressure at fan inlet

---

## Streaming Simulation Strategy

### How CMAPSS Becomes a Live Stream

The dataset contains run-to-failure sequences where each row = one engine cycle.
We simulate IoT streaming by replaying these rows at configurable speed via Zerobus.

```python
# Pseudocode: CMAPSS replay as Zerobus producer
import time
import pandas as pd

df = pd.read_csv("train_FD001.txt", sep=" ", header=None)
# Add timestamp column (current time + cycle offset)

for _, row in df.iterrows():
    payload = {
        "asset_id": row["unit_id"],
        "timestamp": datetime.utcnow().isoformat(),
        "cycle": row["cycle"],
        "sensors": {f"s{i}": row[f"sensor_{i}"] for i in range(1, 22)},
        "op_settings": [row["op_setting_1"], row["op_setting_2"], row["op_setting_3"]]
    }
    zerobus_client.produce("livetwin_sensors", payload)
    time.sleep(REPLAY_SPEED)  # 0.1s = real-time; 0.01s = 10x faster for demo
```

**Demo speed:** 10x replay (0.01s between cycles)
→ A 200-cycle engine run takes 2 seconds in the demo
→ Anomaly appears at cycle 150 → visible ~1.5 seconds into the stream
→ Agent chain triggers within 2 seconds of anomaly appearing

### Injecting a Known Failure

For demo reliability, pre-identify one engine (e.g., engine_id=15 from FD001)
that degrades sharply and predictably. Replay that engine's final 50 cycles
at the start of the demo to guarantee anomaly detection fires within 30 seconds.

---

## Delta Live Tables Pipeline

### Bronze Layer

```python
# DLT Python notation
@dlt.table(
    name="bronze_sensor_readings",
    comment="Raw sensor readings from Zerobus. Immutable, append-only.",
    table_properties={"quality": "bronze"}
)
def bronze_sensor_readings():
    return (
        spark.readStream
            .format("delta")  # Zerobus writes here
            .load("/mnt/livetwin/zerobus_landing")
    )
```

Schema enforcement here: reject rows missing `asset_id` or `timestamp`.
Dead letter table: `bronze_sensor_rejected` for malformed records.

---

### Silver Layer

```python
@dlt.expect_or_drop("valid_asset_id", "asset_id IS NOT NULL AND asset_id > 0")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.table(
    name="silver_sensor_readings",
    comment="Cleaned readings with z-score anomaly flags and rolling statistics",
    table_properties={"quality": "silver"}
)
def silver_sensor_readings():
    bronze = dlt.read_stream("bronze_sensor_readings")

    # Z-score calculation against rolling 50-cycle window per engine
    # Anomaly flag: z-score > 3.0 on any of the 9 high-importance sensors
    # Rolling mean/std stored for monitoring drift over time

    return bronze.withColumn("anomaly_score", ...) \
                 .withColumn("anomaly_flag", col("anomaly_score") > 3.0) \
                 .withColumn("z_scores", ...) \
                 .withColumn("rolling_mean", ...) \
                 .withColumn("rolling_std", ...)
```

---

### Gold Layer

```python
@dlt.table(
    name="gold_engine_health",
    comment="Per-engine health KPIs. Queried by agents and Genie.",
    table_properties={"quality": "gold"}
)
def gold_engine_health():
    silver = dlt.read("silver_sensor_readings")

    # Aggregations per engine:
    # - health_score: composite 0–100 (weighted sensor deviations)
    # - estimated_rul: from MLflow RUL model (updated every 10 cycles)
    # - failure_probability: from classifier model
    # - anomaly_count_24h: count of anomaly flags in last 24 hours
    # - last_maintenance: from Lakebase join
    # - total_cycles: current operational age

    return silver.groupBy("asset_id") \
                 .agg(...)
```

---

## Lakebase Write Strategy

Silver → Lakebase is a critical write path. Two tables get continuous writes:

### `sensor_readings` (hot table, last 1 hour only)

```sql
-- Lakebase DDL
CREATE TABLE sensor_readings (
    id          BIGSERIAL PRIMARY KEY,
    asset_id    INTEGER NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    cycle       INTEGER,
    sensors     JSONB,         -- all 21 sensor values
    anomaly_score FLOAT,
    anomaly_flag  BOOLEAN,
    INDEX idx_asset_ts (asset_id, timestamp DESC)
);

-- Partition strategy: keep only last 1 hour (agents only need current state)
-- Historical data lives in Silver/Gold Delta tables
-- TTL: delete rows older than 1 hour automatically
```

### `maintenance_orders` (append-only, written by Guardian)

```sql
CREATE TABLE maintenance_orders (
    id                BIGSERIAL PRIMARY KEY,
    asset_id          INTEGER NOT NULL,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    failure_mode      VARCHAR(50),
    rul_at_detection  FLOAT,
    recommended_window_start TIMESTAMPTZ,
    recommended_window_end   TIMESTAMPTZ,
    estimated_cost    DECIMAL(10,2),
    guardian_approved BOOLEAN NOT NULL,
    guardian_reason   TEXT,
    agent_decision_id BIGINT REFERENCES agent_decisions(id),
    approved_by_user  VARCHAR(100), -- OBO: the human operator who triggered the chain
    status            VARCHAR(20) DEFAULT 'pending' -- pending/scheduled/completed/cancelled
);
```

---

## Vector Search Index

### Document Corpus

50–100 synthetic maintenance incident reports, structured as:

```json
{
  "incident_id": "INC-2019-0042",
  "asset_type": "turbofan",
  "failure_mode": "bearing_degradation",
  "sensor_pattern": "s2_elevated, s4_elevated, s11_decreasing",
  "cycles_before_failure": 28,
  "root_cause": "Lubrication failure in HPT bearing housing",
  "corrective_action": "Replace HPT bearing assembly, flush lubrication system",
  "outcome": "Engine returned to service after 6-hour maintenance window",
  "cost": 45000
}
```

Generated with GPT-4o using CMAPSS sensor labels as grounding.
10 incidents per failure mode: bearing (10), thermal (10), vibration (10),
fuel system (10), compressor (10) = 50 base documents.

### Index Configuration

```python
# Databricks Vector Search
index = workspace.vector_search.create_index(
    endpoint_name="livetwin-vs-endpoint",
    index_name="livetwin.incidents.maintenance_reports_index",
    primary_key="incident_id",
    embedding_source_column="full_text",  # concatenation of all fields
    embedding_model_endpoint_name="databricks-gte-large-en"
)
```

### Query Pattern (Diagnosis Agent)

```python
# When Diagnosis Agent needs similar incidents:
results = index.similarity_search(
    query_text=f"Sensor pattern: {anomaly_summary}. Failure mode: {predicted_mode}.",
    num_results=3,
    filters={"asset_type": "turbofan"}
)
# Returns top 3 most similar historical incidents with full context
```

---

## Data Governance (Unity Catalog)

### Catalog Structure

```
livetwin (catalog)
├── raw (schema)
│   └── bronze_sensor_readings
├── processed (schema)
│   ├── silver_sensor_readings
│   └── gold_engine_health
├── operational (schema)
│   └── [Lakebase tables synced here via Unity Catalog integration]
├── ml (schema)
│   ├── anomaly_detector (model)
│   ├── rul_predictor (model)
│   └── failure_mode_classifier (model)
└── vectors (schema)
    └── maintenance_reports_index
```

### Access Control (ABAC demo)

| Role | Access |
|---|---|
| `plant_a_operator` | `processed.*` WHERE `asset_id IN (1..10)` — Plant A only |
| `plant_b_operator` | `processed.*` WHERE `asset_id IN (11..20)` — Plant B only |
| `maintenance_supervisor` | All assets, can write `maintenance_orders` |
| `read_only_analyst` | `gold_engine_health` only, no Lakebase write |

**Demo flow for governance:**
1. Agent triggered by `plant_a_operator` login
2. Monitor Agent scans all assets — ABAC silently filters to Plant A assets only
3. Diagnosis Agent queries Gold — Unity Catalog enforces row-level filter
4. Show Unity Catalog audit log: zero queries touched Plant B data
5. Switch to `maintenance_supervisor` login → now all assets visible

No code changes. No filters in agent code. Pure infrastructure governance.
