# LiveTwin — Agent Design Specification

---

## Agent System Overview

Four agents. One direction of flow. One write path.

```
Monitor → Diagnosis → Plan → Guardian → [Lakebase write OR Human escalation]
```

**Design principles:**
- Each agent has exactly one job
- Only Guardian writes to Lakebase — all other agents are read-only
- Failure in any agent before Guardian cannot corrupt operational data
- Every agent call is traced by MLflow 3.0 — full decision chain is auditable
- Agents run under OBO authorization — inherit the triggering user's permissions

---

## Agent 1: Monitor Agent

### Job
Continuously scan the fleet for anomalies. When one is found, trigger the Diagnosis Agent.

### Trigger
Runs on a continuous loop. Does NOT wait for user input.
Powered by Real-Time Spark Mode (<300ms per fleet scan cycle).

### Inputs
- Lakebase `sensor_readings` table (current readings, last 100ms window)
- Anomaly thresholds config (stored in Lakebase `config` table)

### Logic

```python
def monitor_agent():
    while True:
        # Real-Time Spark: scan entire fleet in <300ms
        anomalies = spark.sql("""
            SELECT asset_id, timestamp, anomaly_score, anomaly_flag,
                   sensors
            FROM livetwin.operational.sensor_readings
            WHERE timestamp > NOW() - INTERVAL '30 seconds'
              AND anomaly_flag = TRUE
              AND asset_id NOT IN (
                  SELECT asset_id FROM maintenance_orders
                  WHERE status = 'pending' OR status = 'scheduled'
              )
            ORDER BY anomaly_score DESC
            LIMIT 5
        """)

        for anomaly in anomalies:
            # Don't re-trigger if already processing this engine
            if not is_in_flight(anomaly.asset_id):
                mark_in_flight(anomaly.asset_id)
                trigger_diagnosis_agent(anomaly)

        time.sleep(0.1)  # 100ms poll cycle
```

### Outputs
- Triggers Diagnosis Agent with: `{asset_id, timestamp, anomaly_score, sensor_snapshot}`

### MLflow Trace Fields
```json
{
  "span": "monitor_agent",
  "inputs": {"fleet_size": 20, "scan_window_ms": 300},
  "outputs": {"anomalies_detected": 1, "asset_id": 15},
  "latency_ms": 287,
  "model_version": null
}
```

---

## Agent 2: Diagnosis Agent

### Job
Given an anomaly, determine: how severe is it, how long until failure, what is the failure mode,
and what historical incidents are most similar?

### Trigger
Called by Monitor Agent on anomaly detection.

### Inputs
- `asset_id` and `timestamp` from Monitor Agent
- Lakebase `sensor_readings` (current state + last 100 cycles)
- Gold Delta `gold_engine_health` (5-year history for this asset)
- MLflow RUL Predictor endpoint
- MLflow Failure Mode Classifier endpoint
- Vector Search index (similar past incidents)

### Logic

```python
def diagnosis_agent(asset_id, anomaly_snapshot):

    # Step 1: Get current state from Lakebase (fast, OLTP)
    current_state = lakebase.query("""
        SELECT * FROM sensor_readings
        WHERE asset_id = %s
        ORDER BY timestamp DESC LIMIT 100
    """, [asset_id])

    # Step 2: Get historical health from Gold Delta (analytical)
    historical_health = spark.sql(f"""
        SELECT * FROM livetwin.processed.gold_engine_health
        WHERE asset_id = {asset_id}
        ORDER BY timestamp DESC LIMIT 500
    """)

    # Step 3: Feature engineering for ML input
    features = engineer_features(current_state, historical_health)

    # Step 4: RUL prediction
    rul_result = mlflow_client.predict(
        endpoint="livetwin-rul-predictor",
        inputs=features
    )
    # Returns: {rul_hours: 23.4, confidence_interval: [18.2, 28.6]}

    # Step 5: Failure mode classification
    failure_result = mlflow_client.predict(
        endpoint="livetwin-failure-classifier",
        inputs=features
    )
    # Returns: {mode: "bearing_degradation", confidence: 0.87}

    # Step 6: Semantic search for similar incidents
    query = f"""
        Turbofan engine showing {anomaly_snapshot['pattern_description']}.
        Predicted failure mode: {failure_result['mode']}.
        Estimated cycles remaining: {rul_result['rul_hours'] / HOURS_PER_CYCLE:.0f}
    """
    similar_incidents = vector_search.similarity_search(
        query_text=query,
        num_results=3
    )

    return {
        "asset_id": asset_id,
        "rul_hours": rul_result["rul_hours"],
        "rul_confidence_interval": rul_result["confidence_interval"],
        "failure_mode": failure_result["mode"],
        "failure_confidence": failure_result["confidence"],
        "similar_incidents": similar_incidents,
        "diagnosis_summary": build_summary(rul_result, failure_result, similar_incidents)
    }
```

### Outputs
```json
{
  "asset_id": 15,
  "rul_hours": 23.4,
  "rul_confidence_interval": [18.2, 28.6],
  "failure_mode": "bearing_degradation",
  "failure_confidence": 0.87,
  "similar_incidents": [
    {
      "incident_id": "INC-2019-0042",
      "similarity_score": 0.94,
      "root_cause": "Lubrication failure in HPT bearing housing",
      "corrective_action": "Replace HPT bearing assembly",
      "cost": 45000
    }
  ],
  "diagnosis_summary": "Engine 15 showing bearing degradation pattern. 23.4 hours to failure (87% confidence). Matches INC-2019-0042 closely (94% similarity)."
}
```

### MLflow Trace Fields
```json
{
  "span": "diagnosis_agent",
  "inputs": {"asset_id": 15, "anomaly_score": 0.89},
  "outputs": {"rul_hours": 23.4, "failure_mode": "bearing_degradation"},
  "latency_ms": 2341,
  "model_versions": {
    "rul_predictor": "v3",
    "failure_classifier": "v2"
  },
  "vector_search_results": 3
}
```

---

## Agent 3: Plan Agent

### Job
Propose a concrete maintenance intervention: what to do, when to do it, what it costs.

### Trigger
Called by Diagnosis Agent with full diagnosis output.

### Inputs
- Diagnosis output (rul_hours, failure_mode, similar_incidents)
- Lakebase `operational_schedule` (when this asset has maintenance windows available)
- Lakebase `assets` (asset priority, location, maintenance team assigned)
- Cost model (looked up from Gold table maintenance cost history)

### Logic

```python
def plan_agent(diagnosis):

    asset_id = diagnosis["asset_id"]
    rul_hours = diagnosis["rul_hours"]
    failure_mode = diagnosis["failure_mode"]

    # Get available maintenance windows within the urgency window
    urgency_window_hours = rul_hours * 0.7  # Act before 70% of RUL is consumed
    available_windows = lakebase.query("""
        SELECT window_start, window_end, priority_override
        FROM operational_schedule
        WHERE asset_id = %s
          AND window_start <= NOW() + INTERVAL '%s hours'
          AND status = 'available'
        ORDER BY window_start
        LIMIT 5
    """, [asset_id, urgency_window_hours])

    # Estimate maintenance cost from similar incidents
    estimated_cost = mean([
        incident["cost"] for incident in diagnosis["similar_incidents"]
    ])

    # Select optimal window (earliest available within urgency window)
    if available_windows:
        optimal_window = available_windows[0]
        urgency = "SCHEDULED" if optimal_window.window_start > NOW() + timedelta(hours=4) \
                  else "URGENT"
    else:
        optimal_window = None
        urgency = "CRITICAL"  # No window available, will escalate

    return {
        "asset_id": asset_id,
        "failure_mode": failure_mode,
        "recommended_window": optimal_window,
        "estimated_cost": estimated_cost,
        "urgency": urgency,
        "maintenance_action": lookup_standard_action(failure_mode),
        "plan_summary": build_plan_summary(...)
    }
```

### Outputs
```json
{
  "asset_id": 15,
  "failure_mode": "bearing_degradation",
  "recommended_window": {
    "start": "2026-04-11T22:00:00Z",
    "end": "2026-04-12T04:00:00Z"
  },
  "estimated_cost": 42500,
  "urgency": "URGENT",
  "maintenance_action": "Replace HPT bearing assembly. Flush lubrication system. Conduct post-repair vibration test.",
  "plan_summary": "Urgent maintenance required within 16.4 hours. Recommended window: tonight 10PM–4AM. Estimated cost: ₹42,500. Action: HPT bearing replacement."
}
```

---

## Agent 4: Guardian Agent

### Job
The only agent with write access. Validate the plan against safety, cost, and operational constraints.
Test on a Lakebase branch before committing. Log everything.

### Trigger
Called by Plan Agent with full plan output.

### Inputs
- Plan Agent output (full plan)
- Diagnosis Agent output (context)
- Lakebase `operational_schedule` (confirming window is still available)
- Safety rule set (stored in Vector Search, queried via RAG)
- Budget threshold config (from Lakebase `config` table)
- OBO user context (who triggered this chain — their permissions are active)

### Logic

```python
def guardian_agent(plan, diagnosis, user_context):

    # === STEP 1: Pre-validation checks (before touching production DB) ===
    checks = []

    # Check 1: Cost within user's authorization limit
    user_cost_limit = get_user_cost_limit(user_context.user_id)
    cost_ok = plan["estimated_cost"] <= user_cost_limit
    checks.append({"check": "cost_authorization", "passed": cost_ok,
                   "detail": f"Cost {plan['estimated_cost']} vs limit {user_cost_limit}"})

    # Check 2: Maintenance window still available (not taken by another agent)
    window_available = lakebase.query("""
        SELECT COUNT(*) FROM operational_schedule
        WHERE asset_id = %s AND window_start = %s AND status = 'available'
    """, [plan["asset_id"], plan["recommended_window"]["start"]])[0] > 0
    checks.append({"check": "window_available", "passed": window_available})

    # Check 3: No conflicting orders for this asset
    no_conflict = lakebase.query("""
        SELECT COUNT(*) FROM maintenance_orders
        WHERE asset_id = %s AND status IN ('pending', 'scheduled')
    """, [plan["asset_id"]])[0] == 0
    checks.append({"check": "no_conflicting_orders", "passed": no_conflict})

    # Check 4: Safety protocol check via RAG
    safety_query = f"Is it safe to take {plan['maintenance_action']} on {plan['failure_mode']} with {diagnosis['rul_hours']} hours remaining?"
    safety_docs = vector_search.similarity_search(safety_query, num_results=2,
                                                   filters={"doc_type": "safety_protocol"})
    safety_ok = evaluate_safety_compliance(safety_docs, plan)
    checks.append({"check": "safety_protocol", "passed": safety_ok,
                   "sources": [d["incident_id"] for d in safety_docs]})

    all_passed = all(c["passed"] for c in checks)

    if not all_passed:
        # Log decision, do NOT write to production
        log_decision(plan, checks, verdict="BLOCKED")
        return {"verdict": "BLOCKED", "reason": [c for c in checks if not c["passed"]]}

    # === STEP 2: Branch production DB and test ===
    branch_id = f"guardian_test_{plan['asset_id']}_{int(time.time())}"

    lakebase.execute(f"CREATE BRANCH {branch_id} FROM main")

    try:
        # Test write on branch
        lakebase.execute(f"""
            USE BRANCH {branch_id};
            INSERT INTO maintenance_orders (asset_id, failure_mode, rul_at_detection,
                recommended_window_start, recommended_window_end, estimated_cost,
                guardian_approved, guardian_reason, approved_by_user)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s)
        """, [..., user_context.user_id])

        # Verify the write looks correct on the branch
        verify = lakebase.query(f"USE BRANCH {branch_id}; SELECT * FROM maintenance_orders WHERE asset_id = %s ORDER BY created_at DESC LIMIT 1", [plan["asset_id"]])
        assert verify is not None, "Branch write verification failed"

        # === STEP 3: Commit to production ===
        lakebase.execute(f"MERGE BRANCH {branch_id} INTO main")
        verdict = "APPROVED"

    except Exception as e:
        lakebase.execute(f"DROP BRANCH {branch_id}")
        verdict = "ERROR"
        log_decision(plan, checks, verdict=verdict, error=str(e))
        raise

    finally:
        lakebase.execute(f"DROP BRANCH {branch_id}")  # cleanup

    # === STEP 4: Log everything to MLflow ===
    log_decision(plan, checks, verdict=verdict, branch_id=branch_id)

    return {
        "verdict": verdict,
        "checks": checks,
        "branch_id": branch_id,
        "committed_order_id": verify["id"],
        "approved_by": user_context.user_id
    }
```

### What the Guardian Blocks

| Condition | Block Reason |
|---|---|
| Cost > operator authorization limit | Requires supervisor approval |
| No maintenance window in urgency timeframe | Escalate to human — no automatic deferral |
| Conflicting pending order for same asset | Deduplicate — don't create duplicate work orders |
| Safety protocol violation (RAG-detected) | Safety constraint overrides all other approvals |
| Branch write verification fails | Commit integrity check failed — do not risk production |

### Demo Script for Guardian

1. Show a clean approval: Machine 7, bearing failure, 23 hours RUL → all checks pass → branch created → committed → maintenance order appears in Lakebase dashboard
2. Show a block: Same machine, but set cost limit to ₹10,000 → cost check fails → Guardian logs BLOCKED → no write to production → Genie shows "escalated to supervisor"
3. Show the audit trail: MLflow Tracing → every span visible, every check, every model version

---

## Agent Interaction Diagram (Detailed)

```
USER ACTION: Plant A operator logs in → Monitor Agent starts under OBO auth

[t=0s]   Zerobus: Engine 15 cycle 187 arrives → Delta Bronze
[t=5s]   DLT: Bronze → Silver (anomaly_score = 0.89, anomaly_flag = TRUE)
[t=5s]   Spark write: Silver → Lakebase sensor_readings
[t=5.3s] Monitor Agent (Real-Time Spark, 287ms scan):
         → Detects engine_15 anomaly_flag=TRUE
         → Confirms: not already in-flight
         → Triggers: diagnosis_agent(asset_id=15, anomaly_score=0.89)

[t=5.3s] Diagnosis Agent starts:
         → Lakebase query: current state engine_15 (latency: 8ms)
         → Gold Delta query: 500 historical readings (latency: 340ms)
         → MLflow RUL endpoint: rul_hours=23.4 (latency: 890ms)
         → MLflow Failure Classifier: bearing_degradation, conf=0.87 (latency: 650ms)
         → Vector Search: top-3 similar incidents (latency: 120ms)
         → Returns full diagnosis [total: ~2.1s]

[t=7.4s] Plan Agent starts:
         → Lakebase: available maintenance windows (latency: 12ms)
         → Cost lookup: mean of similar incidents = ₹42,500
         → Returns plan: window tonight 10PM, cost ₹42,500 [total: ~0.8s]

[t=8.2s] Guardian Agent starts:
         → Cost check: ₹42,500 < ₹100,000 limit ✓ (2ms)
         → Window check: available ✓ (5ms)
         → Conflict check: no pending orders ✓ (4ms)
         → Safety RAG: protocol check ✓ (180ms)
         → CREATE BRANCH guardian_test_15_1744372100 (900ms)
         → Test INSERT on branch ✓ (50ms)
         → MERGE branch to main (800ms)
         → MLflow Trace logged
         → Returns: APPROVED, order_id=1042 [total: ~2.0s]

[t=10.2s] Maintenance order #1042 visible in Lakebase dashboard
[t=10.2s] Genie alert triggered: "Machine 15 maintenance scheduled"

TOTAL TIME FROM SENSOR READING TO MAINTENANCE ORDER: ~10 seconds
(vs. human process: 2-6 hours of manual inspection + paperwork)
```

---

## Agent Failure Modes and Fallbacks

| Failure | Impact | Fallback |
|---|---|---|
| Monitor Agent crash | Anomaly detection stops | Auto-restart via Databricks Jobs; alert sent after 30s downtime |
| Diagnosis Agent timeout (>10s) | Agent chain stalls | Timeout exception → log "diagnosis_timeout" → trigger human review |
| MLflow endpoint unavailable | RUL unknown | Fallback to threshold-based heuristic (cycle > 150 AND sensor_2 > threshold) |
| Vector Search unavailable | No similar incidents | Diagnosis proceeds without incident context; flagged in output |
| Plan Agent: no windows available | Cannot schedule | Escalate as CRITICAL → human receives notification via Genie |
| Guardian branch failure | Cannot test commit | Abort entire chain — do NOT write to production; log error |
| Lakebase write fails after merge | Order not recorded | MLflow trace still has all data; manual recovery from trace |
