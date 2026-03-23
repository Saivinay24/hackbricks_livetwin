# LiveTwin — Master Project Plan
## Why · What · How · When · Where

---

## 1. WHY — The Problem Worth Solving

### The Industry Reality

Industrial equipment failure is one of the most expensive and preventable problems in the world.

| Statistic | Source |
|---|---|
| $170B lost annually to unplanned industrial downtime | Aberdeen Group |
| $2M+ cost of a single turbine failure (parts + downtime + labor) | GE Digital |
| $3,000–$10,000 cost of one manual bridge/equipment inspection | ASCE 2025 |
| 40% disagreement rate between human inspectors on severity | ASCE 2025 |
| 1 in 5 worker deaths occur in construction/industrial settings | OSHA |

### Why Existing Solutions Fail

Every major tech company has a digital twin product:
Siemens Xcelerator, PTC ThingWorx, Microsoft Azure Digital Twins, GE Predix,
AWS IoT TwinMaker, AVEVA, IBM Maximo.

They all share the same architectural limitation:

**They are analytical systems pretending to be operational ones.**

- They ingest sensor data and store it ✓
- They visualize historical trends ✓
- They run batch ML models ✓
- They maintain live equipment state at millisecond latency ✗
- They let agents query live state AND historical data simultaneously ✗
- They enforce per-user permissions when agents take actions ✗
- They provide a full audit trail of agent decisions with rollback ✗

To do all of the above, today you need:
1. PostgreSQL or MongoDB (operational live state)
2. A data lake / warehouse (historical analytics)
3. Kafka or Kinesis (message bus for streaming)
4. LangChain/AutoGen or similar (agent framework)
5. A separate IAM/auth service (permissions for agent actions)
6. A logging platform (audit trail)

**Six platforms. Six integrations. Six failure modes under production load.**

### Why Now

Databricks shipped four architectural changes in the last 8 months that, together,
make single-platform operational AI possible for the first time:

- **Lakebase** (GA Feb 2026): OLTP database native inside the Lakehouse — eliminates #1 and #2 above
- **Zerobus** (GA Feb 2026): Direct sensor-to-Delta streaming — eliminates #3 above
- **Agent Framework + OBO Authorization**: Agents that inherit user permissions — addresses #4 and #5
- **MLflow 3.0** (GA June 2025): Cross-platform agent observability — addresses #6

This is the window. The platform just caught up to the problem.

### Why This Team

The builder has independently designed:
- Multi-agent systems with Guardian governance architecture (Aegis)
- Infrastructure digital twins with multi-layer AI inspection (CityMind)
- Automated BIM generation from physical-world sensors (Scan-to-BIM)
- Real-time telemetry optimization for 5G networks (5G Optimizer)

LiveTwin is not a new direction — it's the convergence of existing work
onto a platform that can now support it at scale.

---

## 2. WHAT — The Product

### Name
**LiveTwin** — Operational AI for Physical Systems

### Tagline
*The digital twin that doesn't just watch. It acts.*

### What It Is

An operational digital twin platform that:

1. **Ingests** real-time sensor data from industrial equipment (turbines, motors, any IoT)
   directly into a Databricks Lakehouse via Zerobus — no message bus required

2. **Maintains** the live operational state of every asset in Lakebase
   (sub-10ms reads, available to agents at runtime)

3. **Processes** sensor history through a Delta medallion pipeline
   (Bronze → Silver → Gold) building health KPIs and degradation curves

4. **Trains and serves** three ML models via MLflow:
   - Anomaly detector (Isolation Forest)
   - Remaining Useful Life predictor (trained on NASA CMAPSS)
   - Failure mode classifier (bearing / thermal / vibration)

5. **Orchestrates** four AI agents that monitor, diagnose, plan, and act:
   - Monitor Agent: catches anomalies in real-time across the full fleet
   - Diagnosis Agent: explains WHY using historical data + semantic search
   - Plan Agent: proposes the right intervention at the right time
   - Guardian Agent: validates safety + governance before execution

6. **Enforces** OBO (On-Behalf-of-User) authorization — agents act with the
   operator's exact permissions, never more, logged to Unity Catalog

7. **Branches** the Lakebase production database before committing agent decisions —
   test the decision against a clone, discard if unsafe, merge if approved

8. **Surfaces** everything via Genie AI/BI — operators ask questions in plain English,
   get SQL-backed answers from their actual data

### What It Is NOT

- Not a chatbot that happens to use Databricks
- Not a dashboard with some ML charts
- Not a research prototype requiring manual data preparation
- Not a pre-built project — architecture designed and built during the hackathon

### The One-Line Demo Moment

> "Machine 7 is 23 hours from bearing failure. The Guardian Agent just branched
> production, tested the maintenance order on the clone, confirmed safety and budget,
> and committed. Here's the full audit trail."

---

## 3. HOW — The Approach

### 3.1 Data Foundation

**Primary Dataset:** NASA CMAPSS Turbofan Degradation Dataset (public domain)
- Source: NASA Ames Research Center
- 4 sub-datasets (FD001–FD004), ~100 engines each
- 21 sensor readings: temperature, pressure, fan speed, fuel flow, vibration proxies
- Run-to-failure sequences with labeled RUL ground truth
- Why: clean, realistic, purpose-built for predictive maintenance, well understood by domain experts

**Streaming Simulation:**
CMAPSS data replayed as a live stream via Zerobus Ingest
→ simulates real IoT sensor feed
→ sub-5-second end-to-end latency to Delta tables
→ no Kafka setup required (this is the demo differentiator)

**Supplemental:**
- Synthetic maintenance reports (GPT-generated, indexed in Vector Search)
- Engine specs and failure mode documentation (indexed in Vector Search for RAG)

### 3.2 Pipeline Design (Delta Medallion)

```
Zerobus Stream → Bronze (raw, immutable)
              → Silver (cleaned, validated, z-score deviations, anomaly flags)
              → Gold (per-engine health KPIs, RUL estimates, failure probabilities)
```

Quality checks at each layer — Lakehouse Monitoring tracks null rates,
distribution drift, and schema drift automatically.

### 3.3 Lakebase Schema Design

```sql
-- Core tables in Lakebase (OLTP, PostgreSQL-compatible)

assets           -- id, name, type, location, last_reading_ts, status
sensor_readings  -- asset_id, timestamp, sensor_1..21, anomaly_score (hot table, last 1h)
maintenance_orders -- id, asset_id, proposed_at, window_start, failure_mode, cost_estimate,
                 --   agent_decision_id, status, approved_by_user
agent_decisions  -- id, agent_name, asset_id, timestamp, input_summary, output_action,
                 --   guardian_verdict, guardian_reason, branch_id
operational_schedule -- asset_id, date, available_windows, priority_override
```

**Database branching for Guardian:**
Before committing any maintenance order:
1. Guardian calls `CREATE BRANCH prod_clone_<decision_id>`
2. Runs validation queries against the clone
3. If safe: `MERGE BRANCH prod_clone_<decision_id> INTO main`
4. If unsafe: `DROP BRANCH prod_clone_<decision_id>` — production untouched

### 3.4 ML Model Specifications

| Model | Algorithm | Training Data | Output | Serving |
|---|---|---|---|---|
| Anomaly Detector | Isolation Forest | Silver layer (normal operating ranges) | Anomaly score 0–1 | MLflow real-time endpoint |
| RUL Predictor | LSTM / Gradient Boosting Regressor | Gold layer (CMAPSS with labels) | Hours to failure ± CI | MLflow real-time endpoint |
| Failure Mode Classifier | Random Forest / XGBoost | Gold layer (labeled failure modes) | Bearing / Thermal / Vibration + confidence | MLflow real-time endpoint |

All models:
- Registered in Unity Catalog model registry
- Tracked with MLflow 3.0 (hyperparameters, metrics, artifacts)
- Monitored by Lakehouse Monitoring (input drift, prediction drift)

### 3.5 Agent Architecture

Four agents orchestrated via Databricks Agent Framework + MCP.

**Interaction flow:**
```
Sensor reading arrives → Zerobus → Delta + Lakebase
                                        ↓
                              Monitor Agent (Real-Time Spark, <300ms)
                              Threshold breach detected?
                                        ↓ YES
                              Diagnosis Agent
                              - Reads Lakebase (current state)
                              - Queries Gold Delta (5-year history)
                              - Runs MLflow RUL endpoint
                              - Vector Search (similar past incidents)
                                        ↓
                              Plan Agent
                              - Reads operational schedule from Lakebase
                              - Proposes maintenance window + cost estimate
                                        ↓
                              Guardian Agent
                              - Branches Lakebase production DB
                              - Validates: safety rules, cost budget, schedule
                              - OBO authorization enforced
                              - MLflow Tracing logs entire chain
                              APPROVED → Write to Lakebase (maintenance_orders)
                              BLOCKED  → Log reason, escalate to human via Genie
```

Full agent design: see [docs/04_agent_design.md](04_agent_design.md)

### 3.6 Governance Architecture

- **Unity Catalog:** all tables, models, functions registered with lineage
- **OBO Authorization:** agents execute as the authenticated operator user
  - Plant A operator → can only see Plant A assets
  - Maintenance supervisor → can see all plants, approve high-cost orders
  - Read-only analyst → agents run as analyst, cannot write maintenance orders
- **MLflow Tracing:** every agent call logged (input, output, latency, model version)
- **Lakebase branching:** every agent commit testable against a clone first

---

## 4. WHEN — Timeline

### Phase 0: Pre-Hackathon Preparation
**Now → April 10, 2026**

| Task | Owner | Deadline |
|---|---|---|
| Finalize team (2–3 people) | Lead | March 28 |
| Complete Round 1 PPT submission | All | Per organizer deadline |
| Set up Databricks workspace + access | Lead | April 1 |
| Download + explore NASA CMAPSS dataset | Data | April 3 |
| Read Lakebase docs + Zerobus docs | All | April 5 |
| Draft Lakebase schema | Lead | April 7 |
| Draft DLT pipeline skeleton | Data | April 7 |
| Draft agent pseudocode for all 4 agents | Lead | April 8 |
| Set up GitHub repo + project structure | Lead | April 9 |
| Pre-load CMAPSS dataset into Databricks | Data | April 10 |
| Pre-train initial RUL model (not final, just to verify setup) | ML | April 10 |

**Goal by April 10:** Nobody writes a line of code at the hackathon venue
without already knowing exactly what it does and where it goes.

---

### Phase 1: Hackathon Day 1 — Build (April 11, 2026)

**Location:** MIT Bengaluru

```
[Hour 0–1]   SETUP
  - Confirm workspace access, cluster configs
  - Load pre-prepared GitHub skeleton
  - Divide workstreams: Pipeline / Models / Agents

[Hour 1–4]   PIPELINE — Data Engineer
  - Zerobus streaming pipeline: CMAPSS replay → Bronze Delta table
  - Delta Live Tables: Bronze → Silver (cleaning, z-score, anomaly flags)
  - Delta Live Tables: Silver → Gold (KPIs, RUL aggregation)
  - Verify: Gold table has correct schema and data

[Hour 1–4]   LAKEBASE — Data Engineer (parallel)
  - Provision Lakebase instance
  - Create schema: assets, sensor_readings, maintenance_orders, agent_decisions
  - Write connector: stream latest readings from Silver → Lakebase sensor_readings
  - Seed assets table with 20 engine IDs from CMAPSS
  - Verify: Lakebase readable from Python agent code

[Hour 4–8]   MODELS — ML Engineer
  - Train Anomaly Detector on Silver layer data (Isolation Forest, ~30min)
  - Train RUL Predictor on Gold layer (CMAPSS labels, GBR, ~45min)
  - Train Failure Mode Classifier (XGBoost on Gold, ~20min)
  - Register all 3 models in MLflow + Unity Catalog
  - Deploy all 3 as MLflow real-time endpoints
  - Verify: each endpoint returns predictions on a test JSON payload

[Hour 4–8]   VECTOR SEARCH — ML Engineer (parallel)
  - Prepare synthetic maintenance report corpus (50–100 docs)
  - Create Vector Search index in Databricks
  - Verify: semantic query returns relevant incident reports

[Hour 8–12]  MONITOR + DIAGNOSIS AGENTS — Lead
  - Implement Monitor Agent: poll Lakebase every 100ms, threshold check
  - Implement Diagnosis Agent: Lakebase + Gold + MLflow + Vector Search
  - Test: inject anomalous reading, verify agents chain correctly

[Hour 12–16] PLAN + GUARDIAN AGENTS — Lead
  - Implement Plan Agent: Lakebase schedule + cost estimate
  - Implement Guardian Agent: branch DB, validate, OBO auth, MLflow Tracing
  - Test: full 4-agent chain on injected anomaly scenario
  - Verify: Lakebase branching works (create, test, merge/drop)

[CHECKPOINT @ Hour 16]
  Full pipeline end-to-end working:
  CMAPSS stream → Delta → Agents → Guardian → Lakebase write
  If any component not working: apply 2-hour recovery plan (see Risk section)
```

---

### Phase 2: Hackathon Day 2 — Polish + Demo (April 12, 2026)

```
[Hour 17–20] INTERFACE — All
  - Set up Genie AI/BI space on Gold tables + Lakebase
  - Configure Genie with domain context (maintenance terminology, table meanings)
  - Build Databricks SQL Dashboard:
      * Fleet health heatmap (all 20 engines)
      * Agent decision timeline
      * Predicted vs actual RUL chart
      * Cost savings counter
  - Test Genie with 5 key questions (see demo script)

[Hour 20–22] UNITY CATALOG + GOVERNANCE — Lead
  - Configure OBO authorization for agents
  - Set up 3 user roles: operator / supervisor / analyst (demo with different access)
  - Add table/model tags, lineage annotations
  - Verify: lineage graph shows sensor → Delta → model → decision → Lakebase

[Hour 22–24] INTEGRATION TESTING — All
  - Run full demo scenario 3x end-to-end
  - Fix any timing/ordering bugs
  - Harden the demo failure scenario (make Machine 7 fail reliably)
  - Run Guardian branch/merge demo 3x to confirm stability

[Hour 24–26] PRESENTATION PREP — All
  - Update PPT slides with actual screenshots, live data
  - Record backup video of working demo (in case of live demo failure)
  - Rehearse 3-minute demo script twice

[Hour 26–28] BUFFER — bug fixes, polish, rehearsal
  - Any remaining issues from testing
  - Final rehearsal

[Hour 28–30] FINAL PREP
  - Finalize GitHub repo (README, architecture diagram, clean notebooks)
  - Prepare submission: GitHub link, demo video, project description
  - Team briefing on judge Q&A responses

[Hour 30–36] PRESENTATION
  - Demo + Q&A session
```

---

## 5. WHERE — Locations and Resources

### Physical Location
- **Round 1:** Online submission (before April 11)
- **Round 2:** Manipal Institute of Technology, Bengaluru — April 11–12, 2026

### Infrastructure
- **Databricks Workspace:** Community Edition or trial (ensure Premium tier for OBO auth, Lakebase, Zerobus)
- **Compute:** Databricks-managed clusters (no local GPU required)
- **Data:** NASA CMAPSS dataset (~30MB, pre-loaded before hackathon)
- **Version Control:** GitHub (required for final submission)

### Databricks Features Required (and tier)

| Feature | Tier Required |
|---|---|
| Lakebase | Available on all tiers (AWS GA, Azure Preview) |
| Zerobus Ingest | Databricks Premium |
| Delta Live Tables | Available on all tiers |
| MLflow 3.0 | Available on all tiers |
| Vector Search | Databricks Premium |
| Agent Framework | Databricks Premium |
| OBO Authorization | Databricks Premium |
| Unity Catalog | Databricks Premium |
| Genie AI/BI | Databricks Premium |
| Real-Time Spark Mode | Preview — check availability on workspace |

**Action required before April 1:** Confirm Premium workspace access.
HackBricks organizers typically provide workspace credentials — confirm with organizers
whether Lakebase and Zerobus are available in the provided workspace.
If not, have a Databricks Community Edition account as fallback.

---

## 6. RISKS AND MITIGATIONS

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Lakebase unavailable in provided workspace | Medium | High | Pre-test on personal workspace; fallback: use Delta Lake as operational store (loses branching demo, but architecture intact) |
| Zerobus not available in workspace tier | Medium | Medium | Fallback: Structured Streaming with Auto Loader reads from S3/ADLS; slightly more setup but same architectural story |
| Real-Time Spark Mode not in preview access | High | Low | Regular Structured Streaming still tells the story; <300ms is a bonus |
| RUL model not converging in time | Low | High | Pre-train before hackathon and bring weights; fallback to simple threshold-based RUL |
| Agent chain timing out in live demo | Medium | High | Pre-record backup demo video; script a "fast mode" with cached responses |
| Guardian branch/merge taking too long in demo | Low | Medium | Pre-branch the DB before presentation; show the result, not the wait |
| Team member unavailable | Low | High | Document every component so any team member can explain any part |

---

## 7. SUCCESS METRICS

### Round 1 (Idea Submission)
- Clear problem statement with quantified impact
- Architecture diagram that shows all Databricks components
- Databricks usage is central, not decorative

### Round 2 (Final Hackathon)
- Full pipeline runs end-to-end without manual intervention
- Guardian agent successfully branches, validates, and commits one decision live
- Genie answers 3 natural language questions correctly from real data
- GitHub repo is clean with README, architecture diagram, and runnable notebooks
- 3-minute demo tells a story a non-technical judge understands

### The Judge Questions We Must Nail
1. "Why did you use Lakebase instead of just Delta Lake?"
   → Because agents need <10ms reads on current machine state. Delta Lake is optimized for analytical scans, not sub-millisecond point lookups. Lakebase fills that gap natively.

2. "Why no Kafka?"
   → Zerobus GA'd last month. Same streaming capability, direct to Delta, no message bus overhead. We chose the right tool for 2026, not 2022.

3. "How is this different from Siemens or PTC?"
   → They're analytical. We're operational. Their agents can't act on live state because they don't have OLTP inside their analytics platform. We do.

4. "What happens if an agent makes a wrong decision?"
   → It can't commit without Guardian approval. Guardian tests on a Lakebase clone first. Every decision is in the MLflow audit trail. Wrong decisions are catchable, traceable, and reversible.

5. "How does this scale beyond 20 engines?"
   → Horizontal: more Zerobus producers write to the same Delta table. Lakebase scales vertically. Spark ML runs on the cluster, not per engine. Unity Catalog governs access as the fleet grows.
