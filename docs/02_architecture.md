# LiveTwin — System Architecture

---

## Architecture Philosophy

**Single-platform operational AI.** Every component is justified by necessity, not completeness.
If a Databricks feature appears here, it's because removing it would break the system or require a separate platform.

---

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PHYSICAL / SENSOR LAYER                            │
│  NASA CMAPSS dataset replayed as live stream (simulates 20 turbine engines) │
│  21 sensors per engine: temp, pressure, RPM, fan speed, vibration proxies   │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ Zerobus Ingest (no Kafka, direct to Delta)
                                │ Sub-5-second latency, 10 GB/sec capable
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION + PIPELINE LAYER                          │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │              Delta Live Tables (Lakeflow Declarative Pipelines)       │  │
│  │                                                                       │  │
│  │  Bronze ──────► Silver ──────────────────────────────► Gold          │  │
│  │  Raw, immutable  Cleaned + validated    Per-engine health KPIs        │  │
│  │  append-only     z-score deviations     RUL estimates                 │  │
│  │                  anomaly flags          failure probabilities          │  │
│  │                  schema validated       maintenance cost projections   │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │                                           │
│                    ┌────────────▼────────────┐                             │
│                    │   Lakehouse Monitoring   │                             │
│                    │   - Null rate tracking   │                             │
│                    │   - Distribution drift   │                             │
│                    │   - Schema drift alerts  │                             │
│                    └─────────────────────────┘                             │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │
          ┌────────────────┴─────────────────┐
          ▼                                  ▼
┌─────────────────────┐          ┌───────────────────────────────────────────┐
│  LAKEBASE (OLTP)    │          │         INTELLIGENCE LAYER                │
│  PostgreSQL-compat  │          │                                           │
│  Sub-10ms latency   │          │  ┌─────────────────────────────────────┐ │
│                     │          │  │ MLflow 3.0                          │ │
│  Tables:            │◄─────────┤  │  Model 1: Anomaly Detector (IForest)│ │
│  - assets           │          │  │  Model 2: RUL Predictor (GBR/LSTM)  │ │
│  - sensor_readings  │          │  │  Model 3: Failure Mode Classifier   │ │
│  - maintenance_orders          │  │  All: versioned, monitored, served  │ │
│  - agent_decisions  │          │  └─────────────────────────────────────┘ │
│  - op_schedule      │          │                                           │
│                     │          │  ┌─────────────────────────────────────┐ │
│  DB Branching:      │          │  │ Vector Search (Rust, billion-scale) │ │
│  Clone → Test →     │          │  │  Index: maintenance reports +       │ │
│  Merge/Drop         │          │  │  equipment manuals + failure docs   │ │
└──────────┬──────────┘          │  │  Used by: Diagnosis Agent (RAG)     │ │
           │                     │  └─────────────────────────────────────┘ │
           │                     └───────────────────────────────────────────┘
           │                                       │
           └───────────────────┬───────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AGENT LAYER                                       │
│              Databricks Agent Framework + MCP + OBO Authorization          │
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │   MONITOR    │───►│  DIAGNOSIS   │───►│    PLAN      │───►│ GUARDIAN │ │
│  │    AGENT     │    │    AGENT     │    │    AGENT     │    │  AGENT   │ │
│  │              │    │              │    │              │    │          │ │
│  │ Polls        │    │ Queries:     │    │ Reads:       │    │ Branches │ │
│  │ Lakebase     │    │ - Lakebase   │    │ - Lakebase   │    │ Lakebase │ │
│  │ every 100ms  │    │ - Gold DLT   │    │   schedule   │    │ DB       │ │
│  │              │    │ - MLflow RUL │    │ - Cost model │    │ Validates│ │
│  │ Real-Time    │    │ - Vector     │    │ - Failure    │    │ OBO auth │ │
│  │ Spark Mode   │    │   Search     │    │   urgency    │    │ Commits  │ │
│  │ (<300ms)     │    │              │    │              │    │ or BLOCKS│ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘ │
│                                                                             │
│  MLflow Tracing: every agent step logged (input, output, latency, model)   │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOVERNANCE LAYER                                    │
│                                                                             │
│  Unity Catalog                                                              │
│  - All tables, models, functions registered with full lineage               │
│  - OBO: agent inherits operator's exact permissions (row/column level)      │
│  - ABAC: Plant A operators cannot see Plant B — automatic, not coded        │
│  - Model registry: governs which RUL version is in production               │
│                                                                             │
│  Lineage chain:                                                             │
│  sensor → Zerobus → Bronze → Silver → Gold → MLflow model → agent          │
│  decision → Lakebase maintenance_order → (traced end-to-end)               │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INTERFACE LAYER                                      │
│                                                                             │
│  Genie AI/BI (compound AI, learns from your schema):                        │
│  "Why did Machine 7 fail?" → chains queries → narrative answer              │
│  "How much did LiveTwin save this month?" → cost calculation from Lakebase  │
│  "Which engines need attention in the next 48 hours?" → RUL from Gold       │
│                                                                             │
│  Databricks SQL Dashboard:                                                  │
│  - Fleet health heatmap (color: green/yellow/red by RUL)                   │
│  - Agent decision timeline (annotated with Guardian verdicts)               │
│  - Predicted vs actual RUL curve (model validation visual)                 │
│  - Cost savings counter ($X prevented this session)                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Decision Log

Every architectural decision is justified. No component is here for completeness.

### Why Zerobus and not Kafka → Structured Streaming?

Kafka → Structured Streaming is the standard pattern, established in 2019.
Zerobus GA'd February 2026. It eliminates the message bus layer entirely:
- No Kafka cluster to provision, monitor, or pay for during a hackathon
- Same streaming semantics, direct to Delta
- Sub-5-second latency is sufficient for predictive maintenance (failure timelines are hours, not milliseconds)
- Single-sink architecture reduces integration surface by 1 platform

**Judge impact:** Every other team uses Kafka. You don't. That's one correct question
from a Databricks engineer that shows platform depth.

---

### Why Lakebase and not just Delta Lake for everything?

Delta Lake is an OLAP store — optimized for large analytical scans.
When an agent needs the current sensor reading for Machine 7, it's a point lookup:
`SELECT * FROM sensor_readings WHERE asset_id = 7 ORDER BY ts DESC LIMIT 1`

On a Delta table with billions of rows, this scan is slow even with Z-ordering.
On Lakebase, this is a sub-10ms indexed primary key lookup.

More importantly: Lakebase enables **database branching** — instantaneous copy-on-write clones.
No Delta Lake or Delta table feature provides this. It's the key to the Guardian's safe execution model.

**Fallback if unavailable:** Delta Lake with partitioning by `asset_id` and `date`.
The Guardian branching demo is lost but the rest of the architecture is intact.

---

### Why OBO Authorization and not a service account?

Service accounts are the lazy solution. They require developers to manually implement
row-level security in agent code, which is fragile and auditable only if someone
writes the logging code.

OBO authorization means:
- An operator triggers the Monitor Agent → Agent executes with exactly the operator's permissions
- If the operator can't see Plant B data, neither can any agent they trigger
- This is not code — it's infrastructure

**The governance story in one demo step:**
Log in as "Plant A Operator" → trigger agent → show Unity Catalog audit log →
the agent only touched Plant A assets. No custom filtering code anywhere.

---

### Why four agents instead of one?

Single-agent systems are simpler to build but fail under judge questioning:
"What happens if the diagnosis is wrong but the plan executes anyway?"

Separation of concerns:
- Monitor is stateless and fast — runs continuously at low cost
- Diagnosis is expensive (MLflow endpoint, Vector Search) — only called on anomaly
- Plan is read-only on operational data — safe to run independently
- Guardian is the only agent with write access to Lakebase

This design means a failure in Diagnosis never causes a false write to Lakebase.
The Guardian is the only write path and it validates independently.

---

### Why Real-Time Spark Mode?

The Monitor Agent needs to scan anomaly scores across all 20 engines simultaneously
(scalable to 10,000 in production). With standard micro-batch Structured Streaming,
this scan has 1–5 second latency per cycle.

Real-Time Mode delivers <300ms p99 via non-blocking shuffle + concurrent stage scheduler.
For a fleet monitoring application, the difference between "5 seconds" and "300ms"
is the difference between "near real-time" and "real-time" in the demo.

---

## Data Flow Timing

| Step | Component | Latency |
|---|---|---|
| Sensor write → Delta Bronze | Zerobus | <5 seconds |
| Bronze → Silver → Gold (DLT) | Lakeflow DLT | ~30 seconds (continuous) |
| Silver → Lakebase sensor_readings | Spark write | ~5 seconds (micro-batch) |
| Agent anomaly scan (fleet) | Real-Time Spark Mode | <300ms |
| Diagnosis Agent (MLflow + Vector Search) | MLflow endpoint + Vector Search | 1–3 seconds |
| Plan Agent | Lakebase read | <100ms |
| Guardian (branch, validate, commit) | Lakebase | 2–5 seconds |
| End-to-end: sensor → maintenance order | Full chain | ~45–60 seconds |

**Note:** The 45–60 second end-to-end time is the right tradeoff for predictive maintenance.
Failure timelines are hours, not seconds. The agent doesn't need to be faster than human cognition —
it needs to be available 24/7 and consistent across 10,000 machines simultaneously.

---

## Scalability Architecture

The demo runs on 20 engines. Here's why the architecture scales without redesign:

| Dimension | Demo Scale | Production Scale | What Changes |
|---|---|---|---|
| Engines | 20 | 10,000+ | Nothing — Zerobus handles 10 GB/sec, DLT scales horizontally |
| Sensor events/sec | ~20/sec | 200,000+/sec | Zerobus throughput, cluster auto-scaling |
| Historical data | 30 MB (CMAPSS) | 5+ TB | Delta Lake partitioning + Z-ordering on asset_id + date |
| Agents running | 4 (one chain) | Parallel chains per anomaly | Agent Framework handles concurrency natively |
| Users / plants | 1 plant | 100+ plants | OBO + ABAC in Unity Catalog — no code change |
| Models in production | 3 models | 30+ (one per failure mode per asset type) | Unity Catalog model registry, MLflow versioning |
