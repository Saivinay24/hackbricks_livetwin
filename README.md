# LiveTwin — Operational AI for Physical Systems

> **HackBricks 2026 | MIT Bengaluru | April 11–12, 2026**

The first operational digital twin platform built natively on **Databricks Lakehouse** — enabling autonomous agents to monitor, diagnose, plan, and execute maintenance decisions on industrial equipment with full governance and audit trails.

Not a dashboard. Not a chatbot. Agents that **act** on physical-world data.

---

## Quick Navigation

| Document | What it covers |
|---|---|
| [docs/01_project_plan.md](docs/01_project_plan.md) | Why / What / How / When / Where — master document |
| [docs/02_architecture.md](docs/02_architecture.md) | Full system architecture, data flows, component decisions |
| [docs/03_data_strategy.md](docs/03_data_strategy.md) | Dataset, pipeline design, Delta medallion, streaming setup |
| [docs/04_agent_design.md](docs/04_agent_design.md) | All 4 agents: logic, inputs, outputs, interaction patterns |
| [docs/05_databricks_stack.md](docs/05_databricks_stack.md) | Every Databricks feature used and exactly why it's there |
| [docs/06_execution_plan.md](docs/06_execution_plan.md) | Hour-by-hour hackathon execution plan, roles, checkpoints |
| [docs/07_demo_script.md](docs/07_demo_script.md) | Full demo walkthrough + judge Q&A prep |
| **[src/](src/)** | **All implementation code** |

---

## The Problem

Industrial equipment failure costs **$170B annually**. Existing digital twin platforms are analytical only — they *describe* what happened, but cannot *act* on live equipment state. Maintenance teams make manual decisions from stale dashboards.

The gap: **5 separate systems** are required to close the loop from sensor → autonomous action:

| Needed | Old approach | LiveTwin |
|---|---|---|
| OLTP database | PostgreSQL | **Lakebase** |
| Message bus | Kafka | **Zerobus** |
| Analytics + ETL | Separate data lake | **Delta Live Tables** |
| Agent framework | Custom code | **Databricks Agent Framework + MCP** |
| Governance | Custom RBAC | **Unity Catalog + OBO Auth** |

LiveTwin collapses all five into one platform.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  NASA CMAPSS (20 turbofan engines, 21 sensors)                       │
│  Replayed as live IoT stream                                         │
└────────────────────────┬─────────────────────────────────────────────┘
                         │ Zerobus (no Kafka, direct to Delta)
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Delta Live Tables  Bronze → Silver → Gold                           │
│  (z-score anomaly flags, rolling stats, health KPIs, RUL estimates) │
└──────────┬───────────────────────────────────────────────────────────┘
           │                              │
           ▼                             ▼
┌────────────────────┐      ┌─────────────────────────────────────┐
│   LAKEBASE (OLTP)  │      │   INTELLIGENCE LAYER                │
│   Sub-10ms reads   │      │   MLflow 3.0:                       │
│   DB Branching:    │◄─────┤    • Anomaly Detector (IForest)     │
│   CREATE → TEST    │      │    • RUL Predictor (GBR + 90% CI)   │
│   → MERGE / DROP   │      │    • Failure Classifier (RF)        │
└────────┬───────────┘      │   Vector Search: 50 incidents       │
         │                  └─────────────────────────────────────┘
         └──────────────────────────────────┐
                                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  AGENT LAYER  (OBO Authorization + MLflow Tracing)                   │
│  Monitor(100ms) → Diagnosis(2.1s) → Plan(0.8s) → Guardian(2.0s)    │
└──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  INTERFACE: Genie AI/BI + SQL Dashboard                              │
│  Unity Catalog ABAC: Plant A / Plant B row-level isolation          │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
src/
├── 00_setup/
│   ├── 00_catalog_setup.py          # Unity Catalog + ABAC + row-level security
│   └── 01_lakebase_schema.sql       # 5 tables + seed data (DDL)
├── 01_data/
│   ├── 00_download_cmapss.py        # NASA dataset download + feature engineering
│   ├── 01_zerobus_producer.py       # Live stream simulation (demo + normal modes)
│   └── 02_synthetic_incidents.py    # 50 maintenance incidents + 5 safety protocols
├── 02_pipeline/
│   ├── dlt_pipeline.py              # DLT Bronze → Silver → Gold (declarative)
│   └── lakebase_sync_job.py         # Silver → Lakebase streaming sync job
├── 03_ml/
│   ├── 01_train_anomaly_detector.py # Isolation Forest + serving endpoint
│   ├── 02_train_rul_predictor.py    # GBR with 90% confidence interval + endpoint
│   └── 03_train_failure_classifier.py # Random Forest 5-class + endpoint
├── 04_vector_search/
│   └── setup_vector_search.py       # Create indexes + verify similarity queries
├── 05_agents/
│   ├── monitor_agent.py             # Real-Time Spark fleet scanner (100ms loop)
│   ├── diagnosis_agent.py           # MLflow + Gold Delta + Vector Search chain
│   ├── plan_agent.py                # Maintenance window + cost planner
│   ├── guardian_agent.py            # Branch → validate → commit (ONLY writer)
│   ├── agent_orchestrator.py        # Demo entry point + full chain test
│   └── utils/
│       ├── lakebase_client.py       # OLTP client + LakebaseBranch context manager
│       ├── mlflow_client.py         # Endpoint callers + AgentTrace
│       └── vector_search_client.py  # Similarity search for incidents + safety
├── 06_dashboard/
│   ├── dashboard_queries.sql        # 10 SQL widgets for Databricks SQL Dashboard
│   └── genie_setup.py               # Genie Space + context + sample questions
└── 07_demo/
    ├── demo_runner.py               # Full 5-scene live demo
    └── blocked_demo.py              # Guardian BLOCKED scenario (cost violation)
```

---

## Setup Guide

### Prerequisites
- Databricks Premium workspace (Lakebase, Zerobus, Agent Framework)
- DBR 15.4+ (Zerobus native format reader)
- Unity Catalog enabled
- Lakebase endpoint provisioned
- Vector Search endpoint created

### Databricks Secrets

```bash
databricks secrets create-scope livetwin
databricks secrets put --scope livetwin --key lakebase_host
databricks secrets put --scope livetwin --key lakebase_password
```

### Execution Order

```
1. src/00_setup/00_catalog_setup.py
2. src/00_setup/01_lakebase_schema.sql    (run against Lakebase connection)
3. src/01_data/00_download_cmapss.py
4. src/01_data/02_synthetic_incidents.py
5. Deploy src/02_pipeline/dlt_pipeline.py as DLT Pipeline (Continuous)
6. Start src/02_pipeline/lakebase_sync_job.py as Databricks Job (Continuous)
7. src/03_ml/01_train_anomaly_detector.py
8. src/03_ml/02_train_rul_predictor.py
9. src/03_ml/03_train_failure_classifier.py
10. src/04_vector_search/setup_vector_search.py
11. src/06_dashboard/genie_setup.py
12. src/07_demo/demo_runner.py  (widget: mode=demo)
```

---

## The Demo (5 Minutes)

| Time | Scene | What happens |
|------|-------|--------------|
| 0:00 | Setup | Dashboard open — all 20 engines green |
| 0:30 | Scene 1 | Zerobus stream starts, Engine 15 turns red |
| 1:00 | Scene 2 | Diagnosis: RUL 23.4h, bearing_degradation 87%, 94% match to INC-2019-0042 |
| 1:30 | Scene 3 | Guardian: CREATE BRANCH → INSERT → verify → MERGE → order #1042 committed |
| 2:00 | Scene 4 | OBO demo + Genie: "How much did LiveTwin save this month?" |
| 2:30 | Impact | 10 seconds vs 2–6 hours, $170B problem |

**Key demo moment:** The Guardian branches the production Lakebase database, runs a test INSERT on the clone, verifies the write, then merges to main — live, in under 2 seconds. No other platform can show this.

---

## Key Q&A

**Why Lakebase?** Delta is OLAP. Point lookups on billions of rows are slow even with Z-ordering. Lakebase: sub-10ms indexed reads. Critically: database branching — no Delta feature gives copy-on-write DB clones for safe agent writes.

**Why no Kafka?** Zerobus GA'd February 2026. Direct Delta write, same semantics, zero cluster overhead. Every other team uses Kafka. We don't. That's one question a Databricks engineer can ask that shows platform depth.

**What stops wrong decisions?** The Guardian tests on a branch first. 4 pre-validation checks before touching production. If anything fails — branch is dropped. Production never modified.

**How does it scale?** Zerobus: 10 GB/sec. DLT: horizontal. Lakebase: >10k QPS. Agent Framework: parallel chains. Unity Catalog ABAC: multi-tenant without code changes.

---

## Dataset

NASA CMAPSS Turbofan Engine Degradation Simulation Dataset — public domain.
A. Saxena and K. Goebel, NASA Ames Prognostics Data Repository.

---

*Built for HackBricks 2026 — Databricks Lakehouse Hackathon, MIT Bengaluru*
