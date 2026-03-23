# LiveTwin — Hackathon Execution Plan

---

## Core Principle

**Nothing is written for the first time at the venue.**

Every component should be understood, designed, and partially prototyped
before April 11. The hackathon is for integration, testing, and polish —
not for figuring out how Lakebase works.

---

## Team Structure

| Role | Responsibilities | Skills Needed |
|---|---|---|
| **Lead (You)** | Agent architecture, Guardian implementation, OBO auth, integration, presentation | Python, Databricks Agent Framework, system design |
| **Data Engineer** | Zerobus pipeline, DLT Bronze→Silver→Gold, Lakebase schema + writes | PySpark, SQL, Databricks DLT |
| **ML Engineer** | Model training, MLflow, Vector Search, Genie setup | scikit-learn / XGBoost, MLflow, Databricks ML |

2-person variant: Lead takes Agents + ML. Data Engineer takes Pipeline + Lakebase.
Genie and Dashboard are the last things to build — can be cut if time is short.

---

## Pre-Hackathon Checklist (Now → April 10)

### Week 1 (March 19–25)
- [ ] Read Lakebase docs end-to-end (databricks.com/product/lakebase)
- [ ] Read Zerobus docs end-to-end
- [ ] Read Agent Framework + OBO Auth docs
- [ ] Download NASA CMAPSS dataset, understand schema
- [ ] Identify Engine 15 FD001 as the demo failure engine (pre-analyze degradation curve)
- [ ] Set up Databricks workspace with Premium tier access

### Week 2 (March 26 – April 1)
- [ ] Confirm team finalization
- [ ] Complete and submit Round 1 PPT
- [ ] Run Zerobus "hello world" — can you produce a message and read it in Delta?
- [ ] Provision Lakebase instance — can you connect via Python psycopg2?
- [ ] Create Lakebase schema (assets, sensor_readings, maintenance_orders, agent_decisions)
- [ ] Load CMAPSS FD001 into Databricks (DBFS or Unity Catalog volume)

### Week 3 (April 2–7)
- [ ] Build and test DLT pipeline (Bronze → Silver → Gold) on CMAPSS data
- [ ] Train RUL model on Gold layer → register in MLflow → test endpoint
- [ ] Train Anomaly Detector → register → test endpoint
- [ ] Train Failure Mode Classifier → register → test endpoint
- [ ] Build Zerobus replay producer (CMAPSS → Zerobus → Delta Bronze)
- [ ] Write Monitor Agent (basic version: poll Lakebase, threshold check)

### Week 4 (April 8–10)
- [ ] Build Diagnosis Agent → test against pre-loaded Gold data
- [ ] Build Plan Agent → test against seeded operational_schedule
- [ ] Build Guardian Agent (basic version: checks only, no branching yet)
- [ ] Add Lakebase branching to Guardian → test branch/merge cycle
- [ ] Run 4-agent chain end-to-end on injected anomaly → verify maintenance order appears
- [ ] Set up Vector Search index with 50 synthetic incidents
- [ ] Configure OBO authorization (3 user roles)
- [ ] Push everything to GitHub with clean structure

**Goal by end of April 10:**
The entire system runs on a pre-loaded dataset.
Engine 15's failure scenario works cleanly end-to-end.
Guardian branching works.
All you need to do at the venue is connect live streaming + polish the demo.

---

## Day 1 — April 11, 2026 (Build Day)

### Hour 0–1: Setup and Sync (All)
```
☐ Connect to venue WiFi, confirm Databricks workspace access
☐ Clone GitHub repo onto venue machines
☐ Verify: all clusters start correctly
☐ Verify: Lakebase connection works from venue network
☐ Verify: pre-loaded CMAPSS data is in Gold layer
☐ Split workstreams: each person takes their lane
☐ Set checkpoint times: Hour 6, Hour 12, Hour 16
```

### Hour 1–4: Pipeline Lane (Data Engineer)
```
☐ Start Zerobus streaming producer (CMAPSS replay)
☐ Verify: Bronze table receiving rows
☐ Start DLT pipeline
☐ Verify: Silver table has anomaly_score column populated
☐ Verify: Gold table updating every ~30 seconds
☐ Start Lakebase write connector (Silver → sensor_readings)
☐ Verify: Lakebase sensor_readings has current readings
☐ CHECKPOINT: show Lead one Gold row and one Lakebase row — both correct
```

### Hour 1–4: Models Lane (ML Engineer — parallel)
```
☐ Trigger MLflow training run for all 3 models (use pre-prepared notebooks)
☐ Monitor training — Anomaly Detector (~15min), RUL (~45min), Classifier (~20min)
☐ Register all models in Unity Catalog model registry
☐ Deploy all 3 as serving endpoints
☐ Test: POST request to each endpoint, verify JSON response
☐ CHECKPOINT: show Lead three curl responses — all returning predictions
```

### Hour 4–8: Vector Search + Agents Setup (Lead + ML Engineer)
```
☐ Create Vector Search endpoint (if not pre-configured)
☐ Load 50 synthetic maintenance reports into UC table
☐ Create Vector Search index on the table
☐ Verify: similarity query returns 3 relevant results
☐ Configure Databricks Agent Framework project
☐ Implement Monitor Agent with Lakebase connection
☐ Test Monitor Agent: inject anomaly manually → verify detection fires
```

### Hour 8–12: Diagnosis + Plan Agents (Lead)
```
☐ Implement Diagnosis Agent with all 4 data sources:
    ☐ Lakebase current state query
    ☐ Gold Delta history query
    ☐ MLflow RUL endpoint call
    ☐ Vector Search similarity call
☐ Test Diagnosis Agent standalone: inject anomaly → verify full diagnosis output
☐ Implement Plan Agent with Lakebase schedule query
☐ Test Plan Agent: verify proposed window is in the future and window is available
☐ Wire Monitor → Diagnosis → Plan chain
☐ CHECKPOINT: Run Monitor→Diagnosis→Plan chain, verify output JSON from Plan Agent
```

### Hour 12–16: Guardian Agent (Lead)
```
☐ Implement Guardian Agent pre-validation checks (cost, window, conflict, safety)
☐ Implement Lakebase branching: CREATE BRANCH, test INSERT, MERGE
☐ Test: each Guardian check individually
☐ Test: full Guardian with all checks on happy path → verify APPROVED
☐ Test: trigger Guardian block (set cost limit to ₹0) → verify BLOCKED, no DB write
☐ Test: full 4-agent chain end-to-end with Engine 15 failure scenario
☐ Verify: maintenance order appears in Lakebase after Guardian approval
☐ Verify: MLflow Tracing shows all 4 agent spans with correct data

CHECKPOINT @ HOUR 16 — CRITICAL GATE
If this passes, the project is complete. Everything after is polish.
"Full pipeline: Zerobus → Delta → Agents → Guardian → Lakebase"
If this fails: apply risk mitigation (see 01_project_plan.md)
```

---

## Day 2 — April 12, 2026 (Polish + Present)

### Hour 17–19: Interface (All)
```
☐ Set up Genie AI/BI space on Gold + Lakebase tables
☐ Configure Genie domain context (table descriptions, terminology)
☐ Test 5 pre-configured Genie questions — all must return correct answers
☐ Build Databricks SQL Dashboard:
    ☐ Fleet health heatmap
    ☐ Agent decision timeline
    ☐ RUL gauge for Engine 15
    ☐ Cost savings counter
☐ Test dashboard updates live when new sensor data arrives
```

### Hour 19–21: Governance Demo Setup (Lead)
```
☐ Configure 3 user accounts in Databricks workspace:
    - plant_a_operator (Plant A only, limited cost auth)
    - maintenance_supervisor (all plants, high cost auth)
    - read_only_analyst (Gold only, no writes)
☐ Test OBO: log in as plant_a_operator → trigger agent chain → verify Plant B assets invisible
☐ Test Unity Catalog ABAC: verify lineage graph shows full sensor → maintenance order chain
☐ Export MLflow Trace screenshot for Plant A operator run (backup in case live trace is slow)
```

### Hour 21–24: Integration Testing (All)
```
☐ Run full demo scenario 3 times without interruption:
    - Fresh Zerobus stream
    - Engine 15 anomaly detected
    - 4-agent chain completes
    - Maintenance order in Lakebase
    - Dashboard updates
    - Genie answers "which engines need attention?"
☐ Run Guardian BLOCK scenario once (low cost limit)
☐ Run OBO role-switch demo once
☐ Time each run — target: under 3 minutes for the fast demo, 5 minutes for full
☐ Fix any timing bugs, race conditions, or UI glitches
```

### Hour 24–26: Presentation Prep (All)
```
☐ Take final screenshots of all demo states (backup for live demo failure)
☐ Record 3-minute backup video of working demo
☐ Update PPT slides with real screenshots and real data numbers
☐ Write GitHub README with: project description, architecture diagram, setup instructions
☐ Clean up notebooks (remove debug cells, add markdown explanations)
☐ Final submission checklist:
    ☐ GitHub repository link
    ☐ Working prototype
    ☐ Short project description (for submission form)
    ☐ Technologies list (Lakebase, Zerobus, DLT, MLflow 3.0, Vector Search, Agent Framework, OBO, Unity Catalog, Genie)
```

### Hour 26–28: Rehearsal
```
☐ Full demo rehearsal × 2 (timed)
☐ Each team member can explain any component
☐ Prepare answers for the 5 judge questions (see 01_project_plan.md)
☐ Decide: who presents, who runs the live demo, who answers technical questions
```

### Hour 28–36: Buffer + Presentation
```
☐ Final bug fixes if any (max 1 hour)
☐ Rest
☐ Presentation + demo
```

---

## Dependency Graph

```
NASA CMAPSS dataset loaded
        │
        ├─── DLT Pipeline (Bronze→Silver→Gold) ──────► Models trained
        │                                                     │
        ├─── Lakebase schema + write connector               │
        │                                                     │
        └─── Zerobus streaming producer                      │
                        │                                     │
                        ▼                                     ▼
              Monitor Agent ───────────────────► MLflow endpoints deployed
                        │                                     │
                        ▼                                     ▼
              Diagnosis Agent ◄─────────────── Vector Search index
                        │
                        ▼
              Plan Agent ◄──────────── Lakebase operational_schedule seeded
                        │
                        ▼
              Guardian Agent ◄──────── OBO authorization configured
                        │
                        ▼
              Lakebase maintenance_orders write
                        │
                        ▼
              Dashboard + Genie (built last, depends on all above)
```

**Critical path:** DLT Pipeline → Gold layer → Models trained → Agent chain

If Models are delayed, every agent depending on MLflow endpoints is blocked.
Pre-train models before the hackathon and bring the weights.

---

## Time Budget Summary

| Phase | Hours | Confidence |
|---|---|---|
| Setup | 1 | High |
| Pipeline (Zerobus + DLT + Lakebase) | 3 | High (pre-built skeleton) |
| Models (train + serve all 3) | 4 | High (pre-built notebooks) |
| Vector Search | 1 | High |
| Monitor + Diagnosis Agents | 4 | Medium (new code at venue) |
| Plan + Guardian Agents | 4 | Medium |
| Integration testing (Day 1) | 1 | High |
| Interface (Genie + Dashboard) | 2 | High |
| Governance demo setup | 2 | High |
| Integration testing (Day 2) | 3 | Medium |
| Presentation prep + rehearsal | 4 | High |
| Buffer | 7 | — |
| **Total** | **36** | — |

---

## Communication Cadence at Venue

- Every 4 hours: 10-minute standup — what's done, what's blocked, what's the risk
- Hour 16 gate: go/no-go on full pipeline — adjust scope if needed
- No solo all-nighters: working tired produces bugs that cost more time to fix than sleep
- If behind at Hour 16: cut Genie + Dashboard (keep core agent demo, use raw Lakebase queries for demo)
- If significantly behind at Hour 16: cut to 2-agent demo (Monitor + Guardian only)
  and focus energy on making those two agents flawless rather than 4 agents broken
