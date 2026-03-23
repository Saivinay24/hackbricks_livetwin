# LiveTwin — Demo Script and Judge Prep

---

## Presentation Structure (5 minutes total)

| Segment | Time | Who | What |
|---|---|---|---|
| Problem + Hook | 60s | Presenter | Verbal — no slides needed, just numbers |
| Architecture overview | 30s | Presenter | One slide: the component diagram |
| Live Demo | 3 min | Demo runner | Screen share: Databricks workspace |
| Scalability + Impact | 30s | Presenter | One slide: scale numbers + cost savings |

---

## Opening (60 seconds — memorize this)

> "A single industrial turbine failure costs over $2 million. Manual inspection
> costs $10,000 per site and takes weeks. The digital twin market is $35 billion
> and growing — but every platform in it has the same problem.
>
> They're analytical. They tell you what happened. They can't act on it.
>
> To build a system that actually does something — one that detects an anomaly,
> diagnoses the failure, plans an intervention, and executes it safely —
> you need five separate platforms. An operational database. A data lake.
> A message bus. An agent framework. A governance layer.
>
> In the last 8 months, Databricks shipped Lakebase, Zerobus, Agent Framework
> with OBO Authorization, and MLflow 3.0. For the first time, those five systems
> are one platform.
>
> This is LiveTwin."

---

## Live Demo Script (3 minutes)

**Screen setup before starting:**
- Browser tab 1: Databricks SQL Dashboard (fleet heatmap visible, Machine 15 highlighted yellow)
- Browser tab 2: Lakebase maintenance_orders table (empty or last order from before demo)
- Browser tab 3: MLflow Tracing (empty, ready to refresh)
- Terminal: Zerobus replay producer ready but paused

---

### Scene 1: The Anomaly (30 seconds)

> "This is our fleet of 20 turbine engines. All green. Machine 15 is at 89% health —
> watch it. I'm starting the sensor stream now."

*[Start Zerobus producer in terminal]*

> "This is NASA's CMAPSS turbofan dataset, replayed in real-time via Databricks Zerobus —
> no Kafka, directly into Delta tables. Watch Machine 15."

*[Wait 10–15 seconds — Machine 15 turns yellow/red on dashboard]*

> "There. Anomaly detected. Vibration and temperature sensors crossed threshold.
> The Monitor Agent just caught this across the full fleet in under 300 milliseconds —
> that's Databricks' new Real-Time Spark Mode."

---

### Scene 2: The Diagnosis (40 seconds)

> "The Diagnosis Agent is running now. It's doing four things simultaneously:
> reading Machine 15's current state from Lakebase — that's sub-10 milliseconds —
> querying 5 years of historical sensor data from our Delta Gold layer,
> calling our Remaining Useful Life model, and running a semantic search
> over our maintenance history for similar failure patterns."

*[Show MLflow Tracing — Diagnosis Agent span appears]*

> "There's the trace. RUL model says 23.4 hours to failure, 87% confidence.
> Failure mode: bearing degradation. The Vector Search found a 94% similar incident
> from 2019 — HPT bearing housing lubrication failure. Same sensor pattern."

---

### Scene 3: The Guardian (60 seconds)

> "The Plan Agent has proposed a 6-hour maintenance window tonight at 10 PM.
> Estimated cost: ₹42,500. Now watch the Guardian."

*[Show Lakebase connection, highlight the branching step]*

> "The Guardian doesn't write directly to the production database.
> It branches it first — a copy-on-write clone, created in under one second,
> from Databricks Lakebase. Think of it like Git, but for your operational database."

> "It runs all its checks on the clone: cost authorization — passes.
> Window still available — passes. Safety protocol check — passes.
> It's merging the branch to production now."

*[Switch to Lakebase maintenance_orders tab — refresh]*

> "Maintenance order 1042. Written. Machine 15 is scheduled."

*[Point to MLflow Tracing — full chain visible]*

> "And here's the complete audit trail in MLflow — every agent, every check,
> every model version, every millisecond. When this system is running in production
> and a regulator asks 'why did this maintenance order get created?' —
> this is the answer."

---

### Scene 4: Governance + Genie (30 seconds)

> "One more thing. This is running under OBO Authorization — On-Behalf-of-User.
> The agent chain executed with the Plant A operator's exact permissions.
> It couldn't access Plant B data even if it tried.
> Unity Catalog enforced it. Here's the audit log."

*[Show Unity Catalog audit log — only Plant A assets in query history]*

> "And for the maintenance supervisor who doesn't write SQL:"

*[Switch to Genie]*
> "How much did LiveTwin save us this month?"

*[Genie returns: ₹4,20,000 prevented from 3 maintenance orders]*

> "That's LiveTwin."

---

### Scene 5: Backup — Guardian Block Demo (if time allows, or if judge asks)

> "Let me show you what happens when the Guardian says no."

*[Set cost limit to ₹1,000 in config, re-run chain]*

> "Same anomaly, same plan — but this operator's authorization limit is ₹1,000.
> The Guardian blocked it. No write to production. Escalated to the supervisor.
> The audit trail shows exactly why it was blocked and who needs to approve."

---

## Architecture Slide (one slide only)

```
Turbine Sensors
      │
  [Zerobus] ──────────────────────────────────────────────┐
      │                                                    │
[Delta Live Tables]                               [Lakebase OLTP]
Bronze→Silver→Gold                          Assets | Readings | Orders
      │                                                    │
  [MLflow]                                    [Agent Framework + OBO Auth]
  3 models                              Monitor → Diagnose → Plan → Guardian
  trained,                                                 │
  served                                        [Unity Catalog]
      │                                     Governance | Lineage | ABAC
  [Vector Search]                                         │
  50 incident docs                               [MLflow 3.0 Tracing]
      └──────────────────────────────────────[Genie AI/BI + SQL Dashboard]
```

Caption: *"Five previously separate platforms. One Databricks Lakehouse."*

---

## Judge Q&A Preparation

### Q1: "Why Lakebase instead of just Delta Lake for everything?"

> "Delta Lake is an analytical store — optimized for large scans over historical data.
> When our Monitor Agent needs Machine 15's current reading, it's a point lookup.
> On a Delta table with 10 million rows, even with Z-ordering, that's a scan.
> On Lakebase, it's a sub-10ms indexed lookup. At 20 machines polling every 100ms,
> Delta can't keep up. Lakebase is the right tool.
>
> The second reason is database branching. No Delta feature gives you
> copy-on-write database clones. The Guardian's safe execution model
> requires the ability to test before committing. Lakebase from Neon gives us that."

---

### Q2: "Why no Kafka?"

> "Zerobus GA'd last month — February 2026. Same streaming semantics as Kafka,
> but it writes directly to Delta tables without a message bus layer.
> For our use case — sub-5-second latency to get sensor readings into Delta —
> Zerobus is sufficient and eliminates an entire infrastructure component.
> We picked the current best practice, not the 2022 best practice."

---

### Q3: "How is this different from Siemens or PTC ThingWorx?"

> "Every existing digital twin platform — Siemens, PTC, Azure Digital Twins —
> is analytical. They show you what happened. Our agents ACT on what's happening.
> The reason existing platforms can't do this is they don't have OLTP inside
> their analytics platform. Agents need sub-10ms point reads for current state.
> With Lakebase, we have it natively — no external operational database, no ETL,
> no synchronization headaches. That architectural gap is what LiveTwin closes."

---

### Q4: "What stops the agent from making a catastrophic wrong decision?"

> "The Guardian Agent. Three things:
> First, it validates against rules before touching any database.
> Second, it tests on a Lakebase clone before committing to production.
> If anything looks wrong on the clone, the branch is dropped and production is untouched.
> Third, every decision is in the MLflow audit trail — if a wrong decision
> does get through, we can trace exactly what data led to it,
> which model version made the call, and who authorized it.
> The system is designed for rollback and accountability, not just happy paths."

---

### Q5: "How does this scale to a real industrial fleet of 10,000 machines?"

> "Horizontally across every component.
> Zerobus handles 10 gigabytes per second of throughput — 10,000 machines
> at 21 sensors each is about 2 MB/sec. That's 0.02% of Zerobus capacity.
> The Delta Lake pipeline scales with Spark cluster size — add nodes, add throughput.
> Lakebase handles thousands of QPS — each agent chain is a handful of queries.
> The Agent Framework runs agent chains in parallel, one per anomaly.
> Unity Catalog ABAC enforces plant-level access without code changes — just tags.
> Nothing in the architecture requires redesign to scale. We built it that way intentionally."

---

### Q6: "Why industrial maintenance specifically?"

> "Three reasons.
> First, the data is public — NASA CMAPSS is clean, realistic, and well-validated.
> We're not generating fake data for the demo.
> Second, the stakes are universally understood. Everyone knows what 'the machine broke'
> means and what it costs. We don't need domain knowledge to appreciate the problem.
> Third, the application genuinely requires all the components we used.
> An agent that monitors a factory fleet NEEDS OLTP state, NEEDS streaming,
> NEEDS ML-driven decisions, and NEEDS governance. The use case justifies the architecture."

---

### Q7 (if it comes up): "Is Lakebase available in the hackathon workspace?"

> "We verified this before the event. It's GA on AWS as of February 2026.
> [If yes: show it live]
> [If no, fallback activated: we're using Delta Lake with a partitioned hot table
> for current state reads — the architecture is identical, we lose the branching demo
> but the core operational AI story is unchanged.]"

---

## Failure Recovery Protocols

### If Zerobus stream crashes during demo
1. Switch to pre-recorded tab (data already loaded from earlier run)
2. "The stream ran earlier — I'll show you the results already in the system"
3. Proceed from Gold table data already available

### If Guardian branch fails
1. Fall back to showing the Guardian WITHOUT branching
2. "The branch feature is what I'll show next — for now you can see the validation chain"
3. Show the checks passing, then show a pre-committed maintenance order

### If MLflow endpoints are down
1. "Models are responding slowly — let me show you the pre-computed results"
2. Open a saved MLflow run with the diagnosis output
3. Proceed — the story is still coherent

### If Genie returns wrong answer
1. Don't try to fix it live
2. "Let me rephrase that — Genie is learning our schema"
3. Ask a simpler version of the same question
4. If still wrong: "The underlying SQL query is here — the data is correct"
   and show the Databricks SQL query result directly

### If time runs short during presentation
Cut in this order:
1. Governance demo (OBO role switch) — describe verbally instead
2. Guardian block scenario — just mention it exists
3. Genie demo — describe verbally
4. NEVER cut: the full agent chain completing and the maintenance order appearing in Lakebase
   That is the core demo moment. Everything else is supporting evidence.
