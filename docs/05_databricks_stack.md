# LiveTwin — Databricks Stack Justification

Every feature used. Every reason documented. No feature is here for decoration.

---

## Feature Map

| Feature | Used For | Without It |
|---|---|---|
| Zerobus Ingest | Streaming sensor data directly to Delta | Need Kafka cluster setup (+2 hrs, extra infra) |
| Lakebase | Agent operational state, maintenance orders, branching | Need external PostgreSQL (+1 platform, no branching) |
| Delta Live Tables | Bronze→Silver→Gold pipeline with quality checks | Manual Spark jobs, no lineage, no quality enforcement |
| Structured Streaming (Real-Time Mode) | Fleet-wide anomaly scanning <300ms | 1–5s latency, slow fleet scan |
| MLflow 3.0 | Model training, versioning, serving, agent tracing | No reproducibility, no audit trail |
| Vector Search (Storage-Optimized) | Semantic similarity over maintenance history | Text search or external vector DB |
| Agent Framework + MCP | Multi-agent orchestration | Manual chaining code, no tool governance |
| OBO Authorization | Agents run as user's permissions | Service account with blanket access, no real governance |
| Unity Catalog (ABAC) | Multi-plant access control, data lineage | Manual row-level filters in every query |
| Lakehouse Monitoring | Data + model drift detection | Blind to silent failures in data quality |
| Genie AI/BI | Natural language analytics for operators | SQL knowledge required to query the system |
| Databricks SQL Dashboard | Fleet health visualization | External BI tool (Tableau, Grafana) |
| Delta Lake | Historical sensor data (Bronze/Silver/Gold) | Foundation of the entire pipeline |

---

## Detailed Feature Justifications

### 1. Zerobus Ingest
**Version:** GA February 2026
**Source:** databricks.com/blog/announcing-general-availability-zerobus-ingest-part-lakeflow-connect

The standard pattern for streaming to Databricks is:
`Producer → Kafka topic → Structured Streaming consumer → Delta table`

Zerobus eliminates the Kafka layer:
`Producer → Zerobus endpoint → Delta table (directly)`

For LiveTwin this means:
- Sensor replay code writes directly to a Zerobus endpoint
- No Kafka cluster provisioning, no topic management, no consumer group configuration
- Sub-5-second end-to-end latency is sufficient (equipment failure timelines are hours)
- During the hackathon: saves ~2 hours of Kafka setup and debugging

**The judge question this answers:**
"Why no Kafka?" → "Because Databricks shipped Zerobus last month. Same streaming semantics, minus an entire infrastructure component. We chose the right stack for 2026."

---

### 2. Lakebase
**Version:** GA on AWS February 2026, Public Preview on Azure
**Source:** databricks.com/blog/databricks-lakebase-generally-available
**Built on:** Neon ($1B acquisition, May 2025) — PostgreSQL with copy-on-write branching

**Two separate justifications:**

**a) OLTP for agent operational reads:**
When Monitor Agent polls for current machine state, it runs:
`SELECT * FROM sensor_readings WHERE asset_id = 15 ORDER BY ts DESC LIMIT 1`

On a Delta table with 10M rows, this is a scan (slow, even with Z-ordering).
On Lakebase, this is an indexed primary key lookup: sub-10ms.

For a polling agent running every 100ms across 20 machines, the difference is:
- Delta: 20 × ~500ms = 10 seconds per poll cycle → can't do real-time
- Lakebase: 20 × ~10ms = 200ms per poll cycle → real-time works

**b) Database branching for Guardian:**
No Delta Lake feature provides copy-on-write database branching.
Lakebase inherits this from Neon's architecture: any size database cloned in <1 second.

This enables the Guardian's safe execution model:
1. Branch production DB before any write
2. Test the maintenance order on the clone
3. Verify it looks correct
4. Merge to production OR discard

The alternative (without branching) is: write to production and hope it's correct.
That's not enterprise-grade. Branching is the architectural difference.

**The demo moment:**
Show the Lakebase branch being created in real-time. Show the test write on the clone.
Show the merge. This is something nobody has seen a developer do with a database
during a hackathon presentation. It stops the room.

---

### 3. Delta Live Tables (Lakeflow Declarative Pipelines)
**Why not regular Spark jobs?**

DLT provides three things regular Spark jobs don't:
1. **Automatic dependency management:** define Silver as depending on Bronze,
   DLT schedules and re-runs correctly. No manual orchestration code.
2. **Data quality enforcement:** `@dlt.expect_or_drop` rules are infrastructure,
   not code. They create quality monitoring dashboards automatically.
3. **Lineage in Unity Catalog:** every DLT table shows its upstream sources
   in the lineage graph. Manual Spark jobs require custom lineage tracking.

For a 2-day hackathon, DLT saves ~3 hours of orchestration and monitoring boilerplate.

---

### 4. Real-Time Mode in Structured Streaming
**Version:** Preview
**Source:** DAIS 2025 session — "Real-Time Mode Technical Deep-Dive: Sub-300ms"

**The architectural change:**
Standard Structured Streaming uses micro-batching: accumulate records for X seconds,
process the batch, repeat. Minimum practical latency: ~1–5 seconds.

Real-Time Mode uses a concurrent stage scheduler + non-blocking shuffle:
events processed as they arrive, not in batches. P99 latency: <300ms.

**For LiveTwin:**
Monitor Agent scans 20 engines every 100ms. Each scan needs results before the next.
With standard streaming: 1–5 second scan = Monitor Agent is always running behind.
With Real-Time Mode: 287ms scan = Monitor Agent stays current.

At 10,000 engines (production scale), standard streaming cannot keep up at all.
Real-Time Mode is the architecture that scales.

**If unavailable:** Standard Structured Streaming with 1-second trigger interval.
The demo still works; the real-time story is slightly weaker.

---

### 5. MLflow 3.0
**Version:** GA June 2025
**Source:** databricks.com/blog/mlflow-30-unified-ai-experimentation-observability-and-governance

Three distinct uses in LiveTwin:

**a) Model training and versioning:**
All three models (Anomaly Detector, RUL Predictor, Failure Mode Classifier)
trained with MLflow experiment tracking. Every hyperparameter, metric, and artifact logged.
Models registered in Unity Catalog model registry with version tags.

**b) Real-time model serving:**
Models served as MLflow real-time endpoints. Agents call them via REST:
`POST /serving-endpoints/livetwin-rul-predictor/invocations`
No model deployment code — MLflow handles the serving infrastructure.

**c) Agent tracing (the governance story):**
MLflow 3.0's agent tracing logs every span in the agent chain:
- Monitor Agent: which assets scanned, latency, anomalies found
- Diagnosis Agent: which models called, what features used, what outputs returned
- Plan Agent: which schedule queried, what plan proposed
- Guardian Agent: which checks ran, what the verdict was, which user approved

This trace is the audit trail. When a judge asks "how do you prove the agent
made the right decision?" — open MLflow Tracing, show the complete decision chain
with timestamps, model versions, and inputs. This is not a slide. It's live data.

---

### 6. Storage-Optimized Vector Search
**Version:** GA
**Source:** databricks.com/blog/announcing-storage-optimized-endpoints-vector-search

**Justification:** Diagnosis Agent needs semantic search over historical maintenance reports.
The standard approach is keyword search (LIKE queries on text columns in Delta).
Keyword search misses: "HPT lubrication failure" doesn't match "bearing housing oil leak"
even though they're the same failure mode.

Vector Search returns semantically similar incidents even with different terminology.
When Diagnosis Agent queries "turbofan showing elevated s4 temperature and s11 pressure drop" —
it returns incidents that match the failure pattern, not the exact words.

**The billion-scale capability:**
Not needed for 50 documents. Relevant for the scalability story:
"Production deployment would index 10 years of maintenance reports across 50,000 assets.
Storage-Optimized Vector Search handles billion-scale indices. We're using the same
infrastructure that scales to enterprise production, not a toy."

---

### 7. Agent Framework + MCP
**Source:** docs.databricks.com/en/generative-ai/agent-framework

**Why Agent Framework over LangChain/AutoGen directly?**

Three reasons:
1. **Tool governance via Unity Catalog:** Tools (UDFs) registered in UC have lineage,
   access control, and audit logs. LangChain tools are just Python functions — no governance.
2. **OBO Authorization integration:** Agent Framework is the pathway through which
   OBO auth flows to tool calls. Direct API calls bypass this.
3. **Agent Evaluation:** Built-in framework for measuring agent quality (response correctness,
   latency, cost per decision). Judges can see this dashboard — it shows production thinking.

**MCP (Model Context Protocol):**
Standardizes how agents connect to external data sources and tools.
Lakebase, Delta tables, and MLflow endpoints are all exposed as MCP resources.
Any future tool (Slack notifications, external maintenance scheduling system)
can be added as an MCP server without touching agent code.

---

### 8. OBO Authorization
**Source:** Databricks Agent Framework docs — On-Behalf-of-User execution

**The problem it solves:**
Every production AI system eventually faces the question: "what permissions should the agent have?"

Naive answer: give it admin access. (Most hackathon projects do this.)
Correct answer: the agent should have exactly the permissions of the human who triggered it.

OBO implements the correct answer at the infrastructure level:
- Human user authenticates with Databricks
- Agent Framework propagates their token through all tool calls
- Unity Catalog enforces their permissions on every data access
- No custom permission filtering code in agents
- Full audit log: every data access attributed to the user, not a service account

**Demo:**
Switch between user roles live. Show how the same agent chain sees
different data depending on who triggered it. Show the Unity Catalog audit log
proving it. This is not a slide — it's a live demonstration of enterprise governance.

---

### 9. Unity Catalog with ABAC
**Version:** ABAC Public Preview December 2025
**Source:** docs.databricks.com/aws/en/data-governance/unity-catalog/abac

**Used for three things:**

1. **Data lineage:** The full chain — sensor → Bronze → Silver → Gold → MLflow model →
   agent decision → Lakebase maintenance order — is visible in Unity Catalog's lineage graph.
   When a judge asks "where did this maintenance order come from?" —
   click the order in Lakebase, trace lineage backwards to the original sensor reading.

2. **ABAC for multi-plant access control:**
   Tag all Plant A assets with `plant=A`. Define one policy:
   `GRANT SELECT ON gold_engine_health WHERE plant = CURRENT_USER_PLANT()`
   Every table in the catalog inherits this policy. No per-table ACLs.
   Add a new plant → tag the assets → done. The policy applies automatically.

3. **Model registry governance:**
   Which version of the RUL model is in production? Unity Catalog tracks this.
   Who promoted it? Unity Catalog audit log shows it.
   If the model performs poorly, which decisions were made by that version?
   MLflow Tracing + Unity Catalog model version → traceable.

---

### 10. Lakehouse Monitoring
**Source:** docs.databricks.com/en/lakehouse-monitoring

**Two monitoring jobs:**

**a) Data quality monitoring on Silver layer:**
- Null rate per sensor column (are sensors dropping out?)
- Distribution drift: are sensor readings outside historical normal range?
- Row count per engine per day (is a sensor source going silent?)
Generates automatic dashboards. No custom alerting code needed.

**b) Model performance monitoring on RUL Predictor:**
- Input drift: are the features the model receives shifting from training distribution?
- Prediction drift: is the model's average prediction changing over time?
- Ground truth logging: when an engine actually fails, compare predicted RUL vs actual RUL
Automatically updates model quality dashboard in MLflow.

**Why this matters to judges:**
Production ML systems fail silently. If sensor 11 starts returning slightly wrong values,
the RUL model produces wrong predictions, and nobody knows until an engine fails unexpectedly.
Lakehouse Monitoring catches this. Most hackathon projects have no monitoring.
Having monitoring shows production thinking, not demo thinking.

---

### 11. Genie AI/BI
**Source:** docs.databricks.com/en/ai-bi/index.html

**Configuration approach:**
Genie is not coded — it's configured. Provide:
- Which tables it can query (Gold, Lakebase views synced to UC)
- Domain context: terminology mapping, business rules, column meanings
- Sample Q&A pairs to calibrate its understanding

**Five pre-configured questions for the demo:**
1. "Why did Machine 7 fail last hour?" → joins agent_decisions + diagnosis context
2. "How much did proactive maintenance save us this month?" → computes from maintenance_orders cost
3. "Which engines need attention in the next 48 hours?" → queries Gold RUL estimates
4. "Show me all Guardian-blocked decisions this week" → queries agent_decisions WHERE verdict=BLOCKED
5. "What is the most common failure mode in Plant A?" → aggregate over maintenance_orders + Gold

**Why Genie over a custom NL interface:**
Building a custom NL-to-SQL interface is a project in itself.
Genie is production-grade compound AI that already handles query disambiguation,
schema understanding, and follow-up questions. Using it demonstrates judgment:
pick the right tool, don't reinvent it.
