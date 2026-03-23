# HackBricks 2026 — Analysis from Your Projects
*Starting point: your GitHub + project map → best fit for Databricks hackathon*

---

## Your Profile in One Sentence

You build **spatial intelligence systems** — you think in 3D, time, and graph topology simultaneously.
Most data science students think in tables. That's your moat.

**North star (from project map):** GenAI-Driven Spatial Computing — "Architect & lead next-gen spatial products"
**Academic:** B.Tech CSE (Data Science), Sem 5, 9.11 GPA

---

## What Judges Actually Ask at a Databricks Hackathon

> "Does this project NEED Databricks, or is Databricks bolted on?"

Projects that win pick a problem where the Lakehouse (Delta Lake + Streaming + MLflow + Genie + Unity Catalog)
is the actual solution — not the container for it.

The second thing judges remember: **the demo that made them lean forward.**
20+ teams present. Which one makes the room go quiet?

---

## Judging Criteria Weights (my assessment from experience)

| Criterion | Weight | What Judges Actually Look For |
|---|---|---|
| Innovation & Creativity | High | "Haven't seen this before" |
| Technical Implementation | High | Architecture decisions that show mastery |
| Effective Databricks Use | Very High | Deep platform integration, not surface-level |
| Real-World Impact | High | Can I explain the problem to my grandmother? |
| Scalability | Medium | Clear growth narrative |
| Presentation Quality | Medium | Does the demo work live? |

---

## Complete Project Scoring

| Project | Databricks Fit | Novelty | Demo Quality | Your Leverage | **Total** |
|---|---|---|---|---|---|
| Smurfing Hunter | 9/10 — GraphFrames, streaming, Delta Lake | 9/10 — nobody uses Databricks for graph AML | 8/10 — live transaction graph | 9/10 — algorithm already validated | **8.8** |
| CityMind + Scan-to-BIM | 8/10 — IoT streams, spatial features, MLflow | 8/10 — BIM + Lakehouse completely unexplored | 9/10 — visually spectacular | 10/10 — deepest domain knowledge | **8.7** |
| 5G Optimizer | 8/10 — telemetry pipelines, time-series streaming | 7/10 — telecom + Databricks exists in industry | 7/10 — abstract for non-telecom judges | 8/10 — deterministic algorithms are compelling | **7.5** |
| Aegis | 7/10 — multi-agent maps to Databricks | 8/10 — data OS is novel framing | 8/10 — voice demo is impressive | 7/10 — needs full architectural redesign | **7.5** |
| AgriSense | 9/10 — textbook Databricks IoT + ML pipeline | 5/10 — agriculture + data is very common theme | 7/10 — standard dashboard demo | 7/10 — existing base but basic scope | **7.0** |
| GenAI Motion Benchmarks | 7/10 — MLflow, distributed eval harness | 7/10 — evaluation infra is actually underexplored | 5/10 — abstract for most judges | 8/10 — Aalto Research credibility | **6.5** |
| BARF (4D Completion) | 5/10 — Databricks would be forced integration | 10/10 — cutting edge | 6/10 — impossible to demo live in 2 days | 8/10 — team of 7, recently built | **6.5** |
| Medical/Neuro RAG | 7/10 — Vector Search + Mosaic AI fit | 5/10 — RAG is completely saturated at hackathons | 6/10 — chat interface is forgettable | 7/10 — offline RAG is interesting angle | **6.2** |

---

## Top 2 Candidates — Deep Analysis

---

### #1 — ChainGuard: Real-Time DeFi AML Intelligence Platform
*Based on: Smurfing Hunter*

**Score: 8.8/10**

#### The Core Insight

NetworkX (what your Smurfing Hunter uses) breaks at ~100k nodes.
A single Ethereum day has **1.4 million transactions**.
GraphFrames on Databricks handles 100M+ edges natively.

That scale gap IS the Databricks story. It's not bolted on — it's the reason the project exists.

#### Why Judges Will Love This

1. Graph analytics on a Lakehouse is extremely rare. Databricks engineers themselves don't see this often.
2. The demo is live and dramatic: ingest a real Ethereum block, watch the graph light up with flagged patterns in real-time.
3. $3.1 trillion laundered annually. Impact is undeniable.
4. Existing tools (Chainalysis, Elliptic) cost $50k+/year and are black boxes. This is open and explainable.

#### Architecture (every component justified)

```
Ethereum/Solana WebSocket API (Alchemy/Infura)
        ↓
Structured Streaming → Bronze Delta Table (raw transactions, ~1.4M/day)
        ↓
Delta Live Tables → Silver (normalized graph edges + nodes, deduped)
        ↓
GraphFrames (Spark) → Distributed pattern detection:
    - Fan-out / Fan-in detection
    - Peeling chain identification
    - Cyclic graph detection
    - Temporal causality validation
        ↓
MLflow → Suspicion scoring model (0-100), versioned, monitored
        ↓
Unity Catalog → Govern flagged wallet data (full compliance audit trail)
        ↓
Genie AI/BI → "Which wallets are connected to address X within 3 hops?"
        ↓
Databricks SQL Dashboard → Live investigator view with real-time updates
```

#### Your Existing Leverage

From Smurfing Hunter you already have:
- Validated suspicion scoring formula: `Score = α×Centrality + β×Illicit_Proximity + γ×Pattern_Involvement + δ×Structural_Anomaly`
- Temporal causality logic (Time_A < Time_B < Time_C)
- Peeling chain threshold at 2% (domain-informed, defensible under judge questioning)
- Fan-out/Fan-in pattern detection with flow conservation logic
- Interactive HTML visualization

You're porting a validated algorithm to a platform where it actually scales.
That's not pre-built — that's smart methodology reuse.

#### Scope for 2-Day Hackathon (Feasible)

- Day 1: Set up Delta Live Tables pipeline with Alchemy WebSocket → Bronze → Silver. GraphFrames pattern detection on Spark.
- Day 2: MLflow model serving for suspicion scoring. Genie setup. Dashboard. Polish demo.

#### What Needs to Be Added vs. Built Fresh

| Component | Source | Work Required |
|---|---|---|
| Pattern detection algorithms | Smurfing Hunter | Port from NetworkX to GraphFrames (~4-6 hrs) |
| Data pipeline | Build fresh | Alchemy API → Structured Streaming (~3-4 hrs) |
| Suspicion scoring | Smurfing Hunter | Wrap as MLflow model (~2 hrs) |
| Dashboard | Build fresh | Databricks SQL (~2 hrs) |
| Genie setup | Build fresh | Natural language config (~1 hr) |

---

### #2 — SiteIntel: 4D Construction Intelligence Lakehouse
*Based on: Scan-to-BIM v1+v2 + CityMind*

**Score: 8.7/10**

#### The Core Insight

Construction is the world's largest industry ($13T) and the least digitized.
The gap between "as-designed" BIM and "as-built" reality costs **$1.8T in overruns annually**.
Nobody has built a Databricks Lakehouse for construction intelligence. The domain is wide open.

#### Why You Specifically

You've built Scan-to-BIM twice (LoFTR + RANSAC, PointNet++, COLMAP, differentiable optimization)
and CityMind (6-layer AI pipeline, multi-agent inspection, structural health index).
No other team in this hackathon has touched this domain at this depth.
Under judge questioning, your architecture decisions will be immediately credible.

#### Architecture

```
Construction Site Data Sources:
  - Drone video frames → extract geometric metadata
  - IoT sensors (vibration, temperature, worker location) → real-time stream
  - BIM files (IFC format) → reference design data
  - Project schedules (CSV/JSON) → timeline data

Delta Live Tables:
  Bronze → Raw frames metadata + raw sensor readings
  Silver → Feature-extracted progress metrics + anomaly flags
  Gold → Zone-by-zone completion % + risk scores + deviation from BIM

Structured Streaming → Real-time site IoT (temperature, stress, worker safety)

Spark ML + MLflow:
  - Schedule delay prediction model
  - Cost overrun risk model
  - Safety incident probability by zone

Genie AI/BI → "Which zones are behind schedule?" → spatial aggregate query
             → "Why is Zone C at risk?" → root cause from Gold table

Unity Catalog → Multi-site governance (portfolio of projects, each isolated)

Databricks SQL Dashboard → BIM overlay vs actual progress heatmap
```

#### Demo Moment

BIM model on one side (the design intent).
Actual site progress heatmap on the other (what Spark ML computed from sensor + video metadata).
Deviation zones highlighted in red.
Genie answers "why is Building A southeast corner at risk?" in plain English.

#### Execution Risk (Honest)

Higher than ChainGuard. 3D data pipelines have failure modes hard to debug under pressure.
The MVP scopes down to: pre-extracted geometric features (not full reconstruction) + IoT simulation.
Full 3D reconstruction is NOT needed for the demo to be compelling.

#### Long-Term Value

This directly feeds your CityMind → Scan-to-BIM → Spatial Computing trajectory.
This is the better investment for your portfolio, even if execution risk is higher.

---

## Honest Comparison

| Dimension | ChainGuard (AML) | SiteIntel (Construction) |
|---|---|---|
| Win probability | Higher | Slightly lower (execution risk) |
| Databricks depth | Excellent | Excellent |
| Demo drama | High (live blockchain) | Very high (visual spatial) |
| Your portfolio value | Medium (diverges from spatial north star) | Very high (extends your core work) |
| Time to MVP | Lower | Higher |
| Judge surprise factor | Very high (graph AML on Lakehouse is rare) | High (construction + Lakehouse unexplored) |
| Aligns with 5-year direction | 6/10 | 10/10 |

---

## Enhancement for Either Project

Neither naturally uses **Databricks Vector Search** or **Foundation Model Fine-tuning**.
Adding either of these pushes you to near-complete Databricks feature coverage:

- **For ChainGuard:** Vector Search over historical investigation reports for semantic similarity. "Find wallets that behave like this known criminal address" → embedding similarity search.
- **For SiteIntel:** RAG over building codes (ACI/ASCE/IBC) using Vector Search — your CityMind already had a Compliance Agent doing exactly this.

---

## Bottom Line

**Pick ChainGuard** if maximizing win probability is the goal.

**Pick SiteIntel** if you want a portfolio centerpiece that matters beyond the hackathon
and advances your spatial computing north star.

Both are genuinely novel. Neither has been done at a Databricks hackathon before.
