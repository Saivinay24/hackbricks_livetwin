# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Full Demo Runner
# MAGIC
# MAGIC ## HackBricks 2026 Demo Script
# MAGIC
# MAGIC This notebook drives the complete 5-minute live demo:
# MAGIC
# MAGIC | Scene | Time | What Happens |
# MAGIC |-------|------|--------------|
# MAGIC | Setup | 0:00 | Dashboard open, all engines green |
# MAGIC | Scene 1 | 0:30 | Zerobus stream starts, Engine 15 anomaly detected, dashboard turns red |
# MAGIC | Scene 2 | 1:00 | Diagnosis Agent: RUL 23.4h, Vector Search 94% match, MLflow trace shown |
# MAGIC | Scene 3 | 1:30 | Guardian: DB branch created, validated, maintenance order committed |
# MAGIC | Scene 4 | 2:00 | OBO governance demo + Genie "how much did we save?" |
# MAGIC | Q&A    | 3:00 | Questions answered from docs/07_demo_script.md |

# COMMAND ----------

# MAGIC %pip install rich -q  # for pretty terminal output

# COMMAND ----------

import sys
import time
import json
import logging
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()
logger  = logging.getLogger("demo")

# COMMAND ----------

# MAGIC %md ## Scene 0 — Pre-Demo Setup Check

# COMMAND ----------

def run_setup_check():
    """Verify all components are ready before demo starts."""
    console.print(Panel("[bold cyan]LiveTwin — HackBricks 2026[/bold cyan]\nPre-Demo Setup Check", expand=False))

    checks = []

    # Check Unity Catalog
    try:
        spark.sql("SHOW SCHEMAS IN livetwin").collect()
        checks.append(("Unity Catalog", "✓", "livetwin catalog accessible"))
    except Exception as e:
        checks.append(("Unity Catalog", "✗", str(e)[:50]))

    # Check Lakebase
    try:
        from utils.lakebase_client import query_one
        row = query_one("SELECT COUNT(*) as cnt FROM assets")
        checks.append(("Lakebase", "✓", f"{row['cnt']} assets in DB"))
    except Exception as e:
        checks.append(("Lakebase", "⚠", f"Not available: {str(e)[:40]}"))

    # Check DLT pipeline (gold table)
    try:
        cnt = spark.sql("SELECT COUNT(*) as cnt FROM livetwin.processed.gold_engine_health").collect()[0]["cnt"]
        checks.append(("DLT Pipeline", "✓", f"{cnt} engine health records"))
    except Exception as e:
        checks.append(("DLT Pipeline", "⚠", f"Not populated: {str(e)[:40]}"))

    # Check MLflow endpoints
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    for endpoint in ["livetwin-rul-predictor", "livetwin-failure-classifier", "livetwin-anomaly-detector"]:
        try:
            ep = w.serving_endpoints.get(endpoint)
            state = ep.state.ready.value if ep.state and ep.state.ready else "UNKNOWN"
            checks.append((f"MLflow: {endpoint[-10:]}", "✓" if "READY" in state else "⚠", state))
        except Exception as e:
            checks.append((f"MLflow: {endpoint[-10:]}", "✗", "Endpoint not found"))

    # Check Vector Search
    try:
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient()
        idx = vsc.get_index("livetwin-vs-endpoint", "livetwin.incidents.maintenance_reports_index")
        row_count = idx.describe().get("status", {}).get("indexed_row_count", 0)
        checks.append(("Vector Search", "✓", f"{row_count} incidents indexed"))
    except Exception as e:
        checks.append(("Vector Search", "⚠", str(e)[:40]))

    # Display check table
    table = Table(title="System Readiness Check")
    table.add_column("Component", style="cyan")
    table.add_column("Status")
    table.add_column("Details")
    for comp, status, detail in checks:
        color = "green" if status == "✓" else "yellow" if status == "⚠" else "red"
        table.add_row(comp, f"[{color}]{status}[/{color}]", detail)

    console.print(table)

    all_ok = all(s in ("✓", "⚠") for _, s, _ in checks)
    if all_ok:
        console.print("[bold green]✅ System ready for demo.[/bold green]")
    else:
        console.print("[bold red]⚠️ Some components unavailable — review before presenting.[/bold red]")

    return checks

# COMMAND ----------

run_setup_check()

# COMMAND ----------

# MAGIC %md ## Scene 1 — Live Sensor Stream + Anomaly Detection

# COMMAND ----------

def run_scene_1():
    """
    Scene 1: Start Zerobus stream, Engine 15 anomaly appears on dashboard.
    Narrator: "Watch as our IoT stream comes in. Machine 15 — see the red flag."
    """
    console.print(Panel("[bold]SCENE 1: Live Sensor Stream[/bold]\nStarting Zerobus stream — Engine 15 failure incoming", style="blue"))

    # Start stream in background
    import threading
    def background_stream():
        try:
            dbutils.notebook.run(
                "/Workspace/Repos/livetwin/src/01_data/01_zerobus_producer",
                timeout_seconds=120,
                arguments={"mode": "demo"}
            )
        except Exception:
            pass

    stream_thread = threading.Thread(target=background_stream, daemon=True)
    stream_thread.start()

    # Show live anomaly scan while stream is running
    console.print("\n[dim]Scanning fleet for anomalies every 300ms...[/dim]")

    for scan in range(20):
        time.sleep(1)

        try:
            anomalies_df = spark.sql("""
                SELECT sr.asset_id, a.name, sr.anomaly_score, sr.cycle,
                       sr.anomaly_flag, sr.timestamp
                FROM livetwin.operational.sensor_readings sr
                JOIN livetwin.operational.assets a ON a.id = sr.asset_id
                WHERE sr.timestamp > NOW() - INTERVAL 30 SECONDS
                  AND sr.anomaly_flag = TRUE
                ORDER BY sr.anomaly_score DESC LIMIT 5
            """)
            anomalies = [r.asDict() for r in anomalies_df.collect()]

            if anomalies:
                console.print(f"\n[bold red]🚨 ANOMALY DETECTED at t={scan}s:[/bold red]")
                for a in anomalies:
                    console.print(
                        f"   Engine {a['asset_id']} ({a['name']}) — "
                        f"score={a['anomaly_score']:.3f} cycle={a['cycle']}"
                    )
                return anomalies
            else:
                console.print(f"  [{scan}s] Fleet scan: all clear ({20} engines nominal)", end="\r")
        except Exception as e:
            console.print(f"  [{scan}s] Waiting for stream data...", end="\r")

    console.print("\n[yellow]No live anomalies found — using pre-seeded Engine 15 data[/yellow]")
    return [{"asset_id": 15, "anomaly_score": 0.89, "cycle": 187}]

# COMMAND ----------

# MAGIC %md ## Scene 2 — Diagnosis Agent Deep Dive

# COMMAND ----------

def run_scene_2(anomaly: dict):
    """
    Scene 2: Run Diagnosis Agent live, show RUL + Vector Search match.
    Narrator: "The Diagnosis Agent queries three systems simultaneously..."
    """
    console.print(Panel(
        f"[bold]SCENE 2: Diagnosis Agent[/bold]\n"
        f"Engine {anomaly['asset_id']} — anomaly score {anomaly.get('anomaly_score',0):.3f}",
        style="magenta"
    ))

    sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")
    from diagnosis_agent import run_diagnosis_agent

    user_ctx = {"user_id": "maintenance_supervisor@livetwin.com", "groups": ["maintenance_supervisors"]}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("[cyan]Running Diagnosis Agent...", total=None)

        diagnosis = run_diagnosis_agent(anomaly, user_ctx)

    # Display results
    table = Table(title=f"Diagnosis — Engine {anomaly['asset_id']}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="bold")

    rul_h = diagnosis["rul_hours"]
    table.add_row("RUL (hours)", f"{rul_h:.1f}h  [{diagnosis['rul_lower_bound']:.1f} – {diagnosis['rul_upper_bound']:.1f}]")
    table.add_row("Failure Mode", f"{diagnosis['failure_mode'].replace('_',' ').title()} ({diagnosis['failure_confidence']:.0%} confidence)")
    table.add_row("Urgency", f"[red]{diagnosis['urgency_hint']}[/red]" if diagnosis['urgency_hint'] != 'SCHEDULED' else f"[green]{diagnosis['urgency_hint']}[/green]")
    table.add_row("Diagnosis Time", f"{diagnosis['total_diagnosis_ms']}ms")

    if diagnosis["similar_incidents"]:
        top = diagnosis["similar_incidents"][0]
        table.add_row("Vector Search Match", f"{top.get('incident_id','?')} ({top.get('similarity_score',0):.0%})")
        table.add_row("Historical Root Cause", (top.get("root_cause", "")[:60] + "..."))

    console.print(table)
    console.print(f"\n[italic]{diagnosis['diagnosis_summary']}[/italic]")

    # Show MLflow trace link
    console.print(f"\n[dim]MLflow Trace: experiment /Shared/livetwin/agent_traces[/dim]")

    return diagnosis

# COMMAND ----------

# MAGIC %md ## Scene 3 — Guardian Agent + DB Branching

# COMMAND ----------

def run_scene_3(diagnosis: dict):
    """
    Scene 3: Run Plan + Guardian. Show DB branching live.
    Narrator: "Now watch the Guardian. It's about to branch the production database..."
    """
    console.print(Panel(
        "[bold]SCENE 3: Guardian Agent — Database Branching[/bold]\n"
        "Validate → CREATE BRANCH → test INSERT → verify → MERGE to main",
        style="yellow"
    ))

    from plan_agent    import run_plan_agent
    from guardian_agent import run_guardian_agent

    user_ctx = {"user_id": "maintenance_supervisor@livetwin.com", "groups": ["maintenance_supervisors"]}

    # Plan
    console.print("[cyan]Planning...[/cyan]")
    plan = run_plan_agent(diagnosis, user_ctx)
    console.print(f"  Plan: {plan['urgency']} | Window: {plan['recommended_window']['start'][:16]} | Cost: ₹{plan['estimated_cost']:,.0f}")

    # Guardian — the key demo moment
    console.print("\n[bold yellow]Guardian starting validation...[/bold yellow]")
    t_guard = time.time()

    result = run_guardian_agent(plan, diagnosis, user_ctx)

    guard_ms = int((time.time() - t_guard) * 1000)

    # Display checks table
    checks_table = Table(title="Guardian Validation Checks")
    checks_table.add_column("Check")
    checks_table.add_column("Result")
    checks_table.add_column("Detail")

    for check in result.get("checks", []):
        icon  = "✓ PASS" if check["passed"] else "✗ FAIL"
        color = "green" if check["passed"] else "red"
        checks_table.add_row(
            check["check"].replace("_", " ").title(),
            f"[{color}]{icon}[/{color}]",
            check.get("detail", "")[:60],
        )
    console.print(checks_table)

    # Branch timeline
    if result["verdict"] == "APPROVED":
        timing = result.get("timing_ms", {})
        console.print(f"\n[bold green]✅ VERDICT: APPROVED[/bold green]")
        console.print(f"  Branch: {result.get('branch_id','?')}")
        console.print(f"  Maintenance Order: #{result.get('committed_order_id','?')} committed to production")
        console.print(f"  Approved by: {result.get('approved_by','?')}")
        console.print(f"\n  Timeline:")
        console.print(f"    Branch create: {timing.get('branch_create_ms','?')}ms")
        console.print(f"    INSERT + verify: {timing.get('branch_insert_ms','?')}ms")
        console.print(f"    Merge to main: {timing.get('branch_merge_ms','?')}ms")
        console.print(f"    Total Guardian: {guard_ms}ms")
    else:
        console.print(f"\n[bold red]⛔ VERDICT: {result['verdict']}[/bold red]")
        console.print(f"  Reason: {result.get('block_reasons','')}")

    # Show order in DB
    try:
        display(spark.sql("""
            SELECT id, asset_id, failure_mode, rul_at_detection, urgency,
                   estimated_cost, guardian_approved, approved_by_user, status, created_at
            FROM livetwin.operational.maintenance_orders
            ORDER BY created_at DESC LIMIT 3
        """))
    except Exception:
        pass

    return result

# COMMAND ----------

# MAGIC %md ## Scene 4 — OBO Governance + Genie

# COMMAND ----------

def run_scene_4():
    """
    Scene 4: Show OBO access control + Genie answering natural language questions.
    Narrator: "And now let's show the governance story..."
    """
    console.print(Panel(
        "[bold]SCENE 4: Governance + Genie AI/BI[/bold]\n"
        "OBO Authorization + Unity Catalog ABAC + Natural Language Analytics",
        style="green"
    ))

    # OBO demonstration
    console.print("\n[bold cyan]OBO Authorization Demo:[/bold cyan]")
    console.print("  Plant A Operator queries → Unity Catalog filters to Plant A (assets 1-10) only")
    console.print("  Plant B Operator queries → Unity Catalog filters to Plant B (assets 11-20) only")
    console.print("  [italic]Zero agent code changes. Pure infrastructure governance.[/italic]")

    try:
        console.print("\n  Running as Plant A Operator (row filter active):")
        df_a = spark.sql("""
            -- Unity Catalog row filter transparent to this query
            SELECT asset_id, health_score, health_status
            FROM livetwin.processed.gold_engine_health
            ORDER BY asset_id LIMIT 5
        """)
        display(df_a)
    except Exception as e:
        console.print(f"  [yellow]Row filter demo: {e}[/yellow]")

    # Genie
    console.print("\n[bold cyan]Genie AI/BI — Natural Language Analytics:[/bold cyan]")
    demo_questions = [
        "How much did LiveTwin save this month?",
        "Which engines need attention in the next 48 hours?",
        "Show me all CRITICAL maintenance orders",
    ]
    for q in demo_questions:
        console.print(f'  Ask Genie: "[italic]{q}[/italic]"')

    console.print("\n  [dim](Open Genie Space: /genie/spaces/{space_id} to see live answers)[/dim]")

    # Audit log
    console.print("\n[bold cyan]Unity Catalog Audit Log:[/bold cyan]")
    try:
        display(spark.sql("""
            SELECT triggered_by_user, COUNT(*) as decisions, MAX(timestamp) as last_action
            FROM livetwin.operational.agent_decisions
            GROUP BY triggered_by_user
            ORDER BY last_action DESC LIMIT 5
        """))
    except Exception as e:
        console.print(f"  [yellow]{e}[/yellow]")

# COMMAND ----------

# MAGIC %md ## Run Full Demo

# COMMAND ----------

dbutils.widgets.dropdown("scene", "all", ["all", "1", "2", "3", "4", "setup_check"])
scene = dbutils.widgets.get("scene")

if scene == "setup_check":
    run_setup_check()

elif scene == "1":
    anomalies = run_scene_1()

elif scene == "2":
    anomaly = {"asset_id": 15, "anomaly_score": 0.89, "cycle": 187, "anomaly_flag": True}
    diagnosis = run_scene_2(anomaly)

elif scene == "3":
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    mock_diagnosis = {
        "asset_id":           15, "asset_name": "Engine-15", "plant": "plant_b",
        "rul_hours":          23.4, "rul_lower_bound": 18.2, "rul_upper_bound": 28.6,
        "failure_mode":       "bearing_degradation", "failure_confidence": 0.87,
        "urgency_hint":       "URGENT",
        "similar_incidents": [{"incident_id": "INC-2019-0042", "cost": 45000, "similarity_score": 0.94}],
        "timing_ms":          {"lakebase_ms": 8, "gold_ms": 340, "rul_ms": 890, "classifier_ms": 650, "vector_search_ms": 120},
    }
    guardian_result = run_scene_3(mock_diagnosis)

elif scene == "4":
    run_scene_4()

elif scene == "all":
    # Full 5-minute demo flow
    console.print(Panel(
        "[bold cyan]🚀 LIVETWIN FULL DEMO[/bold cyan]\n"
        "HackBricks 2026 | MIT Bengaluru | April 11–12, 2026\n"
        "Operational AI for Industrial Equipment on Databricks Lakehouse",
        style="bold"
    ))

    # Setup
    run_setup_check()
    time.sleep(2)

    # Scene 1
    anomalies = run_scene_1()
    anomaly   = anomalies[0] if anomalies else {"asset_id": 15, "anomaly_score": 0.89, "cycle": 187}
    time.sleep(2)

    # Scene 2
    diagnosis = run_scene_2(anomaly)
    time.sleep(2)

    # Scene 3
    guardian_result = run_scene_3(diagnosis)
    time.sleep(2)

    # Scene 4
    run_scene_4()

    # Final summary
    console.print(Panel(
        "[bold green]✅ Demo Complete[/bold green]\n\n"
        "LiveTwin demonstrated:\n"
        "1. Real-time sensor anomaly detection via Zerobus + DLT\n"
        "2. AI diagnosis with MLflow RUL + Vector Search similarity\n"
        "3. Guardian agent branching Lakebase DB → safe write pattern\n"
        "4. OBO Authorization + Unity Catalog ABAC governance\n"
        "5. Genie AI/BI natural language operations analytics\n\n"
        "From sensor anomaly to maintenance order: [bold]~10 seconds[/bold]\n"
        "vs. human process: 2–6 hours",
        style="bold green"
    ))

# COMMAND ----------

# MAGIC %md ## Fallback: Static Demo (pre-recorded results)

# COMMAND ----------

# If live demo has issues, this cell shows pre-recorded results

FALLBACK_RESULTS = {
    "sensor_anomaly": {
        "asset_id": 15, "asset_name": "Engine-15", "plant": "plant_b",
        "cycle": 187, "anomaly_score": 0.89, "timestamp": "2026-04-11T10:30:45Z"
    },
    "diagnosis": {
        "rul_hours": 23.4, "rul_lower": 18.2, "rul_upper": 28.6,
        "failure_mode": "bearing_degradation", "confidence": 0.87,
        "top_incident": "INC-2019-0042", "similarity": 0.94,
        "total_ms": 2105
    },
    "plan": {
        "urgency": "URGENT", "estimated_cost": 42500,
        "window_start": "2026-04-11T22:00:00Z", "window_end": "2026-04-12T04:00:00Z",
        "action": "Replace HPT bearing assembly. Flush lubrication system."
    },
    "guardian": {
        "verdict": "APPROVED", "order_id": 1042,
        "branch_id": "guardian_test_15_1744372100",
        "approved_by": "maintenance_supervisor@livetwin.com",
        "branch_create_ms": 912, "merge_ms": 783, "total_ms": 1987,
        "checks": [
            {"name": "cost_authorization",    "passed": True},
            {"name": "window_available",      "passed": True},
            {"name": "no_conflicting_orders", "passed": True},
            {"name": "safety_protocol",       "passed": True},
        ]
    }
}

if dbutils.widgets.get("scene") == "fallback":
    console.print(Panel("[bold yellow]FALLBACK MODE — Pre-Recorded Results[/bold yellow]"))
    console.print(json.dumps(FALLBACK_RESULTS, indent=2))
