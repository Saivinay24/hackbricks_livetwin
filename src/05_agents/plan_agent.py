# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Plan Agent
# MAGIC
# MAGIC Given a diagnosis, proposes a concrete maintenance intervention.
# MAGIC
# MAGIC - **Trigger:** Called by Diagnosis Agent
# MAGIC - **Inputs:** Diagnosis output, Lakebase operational_schedule, cost history
# MAGIC - **Output:** Maintenance plan with window, cost, urgency, and action
# MAGIC - **Timing:** ~0.8 seconds (all Lakebase reads, sub-10ms each)
# MAGIC - **Access:** Read-only — never writes to production

# COMMAND ----------

import sys
import time
import logging
import statistics
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

from utils.lakebase_client import (
    get_available_windows_v2, get_asset, get_config, update_in_flight_stage, query_all,
)
from utils.mlflow_client import AgentTrace

logger = logging.getLogger("plan_agent")

# COMMAND ----------

# ─── Standard Maintenance Actions (per failure mode) ─────────────────────────

MAINTENANCE_ACTIONS = {
    "bearing_degradation": (
        "Replace HPT/LPT bearing assembly. Flush lubrication system. "
        "Inspect adjacent seals and bearing housings. Conduct post-repair vibration test."
    ),
    "thermal_degradation": (
        "Inspect and recoat thermal barrier coating on HPT blades. "
        "Check NGV oxidation, cooling hole blockages, and combustor liner. "
        "Verify EGT margins after repair."
    ),
    "vibration": (
        "Conduct rotor balance check. Inspect fan and compressor blades for damage/erosion. "
        "Verify bearing clearances. Post-repair vibration acceptance test at all speeds."
    ),
    "fuel_system": (
        "Inspect and replace fuel control unit valve. "
        "Check fuel pump pressure, nozzle flow rates, and manifold integrity. "
        "Conduct full fuel system pressure test after repair."
    ),
    "compressor_degradation": (
        "Borescope inspect all HPC stages. Replace eroded blades (stages with >2mm LE loss). "
        "Check variable stator vane angles. Conduct post-repair compressor performance run."
    ),
    "unknown": (
        "Full borescope inspection of all major sections. "
        "Oil analysis and system pressure test. Vibration survey at all power settings."
    ),
}


def get_maintenance_action(failure_mode: str) -> str:
    return MAINTENANCE_ACTIONS.get(failure_mode, MAINTENANCE_ACTIONS["unknown"])


# ─── Cost Estimation ──────────────────────────────────────────────────────────

def estimate_cost_from_incidents(similar_incidents: list[dict]) -> tuple[float, float, float]:
    """
    Estimate maintenance cost from similar historical incidents.
    Returns (mean_cost, min_cost, max_cost).
    """
    if not similar_incidents:
        return (45000.0, 25000.0, 75000.0)  # default estimate

    costs = []
    for inc in similar_incidents:
        cost = inc.get("cost", 0)
        if isinstance(cost, str):
            try:
                cost = float(cost)
            except ValueError:
                cost = 0
        if cost and float(cost) > 0:
            costs.append(float(cost))

    if not costs:
        return (45000.0, 25000.0, 75000.0)

    mean_cost = statistics.mean(costs)
    return (mean_cost, min(costs), max(costs))


# ─── Urgency Determination ────────────────────────────────────────────────────

def determine_urgency(rul_hours: float, available_windows: list[dict]) -> str:
    """
    Determine urgency level based on RUL and available windows.
    SCHEDULED / URGENT / CRITICAL
    """
    rul_urgent_h   = float(get_config("rul_urgent_threshold_h",   "24"))
    rul_critical_h = float(get_config("rul_critical_threshold_h", "8"))

    if rul_hours <= rul_critical_h:
        return "CRITICAL"
    if rul_hours <= rul_urgent_h:
        return "URGENT"
    if not available_windows:
        return "URGENT"  # no window = escalate
    return "SCHEDULED"


# ─── Main Plan Function ───────────────────────────────────────────────────────

def run_plan_agent(diagnosis: dict, user_context: dict) -> dict:
    """
    Build a maintenance plan from diagnosis output.

    Returns:
        {
            asset_id, failure_mode, recommended_window (start/end),
            estimated_cost, urgency, maintenance_action, plan_summary,
            timing_ms
        }
    """
    asset_id     = diagnosis["asset_id"]
    rul_hours    = diagnosis["rul_hours"]
    failure_mode = diagnosis["failure_mode"]
    timing       = {}

    with AgentTrace("plan_agent", asset_id=asset_id) as trace:

        # ── Step 1: Get available maintenance windows ─────────────────────────
        t0 = time.time()
        urgency_window_hours = rul_hours * 0.7  # act before 70% of RUL consumed

        available_windows = get_available_windows_v2(
            asset_id,
            hours_ahead=max(urgency_window_hours, 4),  # minimum 4 hours ahead
        )
        timing["windows_ms"] = int((time.time() - t0) * 1000)

        logger.info(
            f"[Plan] asset={asset_id} available_windows={len(available_windows)} "
            f"urgency_window={urgency_window_hours:.1f}h lakebase={timing['windows_ms']}ms"
        )
        trace.log("window_fetch",
                  inputs={"asset_id": asset_id, "hours_ahead": urgency_window_hours},
                  outputs={"windows_available": len(available_windows)},
                  latency_ms=timing["windows_ms"])

        # ── Step 2: Cost estimation from similar incidents ────────────────────
        t0 = time.time()
        est_cost, min_cost, max_cost = estimate_cost_from_incidents(
            diagnosis.get("similar_incidents", [])
        )
        timing["cost_ms"] = int((time.time() - t0) * 1000)

        logger.info(f"[Plan] Cost estimate: ₹{est_cost:,.0f} [₹{min_cost:,.0f}–₹{max_cost:,.0f}]")

        # ── Step 3: Select optimal window ─────────────────────────────────────
        now = datetime.now(timezone.utc)

        if available_windows:
            # Choose earliest window within urgency timeframe
            optimal_window = available_windows[0]
            window_start   = optimal_window.get("window_start")
            window_end     = optimal_window.get("window_end")

            # Convert to timezone-aware datetime if needed
            if isinstance(window_start, str):
                window_start = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
                window_end   = datetime.fromisoformat(window_end.replace("Z", "+00:00"))
        else:
            # CRITICAL: no window, create an emergency slot
            window_start = now + timedelta(hours=1)
            window_end   = window_start + timedelta(hours=4)
            optimal_window = {
                "window_start": window_start.isoformat(),
                "window_end":   window_end.isoformat(),
                "priority_override": True,
            }
            logger.warning(f"[Plan] No available windows for asset {asset_id} — emergency scheduling")

        urgency = determine_urgency(rul_hours, available_windows)
        maintenance_action = get_maintenance_action(failure_mode)

        # ── Step 4: Also check cost vs user's authorization limit ──────────────
        # (Guardian will enforce this — Plan just flags it)
        from utils.lakebase_client import get_user_cost_limit
        user_cost_limit = get_user_cost_limit(user_context.get("user_id", ""))
        cost_warning = est_cost > user_cost_limit

        # ── Step 5: Build plan summary ─────────────────────────────────────────
        hours_until_window = 0
        if isinstance(window_start, datetime):
            hours_until_window = max(0, (window_start - now).total_seconds() / 3600)

        plan_summary = (
            f"{'⚠️ URGENT' if urgency in ('URGENT','CRITICAL') else 'SCHEDULED'} maintenance for "
            f"Engine {asset_id} ({diagnosis.get('asset_name','?')}). "
            f"Required within {rul_hours * 0.7:.1f}h (70% of {rul_hours:.1f}h RUL). "
            f"Recommended window: "
            f"{window_start.strftime('%b %d %H:%M') if isinstance(window_start, datetime) else window_start} – "
            f"{window_end.strftime('%H:%M') if isinstance(window_end, datetime) else window_end}. "
            f"Estimated cost: ₹{est_cost:,.0f}. "
            f"Action: {maintenance_action[:80]}..."
            + (" [COST EXCEEDS OPERATOR LIMIT — supervisor approval required]" if cost_warning else "")
        )

        total_ms = sum(timing.values())

        result = {
            "asset_id":                 asset_id,
            "failure_mode":             failure_mode,
            "recommended_window":       {
                "start":                window_start.isoformat() if isinstance(window_start, datetime) else str(window_start),
                "end":                  window_end.isoformat()   if isinstance(window_end, datetime)   else str(window_end),
                "priority_override":    optimal_window.get("priority_override", False),
                "schedule_id":          optimal_window.get("id"),
            },
            "estimated_cost":           est_cost,
            "cost_range":               {"min": min_cost, "max": max_cost},
            "urgency":                  urgency,
            "maintenance_action":       maintenance_action,
            "plan_summary":             plan_summary,
            "cost_warning":             cost_warning,
            "user_cost_limit":          user_cost_limit,
            "timing_ms":                timing,
            "total_plan_ms":            total_ms,
            # Pass-through for Guardian
            "rul_hours":                rul_hours,
            "similar_incidents":        diagnosis.get("similar_incidents", []),
        }

        trace.log("plan_complete",
                  inputs={"asset_id": asset_id, "rul_hours": rul_hours},
                  outputs={
                      "urgency":       urgency,
                      "estimated_cost": est_cost,
                      "windows_found": len(available_windows),
                      "total_ms":      total_ms,
                  },
                  latency_ms=total_ms)

        logger.info(
            f"[Plan] COMPLETE asset={asset_id} urgency={urgency} "
            f"cost=₹{est_cost:,.0f} window_in={hours_until_window:.1f}h total={total_ms}ms"
        )

        return result


# COMMAND ----------

# MAGIC %md ## Test

# COMMAND ----------

if __name__ == "__main__" or (hasattr(dbutils, "widgets") and dbutils.widgets.get("run_test") == "true"):
    # Simulate Diagnosis Agent output
    test_diagnosis = {
        "asset_id":           15,
        "asset_name":         "Engine-15",
        "plant":              "plant_b",
        "rul_hours":          23.4,
        "rul_lower_bound":    18.2,
        "rul_upper_bound":    28.6,
        "failure_mode":       "bearing_degradation",
        "failure_confidence": 0.87,
        "similar_incidents": [
            {"incident_id": "INC-2019-0042", "cost": 45000, "similarity_score": 0.94},
            {"incident_id": "INC-2020-0117", "cost": 78000, "similarity_score": 0.81},
            {"incident_id": "INC-2021-0089", "cost": 41000, "similarity_score": 0.76},
        ],
    }
    test_user = {"user_id": "maintenance_supervisor@livetwin.com", "groups": ["maintenance_supervisors"]}

    result = run_plan_agent(test_diagnosis, test_user)

    print(f"\n{'='*60}")
    print(f"PLAN RESULT")
    print(f"{'='*60}")
    print(f"Urgency:    {result['urgency']}")
    print(f"Window:     {result['recommended_window']['start'][:19]} – {result['recommended_window']['end'][11:16]}")
    print(f"Cost:       ₹{result['estimated_cost']:,.0f}  (range: ₹{result['cost_range']['min']:,.0f}–₹{result['cost_range']['max']:,.0f})")
    print(f"Cost warn:  {result['cost_warning']}")
    print(f"Action:     {result['maintenance_action'][:80]}...")
    print(f"Total time: {result['total_plan_ms']}ms")
    print(f"\nSummary: {result['plan_summary'][:200]}")
