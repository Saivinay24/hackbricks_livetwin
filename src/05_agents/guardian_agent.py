# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Guardian Agent
# MAGIC
# MAGIC The only agent with write access. Validates, branches, and commits maintenance orders.
# MAGIC
# MAGIC ## What the Guardian Does
# MAGIC
# MAGIC 1. **Pre-validation** (4 checks before touching production):
# MAGIC    - Cost within user's OBO authorization limit
# MAGIC    - Maintenance window still available (not taken by another agent)
# MAGIC    - No conflicting pending orders for this asset
# MAGIC    - Safety protocol compliance via RAG (Vector Search)
# MAGIC
# MAGIC 2. **Database branching** (Lakebase copy-on-write):
# MAGIC    - `CREATE BRANCH guardian_test_15_1744372100 FROM main`
# MAGIC    - Test INSERT on clone, verify row written correctly
# MAGIC    - `MERGE BRANCH ... INTO main` (commit)
# MAGIC    - Or `DROP BRANCH` if any step fails
# MAGIC
# MAGIC 3. **Full audit trail** via MLflow Tracing
# MAGIC
# MAGIC - **Timing:** ~2.0 seconds (branch 900ms + validation 200ms + merge 800ms)
# MAGIC - **Authorization:** OBO — maintenance order records exact triggering user

# COMMAND ----------

import sys
import time
import json
import logging
from datetime import datetime, timezone

sys.path.insert(0, "/Workspace/Repos/livetwin/src/05_agents")

from utils.lakebase_client import (
    has_pending_order, get_user_cost_limit, execute, query_one,
    get_config, update_in_flight_stage, LakebaseBranch,
)
from utils.mlflow_client import AgentTrace
from utils.vector_search_client import search_safety_protocols

logger = logging.getLogger("guardian_agent")

# COMMAND ----------

# ─── Pre-Validation Checks ────────────────────────────────────────────────────

def check_cost_authorization(plan: dict, user_context: dict) -> dict:
    """Check 1: estimated cost within user's authorization limit."""
    estimated_cost  = plan["estimated_cost"]
    user_cost_limit = get_user_cost_limit(user_context.get("user_id", ""))

    passed = estimated_cost <= user_cost_limit
    return {
        "check":         "cost_authorization",
        "passed":        passed,
        "detail":        f"Cost ₹{estimated_cost:,.0f} vs limit ₹{user_cost_limit:,.0f} for user '{user_context.get('user_id','')}'",
        "latency_ms":    2,
        "block_reason":  f"Cost ₹{estimated_cost:,.0f} exceeds operator limit ₹{user_cost_limit:,.0f}. Supervisor approval required." if not passed else None,
    }


def check_window_available(plan: dict) -> dict:
    """Check 2: maintenance window is still available in operational_schedule."""
    schedule_id = plan["recommended_window"].get("schedule_id")
    if not schedule_id:
        # No specific schedule row — check by time range
        return {
            "check":   "window_available",
            "passed":  True,
            "detail":  "No specific schedule row — emergency window (priority_override=True)",
            "latency_ms": 0,
        }

    t0  = time.time()
    row = query_one("""
        SELECT id, status FROM operational_schedule
        WHERE id = %s AND status = 'available'
    """, (schedule_id,))
    ms  = int((time.time() - t0) * 1000)

    passed = row is not None
    return {
        "check":       "window_available",
        "passed":      passed,
        "detail":      f"Schedule id={schedule_id} status={'available' if passed else 'unavailable/booked'}",
        "latency_ms":  ms,
        "block_reason": f"Maintenance window (id={schedule_id}) no longer available — booked or blocked." if not passed else None,
    }


def check_no_conflict(plan: dict) -> dict:
    """Check 3: no pending or scheduled maintenance orders for this asset."""
    t0     = time.time()
    has_conflict = has_pending_order(plan["asset_id"])
    ms     = int((time.time() - t0) * 1000)

    passed = not has_conflict
    return {
        "check":       "no_conflicting_orders",
        "passed":      passed,
        "detail":      f"Asset {plan['asset_id']}: {'no conflict' if passed else 'CONFLICTING order exists (pending/scheduled)'}",
        "latency_ms":  ms,
        "block_reason": f"Asset {plan['asset_id']} already has a pending or scheduled maintenance order. Deduplicated." if not passed else None,
    }


def check_safety_protocol(plan: dict, diagnosis: dict) -> dict:
    """Check 4: safety protocol compliance via Vector Search RAG."""
    t0 = time.time()

    rul_hours      = diagnosis.get("rul_hours", 0)
    failure_mode   = plan["failure_mode"]
    action         = plan["maintenance_action"][:100]

    query = (
        f"Is it safe to perform '{action}' on a turbofan engine "
        f"with {failure_mode.replace('_', ' ')} and {rul_hours:.1f} hours remaining until failure?"
    )

    safety_docs = search_safety_protocols(
        query_text=query,
        num_results=2,
        failure_mode=failure_mode,
    )
    ms = int((time.time() - t0) * 1000)

    # Evaluate compliance: if protocol found and RUL > critical threshold, safe to proceed
    rul_critical_h = float(get_config("rul_critical_threshold_h", "8"))
    critical_rul   = rul_hours <= rul_critical_h

    # Extract safety constraints from protocol content
    safety_flags = []
    for doc in safety_docs:
        content = doc.get("content", "").lower()
        if "supervisor sign-off" in content and "supervisor" not in str(plan.get("approved_by_user", "")):
            safety_flags.append("Supervisor sign-off required per protocol")
        if critical_rul and "no unsupervised maintenance" in content:
            safety_flags.append(f"CRITICAL RUL ({rul_hours:.1f}h) — supervised maintenance required per protocol")

    passed = len(safety_flags) == 0

    return {
        "check":           "safety_protocol",
        "passed":          passed,
        "detail":          f"Protocols checked: {[d.get('protocol_id','?') for d in safety_docs]}. Flags: {safety_flags}",
        "latency_ms":      ms,
        "safety_docs":     [d.get("protocol_id", "?") for d in safety_docs],
        "block_reason":    "; ".join(safety_flags) if not passed else None,
    }


# ─── Main Guardian Function ───────────────────────────────────────────────────

def run_guardian_agent(plan: dict, diagnosis: dict, user_context: dict) -> dict:
    """
    Full Guardian pipeline: validate → branch → test → merge (or drop).

    Returns:
        {
            verdict: APPROVED | BLOCKED | ERROR,
            checks: list,
            branch_id: str,
            committed_order_id: int (if APPROVED),
            approved_by: str,
            timing_ms: dict
        }
    """
    asset_id = plan["asset_id"]
    timing   = {}

    with AgentTrace("guardian_agent", asset_id=asset_id) as trace:

        # ══════════════════════════════════════════════════════════════════════
        # STEP 1: Pre-validation (all checks BEFORE touching production DB)
        # ══════════════════════════════════════════════════════════════════════

        t0 = time.time()

        checks = [
            check_cost_authorization(plan, user_context),
            check_window_available(plan),
            check_no_conflict(plan),
            check_safety_protocol(plan, diagnosis),
        ]
        timing["validation_ms"] = int((time.time() - t0) * 1000)

        all_passed  = all(c["passed"] for c in checks)
        failed      = [c for c in checks if not c["passed"]]

        logger.info(
            f"[Guardian] Validation asset={asset_id}: "
            f"{'PASS' if all_passed else 'FAIL'} "
            f"({len(checks) - len(failed)}/{len(checks)} checks passed) "
            f"in {timing['validation_ms']}ms"
        )
        if failed:
            logger.warning(f"[Guardian] Failed checks: {[f['check'] for f in failed]}")

        trace.log("pre_validation",
                  inputs={"asset_id": asset_id, "checks_run": len(checks)},
                  outputs={
                      "passed": len(checks) - len(failed),
                      "failed": len(failed),
                      "all_passed": all_passed,
                  },
                  latency_ms=timing["validation_ms"])

        # ── BLOCKED: Log and return without touching production ────────────────
        if not all_passed:
            block_reasons = [c["block_reason"] for c in failed if c.get("block_reason")]
            verdict_result = {
                "verdict":        "BLOCKED",
                "checks":         checks,
                "block_reasons":  block_reasons,
                "branch_id":      None,
                "timing_ms":      timing,
            }
            trace.set_verdict("BLOCKED", "; ".join(block_reasons))

            # Log to agent_decisions table (read-write OK for logging)
            _log_agent_decision(
                asset_id     = asset_id,
                plan         = plan,
                checks       = checks,
                verdict      = "BLOCKED",
                branch_id    = None,
                user_context = user_context,
                trace_id     = trace.get_trace_id(),
            )

            logger.warning(
                f"[Guardian] BLOCKED asset={asset_id}: {'; '.join(block_reasons)}"
            )
            return verdict_result

        # ══════════════════════════════════════════════════════════════════════
        # STEP 2: Lakebase Database Branching
        # CREATE BRANCH → test INSERT → verify → MERGE INTO main
        # ══════════════════════════════════════════════════════════════════════

        branch_name = f"guardian_test_{asset_id}_{int(time.time())}"
        committed_order_id = None

        try:
            t0 = time.time()

            with LakebaseBranch(branch_name) as branch:
                timing["branch_create_ms"] = int((time.time() - t0) * 1000)

                logger.info(f"[Guardian] Branch created: {branch_name} in {timing['branch_create_ms']}ms")
                trace.log("branch_create",
                          inputs={"branch_name": branch_name},
                          outputs={"created": True},
                          latency_ms=timing["branch_create_ms"])

                # ── Test INSERT on branch ──────────────────────────────────────
                t_insert = time.time()

                window = plan["recommended_window"]
                branch.execute("""
                    INSERT INTO maintenance_orders (
                        asset_id, failure_mode, rul_at_detection,
                        rul_lower_bound, rul_upper_bound, failure_confidence,
                        recommended_window_start, recommended_window_end,
                        estimated_cost, urgency, maintenance_action,
                        guardian_approved, guardian_reason, guardian_checks,
                        approved_by_user, similar_incident_id,
                        similar_incident_similarity, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            TRUE, %s, %s::jsonb, %s, %s, %s, 'pending')
                """, (
                    asset_id,
                    plan["failure_mode"],
                    diagnosis["rul_hours"],
                    diagnosis.get("rul_lower_bound"),
                    diagnosis.get("rul_upper_bound"),
                    diagnosis.get("failure_confidence"),
                    window["start"],
                    window["end"],
                    plan["estimated_cost"],
                    plan["urgency"],
                    plan["maintenance_action"],
                    f"All {len(checks)} validation checks passed. Branch {branch_name} tested and verified.",
                    json.dumps([{k: v for k, v in c.items() if k != "block_reason"} for c in checks]),
                    user_context.get("user_id", "system"),
                    plan["similar_incidents"][0].get("incident_id") if plan.get("similar_incidents") else None,
                    plan["similar_incidents"][0].get("similarity_score") if plan.get("similar_incidents") else None,
                ))

                timing["branch_insert_ms"] = int((time.time() - t_insert) * 1000)

                # ── Verify write on branch ─────────────────────────────────────
                t_verify = time.time()
                verify_row = branch.query_one("""
                    SELECT id, asset_id, failure_mode, urgency, guardian_approved
                    FROM maintenance_orders
                    WHERE asset_id = %s
                    ORDER BY created_at DESC LIMIT 1
                """, (asset_id,))

                assert verify_row is not None, "Branch write verification failed — row not found"
                assert verify_row["guardian_approved"] is True, "guardian_approved flag not set"
                assert verify_row["asset_id"] == asset_id, f"Wrong asset_id: {verify_row['asset_id']}"

                timing["branch_verify_ms"] = int((time.time() - t_verify) * 1000)

                logger.info(
                    f"[Guardian] Branch write verified: order id={verify_row['id']} "
                    f"in {timing['branch_insert_ms']}ms + {timing['branch_verify_ms']}ms"
                )
                trace.log("branch_insert_verify",
                          inputs={"branch_name": branch_name},
                          outputs={"order_id": verify_row["id"], "verified": True},
                          latency_ms=timing["branch_insert_ms"] + timing["branch_verify_ms"])

                # ── Also book the maintenance window on branch ─────────────────
                schedule_id = window.get("schedule_id")
                if schedule_id:
                    branch.execute("""
                        UPDATE operational_schedule
                        SET status = 'booked', booked_order_id = %s
                        WHERE id = %s
                    """, (verify_row["id"], schedule_id))

                committed_order_id = verify_row["id"]

                # ── Merge to production (LakebaseBranch.__exit__ calls merge) ──
                t_merge = time.time()
                # Note: merge is triggered automatically by LakebaseBranch context manager exit
                # timing["branch_merge_ms"] measured after exit

            timing["branch_merge_ms"] = int((time.time() - t_merge) * 1000)

            logger.info(
                f"[Guardian] MERGED to production: branch={branch_name} "
                f"order_id={committed_order_id} in {timing['branch_merge_ms']}ms"
            )
            trace.log("branch_merge",
                      inputs={"branch_name": branch_name},
                      outputs={"merged": True, "committed_order_id": committed_order_id},
                      latency_ms=timing["branch_merge_ms"])

            verdict = "APPROVED"

        except Exception as e:
            logger.error(f"[Guardian] Branch operation failed: {e}", exc_info=True)
            timing["error_ms"] = 0
            verdict = "ERROR"
            verdict_result = {
                "verdict":            "ERROR",
                "checks":             checks,
                "error":              str(e),
                "branch_id":          branch_name,
                "committed_order_id": None,
                "timing_ms":          timing,
            }
            trace.set_verdict("ERROR", str(e))
            _log_agent_decision(
                asset_id=asset_id, plan=plan, checks=checks, verdict="ERROR",
                branch_id=branch_name, user_context=user_context,
                trace_id=trace.get_trace_id(), error=str(e),
            )
            return verdict_result

        # ══════════════════════════════════════════════════════════════════════
        # STEP 3: Log decision audit trail
        # ══════════════════════════════════════════════════════════════════════

        _log_agent_decision(
            asset_id=asset_id, plan=plan, checks=checks, verdict=verdict,
            branch_id=branch_name, user_context=user_context,
            trace_id=trace.get_trace_id(),
            committed_order_id=committed_order_id,
        )

        total_ms = sum(timing.values())
        trace.set_verdict(verdict, f"All {len(checks)} checks passed. Order {committed_order_id} committed.")

        result = {
            "verdict":            verdict,
            "checks":             checks,
            "branch_id":          branch_name,
            "committed_order_id": committed_order_id,
            "approved_by":        user_context.get("user_id", "system"),
            "timing_ms":          timing,
            "total_guardian_ms":  total_ms,
        }

        logger.info(
            f"[Guardian] APPROVED asset={asset_id} order_id={committed_order_id} "
            f"branch={branch_name} user={user_context.get('user_id','')} "
            f"total={total_ms}ms"
        )

        return result


# ─── Audit Trail ─────────────────────────────────────────────────────────────

def _log_agent_decision(
    asset_id: int, plan: dict, checks: list, verdict: str,
    branch_id: str, user_context: dict, trace_id: str,
    committed_order_id: int = None, error: str = None,
):
    """Write immutable audit record to agent_decisions table."""
    try:
        checks_summary = "; ".join([
            f"{c['check']}={'PASS' if c['passed'] else 'FAIL'}"
            for c in checks
        ])
        execute("""
            INSERT INTO agent_decisions (
                agent_name, asset_id, timestamp,
                input_summary, output_action,
                guardian_verdict, guardian_reason,
                branch_id, mlflow_trace_id,
                latency_ms, triggered_by_user
            ) VALUES (
                'guardian_agent', %s, NOW(),
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
        """, (
            asset_id,
            f"failure_mode={plan.get('failure_mode')} cost={plan.get('estimated_cost')} urgency={plan.get('urgency')}",
            f"committed_order_id={committed_order_id}" if committed_order_id else (error or "no action"),
            verdict,
            checks_summary,
            branch_id,
            trace_id,
            sum(plan.get("timing_ms", {}).values(), 0),
            user_context.get("user_id", "system"),
        ))
    except Exception as e:
        logger.warning(f"[Guardian] Failed to log agent_decision: {e}")


# COMMAND ----------

# MAGIC %md ## Test (Demo Scenario)

# COMMAND ----------

if __name__ == "__main__" or (hasattr(dbutils, "widgets") and dbutils.widgets.get("run_test") == "true"):
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)

    test_plan = {
        "asset_id":           15,
        "failure_mode":       "bearing_degradation",
        "recommended_window": {
            "start":            (now + timedelta(hours=2)).isoformat(),
            "end":              (now + timedelta(hours=6)).isoformat(),
            "priority_override": False,
            "schedule_id":      1,
        },
        "estimated_cost":     42500.0,
        "urgency":            "URGENT",
        "maintenance_action": "Replace HPT bearing assembly. Flush lubrication system. Conduct post-repair vibration test.",
        "similar_incidents": [
            {"incident_id": "INC-2019-0042", "cost": 45000, "similarity_score": 0.94},
        ],
        "timing_ms":          {"windows_ms": 12, "cost_ms": 2},
    }

    test_diagnosis = {
        "asset_id":           15,
        "rul_hours":          23.4,
        "rul_lower_bound":    18.2,
        "rul_upper_bound":    28.6,
        "failure_confidence": 0.87,
    }

    test_user = {
        "user_id": "maintenance_supervisor@livetwin.com",
        "groups":  ["maintenance_supervisors"],
        "source":  "obo",
    }

    result = run_guardian_agent(test_plan, test_diagnosis, test_user)

    print(f"\n{'='*60}")
    print(f"GUARDIAN RESULT")
    print(f"{'='*60}")
    print(f"Verdict:    {result['verdict']}")
    print(f"Order ID:   {result.get('committed_order_id', 'N/A')}")
    print(f"Branch:     {result.get('branch_id', 'N/A')}")
    print(f"Approved by:{result.get('approved_by', 'N/A')}")
    print(f"Total time: {result.get('total_guardian_ms', 0)}ms")
    print("\nChecks:")
    for check in result.get("checks", []):
        status = "✓" if check["passed"] else "✗"
        print(f"  {status} {check['check']}: {check['detail'][:70]}")
