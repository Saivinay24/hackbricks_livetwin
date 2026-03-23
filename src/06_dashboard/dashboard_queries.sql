-- LiveTwin — Databricks SQL Dashboard Queries
-- Import each query as a separate widget in Databricks SQL Dashboards
-- Dashboard name: "LiveTwin Operations Center"

-- ============================================================
-- WIDGET 1: Fleet Health Heatmap
-- Visualization: Heatmap table (color: green=healthy, yellow=degrading, red=critical/failure_imminent)
-- ============================================================

SELECT
    g.asset_id,
    a.name                                     AS engine_name,
    a.plant,
    a.location,
    ROUND(g.health_score, 1)                   AS health_score,
    g.health_status,
    ROUND(g.avg_anomaly_score, 3)              AS avg_anomaly_score,
    g.anomaly_count_24h,
    g.total_cycles,
    CASE
        WHEN g.health_status = 'healthy'          THEN '#22c55e'  -- green
        WHEN g.health_status = 'degrading'        THEN '#f59e0b'  -- amber
        WHEN g.health_status = 'critical'         THEN '#ef4444'  -- red
        WHEN g.health_status = 'failure_imminent' THEN '#7c3aed'  -- purple
        ELSE '#6b7280'
    END                                        AS status_color,
    g.last_seen_ts
FROM livetwin.processed.gold_engine_health g
JOIN livetwin.operational.assets a ON a.id = g.asset_id
ORDER BY g.health_score ASC;

-- ============================================================
-- WIDGET 2: Fleet Summary KPIs (counter cards)
-- ============================================================

SELECT
    total_engines,
    healthy_count                                                    AS healthy,
    degrading_count                                                  AS degrading,
    critical_count + failure_imminent_count                          AS critical_or_failing,
    ROUND(fleet_avg_health_score, 1)                                 AS avg_fleet_health,
    fleet_anomaly_count_24h                                          AS anomalies_24h,
    summary_ts
FROM livetwin.processed.gold_fleet_summary
LIMIT 1;

-- ============================================================
-- WIDGET 3: Recent Maintenance Orders (Agent Decision Timeline)
-- ============================================================

SELECT
    mo.id                                       AS order_id,
    mo.asset_id,
    a.name                                      AS engine_name,
    a.plant,
    mo.failure_mode,
    ROUND(mo.rul_at_detection, 1)               AS rul_at_detection_h,
    mo.urgency,
    ROUND(mo.estimated_cost, 0)                 AS estimated_cost_inr,
    mo.guardian_approved,
    mo.approved_by_user,
    mo.status,
    mo.recommended_window_start                 AS maintenance_window_start,
    TIMESTAMPDIFF(HOUR, mo.created_at, mo.recommended_window_start) AS hours_until_maintenance,
    mo.similar_incident_id,
    ROUND(mo.similar_incident_similarity * 100, 0)  AS similarity_pct,
    mo.created_at
FROM livetwin.operational.maintenance_orders mo
JOIN livetwin.operational.assets a ON a.id = mo.asset_id
ORDER BY mo.created_at DESC
LIMIT 20;

-- ============================================================
-- WIDGET 4: Agent Decision Audit Log
-- ============================================================

SELECT
    ad.id,
    ad.agent_name,
    ad.asset_id,
    a.name                                      AS engine_name,
    ad.guardian_verdict,
    ad.triggered_by_user,
    ad.input_summary,
    ad.output_action,
    ad.guardian_reason,
    ad.latency_ms,
    ad.mlflow_trace_id,
    ad.timestamp
FROM livetwin.operational.agent_decisions ad
LEFT JOIN livetwin.operational.assets a ON a.id = ad.asset_id
WHERE ad.agent_name = 'guardian_agent'
ORDER BY ad.timestamp DESC
LIMIT 50;

-- ============================================================
-- WIDGET 5: Guardian Verdict Distribution (Pie chart)
-- ============================================================

SELECT
    guardian_verdict,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM livetwin.operational.agent_decisions
WHERE agent_name = 'guardian_agent'
GROUP BY guardian_verdict
ORDER BY count DESC;

-- ============================================================
-- WIDGET 6: Cost Savings Counter
-- Genie query equivalent: "How much did LiveTwin save this month?"
-- ============================================================

WITH failure_costs AS (
    -- Average cost of unplanned failure (industry benchmark ~3x planned maintenance)
    SELECT
        failure_mode,
        AVG(estimated_cost) * 3.0       AS avg_unplanned_cost,
        AVG(estimated_cost)              AS avg_planned_cost,
        AVG(estimated_cost) * 2.0        AS savings_per_order
    FROM livetwin.operational.maintenance_orders
    WHERE guardian_approved = TRUE
      AND created_at >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
    GROUP BY failure_mode
),
monthly_orders AS (
    SELECT
        mo.failure_mode,
        COUNT(*) AS orders_this_month,
        SUM(mo.estimated_cost) AS total_maintenance_cost
    FROM livetwin.operational.maintenance_orders mo
    WHERE mo.guardian_approved = TRUE
      AND mo.created_at >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
    GROUP BY mo.failure_mode
)
SELECT
    m.failure_mode,
    m.orders_this_month,
    ROUND(m.total_maintenance_cost, 0)       AS maintenance_cost_inr,
    ROUND(fc.avg_unplanned_cost * m.orders_this_month, 0) AS avoided_cost_inr,
    ROUND(fc.savings_per_order * m.orders_this_month, 0)  AS net_savings_inr
FROM monthly_orders m
JOIN failure_costs fc ON fc.failure_mode = m.failure_mode
ORDER BY net_savings_inr DESC;

-- ============================================================
-- WIDGET 7: RUL at Detection (model validation — predicted vs timeline)
-- ============================================================

SELECT
    mo.id,
    mo.asset_id,
    a.name                                  AS engine_name,
    mo.failure_mode,
    ROUND(mo.rul_at_detection, 1)           AS rul_predicted_h,
    ROUND(mo.rul_lower_bound, 1)            AS rul_lower_h,
    ROUND(mo.rul_upper_bound, 1)            AS rul_upper_h,
    ROUND(mo.failure_confidence * 100, 0)   AS confidence_pct,
    mo.urgency,
    mo.created_at
FROM livetwin.operational.maintenance_orders mo
JOIN livetwin.operational.assets a ON a.id = mo.asset_id
WHERE mo.guardian_approved = TRUE
ORDER BY mo.created_at DESC
LIMIT 30;

-- ============================================================
-- WIDGET 8: Sensor Trend — Engine 15 (Demo Focus)
-- ============================================================

SELECT
    sr.timestamp                            AS reading_time,
    sr.cycle,
    sr.anomaly_score,
    sr.anomaly_flag,
    CAST(sr.sensors->>'s2'  AS FLOAT)       AS s2_lpc_outlet_temp,
    CAST(sr.sensors->>'s4'  AS FLOAT)       AS s4_lpt_outlet_temp,
    CAST(sr.sensors->>'s7'  AS FLOAT)       AS s7_hpc_pressure,
    CAST(sr.sensors->>'s11' AS FLOAT)       AS s11_hpc_static_pressure,
    CAST(sr.sensors->>'s12' AS FLOAT)       AS s12_fuel_ratio
FROM livetwin.operational.sensor_readings sr
WHERE sr.asset_id = 15
ORDER BY sr.timestamp DESC
LIMIT 200;

-- ============================================================
-- WIDGET 9: ABAC Governance Demo — OBO Access by User
-- ============================================================

SELECT
    triggered_by_user,
    COUNT(*)                                AS decisions_made,
    COUNT(DISTINCT asset_id)                AS assets_touched,
    SUM(CASE WHEN guardian_verdict = 'APPROVED' THEN 1 ELSE 0 END)  AS approved,
    SUM(CASE WHEN guardian_verdict = 'BLOCKED'  THEN 1 ELSE 0 END)  AS blocked,
    MAX(timestamp)                          AS last_action
FROM livetwin.operational.agent_decisions
GROUP BY triggered_by_user
ORDER BY decisions_made DESC;

-- ============================================================
-- WIDGET 10: Active Agent Chains (real-time)
-- ============================================================

SELECT
    ai.asset_id,
    a.name        AS engine_name,
    a.plant,
    ai.stage      AS current_stage,
    ai.started_at,
    TIMESTAMPDIFF(SECOND, ai.started_at, NOW()) AS seconds_in_chain
FROM livetwin.operational.agent_inflight ai
JOIN livetwin.operational.assets a ON a.id = ai.asset_id
ORDER BY ai.started_at;
