-- LiveTwin — Lakebase (PostgreSQL-compatible OLTP) Schema
-- Run against your Lakebase endpoint via psql or DBeaver
-- Lakebase GA Feb 2026 — requires Databricks Premium workspace

-- ============================================================
-- 1. ASSETS (static master data — what machines we monitor)
-- ============================================================

CREATE TABLE IF NOT EXISTS assets (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL,
    type                VARCHAR(50)  NOT NULL DEFAULT 'turbofan',
    location            VARCHAR(100),
    plant               VARCHAR(10)  NOT NULL,  -- 'plant_a' or 'plant_b'
    status              VARCHAR(20)  NOT NULL DEFAULT 'operational',
    total_cycles        INTEGER      DEFAULT 0,
    last_reading_ts     TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Seed: 20 engines — 1-10 Plant A, 11-20 Plant B
INSERT INTO assets (id, name, type, location, plant) VALUES
    (1,  'Engine-01', 'turbofan', 'Plant A Bay 1',  'plant_a'),
    (2,  'Engine-02', 'turbofan', 'Plant A Bay 1',  'plant_a'),
    (3,  'Engine-03', 'turbofan', 'Plant A Bay 2',  'plant_a'),
    (4,  'Engine-04', 'turbofan', 'Plant A Bay 2',  'plant_a'),
    (5,  'Engine-05', 'turbofan', 'Plant A Bay 3',  'plant_a'),
    (6,  'Engine-06', 'turbofan', 'Plant A Bay 3',  'plant_a'),
    (7,  'Engine-07', 'turbofan', 'Plant A Bay 4',  'plant_a'),
    (8,  'Engine-08', 'turbofan', 'Plant A Bay 4',  'plant_a'),
    (9,  'Engine-09', 'turbofan', 'Plant A Bay 5',  'plant_a'),
    (10, 'Engine-10', 'turbofan', 'Plant A Bay 5',  'plant_a'),
    (11, 'Engine-11', 'turbofan', 'Plant B Bay 1',  'plant_b'),
    (12, 'Engine-12', 'turbofan', 'Plant B Bay 1',  'plant_b'),
    (13, 'Engine-13', 'turbofan', 'Plant B Bay 2',  'plant_b'),
    (14, 'Engine-14', 'turbofan', 'Plant B Bay 2',  'plant_b'),
    (15, 'Engine-15', 'turbofan', 'Plant B Bay 3',  'plant_b'),  -- DEMO failure engine
    (16, 'Engine-16', 'turbofan', 'Plant B Bay 3',  'plant_b'),
    (17, 'Engine-17', 'turbofan', 'Plant B Bay 4',  'plant_b'),
    (18, 'Engine-18', 'turbofan', 'Plant B Bay 4',  'plant_b'),
    (19, 'Engine-19', 'turbofan', 'Plant B Bay 5',  'plant_b'),
    (20, 'Engine-20', 'turbofan', 'Plant B Bay 5',  'plant_b')
ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- 2. SENSOR READINGS (hot table — last 1 hour only)
--    Historical data lives in Silver/Gold Delta tables
-- ============================================================

CREATE TABLE IF NOT EXISTS sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        INTEGER      NOT NULL REFERENCES assets(id),
    timestamp       TIMESTAMPTZ  NOT NULL,
    cycle           INTEGER,
    sensors         JSONB,          -- {"s1": 0.89, "s2": 643.02, ..., "s21": 23.4}
    op_settings     JSONB,          -- {"op1": 0.0, "op2": 0.0, "op3": 100.0}
    anomaly_score   FLOAT        DEFAULT 0.0,
    anomaly_flag    BOOLEAN      DEFAULT FALSE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_asset_ts ON sensor_readings (asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_anomaly  ON sensor_readings (anomaly_flag) WHERE anomaly_flag = TRUE;

-- TTL: keep only last 1 hour. Run via a Databricks job every 10 minutes.
-- DELETE FROM sensor_readings WHERE timestamp < NOW() - INTERVAL '1 hour';

-- ============================================================
-- 3. AGENT DECISIONS (immutable audit log)
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_decisions (
    id                  BIGSERIAL PRIMARY KEY,
    agent_name          VARCHAR(50)  NOT NULL,
    asset_id            INTEGER      REFERENCES assets(id),
    timestamp           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    input_summary       TEXT,
    output_action       TEXT,
    guardian_verdict    VARCHAR(20),   -- APPROVED / BLOCKED / ERROR
    guardian_reason     TEXT,
    branch_id           VARCHAR(100),
    mlflow_trace_id     VARCHAR(200),
    latency_ms          INTEGER,
    triggered_by_user   VARCHAR(100)   -- OBO: the human who triggered this chain
);

CREATE INDEX IF NOT EXISTS idx_decisions_asset ON agent_decisions (asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_verdict ON agent_decisions (guardian_verdict);

-- ============================================================
-- 4. MAINTENANCE ORDERS (written exclusively by Guardian Agent)
-- ============================================================

CREATE TABLE IF NOT EXISTS maintenance_orders (
    id                          BIGSERIAL PRIMARY KEY,
    asset_id                    INTEGER      NOT NULL REFERENCES assets(id),
    created_at                  TIMESTAMPTZ  DEFAULT NOW(),
    failure_mode                VARCHAR(50),  -- bearing_degradation / thermal / vibration / fuel_system / compressor
    rul_at_detection            FLOAT,        -- hours
    rul_lower_bound             FLOAT,
    rul_upper_bound             FLOAT,
    failure_confidence          FLOAT,        -- 0.0 – 1.0
    recommended_window_start    TIMESTAMPTZ,
    recommended_window_end      TIMESTAMPTZ,
    estimated_cost              DECIMAL(12,2),
    urgency                     VARCHAR(20)   DEFAULT 'SCHEDULED',  -- SCHEDULED / URGENT / CRITICAL
    maintenance_action          TEXT,
    guardian_approved           BOOLEAN       NOT NULL,
    guardian_reason             TEXT,
    guardian_checks             JSONB,        -- all 4 check results
    branch_id                   VARCHAR(100),
    agent_decision_id           BIGINT        REFERENCES agent_decisions(id),
    approved_by_user            VARCHAR(100),  -- OBO: operator who triggered chain
    similar_incident_id         VARCHAR(50),   -- top Vector Search match
    similar_incident_similarity FLOAT,
    status                      VARCHAR(20)   DEFAULT 'pending',  -- pending / scheduled / in_progress / completed / cancelled
    updated_at                  TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_asset    ON maintenance_orders (asset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON maintenance_orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_urgency  ON maintenance_orders (urgency) WHERE status = 'pending';

-- ============================================================
-- 5. OPERATIONAL SCHEDULE (maintenance windows)
-- ============================================================

CREATE TABLE IF NOT EXISTS operational_schedule (
    id              SERIAL PRIMARY KEY,
    asset_id        INTEGER     NOT NULL REFERENCES assets(id),
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    status          VARCHAR(20) DEFAULT 'available',  -- available / booked / blocked
    priority_override BOOLEAN   DEFAULT FALSE,
    booked_order_id   BIGINT    REFERENCES maintenance_orders(id),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_asset ON operational_schedule (asset_id, window_start);
CREATE INDEX IF NOT EXISTS idx_schedule_avail ON operational_schedule (status, window_start) WHERE status = 'available';

-- Seed: 5 upcoming maintenance windows per engine
DO $$
DECLARE
    engine_id INTEGER;
    window_offset INTERVAL;
    i INTEGER;
BEGIN
    FOR engine_id IN 1..20 LOOP
        FOR i IN 1..5 LOOP
            window_offset := (i * 12 || ' hours')::INTERVAL;
            INSERT INTO operational_schedule (asset_id, window_start, window_end, status)
            VALUES (
                engine_id,
                NOW() + window_offset,
                NOW() + window_offset + INTERVAL '4 hours',
                'available'
            ) ON CONFLICT DO NOTHING;
        END LOOP;
    END LOOP;
END $$;

-- ============================================================
-- 6. CONFIG (agent thresholds + cost limits)
-- ============================================================

CREATE TABLE IF NOT EXISTS config (
    key         VARCHAR(100) PRIMARY KEY,
    value       TEXT         NOT NULL,
    description TEXT,
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

INSERT INTO config (key, value, description) VALUES
    ('anomaly_score_threshold',  '0.75',  'Anomaly score above which Monitor Agent triggers diagnosis'),
    ('rul_urgent_threshold_h',   '24',    'RUL hours below which urgency = URGENT'),
    ('rul_critical_threshold_h', '8',     'RUL hours below which urgency = CRITICAL'),
    ('cost_limit_operator',      '50000', 'Max maintenance cost (INR) plant operator can approve'),
    ('cost_limit_supervisor',    '500000','Max maintenance cost (INR) supervisor can approve'),
    ('monitor_poll_interval_ms', '100',   'Monitor Agent fleet scan cycle in milliseconds'),
    ('demo_failure_engine_id',   '15',    'Engine pre-seeded for reliable demo failure'),
    ('demo_replay_speed',        '10',    'Replay speed multiplier for CMAPSS stream (10 = 10x faster)')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

-- ============================================================
-- 7. AGENT IN-FLIGHT REGISTRY (deduplication)
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_inflight (
    asset_id    INTEGER     PRIMARY KEY REFERENCES assets(id),
    started_at  TIMESTAMPTZ DEFAULT NOW(),
    stage       VARCHAR(50) DEFAULT 'monitor'  -- monitor / diagnosis / plan / guardian
);

-- ============================================================
-- Verify
-- ============================================================

SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
