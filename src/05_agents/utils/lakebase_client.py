"""
LiveTwin — Lakebase Client Utility
PostgreSQL-compatible OLTP client for Lakebase operations.
Sub-10ms indexed reads, database branching for Guardian pattern.
"""

import os
import json
import time
import logging
from contextlib import contextmanager
from typing import Optional, Any

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)


# ─── Connection config (loaded from Databricks Secrets) ─────────────────────

def _get_conn_params():
    """Load connection params from Databricks Secrets or environment vars."""
    try:
        from databricks.sdk.runtime import dbutils
        return {
            "host":     dbutils.secrets.get("livetwin", "lakebase_host"),
            "port":     5432,
            "dbname":   "livetwin_ops",
            "user":     "livetwin_agent",
            "password": dbutils.secrets.get("livetwin", "lakebase_password"),
        }
    except Exception:
        # Fallback to env vars for local/CI usage
        return {
            "host":     os.environ.get("LAKEBASE_HOST", "localhost"),
            "port":     int(os.environ.get("LAKEBASE_PORT", 5432)),
            "dbname":   os.environ.get("LAKEBASE_DB", "livetwin_ops"),
            "user":     os.environ.get("LAKEBASE_USER", "livetwin_agent"),
            "password": os.environ.get("LAKEBASE_PASSWORD", ""),
        }


# ─── Connection Pool ─────────────────────────────────────────────────────────

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        params = _get_conn_params()
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            connect_timeout=5,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            **params,
        )
        logger.info("Lakebase connection pool created")
    return _pool


@contextmanager
def get_conn():
    """Context manager: borrow a connection from pool, return on exit."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ─── Query helpers ───────────────────────────────────────────────────────────

def query_one(sql: str, params=None) -> Optional[dict]:
    """Execute SQL and return one row as a dict (or None)."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def query_all(sql: str, params=None) -> list[dict]:
    """Execute SQL and return all rows as list of dicts."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def execute(sql: str, params=None) -> int:
    """Execute a DML statement. Returns rowcount."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount


def execute_many(sql: str, records: list) -> int:
    """Batch insert via execute_values. Returns rowcount."""
    with get_conn() as conn:
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        conn.commit()
        return cur.rowcount


# ─── Config helper ───────────────────────────────────────────────────────────

_config_cache: dict[str, str] = {}


def get_config(key: str, default: str = "") -> str:
    """Fetch config value from Lakebase config table with in-process cache."""
    if key in _config_cache:
        return _config_cache[key]
    row = query_one("SELECT value FROM config WHERE key = %s", (key,))
    val = row["value"] if row else default
    _config_cache[key] = val
    return val


# ─── Asset helpers ───────────────────────────────────────────────────────────

def get_asset(asset_id: int) -> Optional[dict]:
    return query_one("SELECT * FROM assets WHERE id = %s", (asset_id,))


def get_latest_sensor_reading(asset_id: int) -> Optional[dict]:
    """Sub-10ms indexed lookup — primary use case for Lakebase over Delta."""
    return query_one("""
        SELECT * FROM sensor_readings
        WHERE asset_id = %s
        ORDER BY timestamp DESC LIMIT 1
    """, (asset_id,))


def get_sensor_history(asset_id: int, limit: int = 100) -> list[dict]:
    """Get recent sensor readings for an engine."""
    return query_all("""
        SELECT * FROM sensor_readings
        WHERE asset_id = %s
        ORDER BY timestamp DESC LIMIT %s
    """, (asset_id, limit))


def get_fleet_anomalies(window_seconds: int = 30) -> list[dict]:
    """
    Fleet-wide anomaly scan. Used by Monitor Agent.
    Real-Time Spark Mode equivalent: sub-300ms query via indexed scan.
    """
    return query_all("""
        SELECT sr.asset_id, sr.timestamp, sr.anomaly_score, sr.anomaly_flag,
               sr.sensors, sr.cycle, a.name, a.plant
        FROM sensor_readings sr
        JOIN assets a ON a.id = sr.asset_id
        WHERE sr.timestamp > NOW() - INTERVAL '%(window)s seconds'
          AND sr.anomaly_flag = TRUE
          AND sr.asset_id NOT IN (
              SELECT DISTINCT asset_id FROM maintenance_orders
              WHERE status IN ('pending', 'scheduled')
          )
          AND sr.asset_id NOT IN (
              SELECT asset_id FROM agent_inflight
          )
        ORDER BY sr.anomaly_score DESC
        LIMIT 5
    """, {"window": window_seconds})


def get_available_windows(asset_id: int, hours_ahead: float) -> list[dict]:
    """Get available maintenance windows within urgency timeframe."""
    return query_all("""
        SELECT id, asset_id, window_start, window_end, priority_override
        FROM operational_schedule
        WHERE asset_id = %s
          AND window_start <= NOW() + INTERVAL '%(hours)s hours'
          AND window_start >= NOW()
          AND status = 'available'
        ORDER BY window_start
        LIMIT 5
    """, {"hours": hours_ahead}, )
    # Note: psycopg2 params must be positional; using dict-style as example — adjust to %s tuple in real usage


def get_available_windows_v2(asset_id: int, hours_ahead: float) -> list[dict]:
    """Get available maintenance windows within urgency timeframe."""
    return query_all("""
        SELECT id, asset_id, window_start, window_end, priority_override
        FROM operational_schedule
        WHERE asset_id = %s
          AND window_start <= NOW() + (%s || ' hours')::INTERVAL
          AND window_start >= NOW()
          AND status = 'available'
        ORDER BY window_start
        LIMIT 5
    """, (asset_id, int(hours_ahead)))


def has_pending_order(asset_id: int) -> bool:
    """Check if asset already has a pending or scheduled maintenance order."""
    row = query_one("""
        SELECT COUNT(*) as cnt FROM maintenance_orders
        WHERE asset_id = %s AND status IN ('pending', 'scheduled')
    """, (asset_id,))
    return row["cnt"] > 0 if row else False


def get_user_cost_limit(user_id: str) -> float:
    """Get cost authorization limit for OBO user."""
    row = query_one("""
        SELECT value FROM config
        WHERE key = CASE
            WHEN %s LIKE '%%supervisor%%' THEN 'cost_limit_supervisor'
            ELSE 'cost_limit_operator'
        END
    """, (user_id,))
    return float(row["value"]) if row else 50000.0


# ─── In-flight deduplication ─────────────────────────────────────────────────

def mark_in_flight(asset_id: int, stage: str = "monitor") -> bool:
    """Mark an asset as being processed by the agent chain. Returns False if already in-flight."""
    try:
        execute("""
            INSERT INTO agent_inflight (asset_id, stage, started_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (asset_id) DO NOTHING
        """, (asset_id, stage))
        return True
    except Exception:
        return False


def is_in_flight(asset_id: int) -> bool:
    row = query_one("SELECT asset_id FROM agent_inflight WHERE asset_id = %s", (asset_id,))
    return row is not None


def clear_in_flight(asset_id: int):
    execute("DELETE FROM agent_inflight WHERE asset_id = %s", (asset_id,))


def update_in_flight_stage(asset_id: int, stage: str):
    execute("""
        UPDATE agent_inflight SET stage = %s WHERE asset_id = %s
    """, (stage, asset_id))


# ─── Lakebase Branching (Guardian Pattern) ───────────────────────────────────

class LakebaseBranch:
    """
    Context manager for Lakebase database branching.
    CREATE BRANCH → test operations → MERGE or DROP.

    Usage:
        with LakebaseBranch("guardian_test_15_1234") as branch:
            branch.execute("INSERT INTO maintenance_orders ...")
            result = branch.query("SELECT * FROM maintenance_orders ...")
            # If no exception raised: branch is merged on __exit__
    """

    def __init__(self, branch_name: str):
        self.branch_name = branch_name
        self._conn       = None

    def __enter__(self):
        params = _get_conn_params()
        self._conn = psycopg2.connect(**params, connect_timeout=10)
        self._conn.autocommit = True

        # Create branch (copy-on-write, near-instantaneous in Lakebase)
        start = time.time()
        self._conn.cursor().execute(f"CREATE BRANCH {self.branch_name} FROM main")
        elapsed = (time.time() - start) * 1000
        logger.info(f"Branch created: {self.branch_name} in {elapsed:.0f}ms")

        # Switch connection to use the branch
        self._conn.cursor().execute(f"USE BRANCH {self.branch_name}")
        return self

    def execute(self, sql: str, params=None) -> int:
        """Execute DML on the branch."""
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur.rowcount

    def query(self, sql: str, params=None) -> list[dict]:
        """Query from the branch."""
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def query_one(self, sql: str, params=None) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def merge(self):
        """Merge branch changes into main production database."""
        self._conn.cursor().execute(f"USE BRANCH main")
        start = time.time()
        self._conn.cursor().execute(f"MERGE BRANCH {self.branch_name} INTO main")
        elapsed = (time.time() - start) * 1000
        logger.info(f"Branch merged: {self.branch_name} into main in {elapsed:.0f}ms")

    def drop(self):
        """Drop the branch (rollback / cleanup)."""
        try:
            self._conn.cursor().execute(f"USE BRANCH main")
            self._conn.cursor().execute(f"DROP BRANCH {self.branch_name}")
            logger.info(f"Branch dropped: {self.branch_name}")
        except Exception as e:
            logger.warning(f"Failed to drop branch {self.branch_name}: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                # No exception — merge to production
                self.merge()
            else:
                # Exception — drop branch (do not touch production)
                logger.warning(f"Exception in branch {self.branch_name}: {exc_val} — dropping branch")
                self.drop()
        finally:
            if self._conn:
                self._conn.close()
        return False  # do not suppress exceptions
