"""
LiveTwin — MLflow Serving Client Utility
Calls all three model endpoints + provides MLflow Tracing helpers.
"""

import os
import json
import time
import logging
from typing import Optional
import requests
import mlflow
import mlflow.tracking

logger = logging.getLogger(__name__)


def _get_headers():
    """Get Databricks auth headers."""
    try:
        from databricks.sdk.runtime import dbutils
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    except Exception:
        token = os.environ.get("DATABRICKS_TOKEN", "")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_workspace_url():
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        return os.environ.get("DATABRICKS_HOST", "")


# ─── Endpoint Callers ────────────────────────────────────────────────────────

def call_endpoint(endpoint_name: str, inputs: list, timeout: int = 15) -> dict:
    """
    Call a Databricks serving endpoint.
    Returns the raw predictions dict.
    """
    host  = _get_workspace_url()
    url   = f"https://{host}/serving-endpoints/{endpoint_name}/invocations"
    start = time.time()

    resp = requests.post(
        url,
        headers=_get_headers(),
        json={"inputs": inputs},
        timeout=timeout,
    )

    elapsed_ms = int((time.time() - start) * 1000)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Endpoint {endpoint_name} returned {resp.status_code}: {resp.text[:200]}"
        )

    result = resp.json()
    result["_latency_ms"] = elapsed_ms
    return result


def predict_anomaly(sensor_features: list[float]) -> dict:
    """
    Call livetwin-anomaly-detector endpoint.
    Returns: {anomaly_score: float, latency_ms: int}
    """
    result = call_endpoint(
        "livetwin-anomaly-detector",
        inputs=[sensor_features],
    )
    score = result.get("predictions", [0.0])[0]
    return {
        "anomaly_score": float(score),
        "latency_ms":    result["_latency_ms"],
    }


def predict_rul(sensor_features: list[float]) -> dict:
    """
    Call livetwin-rul-predictor endpoint.
    Returns: {rul_hours: float, lower_bound: float, upper_bound: float, latency_ms: int}
    """
    result = call_endpoint(
        "livetwin-rul-predictor",
        inputs=[sensor_features],
    )
    preds = result.get("predictions", [{}])
    pred  = preds[0] if preds else {}

    return {
        "rul_hours":    float(pred.get("rul_hours",    24.0)),
        "lower_bound":  float(pred.get("lower_bound",  18.0)),
        "upper_bound":  float(pred.get("upper_bound",  30.0)),
        "latency_ms":   result["_latency_ms"],
    }


def predict_failure_mode(sensor_features: list[float]) -> dict:
    """
    Call livetwin-failure-classifier endpoint.
    Returns: {failure_mode: str, confidence: float, probabilities: dict, latency_ms: int}
    """
    result = call_endpoint(
        "livetwin-failure-classifier",
        inputs=[sensor_features],
    )
    preds = result.get("predictions", [{}])
    pred  = preds[0] if preds else {}

    probs_raw = pred.get("probabilities", "{}")
    if isinstance(probs_raw, str):
        try:
            probs = json.loads(probs_raw.replace("'", '"'))
        except Exception:
            probs = {}
    else:
        probs = probs_raw

    return {
        "failure_mode":   str(pred.get("failure_mode", "unknown")),
        "confidence":     float(pred.get("confidence", 0.5)),
        "probabilities":  probs,
        "latency_ms":     result["_latency_ms"],
    }


# ─── Fallback Heuristics (when endpoint unavailable) ─────────────────────────

def predict_rul_heuristic(sensors: dict, cycle: int) -> dict:
    """
    Threshold-based RUL fallback if MLflow endpoint is unavailable.
    Based on CMAPSS domain knowledge: s2 + s4 elevation correlates with degradation.
    """
    s2  = sensors.get("s2", 650.0)
    s4  = sensors.get("s4", 1400.0)
    s11 = sensors.get("s11", 47.0)

    # Simple linear heuristic (not accurate, but keeps system running)
    degradation = max(0, (s2 - 640) / 20 + (s4 - 1380) / 50 + max(0, (47 - s11) / 5))
    rul_estimate = max(1, 100 - degradation * 20 - cycle * 0.1)

    return {
        "rul_hours":   rul_estimate,
        "lower_bound": rul_estimate * 0.7,
        "upper_bound": rul_estimate * 1.3,
        "latency_ms":  0,
        "source":      "heuristic_fallback",
    }


# ─── MLflow Tracing ──────────────────────────────────────────────────────────

class AgentTrace:
    """
    MLflow 3.0 tracing context manager.
    Records every agent step with inputs, outputs, latency, and model versions.

    Usage:
        with AgentTrace("monitor_agent", asset_id=15) as trace:
            trace.log("fleet_scan", inputs={"engines": 20}, outputs={"anomalies": 1})
            trace.set_model_version("anomaly_detector", "v3")
    """

    def __init__(self, agent_name: str, asset_id: Optional[int] = None,
                 experiment_path: str = "/Shared/livetwin/agent_traces"):
        self.agent_name      = agent_name
        self.asset_id        = asset_id
        self.experiment_path = experiment_path
        self._run            = None
        self._trace_id       = None
        self._spans: list    = []

    def __enter__(self):
        mlflow.set_experiment(self.experiment_path)
        self._run_ctx = mlflow.start_run(run_name=f"{self.agent_name}_asset{self.asset_id}")
        self._run = self._run_ctx.__enter__()
        mlflow.set_tag("agent_name", self.agent_name)
        if self.asset_id:
            mlflow.set_tag("asset_id", str(self.asset_id))
        self._start_time = time.time()
        return self

    def log(self, step_name: str, inputs: dict = None, outputs: dict = None,
            latency_ms: int = 0, model_version: str = None):
        """Log a single agent step as a span."""
        span = {
            "step":         step_name,
            "inputs":       inputs or {},
            "outputs":      outputs or {},
            "latency_ms":   latency_ms,
        }
        if model_version:
            span["model_version"] = model_version

        self._spans.append(span)
        mlflow.log_metric(f"{step_name}_latency_ms", latency_ms)
        if outputs:
            for k, v in outputs.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(f"{step_name}_{k}", v)

    def set_verdict(self, verdict: str, reason: str = ""):
        mlflow.set_tag("guardian_verdict", verdict)
        mlflow.set_tag("guardian_reason", reason[:250] if reason else "")

    def get_trace_id(self) -> str:
        return self._run.info.run_id if self._run else ""

    def __exit__(self, exc_type, exc_val, exc_tb):
        total_ms = int((time.time() - self._start_time) * 1000)
        mlflow.log_metric("total_latency_ms", total_ms)
        mlflow.log_dict({"spans": self._spans}, "agent_trace.json")
        if exc_type:
            mlflow.set_tag("status", "error")
            mlflow.set_tag("error_message", str(exc_val)[:250])
        else:
            mlflow.set_tag("status", "success")
        self._run_ctx.__exit__(exc_type, exc_val, exc_tb)
        return False
