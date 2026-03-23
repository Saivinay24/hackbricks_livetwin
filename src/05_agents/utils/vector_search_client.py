"""
LiveTwin — Vector Search Client Utility
Semantic similarity search for Diagnosis and Guardian agents.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

VS_ENDPOINT      = "livetwin-vs-endpoint"
INCIDENTS_INDEX  = "livetwin.incidents.maintenance_reports_index"
SAFETY_INDEX     = "livetwin.incidents.safety_protocols_index"


def _get_vs_client():
    from databricks.vector_search.client import VectorSearchClient
    return VectorSearchClient()


def search_similar_incidents(
    query_text: str,
    num_results: int = 3,
    failure_mode_filter: Optional[str] = None,
) -> list[dict]:
    """
    Search maintenance incident reports for semantic similarity.
    Used by Diagnosis Agent.

    Returns list of dicts with incident details + similarity_score.
    """
    vsc   = _get_vs_client()
    index = vsc.get_index(VS_ENDPOINT, INCIDENTS_INDEX)

    filters = {"asset_type": "turbofan"}
    if failure_mode_filter:
        filters["failure_mode"] = failure_mode_filter

    try:
        results = index.similarity_search(
            query_text=query_text,
            num_results=num_results,
            filters=filters,
            columns=[
                "incident_id", "failure_mode", "root_cause",
                "corrective_action", "outcome", "cost",
                "downtime_hours", "cycles_before_failure",
                "sensor_pattern", "severity",
            ],
        )

        rows = results.get("result", {}).get("data_array", [])
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c["name"] for c in cols] if cols else []

        incidents = []
        for row in rows:
            if col_names and len(row) >= len(col_names):
                d = dict(zip(col_names, row))
            else:
                # Fallback: treat as positional
                d = {
                    "incident_id":       row[0] if len(row) > 0 else "",
                    "failure_mode":      row[1] if len(row) > 1 else "",
                    "root_cause":        row[2] if len(row) > 2 else "",
                    "corrective_action": row[3] if len(row) > 3 else "",
                    "outcome":           row[4] if len(row) > 4 else "",
                    "cost":              row[5] if len(row) > 5 else 0,
                    "similarity_score":  row[-1] if row else 0.0,
                }

            # Extract similarity score (last field in most VS responses)
            if "score" in d:
                d["similarity_score"] = float(d.pop("score"))
            elif row and isinstance(row[-1], float):
                d["similarity_score"] = float(row[-1])

            incidents.append(d)

        return incidents

    except Exception as e:
        logger.warning(f"Vector search failed (incidents): {e}")
        return []


def search_safety_protocols(
    query_text: str,
    num_results: int = 2,
    failure_mode: Optional[str] = None,
) -> list[dict]:
    """
    Search safety protocols for Guardian compliance check.
    Used by Guardian Agent.

    Returns list of protocol dicts with similarity_score.
    """
    vsc   = _get_vs_client()
    index = vsc.get_index(VS_ENDPOINT, SAFETY_INDEX)

    filters = {"doc_type": "safety_protocol"}
    if failure_mode:
        filters["failure_mode"] = failure_mode

    try:
        results = index.similarity_search(
            query_text=query_text,
            num_results=num_results,
            filters=filters,
            columns=["protocol_id", "title", "content", "failure_mode"],
        )

        rows = results.get("result", {}).get("data_array", [])
        protocols = []
        for row in rows:
            d = {
                "protocol_id":      row[0] if len(row) > 0 else "",
                "title":            row[1] if len(row) > 1 else "",
                "content":          row[2] if len(row) > 2 else "",
                "failure_mode":     row[3] if len(row) > 3 else "",
                "similarity_score": float(row[-1]) if row else 0.0,
            }
            protocols.append(d)

        return protocols

    except Exception as e:
        logger.warning(f"Vector search failed (safety protocols): {e}")
        # Return default safe protocol on failure
        return [{
            "protocol_id":      "SAF-DEFAULT",
            "title":            "Default Safety Protocol",
            "content":          "Standard maintenance safety checks apply. Verify LOTO before any work.",
            "failure_mode":     "all",
            "similarity_score": 0.5,
        }]
