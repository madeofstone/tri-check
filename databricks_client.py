"""
Databricks SDK client for Job Run Tracker.

Condensed wrapper around the Databricks Python SDK to fetch:
  - Run details (job name, spark conf, cluster ID)
  - Cluster events (chronological timeline)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from dataclasses import is_dataclass
from typing import Any, Optional

from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)


class DatabricksClientError(Exception):
    """Raised when a Databricks API call fails."""
    pass


def _serialize(obj: Any, depth: int = 0, max_depth: int = 6) -> Any:
    """Recursively convert SDK dataclass objects into plain dicts."""
    if depth > max_depth or obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    # Enums
    if hasattr(obj, "value") and hasattr(obj, "name") and hasattr(type(obj), "__members__"):
        return obj.value
    # Dataclasses / objects with __dict__
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize(v, depth + 1, max_depth) for k, v in obj.__dict__.items() if v is not None}
    if isinstance(obj, dict):
        return {k: _serialize(v, depth + 1, max_depth) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(i, depth + 1, max_depth) for i in obj]
    if hasattr(obj, "__dict__"):
        return {k: _serialize(v, depth + 1, max_depth) for k, v in obj.__dict__.items() if v is not None}
    return str(obj)


def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    """Convert millisecond epoch to ISO-8601 string."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class DatabricksClient:
    """Lightweight wrapper for the Databricks SDK calls we need."""

    def __init__(self, host: str, token: str):
        if not host or not token:
            raise DatabricksClientError("DATABRICKS_HOST and DATABRICKS_TOKEN must be configured")
        self.client = WorkspaceClient(host=host, token=token)

    # ------------------------------------------------------------------ #
    #  Find latest run for a job ID
    # ------------------------------------------------------------------ #
    def get_job_latest_run(self, job_id: int) -> dict:
        """
        Look up the most recent run for a Databricks job ID,
        then return get_run_details() for that run.
        """
        try:
            runs_iter = self.client.jobs.list_runs(job_id=int(job_id), limit=1)
            latest_run = None
            for run in runs_iter:
                latest_run = run
                break
            if not latest_run:
                raise DatabricksClientError(f"No runs found for job_id={job_id}")
            run_id = getattr(latest_run, "run_id", None)
            if not run_id:
                raise DatabricksClientError(f"Run found but no run_id for job_id={job_id}")
            return self.get_run_details(run_id)
        except DatabricksClientError:
            raise
        except Exception as e:
            raise DatabricksClientError(f"jobs.list_runs({job_id}) failed: {e}") from e

    # ------------------------------------------------------------------ #
    #  Run details
    # ------------------------------------------------------------------ #
    def get_run_details(self, run_id: int) -> dict:
        """
        Fetch a Databricks job run and extract the fields we care about.

        Returns dict with keys:
            jobName, sparkConf, clusterId, nodeTypeId,
            autoscale {minWorkers, maxWorkers},
            timing {startTime, endTime, setupDuration, executionDuration, cleanupDuration}
        """
        try:
            run = self.client.jobs.get_run(run_id=int(run_id))
        except Exception as e:
            raise DatabricksClientError(f"jobs.get_run({run_id}) failed: {e}") from e

        job_name = getattr(run, "run_name", None)

        # Extract spark_conf and cluster info from first task
        spark_conf: dict = {}
        cluster_id: Optional[str] = None
        node_type_id: Optional[str] = None
        autoscale: Optional[dict] = None

        tasks = getattr(run, "tasks", None) or []
        if tasks:
            task = tasks[0]
            # cluster_id from cluster_instance
            ci = getattr(task, "cluster_instance", None)
            if ci:
                cluster_id = getattr(ci, "cluster_id", None)
            # spark_conf from new_cluster
            nc = getattr(task, "new_cluster", None)
            if nc:
                raw_conf = getattr(nc, "spark_conf", None)
                if raw_conf and isinstance(raw_conf, dict):
                    spark_conf = dict(raw_conf)
                node_type_id = getattr(nc, "node_type_id", None)
                asc = getattr(nc, "autoscale", None)
                if asc:
                    autoscale = {
                        "minWorkers": getattr(asc, "min_workers", None),
                        "maxWorkers": getattr(asc, "max_workers", None),
                    }

        # Also check cluster_spec (single-task jobs)
        if not spark_conf:
            cs = getattr(run, "cluster_spec", None)
            if cs:
                nc = getattr(cs, "new_cluster", None)
                if nc:
                    raw_conf = getattr(nc, "spark_conf", None)
                    if raw_conf and isinstance(raw_conf, dict):
                        spark_conf = dict(raw_conf)
                    if not node_type_id:
                        node_type_id = getattr(nc, "node_type_id", None)
                    if not autoscale:
                        asc = getattr(nc, "autoscale", None)
                        if asc:
                            autoscale = {
                                "minWorkers": getattr(asc, "min_workers", None),
                                "maxWorkers": getattr(asc, "max_workers", None),
                            }

        # Timing
        timing = {
            "startTime": _ms_to_iso(getattr(run, "start_time", None)),
            "endTime": _ms_to_iso(getattr(run, "end_time", None)),
            "setupDurationMs": getattr(run, "setup_duration", None),
            "executionDurationMs": getattr(run, "execution_duration", None),
            "cleanupDurationMs": getattr(run, "cleanup_duration", None),
        }

        return {
            "jobName": job_name,
            "sparkConf": spark_conf,
            "clusterId": cluster_id,
            "nodeTypeId": node_type_id,
            "autoscale": autoscale,
            "timing": timing,
        }

    # ------------------------------------------------------------------ #
    #  Cluster events
    # ------------------------------------------------------------------ #
    def get_cluster_events(self, cluster_id: str, limit: int = 50) -> list[dict]:
        """
        Fetch cluster events and return them in chronological order.

        Each event: {timestamp, isoTime, eventType, details}
        """
        if not cluster_id:
            return []

        try:
            raw_events = []
            for i, event in enumerate(self.client.clusters.events(cluster_id=cluster_id)):
                if i >= limit:
                    break
                raw_events.append(event)
        except Exception as e:
            raise DatabricksClientError(f"clusters.events({cluster_id}) failed: {e}") from e

        events = []
        for ev in raw_events:
            ts = getattr(ev, "timestamp", None)
            event_type = getattr(ev, "type", None)
            if event_type:
                event_type = _serialize(event_type)

            details_obj = getattr(ev, "details", None)
            details: dict = {}
            if details_obj:
                cause = getattr(details_obj, "cause", None)
                if cause:
                    details["cause"] = _serialize(cause)
                reason = getattr(details_obj, "reason", None)
                if reason:
                    details["reasonCode"] = _serialize(getattr(reason, "code", None))
                cur = getattr(details_obj, "current_num_workers", None)
                tgt = getattr(details_obj, "target_num_workers", None)
                if cur is not None:
                    details["currentWorkers"] = cur
                if tgt is not None:
                    details["targetWorkers"] = tgt

            events.append({
                "timestamp": ts,
                "isoTime": _ms_to_iso(ts),
                "eventType": event_type,
                "details": details,
            })

        # Return chronological (oldest first)
        events.sort(key=lambda e: e.get("timestamp") or 0)
        return events
