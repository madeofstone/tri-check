"""
Bundle the Databricks logs for one AAC job run into a zip.

AAC configures its job clusters with cluster_log_conf = dbfs:/trifacta/logs, so
everything for a run lands under /trifacta/logs/<cluster_id>/:

    driver/    stdout, stderr, log4j-active.log, stacktrace.log
    eventlog/  <cluster>_<uuid>/<app>/eventlog (+ rotated .gz for longer runs)
    executor/  <app>/<n>/stdout, stderr   (one directory per executor)

Roughly 3 MB for a short run. The bundle also carries the run record and the
cluster event timeline, which are API-only and not on DBFS.

Access caveat: a PAT can only read runs whose ephemeral job it has rights to,
which on this tenant is a subset tied to the AAC user that triggered the run.
When the run is not readable this raises DbxLogError with that explanation
rather than producing an empty zip.
"""

from __future__ import annotations

import json
import logging
import zipfile
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)

LOG_ROOT = "/trifacta/logs"
MAX_FILE_BYTES = 200 * 1024 * 1024      # skip anything pathological
MAX_TOTAL_BYTES = 500 * 1024 * 1024
SUBDIRS = ("driver", "eventlog", "executor")


class DbxLogError(Exception):
    """Raised when the logs cannot be collected."""


def _serialize(obj):
    """Best-effort JSON for SDK dataclasses."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return {k: _serialize(v) for k, v in asdict(obj).items() if v is not None}
        except Exception:
            pass
    if hasattr(obj, "value"):          # enums
        return obj.value
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(i) for i in obj]
    if hasattr(obj, "__dict__"):
        return {k: _serialize(v) for k, v in vars(obj).items() if v is not None}
    return str(obj)


def _walk(client: WorkspaceClient, path: str, depth: int = 0, max_depth: int = 6) -> list[dict]:
    """Flat list of {path, size} for every file beneath a DBFS directory."""
    if depth > max_depth:
        return []
    try:
        entries = list(client.dbfs.list(path))
    except Exception as e:
        log.info(f"dbfs.list({path}) failed: {e}")
        return []
    out: list[dict] = []
    for entry in entries:
        if entry.is_dir:
            out.extend(_walk(client, entry.path, depth + 1, max_depth))
        else:
            out.append({"path": entry.path, "size": entry.file_size or 0})
    return out


def resolve_run_context(client: WorkspaceClient, run_id: int) -> dict:
    """Fetch the run and pull out the cluster that executed it."""
    try:
        run = client.jobs.get_run(run_id=int(run_id))
    except Exception as e:
        msg = str(e)
        if "does not have" in msg or "permission" in msg.lower():
            raise DbxLogError(
                f"Databricks will not show run {run_id} to this token. On this tenant a PAT "
                f"can only read runs belonging to certain AAC users, so logs are unavailable "
                f"for this job unless the PAT is granted access to its job."
            ) from e
        raise DbxLogError(f"Could not read Databricks run {run_id}: {e}") from e

    tasks = getattr(run, "tasks", None) or []
    cluster_id = None
    if tasks:
        ci = getattr(tasks[0], "cluster_instance", None)
        cluster_id = getattr(ci, "cluster_id", None)
    if not cluster_id:
        ci = getattr(run, "cluster_instance", None)
        cluster_id = getattr(ci, "cluster_id", None)

    state = getattr(run, "state", None)
    return {
        "runId": int(run_id),
        "runName": getattr(run, "run_name", None),
        "jobId": getattr(run, "job_id", None),
        "clusterId": cluster_id,
        "resultState": _serialize(getattr(state, "result_state", None)) if state else None,
        "lifeCycleState": _serialize(getattr(state, "life_cycle_state", None)) if state else None,
        "stateMessage": getattr(state, "state_message", None) if state else None,
        "run": _serialize(run),
    }


def list_available_logs(client: WorkspaceClient, cluster_id: str) -> dict:
    """What exists on DBFS for a cluster, grouped by kind, without downloading."""
    if not cluster_id:
        return {"files": [], "byKind": {}, "totalBytes": 0}
    files = []
    for sub in SUBDIRS:
        for f in _walk(client, f"{LOG_ROOT}/{cluster_id}/{sub}"):
            f["kind"] = sub
            files.append(f)
    by_kind: dict[str, dict] = {}
    for f in files:
        k = by_kind.setdefault(f["kind"], {"files": 0, "bytes": 0})
        k["files"] += 1
        k["bytes"] += f["size"]
    return {"files": files, "byKind": by_kind, "totalBytes": sum(f["size"] for f in files)}


def build_log_bundle(host: str, token: str, run_id: int, out_path: Path,
                     job_meta: Optional[dict] = None) -> dict:
    """
    Collect everything available for a run into a zip at out_path.

    Returns a summary describing what was included and what was skipped, so the
    caller can tell the user when a bundle is partial rather than failing
    silently.
    """
    if not token:
        raise DbxLogError("DATABRICKS_TOKEN is not configured")
    if not host:
        raise DbxLogError("No Databricks workspace URL for this job")

    client = WorkspaceClient(host=str(host).rstrip("/"), token=token)
    ctx = resolve_run_context(client, run_id)
    cluster_id = ctx.get("clusterId")

    inventory = list_available_logs(client, cluster_id) if cluster_id else {"files": []}
    included, skipped, total = [], [], 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for f in inventory["files"]:
            size = f["size"]
            if size > MAX_FILE_BYTES:
                skipped.append({**f, "reason": "file too large"})
                continue
            if total + size > MAX_TOTAL_BYTES:
                skipped.append({**f, "reason": "bundle size limit reached"})
                continue
            # Preserve the driver/eventlog/executor layout inside the zip.
            arcname = f["path"].split(f"{cluster_id}/", 1)[-1] if cluster_id else f["path"].lstrip("/")
            try:
                with client.dbfs.download(f["path"]) as remote:
                    with z.open(arcname, "w") as dest:
                        while True:
                            chunk = remote.read(1024 * 1024)
                            if not chunk:
                                break
                            dest.write(chunk)
                included.append({**f, "arcname": arcname})
                total += size
            except Exception as e:
                skipped.append({**f, "reason": str(e)[:200]})

        # API-only extras that are not on DBFS.
        try:
            events = [_serialize(e) for e in client.clusters.events(cluster_id=cluster_id)] if cluster_id else []
            z.writestr("cluster_events.json", json.dumps(events, indent=2))
        except Exception as e:
            skipped.append({"path": "cluster_events.json", "reason": str(e)[:200]})

        try:
            if cluster_id:
                z.writestr("cluster.json", json.dumps(_serialize(client.clusters.get(cluster_id=cluster_id)), indent=2))
        except Exception as e:
            skipped.append({"path": "cluster.json", "reason": str(e)[:200]})

        z.writestr("run.json", json.dumps(ctx.get("run"), indent=2))

        manifest = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "workspace": str(host).rstrip("/"),
            "databricksRunId": ctx.get("runId"),
            "databricksRunName": ctx.get("runName"),
            "databricksJobId": ctx.get("jobId"),
            "clusterId": cluster_id,
            "resultState": ctx.get("resultState"),
            "lifeCycleState": ctx.get("lifeCycleState"),
            "stateMessage": ctx.get("stateMessage"),
            "aacJob": job_meta or {},
            "included": included,
            "skipped": skipped,
            "totalBytes": total,
        }
        z.writestr("manifest.json", json.dumps(manifest, indent=2))

    return {
        "path": str(out_path),
        "clusterId": cluster_id,
        "runName": ctx.get("runName"),
        "fileCount": len(included),
        "bytes": total,
        "zipBytes": out_path.stat().st_size,
        "included": included,
        "skipped": skipped,
    }


BUNDLE_TTL_DAYS = 7


def purge_old_bundles(bundle_dir: Path, ttl_days: int = BUNDLE_TTL_DAYS) -> int:
    """
    Delete cached log zips older than the TTL.

    Bundles are a few hundred KB each and are cached indefinitely once built,
    so without this they accumulate for as long as the server runs.
    """
    if not bundle_dir.exists():
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - ttl_days * 86400
    removed = 0
    for f in bundle_dir.glob("dbx-logs-*.zip"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        except OSError:
            continue
    if removed:
        log.info(f"Removed {removed} cached log bundles older than {ttl_days} days")
    return removed
