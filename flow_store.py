"""
Flow Store — File-based persistence for Tri-Tracker.

Directory layout:
    flows/<flowName>/
        flow_data.json          # Pairs, metadata, base URLs
        dbx/<jobRunId>.json     # Cached Databricks details
        eventlogs/<jobRunId>/
            eventlog            # Raw downloaded event log
            analysis.json       # Compressed analysis output
"""

import json
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

FLOWS_DIR = Path(__file__).resolve().parent / "flows"


def _sanitize_name(name: str) -> str:
    """Sanitize flow name for safe use as a directory name."""
    # Replace problematic chars with underscores, collapse multiples
    safe = re.sub(r'[^\w\s\-.]', '_', name)
    safe = re.sub(r'[\s]+', ' ', safe).strip()
    return safe if safe else "unnamed"


def _flow_dir(name: str) -> Path:
    return FLOWS_DIR / _sanitize_name(name)


def _ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Optional[dict]:
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return None


def _write_json(path: Path, data: Any):
    _ensure_dir(path.parent)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Flow CRUD
# ---------------------------------------------------------------------------

def list_flows() -> list[dict]:
    """Return metadata for all saved flows."""
    if not FLOWS_DIR.exists():
        return []

    result = []
    for entry in sorted(FLOWS_DIR.iterdir()):
        if entry.is_dir():
            data = _read_json(entry / "flow_data.json")
            if data:
                result.append({
                    "name": data.get("name", entry.name),
                    "lastFetched": data.get("lastFetched"),
                    "jobCount": len(data.get("pairs", [])),
                })
    return result


def load_flow(name: str) -> Optional[dict]:
    """
    Load full flow data including which jobs have cached DBX and analysis.

    Returns dict with keys:
        name, pairs, aacBaseUrl, onpremBaseUrl, onpremEnabled,
        matchWindowMinutes, lastFetched, errors,
        analyzedJobs: [jobRunId, ...],
        dbxCachedJobs: [jobRunId, ...]
    """
    data = _read_json(_flow_dir(name) / "flow_data.json")
    if not data:
        return None

    data["analyzedJobs"] = list_analyzed_jobs(name)
    data["dbxCachedJobs"] = list_dbx_cached_jobs(name)
    return data


def save_flow(name: str, pairs: list, metadata: dict):
    """Persist flow data to disk."""
    fdir = _flow_dir(name)
    payload = {
        "name": name,
        "pairs": pairs,
        "lastFetched": datetime.now(timezone.utc).isoformat(),
        **metadata,
    }
    _write_json(fdir / "flow_data.json", payload)
    log.info(f"Saved flow '{name}' with {len(pairs)} pairs")


def merge_jobs(name: str, new_pairs: list, metadata: dict) -> list:
    """
    Merge new job pairs with existing saved data.

    Keyed by AAC jobRunId (or onprem jobRunId for unmatched on-prem rows).
    New runs are added; existing runs update status/times if changed.

    Returns the merged pairs list.
    """
    existing = _read_json(_flow_dir(name) / "flow_data.json")
    if not existing or not existing.get("pairs"):
        # No existing data — just save and return
        save_flow(name, new_pairs, metadata)
        return new_pairs

    # Build index of existing pairs by jobRunId
    existing_pairs = existing["pairs"]
    index = {}
    for pair in existing_pairs:
        key = _pair_key(pair)
        if key:
            index[key] = pair

    # Merge new pairs in
    for pair in new_pairs:
        key = _pair_key(pair)
        if key and key in index:
            # Update status and timing fields, keep rest
            old = index[key]
            if pair.get("aac"):
                old["aac"] = pair["aac"]
            if pair.get("onprem"):
                old["onprem"] = pair["onprem"]
            old["matched"] = pair.get("matched", old.get("matched", False))
        elif key:
            # New job run — add it
            existing_pairs.append(pair)
            index[key] = pair

    save_flow(name, existing_pairs, metadata)
    return existing_pairs


def _pair_key(pair: dict) -> Optional[str]:
    """Extract a unique key from a pair for merge dedup."""
    aac = pair.get("aac")
    if aac and aac.get("jobRunId"):
        return f"aac_{aac['jobRunId']}"
    onprem = pair.get("onprem")
    if onprem and onprem.get("jobRunId"):
        return f"op_{onprem['jobRunId']}"
    return None


def delete_flow(name: str) -> bool:
    """Remove a flow and all its data."""
    fdir = _flow_dir(name)
    if fdir.exists():
        shutil.rmtree(fdir)
        log.info(f"Deleted flow '{name}'")
        return True
    return False


# ---------------------------------------------------------------------------
# DBX Cache
# ---------------------------------------------------------------------------

def load_dbx(name: str, job_run_id: str) -> Optional[dict]:
    """Return cached DBX details or None."""
    return _read_json(_flow_dir(name) / "dbx" / f"{job_run_id}.json")


def save_dbx(name: str, job_run_id: str, data: dict):
    """Persist DBX details to disk."""
    _write_json(_flow_dir(name) / "dbx" / f"{job_run_id}.json", data)
    log.info(f"Cached DBX for flow='{name}' job={job_run_id}")


def list_dbx_cached_jobs(name: str) -> list[str]:
    """Return list of jobRunIds that have cached DBX data."""
    dbx_dir = _flow_dir(name) / "dbx"
    if not dbx_dir.exists():
        return []
    return [p.stem for p in dbx_dir.glob("*.json")]


# ---------------------------------------------------------------------------
# Event Log Paths
# ---------------------------------------------------------------------------

def eventlog_dir(name: str, job_run_id: str) -> str:
    """Return the path for storing event logs for a given job run."""
    d = _flow_dir(name) / "eventlogs" / str(job_run_id)
    _ensure_dir(d)
    return str(d)


def list_analyzed_jobs(name: str) -> list[str]:
    """Return list of jobRunIds that have analysis.json."""
    el_dir = _flow_dir(name) / "eventlogs"
    if not el_dir.exists():
        return []
    return [
        d.name for d in el_dir.iterdir()
        if d.is_dir() and (d / "analysis.json").exists()
    ]


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def migrate_legacy_jobs(legacy_jobs_dir: Path, flow_name: str):
    """
    Migrate data from the old tri-track/jobs/<jobRunId>/ structure
    into the new flow-based layout.
    """
    if not legacy_jobs_dir.exists():
        return

    target_el_dir = _flow_dir(flow_name) / "eventlogs"

    for job_dir in legacy_jobs_dir.iterdir():
        if job_dir.is_dir():
            job_run_id = job_dir.name
            dest = target_el_dir / job_run_id
            if not dest.exists():
                _ensure_dir(dest.parent)
                shutil.copytree(job_dir, dest)
                log.info(f"Migrated legacy job {job_run_id} → {dest}")

    log.info(f"Migration complete for flow '{flow_name}'")
