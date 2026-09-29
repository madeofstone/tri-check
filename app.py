#!/usr/bin/env python3
"""
Tri-Tracker – Flask API Server

Serves the web UI and provides endpoints to:
  - Fetch jobs from AAC (and optionally on-prem Trifacta) environments
  - Match jobs across environments by creation time (±10 min)
  - Read/update configuration (tokens masked on read)
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS

from config import Config
from platform_api import PlatformAPI, PlatformAPIError
from databricks_client import DatabricksClient, DatabricksClientError
from dbfs_eventlog import download_eventlog, EventLogError
import dbx_logs
import flow_store  # kept for eventlog_dir and list_analyzed_jobs (file-based)
import classifier
import db
import aac_sync

log = logging.getLogger(__name__)

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ENV_FILE = Path(__file__).resolve().parent / ".env"
# Log bundles live OUTSIDE the project tree on purpose: the dev server runs with
# the reloader on, and writing a zip inside the project restarts the server
# mid-request, truncating the download.
DBX_BUNDLE_DIR = Path(tempfile.gettempdir()) / "tri-tracker-dbx-logs"
LEGACY_JOBS_DIR = Path(__file__).resolve().parent / "jobs"
ANALYZER_SCRIPT = Path(__file__).resolve().parent / "eventlog-analyzer" / "analyze_eventlog.py"

# Keys we allow to be read/written through the config API
CONFIGURABLE_KEYS = [
    "PLATFORM_API_BASE_URL",
    "PLATFORM_API_TOKEN",
    "ONPREM_ENABLED",
    "ONPREM_API_BASE_URL",
    "ONPREM_API_TOKEN",
    "DATABRICKS_TOKEN",
    "DEFAULT_LIMIT",
    "RANFOR_FILTER",
    "MATCH_WINDOW_MINUTES",
    "MATCH_STRATEGY",
    "RETENTION_DAYS",
]

# Keys whose values should be masked when sent to the browser
SECRET_KEYS = {"PLATFORM_API_TOKEN", "ONPREM_API_TOKEN", "DATABRICKS_TOKEN"}


def normalize_host(url: str | None) -> str:
    """Normalize a Databricks workspace URL (AAC returns it with a trailing slash)."""
    if not url:
        return ""
    return str(url).strip().rstrip("/")


def resolve_dbx_host(body: dict, job_run_id=None) -> str:
    """
    Determine which Databricks workspace to call for a job.

    The workspace URL is never configured — it comes from the AAC job itself
    (cpJobId.databricksServiceUrl), so we always query the workspace that ran
    the job. Prefer the value the UI passed; fall back to the stored job row.
    """
    host = normalize_host(body.get("databricksServiceUrl"))
    if host:
        return host
    if job_run_id:
        stored = db.get_job_dbx_host(job_run_id)
        if stored:
            return normalize_host(stored)
    return ""


NO_HOST_ERROR = (
    "No Databricks workspace URL for this job. It comes from the AAC job's "
    "cpJobId.databricksServiceUrl — re-collect the job to pick it up."
)


def mask_value(value: str) -> str:
    """Mask a secret value, showing only the last 4 characters."""
    if not value or len(value) <= 4:
        return "••••"
    return "•" * (len(value) - 4) + value[-4:]


def read_env_file() -> dict:
    """Read .env file into a dict (key=value lines only)."""
    env = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, _, value = stripped.partition("=")
                env[key.strip()] = value.strip()
    return env


def write_env_file(updates: dict):
    """Update specific keys in the .env file, preserving comments & order."""
    lines = ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []
    updated_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _, _ = stripped.partition("=")
            key = key.strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}")
                updated_keys.add(key)
                continue
        new_lines.append(line)
    # Append any keys not already in the file
    for key, value in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}")
    ENV_FILE.write_text("\n".join(new_lines) + "\n")


def reload_config():
    """Reload Config class attributes from .env file."""
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_FILE, override=True)
    Config.PLATFORM_API_BASE_URL = os.getenv("PLATFORM_API_BASE_URL", "https://eu1.alteryxcloud.com/v4")
    Config.PLATFORM_API_TOKEN = os.getenv("PLATFORM_API_TOKEN", "")
    Config.ONPREM_API_BASE_URL = os.getenv("ONPREM_API_BASE_URL", "")
    Config.ONPREM_API_TOKEN = os.getenv("ONPREM_API_TOKEN", "")
    Config.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    Config.DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "25"))
    Config.ONPREM_ENABLED = os.getenv("ONPREM_ENABLED", "false").lower() in ("true", "1", "yes")
    Config.RANFOR_FILTER = os.getenv("RANFOR_FILTER", "recipe,plan")
    Config.MATCH_WINDOW_MINUTES = int(os.getenv("MATCH_WINDOW_MINUTES", "10"))
    Config.RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "14"))


def parse_iso(ts: str) -> datetime | None:
    """Parse an ISO timestamp string to datetime."""
    if not ts:
        return None
    try:
        # Handle '2026-01-29T19:32:42.000Z' format
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def compute_execution_minutes(created_at: str, updated_at: str) -> float | None:
    """Compute execution time in minutes between created and updated."""
    dt_created = parse_iso(created_at)
    dt_updated = parse_iso(updated_at)
    if dt_created and dt_updated:
        delta = (dt_updated - dt_created).total_seconds() / 60.0
        return round(delta, 1)
    return None


def extract_job_summary(job_data: dict) -> dict:
    """Extract the fields we care about from a jobLibrary API entry."""
    created_at = job_data.get("createdAt", "")
    updated_at = job_data.get("updatedAt", "")

    flow_id = None
    flow_name = None
    wd = job_data.get("wrangledDataset")
    if wd:
        flow = wd.get("flow")
        if flow:
            flow_id = flow.get("id")
            flow_name = flow.get("name")

    # jobs.data holds EVERY job in the group (ingest, schemadrift, wrangle,
    # filewriter, publish). Only the "wrangle" job runs on Spark and carries the
    # cpJobId that links back to Databricks; its position in the array varies with
    # how many ingest/schemadrift jobs precede it, so select it by type.
    inner_job = None
    jobs_wrapper = job_data.get("jobs")
    if jobs_wrapper:
        jobs_list = jobs_wrapper.get("data", []) or []
        inner_job = next(
            (ij for ij in jobs_list if (ij or {}).get("jobType") == "wrangle"),
            None,
        )
        # Fall back to any job that carries a cpJobId, in case jobType is renamed.
        if inner_job is None:
            inner_job = next((ij for ij in jobs_list if (ij or {}).get("cpJobId")), None)

    execution_language = None
    databricks_run_id = None
    databricks_job_definition_id = None
    databricks_service_url = None
    if inner_job:
        execution_language = inner_job.get("executionLanguage")
        # cpJobId is a JSON *string* describing the Databricks execution.
        # Field naming is misleading: "databricksJobId" is the RUN id (what
        # jobs/runs/get takes), while "databricksJobDefinitionId" is the job id.
        cp_job_id_raw = inner_job.get("cpJobId")
        if cp_job_id_raw and isinstance(cp_job_id_raw, str):
            try:
                cp_job = json.loads(cp_job_id_raw)
                databricks_run_id = cp_job.get("databricksJobId")
                databricks_job_definition_id = cp_job.get("databricksJobDefinitionId")
                databricks_service_url = normalize_host(cp_job.get("databricksServiceUrl"))
            except (json.JSONDecodeError, TypeError):
                pass

    # Extract ranfor, ranfrom, and creator email (API uses lowercase keys)
    ran_for = job_data.get("ranfor", "")
    ran_from = job_data.get("ranfrom", "")
    creator_email = ""
    # creator email may be in updater or creator object
    updater = job_data.get("updater")
    if updater and isinstance(updater, dict):
        creator_email = updater.get("email", "")
    if not creator_email:
        creator = job_data.get("creator")
        if creator and isinstance(creator, dict):
            creator_email = creator.get("email", "")

    return {
        "jobRunId": job_data.get("id"),
        "jobGroupId": job_data.get("id"),
        "status": job_data.get("status"),
        "flowId": flow_id,
        "flowName": flow_name,
        "ranFor": ran_for,
        "ranFrom": ran_from,
        "creatorEmail": creator_email,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "executionTimeMinutes": compute_execution_minutes(created_at, updated_at),
        "executionLanguage": execution_language,
        # Kept as "databricksJobId" for UI/storage compatibility — it is a run id.
        "databricksJobId": databricks_run_id,
        "databricksJobDefinitionId": databricks_job_definition_id,
        "databricksServiceUrl": databricks_service_url,
    }


def match_jobs(aac_jobs: list[dict], onprem_jobs: list[dict], window_minutes: int = 10, strategy: str = "time_and_name") -> list[dict]:
    """
    Match AAC and on-prem jobs.
    If strategy == "name_only": Match sequentially based on flowName only, assuming arrays are sorted newest-first.
    Otherwise: Match by flowName and createdAt within ±window_minutes.
    Returns a list of paired rows.
    """
    used_onprem = set()
    pairs = []

    for aac in aac_jobs:
        aac_dt = parse_iso(aac.get("createdAt", ""))
        aac_flow = aac.get("flowName", "").strip()
        best_match = None
        best_delta = None

        if strategy == "name_only":
            for idx, op in enumerate(onprem_jobs):
                if idx in used_onprem:
                    continue
                op_flow = op.get("flowName", "").strip()
                if aac_flow and op_flow and aac_flow.lower() != op_flow.lower():
                    continue
                # Match found! (first one that hasn't been used yet is the most recent)
                best_match = idx
                break
        elif aac_dt:
            for idx, op in enumerate(onprem_jobs):
                if idx in used_onprem:
                    continue
                
                # Flow names must match
                op_flow = op.get("flowName", "").strip()
                if aac_flow and op_flow and aac_flow.lower() != op_flow.lower():
                    continue

                op_dt = parse_iso(op.get("createdAt", ""))
                if op_dt:
                    delta = abs((aac_dt - op_dt).total_seconds())
                    if delta <= window_minutes * 60:
                        if best_delta is None or delta < best_delta:
                            best_match = idx
                            best_delta = delta

        if best_match is not None:
            used_onprem.add(best_match)
            pairs.append({"aac": aac, "onprem": onprem_jobs[best_match], "matched": True})
        else:
            pairs.append({"aac": aac, "onprem": None, "matched": False})

    # Remaining unmatched on-prem jobs
    for idx, op in enumerate(onprem_jobs):
        if idx not in used_onprem:
            pairs.append({"aac": None, "onprem": op, "matched": False})

    return pairs


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def serve_index():
    return send_from_directory(".", "index.html")


@app.route("/api/jobs", methods=["POST"])
def fetch_jobs():
    """Fetch jobs from both environments and return matched pairs."""
    body = request.get_json(silent=True) or {}
    flow_name = body.get("flowName", "").strip()
    limit = body.get("limit", Config.DEFAULT_LIMIT)

    if not flow_name:
        return jsonify({"error": "flowName is required"}), 400

    results = {"flowName": flow_name, "aac": [], "onprem": [], "pairs": []}
    errors = []

    # Strip /v4 suffix to get the base host URL for job links
    def base_host(url: str) -> str:
        return url.rstrip("/").removesuffix("/v4").removesuffix("/v4/")

    # Fetch AAC jobs
    if Config.PLATFORM_API_TOKEN and Config.PLATFORM_API_BASE_URL:
        try:
            aac_api = PlatformAPI(
                token=Config.PLATFORM_API_TOKEN,
                base_url=Config.PLATFORM_API_BASE_URL,
            )
            aac_response = aac_api.get_jobs_for_flow(flow_name, limit=limit)
            aac_data = aac_response.get("data", [])
            results["aac"] = [extract_job_summary(j) for j in aac_data]
        except PlatformAPIError as e:
            errors.append(f"AAC: {e}")
    else:
        errors.append("AAC: Missing PLATFORM_API_BASE_URL or PLATFORM_API_TOKEN")

    # Fetch on-prem jobs (only if enabled)
    onprem_enabled = getattr(Config, 'ONPREM_ENABLED', False)
    if onprem_enabled and Config.ONPREM_API_TOKEN and Config.ONPREM_API_BASE_URL:
        try:
            onprem_api = PlatformAPI(
                token=Config.ONPREM_API_TOKEN,
                base_url=Config.ONPREM_API_BASE_URL,
                verify_ssl=False,
            )
            onprem_response = onprem_api.get_jobs_for_flow(flow_name, limit=limit)
            onprem_data = onprem_response.get("data", [])
            results["onprem"] = [extract_job_summary(j) for j in onprem_data]
        except PlatformAPIError as e:
            errors.append(f"On-Prem: {e}")
    elif onprem_enabled:
        errors.append("On-Prem: Missing ONPREM_API_BASE_URL or ONPREM_API_TOKEN")

    # Match jobs using configurable window
    window = Config.MATCH_WINDOW_MINUTES
    strategy = getattr(Config, "MATCH_STRATEGY", "time_and_name")
    new_pairs = match_jobs(results["aac"], results["onprem"], window_minutes=window, strategy=strategy)

    # Build metadata for storage
    aac_base = base_host(Config.PLATFORM_API_BASE_URL) if Config.PLATFORM_API_BASE_URL else ""
    onprem_base = base_host(Config.ONPREM_API_BASE_URL) if Config.ONPREM_API_BASE_URL else ""
    metadata = {
        "aacBaseUrl": aac_base,
        "onpremBaseUrl": onprem_base,
        "onpremEnabled": onprem_enabled,
        "matchWindowMinutes": window,
        "errors": errors,
    }

    # Merge with existing saved data and persist
    merged_pairs = db.merge_jobs_flow(flow_name, new_pairs, metadata)

    results["pairs"] = merged_pairs
    results["matchWindowMinutes"] = window
    results["errors"] = errors
    results["aacBaseUrl"] = aac_base
    results["onpremBaseUrl"] = onprem_base
    results["onpremEnabled"] = onprem_enabled

    # Include cache status for the frontend (eventlogs are still file-based)
    results["analyzedJobs"] = flow_store.list_analyzed_jobs(flow_name)
    results["dbxCachedJobs"] = db.list_dbx_cached_jobs(flow_name)

    return jsonify(results)


@app.route("/api/databricks", methods=["POST"])
def fetch_databricks_details():
    """Fetch Databricks run details and cluster events for a given run ID."""
    body = request.get_json(silent=True) or {}
    dbx_job_id = body.get("databricksJobId")
    flow_name = body.get("flowName", "").strip()
    job_run_id = body.get("jobRunId")
    if not dbx_job_id:
        return jsonify({"error": "databricksJobId is required"}), 400

    # Check cache first
    if flow_name and job_run_id:
        cached = db.load_dbx(flow_name, str(job_run_id))
        if cached:
            cached["cached"] = True
            return jsonify(cached)

    if not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_TOKEN must be configured"}), 400

    host = resolve_dbx_host(body, job_run_id)
    if not host:
        return jsonify({"error": NO_HOST_ERROR}), 400

    try:
        dbx = DatabricksClient(host=host, token=Config.DATABRICKS_TOKEN)
        run_details = dbx.get_run_details(dbx_job_id)

        # Fetch cluster events if we got a cluster_id
        cluster_events = []
        cluster_id = run_details.get("clusterId")
        if cluster_id:
            try:
                cluster_events = dbx.get_cluster_events(cluster_id)
            except DatabricksClientError as e:
                run_details["clusterEventsError"] = str(e)

        response_data = {
            "databricksJobId": dbx_job_id,
            "databricksHost": host,
            "runDetails": run_details,
            "clusterEvents": cluster_events,
            "cached": False,
        }

        # Save to cache
        if flow_name and job_run_id:
            db.save_dbx(flow_name, str(job_run_id), response_data)

        return jsonify(response_data)
    except DatabricksClientError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/databricks/refresh", methods=["POST"])
def refresh_databricks_details():
    """Clear cached DBX data and event log analysis, then re-fetch from Databricks."""
    body = request.get_json(silent=True) or {}
    dbx_job_id = body.get("databricksJobId")
    flow_name = body.get("flowName", "").strip()
    job_run_id = body.get("jobRunId")

    if not dbx_job_id:
        return jsonify({"error": "databricksJobId is required"}), 400

    # Clear cached data
    if flow_name and job_run_id:
        db.clear_dbx_job(flow_name, str(job_run_id))
        flow_store.clear_dbx_job(flow_name, str(job_run_id))  # also clear file-based eventlogs

    if not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_TOKEN must be configured"}), 400

    host = resolve_dbx_host(body, job_run_id)
    if not host:
        return jsonify({"error": NO_HOST_ERROR}), 400

    try:
        dbx = DatabricksClient(host=host, token=Config.DATABRICKS_TOKEN)
        run_details = dbx.get_run_details(dbx_job_id)

        cluster_events = []
        cluster_id = run_details.get("clusterId")
        if cluster_id:
            try:
                cluster_events = dbx.get_cluster_events(cluster_id)
            except DatabricksClientError as e:
                run_details["clusterEventsError"] = str(e)

        response_data = {
            "databricksJobId": dbx_job_id,
            "databricksHost": host,
            "runDetails": run_details,
            "clusterEvents": cluster_events,
            "cached": False,
        }

        # Save fresh data to cache
        if flow_name and job_run_id:
            db.save_dbx(flow_name, str(job_run_id), response_data)

        return jsonify(response_data)
    except DatabricksClientError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/eventlog", methods=["POST"])
def fetch_eventlog():
    """Download a Spark event log from DBFS and run the analyzer."""
    body = request.get_json(silent=True) or {}
    cluster_id = body.get("clusterId")
    job_run_id = body.get("jobRunId")
    flow_name = body.get("flowName", "").strip()

    if not cluster_id:
        return jsonify({"error": "clusterId is required"}), 400
    if not job_run_id:
        return jsonify({"error": "jobRunId is required"}), 400
    if not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_TOKEN must be configured"}), 400

    host = resolve_dbx_host(body, job_run_id)
    if not host:
        return jsonify({"error": NO_HOST_ERROR}), 400

    # Always store in jobs/<jobRunId>/ directory
    job_dir = LEGACY_JOBS_DIR / str(job_run_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    analysis_file = job_dir / "analysis.json"

    # Return cached analysis if it exists
    if analysis_file.exists():
        with open(analysis_file, "r") as f:
            return jsonify({"status": "complete", "cached": True, "analysis": json.load(f)})

    # Step 1: Download event log from DBFS
    try:
        eventlog_path = download_eventlog(
            host=host,
            token=Config.DATABRICKS_TOKEN,
            cluster_id=cluster_id,
            local_dir=str(job_dir),
        )
    except EventLogError as e:
        return jsonify({"error": f"Event log download failed: {e}"}), 500

    # Step 2: Run the analyzer script
    try:
        result = subprocess.run(
            [sys.executable, str(ANALYZER_SCRIPT), eventlog_path],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            return jsonify({
                "error": f"Analyzer failed: {result.stderr.strip()}",
                "stdout": result.stdout.strip(),
            }), 500
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Analyzer timed out (120s limit)"}), 500

    # Step 3: Read and return the analysis
    if not analysis_file.exists():
        return jsonify({"error": "Analyzer completed but analysis.json was not produced"}), 500

    with open(analysis_file, "r") as f:
        analysis = json.load(f)

    return jsonify({"status": "complete", "cached": False, "analysis": analysis})


@app.route("/api/eventlog/<job_run_id>", methods=["GET"])
def get_eventlog_analysis(job_run_id):
    """Return previously generated analysis.json for a job run."""
    # Primary: check jobs/<id>/
    analysis_file = LEGACY_JOBS_DIR / str(job_run_id) / "analysis.json"

    # Fallback: check flow-based storage for backwards compatibility
    if not analysis_file.exists():
        flow_name = request.args.get("flowName", "").strip()
        if flow_name:
            alt = Path(flow_store.eventlog_dir(flow_name, str(job_run_id))) / "analysis.json"
            if alt.exists():
                analysis_file = alt
        # Also scan all flow directories
        if not analysis_file.exists():
            flows_dir = Path(__file__).resolve().parent / "flows"
            if flows_dir.exists():
                for fd in flows_dir.iterdir():
                    candidate = fd / "eventlogs" / str(job_run_id) / "analysis.json"
                    if candidate.exists():
                        analysis_file = candidate
                        break

    if not analysis_file.exists():
        return jsonify({"error": "No analysis found for this job run", "exists": False}), 404

    with open(analysis_file, "r") as f:
        return jsonify({"status": "complete", "exists": True, "analysis": json.load(f)})

@app.route("/api/flows", methods=["GET"])
def get_flows():
    """Return list of saved flows with their cached data."""
    flows_list = db.list_flows()
    result = []
    for f in flows_list:
        flow_data = db.load_flow(f["name"])
        if flow_data:
            # Add analyzed jobs info (file-based)
            flow_data["analyzedJobs"] = flow_store.list_analyzed_jobs(f["name"])
            flow_data["dbxCachedJobs"] = db.list_dbx_cached_jobs(f["name"])
            result.append(flow_data)
    return jsonify({"flows": result})


@app.route("/api/flows/<path:flow_name>", methods=["DELETE"])
def remove_flow(flow_name):
    """Remove a flow and all its data."""
    db.delete_flow(flow_name)
    deleted = flow_store.delete_flow(flow_name)  # clean up file-based data too
    return jsonify({"success": True})


@app.route("/api/config", methods=["GET"])
def get_config():
    """Return current config with masked secrets."""
    env_vals = read_env_file()
    cfg = {}
    for key in CONFIGURABLE_KEYS:
        raw = env_vals.get(key, getattr(Config, key, ""))
        if isinstance(raw, int):
            raw = str(raw)
        if key in SECRET_KEYS:
            cfg[key] = {"value": mask_value(str(raw)), "masked": True}
        else:
            cfg[key] = {"value": str(raw), "masked": False}
    return jsonify(cfg)


@app.route("/api/config", methods=["PUT"])
def update_config():
    """Update .env file and reload config in-memory."""
    body = request.get_json(silent=True) or {}
    updates = {}
    for key in CONFIGURABLE_KEYS:
        if key in body:
            val = body[key]
            # Skip masked placeholder values (user didn't change the field)
            if isinstance(val, str) and "••" in val:
                continue
            updates[key] = str(val)
    if updates:
        write_env_file(updates)
        reload_config()
    return jsonify({"success": True, "updated": list(updates.keys())})


# ---------------------------------------------------------------------------
# All Jobs API
# ---------------------------------------------------------------------------

@app.route("/api/all-jobs/fetch", methods=["POST"])
def fetch_all_jobs():
    """Fetch latest jobs from AACP and store in SQLite."""
    body = request.get_json(silent=True) or {}
    count = body.get("count", 100)  # initial fetch count
    refresh = body.get("refresh", False)  # True = incremental from latest known

    if not Config.PLATFORM_API_TOKEN or not Config.PLATFORM_API_BASE_URL:
        return jsonify({"error": "PLATFORM_API_BASE_URL and PLATFORM_API_TOKEN must be configured"}), 400

    try:
        api = PlatformAPI(
            token=Config.PLATFORM_API_TOKEN,
            base_url=Config.PLATFORM_API_BASE_URL,
        )

        stop_at_id = None
        if refresh:
            stop_at_id = db.get_latest_job_id("aacp")

        all_jobs = []
        offset = 0
        page_size = 25

        while len(all_jobs) < count:
            raw_jobs, has_more = api.get_all_jobs(
                limit=page_size, offset=offset, stop_at_id=stop_at_id
            )
            summaries = [extract_job_summary(j) for j in raw_jobs]
            all_jobs.extend(summaries)
            offset += page_size

            if not has_more:
                break

        # Store in SQLite
        db.upsert_jobs(all_jobs, source="aacp")

        return jsonify({
            "fetched": len(all_jobs),
            "message": f"Fetched {len(all_jobs)} jobs",
        })
    except PlatformAPIError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/all-jobs/load-more", methods=["POST"])
def load_more_jobs():
    """Fetch next page of jobs from AACP API and store."""
    body = request.get_json(silent=True) or {}
    offset = body.get("offset", 0)

    if not Config.PLATFORM_API_TOKEN or not Config.PLATFORM_API_BASE_URL:
        return jsonify({"error": "PLATFORM_API_BASE_URL and PLATFORM_API_TOKEN must be configured"}), 400

    try:
        api = PlatformAPI(
            token=Config.PLATFORM_API_TOKEN,
            base_url=Config.PLATFORM_API_BASE_URL,
        )
        raw_jobs, has_more = api.get_all_jobs(limit=25, offset=offset)
        summaries = [extract_job_summary(j) for j in raw_jobs]
        db.upsert_jobs(summaries, source="aacp")

        return jsonify({
            "fetched": len(summaries),
            "hasMore": has_more,
            "nextOffset": offset + 25,
        })
    except PlatformAPIError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/all-jobs/refresh", methods=["POST"])
def refresh_all_jobs():
    """Re-fetch all existing jobs from AACP to update their statuses.

    Counts how many jobs we have in the DB, then pages through
    that many from the API (in 25-job chunks) and upserts them.
    DBX cache is untouched — only job columns are updated.
    """
    if not Config.PLATFORM_API_TOKEN or not Config.PLATFORM_API_BASE_URL:
        return jsonify({"error": "PLATFORM_API_BASE_URL and PLATFORM_API_TOKEN must be configured"}), 400

    try:
        stats = db.get_kpi_stats("aacp")
        total_in_db = stats.get("total_jobs", 0)
        if total_in_db == 0:
            return jsonify({"refreshed": 0, "message": "No jobs to refresh"})

        api = PlatformAPI(
            token=Config.PLATFORM_API_TOKEN,
            base_url=Config.PLATFORM_API_BASE_URL,
        )

        all_jobs = []
        offset = 0
        page_size = 25

        while offset < total_in_db:
            raw_jobs, has_more = api.get_all_jobs(limit=page_size, offset=offset)
            summaries = [extract_job_summary(j) for j in raw_jobs]
            all_jobs.extend(summaries)
            offset += page_size

            if not has_more:
                break

        db.upsert_jobs(all_jobs, source="aacp")

        return jsonify({
            "refreshed": len(all_jobs),
            "totalInDb": total_in_db,
            "message": f"Refreshed {len(all_jobs)} jobs",
        })
    except PlatformAPIError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/all-jobs", methods=["GET"])
def query_all_jobs():
    """Query stored jobs with filters and pagination."""
    offset = request.args.get("offset", 0, type=int)
    limit = request.args.get("limit", 50, type=int)
    group = request.args.get("group", "", type=str)

    filters = {}
    for key in ("status", "flow_id", "flow_name", "creator_email", "ran_for", "search", "days",
                "error_rule", "error_category", "failure_stage", "error_signature",
                "schema_drift"):
        val = request.args.get(key, "").strip()
        if val:
            filters[key] = val

    if group == "flow":
        groups = db.get_jobs_grouped_by_flow(filters=filters)
        return jsonify({"grouped": True, "groups": groups})

    jobs, total = db.get_jobs(filters=filters, offset=offset, limit=limit)
    return jsonify({
        "jobs": jobs,
        "total": total,
        "offset": offset,
        "limit": limit,
        "hasMore": offset + limit < total,
    })


# ---------------------------------------------------------------------------
# Dashboard API
# ---------------------------------------------------------------------------

WINDOWS = ("1h", "6h", "24h", "7d", "14d")


def _dashboard_scope(args) -> tuple[str, Optional[str], dict, dict]:
    """
    Resolve the requested slice of data.

    A window sets the outer bounds; `from`/`to` narrow it further when the user
    clicks a bar in the trend chart. Dimension filters (category, cause, stage,
    flow) apply to every failure-oriented panel so the whole dashboard
    cross-filters together.
    """
    window = args.get("window", "24h")
    since = aac_sync.window_since(window)
    until = None
    if args.get("from"):
        since = max(since, args["from"]) if args["from"] >= since else since
        since = args["from"]
    if args.get("to"):
        until = args["to"]

    filters = {}
    for key in ("error_category", "error_rule", "failure_stage", "flow_id", "schema_drift"):
        val = (args.get(key) or "").strip()
        if val:
            filters[key] = val
    return since, until, filters, {"window": window, "from": args.get("from"),
                                   "to": args.get("to"), **filters}


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    """Everything the Dashboard tab needs for one slice of data, in one call."""
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        return jsonify({"error": f"window must be one of {', '.join(WINDOWS)}"}), 400

    since, until, filters, active = _dashboard_scope(request.args)
    bucket = aac_sync.bucket_minutes(window)
    return jsonify({
        "window": window,
        "since": since,
        "until": until,
        "filters": active,
        "summary": db.dashboard_summary(since, until=until, filters=filters),
        "timeseries": db.dashboard_timeseries(since, bucket, until=until, filters=filters),
        "bucketMinutes": bucket,
        "failureStages": db.failure_stage_breakdown(since, until=until, filters=filters),
        "errorCategories": db.error_category_breakdown(since, until=until, filters=filters),
        "errorRules": db.error_rule_breakdown(since, until=until, filters=filters),
        "errorMessages": db.error_messages(since, until=until, filters=filters, limit=25),
        "topFlows": db.top_failing_flows(since, until=until, filters=filters),
        "topUsers": db.top_failing_users(since, until=until, filters=filters),
        "ranFrom": db.ran_from_breakdown(since, until=until),
        "sync": aac_sync.get_status(),
    })


@app.route("/api/failures/message", methods=["GET"])
def failure_message_detail():
    """Flows and a full sample message for one error signature."""
    signature = request.args.get("signature", "")
    if not signature:
        return jsonify({"error": "signature is required"}), 400
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        return jsonify({"error": f"window must be one of {', '.join(WINDOWS)}"}), 400

    since, until, filters, _ = _dashboard_scope(request.args)
    rows = db.error_messages(since, until=until,
                             filters={**filters, "error_signature": signature}, limit=1)
    return jsonify({
        "signature": signature,
        "summary": rows[0] if rows else None,
        "flows": db.signature_flows(since, signature, until=until, limit=15),
    })


# ---------------------------------------------------------------------------
# Job detail + Databricks log bundle
# ---------------------------------------------------------------------------

STAGE_ORDER = {"ingest": 0, "schemadrift": 1, "wrangle": 2, "profile": 3,
               "filewriter": 4, "publish": 5}


def _stage_seconds(started: Optional[str], finished: Optional[str]):
    a, b = parse_iso(started or ""), parse_iso(finished or "")
    if a and b:
        return round((b - a).total_seconds(), 1)
    return None


@app.route("/api/job/<int:job_id>/detail", methods=["GET"])
def job_detail(job_id):
    """
    Everything known about one job group: the stored row, every stage with its
    status and duration, the classified error on the stage that actually failed,
    and the source/target the run touched.

    Fetched live rather than stored — the stage list is only needed when someone
    opens a row, and it costs one call.
    """
    stored = db.get_job(job_id)
    if not stored:
        return jsonify({"error": f"job {job_id} is not in the local store"}), 404
    if not Config.PLATFORM_API_TOKEN:
        return jsonify({"error": "PLATFORM_API_TOKEN is not configured"}), 400

    api = PlatformAPI()
    detail, warnings = {}, []
    try:
        resp = api.session.get(
            f"{api.base_url}/jobGroups/{job_id}?embed=jobs.errorMessage", timeout=60)
        detail = resp.json() if resp.status_code == 200 else {}
        if resp.status_code != 200:
            warnings.append(f"AAC returned {resp.status_code} for the job group")
    except Exception as e:
        warnings.append(f"Could not read the job group from AAC: {e}")

    inner = (detail.get("jobs") or {}).get("data") or []
    root = classifier.pick_root_failure(inner)

    stages = []
    for job in inner:
        parsed = classifier.parse_error_message(job.get("errorMessage"))
        has_error = bool(job.get("errorMessage"))
        job_type = (job.get("jobType") or "").lower()
        is_cascade = (parsed.get("exceptionType") or "").lower() == "upstreamjobfailed"
        stages.append({
            "id": job.get("id"),
            "jobType": job.get("jobType"),
            "status": job.get("status"),
            "startedAt": job.get("startedAt"),
            "finishedAt": job.get("finishedAt"),
            "durationSeconds": _stage_seconds(job.get("startedAt"), job.get("finishedAt")),
            "percentComplete": job.get("percentComplete"),
            "hasWarnings": job.get("hasWarnings"),
            "isRootFailure": bool(root and job.get("id") == root.get("id")),
            # Advisory: schema drift reports a source change but does not stop
            # the transformation. Cascade: skipped because an earlier stage
            # failed. Neither should read as the thing to investigate.
            "isAdvisory": job_type in classifier.ADVISORY_STAGES,
            "isCascade": is_cascade,
            "errorType": parsed.get("exceptionType") if has_error else None,
            "errorWhy": parsed.get("why") if has_error else None,
            "errorMessage": parsed.get("message") if has_error else None,
        })
    stages.sort(key=lambda s: (STAGE_ORDER.get((s["jobType"] or "").lower(), 99),
                               s.get("startedAt") or ""))

    schema_drift = classifier.has_advisory_failure(inner)

    failure = None
    if root is not None:
        parsed = classifier.parse_error_message(root.get("errorMessage"))
        root_is_advisory = (root.get("jobType") or "").lower() in classifier.ADVISORY_STAGES
        result = classifier.advisory_result() if root_is_advisory else classifier.classify(parsed)
        failure = {**result, "stage": root.get("jobType"), "advisory": root_is_advisory,
                 "exceptionType": parsed.get("exceptionType"),
                 "exceptionLabel": parsed.get("exceptionLabel"),
                 "errorCode": parsed.get("errorCode"),
                 "why": parsed.get("why"),
                 "message": parsed.get("message"),
                 "signature": classifier.signature(parsed)}

    # Source and destination: useful for debugging, one extra call each, and
    # non-fatal if the tenant withholds them.
    io = {}
    for key, path in (("inputs", f"/jobGroups/{job_id}/inputs"),
                      ("outputs", f"/jobGroups/{job_id}/outputs")):
        try:
            r = api.session.get(api.base_url + path, timeout=45)
            if r.status_code == 200:
                io[key] = r.json()
        except Exception:
            pass

    return jsonify({
        "job": stored,
        "stages": stages,
        "schemaDrift": schema_drift,
        # NOT "error": that key is reserved for API failures, and the UI treats
        # any truthy `error` as a failed request.
        "failure": failure,
        "inputs": io.get("inputs"),
        "outputs": io.get("outputs"),
        "databricks": {
            "runId": stored.get("databricks_job_id"),
            "jobId": stored.get("databricks_job_definition_id"),
            "workspace": stored.get("databricks_service_url"),
            "available": bool(stored.get("databricks_job_id") and stored.get("databricks_service_url")),
        },
        "warnings": warnings,
    })


@app.route("/api/job/<int:job_id>/dbx-logs", methods=["GET"])
def download_dbx_logs(job_id):
    """
    Stream a zip of the Databricks logs for this job's Spark run:
    driver stdout/stderr/log4j/stacktrace, the Spark event log, per-executor
    stdout/stderr, plus the run record and cluster event timeline.
    """
    stored = db.get_job(job_id)
    if not stored:
        return jsonify({"error": f"job {job_id} is not in the local store"}), 404

    run_id = stored.get("databricks_job_id")
    host = stored.get("databricks_service_url")
    if not run_id:
        return jsonify({"error": "This job has no Databricks run — it never reached Spark."}), 400
    if not host:
        return jsonify({"error": NO_HOST_ERROR}), 400
    if not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_TOKEN must be configured"}), 400

    bundle_path = DBX_BUNDLE_DIR / f"dbx-logs-{job_id}.zip"
    # Only reuse a cached bundle that is actually a complete zip — a partial
    # write from an interrupted build must never be served as the logs.
    reusable = (bundle_path.exists() and bundle_path.stat().st_size > 0
                and zipfile.is_zipfile(bundle_path)
                and request.args.get("refresh") != "1")
    try:
        if not reusable:
            dbx_logs.build_log_bundle(
                host=host, token=Config.DATABRICKS_TOKEN, run_id=int(run_id),
                out_path=bundle_path,
                job_meta={k: stored.get(k) for k in
                          ("job_id", "flow_id", "flow_name", "status", "created_at",
                           "creator_email", "failure_stage", "error_title", "error_category")},
            )
    except dbx_logs.DbxLogError as e:
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        log.exception("dbx log bundle failed")
        return jsonify({"error": f"Could not build the log bundle: {e}"}), 500

    flow = (stored.get("flow_name") or "job").replace("/", "_")[:60]
    return send_file(bundle_path, as_attachment=True,
                     download_name=f"dbx-logs-{flow}-{job_id}.zip",
                     mimetype="application/zip")


@app.route("/api/job/<int:job_id>/dbx-logs/available", methods=["GET"])
def dbx_logs_available(job_id):
    """
    Whether logs can actually be fetched, and how big they are, so the UI can
    say so before someone waits on a download.
    """
    stored = db.get_job(job_id)
    if not stored:
        return jsonify({"error": f"job {job_id} is not in the local store"}), 404
    run_id, host = stored.get("databricks_job_id"), stored.get("databricks_service_url")
    if not run_id or not host or not Config.DATABRICKS_TOKEN:
        return jsonify({"available": False,
                        "reason": "No Databricks run recorded for this job."})
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient(host=str(host).rstrip("/"), token=Config.DATABRICKS_TOKEN)
        ctx = dbx_logs.resolve_run_context(client, int(run_id))
        inv = dbx_logs.list_available_logs(client, ctx.get("clusterId"))
        return jsonify({"available": bool(inv["files"]), "clusterId": ctx.get("clusterId"),
                        "runName": ctx.get("runName"), "resultState": ctx.get("resultState"),
                        "byKind": inv["byKind"], "totalBytes": inv["totalBytes"],
                        "fileCount": len(inv["files"])})
    except dbx_logs.DbxLogError as e:
        return jsonify({"available": False, "reason": str(e)})
    except Exception as e:
        return jsonify({"available": False, "reason": f"Databricks lookup failed: {e}"})


@app.route("/api/retention", methods=["GET"])
def retention_status():
    """Retention window, what it would drop, and how much disk the store uses."""
    cutoff = aac_sync.window_since_days(aac_sync.RETENTION_DAYS)
    return jsonify({
        "retentionDays": aac_sync.RETENTION_DAYS,
        "cutoff": cutoff,
        "expiredJobs": db.count_jobs_older_than(cutoff),
        "storage": db.database_stats(),
        "lastPurgeAt": db.get_sync_state("last_purge_at"),
        "lastPurgeCount": db.get_sync_state("last_purge_count"),
    })


@app.route("/api/retention/purge", methods=["POST"])
def purge_now():
    """Apply retention immediately rather than waiting for the next sync."""
    deleted = aac_sync.purge_expired()
    return jsonify({"deleted": deleted, "storage": db.database_stats()})


@app.route("/api/reclassify", methods=["POST"])
def reclassify_failures():
    """
    Re-run the classification rules over stored failures.

    Use after editing error_rules.json — it reads the error text already stored,
    so nothing is re-fetched from AAC.
    """
    body = request.get_json(silent=True) or {}
    result = aac_sync.reclassify(only_unclassified=not body.get("all"))
    return jsonify(result)


@app.route("/api/reenrich", methods=["POST"])
def reenrich_failures():
    """
    Queue stored failures to be read from AAC again.

    Needed when the stage list matters — root-cause attribution and the
    schema-drift flag come from it and are not stored. Pass {"stage": "..."} to
    limit the scope, or {"all": true} for every failure. The next sync does the
    work; pass {"sync": true} to start one immediately.
    """
    body = request.get_json(silent=True) or {}
    stage = body.get("stage")
    if not stage and not body.get("all"):
        return jsonify({"error": "pass {\"all\": true} or {\"stage\": \"<stage>\"}"}), 400
    queued = db.clear_enrichment(failure_stage=stage)
    started = aac_sync.run_sync() if body.get("sync") else {"started": False}
    return jsonify({"queued": queued, "sync": started})


@app.route("/api/error-rules", methods=["GET"])
def get_error_rules():
    """The classification rules, so the UI can show each cause's remediation hint."""
    import classifier
    return jsonify({"rules": [
        {"id": r.get("id"), "title": r.get("title"), "category": r.get("category"), "hint": r.get("hint")}
        for r in classifier.load_rules()
    ]})


@app.route("/api/sync", methods=["POST"])
def start_sync():
    """Kick off a background sync. `full=true` re-walks the whole retention window."""
    body = request.get_json(silent=True) or {}
    result = aac_sync.run_sync(full=bool(body.get("full")))
    return jsonify({**result, "status": aac_sync.get_status()})


@app.route("/api/sync/status", methods=["GET"])
def sync_status():
    return jsonify(aac_sync.get_status())


@app.route("/api/failures", methods=["GET"])
def list_failures():
    """Failed job groups in a window, optionally narrowed to one cause or flow."""
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        return jsonify({"error": f"window must be one of {', '.join(WINDOWS)}"}), 400

    filters = {"status": "Failed", "created_since": aac_sync.window_since(window)}
    for key in ("error_rule", "error_category", "failure_stage", "flow_id", "creator_email",
                "schema_drift"):
        val = request.args.get(key, "").strip()
        if val:
            filters[key] = val

    jobs, total = db.get_jobs(
        filters=filters,
        offset=request.args.get("offset", 0, type=int),
        limit=request.args.get("limit", 50, type=int),
    )
    return jsonify({"jobs": jobs, "total": total, "window": window})


@app.route("/api/all-jobs/kpis", methods=["GET"])
def all_jobs_kpis():
    """Return KPI stats for the All Jobs tab."""
    days = request.args.get("days", 30, type=int)
    stats = db.get_kpi_stats("aacp", days=days)
    return jsonify(stats)


@app.route("/api/all-jobs/daily-chart", methods=["GET"])
def all_jobs_daily_chart():
    """Return daily job counts (success/failed/other) for bar chart."""
    days = request.args.get("days", 30, type=int)
    status_filter = request.args.get("status", "", type=str).strip()
    data = db.get_daily_job_counts(source="aacp", days=days, status_filter=status_filter)
    return jsonify({"days": data})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Initialize DB on import
db.init_db()
db.migrate_from_files()

if __name__ == "__main__":
    # Defaults are deliberately conservative for something handed to a customer:
    #   - loopback only, so the dashboard and its tokens are not served to the
    #     whole network; set HOST=0.0.0.0 to share it deliberately
    #   - debug off, since Werkzeug's debugger allows code execution in-browser
    #     on any traceback, and its reloader restarts the server mid-request
    port = int(os.getenv("PORT", "5050"))
    host = os.getenv("HOST", "127.0.0.1")
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
    )
    print(f"🚀 Tri-Tracker starting on http://localhost:{port}")
    app.run(host=host, port=port, debug=debug)
