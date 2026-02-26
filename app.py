#!/usr/bin/env python3
"""
Tri-Tracker – Flask API Server

Serves the web UI and provides endpoints to:
  - Fetch jobs from AAC (and optionally on-prem Trifacta) environments
  - Match jobs across environments by creation time (±10 min)
  - Read/update configuration (tokens masked on read)
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from config import Config
from platform_api import PlatformAPI, PlatformAPIError
from databricks_client import DatabricksClient, DatabricksClientError
from dbfs_eventlog import download_eventlog, EventLogError
import flow_store

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ENV_FILE = Path(__file__).resolve().parent / ".env"
LEGACY_JOBS_DIR = Path(__file__).resolve().parent / "jobs"
ANALYZER_SCRIPT = Path(__file__).resolve().parent / "eventlog-analyzer" / "analyze_eventlog.py"

# Keys we allow to be read/written through the config API
CONFIGURABLE_KEYS = [
    "PLATFORM_API_BASE_URL",
    "PLATFORM_API_TOKEN",
    "ONPREM_ENABLED",
    "ONPREM_API_BASE_URL",
    "ONPREM_API_TOKEN",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DEFAULT_LIMIT",
    "RANFOR_FILTER",
    "MATCH_WINDOW_MINUTES",
]

# Keys whose values should be masked when sent to the browser
SECRET_KEYS = {"PLATFORM_API_TOKEN", "ONPREM_API_TOKEN", "DATABRICKS_TOKEN"}


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
    Config.DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
    Config.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    Config.DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "25"))
    Config.ONPREM_ENABLED = os.getenv("ONPREM_ENABLED", "true").lower() in ("true", "1", "yes")
    Config.RANFOR_FILTER = os.getenv("RANFOR_FILTER", "recipe,plan")
    Config.MATCH_WINDOW_MINUTES = int(os.getenv("MATCH_WINDOW_MINUTES", "10"))


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

    # The inner job details (executionLanguage, cpJobId) live inside jobs.data[0]
    inner_job = None
    jobs_wrapper = job_data.get("jobs")
    if jobs_wrapper:
        jobs_list = jobs_wrapper.get("data", [])
        if jobs_list:
            inner_job = jobs_list[0]

    execution_language = None
    databricks_job_id = None
    if inner_job:
        execution_language = inner_job.get("executionLanguage")
        # Parse databricksJobId from cpJobId JSON string
        # e.g. cpJobId: '{"databricksWorkspaceId":"...","databricksJobId":"943293893227722"}'
        cp_job_id_raw = inner_job.get("cpJobId")
        if cp_job_id_raw and isinstance(cp_job_id_raw, str):
            try:
                cp_job = json.loads(cp_job_id_raw)
                databricks_job_id = cp_job.get("databricksJobId")
            except (json.JSONDecodeError, TypeError):
                pass

    return {
        "jobRunId": job_data.get("id"),
        "jobGroupId": job_data.get("id"),
        "status": job_data.get("status"),
        "flowId": flow_id,
        "flowName": flow_name,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "executionTimeMinutes": compute_execution_minutes(created_at, updated_at),
        "executionLanguage": execution_language,
        "databricksJobId": databricks_job_id,
    }


def match_jobs(aac_jobs: list[dict], onprem_jobs: list[dict], window_minutes: int = 10) -> list[dict]:
    """
    Match AAC and on-prem jobs by createdAt within ±window_minutes.
    Returns a list of paired rows.
    """
    used_onprem = set()
    pairs = []

    for aac in aac_jobs:
        aac_dt = parse_iso(aac.get("createdAt", ""))
        best_match = None
        best_delta = None

        if aac_dt:
            for idx, op in enumerate(onprem_jobs):
                if idx in used_onprem:
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
    onprem_enabled = getattr(Config, 'ONPREM_ENABLED', True)
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
    new_pairs = match_jobs(results["aac"], results["onprem"], window_minutes=window)

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
    merged_pairs = flow_store.merge_jobs(flow_name, new_pairs, metadata)

    results["pairs"] = merged_pairs
    results["matchWindowMinutes"] = window
    results["errors"] = errors
    results["aacBaseUrl"] = aac_base
    results["onpremBaseUrl"] = onprem_base
    results["onpremEnabled"] = onprem_enabled

    # Include cache status for the frontend
    results["analyzedJobs"] = flow_store.list_analyzed_jobs(flow_name)
    results["dbxCachedJobs"] = flow_store.list_dbx_cached_jobs(flow_name)

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
        cached = flow_store.load_dbx(flow_name, str(job_run_id))
        if cached:
            cached["cached"] = True
            return jsonify(cached)

    if not Config.DATABRICKS_HOST or not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_HOST and DATABRICKS_TOKEN must be configured"}), 400

    try:
        dbx = DatabricksClient(host=Config.DATABRICKS_HOST, token=Config.DATABRICKS_TOKEN)
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
            "databricksHost": Config.DATABRICKS_HOST,
            "runDetails": run_details,
            "clusterEvents": cluster_events,
            "cached": False,
        }

        # Save to cache
        if flow_name and job_run_id:
            flow_store.save_dbx(flow_name, str(job_run_id), response_data)

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
        flow_store.clear_dbx_job(flow_name, str(job_run_id))

    if not Config.DATABRICKS_HOST or not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_HOST and DATABRICKS_TOKEN must be configured"}), 400

    try:
        dbx = DatabricksClient(host=Config.DATABRICKS_HOST, token=Config.DATABRICKS_TOKEN)
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
            "databricksHost": Config.DATABRICKS_HOST,
            "runDetails": run_details,
            "clusterEvents": cluster_events,
            "cached": False,
        }

        # Save fresh data to cache
        if flow_name and job_run_id:
            flow_store.save_dbx(flow_name, str(job_run_id), response_data)

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
    if not Config.DATABRICKS_HOST or not Config.DATABRICKS_TOKEN:
        return jsonify({"error": "DATABRICKS_HOST and DATABRICKS_TOKEN must be configured"}), 400

    # Determine storage directory
    if flow_name:
        job_dir = Path(flow_store.eventlog_dir(flow_name, str(job_run_id)))
    else:
        job_dir = LEGACY_JOBS_DIR / str(job_run_id)
    analysis_file = job_dir / "analysis.json"

    # Return cached analysis if it exists
    if analysis_file.exists():
        with open(analysis_file, "r") as f:
            return jsonify({"status": "complete", "cached": True, "analysis": json.load(f)})

    # Step 1: Download event log from DBFS
    try:
        eventlog_path = download_eventlog(
            host=Config.DATABRICKS_HOST,
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
    # Check flow-based storage first
    flow_name = request.args.get("flowName", "").strip()
    if flow_name:
        analysis_file = Path(flow_store.eventlog_dir(flow_name, str(job_run_id))) / "analysis.json"
    else:
        analysis_file = LEGACY_JOBS_DIR / str(job_run_id) / "analysis.json"

    if not analysis_file.exists():
        return jsonify({"error": "No analysis found for this job run", "exists": False}), 404

    with open(analysis_file, "r") as f:
        return jsonify({"status": "complete", "exists": True, "analysis": json.load(f)})

@app.route("/api/flows", methods=["GET"])
def get_flows():
    """Return list of saved flows with their cached data."""
    flows_list = flow_store.list_flows()
    result = []
    for f in flows_list:
        flow_data = flow_store.load_flow(f["name"])
        if flow_data:
            result.append(flow_data)
    return jsonify({"flows": result})


@app.route("/api/flows/<path:flow_name>", methods=["DELETE"])
def remove_flow(flow_name):
    """Remove a flow and all its data."""
    deleted = flow_store.delete_flow(flow_name)
    return jsonify({"success": deleted})


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
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("🚀 Tri-Tracker starting on http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
