"""
Database Layer — SQLite persistence for Tri-Tracker.

Replaces the file-based flow_store.py for structured data (jobs, flows, DBX cache).
Event logs and analysis.json files remain on disk.

DB file: tri-tracker.db in the app root.
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "tri-tracker.db"

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """Get a connection with WAL mode and foreign keys enabled."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    """Context manager for database operations."""
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          INTEGER PRIMARY KEY,
    status          TEXT,
    flow_id         INTEGER,
    flow_name       TEXT,
    ran_for         TEXT,
    ran_from        TEXT,
    creator_email   TEXT,
    created_at      TEXT,
    updated_at      TEXT,
    execution_time_min REAL,
    execution_language TEXT,
    databricks_job_id TEXT,
    databricks_job_definition_id TEXT,
    databricks_service_url TEXT,
    source          TEXT DEFAULT 'aacp',
    fetched_at      TEXT,
    -- Failure analysis (populated by the enrichment pass, failures only)
    failure_stage   TEXT,
    error_rule      TEXT,
    error_title     TEXT,
    error_category  TEXT,
    error_type      TEXT,
    error_label     TEXT,
    error_code      INTEGER,
    error_why       TEXT,
    error_detail    TEXT,
    error_message_id INTEGER,
    error_signature TEXT,
    enriched_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_flow_id ON jobs(flow_id);
CREATE INDEX IF NOT EXISTS idx_jobs_flow_name ON jobs(flow_name);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

CREATE TABLE IF NOT EXISTS flows (
    name            TEXT PRIMARY KEY,
    pairs           TEXT,
    aac_base_url    TEXT,
    onprem_base_url TEXT,
    onprem_enabled  INTEGER DEFAULT 1,
    match_window    INTEGER DEFAULT 10,
    errors          TEXT,
    last_fetched    TEXT
);

CREATE TABLE IF NOT EXISTS sync_state (
    key             TEXT PRIMARY KEY,
    value           TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS dbx_cache (
    flow_name       TEXT,
    job_run_id      TEXT,
    data            TEXT,
    cached_at       TEXT,
    PRIMARY KEY (flow_name, job_run_id)
);
"""


# Columns added after the initial release. CREATE TABLE IF NOT EXISTS will not
# add them to a database created by an older build, so apply them explicitly.
MIGRATIONS = [
    ("jobs", "databricks_job_definition_id", "TEXT"),
    ("jobs", "databricks_service_url", "TEXT"),
    ("jobs", "failure_stage", "TEXT"),
    ("jobs", "error_rule", "TEXT"),
    ("jobs", "error_title", "TEXT"),
    ("jobs", "error_category", "TEXT"),
    ("jobs", "error_type", "TEXT"),
    ("jobs", "error_label", "TEXT"),
    ("jobs", "error_code", "INTEGER"),
    ("jobs", "error_why", "TEXT"),
    ("jobs", "error_detail", "TEXT"),
    ("jobs", "error_message_id", "INTEGER"),
    ("jobs", "enriched_at", "TEXT"),
    ("jobs", "error_signature", "TEXT"),
]


def _apply_migrations(conn):
    """Add any columns missing from an existing database."""
    for table, column, coltype in MIGRATIONS:
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if not existing:
            continue  # table not created yet; SCHEMA_SQL already has the column
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
            log.info(f"Migrated {table}: added column {column}")


# Indexes that cover migrated columns — created after the ALTER TABLEs so they
# also apply to databases upgraded from an older build.
POST_MIGRATION_SQL = """
CREATE INDEX IF NOT EXISTS idx_jobs_error_rule ON jobs(error_rule);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_enriched ON jobs(status, enriched_at);
CREATE INDEX IF NOT EXISTS idx_jobs_error_signature ON jobs(error_signature);
"""


def init_db():
    """Initialize the database schema."""
    with get_db() as conn:
        conn.executescript(SCHEMA_SQL)
        _apply_migrations(conn)
        conn.executescript(POST_MIGRATION_SQL)
    log.info(f"Database initialized at {DB_PATH}")


# ---------------------------------------------------------------------------
# Jobs CRUD
# ---------------------------------------------------------------------------

def upsert_jobs(jobs: list[dict], source: str = "aacp"):
    """Insert or update jobs from API response."""
    if not jobs:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        count = 0
        for j in jobs:
            job_id = j.get("jobRunId") or j.get("id")
            if not job_id:
                continue
            conn.execute("""
                INSERT INTO jobs (job_id, status, flow_id, flow_name, ran_for, ran_from,
                                  creator_email, created_at, updated_at, execution_time_min,
                                  execution_language, databricks_job_id,
                                  databricks_job_definition_id, databricks_service_url,
                                  source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    status = excluded.status,
                    ran_for = excluded.ran_for,
                    ran_from = excluded.ran_from,
                    creator_email = excluded.creator_email,
                    updated_at = excluded.updated_at,
                    execution_time_min = excluded.execution_time_min,
                    execution_language = COALESCE(excluded.execution_language, jobs.execution_language),
                    databricks_job_id = COALESCE(excluded.databricks_job_id, jobs.databricks_job_id),
                    databricks_job_definition_id = COALESCE(
                        excluded.databricks_job_definition_id, jobs.databricks_job_definition_id),
                    databricks_service_url = COALESCE(
                        excluded.databricks_service_url, jobs.databricks_service_url),
                    fetched_at = excluded.fetched_at
            """, (
                int(job_id),
                j.get("status"),
                j.get("flowId"),
                j.get("flowName"),
                j.get("ranFor"),
                j.get("ranFrom"),
                j.get("creatorEmail"),
                j.get("createdAt"),
                j.get("updatedAt"),
                j.get("executionTimeMinutes"),
                j.get("executionLanguage"),
                j.get("databricksJobId"),
                j.get("databricksJobDefinitionId"),
                j.get("databricksServiceUrl"),
                source,
                now,
            ))
            count += 1
    log.info(f"Upserted {count} jobs (source={source})")
    return count


def get_jobs(
    filters: Optional[dict] = None,
    offset: int = 0,
    limit: int = 25,
    source: str = "aacp",
) -> tuple[list[dict], int]:
    """
    Query jobs with optional filters.

    filters keys: status, flow_id, flow_name, creator_email, ran_for, search (text across fields)
    Returns: (list_of_jobs, total_count)
    """
    where_clauses = ["source = ?"]
    params: list[Any] = [source]

    if filters:
        if filters.get("status"):
            where_clauses.append("LOWER(status) = LOWER(?)")
            params.append(filters["status"])
        if filters.get("flow_id"):
            where_clauses.append("flow_id = ?")
            params.append(filters["flow_id"])
        if filters.get("flow_name"):
            where_clauses.append("flow_name LIKE ?")
            params.append(f"%{filters['flow_name']}%")
        if filters.get("creator_email"):
            where_clauses.append("creator_email LIKE ?")
            params.append(f"%{filters['creator_email']}%")
        if filters.get("ran_for"):
            where_clauses.append("LOWER(ran_for) = LOWER(?)")
            params.append(filters["ran_for"])
        if filters.get("days"):
            where_clauses.append("created_at >= datetime('now', ?)")
            params.append(f"-{filters['days']} days")
        if filters.get("created_since"):
            where_clauses.append("created_at >= ?")
            params.append(filters["created_since"])
        for col in ("error_rule", "error_category", "failure_stage", "error_signature"):
            if filters.get(col):
                where_clauses.append(f"{col} = ?")
                params.append(filters[col])
        if filters.get("search"):
            search = f"%{filters['search']}%"
            where_clauses.append(
                "(CAST(job_id AS TEXT) LIKE ? OR flow_name LIKE ? OR creator_email LIKE ? OR status LIKE ?)"
            )
            params.extend([search, search, search, search])

    where = " AND ".join(where_clauses)

    with get_db() as conn:
        # Total count
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM jobs WHERE {where}", params).fetchone()
        total = row["cnt"]

        # Page of results
        rows = conn.execute(
            f"SELECT * FROM jobs WHERE {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

    return [dict(r) for r in rows], total


def get_job_dbx_host(job_id) -> Optional[str]:
    """Return the Databricks workspace URL recorded for a job, if we have one."""
    try:
        job_id = int(job_id)
    except (TypeError, ValueError):
        return None
    with get_db() as conn:
        row = conn.execute(
            "SELECT databricks_service_url FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
    return row["databricks_service_url"] if row else None


def get_latest_job_id(source: str = "aacp") -> Optional[int]:
    """Return the most recent job_id we have stored."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT job_id FROM jobs WHERE source = ? ORDER BY created_at DESC LIMIT 1",
            (source,),
        ).fetchone()
    return row["job_id"] if row else None


def get_jobs_grouped_by_flow(
    filters: Optional[dict] = None,
    source: str = "aacp",
) -> list[dict]:
    """
    Return jobs grouped by flow_id with aggregate stats.
    Each group has: flow_id, flow_name, job_count, min_exec, max_exec, avg_exec,
    earliest_created_at, latest_created_at, status_counts, ran_from_counts, creators, jobs[]
    """
    where_clauses = ["source = ?"]
    params: list[Any] = [source]

    if filters:
        if filters.get("status"):
            where_clauses.append("LOWER(status) = LOWER(?)")
            params.append(filters["status"])
        if filters.get("flow_name"):
            where_clauses.append("flow_name LIKE ?")
            params.append(f"%{filters['flow_name']}%")
        if filters.get("creator_email"):
            where_clauses.append("creator_email LIKE ?")
            params.append(f"%{filters['creator_email']}%")
        if filters.get("days"):
            where_clauses.append("created_at >= datetime('now', ?)")
            params.append(f"-{filters['days']} days")
        if filters.get("created_since"):
            where_clauses.append("created_at >= ?")
            params.append(filters["created_since"])
        for col in ("error_rule", "error_category", "failure_stage", "error_signature"):
            if filters.get(col):
                where_clauses.append(f"{col} = ?")
                params.append(filters[col])

    where = " AND ".join(where_clauses)

    with get_db() as conn:
        groups = conn.execute(f"""
            SELECT flow_id, flow_name,
                   COUNT(*) as job_count,
                   MIN(execution_time_min) as min_exec,
                   MAX(execution_time_min) as max_exec,
                   ROUND(AVG(execution_time_min), 1) as avg_exec,
                   MIN(created_at) as earliest_created_at,
                   MAX(created_at) as latest_created_at
            FROM jobs
            WHERE {where} AND flow_id IS NOT NULL
            GROUP BY flow_id
            ORDER BY MAX(created_at) DESC
        """, params).fetchall()

        result = []
        for g in groups:
            jobs = conn.execute(
                f"SELECT * FROM jobs WHERE {where} AND flow_id = ? ORDER BY created_at DESC",
                params + [g["flow_id"]],
            ).fetchall()

            jobs_list = [dict(j) for j in jobs]

            # Compute status counts
            status_counts = {}
            for j in jobs_list:
                s = j.get("status", "Unknown")
                status_counts[s] = status_counts.get(s, 0) + 1

            # Compute ran_from counts
            ran_from_counts = {}
            for j in jobs_list:
                rf = j.get("ran_from") or "Unknown"
                ran_from_counts[rf] = ran_from_counts.get(rf, 0) + 1

            # Compute ran_for counts
            ran_for_counts = {}
            for j in jobs_list:
                rfr = j.get("ran_for") or "Unknown"
                ran_for_counts[rfr] = ran_for_counts.get(rfr, 0) + 1

            # Distinct creators
            creators = list({j.get("creator_email", "") for j in jobs_list if j.get("creator_email")})

            result.append({
                "flow_id": g["flow_id"],
                "flow_name": g["flow_name"],
                "job_count": g["job_count"],
                "min_exec": g["min_exec"],
                "max_exec": g["max_exec"],
                "avg_exec": g["avg_exec"],
                "earliest_created_at": g["earliest_created_at"],
                "latest_created_at": g["latest_created_at"],
                "status_counts": status_counts,
                "ran_from_counts": ran_from_counts,
                "ran_for_counts": ran_for_counts,
                "creators": creators,
                "jobs": jobs_list,
            })

    return result


def get_kpi_stats(source: str = "aacp", days: int = 30) -> dict:
    """Compute KPI stats: total jobs, success rate, jobs per day in the given window."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE source = ? AND created_at >= datetime('now', ?)",
            (source, f"-{days} days")
        ).fetchone()["cnt"]

        completed = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE source = ? AND LOWER(status) IN ('complete', 'completed') AND created_at >= datetime('now', ?)",
            (source, f"-{days} days"),
        ).fetchone()["cnt"]

    return {
        "total_jobs": total,
        "completed_jobs": completed,
        "success_rate": round(completed / total * 100, 1) if total > 0 else 0,
        "jobs_per_day": round(total / days, 1),
    }


def get_daily_job_counts(
    source: str = "aacp",
    days: int = 30,
    status_filter: str = "",
) -> list[dict]:
    """Return daily job counts broken down by success/failure for the last N days.

    Each element: {date: "YYYY-MM-DD", success: int, failed: int, other: int}
    If status_filter is set, only that status is counted (still split into the
    appropriate bucket so the chart keeps its colour semantics).
    """
    where_clauses = ["source = ?", "created_at >= datetime('now', ?)"]
    params: list = [source, f"-{days} days"]

    if status_filter:
        where_clauses.append("LOWER(status) = LOWER(?)")
        params.append(status_filter)

    where = " AND ".join(where_clauses)

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT DATE(created_at) as day,
                   SUM(CASE WHEN LOWER(status) IN ('complete','completed') THEN 1 ELSE 0 END) as success,
                   SUM(CASE WHEN LOWER(status) = 'failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN LOWER(status) NOT IN ('complete','completed','failed') THEN 1 ELSE 0 END) as other
            FROM jobs
            WHERE {where}
            GROUP BY DATE(created_at)
            ORDER BY day ASC
        """, params).fetchall()

    return [{"date": r["day"], "success": r["success"], "failed": r["failed"], "other": r["other"]} for r in rows]


# ---------------------------------------------------------------------------
# Flow CRUD (replaces flow_store.py for structured data)
# ---------------------------------------------------------------------------

def save_flow(name: str, pairs: list, metadata: dict):
    """Save or update a flow."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO flows (name, pairs, aac_base_url, onprem_base_url, onprem_enabled,
                               match_window, errors, last_fetched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                pairs = excluded.pairs,
                aac_base_url = excluded.aac_base_url,
                onprem_base_url = excluded.onprem_base_url,
                onprem_enabled = excluded.onprem_enabled,
                match_window = excluded.match_window,
                errors = excluded.errors,
                last_fetched = excluded.last_fetched
        """, (
            name,
            json.dumps(pairs, default=str),
            metadata.get("aacBaseUrl", ""),
            metadata.get("onpremBaseUrl", ""),
            1 if metadata.get("onpremEnabled", True) else 0,
            metadata.get("matchWindowMinutes", 10),
            json.dumps(metadata.get("errors", []), default=str),
            now,
        ))
    log.info(f"Saved flow '{name}' with {len(pairs)} pairs")


def load_flow(name: str) -> Optional[dict]:
    """Load a flow by name."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM flows WHERE name = ?", (name,)).fetchone()
    if not row:
        return None
    
    pairs = json.loads(row["pairs"]) if row["pairs"] else []
    _sort_pairs(pairs)

    return {
        "name": row["name"],
        "pairs": pairs,
        "aacBaseUrl": row["aac_base_url"] or "",
        "onpremBaseUrl": row["onprem_base_url"] or "",
        "onpremEnabled": bool(row["onprem_enabled"]),
        "matchWindowMinutes": row["match_window"],
        "errors": json.loads(row["errors"]) if row["errors"] else [],
        "lastFetched": row["last_fetched"],
    }


def list_flows() -> list[dict]:
    """Return all flows with basic metadata."""
    with get_db() as conn:
        rows = conn.execute("SELECT name, last_fetched, pairs FROM flows ORDER BY name").fetchall()
    return [{
        "name": r["name"],
        "lastFetched": r["last_fetched"],
        "jobCount": len(json.loads(r["pairs"])) if r["pairs"] else 0,
    } for r in rows]


def delete_flow(name: str) -> bool:
    """Delete a flow and its DBX cache entries."""
    with get_db() as conn:
        conn.execute("DELETE FROM dbx_cache WHERE flow_name = ?", (name,))
        result = conn.execute("DELETE FROM flows WHERE name = ?", (name,))
    deleted = result.rowcount > 0
    if deleted:
        log.info(f"Deleted flow '{name}'")
    return deleted


def merge_jobs_flow(name: str, new_pairs: list, metadata: dict) -> list:
    """Merge new pair data with existing flow data. Returns merged pairs."""
    existing = load_flow(name)
    if not existing or not existing.get("pairs"):
        save_flow(name, new_pairs, metadata)
        return new_pairs

    existing_pairs = existing["pairs"]
    index = {}
    for pair in existing_pairs:
        key = _pair_key(pair)
        if key:
            index[key] = pair

    for pair in new_pairs:
        key = _pair_key(pair)
        if key and key in index:
            old = index[key]
            if pair.get("aac"):
                old["aac"] = pair["aac"]
            if pair.get("onprem"):
                old["onprem"] = pair["onprem"]
            old["matched"] = pair.get("matched", old.get("matched", False))
        elif key:
            existing_pairs.append(pair)
            index[key] = pair

    _sort_pairs(existing_pairs)
    save_flow(name, existing_pairs, metadata)
    return existing_pairs


def _pair_key(pair: dict) -> Optional[str]:
    aac = pair.get("aac")
    if aac and aac.get("jobRunId"):
        return f"aac_{aac['jobRunId']}"
    onprem = pair.get("onprem")
    if onprem and onprem.get("jobRunId"):
        return f"op_{onprem['jobRunId']}"
    return None


def _sort_pairs(pairs: list):
    """Sort pairs in reverse chronological order based on job createdAt time."""
    def get_time(p):
        aac = p.get("aac") or {}
        op = p.get("onprem") or {}
        return aac.get("createdAt") or op.get("createdAt") or ""
    pairs.sort(key=get_time, reverse=True)


# ---------------------------------------------------------------------------
# DBX Cache
# ---------------------------------------------------------------------------

def load_dbx(flow_name: str, job_run_id: str) -> Optional[dict]:
    """Return cached DBX details or None."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM dbx_cache WHERE flow_name = ? AND job_run_id = ?",
            (flow_name, job_run_id),
        ).fetchone()
    if row:
        return json.loads(row["data"])
    return None


def save_dbx(flow_name: str, job_run_id: str, data: dict):
    """Persist DBX details."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO dbx_cache (flow_name, job_run_id, data, cached_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(flow_name, job_run_id) DO UPDATE SET
                data = excluded.data, cached_at = excluded.cached_at
        """, (flow_name, job_run_id, json.dumps(data, default=str), now))
    log.info(f"Cached DBX for flow='{flow_name}' job={job_run_id}")


def list_dbx_cached_jobs(flow_name: str) -> list[str]:
    """Return list of job_run_ids that have cached DBX data."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT job_run_id FROM dbx_cache WHERE flow_name = ?",
            (flow_name,),
        ).fetchall()
    return [r["job_run_id"] for r in rows]


def clear_dbx_job(flow_name: str, job_run_id: str) -> bool:
    """Remove cached DBX details for a specific job run."""
    with get_db() as conn:
        result = conn.execute(
            "DELETE FROM dbx_cache WHERE flow_name = ? AND job_run_id = ?",
            (flow_name, job_run_id),
        )
    deleted = result.rowcount > 0
    if deleted:
        log.info(f"Cleared DBX cache for flow='{flow_name}' job={job_run_id}")
    return deleted


# ---------------------------------------------------------------------------
# Migration from file-based flow_store
# ---------------------------------------------------------------------------

def migrate_from_files():
    """
    Migrate existing file-based data into SQLite.
    Reads flows/<name>/flow_data.json and flows/<name>/dbx/<id>.json
    """
    flows_dir = Path(__file__).resolve().parent / "flows"
    if not flows_dir.exists():
        return

    migrated = 0
    for entry in flows_dir.iterdir():
        if not entry.is_dir():
            continue

        flow_data_file = entry / "flow_data.json"
        if not flow_data_file.exists():
            continue

        try:
            with open(flow_data_file) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"Skipping migration of {flow_data_file}: {e}")
            continue

        name = data.get("name", entry.name)

        # Check if flow already exists in DB
        existing = load_flow(name)
        if existing:
            continue

        pairs = data.get("pairs", [])
        metadata = {
            "aacBaseUrl": data.get("aacBaseUrl", ""),
            "onpremBaseUrl": data.get("onpremBaseUrl", ""),
            "onpremEnabled": data.get("onpremEnabled", True),
            "matchWindowMinutes": data.get("matchWindowMinutes", 10),
            "errors": data.get("errors", []),
        }
        save_flow(name, pairs, metadata)
        migrated += 1

        # Migrate DBX cache
        dbx_dir = entry / "dbx"
        if dbx_dir.exists():
            for dbx_file in dbx_dir.glob("*.json"):
                try:
                    with open(dbx_file) as f:
                        dbx_data = json.load(f)
                    save_dbx(name, dbx_file.stem, dbx_data)
                except (json.JSONDecodeError, IOError) as e:
                    log.warning(f"Skipping DBX migration {dbx_file}: {e}")

    if migrated > 0:
        log.info(f"Migrated {migrated} flows from files to SQLite")


# ---------------------------------------------------------------------------
# Sync bookkeeping
# ---------------------------------------------------------------------------

def get_sync_state(key: str, default=None):
    """Read a value from the sync_state key/value table."""
    with get_db() as conn:
        row = conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_sync_state(key: str, value: str):
    """Write a value to the sync_state key/value table."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sync_state (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, str(value), now),
        )


# ---------------------------------------------------------------------------
# Failure enrichment
# ---------------------------------------------------------------------------

def failures_needing_enrichment(limit: int = 200, source: str = "aacp") -> list[int]:
    """Job ids of stored failures that have not been enriched with error detail yet."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT job_id FROM jobs WHERE source = ? AND status = 'Failed' AND enriched_at IS NULL "
            "ORDER BY created_at DESC LIMIT ?",
            (source, limit),
        ).fetchall()
    return [r["job_id"] for r in rows]


def save_failure_details(job_id: int, details: dict):
    """Store the root failure stage and classified error for one job group."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("""
            UPDATE jobs SET
                failure_stage = ?, error_rule = ?, error_title = ?, error_category = ?,
                error_type = ?, error_label = ?, error_code = ?, error_why = ?,
                error_detail = ?, error_message_id = ?, error_signature = ?, enriched_at = ?
            WHERE job_id = ?
        """, (
            details.get("failureStage"), details.get("ruleId"), details.get("title"),
            details.get("category"), details.get("exceptionType"), details.get("exceptionLabel"),
            details.get("errorCode"), details.get("why"), details.get("detail"),
            details.get("errorMessageId"), details.get("signature"), now, int(job_id),
        ))


# ---------------------------------------------------------------------------
# Dashboard aggregates
# ---------------------------------------------------------------------------

# created_at is always '2026-09-22T10:46:00.000Z', so window filters and time
# bucketing are plain string operations — no date parsing, no timezone drift.
def _bucket_expr(minutes: int) -> str:
    if minutes >= 1440:
        return "substr(created_at, 1, 10)"                     # day
    if minutes >= 60:
        return "substr(created_at, 1, 13) || ':00'"            # hour
    return (f"substr(created_at, 1, 14) || printf('%02d', "
            f"(CAST(substr(created_at, 15, 2) AS INTEGER) / {minutes}) * {minutes})")


def _failure_scope(since: str, until: Optional[str], filters: Optional[dict],
                   source: str) -> tuple[str, list]:
    """Shared WHERE for the failure-oriented dashboard panels."""
    where = ["source = ?", "created_at >= ?", "status = 'Failed'"]
    params: list[Any] = [source, since]
    if until:
        where.append("created_at < ?")
        params.append(until)
    for col in ("error_rule", "error_category", "failure_stage", "flow_id", "error_signature"):
        if filters and filters.get(col):
            where.append(f"{col} = ?")
            params.append(filters[col])
    return " AND ".join(where), params


def dashboard_summary(since: str, source: str = "aacp", until: Optional[str] = None,
                      filters: Optional[dict] = None) -> dict:
    """Headline counters for a window: volume, outcome split, reach."""
    if filters:
        # A dimension filter only makes sense against failures, so the tiles
        # describe that subset rather than mixing in unrelated successes.
        where, params = _failure_scope(since, until, filters, source)
        with get_db() as conn:
            row = conn.execute(f"""
                SELECT COUNT(*) AS total, 0 AS complete, COUNT(*) AS failed,
                       0 AS canceled, 0 AS running,
                       COUNT(DISTINCT flow_id) AS flows,
                       COUNT(DISTINCT creator_email) AS users,
                       SUM(execution_time_min) AS failed_minutes
                FROM jobs WHERE {where}
            """, params).fetchone()
    else:
        clause = "source = ? AND created_at >= ?" + (" AND created_at < ?" if until else "")
        params = [source, since] + ([until] if until else [])
        with get_db() as conn:
            row = conn.execute(f"""
                SELECT COUNT(*) AS total,
                       SUM(status = 'Complete') AS complete,
                       SUM(status = 'Failed') AS failed,
                       SUM(status = 'Canceled') AS canceled,
                       SUM(status IN ('InProgress', 'Created')) AS running,
                       COUNT(DISTINCT flow_id) AS flows,
                       COUNT(DISTINCT creator_email) AS users,
                       SUM(CASE WHEN status = 'Failed' THEN execution_time_min ELSE 0 END) AS failed_minutes
                FROM jobs WHERE {clause}
            """, params).fetchone()
    d = {k: (row[k] or 0) for k in row.keys()}
    finished = d["complete"] + d["failed"] + d["canceled"]
    d["failureRate"] = round(100.0 * d["failed"] / finished, 1) if finished else 0.0
    d["wastedHours"] = round((d["failed_minutes"] or 0) / 60.0, 1)
    return d


def dashboard_timeseries(since: str, bucket_minutes: int, source: str = "aacp",
                         until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    """Per-bucket run counts split by outcome, for the trend chart."""
    expr = _bucket_expr(bucket_minutes)
    if filters:
        where, params = _failure_scope(since, until, filters, source)
    else:
        where = "source = ? AND created_at >= ?" + (" AND created_at < ?" if until else "")
        params = [source, since] + ([until] if until else [])
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT {expr} AS bucket,
                   COUNT(*) AS total,
                   SUM(status = 'Complete') AS complete,
                   SUM(status = 'Failed') AS failed,
                   SUM(status = 'Canceled') AS canceled
            FROM jobs WHERE {where}
            GROUP BY bucket ORDER BY bucket
        """, params).fetchall()
    return [{k: (r[k] or 0) for k in r.keys()} for r in rows]


def _breakdown(column: str, since: str, source: str, limit: int,
               until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    where, params = _failure_scope(since, until, filters, source)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT COALESCE({column}, '(unknown)') AS name, COUNT(*) AS count
            FROM jobs WHERE {where}
            GROUP BY name ORDER BY count DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [{"name": r["name"], "count": r["count"]} for r in rows]


def failure_stage_breakdown(since: str, source: str = "aacp", limit: int = 20,
                            until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    """Which pipeline stage actually failed (cascades already collapsed at enrichment)."""
    return _breakdown("failure_stage", since, source, limit, until, filters)


def error_category_breakdown(since: str, source: str = "aacp", limit: int = 20,
                             until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    return _breakdown("error_category", since, source, limit, until, filters)


def error_rule_breakdown(since: str, source: str = "aacp", limit: int = 20,
                         until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    """Specific causes, with the human-readable title carried along."""
    where, params = _failure_scope(since, until, filters, source)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT COALESCE(error_rule, 'pending') AS rule,
                   COALESCE(error_title, 'Not yet enriched') AS title,
                   COALESCE(error_category, 'Pending') AS category,
                   COUNT(*) AS count
            FROM jobs WHERE {where}
            GROUP BY rule, title, category ORDER BY count DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [dict(r) for r in rows]


def top_failing_flows(since: str, source: str = "aacp", limit: int = 10,
                      until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    """Repeat offenders — flows that fail over and over in the window."""
    where, params = _failure_scope(since, until, filters, source)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT flow_id, COALESCE(flow_name, '(unknown)') AS flow_name,
                   COUNT(*) AS failures,
                   COUNT(DISTINCT COALESCE(error_rule, 'pending')) AS distinct_causes,
                   MAX(COALESCE(error_title, '')) AS sample_cause,
                   MAX(created_at) AS last_failure
            FROM jobs WHERE {where}
            GROUP BY flow_id, flow_name ORDER BY failures DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [dict(r) for r in rows]


def top_failing_users(since: str, source: str = "aacp", limit: int = 10,
                      until: Optional[str] = None, filters: Optional[dict] = None) -> list[dict]:
    where, params = _failure_scope(since, until, filters, source)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT COALESCE(creator_email, '(unknown)') AS creator_email,
                   COUNT(*) AS failures,
                   COUNT(DISTINCT flow_id) AS flows
            FROM jobs WHERE {where}
            GROUP BY creator_email ORDER BY failures DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [dict(r) for r in rows]


def ran_from_breakdown(since: str, source: str = "aacp", until: Optional[str] = None) -> list[dict]:
    """How jobs were triggered (api / schedule / ui), with their failure counts."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT COALESCE(ran_from, '(unknown)') AS name,
                   COUNT(*) AS total,
                   SUM(status = 'Failed') AS failed
            FROM jobs WHERE source = ? AND created_at >= ?
              AND (? IS NULL OR created_at < ?)
            GROUP BY name ORDER BY total DESC
        """, (source, since, until, until)).fetchall()
    return [{k: (r[k] or 0) if k != "name" else r[k] for k in r.keys()} for r in rows]


# ---------------------------------------------------------------------------
# Error message drill-down
# ---------------------------------------------------------------------------

def error_messages(since: str, until: Optional[str] = None, filters: Optional[dict] = None,
                   source: str = "aacp", limit: int = 50) -> list[dict]:
    """
    Distinct error messages behind a set of failures, most frequent first.

    Grouped on error_signature (ids and paths masked) so that hundreds of
    instances of one problem collapse into a single row, while a real sample of
    the raw text is carried along for the person debugging it.
    """
    where = ["source = ?", "status = 'Failed'", "created_at >= ?", "error_signature IS NOT NULL"]
    params: list[Any] = [source, since]
    if until:
        where.append("created_at < ?")
        params.append(until)
    for col in ("error_rule", "error_category", "failure_stage", "flow_id", "error_signature"):
        if filters and filters.get(col):
            where.append(f"{col} = ?")
            params.append(filters[col])

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT error_signature AS signature,
                   COUNT(*) AS count,
                   COUNT(DISTINCT flow_id) AS flows,
                   COUNT(DISTINCT creator_email) AS users,
                   MIN(created_at) AS first_seen,
                   MAX(created_at) AS last_seen,
                   MAX(error_title) AS title,
                   MAX(error_category) AS category,
                   MAX(error_rule) AS rule,
                   MAX(error_type) AS exception_type,
                   MAX(error_label) AS exception_label,
                   MAX(error_code) AS error_code,
                   MAX(failure_stage) AS failure_stage,
                   MAX(error_why) AS sample_why,
                   MAX(error_detail) AS sample_detail,
                   MAX(job_id) AS sample_job_id
            FROM jobs WHERE {' AND '.join(where)}
            GROUP BY error_signature
            ORDER BY count DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [dict(r) for r in rows]


def signature_flows(since: str, signature: str, until: Optional[str] = None,
                    source: str = "aacp", limit: int = 10) -> list[dict]:
    """Which flows are hitting one specific error message."""
    where = ["source = ?", "status = 'Failed'", "created_at >= ?", "error_signature = ?"]
    params: list[Any] = [source, since, signature]
    if until:
        where.append("created_at < ?")
        params.append(until)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT flow_id, COALESCE(flow_name, '(unknown)') AS flow_name,
                   COUNT(*) AS failures, MAX(created_at) AS last_failure,
                   MAX(COALESCE(creator_email, '')) AS creator_email
            FROM jobs WHERE {' AND '.join(where)}
            GROUP BY flow_id, flow_name ORDER BY failures DESC LIMIT ?
        """, params + [limit]).fetchall()
    return [dict(r) for r in rows]


def rows_needing_signature(limit: int = 5000, source: str = "aacp") -> list[dict]:
    """Already-enriched failures written before signatures existed."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT job_id, error_why, error_detail, error_type
            FROM jobs
            WHERE source = ? AND status = 'Failed' AND enriched_at IS NOT NULL
              AND error_signature IS NULL
            LIMIT ?
        """, (source, limit)).fetchall()
    return [dict(r) for r in rows]


def set_signature(job_id: int, signature: str):
    with get_db() as conn:
        conn.execute("UPDATE jobs SET error_signature = ? WHERE job_id = ?", (signature, int(job_id)))


def get_job(job_id: int, source: str = "aacp") -> Optional[dict]:
    """One stored job group row, or None."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ? AND source = ?",
                           (int(job_id), source)).fetchone()
    return dict(row) if row else None
