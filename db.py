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
    source          TEXT DEFAULT 'aacp',
    fetched_at      TEXT
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

CREATE TABLE IF NOT EXISTS dbx_cache (
    flow_name       TEXT,
    job_run_id      TEXT,
    data            TEXT,
    cached_at       TEXT,
    PRIMARY KEY (flow_name, job_run_id)
);
"""


def init_db():
    """Initialize the database schema."""
    with get_db() as conn:
        conn.executescript(SCHEMA_SQL)
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
                                  execution_language, databricks_job_id, source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    execution_time_min = excluded.execution_time_min,
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
    Each group has: flow_id, flow_name, job_count, min_exec, max_exec, latest_created_at, jobs[]
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

    where = " AND ".join(where_clauses)

    with get_db() as conn:
        groups = conn.execute(f"""
            SELECT flow_id, flow_name,
                   COUNT(*) as job_count,
                   MIN(execution_time_min) as min_exec,
                   MAX(execution_time_min) as max_exec,
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

            result.append({
                "flow_id": g["flow_id"],
                "flow_name": g["flow_name"],
                "job_count": g["job_count"],
                "min_exec": g["min_exec"],
                "max_exec": g["max_exec"],
                "latest_created_at": g["latest_created_at"],
                "jobs": [dict(j) for j in jobs],
            })

    return result


def get_kpi_stats(source: str = "aacp") -> dict:
    """Compute KPI stats: total jobs, success rate, jobs per day (last 7 days)."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE source = ?", (source,)
        ).fetchone()["cnt"]

        completed = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE source = ? AND LOWER(status) IN ('complete', 'completed')",
            (source,),
        ).fetchone()["cnt"]

        # Jobs per day over last 7 days
        recent = conn.execute("""
            SELECT COUNT(*) as cnt FROM jobs
            WHERE source = ? AND created_at >= datetime('now', '-7 days')
        """, (source,)).fetchone()["cnt"]

    return {
        "total_jobs": total,
        "completed_jobs": completed,
        "success_rate": round(completed / total * 100, 1) if total > 0 else 0,
        "jobs_per_day_7d": round(recent / 7, 1),
    }


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
    return {
        "name": row["name"],
        "pairs": json.loads(row["pairs"]) if row["pairs"] else [],
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
