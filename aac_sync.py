"""
Background sync of AAC job groups into the local store.

Three passes, all read-only against AAC:

  backfill   — walk the retention window once (dateFilter on createdAt)
  incremental— pull anything created OR updated since the last run, which is
               what moves a job off InProgress without re-reading the world
  enrich     — for each stored failure, fetch its error message, find the root
               failed stage and classify it

Failures are terminal on the AAC side, so an enriched failure is never re-read.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import classifier
import db
from config import Config
from platform_api import PlatformAPI, PlatformAPIError

log = logging.getLogger(__name__)

PAGE_SIZE = 1000          # verified server-side maximum that still responds quickly
RETENTION_DAYS = 14
OVERLAP_MINUTES = 10      # re-read a little either side of the last sync
ENRICH_BATCH = 300
ENRICH_WORKERS = 8        # one error-message fetch per failure; the API tolerates this comfortably

_lock = threading.Lock()
_state = {"running": False, "phase": "idle", "message": "", "scanned": 0,
          "stored": 0, "enriched": 0, "startedAt": None, "finishedAt": None, "error": None}


def _iso(dt: datetime) -> str:
    """AAC's own timestamp format, so string comparisons against stored rows line up."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def get_status() -> dict:
    with _lock:
        s = dict(_state)
    s["lastSyncAt"] = db.get_sync_state("last_sync_at")
    s["backfillDone"] = db.get_sync_state("backfill_done") == "1"
    s["retentionDays"] = RETENTION_DAYS
    return s


def _set(**kw):
    with _lock:
        _state.update(kw)


# ---------------------------------------------------------------------------
# Fetch passes
# ---------------------------------------------------------------------------

def _fetch_window(api: PlatformAPI, field: str, since: datetime, until: datetime | None = None) -> int:
    """
    Page through jobLibrary filtered on a date field and upsert everything found.

    `field` is createdAt for the backfill and updatedAt for incremental runs —
    a job that finishes gets a new updatedAt, so that pass catches transitions
    as well as new work.
    """
    from app import extract_job_summary  # local import: avoids a circular import at module load

    offset, stored = 0, 0
    params_base = (f"dateFilter[{field}][gte]={_epoch_ms(since)}"
                   + (f"&dateFilter[{field}][lt]={_epoch_ms(until)}" if until else ""))
    while True:
        url = (f"{api.base_url}/jobLibrary?limit={PAGE_SIZE}&offset={offset}"
               f"&sort=-{field}&{params_base}")
        try:
            resp = api.session.get(url, timeout=api.timeout)
            resp.raise_for_status()
            rows = resp.json().get("data", [])
        except Exception as e:
            raise PlatformAPIError(f"jobLibrary page at offset {offset} failed: {e}") from e

        if not rows:
            break
        summaries = [extract_job_summary(r) for r in rows]
        stored += db.upsert_jobs(summaries)
        offset += PAGE_SIZE
        _set(scanned=_state["scanned"] + len(rows), stored=_state["stored"] + len(summaries),
             message=f"{_state['scanned'] + len(rows)} job groups scanned")
        if len(rows) < PAGE_SIZE:
            break
    return stored


_thread_local = threading.local()


def _worker_api() -> PlatformAPI:
    """A PlatformAPI (and therefore requests.Session) per worker thread."""
    api = getattr(_thread_local, "api", None)
    if api is None:
        api = PlatformAPI()
        _thread_local.api = api
    return api


def _fetch_failure_detail(job_id: int) -> tuple[int, dict]:
    api = _worker_api()
    try:
        resp = api.session.get(
            f"{api.base_url}/jobGroups/{job_id}?embed=jobs.errorMessage", timeout=60)
        return job_id, (resp.json() if resp.status_code == 200 else {})
    except Exception as e:
        log.warning(f"enrich {job_id} failed: {e}")
        return job_id, {}


def _enrich_failures(api: PlatformAPI, limit: int = ENRICH_BATCH) -> int:
    """
    Attach the root failure stage and a classified cause to stored failures.

    Fetching is the slow part and is independent per job, so it runs on a small
    pool; the SQLite writes stay on this thread to avoid lock contention.
    """
    job_ids = db.failures_needing_enrichment(limit=limit)
    if not job_ids:
        return 0
    done = 0
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        for job_id, detail in pool.map(_fetch_failure_detail, job_ids):
            if not detail:
                continue
            inner = (detail.get("jobs") or {}).get("data") or []
            root = classifier.pick_root_failure(inner)
            if root is None:
                # Nothing failed inside the group (or AAC returned nothing usable).
                # Mark it enriched anyway so it does not get retried forever.
                db.save_failure_details(job_id, {"failureStage": None, "ruleId": "no_failed_stage",
                                                 "title": "No failed stage reported",
                                                 "category": "Unclassified"})
                done += 1
                continue

            parsed = classifier.parse_error_message(root.get("errorMessage"))
            result = classifier.classify(parsed)
            db.save_failure_details(job_id, {
                "failureStage": root.get("jobType"),
                "ruleId": result["ruleId"], "title": result["title"], "category": result["category"],
                "exceptionType": parsed.get("exceptionType"), "exceptionLabel": parsed.get("exceptionLabel"),
                "errorCode": parsed.get("errorCode"), "why": (parsed.get("why") or "")[:2000],
                "detail": (parsed.get("message") or "")[:4000],
                "errorMessageId": parsed.get("errorMessageId"),
                "signature": classifier.signature(parsed),
            })
            done += 1
            _set(enriched=_state["enriched"] + 1,
                 message=f"{_state['enriched'] + 1} failures classified")
    return done


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_sync(full: bool = False):
    """Run one sync cycle. Safe to call repeatedly; only one runs at a time."""
    with _lock:
        if _state["running"]:
            return {"started": False, "reason": "a sync is already running"}
        _state.update({"running": True, "phase": "starting", "message": "", "scanned": 0,
                       "stored": 0, "enriched": 0, "error": None,
                       "startedAt": datetime.now(timezone.utc).isoformat(), "finishedAt": None})

    def worker():
        started = time.time()
        try:
            if not Config.PLATFORM_API_TOKEN:
                raise PlatformAPIError("PLATFORM_API_TOKEN is not configured")
            api = PlatformAPI()
            now = datetime.now(timezone.utc)
            backfilled = db.get_sync_state("backfill_done") == "1"

            if full or not backfilled:
                _set(phase="backfill", message=f"walking the last {RETENTION_DAYS} days")
                _fetch_window(api, "createdAt", now - timedelta(days=RETENTION_DAYS))
                db.set_sync_state("backfill_done", "1")
            else:
                last = db.get_sync_state("last_sync_at")
                since = (datetime.fromisoformat(last) if last else now - timedelta(days=1))
                since -= timedelta(minutes=OVERLAP_MINUTES)
                _set(phase="incremental", message="fetching new and updated jobs")
                _fetch_window(api, "updatedAt", since)

            _set(phase="signatures", message="grouping error messages")
            backfill_signatures()

            _set(phase="enrich", message="classifying failures")
            while _enrich_failures(api) >= ENRICH_BATCH:
                pass  # keep going while full batches come back

            db.set_sync_state("last_sync_at", datetime.now(timezone.utc).isoformat())
            _set(phase="idle", message=f"done in {time.time() - started:.0f}s")
        except Exception as e:
            log.exception("sync failed")
            _set(phase="error", error=str(e), message=str(e))
        finally:
            _set(running=False, finishedAt=datetime.now(timezone.utc).isoformat())

    threading.Thread(target=worker, name="aac-sync", daemon=True).start()
    return {"started": True}


def window_since(window: str) -> str:
    """Translate a dashboard window key into an AAC-format cutoff timestamp."""
    minutes = {"1h": 60, "6h": 360, "24h": 1440, "7d": 10080, "14d": 20160}.get(window, 1440)
    return _iso(datetime.now(timezone.utc) - timedelta(minutes=minutes))


def bucket_minutes(window: str) -> int:
    """Chart bucket size that keeps each window readable."""
    return {"1h": 5, "6h": 30, "24h": 60, "7d": 1440, "14d": 1440}.get(window, 60)


def backfill_signatures(batch: int = 5000) -> int:
    """
    Compute message signatures for failures enriched before signatures existed.

    Purely local — it re-derives the signature from the error text already
    stored, so no AAC calls and no re-enrichment.
    """
    total = 0
    while True:
        rows = db.rows_needing_signature(limit=batch)
        if not rows:
            break
        for row in rows:
            sig = classifier.signature({
                "why": row.get("error_why"),
                "message": row.get("error_detail"),
                "exceptionType": row.get("error_type"),
            })
            db.set_signature(row["job_id"], sig)
            total += 1
        _set(message=f"{total} error messages grouped")
        if len(rows) < batch:
            break
    if total:
        log.info(f"Backfilled {total} error signatures")
    return total
