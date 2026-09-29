# Architecture — Tri-Tracker

Read this before changing anything. It covers what the app does, where the code
lives, and the platform behaviours that are not guessable from the source.

## What this app is

A local web app for triaging **Alteryx Analytics Cloud (AAC)** job failures at
high volume. The target tenant runs ~3,000 job groups a day at a ~20% failure
rate, so the job is not "show me the jobs" but "what broke today, why, and who
keeps hitting it".

It reads AAC's Platform API and (where permitted) Databricks. It is **read-only**
against both: it never triggers, cancels or modifies a job.

Two tabs:

- **Dashboard** (landing tab) — volume, failure rate, root stage, cause
  categories, repeat-offender flows, error-message drill-down. Everything
  cross-filters.
- **All Jobs** — the job list, with a per-job detail panel (stages, failure,
  source/target) and a Databricks log bundle download.

## Data flow

```
AAC Platform API  ──►  aac_sync  ──►  SQLite (tri-tracker.db)  ──►  Flask API  ──►  browser
  /v4/jobLibrary        backfill        jobs table                  /api/dashboard
  /v4/jobGroups/{id}    incremental                                 /api/all-jobs
                        enrich (classify)                           /api/job/{id}/detail
                        purge (retention)
Databricks  ──►  dbx_logs  ──►  zip bundle (temp dir, 7-day TTL)
  jobs/runs/get           driver + eventlog + executor logs
  DBFS /trifacta/logs
```

Nothing is computed in the browser: the API returns finished aggregates.

## Backend modules

| File | Responsibility |
|---|---|
| `app.py` | Flask app: every HTTP route, job-detail assembly, config read/write. The only module the frontend talks to. |
| `aac_sync.py` | Background sync: backfill, incremental, enrichment, reclassify, purge. Owns the sync state machine and its threading. |
| `db.py` | SQLite schema, migrations, and every query. All SQL lives here — no SQL in `app.py`. |
| `classifier.py` | Turns a raw AAC error into a cause: root-failure selection, rule matching, message signatures. |
| `error_rules.json` | The classification rules (data, not code). Edit this to add a cause; no code change needed. |
| `platform_api.py` | AAC Platform API client (auth, pagination, timeouts). |
| `databricks_client.py` | Databricks SDK wrapper for run details and cluster events. |
| `dbx_logs.py` | Builds the Databricks log zip from DBFS + API extras. |
| `config.py` | Env-backed settings. Only tokens and a few tunables; see "Config" below. |
| `dbfs_eventlog.py`, `flow_store.py`, `fetch_driver_logs.py`, `eventlog-analyzer/` | Spark event-log download and analysis. Carried over from the tool's earlier tuning purpose; still used by the event-log path on the DBX panel. Not part of the failure-triage flow. |

## Frontend

No framework, no build step. Plain files served by Flask; Chart.js from CDN.

| File | Serves |
|---|---|
| `index.html` | Both tab panels, the settings modal, script/style includes |
| `overview.js` / `overview.css` | Dashboard tab: KPIs, charts, cross-filtering, message drill-down |
| `all_jobs.js` | All Jobs tab: table, filters, job detail panel, log download |
| `app.js` | Shared helpers (`esc`, `statusBadge`, `formatDateTime`), the settings modal, the DBX panel, compare modal |
| `dashboard.js` / `dashboard.css` | Spark performance dashboard for a single event log (older feature) |
| `styles.css` | Base styling and CSS variables (`--bg-card`, `--red`, …) |

`app.js` loads after `all_jobs.js` but defines helpers both use — fine at runtime,
since calls happen after load. **Bind listeners defensively** (`bind(id, ...)` in
`app.js`): a missing element must not throw, because a top-level throw aborts the
rest of that file's initialisation silently.

## Data model

One table does the work: `jobs`, one row per **job group** (one run of a flow or
plan), keyed by `job_id`.

- **Identity** — `job_id`, `flow_id`, `flow_name`, `creator_email`, `ran_from`
  (api/schedule/ui), `ran_for` (recipe/plan)
- **Timing** — `created_at`, `updated_at`, `execution_time_min` (ISO-8601 with
  `Z`, stored exactly as AAC returns it so string comparison works for windows)
- **Databricks** — `databricks_job_id` (a *run* id, see below),
  `databricks_job_definition_id`, `databricks_service_url`
- **Failure analysis** (failures only, set by enrichment) — `failure_stage`,
  `error_rule`, `error_title`, `error_category`, `error_type`, `error_label`,
  `error_code`, `error_why`, `error_detail`, `error_signature`, `schema_drift`,
  `enriched_at`

Plus `sync_state` (key/value bookkeeping), `dbx_cache` and `flows` (older
features).

Counts everywhere are **job groups, not stages and not flows**. A group with six
ingest jobs is one row.

## Domain facts you cannot guess from the code

These were all established by probing the live tenant. Changing code that touches
them without knowing them will produce plausible, wrong results.

**A job group contains many jobs.** `jobs.data` in a jobLibrary entry holds every
stage — `ingest`, `schemadrift`, `wrangle`, `profile`, `filewriter`, `publish`.
Only the `wrangle` stage runs on Spark and carries `cpJobId`. Its index varies,
so **select it by `jobType`, never by position**.

**`cpJobId.databricksJobId` is a RUN id**, not a job id. The job id is
`databricksJobDefinitionId`, and those jobs are ephemeral — `jobs/get` returns
"does not exist" while `jobs/runs/get` on the run works. `databricksServiceUrl`
gives the workspace, which is why the workspace URL is not configurable.

**Most failed inner jobs are not the cause.** Roughly 27% are
`UpstreamJobFailed` cascades, and `schemadrift` failures are advisory — the
transformation runs regardless. `classifier.pick_root_failure()` skips both when
anything else failed. Do not reintroduce "first failed stage wins".

**AAC's own `errorCategory` is useless here.** It maps 83 exception types to
"user" and defaults everything else to "infrastructure", which buckets 99.8% of
this tenant together. We classify ourselves; see `error_rules.json`.

**Error text needs a second call.** The list endpoint returns only
`errorMessage: {id}`. Full text comes from
`/v4/jobGroups/{id}?embed=jobs.errorMessage`. `/v4/errorMessages/{id}` is
internal-only and 404s.

**Undocumented jobLibrary query params** (found in
`webapp/src/server/lib/crud/list.js` in the trifacta-ayx-cloud monorepo):
`dateFilter[createdAt][gte]=<epoch-ms>` (also `lt`, and `updatedAt`), any model
property as a filter (`status`, `ranfrom`, `ranfor`), and `limit` up to 1000
(default 25). `/v4/jobLibrary/count` returns a filtered count in under a second.
Without these, a 14-day backfill is a long crawl; with them it is minutes.

**Databricks visibility is partial.** A PAT only reads runs of jobs it has
rights to, which on this tenant is a subset tied to the AAC user that triggered
the run — about 13%. `jobs/runs/list` shows more than `jobs/get` will confirm.
`errorReport`, `jobStatistics` and `jobGraph` are admin-gated (403). Check
availability before offering a log download.

**Databricks logs live at** `/trifacta/logs/<cluster_id>/` (from the cluster's
`cluster_log_conf`), in `driver/`, `eventlog/` and `executor/`. ~3 MB per short
run.

## Sync lifecycle

`aac_sync.run_sync()` runs one cycle on a background thread:

1. **backfill** (first run, or `full=true`) — page `createdAt >= now - retention`
2. **incremental** (thereafter) — page `updatedAt >= last_sync - 10min`, which
   catches new jobs *and* ones leaving `InProgress` without re-reading the window
3. **signatures** — backfill any missing message signatures (local, no API)
4. **enrich** — for each failure with `enriched_at IS NULL`, fetch its error
   message, pick the root stage, classify, store. Runs on an 8-thread pool
5. **purge** — delete rows older than retention + 1 day grace; VACUUM after a
   large delete; expire cached log zips

Failures are terminal on the AAC side, so an enriched failure is never re-read.
When attribution logic changes, `POST /api/reenrich` clears `enriched_at` so the
next sync redoes them.

## API surface

Grouped by purpose; all under `/api`.

- **Dashboard** — `GET /dashboard?window=1h|6h|24h|7d|14d` plus optional
  `from`/`to` and dimension filters (`error_category`, `error_rule`,
  `failure_stage`, `flow_id`, `schema_drift`). Returns every panel in one call.
  `GET /failures/message?signature=…` drills into one error.
- **Jobs** — `GET /all-jobs`, `GET /failures`, `GET /job/{id}/detail`,
  `GET /job/{id}/dbx-logs` (zip), `GET /job/{id}/dbx-logs/available`
- **Operations** — `POST /sync`, `GET /sync/status`, `POST /reclassify`,
  `POST /reenrich`, `GET /retention`, `POST /retention/purge`,
  `GET /error-rules`
- **Config** — `GET|PUT /config` (secrets masked on read)

Reserved key: **`error` in a JSON response means the request failed.** Payload
data must never use that key — the job detail endpoint returns the classified
failure as `failure` for exactly this reason.

## Config

Only tokens and a few tunables live in `.env`; see `.env.example`.
`PLATFORM_API_BASE_URL`, `PLATFORM_API_TOKEN`, `DATABRICKS_TOKEN`,
`RETENTION_DAYS`, `DEFAULT_LIMIT`, `RANFOR_FILTER`. Editable from the gear icon,
which writes `.env` and reloads in place.

The Databricks **workspace URL is deliberately not configurable** — each job
reports the workspace that ran it, so the two can never drift apart.

`app.py` defaults to loopback with debug off. `HOST=0.0.0.0` and `FLASK_DEBUG=1`
are opt-in: the Werkzeug debugger allows code execution from the browser, and its
reloader restarts the server mid-request (which once truncated a log download to
0 bytes because the zip was written inside the project tree).

## Running it

```bash
./tri-track.sh start    # background, installs deps on first run
./tri-track.sh status   # running? last sync?
./tri-track.sh logs
./tri-track.sh stop
```

`PORT=5060 ./tri-track.sh start` for another port. Data lives in
`tri-tracker.db` (gitignored); delete it for a clean slate and re-run the sync.
