# Handoff

Overwrite **Current State** at the end of each session. Append to **Session Log**;
never edit past entries.

## Current State

**As of:** 2026-09-29
**Branch:** `tri-check` (not yet pushed to a remote)

**What's built and working**

- Background sync of AAC job groups: 14-day backfill, incremental top-up,
  failure enrichment, retention purge
- Failure classification: 25 rules, ~1.4% unclassified across ~11k failures
- Dashboard: 1h/6h/24h/7d/14d windows, KPIs, outcome trend, root stage, cause
  categories, top causes, repeat offenders, users, trigger source — all
  cross-filtering, with an error-message drill-down
- All Jobs: list with filters, per-job detail panel (stages with the root
  failure marked, classified cause, source/target), Databricks log bundle
- Schema drift tracked separately from failures
- `tri-track.sh` for start/stop/restart/status/logs

**What's in progress**

Nothing mid-flight. The last full re-enrichment completed: 11,340 failures
re-read, 5 permanently unreadable.

**Immediate next action**

Push `tri-check` to a new GitHub branch and share with the customer. Nothing is
blocking that.

---

## Open Questions

- [ ] **Databricks PAT scope.** The token only reads runs belonging to certain
      AAC users (~13% of runs on 2026-09-29; it was ~100% on 2026-09-22). Log
      download is unavailable for the rest. Needs the customer's Databricks
      admin — not a code fix.
- [ ] **Per-user failure counts are shown by email.** Fine as an ops tool,
      potentially awkward in a room with those 30 people. Consider a toggle.
- [ ] **Remaining unclassified failures** (~1.4%): 51 with no error detail at
      all, 42 `RuntimeException`, 36 `TypeError: scriptLines is not iterable`,
      21 storage-credential `TrifactaException`, 20 `RequestException`. Wording
      for these should be agreed with the customer before adding rules.
- [ ] **No drift indicator in the All Jobs table** — visible only after
      expanding a row or by filtering. A small marker in the status column would
      fix it.
- [ ] **Longer history.** Retention is 14 days and the dashboard cannot look
      further back. If the customer wants monthly trends, add daily rollup
      tables before raising `RETENTION_DAYS` — raising it alone grows the store
      linearly (~0.7 GB/year unpurged at this volume).

---

## Environment notes

- Tenant during development: IQVIA, `eu1.alteryxcloud.com`, ~3,000 job groups/day
- Failure rate observed: 18–27% depending on window
- **Live issue on the tenant:** an expired OAuth connection credential has been
  failing since 2026-09-17 and now dominates the dashboard (thousands of
  failures). It is a real customer problem, not a tool artifact — say so before
  anyone sees the dashboard cold.

---

## Session Log

### Session 1 — 2026-09-22 — Revival and Databricks linkage
- Resolved a committed merge conflict in `.gitignore`; cut branch `tri-check`
- Fixed the AAC→Databricks link: select the `wrangle` stage by type (was
  `data[0]`, null on 95/96 jobs); established that `databricksJobId` is a run id
- Removed `DATABRICKS_HOST` from config in favour of the per-job workspace URL
- Fixed the jobs upsert dropping late-arriving Databricks links

### Session 2 — 2026-09-22 — Repurpose to failure triage
- Explored the API surface, including the undocumented `dateFilter` and
  model-property filters found in the trifacta-ayx-cloud source
- Built the sync, the classifier and the Dashboard tab; retired Flow Analysis
- 14-day backfill: 36k job groups in ~5 min, 6.7k failures classified in ~10 min

### Session 3 — 2026-09-23 — Triage tooling
- Cross-filtering across every dashboard panel; hand-off to All Jobs
- Error-message drill-down grouped by masked signature

### Session 4 — 2026-09-29 — Detail panel, logs, retention, drift
- Job detail panel: stages, root failure, classified cause, source/target
- Databricks log bundle (driver + eventlog + executor + run/cluster JSON)
- Discovered Databricks PAT visibility had dropped to ~13% of runs
- Retention purging (56 MB → 31 MB), local reclassification, two schema rules
- Fixed: detail panel broken for failed jobs (`error` key collision); settings
  gear dead (init aborted by a missing element); empty dashboard on first paint
- Schema drift made advisory: 421 failures re-attributed to their real cause
- Fixed the enrichment loop stopping early; re-read all 11,340 failures
- Added `tri-track.sh`; app now defaults to loopback with debug off

### Session 5 — 2026-09-29 — Agent workspace
- Added `.agents/` (this workspace), committed rather than gitignored so it
  reaches the customer and other branches
