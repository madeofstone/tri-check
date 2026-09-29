# Decision log

Append-only. Add a row for any non-obvious choice; do not rewrite history. The
rationale matters more than the decision — most of these look arbitrary without
the measurement behind them.

| Date | Decision | Rationale | Alternatives rejected |
|------|----------|-----------|----------------------|
| 2026-09-22 | Select the `wrangle` stage by `jobType`, never by array position | `jobs.data[0]` is whichever stage ran first (usually ingest), so the Databricks link was null on 95 of 96 jobs | Index-based access; "first job with a cpJobId" kept only as a fallback |
| 2026-09-22 | Derive the Databricks workspace URL per job from `cpJobId.databricksServiceUrl`; remove `DATABRICKS_HOST` from config | The job states the workspace that ran it, so config cannot drift out of sync with the PAT | Keeping a configured host; asking the user per workspace |
| 2026-09-22 | Treat `cpJobId.databricksJobId` as a **run** id and call `jobs/runs/get` | Verified live: `jobs/get` on `databricksJobDefinitionId` returns "does not exist" (ephemeral jobs) while the run reads fine | Trusting the field name |
| 2026-09-22 | Retire the Flow Analysis tab and the on-prem Trifacta comparison | Branch repurposed from Spark tuning to AAC failure triage; kept on the original branch | Keeping both purposes in one UI |
| 2026-09-22 | Store every job group, not just failures | "Jobs run" and per-flow volume need the successes; 14 days is only ~42k rows / 30 MB | Counting via `/jobLibrary/count` alone (no per-flow breakdown) |
| 2026-09-22 | Ignore AAC's `errorCategory`; classify with our own ordered rules in `error_rules.json` | It maps 83 exception types to "user" and defaults the rest to "infrastructure" — 99.8% of this tenant lands in one bucket | Using the platform taxonomy; clustering/NLP over messages |
| 2026-09-22 | Collapse `UpstreamJobFailed` cascades before attributing a cause | 27% of failed inner jobs are cascades; counting them skews the stage breakdown badly | Counting every failed stage |
| 2026-09-22 | Incremental sync filters on `updatedAt`, not `createdAt` | A job that finishes gets a new `updatedAt`, so one pass catches new jobs *and* status transitions | Re-walking the window; polling InProgress rows individually |
| 2026-09-22 | Failures are terminal — never re-enrich an enriched failure | Confirmed with the user: AAC failures do not change after the fact | Re-checking failures on every sync |
| 2026-09-23 | Group failures by a masked message **signature** for drill-down | Masks ids, paths, tokens and quoted object names, collapsing 6,731 failures into ~20 rows; the same permission fault across 40 tables is one problem, not 40 | Grouping by exception type (too coarse); showing raw messages (too many) |
| 2026-09-23 | Reserve the `error` key in JSON responses for API failures | The job detail payload used `error` for the classified failure, and the UI's `if (data.error) throw` broke the panel for every failed job | Special-casing the check in one caller |
| 2026-09-29 | Fetch job stages on demand rather than storing them | Only needed when a row is expanded, and it is one call | Storing the stage list per job |
| 2026-09-29 | Check Databricks log availability before offering the download | A PAT only reads runs of certain AAC users (~13% here), so the button would otherwise fail after a wait | Always showing the button; hiding the feature entirely |
| 2026-09-29 | Write log bundles outside the project tree | The dev reloader restarts on any file change under the project, which truncated a download to 0 bytes mid-request | Excluding paths from the reloader; disabling caching |
| 2026-09-29 | Purge at retention + 1 day grace, VACUUM only after a large delete | Purging exactly at the boundary eats the oldest bucket of the 14-day chart; VACUUM rewrites the whole file | Purging at the exact boundary; VACUUM every sync; no purge |
| 2026-09-29 | Default to loopback with debug off | Ships to a customer with live tokens; the Werkzeug debugger allows code execution from the browser | `0.0.0.0` + `debug=True` (the previous default) |
| 2026-09-29 | Treat `schemadrift` as advisory, never as a cause | Drift reports a source change but the transformation still runs — confirmed in data: of 11,340 failures, 465 had drift and **0** had drift as the only failure | Counting drift as a failure stage (it was hiding the real cause on 421 jobs) |
| 2026-09-29 | Drive backlog loops off progress, not batch size | `while _enrich_failures(api) >= ENRICH_BATCH` stopped on a single failed fetch, reporting success having done 5,717 of 11,340 | Comparing against batch size; retrying the whole pass |
| 2026-09-29 | Commit `.agents/` rather than gitignoring it | These docs are for the customer and for agents on other branches; the usual local-only agent workspace would not reach them | The init-feature-agent default (gitignored, local only) |
