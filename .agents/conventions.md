# Working conventions

How to make changes here without breaking things that look fine.

## Verify against the live tenant, not against assumptions

This codebase exists because the platform's behaviour is not what the field names
suggest. Nearly every significant bug in its history came from reasoning about
the API instead of calling it. Before building on an assumption about AAC or
Databricks, make the call and look at the response.

Examples that cost real time:

- `cpJobId.databricksJobId` looks like a job id. It is a run id.
- `jobs.data[0]` looks like "the job". It is whichever stage happens to be first.
- AAC's `errorCategory` looks like a ready-made taxonomy. It is 99.8% one value.
- The first sample of an error bucket looked like an OAuth failure, so the
  headline cause was reported wrong until the full corpus was checked. Sample
  size of one is not a finding.

When you measure something, put the number in the commit message. "Fixed
attribution" is worth less than "421 failures re-attributed; schemadrift no
longer appears as a root stage".

## Test the wiring, not just the parts

Several bugs passed every server-side test and every unit-level check while the
feature was completely broken in the browser:

- the job detail panel threw for **failed** jobs only, because the payload used
  the reserved `error` key
- the settings gear did nothing, because an unrelated missing element threw
  during init and aborted the rest of the file
- the dashboard was blank on first load, because nothing called `ovInit()`

If a change touches the UI, open it in a browser and click the thing. API tests
passing is not evidence the feature works.

## Loops that process a backlog

Drive them off "did this pass make progress", not "was the batch full". The
enrichment loop used `>= ENRICH_BATCH` and stopped the moment one fetch in a
batch of 300 failed — reporting success having done half the work. Silent
half-completion is worse than an error.

## Errors the user will read

Every failure path the UI can hit should say what happened and what to do. The
Databricks permission error names the workspace and explains the PAT scoping,
because "403" would send someone hunting in the wrong place. Prefer a clear 400
over a 500.

## Classification changes

`error_rules.json` is data. Adding a cause means adding a rule, not writing code.
Rules are ordered, first match wins, and new ones go **above** `upstream_cascade`,
which stays last.

After editing rules, apply them to history with `POST /api/reclassify` — it
re-runs the rules over stored error text with no AAC calls. Only use
`POST /api/reenrich` when the *stage list* matters (root attribution, schema
drift), since that requires re-reading job groups from AAC.

Keep the `unclassified` bucket visible on the dashboard. It is the backlog, and
hiding it means the taxonomy quietly stops improving.

## Database changes

Add the column to `SCHEMA_SQL` **and** to `MIGRATIONS`. `CREATE TABLE IF NOT
EXISTS` will not add a column to an existing database, and customers will have
one. Indexes covering migrated columns belong in `POST_MIGRATION_SQL`, which runs
after the `ALTER TABLE`s.

All SQL lives in `db.py`. Keep `app.py` free of it.

## Scale

Assume ~3,000 job groups a day and a 14-day window: ~42k rows, ~30 MB. Window
queries are index-range scans and run in 7–23 ms, so cost tracks the *window*,
not the table. Anything that scans the whole table, or that runs per row in a
request, will not hold up.

Never bulk-store Spark event logs. They are ~3 MB per run; at this volume that is
hundreds of MB a day. Fetch them on demand.

## Committing

Conventional prefixes (`feat:`, `fix:`, `refactor:`, `docs:`). Explain *why* and
include the numbers you measured. Never push without being asked.
