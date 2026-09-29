# Tri-Tracker — agent context

Failure-triage tool for Alteryx Analytics Cloud (AAC) job runs, with Databricks
log retrieval. Python/Flask + SQLite backend, plain-JS frontend, no build step.

**Before changing anything, read [.agents/architecture.md](.agents/architecture.md).**
It covers what the app does, where the code lives, and the platform behaviours
that are not guessable from the source — the API field names are misleading in
ways that have caused real bugs.

## Start here

| Read | For |
|---|---|
| [.agents/architecture.md](.agents/architecture.md) | The codebase map and the domain facts |
| [.agents/conventions.md](.agents/conventions.md) | How to change things safely |
| [.agents/decisions.md](.agents/decisions.md) | Why the design is as it is |
| [.agents/handoff.md](.agents/handoff.md) | Current state, open questions, history |

## Session protocol

**Start:** check the branch (`git branch --show-current`), read
`.agents/architecture.md` and `.agents/handoff.md`, then say what you plan to do.

**End:** update `.agents/handoff.md`, append any non-obvious choice to
`.agents/decisions.md` with the reasoning, and commit with a conventional prefix
(`feat:`, `fix:`, `refactor:`, `docs:`). Never push without being asked.

For a discrete piece of work, copy `.agents/features/TEMPLATE/` to
`.agents/features/<feature-name>/`.

## Running it

```bash
./tri-track.sh start     # background; installs deps on first run
./tri-track.sh status    # running? last sync?
./tri-track.sh logs
./tri-track.sh stop
```

Then http://localhost:5050. Needs an AAC API token and a Databricks PAT in
`.env` (copy `.env.example`), or set them from the gear icon in the app.

## The one habit that matters

This app talks to two APIs whose field names lie — `cpJobId.databricksJobId` is
a *run* id, `jobs.data[0]` is not "the job", AAC's `errorCategory` is 99.8% one
value. **Verify against the live tenant before building on an assumption**, and
put the measurement in the commit message.
