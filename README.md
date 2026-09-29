# Tri-Tracker

A web-based tool for tracking and comparing job runs across Alteryx Analytics Cloud (AAC) and on-prem Trifacta environments — with integrated Spark performance analysis.

## Features

- **Job Run Comparison** — Side-by-side view of AAC and on-prem job runs, matched by execution time window
- **Databricks Integration** — Expand any AAC job to see cluster details, Spark config, autoscale settings, and cluster events
- **Event Log Analysis** — Download and analyze Spark event logs directly from DBFS with one click
- **Performance Dashboard** — Sliding panel with Chart.js-powered executor timeline, stage breakdown (compute/delay/GC), KPI cards, and automated red flag detection
- **Flow Persistence** — All collected data is saved to disk and restored on restart
- **Settings UI** — Configure API endpoints, tokens, and job filters from the browser

## Quick Start

### 1. Clone & Install

```bash
git clone <repo-url>
cd sparkTunning
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

| Variable                | Description                                                   |
| ----------------------- | ------------------------------------------------------------- |
| `PLATFORM_API_BASE_URL` | AAC platform API URL (e.g. `https://eu1.alteryxcloud.com/v4`) |
| `PLATFORM_API_TOKEN`    | AAC API bearer token                                          |
| `DATABRICKS_TOKEN`      | Databricks PAT for the workspace that runs your AAC jobs      |
| `ONPREM_API_BASE_URL`   | On-prem Trifacta API URL _(optional)_                         |
| `ONPREM_API_TOKEN`      | On-prem API token _(optional)_                                |
| `DEFAULT_LIMIT`         | Max jobs to fetch per request (default: `25`)                 |
| `MATCH_WINDOW_MINUTES`  | Time window for pairing jobs (default: `10`)                  |
| `RANFOR_FILTER`         | Job type filter (default: `recipe,plan`)                      |

The Databricks workspace URL is deliberately not configurable. Each AAC job
reports the workspace that ran it (`cpJobId.databricksServiceUrl`), and Tri-Tracker
calls that workspace, so the two can never drift apart.

### 3. Run

```bash
./tri-track.sh start      # runs in the background, gives the terminal back
```

Open **http://localhost:5050** in your browser.

| Command | Does |
| --------------------- | ------------------------------------------- |
| `./tri-track.sh start` | Start in the background (installs deps on first run) |
| `./tri-track.sh stop` | Stop it |
| `./tri-track.sh restart` | Stop, then start |
| `./tri-track.sh status` | Whether it is running, plus last sync |
| `./tri-track.sh logs` | Follow the log |

Use another port with `PORT=5060 ./tri-track.sh start`. The app listens on
localhost only; set `HOST=0.0.0.0` to expose it on the network deliberately.

Tokens can also be set from the gear icon in the app rather than by editing
`.env` by hand.

## Usage

1. Click **+** in the sidebar to add a flow name
2. Click **Collect Latest Runs** to fetch job data from AAC (and optionally on-prem)
3. Click **DBX ▸** on any Databricks-backed job to see cluster details
4. Click **Download Event Log** to analyze Spark performance
5. Click **➜ Dashboard** to view the interactive performance dashboard

## For developers and agents

Working documents live in [`.agents/`](.agents/) and are committed so they travel
with the branch:

- [`.agents/architecture.md`](.agents/architecture.md) — what the app does, where
  the code lives, and the AAC/Databricks behaviours that are not guessable from
  the source
- [`.agents/conventions.md`](.agents/conventions.md) — how to change things safely
- [`.agents/decisions.md`](.agents/decisions.md) — why the design is as it is
- [`.agents/handoff.md`](.agents/handoff.md) — current state and open questions

`AGENTS.md`, `CLAUDE.md` and `GEMINI.md` at the repo root point coding agents at
the same docs.

## Project Structure

```
tri-track/
├── app.py                  # Flask backend — API endpoints
├── app.js                  # Frontend logic — flows, results table, DBX panel
├── index.html              # Main HTML shell
├── styles.css              # Core stylesheet
├── dashboard.js            # Dashboard charts, KPI cards, red flags
├── dashboard.css           # Dashboard-specific styles
├── databricks_client.py    # Databricks SDK wrapper
├── dbfs_eventlog.py        # DBFS event log discovery & download
├── flow_store.py           # File-based persistence layer
└── eventlog-analyzer/
    ├── analyze_eventlog.py         # Spark event log parser
    ├── tuning_recommendations.json # Tuning knowledge base
    └── README.md
```

**Parent directory** (shared modules):

- `config.py` — Environment configuration loader
- `platform_api.py` — AAC Platform API client
- `requirements.txt` — Python dependencies
- `.env` — Environment variables _(not committed)_

## Performance Dashboard

The dashboard automatically detects and flags:

| Red Flag                      | Trigger                               |
| ----------------------------- | ------------------------------------- |
| **Infrastructure Bottleneck** | Scheduling delay exceeds compute time |
| **Over-partitioning**         | Per-partition data < 10 MB            |
| **Preemption Risk**           | Executor removed with "worker lost"   |
| **High GC Overhead**          | GC > 20% of task runtime per stage    |

## Data Storage

Runtime data is stored locally in `tri-track/flows/` (git-ignored):

```
flows/<flowName>/
  flow_data.json              # Job pairs, metadata, URLs
  dbx/<jobRunId>.json          # Cached Databricks details
  eventlogs/<jobRunId>/
    eventlog                   # Raw Spark event log
    analysis.json              # Parsed analysis metrics
```

## License

Internal tool — Alteryx.
