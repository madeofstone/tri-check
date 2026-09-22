/* ===========================================================================
   Dashboard tab — failure trends and categorisation across the AAC tenant.

   Colour roles (validated against the app's card surface #1c1f2e):
     status trio  — outcomes only (complete / canceled / failed)
     categorical  — error categories, assigned in fixed slot order, never cycled
   =========================================================================== */

const OV_STATUS = { complete: "#0ca30c", canceled: "#fab219", failed: "#d03b3b" };

// Fixed categorical slot order. A 9th category folds into "Other" rather than
// inventing a hue.
const OV_SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500",
                   "#d55181", "#008300", "#9085e9", "#e66767"];

const OV_SURFACE = "#1c1f2e";      // 2px spacer between adjacent fills
const OV_GRID = "rgba(255,255,255,0.06)";
const OV_TEXT = "#9498ab";

let ovWindow = "24h";
let ovCharts = {};
let ovData = null;

const OV_WINDOW_LABELS = { "1h": "1 hour", "6h": "6 hours", "24h": "24 hours", "7d": "7 days", "14d": "14 days" };

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

async function ovLoad(window) {
    if (window) ovWindow = window;
    document.querySelectorAll(".ov-window-btn").forEach(b =>
        b.classList.toggle("active", b.dataset.window === ovWindow));

    const wrap = document.getElementById("ovContent");
    if (!ovData) wrap.innerHTML = `<div class="loading"><div class="spinner"></div><span>Loading dashboard…</span></div>`;

    try {
        const resp = await fetch(`${API_BASE}/api/dashboard?window=${encodeURIComponent(ovWindow)}`);
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        ovData = data;
        ovRender(data);
    } catch (e) {
        wrap.innerHTML = `<div class="ov-error">⚠ ${esc(e.message)}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

function ovRender(d) {
    const s = d.summary;
    const finished = s.complete + s.failed + s.canceled;

    document.getElementById("ovContent").innerHTML = `
        <div class="ov-kpis">
            ${ovKpi("Failure rate", `${s.failureRate}%`, `${fmtNum(s.failed)} of ${fmtNum(finished)} finished runs`, s.failureRate >= 15 ? "bad" : s.failureRate >= 5 ? "warn" : "good")}
            ${ovKpi("Job runs", fmtNum(s.total), `${fmtNum(s.running)} still running`)}
            ${ovKpi("Succeeded", fmtNum(s.complete), "", "good")}
            ${ovKpi("Failed", fmtNum(s.failed), s.canceled ? `${fmtNum(s.canceled)} canceled` : "", "bad")}
            ${ovKpi("Flows run", fmtNum(s.flows), `${fmtNum(s.users)} users`)}
            ${ovKpi("Compute wasted", `${fmtNum(s.wastedHours)}h`, "time spent on runs that failed")}
        </div>

        <section class="ov-card ov-card-wide">
            <div class="ov-card-head">
                <h3>Runs over the last ${OV_WINDOW_LABELS[d.window]}</h3>
                <span class="ov-card-sub">${d.bucketMinutes >= 1440 ? "per day" : d.bucketMinutes >= 60 ? "per hour" : `per ${d.bucketMinutes} min`}</span>
            </div>
            <div class="ov-chart-box"><canvas id="ovTrendChart"></canvas></div>
        </section>

        <div class="ov-row">
            <section class="ov-card">
                <div class="ov-card-head">
                    <h3>Where failures happen</h3>
                    <span class="ov-card-sub">root stage, cascades excluded</span>
                </div>
                <div class="ov-chart-box ov-chart-sm"><canvas id="ovStageChart"></canvas></div>
            </section>
            <section class="ov-card">
                <div class="ov-card-head">
                    <h3>Failure categories</h3>
                    <span class="ov-card-sub">classified from the AAC error message</span>
                </div>
                <div class="ov-chart-box ov-chart-sm"><canvas id="ovCategoryChart"></canvas></div>
            </section>
        </div>

        <div class="ov-row">
            <section class="ov-card">
                <div class="ov-card-head"><h3>Top causes</h3></div>
                ${ovCausesTable(d.errorRules, s.failed)}
            </section>
            <section class="ov-card">
                <div class="ov-card-head">
                    <h3>Repeat offenders</h3>
                    <span class="ov-card-sub">flows failing most often</span>
                </div>
                ${ovFlowsTable(d.topFlows)}
            </section>
        </div>

        <div class="ov-row">
            <section class="ov-card">
                <div class="ov-card-head"><h3>Users with most failures</h3></div>
                ${ovUsersTable(d.topUsers)}
            </section>
            <section class="ov-card">
                <div class="ov-card-head"><h3>How jobs were triggered</h3></div>
                ${ovTriggerTable(d.ranFrom)}
            </section>
        </div>`;

    ovDrawTrend(d);
    ovDrawStages(d);
    ovDrawCategories(d);
    ovRenderSync(d.sync);
}

function ovKpi(label, value, sub, tone) {
    return `<div class="ov-kpi ${tone ? "ov-kpi-" + tone : ""}">
        <span class="ov-kpi-label">${esc(label)}</span>
        <span class="ov-kpi-value">${esc(String(value))}</span>
        <span class="ov-kpi-sub">${esc(sub || "")}</span>
    </div>`;
}

function fmtNum(n) {
    if (n == null) return "0";
    return Number(n).toLocaleString(undefined, { maximumFractionDigits: 1 });
}

// ---------------------------------------------------------------------------
// Charts
// ---------------------------------------------------------------------------

function ovDestroy(key) {
    if (ovCharts[key]) { ovCharts[key].destroy(); delete ovCharts[key]; }
}

const OV_BASE_OPTS = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
        legend: { labels: { color: OV_TEXT, boxWidth: 10, boxHeight: 10, usePointStyle: true, font: { size: 11 } } },
        tooltip: {
            backgroundColor: "#232738", borderColor: "rgba(255,255,255,0.10)", borderWidth: 1,
            titleColor: "#e8eaf0", bodyColor: "#e8eaf0", padding: 10, cornerRadius: 6,
        },
    },
};

function ovDrawTrend(d) {
    ovDestroy("trend");
    const ctx = document.getElementById("ovTrendChart");
    if (!ctx) return;
    const labels = d.timeseries.map(p => ovBucketLabel(p.bucket, d.bucketMinutes));
    const series = [
        { label: "Succeeded", key: "complete", color: OV_STATUS.complete },
        { label: "Canceled", key: "canceled", color: OV_STATUS.canceled },
        { label: "Failed", key: "failed", color: OV_STATUS.failed },
    ];
    ovCharts.trend = new Chart(ctx, {
        type: "bar",
        data: {
            labels,
            datasets: series.map(s => ({
                label: s.label,
                data: d.timeseries.map(p => p[s.key]),
                backgroundColor: s.color,
                // 2px surface gap between stacked segments, 4px rounded data end
                borderColor: OV_SURFACE, borderWidth: { top: 2, right: 0, bottom: 0, left: 0 },
                borderRadius: 4, borderSkipped: false,
            })),
        },
        options: {
            ...OV_BASE_OPTS,
            scales: {
                x: { stacked: true, grid: { display: false }, ticks: { color: OV_TEXT, font: { size: 10 }, maxRotation: 0, autoSkipPadding: 16 } },
                y: { stacked: true, beginAtZero: true, grid: { color: OV_GRID, drawBorder: false }, ticks: { color: OV_TEXT, font: { size: 10 }, precision: 0 } },
            },
        },
    });
}

function ovBucketLabel(bucket, minutes) {
    if (!bucket) return "";
    if (minutes >= 1440) return bucket.slice(5);                 // MM-DD
    return bucket.slice(11).replace(":00", "") + (minutes >= 60 ? ":00" : "");
}

/* One series → one hue, no legend; the card title names it. Direct value
   labels sit at the end of each bar so the ranking is readable without hover. */
function ovHBar(canvasId, key, rows, colorFn) {
    ovDestroy(key);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    if (!rows.length) {
        ctx.parentElement.innerHTML = `<div class="ov-empty-sm">No failures in this window</div>`;
        return;
    }
    ovCharts[key] = new Chart(ctx, {
        type: "bar",
        data: {
            labels: rows.map(r => r.name),
            datasets: [{
                label: "Failures",
                data: rows.map(r => r.count),
                backgroundColor: rows.map((r, i) => colorFn(r, i)),
                borderColor: OV_SURFACE, borderWidth: { top: 0, bottom: 0, left: 0, right: 2 },
                borderRadius: 4, borderSkipped: false, barThickness: 18,
            }],
        },
        options: {
            ...OV_BASE_OPTS,
            indexAxis: "y",
            plugins: { ...OV_BASE_OPTS.plugins, legend: { display: false } },
            scales: {
                x: { beginAtZero: true, grid: { color: OV_GRID, drawBorder: false }, ticks: { color: OV_TEXT, font: { size: 10 }, precision: 0 } },
                y: { grid: { display: false }, ticks: { color: "#e8eaf0", font: { size: 11 } } },
            },
        },
    });
}

function ovDrawStages(d) {
    ovHBar("ovStageChart", "stages", d.failureStages, () => OV_SERIES[0]);
}

function ovDrawCategories(d) {
    // Identity encoding: each category keeps its slot colour regardless of rank.
    const cats = d.errorCategories.slice(0, 8);
    const rest = d.errorCategories.slice(8);
    if (rest.length) cats.push({ name: "Other", count: rest.reduce((a, r) => a + r.count, 0) });
    const order = [...cats].sort((a, b) => a.name.localeCompare(b.name)).map(c => c.name);
    ovHBar("ovCategoryChart", "categories", cats,
           r => OV_SERIES[order.indexOf(r.name) % OV_SERIES.length]);
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

function ovCausesTable(rules, totalFailed) {
    if (!rules.length) return `<div class="ov-empty-sm">No failures in this window</div>`;
    return `<table class="ov-table"><thead><tr>
            <th>Cause</th><th>Category</th><th class="num">Failures</th><th class="num">Share</th>
        </tr></thead><tbody>
        ${rules.map(r => `<tr>
            <td>${esc(r.title)}</td>
            <td><span class="ov-pill">${esc(r.category)}</span></td>
            <td class="num">${fmtNum(r.count)}</td>
            <td class="num">${totalFailed ? (100 * r.count / totalFailed).toFixed(1) : "0"}%</td>
        </tr>`).join("")}
    </tbody></table>`;
}

function ovFlowsTable(flows) {
    if (!flows.length) return `<div class="ov-empty-sm">No failures in this window</div>`;
    return `<table class="ov-table"><thead><tr>
            <th>Flow</th><th class="num">Failures</th><th class="num">Causes</th><th>Last</th>
        </tr></thead><tbody>
        ${flows.map(f => `<tr>
            <td title="${esc(f.flow_name)}">${esc(f.flow_name)}</td>
            <td class="num">${fmtNum(f.failures)}</td>
            <td class="num">${f.distinct_causes}</td>
            <td class="ov-dim">${esc(ovShortTime(f.last_failure))}</td>
        </tr>`).join("")}
    </tbody></table>`;
}

function ovUsersTable(users) {
    if (!users.length) return `<div class="ov-empty-sm">No failures in this window</div>`;
    return `<table class="ov-table"><thead><tr>
            <th>User</th><th class="num">Failures</th><th class="num">Flows</th>
        </tr></thead><tbody>
        ${users.map(u => `<tr>
            <td title="${esc(u.creator_email)}">${esc(u.creator_email)}</td>
            <td class="num">${fmtNum(u.failures)}</td>
            <td class="num">${u.flows}</td>
        </tr>`).join("")}
    </tbody></table>`;
}

function ovTriggerTable(rows) {
    if (!rows.length) return `<div class="ov-empty-sm">No runs in this window</div>`;
    return `<table class="ov-table"><thead><tr>
            <th>Triggered by</th><th class="num">Runs</th><th class="num">Failed</th><th class="num">Rate</th>
        </tr></thead><tbody>
        ${rows.map(r => `<tr>
            <td>${esc(r.name)}</td>
            <td class="num">${fmtNum(r.total)}</td>
            <td class="num">${fmtNum(r.failed)}</td>
            <td class="num">${r.total ? (100 * r.failed / r.total).toFixed(1) : "0"}%</td>
        </tr>`).join("")}
    </tbody></table>`;
}

function ovShortTime(iso) {
    if (!iso) return "—";
    return iso.slice(5, 16).replace("T", " ");
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

function ovRenderSync(sync) {
    const el = document.getElementById("ovSyncStatus");
    if (!el || !sync) return;
    if (sync.running) {
        el.innerHTML = `<span class="spinner spinner-sm"></span> ${esc(sync.phase)} — ${esc(sync.message || "working")}`;
        el.className = "ov-sync-status active";
    } else if (sync.error) {
        el.textContent = `⚠ ${sync.error}`;
        el.className = "ov-sync-status error";
    } else if (sync.lastSyncAt) {
        el.textContent = `Synced ${ovShortTime(sync.lastSyncAt)} · ${sync.retentionDays}-day window`;
        el.className = "ov-sync-status";
    } else {
        el.textContent = "Never synced — run a sync to populate the dashboard";
        el.className = "ov-sync-status";
    }
    document.getElementById("ovSyncBtn").disabled = !!sync.running;
}

async function ovStartSync(full = false) {
    document.getElementById("ovSyncBtn").disabled = true;
    try {
        const resp = await fetch(`${API_BASE}/api/sync`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ full }),
        });
        const data = await resp.json();
        ovRenderSync(data.status);
        ovPollSync();
    } catch (e) {
        document.getElementById("ovSyncStatus").textContent = `⚠ ${e.message}`;
    }
}

let _ovPoll = null;
function ovPollSync() {
    if (_ovPoll) clearInterval(_ovPoll);
    _ovPoll = setInterval(async () => {
        try {
            const s = await (await fetch(`${API_BASE}/api/sync/status`)).json();
            ovRenderSync(s);
            if (!s.running) { clearInterval(_ovPoll); _ovPoll = null; ovLoad(); }
        } catch (_) { clearInterval(_ovPoll); _ovPoll = null; }
    }, 3000);
}

function ovInit() {
    ovLoad(ovWindow);
    // If a sync is already running (e.g. started before a page reload), follow it.
    fetch(`${API_BASE}/api/sync/status`).then(r => r.json()).then(s => {
        if (s.running) ovPollSync();
    }).catch(() => {});
}
