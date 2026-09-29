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

// Cross-filter state. Every panel re-queries against this, so clicking a bar
// narrows the whole dashboard rather than just one chart.
let ovFilters = {};
let ovOpenMessage = null;   // signature of the expanded drill-down row

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
        const params = new URLSearchParams({ window: ovWindow });
        Object.entries(ovFilters).forEach(([k, v]) => { if (v != null && v !== "") params.set(k, v); });
        const resp = await fetch(`${API_BASE}/api/dashboard?${params}`);
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        ovData = data;
        ovRender(data);
    } catch (e) {
        wrap.innerHTML = `<div class="ov-error">⚠ ${esc(e.message)}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Cross-filtering
// ---------------------------------------------------------------------------

const OV_FILTER_LABELS = {
    error_category: "Category", error_rule: "Cause", failure_stage: "Stage",
    flow_id: "Flow", from: "From", to: "To",
};

function ovSetFilter(key, value) {
    if (value == null || value === "" || ovFilters[key] === value) {
        delete ovFilters[key];            // clicking the active slice clears it
    } else {
        ovFilters[key] = value;
    }
    ovOpenMessage = null;
    ovLoad();
}

function ovSetTimeRange(from, to) {
    if (ovFilters.from === from && ovFilters.to === to) {
        delete ovFilters.from; delete ovFilters.to;
    } else {
        ovFilters.from = from; ovFilters.to = to;
    }
    ovOpenMessage = null;
    ovLoad();
}

function ovClearFilter(key) {
    delete ovFilters[key];
    if (key === "from") delete ovFilters.to;
    ovLoad();
}

function ovClearAllFilters() {
    ovFilters = {};
    ovOpenMessage = null;
    ovLoad();
}

function ovFilterChips() {
    const keys = Object.keys(ovFilters).filter(k => k !== "to");
    if (!keys.length) return "";
    const chips = keys.map(k => {
        const label = OV_FILTER_LABELS[k] || k;
        let value = ovFilters[k];
        if (k === "from") {
            value = `${ovShortTime(ovFilters.from)} → ${ovShortTime(ovFilters.to)}`;
        }
        if (k === "flow_id" && ovData) {
            const hit = ovData.topFlows.find(f => String(f.flow_id) === String(value));
            if (hit) value = hit.flow_name;
        }
        return `<span class="ov-chip">${esc(label)}: <strong>${esc(String(value).slice(0, 60))}</strong>
            <button class="ov-chip-x" onclick="ovClearFilter('${k}')" title="Remove">×</button></span>`;
    }).join("");
    return `<div class="ov-chip-bar"><span class="ov-chip-lead">Filtered</span>${chips}
        <button class="ov-chip-clear" onclick="ovClearAllFilters()">Clear all</button></div>`;
}

/* Hand the current slice to the All Jobs tab, where the individual runs live. */
function ovViewInAllJobs(extra) {
    const handoff = {};
    ["error_category", "error_rule", "failure_stage", "flow_id"].forEach(k => {
        if (ovFilters[k]) handoff[k] = ovFilters[k];
    });
    Object.assign(handoff, extra || {});
    switchTab("allJobs");
    ajApplyHandoff(handoff);
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

function ovRender(d) {
    const s = d.summary;
    const finished = s.complete + s.failed + s.canceled;

    const filtered = Object.keys(ovFilters).length > 0;

    document.getElementById("ovContent").innerHTML = `
        ${ovFilterChips()}
        ${filtered ? `<div class="ov-filter-note">Showing failures only — <button class="ov-link" onclick="ovViewInAllJobs()">view these runs in All Jobs →</button></div>` : ""}
        <div class="ov-kpis">
            ${ovKpi("Failure rate", `${s.failureRate}%`, `${fmtNum(s.failed)} of ${fmtNum(finished)} finished runs`, s.failureRate >= 15 ? "bad" : s.failureRate >= 5 ? "warn" : "good")}
            ${ovKpi("Job runs", fmtNum(s.total), filtered ? "matching runs" : `${fmtNum(s.running)} still running`)}
            ${ovKpi("Succeeded", fmtNum(s.complete), "", "good")}
            ${ovKpi("Failed", fmtNum(s.failed), s.canceled ? `${fmtNum(s.canceled)} canceled` : "", "bad")}
            ${ovKpi("Distinct flows", fmtNum(s.flows), `${fmtNum(s.users)} users`)}
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

        <section class="ov-card ov-card-wide">
            <div class="ov-card-head">
                <h3>Error messages</h3>
                <span class="ov-card-sub">grouped by message text — click one for the full error and affected flows</span>
            </div>
            ${ovMessagesList(d.errorMessages, s.failed)}
        </section>

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
            onClick: (evt, els) => {
                if (!els.length) return;
                const p = d.timeseries[els[0].index];
                if (p) ovSetTimeRange(...ovBucketRange(p.bucket, d.bucketMinutes));
            },
            onHover: (evt, els) => { evt.native.target.style.cursor = els.length ? "pointer" : "default"; },
            scales: {
                x: { stacked: true, grid: { display: false }, ticks: { color: OV_TEXT, font: { size: 10 }, maxRotation: 0, autoSkipPadding: 16 } },
                y: { stacked: true, beginAtZero: true, grid: { color: OV_GRID, drawBorder: false }, ticks: { color: OV_TEXT, font: { size: 10 }, precision: 0 } },
            },
        },
    });
}

/* A clicked bucket becomes an explicit [from, to) range the API can filter on. */
function ovBucketRange(bucket, minutes) {
    let from;
    // SQL buckets are 'YYYY-MM-DD' (day) or 'YYYY-MM-DDTHH:MM' (hour/minute).
    from = minutes >= 1440 ? `${bucket}T00:00:00.000Z` : `${bucket}:00.000Z`;
    const to = new Date(new Date(from).getTime() + minutes * 60000)
        .toISOString().replace(/\.\d{3}Z$/, ".000Z");
    return [from, to];
}

function ovBucketLabel(bucket, minutes) {
    if (!bucket) return "";
    if (minutes >= 1440) return bucket.slice(5);                 // MM-DD
    return bucket.slice(11).replace(":00", "") + (minutes >= 60 ? ":00" : "");
}

/* One series → one hue, no legend; the card title names it. Direct value
   labels sit at the end of each bar so the ranking is readable without hover. */
function ovHBar(canvasId, key, rows, colorFn, filterKey) {
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
            onClick: (evt, els) => {
                if (!els.length || !filterKey) return;
                const row = rows[els[0].index];
                if (row && row.name !== "Other") ovSetFilter(filterKey, row.name);
            },
            onHover: (evt, els) => { evt.native.target.style.cursor = (els.length && filterKey) ? "pointer" : "default"; },
            plugins: { ...OV_BASE_OPTS.plugins, legend: { display: false } },
            scales: {
                x: { beginAtZero: true, grid: { color: OV_GRID, drawBorder: false }, ticks: { color: OV_TEXT, font: { size: 10 }, precision: 0 } },
                y: { grid: { display: false }, ticks: { color: "#e8eaf0", font: { size: 11 } } },
            },
        },
    });
}

function ovDrawStages(d) {
    ovHBar("ovStageChart", "stages", d.failureStages, () => OV_SERIES[0], "failure_stage");
}

function ovDrawCategories(d) {
    // Identity encoding: each category keeps its slot colour regardless of rank.
    const cats = d.errorCategories.slice(0, 8);
    const rest = d.errorCategories.slice(8);
    if (rest.length) cats.push({ name: "Other", count: rest.reduce((a, r) => a + r.count, 0) });
    const order = [...cats].sort((a, b) => a.name.localeCompare(b.name)).map(c => c.name);
    ovHBar("ovCategoryChart", "categories", cats,
           r => OV_SERIES[order.indexOf(r.name) % OV_SERIES.length], "error_category");
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

function ovCausesTable(rules, totalFailed) {
    if (!rules.length) return `<div class="ov-empty-sm">No failures in this window</div>`;
    return `<table class="ov-table"><thead><tr>
            <th>Cause</th><th>Category</th><th class="num">Failures</th><th class="num">Share</th>
        </tr></thead><tbody>
        ${rules.map(r => `<tr class="ov-clickable ${ovFilters.error_rule === r.rule ? "ov-active" : ""}" onclick="ovSetFilter('error_rule', decodeURIComponent('${encodeURIComponent(r.rule)}'))">
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
        ${flows.map(f => `<tr class="ov-clickable ${String(ovFilters.flow_id) === String(f.flow_id) ? "ov-active" : ""}" onclick="ovSetFilter('flow_id', '${esc(String(f.flow_id))}')">
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

function ovBytes(n) {
    if (!n) return "0 B";
    const u = ["B", "KB", "MB", "GB"];
    let i = 0, v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
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
        const st = sync.storage || {};
        const size = st.dbBytes ? ` · ${ovBytes(st.dbBytes)}` : "";
        const rows = st.jobs ? ` · ${fmtNum(st.jobs)} runs stored` : "";
        el.textContent = `Synced ${ovShortTime(sync.lastSyncAt)} · keeping ${sync.retentionDays} days${rows}${size}`;
        el.title = "Older runs are purged automatically on each sync";
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
    ovLoadHints();
    ovLoad(ovWindow);
    // If a sync is already running (e.g. started before a page reload), follow it.
    fetch(`${API_BASE}/api/sync/status`).then(r => r.json()).then(s => {
        if (s.running) ovPollSync();
    }).catch(() => {});
}

// ---------------------------------------------------------------------------
// Error message drill-down
// ---------------------------------------------------------------------------

/*
 * The categories answer "what kind of problem"; this answers "what does the
 * platform actually say", which is what someone needs to fix it. Messages are
 * grouped on a signature with ids, paths and quoted names masked, so one real
 * problem is one row no matter how many times it fired.
 */
function ovMessagesList(messages, totalFailed) {
    if (!messages || !messages.length) {
        return `<div class="ov-empty-sm">No classified failures in this slice</div>`;
    }
    return `<div class="ov-msgs">${messages.map(m => ovMessageRow(m, totalFailed)).join("")}</div>`;
}

function ovMessageRow(m, totalFailed) {
    const open = ovOpenMessage === m.signature;
    const share = totalFailed ? (100 * m.count / totalFailed).toFixed(1) : "0";
    const sig = encodeURIComponent(m.signature || "");
    return `
    <div class="ov-msg ${open ? "open" : ""}">
        <div class="ov-msg-head" onclick="ovToggleMessage('${sig}')">
            <span class="ov-msg-arrow">${open ? "▾" : "▸"}</span>
            <span class="ov-msg-count">${fmtNum(m.count)}</span>
            <span class="ov-msg-pill">${esc(m.category || "—")}</span>
            <span class="ov-msg-text" title="${esc(m.signature || "")}">${esc(m.signature || "")}</span>
            <span class="ov-msg-meta">${fmtNum(m.flows)} flows · ${share}%</span>
        </div>
        <div class="ov-msg-body" id="ovMsgBody-${ovSigKey(m.signature)}">
            ${open ? ovMessageBodyPlaceholder() : ""}
        </div>
    </div>`;
}

// A DOM-safe id for a signature (which is free-form text).
function ovSigKey(signature) {
    let h = 0;
    const s = signature || "";
    for (let i = 0; i < s.length; i++) { h = ((h << 5) - h + s.charCodeAt(i)) | 0; }
    return `s${Math.abs(h)}`;
}

function ovMessageBodyPlaceholder() {
    return `<div class="ov-msg-loading"><span class="spinner spinner-sm"></span> Loading error detail…</div>`;
}

async function ovToggleMessage(encodedSig) {
    const signature = decodeURIComponent(encodedSig);
    ovOpenMessage = (ovOpenMessage === signature) ? null : signature;
    ovRender(ovData);                      // redraw rows so arrows/highlight update
    if (!ovOpenMessage) return;

    const body = document.getElementById(`ovMsgBody-${ovSigKey(signature)}`);
    if (!body) return;
    try {
        const params = new URLSearchParams({ window: ovWindow, signature });
        Object.entries(ovFilters).forEach(([k, v]) => { if (v != null && v !== "") params.set(k, v); });
        const data = await (await fetch(`${API_BASE}/api/failures/message?${params}`)).json();
        if (data.error) throw new Error(data.error);
        body.innerHTML = ovMessageDetail(data);
    } catch (e) {
        body.innerHTML = `<div class="ov-error">⚠ ${esc(e.message)}</div>`;
    }
}

function ovMessageDetail(data) {
    const s = data.summary || {};
    const hint = OV_HINTS[s.rule];
    const full = s.sample_why || s.sample_detail || "";
    return `
    <div class="ov-msg-detail">
        <div class="ov-msg-facts">
            ${ovFact("Cause", s.title)}
            ${ovFact("Stage", s.failure_stage)}
            ${ovFact("Exception", s.exception_type)}
            ${ovFact("Label", s.exception_label)}
            ${ovFact("Code", s.error_code)}
            ${ovFact("First seen", ovShortTime(s.first_seen))}
            ${ovFact("Last seen", ovShortTime(s.last_seen))}
        </div>
        ${hint ? `<div class="ov-msg-hint">💡 ${esc(hint)}</div>` : ""}
        <div class="ov-msg-label">Full message from AAC</div>
        <pre class="ov-msg-raw">${esc(full)}</pre>
        <div class="ov-msg-label">Flows hitting this error</div>
        <table class="ov-table ov-table-tight"><thead><tr>
            <th>Flow</th><th>User</th><th class="num">Failures</th><th>Last</th><th></th>
        </tr></thead><tbody>
            ${(data.flows || []).map(f => `<tr>
                <td title="${esc(f.flow_name)}">${esc(f.flow_name)}</td>
                <td class="ov-dim">${esc((f.creator_email || "").split("@")[0])}</td>
                <td class="num">${fmtNum(f.failures)}</td>
                <td class="ov-dim">${esc(ovShortTime(f.last_failure))}</td>
                <td><button class="ov-link" onclick="ovViewFlowJobs(${f.flow_id}, '${encodeURIComponent(data.signature)}')">runs →</button></td>
            </tr>`).join("")}
        </tbody></table>
        <div class="ov-msg-actions">
            <button class="btn-secondary" onclick="ovViewSignatureJobs('${encodeURIComponent(data.signature)}')">
                View all ${fmtNum(s.count)} runs in All Jobs →
            </button>
        </div>
    </div>`;
}

function ovFact(label, value) {
    if (value == null || value === "") return "";
    return `<div class="ov-fact"><span class="ov-fact-label">${esc(label)}</span><span class="ov-fact-value">${esc(String(value))}</span></div>`;
}

function ovViewSignatureJobs(encodedSig) {
    ovViewInAllJobs({ error_signature: decodeURIComponent(encodedSig) });
}

function ovViewFlowJobs(flowId, encodedSig) {
    ovViewInAllJobs({ flow_id: String(flowId), error_signature: decodeURIComponent(encodedSig) });
}

// Remediation hints come from the rule file, surfaced with the error text.
const OV_HINTS = {};
async function ovLoadHints() {
    try {
        const rules = await (await fetch(`${API_BASE}/api/error-rules`)).json();
        (rules.rules || []).forEach(r => { if (r.hint) OV_HINTS[r.id] = r.hint; });
    } catch (_) { /* hints are a nicety; the dashboard works without them */ }
}

// The Dashboard is the default tab, so it loads on startup rather than waiting
// for a tab switch.
if (document.getElementById("ovContent")) {
    ovInit();
}
