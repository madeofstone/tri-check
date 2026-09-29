/* ============================================================
   All Jobs Tab — Fetch, display, filter, and group all AACP jobs
   ============================================================
   Depends on: app.js (for esc(), fmtDurationMs(), statusBadge(),
               formatDateTime(), toggleDbxPanel(), renderDbxPanel())
   ============================================================ */

// State
let _ajJobs = [];        // current page of jobs
let _ajTotal = 0;        // total in DB
let _ajOffset = 0;       // current query offset
let _ajApiOffset = 0;    // next API offset for load-more from AACP
let _ajGrouped = false;  // group-by-flow toggle
let _ajHasMore = true;   // more pages available from API
let _ajHasDbMore = true; // more rows in DB

// ---------------------------------------------------------------------------
// Tab Switching
// ---------------------------------------------------------------------------

function switchTab(tab) {
    // Update tab button active state
    document.querySelectorAll(".topbar-tab").forEach(b => b.classList.remove("active"));
    document.querySelector(`.topbar-tab[data-tab="${tab}"]`)?.classList.add("active");

    // Toggle panel visibility via class (CSS handles display)
    const panels = { dashboard: "tabDashboard", allJobs: "tabAllJobs" };
    Object.entries(panels).forEach(([name, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle("active", tab === name);
        el.style.display = "";  // clear any inline style
    });

    if (tab === "allJobs") {
        ajLoadKpis();
        ajQueryJobs();
    } else if (tab === "dashboard") {
        ovLoad();
    }
}

// ---------------------------------------------------------------------------
// Fetch from AACP API
// ---------------------------------------------------------------------------

async function ajFetchJobs(refresh = true) {
    const btn = document.getElementById("ajFetchBtn");
    const loading = document.getElementById("ajLoading");
    const error = document.getElementById("ajError");
    const empty = document.getElementById("ajEmpty");

    btn.disabled = true;
    loading.style.display = "";
    error.style.display = "none";
    empty.style.display = "none";

    try {
        const resp = await fetch(`${API_BASE}/api/all-jobs/fetch`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ count: 100, refresh }),
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);

        _ajApiOffset = data.fetched;

        // Reload from DB
        await ajQueryJobs();
        await ajLoadKpis();
    } catch (e) {
        error.textContent = `⚠ ${e.message}`;
        error.style.display = "";
    } finally {
        btn.disabled = false;
        loading.style.display = "none";
    }
}

async function ajRefreshJobs() {
    const btn = document.getElementById("ajRefreshBtn");
    const fetchBtn = document.getElementById("ajFetchBtn");
    const loading = document.getElementById("ajLoading");
    const error = document.getElementById("ajError");

    btn.disabled = true;
    fetchBtn.disabled = true;
    btn.textContent = "Refreshing…";
    loading.style.display = "";
    error.style.display = "none";

    try {
        const resp = await fetch(`${API_BASE}/api/all-jobs/refresh`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);

        // Reload table + KPIs with fresh data
        await ajQueryJobs();
        await ajLoadKpis();
    } catch (e) {
        error.textContent = `⚠ ${e.message}`;
        error.style.display = "";
    } finally {
        btn.disabled = false;
        fetchBtn.disabled = false;
        btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" width="14" height="14"><polyline points="1 4 1 10 7 10"/><polyline points="23 20 23 14 17 14"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"/></svg> Refresh`;
        loading.style.display = "none";
    }
}

// ---------------------------------------------------------------------------
// Query from SQLite (via backend)
// ---------------------------------------------------------------------------

async function ajQueryJobs(append = false) {
    const wrap = document.getElementById("ajTableWrap");
    const empty = document.getElementById("ajEmpty");
    const error = document.getElementById("ajError");

    const filters = _ajBuildFilterParams();

    if (_ajGrouped) {
        try {
            const resp = await fetch(`${API_BASE}/api/all-jobs?group=flow&${filters}`);
            const data = await resp.json();
            if (data.error) throw new Error(data.error);

            _ajRenderGrouped(data.groups || []);
            wrap.style.display = "";
            empty.style.display = "none";
            document.getElementById("ajLoadMore").style.display = "none";
        } catch (e) {
            error.textContent = `⚠ ${e.message}`;
            error.style.display = "";
        }
        return;
    }

    if (!append) _ajOffset = 0;

    try {
        const resp = await fetch(
            `${API_BASE}/api/all-jobs?offset=${_ajOffset}&limit=50&${filters}`
        );
        const data = await resp.json();
        if (data.error) throw new Error(data.error);

        if (append) {
            _ajJobs = _ajJobs.concat(data.jobs || []);
        } else {
            _ajJobs = data.jobs || [];
        }
        _ajTotal = data.total || 0;
        _ajHasDbMore = data.hasMore || false;

        _ajRenderFlat(_ajJobs);

        if (_ajJobs.length > 0) {
            wrap.style.display = "";
            empty.style.display = "none";
        } else {
            wrap.style.display = "none";
            empty.style.display = "";
        }

        // Load more button
        const loadMore = document.getElementById("ajLoadMore");
        const count = document.getElementById("ajLoadMoreCount");
        if (_ajHasDbMore || _ajHasMore) {
            loadMore.style.display = "";
            count.textContent = `Showing ${_ajJobs.length} of ${_ajTotal}`;
        } else {
            loadMore.style.display = "none";
        }
    } catch (e) {
        error.textContent = `⚠ ${e.message}`;
        error.style.display = "";
    }
}

async function ajLoadMore() {
    const btn = document.getElementById("ajLoadMoreBtn");
    btn.disabled = true;
    btn.textContent = "Loading…";

    if (_ajHasDbMore) {
        // Load more from DB
        _ajOffset += 50;
        await ajQueryJobs(true);
    } else if (_ajHasMore) {
        // Fetch more from API
        try {
            const resp = await fetch(`${API_BASE}/api/all-jobs/load-more`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ offset: _ajApiOffset }),
            });
            const data = await resp.json();
            if (data.error) throw new Error(data.error);

            _ajApiOffset = data.nextOffset;
            _ajHasMore = data.hasMore;

            // Re-query from DB to pick up new data
            _ajOffset = 0;
            await ajQueryJobs();
        } catch (e) {
            document.getElementById("ajError").textContent = `⚠ ${e.message}`;
            document.getElementById("ajError").style.display = "";
        }
    }

    btn.disabled = false;
    btn.textContent = "Load More";
}

// ---------------------------------------------------------------------------
// KPIs + Daily Chart
// ---------------------------------------------------------------------------

async function ajLoadKpis() {
    try {
        const filters = _ajBuildFilterParams();
        const resp = await fetch(`${API_BASE}/api/all-jobs/kpis?${filters}`);
        const data = await resp.json();
        document.getElementById("ajKpiTotal").textContent = data.total_jobs ?? "—";
        document.getElementById("ajKpiRate").textContent =
            data.success_rate != null ? `${data.success_rate}%` : "—";
        
        const days = document.getElementById("ajDaysFilter")?.value || 30;
        document.getElementById("ajKpiDaily").textContent =
            data.jobs_per_day != null ? data.jobs_per_day : "—";
        document.getElementById("ajKpiDaily").nextElementSibling.textContent = `Jobs / Day (${days}d)`;
    } catch (e) {
        // Silently fail — KPIs are non-critical
    }
    // Also refresh the daily chart
    ajLoadDailyChart();
}

let _ajDailyChartInstance = null;

async function ajLoadDailyChart() {
    const filters = _ajBuildFilterParams();
    try {
        const resp = await fetch(`${API_BASE}/api/all-jobs/daily-chart?${filters}`);
        const data = await resp.json();
        const days = data.days || [];

        const labels = days.map(d => {
            // Format as "Mon DD" for readability
            const dt = new Date(d.date + "T00:00:00");
            return dt.toLocaleDateString("en-US", { month: "short", day: "numeric" });
        });
        const successData = days.map(d => d.success);
        const failedData = days.map(d => d.failed);
        const otherData = days.map(d => d.other);

        const canvas = document.getElementById("ajDailyChart");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");

        // Destroy previous instance to avoid canvas reuse errors
        if (_ajDailyChartInstance) {
            _ajDailyChartInstance.destroy();
            _ajDailyChartInstance = null;
        }

        _ajDailyChartInstance = new Chart(ctx, {
            type: "bar",
            data: {
                labels,
                datasets: [
                    {
                        label: "Successful",
                        data: successData,
                        backgroundColor: "rgba(52, 211, 153, 0.75)",
                        borderColor: "rgba(52, 211, 153, 1)",
                        borderWidth: 1,
                        borderRadius: 2,
                    },
                    {
                        label: "Failed",
                        data: failedData,
                        backgroundColor: "rgba(248, 113, 113, 0.75)",
                        borderColor: "rgba(248, 113, 113, 1)",
                        borderWidth: 1,
                        borderRadius: 2,
                    },
                    {
                        label: "Other",
                        data: otherData,
                        backgroundColor: "rgba(251, 191, 36, 0.55)",
                        borderColor: "rgba(251, 191, 36, 1)",
                        borderWidth: 1,
                        borderRadius: 2,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false, // Hidden per user request
                    },
                    tooltip: {
                        backgroundColor: "#232738",
                        titleColor: "#e8eaf0",
                        bodyColor: "#9498ab",
                        borderColor: "rgba(255,255,255,0.1)",
                        borderWidth: 1,
                        padding: 10,
                        cornerRadius: 6,
                        titleFont: { family: "Inter", weight: "600" },
                        bodyFont: { family: "Inter" },
                        callbacks: {
                            title: (items) => {
                                if (!items.length) return "";
                                const idx = items[0].dataIndex;
                                return days[idx]?.date || items[0].label;
                            },
                            afterBody: (items) => {
                                if (!items.length) return "";
                                const idx = items[0].dataIndex;
                                const total = (days[idx]?.success || 0) + (days[idx]?.failed || 0) + (days[idx]?.other || 0);
                                return `Total: ${total}`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            color: "#5d6177",
                            font: { size: 9, family: "Inter" },
                            maxRotation: 45,
                            autoSkip: true,
                            maxTicksLimit: 15,
                        },
                        border: { color: "rgba(255,255,255,0.06)" },
                    },
                    y: {
                        stacked: true,
                        beginAtZero: true,
                        grid: { color: "rgba(255,255,255,0.04)" },
                        ticks: {
                            color: "#5d6177",
                            font: { size: 10, family: "Inter" },
                            precision: 0,
                        },
                        border: { display: false },
                    },
                },
            },
        });
    } catch (e) {
        // Silently fail — chart is non-critical
    }
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

// Filters handed over from the Dashboard ("view these failures in All Jobs").
// Kept separate from the sidebar controls so clearing one does not wipe the other.
let _ajHandoff = null;

function ajApplyHandoff(handoff) {
    _ajHandoff = handoff && Object.keys(handoff).length ? { ...handoff } : null;
    const statusEl = document.getElementById("ajStatusFilter");
    if (statusEl && _ajHandoff) statusEl.value = "Failed";
    _ajRenderHandoffChip();
    ajLoadKpis();
    ajQueryJobs();
}

function ajClearHandoff() {
    _ajHandoff = null;
    _ajRenderHandoffChip();
    ajLoadKpis();
    ajQueryJobs();
}

function _ajRenderHandoffChip() {
    const bar = document.getElementById("ajHandoffBar");
    if (!bar) return;
    if (!_ajHandoff) { bar.style.display = "none"; bar.innerHTML = ""; return; }
    const labels = { error_category: "Category", error_rule: "Cause",
                     failure_stage: "Stage", flow_id: "Flow", error_signature: "Error" };
    const chips = Object.entries(_ajHandoff)
        .filter(([k]) => labels[k])
        .map(([k, v]) => `<span class="ov-chip">${labels[k]}: <strong>${esc(String(v).slice(0, 70))}</strong></span>`)
        .join("");
    bar.innerHTML = `<span class="ov-chip-lead">From dashboard</span>${chips}
        <button class="ov-chip-clear" onclick="ajClearHandoff()">Clear</button>`;
    bar.style.display = "";
}

function _ajBuildFilterParams() {
    const params = new URLSearchParams();
    const search = document.getElementById("ajSearch")?.value?.trim();
    const status = document.getElementById("ajStatusFilter")?.value;
    const days = document.getElementById("ajDaysFilter")?.value;

    if (search) params.set("search", search);
    if (status) params.set("status", status);
    if (days) params.set("days", days);

    if (_ajHandoff) {
        Object.entries(_ajHandoff).forEach(([k, v]) => {
            if (v != null && v !== "") params.set(k, v);
        });
    }

    return params.toString();
}

let _ajFilterTimer = null;
function ajApplyFilters() {
    clearTimeout(_ajFilterTimer);
    _ajFilterTimer = setTimeout(() => {
        ajLoadKpis();
        ajQueryJobs();
        ajLoadDailyChart();
    }, 300);
}

function ajToggleGroup() {
    _ajGrouped = document.getElementById("ajGroupToggle")?.checked || false;
    ajQueryJobs();
}

// ---------------------------------------------------------------------------
// Render: Flat Table
// ---------------------------------------------------------------------------

function _ajRenderFlat(jobs) {
    const head = document.getElementById("ajTableHead");
    const body = document.getElementById("ajTableBody");

    head.innerHTML = `<tr>
        <th style="width: 40px;"></th>
        <th style="width: 28px;"></th>
        <th>Job ID</th>
        <th>Status</th>
        <th>Flow Name</th>
        <th>Flow ID</th>
        <th>Ran From</th>
        <th>Ran For</th>
        <th>Creator</th>
        <th>Started</th>
        <th>Exec (min)</th>
        <th class="dbx-col-header">DBX</th>
    </tr>`;

    body.innerHTML = jobs.map(j => {
        const dbxBtn = j.databricks_job_id
            ? `<button class="dbx-btn" data-dbxid="${esc(j.databricks_job_id)}" data-dbxurl="${esc(j.databricks_service_url || "")}" data-jobid="${j.job_id}" onclick="ajToggleDbx(this)">DBX ▸</button>`
            : `<span class="cell-empty">—</span>`;

        const isChecked = selectedCompareJobs.some(c => c.job_id === j.job_id) ? "checked" : "";
        return `<tr data-jobid="${j.job_id}">
            <td><input type="checkbox" class="compare-cb" value="${j.job_id}" onchange="toggleCompareCheck(this, ${j.job_id})" ${isChecked}></td>
            <td><button class="aj-expand-btn" data-jobid="${j.job_id}" onclick="ajToggleDetail(this, ${j.job_id})" title="Job details">▸</button></td>
            <td><strong>${j.job_id}</strong></td>
            <td>${statusBadge(j.status)}</td>
            <td>${esc(j.flow_name || "—")}</td>
            <td>${j.flow_id || "—"}</td>
            <td>${esc(j.ran_from || "—")}</td>
            <td>${esc(j.ran_for || "—")}</td>
            <td>${esc(j.creator_email || "—")}</td>
            <td>${formatDateTime(j.created_at)}</td>
            <td>${j.execution_time_min != null ? j.execution_time_min.toFixed(1) : "—"}</td>
            <td class="dbx-cell">${dbxBtn}</td>
        </tr>`;
    }).join("");
}

// ---------------------------------------------------------------------------
// Render: Grouped by Flow
// ---------------------------------------------------------------------------

function _ajRenderGrouped(groups) {
    const head = document.getElementById("ajTableHead");
    const body = document.getElementById("ajTableBody");

    // Same headers as flat view, but "Job ID" becomes "Jobs"
    head.innerHTML = `<tr>
        <th class="aj-group-toggle-col"></th>
        <th style="width: 40px;"></th>
        <th>Jobs</th>
        <th>Status</th>
        <th>Flow Name</th>
        <th>Flow ID</th>
        <th>Ran From</th>
        <th>Ran For</th>
        <th>Creator</th>
        <th>Started</th>
        <th>Exec (min)</th>
        <th class="dbx-col-header">DBX</th>
    </tr>`;

    let html = "";
    for (const g of groups) {
        const gid = `aj-group-${g.flow_id}`;

        // Status badges with counts
        const statusHtml = Object.entries(g.status_counts || {})
            .map(([s, n]) => `${statusBadge(s)}<span class="aj-count-badge">${n}</span>`)
            .join(" ");

        // Ran From with counts
        const ranFromHtml = Object.entries(g.ran_from_counts || {})
            .map(([rf, n]) => `<span class="aj-ran-tag">${esc(rf)} <span class="aj-count-badge">${n}</span></span>`)
            .join(" ");

        // Ran For with counts
        const ranForHtml = Object.entries(g.ran_for_counts || {})
            .map(([rf, n]) => `<span class="aj-ran-tag">${esc(rf)} <span class="aj-count-badge">${n}</span></span>`)
            .join(" ");

        // Creators concatenated
        const creatorsHtml = (g.creators || []).map(c => esc(c)).join(", ") || "—";

        // Exec: min – max – avg
        const minE = g.min_exec != null ? g.min_exec.toFixed(1) : "—";
        const maxE = g.max_exec != null ? g.max_exec.toFixed(1) : "—";
        const avgE = g.avg_exec != null ? g.avg_exec.toFixed(1) : "—";
        const execHtml = g.min_exec != null ? `${minE} – ${maxE} – ${avgE}` : "—";

        html += `<tr class="aj-group-header" onclick="ajToggleGroupRow('${gid}')">
            <td class="aj-group-toggle">▸</td>
            <td></td>
            <td><strong>${g.job_count}</strong></td>
            <td class="aj-status-cell">${statusHtml}</td>
            <td><strong>${esc(g.flow_name || "—")}</strong></td>
            <td>${g.flow_id || "—"}</td>
            <td class="aj-ranfrom-cell">${ranFromHtml}</td>
            <td class="aj-ranfrom-cell">${ranForHtml}</td>
            <td class="aj-creators-cell" title="${esc(creatorsHtml)}">${creatorsHtml}</td>
            <td>${formatDateTime(g.earliest_created_at)}</td>
            <td class="aj-exec-range">${execHtml}</td>
            <td></td>
        </tr>`;

        // Expanded child rows — same columns as flat view
        for (const j of g.jobs) {
            const dbxBtn = j.databricks_job_id
                ? `<button class="dbx-btn" data-dbxid="${esc(j.databricks_job_id)}" data-dbxurl="${esc(j.databricks_service_url || "")}" data-jobid="${j.job_id}" onclick="ajToggleDbx(this)">DBX ▸</button>`
                : `<span class="cell-empty">—</span>`;

            const isChecked = selectedCompareJobs.some(c => c.job_id === j.job_id) ? "checked" : "";
            html += `<tr class="aj-group-child ${gid}" style="display:none">
                <td><button class="aj-expand-btn" data-jobid="${j.job_id}" onclick="ajToggleDetail(this, ${j.job_id})" title="Job details">▸</button></td>
                <td><input type="checkbox" class="compare-cb" value="${j.job_id}" onchange="toggleCompareCheck(this, ${j.job_id})" ${isChecked}></td>
                <td><strong>${j.job_id}</strong></td>
                <td>${statusBadge(j.status)}</td>
                <td>${esc(j.flow_name || "—")}</td>
                <td>${j.flow_id || "—"}</td>
                <td>${esc(j.ran_from || "—")}</td>
                <td>${esc(j.ran_for || "—")}</td>
                <td>${esc(j.creator_email || "—")}</td>
                <td>${formatDateTime(j.created_at)}</td>
                <td>${j.execution_time_min != null ? j.execution_time_min.toFixed(1) : "—"}</td>
                <td class="dbx-cell">${dbxBtn}</td>
            </tr>`;
        }
    }
    body.innerHTML = html;
}

function ajToggleGroupRow(gid) {
    const children = document.querySelectorAll(`.${gid}`);
    const header = children.length > 0 ? children[0].previousElementSibling : null;
    const toggle = header?.querySelector(".aj-group-toggle");

    const visible = children[0]?.style.display !== "none";
    children.forEach(c => c.style.display = visible ? "none" : "");
    if (toggle) toggle.textContent = visible ? "▸" : "▾";
}

// ---------------------------------------------------------------------------
// DBX Panel (reuses shared code from app.js)
// ---------------------------------------------------------------------------

async function ajToggleDbx(btn) {
    const parentRow = btn.closest("tr");
    const existingDetail = parentRow.nextElementSibling;

    // Toggle off
    if (existingDetail && existingDetail.classList.contains("dbx-detail-row")) {
        existingDetail.remove();
        btn.textContent = "DBX ▸";
        btn.classList.remove("active");
        return;
    }

    // Toggle on
    btn.textContent = "DBX ▾";
    btn.classList.add("active");

    const dbxId = btn.dataset.dbxid;
    const dbxUrl = btn.dataset.dbxurl || "";
    const jobId = btn.dataset.jobid;
    const totalCols = 10;

    const detailRow = document.createElement("tr");
    detailRow.className = "dbx-detail-row";
    detailRow.innerHTML = `<td colspan="${totalCols}" class="dbx-detail-cell"><div class="dbx-loading"><span class="spinner"></span> Loading Databricks details…</div></td>`;
    parentRow.after(detailRow);

    try {
        const resp = await fetch(`${API_BASE}/api/databricks`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ databricksJobId: dbxId, flowName: "", jobRunId: jobId, databricksServiceUrl: dbxUrl }),
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        data._jobRunId = jobId;
        data._flowName = "";

        // Check if analysis already exists for this job
        try {
            const analysisResp = await fetch(`${API_BASE}/api/eventlog/${encodeURIComponent(jobId)}`);
            if (analysisResp.ok) {
                const analysisData = await analysisResp.json();
                if (analysisData.analysis && !analysisData.error) {
                    data._isAnalyzed = true;
                }
            }
        } catch (_) { /* ignore — just means no analysis yet */ }

        detailRow.querySelector(".dbx-detail-cell").innerHTML = renderDbxPanel(data);
    } catch (e) {
        detailRow.querySelector(".dbx-detail-cell").innerHTML = `<div class="dbx-error">⚠ ${esc(e.message)}</div>`;
    }
}

// ---------------------------------------------------------------------------
// Init — load from DB on first visit
// ---------------------------------------------------------------------------

function ajInit() {
    ajLoadKpis();
    ajQueryJobs();
}

// ---------------------------------------------------------------------------
// Job detail panel
// ---------------------------------------------------------------------------

const AJ_DETAIL_COLS = 12;

async function ajToggleDetail(btn, jobId) {
    const row = btn.closest("tr");
    const next = row.nextElementSibling;

    if (next && next.classList.contains("aj-detail-row")) {
        next.remove();
        btn.textContent = "▸";
        btn.classList.remove("active");
        return;
    }

    btn.textContent = "▾";
    btn.classList.add("active");
    const detail = document.createElement("tr");
    detail.className = "aj-detail-row";
    detail.innerHTML = `<td colspan="${AJ_DETAIL_COLS}" class="aj-detail-cell">
        <div class="aj-detail-loading"><span class="spinner spinner-sm"></span> Loading job detail…</div></td>`;
    row.after(detail);

    try {
        const data = await (await fetch(`${API_BASE}/api/job/${jobId}/detail`)).json();
        if (data.error) throw new Error(data.error);
        detail.querySelector(".aj-detail-cell").innerHTML = ajRenderDetail(data);
        ajProbeDbxLogs(jobId);
    } catch (e) {
        detail.querySelector(".aj-detail-cell").innerHTML =
            `<div class="aj-detail-error">⚠ ${esc(e.message)}</div>`;
    }
}

function ajRenderDetail(d) {
    const j = d.job || {};
    const err = d.error;

    return `<div class="aj-detail">
        <div class="aj-detail-facts">
            ${ajFact("Job group", j.job_id)}
            ${ajFact("Flow", j.flow_name)}
            ${ajFact("Flow ID", j.flow_id)}
            ${ajFact("Status", j.status)}
            ${ajFact("Triggered by", j.ran_from)}
            ${ajFact("Ran for", j.ran_for)}
            ${ajFact("Creator", j.creator_email)}
            ${ajFact("Started", formatDateTime(j.created_at))}
            ${ajFact("Runtime", j.execution_time_min != null ? `${j.execution_time_min.toFixed(1)} min` : null)}
            ${ajFact("Engine", j.execution_language)}
        </div>

        ${d.warnings && d.warnings.length ? `<div class="aj-detail-warn">${d.warnings.map(esc).join("<br>")}</div>` : ""}

        <div class="aj-detail-cols">
            <div class="aj-detail-col">
                <div class="aj-detail-label">Stages</div>
                ${ajStagesTable(d.stages)}
            </div>
            <div class="aj-detail-col">
                <div class="aj-detail-label">${err ? "Failure" : "Source & target"}</div>
                ${err ? ajErrorBlock(err) : ajIoBlock(d)}
            </div>
        </div>

        ${err ? `<div class="aj-detail-label">Source & target</div>${ajIoBlock(d)}` : ""}

        <div class="aj-detail-label">Databricks logs</div>
        <div class="aj-dbx-logs" id="ajDbxLogs-${j.job_id}">
            ${d.databricks && d.databricks.available
                ? `<div class="aj-detail-loading"><span class="spinner spinner-sm"></span> Checking what Databricks has…</div>`
                : `<div class="aj-detail-dim">This job has no Databricks run recorded — it never reached Spark.</div>`}
        </div>
    </div>`;
}

function ajFact(label, value) {
    if (value == null || value === "") return "";
    return `<div class="aj-fact"><span class="aj-fact-label">${esc(label)}</span>
        <span class="aj-fact-value">${esc(String(value))}</span></div>`;
}

/* Pipeline stages in execution order, with the stage that actually failed
   marked — the cascade stages around it are not the thing to investigate. */
function ajStagesTable(stages) {
    if (!stages || !stages.length) {
        return `<div class="aj-detail-dim">AAC returned no stage detail for this job.</div>`;
    }
    return `<table class="ov-table ov-table-tight aj-stages"><thead><tr>
            <th>Stage</th><th>Status</th><th class="num">Duration</th><th></th>
        </tr></thead><tbody>
        ${stages.map(s => `<tr class="${s.isRootFailure ? "aj-stage-root" : ""}">
            <td>${esc(s.jobType || "—")}</td>
            <td>${statusBadge(s.status)}</td>
            <td class="num">${s.durationSeconds != null ? ajDuration(s.durationSeconds) : "—"}</td>
            <td class="aj-stage-flag">${s.isRootFailure ? "failed here" : (s.hasWarnings ? "warnings" : "")}</td>
        </tr>`).join("")}
    </tbody></table>`;
}

function ajDuration(seconds) {
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    const m = Math.floor(seconds / 60);
    return `${m}m ${Math.round(seconds - m * 60)}s`;
}

function ajErrorBlock(err) {
    return `<div class="aj-error-block">
        <div class="aj-error-head">
            <span class="aj-error-title">${esc(err.title || "Failure")}</span>
            <span class="ov-pill">${esc(err.category || "—")}</span>
        </div>
        <div class="aj-error-meta">
            ${[["Stage", err.stage], ["Exception", err.exceptionType], ["Label", err.exceptionLabel],
               ["Code", err.errorCode]].filter(([, v]) => v).map(([k, v]) =>
                `<span>${esc(k)}: <strong>${esc(String(v))}</strong></span>`).join("")}
        </div>
        ${err.hint ? `<div class="ov-msg-hint">💡 ${esc(err.hint)}</div>` : ""}
        <pre class="ov-msg-raw">${esc(err.why || err.message || "(no message)")}</pre>
    </div>`;
}

function ajIoBlock(d) {
    const inputs = d.inputs && d.inputs.data ? d.inputs.data : [];
    const outputs = d.outputs || {};
    const tables = outputs.tables || [];
    const files = outputs.files || [];
    if (!inputs.length && !tables.length && !files.length) {
        return `<div class="aj-detail-dim">No source or target detail available.</div>`;
    }
    const srcLines = [];
    inputs.forEach(ds => (ds.inputs || []).forEach(i => {
        const q = Array.isArray(i.query) ? i.query[0] : i.query;
        srcLines.push(`${esc(ds.name || "dataset")} · ${esc(i.vendor || i.protocol || "source")}${q ? `\n    ${esc(String(q).slice(0, 220))}` : ""}`);
    }));
    const dstLines = [
        ...tables.map(tb => `${esc(tb.action || "write")} · ${esc(tb.table || tb.databaseConnectString || "table")}`),
        ...files.map(f => `${esc(f.fileType || "file")} · ${esc(f.uri || "")}`),
    ];
    return `<div class="aj-io">
        ${srcLines.length ? `<div class="aj-io-group"><span class="aj-io-label">Sources</span><pre class="ov-msg-raw">${srcLines.join("\n")}</pre></div>` : ""}
        ${dstLines.length ? `<div class="aj-io-group"><span class="aj-io-label">Targets</span><pre class="ov-msg-raw">${dstLines.join("\n")}</pre></div>` : ""}
    </div>`;
}

// ---------------------------------------------------------------------------
// Databricks log bundle
// ---------------------------------------------------------------------------

/* Checked before offering the download: on this tenant a PAT can only read the
   runs of certain AAC users, so the button would otherwise fail after a wait. */
async function ajProbeDbxLogs(jobId) {
    const box = document.getElementById(`ajDbxLogs-${jobId}`);
    if (!box) return;
    try {
        const a = await (await fetch(`${API_BASE}/api/job/${jobId}/dbx-logs/available`)).json();
        if (!a.available) {
            box.innerHTML = `<div class="aj-detail-dim">⚠ ${esc(a.reason || "Logs are not available for this run.")}</div>`;
            return;
        }
        const kinds = Object.entries(a.byKind || {})
            .map(([k, v]) => `${k} (${v.files} file${v.files === 1 ? "" : "s"}, ${ajBytes(v.bytes)})`)
            .join(" · ");
        box.innerHTML = `
            <div class="aj-dbx-logs-row">
                <button class="btn-primary" onclick="ajDownloadDbxLogs(${jobId}, this)">
                    Download DBX Job Logs (${ajBytes(a.totalBytes)})
                </button>
                <div class="aj-dbx-logs-meta">
                    <div>${esc(kinds)}</div>
                    <div class="aj-detail-dim">cluster ${esc(a.clusterId || "—")} · Spark result ${esc(a.resultState || "—")}</div>
                </div>
            </div>`;
    } catch (e) {
        box.innerHTML = `<div class="aj-detail-dim">⚠ ${esc(e.message)}</div>`;
    }
}

function ajBytes(n) {
    if (!n) return "0 B";
    const u = ["B", "KB", "MB", "GB"];
    let i = 0, v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
}

/* The zip is assembled server-side from DBFS, which takes a few seconds, so
   the button reports progress rather than appearing to do nothing. */
async function ajDownloadDbxLogs(jobId, btn) {
    const original = btn.textContent;
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner spinner-sm"></span> Collecting logs from Databricks…`;
    try {
        const resp = await fetch(`${API_BASE}/api/job/${jobId}/dbx-logs`);
        if (!resp.ok) {
            let msg = `Download failed (${resp.status})`;
            try { msg = (await resp.json()).error || msg; } catch (_) {}
            throw new Error(msg);
        }
        const blob = await resp.blob();
        const cd = resp.headers.get("Content-Disposition") || "";
        const match = cd.match(/filename="?([^";]+)"?/);
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = match ? match[1] : `dbx-logs-${jobId}.zip`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        btn.textContent = `Downloaded (${ajBytes(blob.size)})`;
        setTimeout(() => { btn.textContent = original; btn.disabled = false; }, 4000);
    } catch (e) {
        btn.textContent = original;
        btn.disabled = false;
        const box = document.getElementById(`ajDbxLogs-${jobId}`);
        if (box) box.insertAdjacentHTML("beforeend",
            `<div class="aj-detail-error">⚠ ${esc(e.message)}</div>`);
    }
}
