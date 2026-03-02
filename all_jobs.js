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
    const allJobsPanel = document.getElementById("tabAllJobs");
    const flowPanel = document.getElementById("tabFlowAnalysis");

    allJobsPanel.classList.toggle("active", tab === "allJobs");
    allJobsPanel.style.display = "";  // clear any inline style
    flowPanel.classList.toggle("active", tab === "flowAnalysis");
    flowPanel.style.display = "";     // clear any inline style

    if (tab === "allJobs") {
        ajLoadKpis();
        ajQueryJobs();
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

function _ajBuildFilterParams() {
    const params = new URLSearchParams();
    const search = document.getElementById("ajSearch")?.value?.trim();
    const status = document.getElementById("ajStatusFilter")?.value;
    const days = document.getElementById("ajDaysFilter")?.value;

    if (search) params.set("search", search);
    if (status) params.set("status", status);
    if (days) params.set("days", days);

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
            ? `<button class="dbx-btn" data-dbxid="${esc(j.databricks_job_id)}" data-jobid="${j.job_id}" onclick="ajToggleDbx(this)">DBX ▸</button>`
            : `<span class="cell-empty">—</span>`;

        return `<tr data-jobid="${j.job_id}">
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
                ? `<button class="dbx-btn" data-dbxid="${esc(j.databricks_job_id)}" data-jobid="${j.job_id}" onclick="ajToggleDbx(this)">DBX ▸</button>`
                : `<span class="cell-empty">—</span>`;

            html += `<tr class="aj-group-child ${gid}" style="display:none">
                <td></td>
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
            body: JSON.stringify({ databricksJobId: dbxId, flowName: "", jobRunId: jobId }),
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
