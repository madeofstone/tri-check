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
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelector(`[data-tab="${tab}"]`)?.classList.add("active");

    document.getElementById("tabAllJobs").style.display = tab === "allJobs" ? "" : "none";
    document.getElementById("tabFlowAnalysis").style.display = tab === "flowAnalysis" ? "" : "none";

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
// KPIs
// ---------------------------------------------------------------------------

async function ajLoadKpis() {
    try {
        const resp = await fetch(`${API_BASE}/api/all-jobs/kpis`);
        const data = await resp.json();
        document.getElementById("ajKpiTotal").textContent = data.total_jobs ?? "—";
        document.getElementById("ajKpiRate").textContent =
            data.success_rate != null ? `${data.success_rate}%` : "—";
        document.getElementById("ajKpiDaily").textContent =
            data.jobs_per_day_7d != null ? data.jobs_per_day_7d : "—";
    } catch (e) {
        // Silently fail — KPIs are non-critical
    }
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

function _ajBuildFilterParams() {
    const params = new URLSearchParams();
    const search = document.getElementById("ajSearch")?.value?.trim();
    const status = document.getElementById("ajStatusFilter")?.value;

    if (search) params.set("search", search);
    if (status) params.set("status", status);

    return params.toString();
}

let _ajFilterTimer = null;
function ajApplyFilters() {
    clearTimeout(_ajFilterTimer);
    _ajFilterTimer = setTimeout(() => ajQueryJobs(), 300);
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

    head.innerHTML = `<tr>
        <th></th>
        <th>Flow Name</th>
        <th>Flow ID</th>
        <th>Jobs</th>
        <th>Min Exec (min)</th>
        <th>Max Exec (min)</th>
        <th>Latest Run</th>
    </tr>`;

    let html = "";
    for (const g of groups) {
        const gid = `aj-group-${g.flow_id}`;
        html += `<tr class="aj-group-header" onclick="ajToggleGroupRow('${gid}')">
            <td class="aj-group-toggle">▸</td>
            <td><strong>${esc(g.flow_name || "—")}</strong></td>
            <td>${g.flow_id || "—"}</td>
            <td>${g.job_count}</td>
            <td>${g.min_exec != null ? g.min_exec.toFixed(1) : "—"}</td>
            <td>${g.max_exec != null ? g.max_exec.toFixed(1) : "—"}</td>
            <td>${formatDateTime(g.latest_created_at)}</td>
        </tr>`;

        // Nested rows (hidden by default)
        for (const j of g.jobs) {
            const dbxBtn = j.databricks_job_id
                ? `<button class="dbx-btn" data-dbxid="${esc(j.databricks_job_id)}" data-jobid="${j.job_id}" onclick="ajToggleDbx(this)">DBX ▸</button>`
                : `<span class="cell-empty">—</span>`;

            html += `<tr class="aj-group-child ${gid}" style="display:none">
                <td></td>
                <td colspan="2">Job ${j.job_id} — ${statusBadge(j.status)}</td>
                <td>${esc(j.creator_email || "—")}</td>
                <td>${j.execution_time_min != null ? j.execution_time_min.toFixed(1) : "—"}</td>
                <td>${formatDateTime(j.created_at)}</td>
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
    const totalCols = 9;

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
