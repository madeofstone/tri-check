/* ============================================================
   Tri-Tracker — Frontend Logic
   ============================================================ */

const API_BASE = "";  // Same origin

// ---- State ------------------------------------------------
let flows = [];          // [{name, pairs, errors, aacCount, onpremCount, aacBaseUrl, onpremBaseUrl, onpremEnabled, fetched}]
let activeFlowIndex = -1;
let onpremEnabled = true; // global state, updated from config/API

// ---- DOM refs ---------------------------------------------
const sidebar        = document.getElementById("sidebarNav");
const mainContent    = document.getElementById("mainContent");
const emptyState     = document.getElementById("emptyState");
const flowDetail     = document.getElementById("flowDetail");
const flowTitle      = document.getElementById("flowTitle");
const flowSubtitle   = document.getElementById("flowSubtitle");
const collectBtn     = document.getElementById("collectBtn");
const loading        = document.getElementById("loading");
const errorBanner    = document.getElementById("errorBanner");
const resultsWrapper = document.getElementById("resultsWrapper");
const resultsStats   = document.getElementById("resultsStats");
const resultsBody    = document.getElementById("resultsBody");

// Modals
const addFlowModal   = document.getElementById("addFlowModal");
const newFlowInput   = document.getElementById("newFlowName");
const settingsModal  = document.getElementById("settingsModal");
const settingsBody   = document.getElementById("settingsBody");

// ---- Init -------------------------------------------------
document.getElementById("addFlowBtn").addEventListener("click", openAddFlowModal);
document.getElementById("addFlowCancel").addEventListener("click", closeAddFlowModal);
document.getElementById("addFlowCancelBtn").addEventListener("click", closeAddFlowModal);
document.getElementById("addFlowConfirm").addEventListener("click", confirmAddFlow);
document.getElementById("settingsBtn").addEventListener("click", openSettings);
document.getElementById("settingsClose").addEventListener("click", closeSettings);
document.getElementById("settingsCancelBtn").addEventListener("click", closeSettings);
document.getElementById("settingsSaveBtn").addEventListener("click", saveSettings);
collectBtn.addEventListener("click", collectRuns);

// Enter key in add-flow input
newFlowInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") confirmAddFlow();
});

// Close modals on backdrop click
addFlowModal.addEventListener("click", (e) => {
    if (e.target === addFlowModal) closeAddFlowModal();
});
settingsModal.addEventListener("click", (e) => {
    if (e.target === settingsModal) closeSettings();
});

// Load saved flows on startup
loadSavedFlows();

// ---- Load Saved Flows on Startup --------------------------
async function loadSavedFlows() {
    try {
        const res = await fetch(`${API_BASE}/api/flows`);
        const data = await res.json();
        const savedFlows = data.flows || [];
        if (savedFlows.length === 0) return;

        savedFlows.forEach(sf => {
            // Skip if already in the list (shouldn't happen but guard)
            if (flows.some(f => f.name === sf.name)) return;
            flows.push({
                name: sf.name,
                pairs: sf.pairs || [],
                errors: sf.errors || [],
                aacCount: (sf.pairs || []).filter(p => p.aac).length,
                onpremCount: (sf.pairs || []).filter(p => p.onprem).length,
                aacBaseUrl: sf.aacBaseUrl || "",
                onpremBaseUrl: sf.onpremBaseUrl || "",
                onpremEnabled: sf.onpremEnabled !== false,
                analyzedJobs: sf.analyzedJobs || [],
                dbxCachedJobs: sf.dbxCachedJobs || [],
                fetched: true,
            });
        });
        renderSidebar();
        if (flows.length > 0 && activeFlowIndex < 0) {
            selectFlow(0);
        }
    } catch (e) {
        console.warn("Could not load saved flows:", e);
    }
}

// ---- Add Flow Modal ---------------------------------------
function openAddFlowModal() {
    newFlowInput.value = "";
    addFlowModal.style.display = "flex";
    setTimeout(() => newFlowInput.focus(), 100);
}

function closeAddFlowModal() {
    addFlowModal.style.display = "none";
}

function confirmAddFlow() {
    const name = newFlowInput.value.trim();
    if (!name) return;
    // Prevent duplicates
    if (flows.some(f => f.name === name)) {
        selectFlow(flows.findIndex(f => f.name === name));
        closeAddFlowModal();
        return;
    }
    flows.push({ name, pairs: [], errors: [], aacCount: 0, onpremCount: 0, aacBaseUrl: "", onpremBaseUrl: "", onpremEnabled: true, fetched: false });
    closeAddFlowModal();
    renderSidebar();
    selectFlow(flows.length - 1);
}

// ---- Sidebar Rendering ------------------------------------
function renderSidebar() {
    sidebar.innerHTML = "";
    flows.forEach((flow, idx) => {
        const item = document.createElement("div");
        item.className = `sidebar-item${idx === activeFlowIndex ? " active" : ""}`;
        item.innerHTML = `
            <span class="sidebar-item-name" title="${esc(flow.name)}">${esc(flow.name)}</span>
            <button class="icon-btn icon-btn-sm remove-btn" title="Remove" aria-label="Remove flow">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>`;
        item.querySelector(".sidebar-item-name").addEventListener("click", () => selectFlow(idx));
        item.querySelector(".remove-btn").addEventListener("click", (e) => {
            e.stopPropagation();
            removeFlow(idx);
        });
        sidebar.appendChild(item);
    });
}

function selectFlow(idx) {
    activeFlowIndex = idx;
    renderSidebar();
    showFlowDetail();
}

async function removeFlow(idx) {
    const flow = flows[idx];
    flows.splice(idx, 1);
    if (activeFlowIndex >= flows.length) activeFlowIndex = flows.length - 1;
    renderSidebar();
    if (flows.length === 0) {
        activeFlowIndex = -1;
        showEmpty();
    } else {
        showFlowDetail();
    }
    // Delete from disk
    if (flow?.name) {
        try { await fetch(`${API_BASE}/api/flows/${encodeURIComponent(flow.name)}`, { method: "DELETE" }); } catch (_) {}
    }
}

function showEmpty() {
    emptyState.style.display = "flex";
    flowDetail.style.display = "none";
}

function showFlowDetail() {
    if (activeFlowIndex < 0 || activeFlowIndex >= flows.length) {
        showEmpty();
        return;
    }
    const flow = flows[activeFlowIndex];
    emptyState.style.display = "none";
    flowDetail.style.display = "block";
    flowTitle.textContent = flow.name;
    flowSubtitle.textContent = flow.fetched
        ? (flow.onpremEnabled
            ? `AAC: ${flow.aacCount} jobs · On-Prem: ${flow.onpremCount} jobs`
            : `AAC: ${flow.aacCount} jobs`)
        : "Not yet fetched";
    renderResults(flow);
}

// ---- Collect Runs -----------------------------------------
async function collectRuns() {
    if (activeFlowIndex < 0) return;
    const flow = flows[activeFlowIndex];

    collectBtn.disabled = true;
    loading.style.display = "flex";
    errorBanner.style.display = "none";
    resultsWrapper.style.display = "none";

    try {
        const res = await fetch(`${API_BASE}/api/jobs`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ flowName: flow.name }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Request failed");

        flow.pairs = data.pairs || [];
        flow.aacCount = (data.aac || []).length;
        flow.onpremCount = (data.onprem || []).length;
        flow.aacBaseUrl = data.aacBaseUrl || "";
        flow.onpremBaseUrl = data.onpremBaseUrl || "";
        flow.onpremEnabled = data.onpremEnabled !== false;
        flow.errors = data.errors || [];
        flow.analyzedJobs = data.analyzedJobs || [];
        flow.dbxCachedJobs = data.dbxCachedJobs || [];
        flow.fetched = true;

        if (flow.errors.length > 0) {
            errorBanner.innerHTML = flow.errors.map(e => esc(e)).join("<br>");
            errorBanner.style.display = "block";
        }
    } catch (err) {
        errorBanner.textContent = `Error: ${err.message}`;
        errorBanner.style.display = "block";
    } finally {
        collectBtn.disabled = false;
        loading.style.display = "none";
        showFlowDetail();
    }
}

// ---- Render Results Table ---------------------------------
function renderResults(flow) {
    if (!flow.fetched || flow.pairs.length === 0) {
        resultsWrapper.style.display = "none";
        return;
    }
    resultsWrapper.style.display = "block";
    const showOnprem = flow.onpremEnabled;

    // Build dynamic table header
    const resultsHead = document.getElementById("resultsHead");
    if (showOnprem) {
        resultsHead.innerHTML = `
            <tr>
                <th colspan="6" class="env-header env-header-onprem">On-Prem Trifacta</th>
                <th class="divider-header"></th>
                <th colspan="7" class="env-header env-header-aac">Alteryx Analytics Cloud (AAC)</th>
            </tr>
            <tr>
                <th>Job Run ID</th><th>Status</th><th>Flow ID</th><th>Flow Name</th><th>Started</th><th>Exec (min)</th>
                <th class="divider-col"></th>
                <th>Job Run ID</th><th>Status</th><th>Flow ID</th><th>Flow Name</th><th>Started</th><th>Exec (min)</th>
                <th class="dbx-col-header">DBX</th>
            </tr>`;
    } else {
        resultsHead.innerHTML = `
            <tr>
                <th colspan="7" class="env-header env-header-aac">Alteryx Analytics Cloud (AAC)</th>
            </tr>
            <tr>
                <th>Job Run ID</th><th>Status</th><th>Flow ID</th><th>Flow Name</th><th>Started</th><th>Exec (min)</th>
                <th class="dbx-col-header">DBX</th>
            </tr>`;
    }

    const matched = flow.pairs.filter(p => p.matched).length;
    const unmatched = flow.pairs.length - matched;
    if (showOnprem) {
        resultsStats.innerHTML = `
            <span>Total pairs: <span class="stat-value">${flow.pairs.length}</span></span>
            <span>Matched: <span class="stat-value">${matched}</span></span>
            <span>Unmatched: <span class="stat-value" style="color:var(--amber)">${unmatched}</span></span>
        `;
    } else {
        resultsStats.innerHTML = `
            <span>Total jobs: <span class="stat-value">${flow.pairs.length}</span></span>
        `;
    }
    flowSubtitle.textContent = showOnprem
        ? `AAC: ${flow.aacCount} jobs · On-Prem: ${flow.onpremCount} jobs`
        : `AAC: ${flow.aacCount} jobs`;

    resultsBody.innerHTML = "";
    flow.pairs.forEach((pair, idx) => {
        const tr = document.createElement("tr");
        tr.dataset.pairIdx = idx;
        if (showOnprem && !pair.matched) tr.className = "row-unmatched";
        if (showOnprem) {
            tr.innerHTML = renderOnpremCells(pair.onprem, flow.onpremBaseUrl)
                + `<td class="divider-col"></td>`
                + renderAacCells(pair.aac, flow.aacBaseUrl, idx);
        } else {
            tr.innerHTML = renderAacCells(pair.aac, flow.aacBaseUrl, idx);
        }
        resultsBody.appendChild(tr);
    });
}

/* --- On-Prem cells (6 cols) --- */
function renderOnpremCells(job, baseUrl) {
    if (!job) return `<td class="cell-empty">—</td>`.repeat(6);
    return _jobCoreCells(job, baseUrl);
}

/* --- AAC cells (7 cols: 6 core + DBX) --- */
function renderAacCells(job, baseUrl, pairIdx) {
    if (!job) return `<td class="cell-empty">—</td>`.repeat(7);
    let dbxCell;
    if (job.executionLanguage === "databricksSpark" && job.databricksJobId) {
        dbxCell = `<td class="dbx-cell"><button class="dbx-btn" data-pair="${pairIdx}" data-dbxid="${job.databricksJobId}" onclick="toggleDbxPanel(this)">DBX ▸</button></td>`;
    } else {
        dbxCell = `<td class="dbx-cell"></td>`;
    }
    return _jobCoreCells(job, baseUrl) + dbxCell;
}

/* --- Shared core cells (6 cols) --- */
function _jobCoreCells(job, baseUrl) {
    const runId = job.jobRunId ?? job.jobGroupId;
    let runIdCell;
    if (runId != null && baseUrl) {
        const jobUrl = `${baseUrl}/jobs/${runId}`;
        runIdCell = `<td><a href="${esc(jobUrl)}" target="_blank" rel="noopener" class="job-link">${runId}</a></td>`;
    } else {
        runIdCell = `<td>${runId ?? "—"}</td>`;
    }
    return `
        ${runIdCell}
        <td>${statusBadge(job.status)}</td>
        <td>${job.flowId ?? "—"}</td>
        <td>${esc(job.flowName || "—")}</td>
        <td>${formatDateTime(job.createdAt)}</td>
        <td>${job.executionTimeMinutes != null ? job.executionTimeMinutes + " min" : "—"}</td>
    `;
}

// ---- DBX Expandable Panel ---------------------------------
async function toggleDbxPanel(btn) {
    const pairIdx = btn.dataset.pair;
    const dbxId = btn.dataset.dbxid;
    const parentRow = btn.closest("tr");
    const existingDetail = parentRow.nextElementSibling;

    // Toggle off
    if (existingDetail && existingDetail.classList.contains("dbx-detail-row")) {
        existingDetail.remove();
        btn.textContent = "DBX ▸";
        btn.classList.remove("active");
        return;
    }

    // Toggle on — create detail row
    btn.textContent = "DBX ▾";
    btn.classList.add("active");
    const totalCols = (flows[activeFlowIndex]?.onpremEnabled !== false) ? 14 : 7;
    const detailRow = document.createElement("tr");
    detailRow.className = "dbx-detail-row";
    detailRow.innerHTML = `<td colspan="${totalCols}" class="dbx-detail-cell"><div class="dbx-loading"><span class="spinner"></span> Loading Databricks details…</div></td>`;
    parentRow.after(detailRow);

    // Look up the AAC jobRunId from the pair data
    const pair = flows[activeFlowIndex]?.pairs?.[pairIdx];
    const jobRunId = pair?.aac?.jobRunId || null;

    const flowName = flows[activeFlowIndex]?.name || "";

    try {
        const resp = await fetch(`${API_BASE}/api/databricks`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ databricksJobId: dbxId, flowName, jobRunId }),
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        data._jobRunId = jobRunId;  // attach for the download button
        data._flowName = flowName;
        detailRow.querySelector(".dbx-detail-cell").innerHTML = renderDbxPanel(data);
    } catch (e) {
        detailRow.querySelector(".dbx-detail-cell").innerHTML = `<div class="dbx-error">⚠ ${esc(e.message)}</div>`;
    }
}

function fmtDurationMs(ms) {
    if (ms == null) return "—";
    const totalSec = Math.round(ms / 1000);
    const m = Math.floor(totalSec / 60);
    const s = totalSec % 60;
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function renderDbxPanel(data) {
    const rd = data.runDetails || {};
    const events = data.clusterEvents || [];
    const host = (data.databricksHost || "").replace(/\/+$/, "");
    const clusterId = rd.clusterId || "";
    const jobRunId = data._jobRunId || "";
    const flowName = data._flowName || "";
    const isCached = data.cached === true;
    const flow = flows[activeFlowIndex];
    const isAnalyzed = flow?.analyzedJobs?.includes(String(jobRunId));

    let html = `<div class="dbx-panel">`;

    // Job name as clickable link to cluster page
    const title = esc(rd.jobName || "Databricks Run");
    const cachedLabel = isCached ? ` <span class="dbx-cached-label">(cached)</span>` : "";
    if (host && clusterId) {
        html += `<a class="dbx-title-link" href="${host}/compute/clusters/${esc(clusterId)}" target="_blank" rel="noopener">${title} ↗</a>${cachedLabel}`;
    } else {
        html += `<div class="dbx-title">${title}${cachedLabel}</div>`;
    }

    // Metric boxes row
    const setupMs = rd.timing?.setupDurationMs;
    const execMs = rd.timing?.executionDurationMs;
    const totalMs = (setupMs != null && execMs != null) ? setupMs + execMs : null;

    html += `<div class="dbx-info-row">`;
    html += `<span class="dbx-tag">Cluster: <strong>${esc(clusterId || "—")}</strong></span>`;
    if (rd.autoscale) html += `<span class="dbx-tag">Workers: <strong>${rd.autoscale.minWorkers}–${rd.autoscale.maxWorkers}</strong></span>`;
    if (rd.nodeTypeId) html += `<span class="dbx-tag">Node: <strong>${esc(rd.nodeTypeId)}</strong></span>`;
    html += `<span class="dbx-tag">Setup: <strong>${fmtDurationMs(setupMs)}</strong></span>`;
    html += `<span class="dbx-tag">Execution: <strong>${fmtDurationMs(execMs)}</strong></span>`;
    html += `<span class="dbx-tag dbx-tag-total">Total: <strong>${fmtDurationMs(totalMs)}</strong></span>`;
    html += `</div>`;

    // Event Log Download Button — or analysis status if already analyzed
    if (clusterId && jobRunId) {
        html += `<div class="dbx-eventlog-row" id="eventlog-row-${esc(jobRunId)}">`;
        if (isAnalyzed) {
            html += `<div class="eventlog-complete"><div class="eventlog-success-title">✅ Analysis Complete`;
            html += `<button class="dash-open-btn" onclick="viewDashboard('${esc(jobRunId)}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" width="14" height="14"><polyline points="9 18 15 12 9 6"/></svg> Dashboard</button>`;
            html += `</div></div>`;
        } else {
            html += `<button class="eventlog-btn" onclick="downloadEventLog('${esc(clusterId)}', '${esc(jobRunId)}')">`;
            html += `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" width="16" height="16"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>`;
            html += ` Download Event Log</button>`;
        }
        html += `</div>`;
    }

    // Two-column layout: events (left) + spark config (right)
    html += `<div class="dbx-columns">`;

    // LEFT: Cluster events
    html += `<div class="dbx-col-events">`;
    if (events.length > 0) {
        html += `<div class="dbx-section-label">Cluster Events (${events.length})</div>`;
        html += `<div class="dbx-timeline">`;
        events.forEach(ev => {
            const time = ev.isoTime ? formatDateTime(ev.isoTime) : "—";
            const evType = ev.eventType || "UNKNOWN";
            let detail = "";
            if (ev.details) {
                if (ev.details.currentWorkers != null && ev.details.targetWorkers != null) {
                    detail = `Workers: ${ev.details.currentWorkers} → ${ev.details.targetWorkers}`;
                } else if (ev.details.cause) {
                    detail = String(ev.details.cause);
                } else if (ev.details.reasonCode) {
                    detail = String(ev.details.reasonCode);
                }
            }
            html += `<div class="dbx-event">`;
            html += `<span class="dbx-event-time">${time}</span>`;
            html += `<span class="dbx-event-type dbx-evt-${evType.toLowerCase().replace(/_/g,'-')}">${esc(evType)}</span>`;
            if (detail) html += `<span class="dbx-event-detail">${esc(detail)}</span>`;
            html += `</div>`;
        });
        html += `</div>`;
    }
    html += `</div>`;

    // RIGHT: Spark config (collapsible)
    const confs = rd.sparkConf || {};
    const confKeys = Object.keys(confs);
    html += `<div class="dbx-col-config">`;
    if (confKeys.length > 0) {
        html += `<div class="dbx-section-label dbx-conf-toggle" onclick="this.parentElement.classList.toggle('dbx-conf-open')">`;
        html += `Spark Configuration (${confKeys.length}) <span class="dbx-conf-arrow">▸</span></div>`;
        html += `<div class="dbx-conf-body">`;
        html += `<table class="dbx-conf-table"><tbody>`;
        confKeys.sort().forEach(k => {
            html += `<tr><td class="dbx-conf-key">${esc(k)}</td><td class="dbx-conf-val">${esc(String(confs[k]))}</td></tr>`;
        });
        html += `</tbody></table>`;
        html += `</div>`;
    }
    html += `</div>`;

    html += `</div>`; // .dbx-columns
    html += `</div>`; // .dbx-panel
    return html;
}

// ---- Event Log Download -----------------------------------
async function downloadEventLog(clusterId, jobRunId) {
    const row = document.getElementById(`eventlog-row-${jobRunId}`);
    if (!row) return;

    const flowName = flows[activeFlowIndex]?.name || "";

    // Show loading state
    row.innerHTML = `<div class="eventlog-loading"><span class="spinner"></span> Downloading & analyzing event log…</div>`;

    try {
        const resp = await fetch(`${API_BASE}/api/eventlog`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ clusterId, jobRunId, flowName }),
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);

        const analysis = data.analysis || {};
        const summary = analysis.summary || {};
        const cached = data.cached ? " (cached)" : "";

        // Show success with key summary stats
        let html = `<div class="eventlog-complete">`;
        html += `<div class="eventlog-success-title">✅ Analysis Complete${cached}</div>`;
        html += `<div class="eventlog-summary-row">`;
        html += `<span class="dbx-tag">Stages: <strong>${summary.total_stages ?? "—"}</strong></span>`;
        html += `<span class="dbx-tag">Tasks: <strong>${summary.total_tasks ?? "—"}</strong></span>`;
        html += `<span class="dbx-tag">Peak Executors: <strong>${summary.peak_executors ?? "—"}</strong></span>`;
        if (summary.gc_pct_of_total_runtime != null) {
            const gcCls = summary.gc_pct_of_total_runtime > 10 ? "eventlog-warn" : "";
            html += `<span class="dbx-tag ${gcCls}">GC: <strong>${summary.gc_pct_of_total_runtime}%</strong></span>`;
        }
        if (summary.total_spill_disk_bytes > 0) {
            html += `<span class="dbx-tag eventlog-warn">Spill: <strong>${fmtBytes(summary.total_spill_disk_bytes)}</strong></span>`;
        }
        if (summary.shuffle_to_input_ratio != null) {
            html += `<span class="dbx-tag">Shuffle/Input: <strong>${summary.shuffle_to_input_ratio}x</strong></span>`;
        }
        html += `</div>`;
        html += `<button class="dash-open-btn" onclick="viewDashboard('${esc(jobRunId)}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" width="14" height="14"><polyline points="9 18 15 12 9 6"/></svg> Dashboard</button>`;
        html += `</div>`;
        row.innerHTML = html;

        // Update the flow's analyzedJobs list so button stays hidden on re-render
        const flow = flows[activeFlowIndex];
        if (flow && !flow.analyzedJobs?.includes(String(jobRunId))) {
            flow.analyzedJobs = flow.analyzedJobs || [];
            flow.analyzedJobs.push(String(jobRunId));
        }

    } catch (e) {
        row.innerHTML = `<div class="eventlog-error">⚠ ${esc(e.message)}</div>`;
    }
}

// ---- View Dashboard ---------------------------------------
async function viewDashboard(jobRunId) {
    const flowName = flows[activeFlowIndex]?.name || "";
    try {
        const url = flowName
            ? `${API_BASE}/api/eventlog/${encodeURIComponent(jobRunId)}?flowName=${encodeURIComponent(flowName)}`
            : `${API_BASE}/api/eventlog/${encodeURIComponent(jobRunId)}`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (data.error) { console.error(data.error); return; }
        openDashboard(data.analysis, jobRunId);
    } catch (e) {
        console.error("Failed to load analysis for dashboard:", e);
    }
}

function fmtBytes(bytes) {
    if (bytes == null || bytes === 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + " " + units[i];
}

function statusBadge(status) {
    if (!status) return `<span class="status-badge status-unknown">—</span>`;
    const s = status.toLowerCase();
    let cls = "status-unknown";
    if (s === "complete" || s === "completed") cls = "status-complete";
    else if (s === "failed" || s === "error") cls = "status-failed";
    else if (s === "running" || s === "inprogress" || s === "in progress") cls = "status-running";
    return `<span class="status-badge ${cls}">${esc(status)}</span>`;
}

function formatDateTime(iso) {
    if (!iso) return "—";
    try {
        const d = new Date(iso);
        const date = d.toLocaleDateString("en-GB", { day: "2-digit", month: "2-digit", year: "numeric" });
        const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
        return `${date} ${time}`;
    } catch { return iso; }
}

// ---- Settings Modal ---------------------------------------
const SETTINGS_CONFIG = [
    { section: "Alteryx Analytics Cloud", keys: [
        { key: "PLATFORM_API_BASE_URL", label: "Base URL", type: "text" },
        { key: "PLATFORM_API_TOKEN", label: "API Token", type: "password" },
    ]},
    { section: "On-Prem Trifacta", toggleKey: "ONPREM_ENABLED", keys: [
        { key: "ONPREM_API_BASE_URL", label: "Base URL", type: "text" },
        { key: "ONPREM_API_TOKEN", label: "API Token", type: "password" },
    ]},
    { section: "Databricks", keys: [
        { key: "DATABRICKS_HOST", label: "Host URL", type: "text" },
        { key: "DATABRICKS_TOKEN", label: "PAT Token", type: "password" },
    ]},
    { section: "Query Settings", keys: [
        { key: "DEFAULT_LIMIT", label: "Default Limit", type: "number" },
        { key: "RANFOR_FILTER", label: "Ranfor Filter", type: "text" },
        { key: "MATCH_WINDOW_MINUTES", label: "Match Window (minutes)", type: "number" },
    ]},
];

async function openSettings() {
    settingsModal.style.display = "flex";
    settingsBody.innerHTML = `<div class="loading"><div class="spinner"></div><span>Loading settings…</span></div>`;

    try {
        const res = await fetch(`${API_BASE}/api/config`);
        const config = await res.json();
        renderSettingsForm(config);
    } catch (err) {
        settingsBody.innerHTML = `<p style="color:var(--red)">Failed to load settings: ${esc(err.message)}</p>`;
    }
}

function renderSettingsForm(config) {
    let html = "";
    SETTINGS_CONFIG.forEach((section, idx) => {
        const hasToggle = !!section.toggleKey;
        const toggleVal = hasToggle ? (config[section.toggleKey]?.value || "true") : "true";
        const isEnabled = toggleVal.toLowerCase() === "true" || toggleVal === "1";
        const disabledClass = hasToggle && !isEnabled ? " settings-section-disabled" : "";

        html += `<div class="settings-section${disabledClass}" ${hasToggle ? `data-toggle-section="${section.toggleKey}"` : ""}>`;

        // Section title with optional toggle checkbox
        if (hasToggle) {
            html += `<div class="settings-section-title">`;
            html += `<label class="settings-toggle-label">`;
            html += `<input type="checkbox" class="settings-toggle-cb" data-toggle-key="${section.toggleKey}" ${isEnabled ? "checked" : ""} onchange="toggleSettingsSection(this)">`;
            html += ` ${esc(section.section)}`;
            html += `</label></div>`;
        } else {
            html += `<div class="settings-section-title">${esc(section.section)}</div>`;
        }

        html += `<div class="settings-section-fields">`;
        section.keys.forEach(({ key, label, type }) => {
            const val = config[key]?.value || "";
            const isMasked = config[key]?.masked || false;
            html += `<label for="cfg_${key}">${label}</label>`;
            if (type === "password") {
                html += `<div class="input-group">
                    <input type="password" id="cfg_${key}" value="${esc(val)}" placeholder="${isMasked ? "Enter new value to change" : ""}" data-key="${key}" data-masked="${isMasked}" ${hasToggle && !isEnabled ? "disabled" : ""}>
                    <button type="button" class="icon-btn icon-btn-sm toggle-vis" onclick="toggleVisibility('cfg_${key}')" title="Toggle visibility" aria-label="Toggle visibility">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                    </button>
                </div>`;
            } else {
                html += `<input type="${type}" id="cfg_${key}" value="${esc(val)}" data-key="${key}" ${hasToggle && !isEnabled ? "disabled" : ""}>`;
            }
        });
        html += `</div>`; // .settings-section-fields
        html += `</div>`; // .settings-section
    });
    settingsBody.innerHTML = html;
}

function toggleSettingsSection(cb) {
    const section = cb.closest(".settings-section");
    const isEnabled = cb.checked;
    section.classList.toggle("settings-section-disabled", !isEnabled);
    section.querySelectorAll(".settings-section-fields input").forEach(input => {
        input.disabled = !isEnabled;
    });
}

function toggleVisibility(inputId) {
    const input = document.getElementById(inputId);
    input.type = input.type === "password" ? "text" : "password";
}

function closeSettings() {
    settingsModal.style.display = "none";
}

async function saveSettings() {
    const inputs = settingsBody.querySelectorAll("input[data-key]");
    const updates = {};
    inputs.forEach(input => {
        const key = input.dataset.key;
        const val = input.value;
        // Don't send masked values back — user hasn't changed them
        if (input.dataset.masked === "true" && val.includes("••")) return;
        updates[key] = val;
    });

    // Include toggle checkboxes
    settingsBody.querySelectorAll(".settings-toggle-cb").forEach(cb => {
        updates[cb.dataset.toggleKey] = cb.checked ? "true" : "false";
    });

    try {
        const res = await fetch(`${API_BASE}/api/config`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(updates),
        });
        const data = await res.json();
        if (data.success) {
            closeSettings();
        }
    } catch (err) {
        alert(`Failed to save settings: ${err.message}`);
    }
}

// ---- Utilities --------------------------------------------
function esc(str) {
    if (str == null) return "";
    const div = document.createElement("div");
    div.textContent = String(str);
    return div.innerHTML;
}
