/* ============================================================
   Spark Performance Dashboard — Chart & Rendering Logic
   ============================================================
   Depends on: Chart.js (loaded via CDN), app.js (for esc(), fmtBytes())
   ============================================================ */

// Track active Chart.js instances for cleanup
let _dashCharts = [];
let _dashActive = false;

// Store current analysis for cross-chart interactions
let _dashAnalysis = null;

// Cached tuning rules (loaded once from tuning_rules.json)
let _tuningRules = null;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Open the dashboard panel with analysis data.
 * @param {Object} analysis — parsed analysis.json
 * @param {string} jobRunId — for display
 */
async function openDashboard(analysis, jobRunId) {
    const slider = document.getElementById("viewSlider");
    const panel = document.getElementById("dashboardPanel");
    const main = document.getElementById("mainContent");
    if (!slider || !panel) return;

    _dashActive = true;
    _dashAnalysis = analysis;
    main.classList.add("slider-active");

    // Load tuning rules once
    if (!_tuningRules) {
        try {
            const resp = await fetch("tuning_rules.json");
            _tuningRules = await resp.json();
        } catch (e) {
            console.warn("Could not load tuning_rules.json:", e);
            _tuningRules = [];
        }
    }

    // Build dashboard HTML
    panel.innerHTML = buildDashboardHTML(analysis, jobRunId);

    // Slide to dashboard
    slider.classList.add("slide-dashboard");

    // Render charts after transition settles
    requestAnimationFrame(() => {
        setTimeout(() => {
            renderExecutorTimeline(analysis);
            renderTaskDistribution(analysis);
            renderStageWaterfall(analysis);
            // Stage task bins — default to longest stage
            const stb = analysis.stage_task_bins || {};
            const defaultStage = stb.longest_stage_id;
            if (defaultStage != null) {
                renderStageTaskBins(analysis, defaultStage);
            }
        }, 100);
    });
}

/**
 * Slide back to main view.
 */
function closeDashboard() {
    const slider = document.getElementById("viewSlider");
    const main = document.getElementById("mainContent");
    if (!slider) return;

    slider.classList.remove("slide-dashboard");
    _dashActive = false;
    _dashAnalysis = null;

    // Destroy charts after transition
    setTimeout(() => {
        main.classList.remove("slider-active");
        _dashCharts.forEach(c => c.destroy());
        _dashCharts = [];
    }, 500);
}

// ---------------------------------------------------------------------------
// HTML Builder
// ---------------------------------------------------------------------------

function buildDashboardHTML(analysis, jobRunId) {
    const meta = analysis.metadata || {};
    const summary = analysis.summary || {};
    const config = analysis.config_snapshot || {};

    let html = `<div class="dash-container">`;

    // Header with back button
    html += `<div class="dash-header">`;
    html += `<div class="dash-header-left">`;
    html += `<button class="dash-back-btn" onclick="closeDashboard()">`;
    html += `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" width="16" height="16"><polyline points="15 18 9 12 15 6"/></svg>`;
    html += ` Back to Jobs</button>`;
    html += `<div>`;
    html += `<div class="dash-title">Spark Performance Dashboard</div>`;
    html += `<div class="dash-subtitle">Job Run ${esc(String(jobRunId))} · ${esc(meta.app_name || "")} · Spark ${esc(meta.spark_version || "")}</div>`;
    html += `</div>`;
    html += `</div>`;
    html += `<span class="dash-app-badge">${esc(meta.app_id || "")}</span>`;
    html += `</div>`;

    // KPI Cards
    html += buildKPICards(summary, config);

    // Two-column layout: charts (left) + tuning panel (right)
    html += `<div class="dash-main-layout">`;

    // LEFT: Charts column
    html += `<div class="dash-charts-col">`;
    html += `<div class="dash-charts-row">`;

    // 1. Executor Scaling Timeline (with pending tasks)
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Executor Scaling Timeline</div>`;
    html += `<canvas id="executorTimelineChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;

    // 2. Task Distribution & Core Parallelism
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Task Distribution & Core Parallelism</div>`;
    html += `<canvas id="taskDistributionChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;

    // 3. Stage Performance Breakdown (Waterfall)
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Stage Performance Breakdown</div>`;
    html += `<canvas id="stageWaterfallChart" class="dash-chart-canvas-tall"></canvas>`;
    html += `</div>`;

    // 4. Stage Task Breakdown (Binned)
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Stage Task Breakdown</div>`;
    html += `<div id="stageTaskBinsLabel" class="dash-chart-subtitle"></div>`;
    html += `<canvas id="stageTaskBinsChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;

    html += `</div>`; // .dash-charts-row

    // Red Flags
    html += buildRedFlags(analysis);

    html += `</div>`; // .dash-charts-col

    // RIGHT: Tuning Panel sidebar
    html += buildTuningPanel(analysis);

    html += `</div>`; // .dash-main-layout

    html += `</div>`; // .dash-container
    return html;
}

// ---------------------------------------------------------------------------
// Tuning Panel — KPIs + settings comparison table
// ---------------------------------------------------------------------------

function _formatBytes(bytes) {
    if (bytes == null) return "—";
    const n = Number(bytes);
    if (isNaN(n)) return String(bytes);
    if (n >= 1073741824) return (n / 1073741824).toFixed(1) + " GB";
    if (n >= 1048576) return (n / 1048576).toFixed(0) + " MB";
    if (n >= 1024) return (n / 1024).toFixed(0) + " KB";
    return n + " B";
}

function _computeSuggested(rule, ti) {
    try {
        const unified_gb = ti.unified_memory_gb || 0;
        const unified_mb = ti.unified_memory_mb || 0;
        const per_core_gb = ti.per_core_gb || 0;
        const cores = ti.cores_per_executor || 1;
        // eslint-disable-next-line no-eval
        return String(eval(rule.compute));
    } catch (e) {
        return "—";
    }
}

function _displayValue(raw, isBytes) {
    if (raw == null || raw === "") return "—";
    const s = String(raw);
    if (isBytes) {
        const n = Number(s);
        if (!isNaN(n) && n > 1024) return _formatBytes(n);
    }
    return s;
}

function buildTuningPanel(analysis) {
    const ti = analysis.tuning_inputs || {};
    const config = analysis.config_snapshot || {};
    const rules = _tuningRules || [];

    let html = `<div class="dash-tuning-panel">`;

    // Title
    html += `<div class="tuning-panel-title">Suggested Tuning</div>`;

    // KPI cards
    html += `<div class="tuning-kpis">`;

    // 1. Unified Memory
    const unifiedGb = ti.unified_memory_gb != null ? ti.unified_memory_gb.toFixed(2) : "—";
    html += `<div class="tuning-kpi">`;
    html += `<div class="tuning-kpi-value">${unifiedGb}<span class="tuning-kpi-unit">GB</span></div>`;
    html += `<div class="tuning-kpi-label">Unified Memory</div>`;
    html += `<div class="tuning-kpi-detail">${ti.executor_memory_mb || 0} MB heap + ${ti.executor_offheap_mb || 0} MB off-heap</div>`;
    html += `</div>`;

    // 2. Cores per Executor
    const cores = ti.cores_per_executor != null ? ti.cores_per_executor : "—";
    html += `<div class="tuning-kpi">`;
    html += `<div class="tuning-kpi-value">${cores}</div>`;
    html += `<div class="tuning-kpi-label">Cores / Executor</div>`;
    html += `</div>`;

    // 3. Memory per Core
    const perCore = ti.per_core_gb != null ? ti.per_core_gb.toFixed(2) : "—";
    html += `<div class="tuning-kpi">`;
    html += `<div class="tuning-kpi-value">${perCore}<span class="tuning-kpi-unit">GB</span></div>`;
    html += `<div class="tuning-kpi-label">Memory / Core</div>`;
    html += `</div>`;

    html += `</div>`; // .tuning-kpis

    // Settings table — split by section
    const sections = [
        { label: "Node-Specific Settings", key: "node" },
        { label: "Cluster-Wide Settings", key: "cluster" },
    ];

    for (const section of sections) {
        const sectionRules = rules.filter(r => r.section === section.key);
        if (sectionRules.length === 0) continue;

        html += `<div class="tuning-section-label">${section.label}</div>`;
        html += `<table class="tuning-table"><tbody>`;

        for (const rule of sectionRules) {
            const currentRaw = config[rule.key];
            const suggested = _computeSuggested(rule, ti);
            const isBytesSetting = rule.key.includes("Bytes") || rule.key.includes("Threshold");

            const currentDisplay = currentRaw != null ? _displayValue(currentRaw, isBytesSetting) : `<span class="tuning-default">${rule.defaultLabel || rule.default}</span>`;
            const suggestedDisplay = _displayValue(suggested, isBytesSetting);

            // Determine if current matches suggested
            const currentNorm = currentRaw != null ? String(currentRaw).toLowerCase().trim() : String(rule.default).toLowerCase().trim();
            const suggestedNorm = String(suggested).toLowerCase().trim();
            const matches = currentNorm === suggestedNorm;

            const rowClass = matches ? "" : " tuning-row-diff";
            const matchIcon = matches ? `<span class="tuning-match">✓</span>` : `<span class="tuning-diff">✗</span>`;

            // Short display name: strip spark.sql. / spark. prefix
            const shortKey = rule.key
                .replace(/^spark\.sql\.adaptive\./, "…adaptive.")
                .replace(/^spark\.sql\./, "…sql.")
                .replace(/^spark\.dynamicAllocation\./, "…dynAlloc.")
                .replace(/^spark\./, "…");

            html += `<tr class="tuning-row${rowClass}" title="${esc(rule.description)}">`;
            html += `<td class="tuning-key">${esc(shortKey)}</td>`;
            html += `<td class="tuning-current">${currentDisplay}</td>`;
            html += `<td class="tuning-suggested">${suggestedDisplay}</td>`;
            html += `<td class="tuning-status">${matchIcon}</td>`;
            html += `</tr>`;
            // Formula row (shown on hover via CSS)
            html += `<tr class="tuning-formula-row"><td colspan="4" class="tuning-formula">${esc(rule.formula)}</td></tr>`;
        }
        html += `</tbody></table>`;
    }

    html += `</div>`; // .dash-tuning-panel
    return html;
}

// ---------------------------------------------------------------------------
// KPI Cards
// ---------------------------------------------------------------------------

function buildKPICards(summary, config) {
    const cards = [];

    // Stages & Tasks
    cards.push({ label: "Total Stages", value: summary.total_stages ?? "—" });
    cards.push({ label: "Total Tasks", value: summary.total_tasks ?? "—" });

    // Failed tasks
    const failedTasks = summary.total_failed_tasks || 0;
    cards.push({
        label: "Failed Tasks", value: failedTasks,
        cls: failedTasks > 0 ? "kpi-critical" : "kpi-good",
    });

    // Peak Executors
    cards.push({ label: "Peak Executors", value: summary.peak_executors ?? "—" });

    // GC %
    const gcPct = summary.gc_pct_of_total_runtime;
    let gcCls = "";
    if (gcPct != null) {
        if (gcPct > 15) gcCls = "kpi-critical";
        else if (gcPct > 8) gcCls = "kpi-warn";
        else gcCls = "kpi-good";
    }
    cards.push({ label: "GC Overhead", value: gcPct != null ? `${gcPct}%` : "—", cls: gcCls });

    // Spill
    const spillDisk = summary.total_spill_disk_bytes || 0;
    cards.push({
        label: "Disk Spill",
        value: spillDisk > 0 ? fmtBytes(spillDisk) : "None",
        cls: spillDisk > 0 ? "kpi-critical" : "kpi-good",
    });

    // Shuffle / Input ratio
    const shuffleRatio = summary.shuffle_to_input_ratio;
    cards.push({ label: "Shuffle / Input", value: shuffleRatio != null ? `${shuffleRatio}x` : "—" });

    // I/O
    const inputBytes = summary.total_input_bytes || 0;
    const outputBytes = summary.total_output_bytes || 0;
    cards.push({ label: "Input", value: fmtBytes(inputBytes) });
    cards.push({ label: "Output", value: fmtBytes(outputBytes) });

    // Longest stage
    const longest = summary.longest_stage;
    if (longest) {
        cards.push({
            label: "Longest Stage",
            value: fmtDurationMs(longest.duration_ms),
            cls: longest.duration_ms > 120000 ? "kpi-warn" : "",
        });
    }

    let html = `<div class="dash-kpi-grid">`;
    cards.forEach(c => {
        html += `<div class="dash-kpi-card ${c.cls || ""}">`;
        html += `<div class="dash-kpi-label">${esc(c.label)}</div>`;
        html += `<div class="dash-kpi-value">${esc(String(c.value))}</div>`;
        html += `</div>`;
    });
    html += `</div>`;
    return html;
}

// ---------------------------------------------------------------------------
// 1. Executor Scaling Timeline (with Pending Tasks)
// ---------------------------------------------------------------------------

function renderExecutorTimeline(analysis) {
    const canvas = document.getElementById("executorTimelineChart");
    if (!canvas) return;

    const startTime = analysis.metadata?.start_time || 0;
    const events = (analysis.executor_timeline || []).filter(
        e => e.event === "added" || e.event === "removed"
    );

    // Build step data — running count of active executors
    let count = 0;
    const executorPoints = [{ x: 0, y: 0 }];

    events.forEach(ev => {
        const secFromStart = (ev.timestamp - startTime) / 1000;
        if (ev.event === "added") {
            count++;
        } else if (ev.event === "removed") {
            count = Math.max(0, count - 1);
        }
        executorPoints.push({ x: Math.round(secFromStart * 10) / 10, y: count });
    });

    // Add endpoint if we have stage data
    const stages = analysis.stages || [];
    if (stages.length > 0) {
        const lastStage = stages[stages.length - 1];
        if (lastStage.completion_time_iso) {
            const endTs = new Date(lastStage.completion_time_iso).getTime();
            const endSec = (endTs - startTime) / 1000;
            if (endSec > executorPoints[executorPoints.length - 1].x) {
                executorPoints.push({ x: Math.round(endSec * 10) / 10, y: count });
            }
        }
    }

    // Build pending tasks data
    const pendingTimeline = analysis.pending_task_timeline || [];
    const pendingPoints = pendingTimeline.map(pt => ({
        x: Math.round(((pt.timestamp - startTime) / 1000) * 10) / 10,
        y: pt.pending,
    }));

    const chart = new Chart(canvas, {
        type: "line",
        data: {
            datasets: [
                {
                    label: "Active Executors",
                    data: executorPoints,
                    borderColor: "#6c7aff",
                    backgroundColor: "rgba(108, 122, 255, 0.12)",
                    fill: true,
                    stepped: true,
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: "#6c7aff",
                    pointBorderColor: "#1c1f2e",
                    pointBorderWidth: 2,
                    tension: 0,
                    yAxisID: "y",
                },
                {
                    label: "Pending Tasks",
                    data: pendingPoints,
                    borderColor: "#f59e0b",
                    backgroundColor: "rgba(245, 158, 11, 0.08)",
                    fill: true,
                    stepped: false,
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    yAxisID: "y1",
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: "index",
                intersect: false,
            },
            scales: {
                x: {
                    type: "linear",
                    title: { display: true, text: "Seconds from App Start", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 } },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y: {
                    position: "left",
                    beginAtZero: true,
                    title: { display: true, text: "Executor Count", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 }, stepSize: 1 },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y1: {
                    position: "right",
                    beginAtZero: true,
                    title: { display: true, text: "Pending Tasks", color: "#f59e0b", font: { size: 11 } },
                    ticks: { color: "#f59e0b", font: { size: 10 } },
                    grid: { drawOnChartArea: false },
                },
            },
            plugins: {
                legend: {
                    display: true,
                    labels: { color: "#9498ab", font: { size: 11 }, boxWidth: 12, padding: 16 },
                },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        title: (items) => `${items[0].parsed.x}s from start`,
                    },
                },
            },
        },
    });
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// 2. Task Distribution & Core Parallelism
// ---------------------------------------------------------------------------

function renderTaskDistribution(analysis) {
    const canvas = document.getElementById("taskDistributionChart");
    if (!canvas) return;

    const dist = analysis.executor_task_distribution || [];
    if (dist.length === 0) return;

    const labels = dist.map(d => `Exec ${d.executor_id}`);
    const tasksData = dist.map(d => d.tasks_processed);
    const coresData = dist.map(d => d.avg_active_cores);

    // Custom plugin: draws a horizontal line across each bar's width
    const coresLinePlugin = {
        id: "coresLine",
        afterDatasetsDraw(chart) {
            const { ctx } = chart;
            const meta = chart.getDatasetMeta(0); // the bar dataset
            const yScale = chart.scales.y1;
            if (!meta || !yScale) return;

            ctx.save();
            ctx.strokeStyle = "#6c7aff";
            ctx.lineWidth = 3;
            ctx.setLineDash([]);

            meta.data.forEach((bar, i) => {
                const coreVal = coresData[i];
                if (coreVal == null) return;
                const yPos = yScale.getPixelForValue(coreVal);
                const halfWidth = bar.width / 2;
                ctx.beginPath();
                ctx.moveTo(bar.x - halfWidth, yPos);
                ctx.lineTo(bar.x + halfWidth, yPos);
                ctx.stroke();

                // Small dot at center for emphasis
                ctx.fillStyle = "#6c7aff";
                ctx.beginPath();
                ctx.arc(bar.x, yPos, 4, 0, Math.PI * 2);
                ctx.fill();
            });
            ctx.restore();
        },
    };

    const chart = new Chart(canvas, {
        type: "bar",
        data: {
            labels,
            datasets: [
                {
                    label: "Tasks Processed",
                    data: tasksData,
                    backgroundColor: "#34d399",
                    borderRadius: 4,
                    yAxisID: "y",
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    ticks: { color: "#9498ab", font: { size: 11, weight: "600" } },
                    grid: { display: false },
                },
                y: {
                    position: "left",
                    beginAtZero: true,
                    title: { display: true, text: "Total Tasks", color: "#34d399", font: { size: 11 } },
                    ticks: { color: "#34d399", font: { size: 10 } },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y1: {
                    position: "right",
                    beginAtZero: true,
                    suggestedMax: Math.max(...coresData) + 1,
                    title: { display: true, text: "Avg Active Cores", color: "#6c7aff", font: { size: 11 } },
                    ticks: { color: "#6c7aff", font: { size: 10 }, stepSize: 1 },
                    grid: { drawOnChartArea: false },
                },
            },
            plugins: {
                legend: {
                    position: "top",
                    labels: {
                        color: "#9498ab", font: { size: 11 }, boxWidth: 12, padding: 16,
                        generateLabels: (chart) => {
                            const defaultLabels = Chart.defaults.plugins.legend.labels.generateLabels(chart);
                            defaultLabels.push({
                                text: "Avg Active Cores",
                                fillStyle: "#6c7aff",
                                strokeStyle: "#6c7aff",
                                lineWidth: 3,
                                hidden: false,
                            });
                            return defaultLabels;
                        },
                    },
                },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        afterBody: (items) => {
                            const idx = items[0]?.dataIndex;
                            if (idx != null && coresData[idx] != null) {
                                return `Avg Active Cores: ${coresData[idx]}`;
                            }
                            return "";
                        },
                    },
                },
            },
        },
        plugins: [coresLinePlugin],
    });
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// 3. Stage Performance Breakdown — Waterfall Timeline
// ---------------------------------------------------------------------------

function renderStageWaterfall(analysis) {
    const canvas = document.getElementById("stageWaterfallChart");
    if (!canvas) return;

    const stages = analysis.stages || [];
    if (stages.length === 0) return;

    const startTime = analysis.metadata?.start_time || 0;

    const labels = stages.map(s => `S${s.stage_id}`);

    // For each stage, compute start/end offset in seconds and segment breakdown
    const computeBars = [];
    const gcBars = [];
    const delayBars = [];

    stages.forEach(s => {
        // Parse stage submission/completion to seconds from app start
        const subIso = s.submission_time_iso;
        const compIso = s.completion_time_iso;
        if (!subIso || !compIso) {
            computeBars.push([0, 0]);
            gcBars.push([0, 0]);
            delayBars.push([0, 0]);
            return;
        }

        const subSec = (new Date(subIso).getTime() - startTime) / 1000;
        const compSec = (new Date(compIso).getTime() - startTime) / 1000;

        const schedulingDelaySec = (s.scheduling_delay_ms || 0) / 1000;
        const gcSec = (s.task_summary?.gc_time_ms?.total || 0) / 1000;
        const totalRunSec = (s.task_summary?.run_time_ms?.total || 0) / 1000;
        const computeSec = Math.max(0, totalRunSec - gcSec);

        // Segments stacked within the stage's time window
        const stageSpan = compSec - subSec;
        const rawTotal = schedulingDelaySec + computeSec + gcSec;

        // Scale segments to fit within the visible stage span
        const scale = rawTotal > 0 ? stageSpan / rawTotal : 1;

        const delayEnd = subSec + schedulingDelaySec * scale;
        const computeEnd = delayEnd + computeSec * scale;
        const gcEnd = computeEnd + gcSec * scale;

        delayBars.push([subSec, delayEnd]);
        computeBars.push([delayEnd, computeEnd]);
        gcBars.push([computeEnd, gcEnd]);
    });

    const chart = new Chart(canvas, {
        type: "bar",
        data: {
            labels,
            datasets: [
                {
                    label: "Scheduling Delay",
                    data: delayBars,
                    backgroundColor: "#fbbf24",
                    borderRadius: 2,
                    barThickness: 30,
                },
                {
                    label: "Compute",
                    data: computeBars,
                    backgroundColor: "#34d399",
                    borderRadius: 2,
                    barThickness: 30,
                },
                {
                    label: "GC Time",
                    data: gcBars,
                    backgroundColor: "#f87171",
                    borderRadius: 2,
                    barThickness: 30,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: "y",
            scales: {
                x: {
                    type: "linear",
                    title: { display: true, text: "Seconds from App Start", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 } },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y: {
                    stacked: true,
                    ticks: { color: "#9498ab", font: { size: 11, weight: "600" } },
                    grid: { display: false },
                },
            },
            plugins: {
                legend: {
                    position: "top",
                    labels: { color: "#9498ab", font: { size: 11 }, boxWidth: 12, padding: 16 },
                },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        afterTitle: (items) => {
                            const idx = items[0].dataIndex;
                            const stage = stages[idx];
                            return stage ? stage.stage_name.substring(0, 50) : "";
                        },
                        label: (item) => {
                            const range = item.raw;
                            if (Array.isArray(range)) {
                                const dur = range[1] - range[0];
                                return ` ${item.dataset.label}: ${dur.toFixed(1)}s`;
                            }
                            return ` ${item.dataset.label}: ${item.raw}`;
                        },
                    },
                },
            },
            onClick: (_event, elements) => {
                if (elements.length > 0 && _dashAnalysis) {
                    const idx = elements[0].index;
                    const stage = stages[idx];
                    if (stage) {
                        renderStageTaskBins(_dashAnalysis, stage.stage_id);
                    }
                }
            },
        },
    });
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// 4. Stage Task Breakdown (Binned)
// ---------------------------------------------------------------------------

function renderStageTaskBins(analysis, stageId) {
    const canvas = document.getElementById("stageTaskBinsChart");
    const label = document.getElementById("stageTaskBinsLabel");
    if (!canvas) return;

    const stb = analysis.stage_task_bins || {};
    const stageBins = (stb.stages || {})[String(stageId)];
    if (!stageBins || stageBins.length === 0) {
        if (label) label.textContent = `Stage ${stageId} — no task data`;
        return;
    }

    // Find the stage name
    const stages = analysis.stages || [];
    const stageInfo = stages.find(s => s.stage_id === stageId);
    const stageName = stageInfo ?
        `Stage ${stageId}: ${stageInfo.stage_name?.substring(0, 60) || ""}` :
        `Stage ${stageId}`;
    if (label) label.textContent = stageName;

    const binLabels = stageBins.map(b => b.label);
    // Convert ms to seconds for compute; spill stays in bytes for tooltip
    const computeData = stageBins.map(b => Math.max(0, (b.avg_duration_ms - b.avg_gc_ms)) / 1000);
    const gcData = stageBins.map(b => b.avg_gc_ms / 1000);
    const spillData = stageBins.map(b => b.avg_spill_bytes);

    // Destroy existing task bins chart if present
    const existingIdx = _dashCharts.findIndex(c => c.canvas === canvas);
    if (existingIdx !== -1) {
        _dashCharts[existingIdx].destroy();
        _dashCharts.splice(existingIdx, 1);
    }

    const chart = new Chart(canvas, {
        type: "bar",
        data: {
            labels: binLabels,
            datasets: [
                {
                    label: "Compute Time",
                    data: computeData,
                    backgroundColor: "#34d399",
                    borderRadius: 2,
                    yAxisID: "y",
                },
                {
                    label: "GC Time",
                    data: gcData,
                    backgroundColor: "#f87171",
                    borderRadius: 2,
                    yAxisID: "y",
                },
                {
                    label: "Disk Spill",
                    data: spillData,
                    backgroundColor: "#fbbf24",
                    borderRadius: 2,
                    yAxisID: "y1",
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    stacked: true,
                    ticks: { color: "#9498ab", font: { size: 10 } },
                    grid: { display: false },
                },
                y: {
                    stacked: true,
                    position: "left",
                    beginAtZero: true,
                    title: { display: true, text: "Duration (seconds)", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 } },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y1: {
                    stacked: true,
                    position: "right",
                    beginAtZero: true,
                    title: { display: true, text: "Disk Spill (bytes)", color: "#fbbf24", font: { size: 11 } },
                    ticks: {
                        color: "#fbbf24",
                        font: { size: 10 },
                        callback: (val) => fmtBytes(val),
                    },
                    grid: { drawOnChartArea: false },
                },
            },
            plugins: {
                legend: {
                    position: "top",
                    labels: { color: "#9498ab", font: { size: 11 }, boxWidth: 12, padding: 16 },
                },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        label: (item) => {
                            if (item.dataset.label === "Disk Spill") {
                                return ` ${item.dataset.label}: ${fmtBytes(item.raw)}`;
                            }
                            return ` ${item.dataset.label}: ${item.raw.toFixed(2)}s`;
                        },
                    },
                },
            },
        },
    });
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// Red Flags
// ---------------------------------------------------------------------------

function buildRedFlags(analysis) {
    const flags = [];
    const stages = analysis.stages || [];
    const timeline = analysis.executor_timeline || [];
    const config = analysis.config_snapshot || {};
    const summary = analysis.summary || {};

    // 1. Infrastructure Bottleneck — scheduling_delay > compute_time
    stages.forEach(s => {
        const totalRunMs = s.task_summary?.run_time_ms?.total || 0;
        const gcMs = s.task_summary?.gc_time_ms?.total || 0;
        const computeMs = totalRunMs - gcMs;
        const schedulingDelay = s.scheduling_delay_ms || 0;

        if (schedulingDelay > computeMs && schedulingDelay > 1000) {
            flags.push({
                severity: "critical",
                icon: "🚨",
                title: `Infrastructure Bottleneck — Stage ${s.stage_id}`,
                desc: `Scheduling delay (${fmtDurationMs(schedulingDelay)}) exceeds compute time (${fmtDurationMs(computeMs)}). The cluster may be too slow to scale or tasks are queued waiting for resources.`,
            });
        }
    });

    // 2. Over-partitioning
    const shufflePartitions = parseInt(config["spark.sql.shuffle.partitions"] || "200");
    const totalInput = summary.total_input_bytes || 0;
    if (totalInput > 0 && shufflePartitions > 1) {
        const perPartitionMB = (totalInput / shufflePartitions) / (1024 * 1024);
        if (perPartitionMB < 10) {
            flags.push({
                severity: "warning",
                icon: "⚠️",
                title: "Potential Over-partitioning",
                desc: `With <code>spark.sql.shuffle.partitions=${shufflePartitions}</code> and ${fmtBytes(totalInput)} input, each partition averages only ${perPartitionMB.toFixed(1)} MB — well below the recommended 128 MB target. Consider reducing partition count or enabling AQE coalescing.`,
            });
        }
    }

    // 3. Preemption Risk — "worker lost" removals
    const lostEvents = timeline.filter(e => e.event === "removed" && e.reason === "worker lost");
    if (lostEvents.length > 0) {
        flags.push({
            severity: "critical",
            icon: "💀",
            title: `Preemption Risk — ${lostEvents.length} executor(s) lost`,
            desc: `${lostEvents.length} executor(s) removed with reason "worker lost". This may indicate spot/preemptible instance eviction or OOM kills. Check cluster event logs for details.`,
        });
    }

    // 4. High GC overhead per-stage
    stages.forEach(s => {
        const gcPct = s.task_summary?.gc_pct_of_runtime || 0;
        if (gcPct > 20) {
            flags.push({
                severity: "warning",
                icon: "🗑️",
                title: `High GC Overhead — Stage ${s.stage_id} (${gcPct}%)`,
                desc: `GC time is ${gcPct}% of task runtime in this stage. Consider increasing <code>spark.executor.memory</code> or <code>spark.memory.fraction</code>.`,
            });
        }
    });

    // Render
    let html = `<div class="dash-flags-section">`;
    html += `<div class="dash-flags-title">Performance Red Flags</div>`;

    if (flags.length === 0) {
        html += `<div class="dash-no-flags">✅ No performance red flags detected</div>`;
    } else {
        html += `<div class="dash-flags-grid">`;
        flags.forEach(f => {
            const cls = f.severity === "critical" ? "" : "flag-warning";
            html += `<div class="dash-flag-card ${cls}">`;
            html += `<span class="dash-flag-icon">${f.icon}</span>`;
            html += `<div class="dash-flag-body">`;
            html += `<div class="dash-flag-title">${esc(f.title)}</div>`;
            html += `<div class="dash-flag-desc">${f.desc}</div>`; // HTML allowed (contains <code>)
            html += `</div></div>`;
        });
        html += `</div>`;
    }
    html += `</div>`;
    return html;
}
