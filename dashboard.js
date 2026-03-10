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

// Track which tab opened the dashboard so we can close to the right place
let _dashOrigin = null; // { slider, panel, main }

// References for synced zoom/pan between executor and stage charts
let _execTimelineChart = null;
let _stageTimelineChart = null;
let _zoomEnabled = false;
let _timelineMin = null; // shared x-axis min (ms timestamp)
let _timelineMax = null; // shared x-axis max (ms timestamp)

// Toggle zoom/pan on both timeline charts
function toggleDashZoom(cb) {
    _zoomEnabled = cb.checked;
    // Pan and zoom are always "enabled" in config but gated by
    // onPanStart / onZoomStart returning false when the checkbox is off.
    // No chart update needed — the callbacks check _zoomEnabled live.
}

// ---------------------------------------------------------------------------
// Helpers — find the correct slider elements for the active tab
// ---------------------------------------------------------------------------

function _getDashElements() {
    // Check which tab is active
    const ajTab = document.getElementById("tabAllJobs");
    if (ajTab && ajTab.classList.contains("active")) {
        return {
            slider: document.getElementById("ajViewSlider"),
            panel: document.getElementById("ajDashboardPanel"),
            main: document.getElementById("allJobsContent"),
        };
    }
    // Default: Flow Analysis tab
    return {
        slider: document.getElementById("viewSlider"),
        panel: document.getElementById("dashboardPanel"),
        main: document.getElementById("mainContent"),
    };
}

// ---------------------------------------------------------------------------
// Compute shared min/max time range for executor + stage timeline charts
// ---------------------------------------------------------------------------

function _computeTimelineRange(analysis) {
    const startTime = analysis.metadata?.start_time || 0;
    let minTs = startTime;
    let maxTs = startTime;

    // Executor events
    const events = analysis.executor_timeline || [];
    events.forEach(ev => {
        if (ev.timestamp < minTs) minTs = ev.timestamp;
        if (ev.timestamp > maxTs) maxTs = ev.timestamp;
    });

    // Pending task timeline
    const pending = analysis.pending_task_timeline || [];
    pending.forEach(pt => {
        if (pt.timestamp < minTs) minTs = pt.timestamp;
        if (pt.timestamp > maxTs) maxTs = pt.timestamp;
    });

    // Stage submission/completion
    const stages = analysis.stages || [];
    stages.forEach(s => {
        if (s.submission_time_iso) {
            const t = new Date(s.submission_time_iso).getTime();
            if (t < minTs) minTs = t;
            if (t > maxTs) maxTs = t;
        }
        if (s.completion_time_iso) {
            const t = new Date(s.completion_time_iso).getTime();
            if (t < minTs) minTs = t;
            if (t > maxTs) maxTs = t;
        }
    });

    // Add 2% padding
    const span = maxTs - minTs;
    _timelineMin = minTs - span * 0.02;
    _timelineMax = maxTs + span * 0.02;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Open the dashboard panel with analysis data.
 * @param {Object} analysis — parsed analysis.json
 * @param {string} jobRunId — for display
 */
async function openDashboard(analysis, jobRunId) {
    const els = _getDashElements();
    if (!els.slider || !els.panel) return;

    _dashOrigin = els;
    _dashActive = true;
    _dashAnalysis = analysis;
    els.main.classList.add("slider-active");

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
    els.panel.innerHTML = buildDashboardHTML(analysis, jobRunId);

    // Slide to dashboard
    els.slider.classList.add("slide-dashboard");

    // Render charts after transition settles
    requestAnimationFrame(() => {
        setTimeout(() => {
            // Compute shared time range for both timeline charts
            _computeTimelineRange(analysis);
            renderExecutorTimeline(analysis);
            renderTaskDistribution(analysis);
            renderStageWaterfall(analysis);
            // Partition Size & Duration — default to longest stage
            const stb = analysis.stage_task_bins || {};
            const defaultStage = stb.longest_stage_id;
            if (defaultStage != null) {
                renderPartitionSizeChart(analysis, defaultStage);
            }
        }, 100);
    });
}

/**
 * Slide back to main view.
 */
function closeDashboard() {
    const els = _dashOrigin || _getDashElements();
    if (!els.slider) return;

    els.slider.classList.remove("slide-dashboard");
    _dashActive = false;
    _dashAnalysis = null;

    // Destroy charts after transition
    setTimeout(() => {
        els.main.classList.remove("slider-active");
        _dashCharts.forEach(c => c.destroy());
        _dashCharts = [];
        _execTimelineChart = null;
        _stageTimelineChart = null;
        _zoomEnabled = false;
        _timelineMin = null;
        _timelineMax = null;
        _dashOrigin = null;
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

    // ── Full-width Event Timeline section ──
    html += `<div class="dash-timeline-section">`;
    html += `<div class="dash-timeline-header">`;
    html += `<div class="dash-chart-title" style="margin-bottom:0">Event Timeline</div>`;
    html += `<label class="dash-zoom-toggle"><input type="checkbox" id="dashZoomToggle" onchange="toggleDashZoom(this)"> Enable zooming</label>`;
    html += `</div>`;

    // Executor Scaling Timeline
    html += `<div class="dash-timeline-chart-wrap">`;
    html += `<div class="dash-timeline-label">Executors</div>`;
    html += `<canvas id="executorTimelineChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;

    // Stage Performance Breakdown (directly below, shared time axis)
    html += `<div class="dash-timeline-chart-wrap">`;
    html += `<div class="dash-timeline-label">Stages</div>`;
    html += `<canvas id="stageWaterfallChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;

    // Partition Size & Duration
    html += `<div class="dash-partition-chart-wrap" style="margin-top: 24px; padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.06);">`;
    html += `<div class="dash-chart-title">Partition Size & Duration</div>`;
    html += `<div id="partitionSizeLabel" class="dash-chart-subtitle"></div>`;
    html += `<canvas id="partitionSizeChart" class="dash-chart-canvas" style="height: 250px;"></canvas>`;
    html += `</div>`;

    html += `</div>`; // .dash-timeline-section

    // ── Two-column layout: remaining charts + tuning panel ──
    html += `<div class="dash-main-layout">`;

    // LEFT: Charts column
    html += `<div class="dash-charts-col">`;
    html += `<div class="dash-charts-row">`;

    // Task Distribution & Core Parallelism
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Task Distribution & Core Parallelism</div>`;
    html += `<canvas id="taskDistributionChart" class="dash-chart-canvas"></canvas>`;
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

    // Build step data — running count of active executors, using real timestamps
    let count = 0;
    const executorPoints = [{ x: startTime, y: 0 }];

    events.forEach(ev => {
        if (ev.event === "added") {
            count++;
        } else if (ev.event === "removed") {
            count = Math.max(0, count - 1);
        }
        executorPoints.push({ x: ev.timestamp, y: count });
    });

    // Add endpoint if we have stage data
    const stages = analysis.stages || [];
    if (stages.length > 0) {
        const lastStage = stages[stages.length - 1];
        if (lastStage.completion_time_iso) {
            const endTs = new Date(lastStage.completion_time_iso).getTime();
            if (endTs > executorPoints[executorPoints.length - 1].x) {
                executorPoints.push({ x: endTs, y: count });
            }
        }
    }

    // Build pending tasks data using real timestamps
    const pendingTimeline = analysis.pending_task_timeline || [];
    const pendingPoints = pendingTimeline.map(pt => ({
        x: pt.timestamp,
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
                    backgroundColor: "rgba(245, 158, 11, 0.06)",
                    fill: true,
                    stepped: false,
                    borderWidth: 1.5,
                    pointRadius: 1,
                    pointHoverRadius: 4,
                    pointBackgroundColor: "#f59e0b",
                    tension: 0,
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
                    type: "time",
                    time: {
                        displayFormats: {
                            second: "HH:mm:ss",
                            minute: "HH:mm",
                            hour: "HH:mm",
                        },
                        tooltipFormat: "yyyy/MM/dd HH:mm:ss",
                    },
                    // Hide x-axis labels — the stage chart below shows the shared axis
                    ticks: { display: false },
                    grid: { color: "rgba(255,255,255,0.04)", drawTicks: false },
                    // Shared min/max so grid lines align with stage chart
                    min: _timelineMin,
                    max: _timelineMax,
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
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: "x",
                        onPanStart: () => _zoomEnabled,
                        onPan: ({ chart: src }) => {
                            if (_stageTimelineChart && src !== _stageTimelineChart) {
                                _stageTimelineChart.options.scales.x.min = src.scales.x.min;
                                _stageTimelineChart.options.scales.x.max = src.scales.x.max;
                                _stageTimelineChart.update('none');
                            }
                        },
                    },
                    zoom: {
                        wheel: { enabled: true },
                        pinch: { enabled: true },
                        mode: "x",
                        onZoomStart: () => _zoomEnabled,
                        onZoom: ({ chart: src }) => {
                            if (_stageTimelineChart && src !== _stageTimelineChart) {
                                _stageTimelineChart.options.scales.x.min = src.scales.x.min;
                                _stageTimelineChart.options.scales.x.max = src.scales.x.max;
                                _stageTimelineChart.update('none');
                            }
                        },
                    },
                },
            },
        },
    });
    _execTimelineChart = chart;
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

    // ── Pack stages into compact rows (non-overlapping stages share rows) ──
    // Each stage occupies [subTs, compTs]. We greedily assign to the first row
    // whose last stage ended before this one starts.
    const stageInfos = stages.map(s => {
        const subIso = s.submission_time_iso;
        const compIso = s.completion_time_iso;
        const subTs = subIso ? new Date(subIso).getTime() : 0;
        const compTs = compIso ? new Date(compIso).getTime() : subTs;
        return { stage: s, subTs, compTs };
    }).filter(si => si.subTs > 0);

    // Sort by submission time for greedy row packing
    stageInfos.sort((a, b) => a.subTs - b.subTs);

    // rows[i] = end timestamp of last stage in row i
    const rows = [];
    const rowAssignments = []; // { stageInfo, row }

    stageInfos.forEach(si => {
        let placed = false;
        for (let r = 0; r < rows.length; r++) {
            if (si.subTs >= rows[r]) {
                rows[r] = si.compTs;
                rowAssignments.push({ ...si, row: r });
                placed = true;
                break;
            }
        }
        if (!placed) {
            rows.push(si.compTs);
            rowAssignments.push({ ...si, row: rows.length - 1 });
        }
    });

    const numRows = rows.length;

    // Build datasets: for each stage create a floating bar on its assigned row
    // We use separate datasets per stage so each bar sits on the correct y index
    // Approach: create 3 datasets (delay, compute, gc) each with data arrays of
    // length = numRows, where only the matching row index has a value.
    // BUT since multiple stages can share a row, we need one set of 3 datasets
    // per stage to avoid data collisions on the same row.
    //
    // Simpler approach: use a custom rendering. We'll create a single bar dataset
    // per stage (showing the full bar) and overlay segments via a plugin.
    //
    // Actually, simplest: create arrays of per-stage data where each entry is
    // placed on its row. Use stacked bars per row.
    //
    // Best approach for Chart.js horizontal bar: create 3 parallel arrays
    // (delay, compute, gc) per stage as individual dataset groups.
    // Since we have many stages, let's use a flat approach:
    // - labels = row indices 0..numRows-1
    // - For each stage, create 3 datasets (delay segment, compute segment, gc segment)
    //   each containing a sparse array where only the row index has the [start, end] value.
    //
    // To keep it manageable, we'll group all stages into 3 datasets by type,
    // with data indexed by a combined key. But Chart.js floating bars expect
    // one value per label index per dataset.
    //
    // Because multiple stages can sit on the same row, we need per-stage datasets.
    // Let's create groups of 3 datasets per stage.

    const labels = Array.from({ length: numRows }, (_, i) => i);
    const datasets = [];
    const stageMap = []; // maps datasetGroupIndex -> stageInfo for click handling

    rowAssignments.forEach((ra, idx) => {
        const s = ra.stage;
        const subTs = ra.subTs;
        const compTs = ra.compTs;

        const schedulingDelayMs = s.scheduling_delay_ms || 0;
        const gcMs = s.task_summary?.gc_time_ms?.total || 0;
        const totalRunMs = s.task_summary?.run_time_ms?.total || 0;
        const computeMs = Math.max(0, totalRunMs - gcMs);

        // Scale segments to fit within the actual wall-clock span
        const stageSpanMs = compTs - subTs;
        const rawTotalMs = schedulingDelayMs + computeMs + gcMs;
        const scale = rawTotalMs > 0 ? stageSpanMs / rawTotalMs : 1;

        const delayEnd = subTs + schedulingDelayMs * scale;
        const computeEnd = delayEnd + computeMs * scale;
        const gcEnd = computeEnd + gcMs * scale;

        // Sparse data: only the assigned row has a value
        const emptyRow = labels.map(() => null);

        const delayData = [...emptyRow];
        const computeData = [...emptyRow];
        const gcData = [...emptyRow];

        delayData[ra.row] = [subTs, delayEnd];
        computeData[ra.row] = [delayEnd, computeEnd];
        gcData[ra.row] = [computeEnd, gcEnd];

        const showLegend = idx === 0; // only show legend labels once

        datasets.push({
            label: showLegend ? "Scheduling Delay" : "",
            data: delayData,
            backgroundColor: "#fbbf24",
            borderRadius: 2,
            barPercentage: 0.9,
            categoryPercentage: 0.95,
            _stageIdx: idx,
        });
        datasets.push({
            label: showLegend ? "Compute" : "",
            data: computeData,
            backgroundColor: "#34d399",
            borderRadius: 2,
            barPercentage: 0.9,
            categoryPercentage: 0.95,
            _stageIdx: idx,
        });
        datasets.push({
            label: showLegend ? "GC Time" : "",
            data: gcData,
            backgroundColor: "#f87171",
            borderRadius: 2,
            barPercentage: 0.9,
            categoryPercentage: 0.95,
            _stageIdx: idx,
        });

        stageMap.push(ra);
    });

    const chart = new Chart(canvas, {
        type: "bar",
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: "y",
            scales: {
                x: {
                    type: "time",
                    time: {
                        displayFormats: {
                            second: "HH:mm:ss",
                            minute: "HH:mm",
                            hour: "HH:mm",
                        },
                        tooltipFormat: "yyyy/MM/dd HH:mm:ss",
                    },
                    ticks: { color: "#5d6177", font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 15 },
                    grid: { color: "rgba(255,255,255,0.04)" },
                    // Shared min/max so grid lines align with executor chart
                    min: _timelineMin,
                    max: _timelineMax,
                },
                y: {
                    stacked: true,
                    ticks: { display: false },
                    grid: { display: false },
                },
            },
            plugins: {
                legend: {
                    position: "top",
                    labels: {
                        color: "#9498ab", font: { size: 11 }, boxWidth: 12, padding: 16,
                        filter: (item) => item.text && item.text.length > 0,
                    },
                },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        title: (items) => {
                            const dsIdx = items[0].datasetIndex;
                            const stageIdx = Math.floor(dsIdx / 3);
                            const ra = rowAssignments[stageIdx];
                            return ra ? `S${ra.stage.stage_id}` : "";
                        },
                        afterTitle: (items) => {
                            const dsIdx = items[0].datasetIndex;
                            const stageIdx = Math.floor(dsIdx / 3);
                            const ra = rowAssignments[stageIdx];
                            return ra ? ra.stage.stage_name.substring(0, 60) : "";
                        },
                        label: (item) => {
                            const range = item.raw;
                            if (Array.isArray(range) && range[0] != null) {
                                const durSec = (range[1] - range[0]) / 1000;
                                return ` ${item.dataset.label || "Segment"}: ${durSec.toFixed(1)}s`;
                            }
                            return "";
                        },
                        afterLabel: (item) => {
                            // Only append the extra info onto the last segment of the stage's tooltip
                            // to avoid duplicating the lines for Delay, Compute, and GC.
                            // We can use datasetIndex to see if it's the last one (GC Time is index % 3 === 2, but a user might only hover over one segment at a time depending on tooltip mode)
                            const dsIdx = item.datasetIndex;
                            const stageIdx = Math.floor(dsIdx / 3);
                            const ra = rowAssignments[stageIdx];
                            if (!ra) return "";

                            // If hovering over the specific segment, add the stage-level details
                            const s = ra.stage;
                            const durSec = (s.duration_ms || 0) / 1000;
                            const tasks = s.task_summary?.total_tasks || 0;
                            const inputBytes = s.io?.input_bytes || 0;
                            const shuffleReadBytes = s.shuffle?.read_bytes || 0;

                            return [
                                ``,
                                ` Total Partitions: ${tasks}`,
                                ` Stage Duration: ${durSec.toFixed(1)}s`,
                                ` Source Read: ${_formatBytes(inputBytes)}`,
                                ` Shuffle Read: ${_formatBytes(shuffleReadBytes)}`
                            ];
                        },
                    },
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: "x",
                        onPanStart: () => _zoomEnabled,
                        onPan: ({ chart: src }) => {
                            if (_execTimelineChart && src !== _execTimelineChart) {
                                _execTimelineChart.options.scales.x.min = src.scales.x.min;
                                _execTimelineChart.options.scales.x.max = src.scales.x.max;
                                _execTimelineChart.update('none');
                            }
                        },
                    },
                    zoom: {
                        wheel: { enabled: true },
                        pinch: { enabled: true },
                        mode: "x",
                        onZoomStart: () => _zoomEnabled,
                        onZoom: ({ chart: src }) => {
                            if (_execTimelineChart && src !== _execTimelineChart) {
                                _execTimelineChart.options.scales.x.min = src.scales.x.min;
                                _execTimelineChart.options.scales.x.max = src.scales.x.max;
                                _execTimelineChart.update('none');
                            }
                        },
                    },
                },
            },
            onClick: (_event, elements) => {
                if (elements.length > 0 && _dashAnalysis) {
                    const dsIdx = elements[0].datasetIndex;
                    const stageIdx = Math.floor(dsIdx / 3);
                    const ra = rowAssignments[stageIdx];
                    if (ra) {
                        renderPartitionSizeChart(_dashAnalysis, ra.stage.stage_id);
                    }
                }
            },
        },
    });
    _stageTimelineChart = chart;
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// 4. Stage Task Breakdown (Binned)
// ---------------------------------------------------------------------------

function renderPartitionSizeChart(analysis, stageId) {
    const canvas = document.getElementById("partitionSizeChart");
    const label = document.getElementById("partitionSizeLabel");
    if (!canvas) return;

    const stb = analysis.stage_task_bins || {};
    const stageTasks = (stb.stages || {})[String(stageId)];
    if (!stageTasks || stageTasks.length === 0) {
        if (label) label.textContent = `Stage ${stageId} — no task data`;
        return;
    }

    // Find the stage name and task count
    const stages = analysis.stages || [];
    const stageInfo = stages.find(s => s.stage_id === stageId);
    const taskCount = stageInfo?.task_summary?.total_tasks || stageTasks.length;
    const stageName = stageInfo ?
        `Stage ${stageId}: ${stageInfo.stage_name?.substring(0, 60) || ""} (${taskCount} partitions)` :
        `Stage ${stageId} (${taskCount} partitions)`;
    if (label) label.textContent = stageName;

    // Use task_id for x-axis
    const binLabels = stageTasks.map(t => `Task ${t.task_id}`);
    
    // Bar data
    const inputData = stageTasks.map(t => t.input_bytes || 0);
    const shuffleData = stageTasks.map(t => t.shuffle_read_bytes || 0);
    
    // Line data
    const durationData = stageTasks.map(t => (t.duration_ms || 0) / 1000);

    // Destroy existing chart if present
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
                    label: "Source Read",
                    data: inputData,
                    backgroundColor: "#3b82f6", // Blue
                    borderRadius: 2,
                    yAxisID: "y",
                    order: 2,
                },
                {
                    label: "Shuffle Read",
                    data: shuffleData,
                    backgroundColor: "#8b5cf6", // Purple
                    borderRadius: 2,
                    yAxisID: "y",
                    order: 2,
                },
                {
                    label: "Duration",
                    data: durationData,
                    type: "line",
                    borderColor: "#fb923c", // Orange
                    backgroundColor: "transparent",
                    borderWidth: 2,
                    stepped: "middle",
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    yAxisID: "y1",
                    order: 1,
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
                    stacked: true,
                    ticks: { color: "#9498ab", font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 20 },
                    grid: { display: false },
                },
                y: {
                    stacked: true,
                    position: "left",
                    beginAtZero: true,
                    title: { display: true, text: "Data Read (bytes)", color: "#a5b4fc", font: { size: 11 } },
                    ticks: {
                        color: "#a5b4fc",
                        font: { size: 10 },
                        callback: (val) => _formatBytes(val),
                    },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y1: {
                    position: "right",
                    beginAtZero: true,
                    title: { display: true, text: "Duration (seconds)", color: "#fdba74", font: { size: 11 } },
                    ticks: { color: "#fdba74", font: { size: 10 } },
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
                            if (item.dataset.label === "Duration") {
                                return ` Duration: ${item.raw.toFixed(1)}s`;
                            }
                            
                            // For data size bars, also show the total
                            const idx = item.dataIndex;
                            const input = inputData[idx] || 0;
                            const shuffle = shuffleData[idx] || 0;
                            const total = input + shuffle;

                            const lines = [
                                ` ${item.dataset.label}: ${_formatBytes(item.raw)}`,
                            ];
                            
                            // Since it's stacked, show total on the top bar (which is usually Source Read due to order, or whichever is hovered)
                            // To be simple, we can just append the total to the tooltip of either.
                            lines.push(` Total Read: ${_formatBytes(total)}`);
                            
                            return lines;
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
