/* ============================================================
   Spark Performance Dashboard — Chart & Rendering Logic
   ============================================================
   Depends on: Chart.js (loaded via CDN), app.js (for esc(), fmtBytes())
   ============================================================ */

// Track active Chart.js instances for cleanup
let _dashCharts = [];
let _dashActive = false;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Open the dashboard panel with analysis data.
 * @param {Object} analysis — parsed analysis.json
 * @param {string} jobRunId — for display
 */
function openDashboard(analysis, jobRunId) {
    const slider = document.getElementById("viewSlider");
    const panel = document.getElementById("dashboardPanel");
    const main = document.getElementById("mainContent");
    if (!slider || !panel) return;

    _dashActive = true;
    main.classList.add("slider-active");

    // Build dashboard HTML
    panel.innerHTML = buildDashboardHTML(analysis, jobRunId);

    // Slide to dashboard
    slider.classList.add("slide-dashboard");

    // Render charts after transition settles
    requestAnimationFrame(() => {
        setTimeout(() => {
            renderExecutorTimeline(analysis);
            renderStageBreakdown(analysis);
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

    // Charts
    html += `<div class="dash-charts-row">`;
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Executor Scaling Timeline</div>`;
    html += `<canvas id="executorTimelineChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;
    html += `<div class="dash-chart-card">`;
    html += `<div class="dash-chart-title">Stage Performance Breakdown</div>`;
    html += `<canvas id="stageBreakdownChart" class="dash-chart-canvas"></canvas>`;
    html += `</div>`;
    html += `</div>`;

    // Red Flags
    html += buildRedFlags(analysis);

    // Recommendations placeholder
    html += `<div class="dash-recs-section">`;
    html += `<div class="dash-flags-title">Tuning Recommendations</div>`;
    html += `<div class="dash-recs-placeholder">Recommendations engine coming soon — will surface actionable insights based on analysis data.</div>`;
    html += `</div>`;

    html += `</div>`; // .dash-container
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
// Executor Scaling Timeline
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
    const dataPoints = [{ x: 0, y: 0 }]; // start at zero

    events.forEach(ev => {
        const secFromStart = (ev.timestamp - startTime) / 1000;
        if (ev.event === "added") {
            count++;
        } else if (ev.event === "removed") {
            count = Math.max(0, count - 1);
        }
        dataPoints.push({ x: Math.round(secFromStart * 10) / 10, y: count });
    });

    // Add endpoint if we have stage data
    const stages = analysis.stages || [];
    if (stages.length > 0) {
        const lastStage = stages[stages.length - 1];
        if (lastStage.completion_time_iso) {
            const endTs = new Date(lastStage.completion_time_iso).getTime();
            const endSec = (endTs - startTime) / 1000;
            if (endSec > dataPoints[dataPoints.length - 1].x) {
                dataPoints.push({ x: Math.round(endSec * 10) / 10, y: count });
            }
        }
    }

    const chart = new Chart(canvas, {
        type: "line",
        data: {
            datasets: [{
                label: "Active Executors",
                data: dataPoints,
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
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: "linear",
                    title: { display: true, text: "Seconds from App Start", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 } },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: "Executor Count", color: "#9498ab", font: { size: 11 } },
                    ticks: { color: "#5d6177", font: { size: 10 }, stepSize: 1 },
                    grid: { color: "rgba(255,255,255,0.04)" },
                },
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: "#232738",
                    titleColor: "#e8eaf0",
                    bodyColor: "#9498ab",
                    borderColor: "rgba(255,255,255,0.1)",
                    borderWidth: 1,
                    callbacks: {
                        title: (items) => `${items[0].parsed.x}s from start`,
                        label: (item) => `Executors: ${item.parsed.y}`,
                    },
                },
            },
        },
    });
    _dashCharts.push(chart);
}

// ---------------------------------------------------------------------------
// Stage Performance Breakdown
// ---------------------------------------------------------------------------

function renderStageBreakdown(analysis) {
    const canvas = document.getElementById("stageBreakdownChart");
    if (!canvas) return;

    const stages = analysis.stages || [];
    if (stages.length === 0) return;

    const labels = stages.map(s => `S${s.stage_id}`);
    const computeData = [];
    const delayData = [];
    const gcData = [];

    stages.forEach(s => {
        const totalRunMs = s.task_summary?.run_time_ms?.total || 0;
        const gcMs = s.task_summary?.gc_time_ms?.total || 0;
        const schedulingDelay = s.scheduling_delay_ms || 0;
        const computeMs = Math.max(0, totalRunMs - gcMs);

        computeData.push(computeMs);
        delayData.push(schedulingDelay);
        gcData.push(gcMs);
    });

    const chart = new Chart(canvas, {
        type: "bar",
        data: {
            labels,
            datasets: [
                {
                    label: "Compute",
                    data: computeData,
                    backgroundColor: "#34d399",
                    borderRadius: 2,
                },
                {
                    label: "Scheduling Delay",
                    data: delayData,
                    backgroundColor: "#fbbf24",
                    borderRadius: 2,
                },
                {
                    label: "GC Time",
                    data: gcData,
                    backgroundColor: "#f87171",
                    borderRadius: 2,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: "y",
            scales: {
                x: {
                    stacked: true,
                    title: { display: true, text: "Milliseconds", color: "#9498ab", font: { size: 11 } },
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
                        label: (item) => ` ${item.dataset.label}: ${fmtDurationMs(item.raw)}`,
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
