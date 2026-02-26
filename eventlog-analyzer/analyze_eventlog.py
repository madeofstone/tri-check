#!/usr/bin/env python3
"""
Spark Event Log Analyzer – Extracts & compresses Databricks Spark event logs
into a compact JSON focused on actionable tuning metrics.

Usage:
    python analyze_eventlog.py <eventlog_path> [--output <output_path>]

If --output is omitted, writes analysis.json next to the input file.
"""

import argparse
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────────────────────
# Spark property keys that map to tunable levers
# ─────────────────────────────────────────────────────────────────────────────
TUNABLE_SPARK_PROPS = [
    # Shuffle
    "spark.sql.shuffle.partitions",
    "spark.reducer.maxSizeInFlight",
    "spark.shuffle.compress",
    "spark.shuffle.spill.compress",
    "spark.shuffle.file.buffer",
    "spark.shuffle.io.maxRetries",
    "spark.shuffle.io.retryWait",
    # Memory
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.memory.offHeap.enabled",
    "spark.memory.offHeap.size",
    # Parallelism & Partitions
    "spark.default.parallelism",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.files.openCostInBytes",
    "spark.sql.files.maxRecordsPerFile",
    # AQE (Adaptive Query Execution)
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    # Speculation
    "spark.speculation",
    "spark.speculation.multiplier",
    "spark.speculation.quantile",
    # Locality
    "spark.locality.wait",
    "spark.locality.wait.node",
    "spark.locality.wait.process",
    "spark.locality.wait.rack",
    # Compression & Serialization
    "spark.serializer",
    "spark.io.compression.codec",
    "spark.sql.parquet.compression.codec",
    # Broadcast
    "spark.sql.autoBroadcastJoinThreshold",
    # Executor cores
    "spark.executor.cores",
    # Databricks-specific
    "spark.databricks.io.cache.enabled",
    "spark.databricks.io.cache.maxDiskUsage",
    "spark.databricks.io.cache.maxMetaDataCache",
    "spark.databricks.delta.optimizeWrite.enabled",
    "spark.databricks.delta.autoCompact.enabled",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def ts_to_iso(epoch_ms):
    """Convert epoch milliseconds to ISO 8601 string."""
    if epoch_ms is None or epoch_ms == 0:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def safe_div(a, b):
    """Safe division, returns 0 if denominator is 0."""
    return round(a / b, 4) if b else 0


def percentile(sorted_vals, pct):
    """Compute the p-th percentile from a sorted list of values."""
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def summarize_values(values):
    """Compute min/max/median/p95/total for a list of numeric values."""
    if not values:
        return {"min": 0, "max": 0, "median": 0, "p95": 0, "total": 0, "count": 0}
    sorted_vals = sorted(values)
    return {
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
        "median": round(statistics.median(sorted_vals), 2),
        "p95": round(percentile(sorted_vals, 95), 2),
        "total": sum(sorted_vals),
        "count": len(sorted_vals),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Event Parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_eventlog(filepath):
    """Read event log file and return a list of parsed JSON event dicts."""
    events = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  ⚠ Skipping line {line_num}: {e}", file=sys.stderr)
    return events


# ─────────────────────────────────────────────────────────────────────────────
# Extraction Functions
# ─────────────────────────────────────────────────────────────────────────────

def extract_metadata(events):
    """Extract application-level metadata."""
    metadata = {
        "app_id": None,
        "app_name": None,
        "spark_version": None,
        "user": None,
        "start_time": None,
        "start_time_iso": None,
    }

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "SparkListenerApplicationStart":
            metadata["app_id"] = ev.get("App ID")
            metadata["app_name"] = ev.get("App Name")
            metadata["user"] = ev.get("User")
            metadata["start_time"] = ev.get("Timestamp")
            metadata["start_time_iso"] = ts_to_iso(ev.get("Timestamp"))

        elif evt == "DBCEventLoggingListenerMetadata":
            metadata["spark_version"] = ev.get("Spark Version")

    return metadata


def extract_config_snapshot(events):
    """Extract tuning-relevant spark properties from environment updates."""
    all_props = {}
    for ev in events:
        if ev.get("Event") == "SparkListenerEnvironmentUpdate":
            spark_props = ev.get("Spark Properties", {})
            all_props.update(spark_props)

    # Filter to only tunable properties that are present
    tunable = {}
    for key in TUNABLE_SPARK_PROPS:
        if key in all_props:
            tunable[key] = all_props[key]

    return tunable


def extract_resource_profiles(events):
    """Extract resource profile configurations."""
    profiles = []
    for ev in events:
        if ev.get("Event") == "SparkListenerResourceProfileAdded":
            profile = {
                "profile_id": ev.get("Resource Profile Id"),
                "executor_memory_mb": None,
                "executor_offheap_mb": None,
                "task_cpus": None,
            }
            exec_reqs = ev.get("Executor Resource Requests", {})
            if "memory" in exec_reqs:
                profile["executor_memory_mb"] = exec_reqs["memory"].get("Amount")
            if "offHeap" in exec_reqs:
                profile["executor_offheap_mb"] = exec_reqs["offHeap"].get("Amount")
            task_reqs = ev.get("Task Resource Requests", {})
            if "cpus" in task_reqs:
                profile["task_cpus"] = task_reqs["cpus"].get("Amount")
            profiles.append(profile)
    return profiles


def extract_executor_timeline(events):
    """Build executor add/remove timeline with memory and core info."""
    timeline = []

    # Also track block manager memory for enrichment
    block_manager_memory = {}  # executor_id -> {max_memory, onheap, offheap}

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "SparkListenerBlockManagerAdded":
            bm_id = ev.get("Block Manager ID", {})
            exec_id = bm_id.get("Executor ID", "")
            block_manager_memory[exec_id] = {
                "max_memory": ev.get("Maximum Memory"),
                "max_onheap": ev.get("Maximum Onheap Memory"),
                "max_offheap": ev.get("Maximum Offheap Memory"),
            }

        elif evt == "SparkListenerExecutorAdded":
            exec_id = ev.get("Executor ID", "")
            exec_info = ev.get("Executor Info", {})
            entry = {
                "timestamp": ev.get("Timestamp"),
                "timestamp_iso": ts_to_iso(ev.get("Timestamp")),
                "event": "added",
                "executor_id": exec_id,
                "host": exec_info.get("Host"),
                "total_cores": exec_info.get("Total Cores"),
                "resource_profile_id": exec_info.get("Resource Profile Id"),
            }
            # Enrich with block manager memory if available
            if exec_id in block_manager_memory:
                entry["memory"] = block_manager_memory[exec_id]
            timeline.append(entry)

        elif evt == "SparkListenerExecutorRemoved":
            exec_id = ev.get("Executor ID", "")
            entry = {
                "timestamp": ev.get("Timestamp"),
                "timestamp_iso": ts_to_iso(ev.get("Timestamp")),
                "event": "removed",
                "executor_id": exec_id,
                "reason": ev.get("Removed Reason"),
            }
            timeline.append(entry)

        elif evt == "SparkListenerBlockManagerRemoved":
            bm_id = ev.get("Block Manager ID", {})
            exec_id = bm_id.get("Executor ID", "")
            entry = {
                "timestamp": ev.get("Timestamp"),
                "timestamp_iso": ts_to_iso(ev.get("Timestamp")),
                "event": "block_manager_removed",
                "executor_id": exec_id,
                "host": bm_id.get("Host"),
            }
            timeline.append(entry)

    # Sort by timestamp
    timeline.sort(key=lambda x: x.get("timestamp", 0))
    return timeline


def extract_stages(events):
    """Extract per-stage aggregated metrics from task events."""

    # Collect task-level data grouped by stage
    stage_tasks = {}   # (stage_id, attempt_id) -> [task_metrics_dicts]
    stage_info = {}    # (stage_id, attempt_id) -> stage metadata

    # Collect accumulables per stage from StageCompleted events
    stage_accumulables = {}  # (stage_id, attempt_id) -> {name: value}

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "SparkListenerStageSubmitted":
            si = ev.get("Stage Info", {})
            key = (si.get("Stage ID"), si.get("Stage Attempt ID", 0))
            stage_info[key] = {
                "stage_id": si.get("Stage ID"),
                "stage_attempt_id": si.get("Stage Attempt ID", 0),
                "stage_name": si.get("Stage Name"),
                "num_tasks": si.get("Number of Tasks"),
                "submission_time": si.get("Submission Time"),
                "submission_time_iso": ts_to_iso(si.get("Submission Time")),
            }

        elif evt == "SparkListenerStageCompleted":
            si = ev.get("Stage Info", {})
            key = (si.get("Stage ID"), si.get("Stage Attempt ID", 0))
            if key in stage_info:
                stage_info[key]["completion_time"] = si.get("Completion Time")
                stage_info[key]["completion_time_iso"] = ts_to_iso(
                    si.get("Completion Time")
                )
                sub = stage_info[key].get("submission_time", 0)
                comp = si.get("Completion Time", 0)
                if sub and comp:
                    stage_info[key]["duration_ms"] = comp - sub

            # Parse accumulables
            accums = {}
            for acc in si.get("Accumulables", []):
                name = acc.get("Name", "")
                val = acc.get("Value", "0")
                try:
                    accums[name] = int(val)
                except (ValueError, TypeError):
                    try:
                        accums[name] = float(val)
                    except (ValueError, TypeError):
                        accums[name] = val
            stage_accumulables[key] = accums

        elif evt == "SparkListenerTaskEnd":
            stage_id = ev.get("Stage ID")
            attempt_id = ev.get("Stage Attempt ID", 0)
            key = (stage_id, attempt_id)
            if key not in stage_tasks:
                stage_tasks[key] = []

            task_info = ev.get("Task Info", {})
            task_metrics = ev.get("Task Metrics", {})
            shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
            shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
            input_m = task_metrics.get("Input Metrics", {})
            output_m = task_metrics.get("Output Metrics", {})

            stage_tasks[key].append({
                "task_id": task_info.get("Task ID"),
                "executor_id": task_info.get("Executor ID"),
                "host": task_info.get("Host"),
                "locality": task_info.get("Locality"),
                "speculative": task_info.get("Speculative", False),
                "launch_time": task_info.get("Launch Time"),
                "finish_time": task_info.get("Finish Time"),
                "failed": task_info.get("Failed", False),
                "killed": task_info.get("Killed", False),
                "task_end_reason": ev.get("Task End Reason", {}).get("Reason"),
                # Core metrics
                "executor_run_time": task_metrics.get("Executor Run Time", 0),
                "executor_cpu_time": task_metrics.get("Executor CPU Time", 0),
                "executor_deserialize_time": task_metrics.get(
                    "Executor Deserialize Time", 0
                ),
                "jvm_gc_time": task_metrics.get("JVM GC Time", 0),
                "peak_execution_memory": task_metrics.get(
                    "Peak Execution Memory", 0
                ),
                "memory_bytes_spilled": task_metrics.get(
                    "Memory Bytes Spilled", 0
                ),
                "disk_bytes_spilled": task_metrics.get("Disk Bytes Spilled", 0),
                "result_size": task_metrics.get("Result Size", 0),
                # Shuffle
                "shuffle_read_bytes": (
                    shuffle_read.get("Remote Bytes Read", 0)
                    + shuffle_read.get("Local Bytes Read", 0)
                ),
                "shuffle_read_records": shuffle_read.get("Total Records Read", 0),
                "shuffle_remote_bytes": shuffle_read.get("Remote Bytes Read", 0),
                "shuffle_local_bytes": shuffle_read.get("Local Bytes Read", 0),
                "shuffle_fetch_wait_time": shuffle_read.get("Fetch Wait Time", 0),
                "shuffle_write_bytes": shuffle_write.get(
                    "Shuffle Bytes Written", 0
                ),
                "shuffle_write_time": shuffle_write.get("Shuffle Write Time", 0),
                "shuffle_write_records": shuffle_write.get(
                    "Shuffle Records Written", 0
                ),
                # I/O
                "input_bytes": input_m.get("Bytes Read", 0),
                "input_records": input_m.get("Records Read", 0),
                "output_bytes": output_m.get("Bytes Written", 0),
                "output_records": output_m.get("Records Written", 0),
            })

    # Now aggregate per stage
    stages = []
    all_keys = set(stage_info.keys()) | set(stage_tasks.keys())

    for key in sorted(all_keys):
        info = stage_info.get(key, {})
        tasks = stage_tasks.get(key, [])
        accums = stage_accumulables.get(key, {})

        # Task metric aggregations
        run_times = [t["executor_run_time"] for t in tasks]
        cpu_times = [t["executor_cpu_time"] for t in tasks]  # nanoseconds
        gc_times = [t["jvm_gc_time"] for t in tasks]
        peak_mems = [t["peak_execution_memory"] for t in tasks]
        mem_spills = [t["memory_bytes_spilled"] for t in tasks]
        disk_spills = [t["disk_bytes_spilled"] for t in tasks]

        total_run_time = sum(run_times) if run_times else 0
        total_gc_time = sum(gc_times) if gc_times else 0

        # Locality distribution
        locality_counts = {}
        for t in tasks:
            loc = t.get("locality", "UNKNOWN")
            locality_counts[loc] = locality_counts.get(loc, 0) + 1

        # Failed/killed task counts
        failed_count = sum(1 for t in tasks if t.get("failed"))
        killed_count = sum(1 for t in tasks if t.get("killed"))
        speculative_count = sum(1 for t in tasks if t.get("speculative"))

        # Scheduling delay: time between stage submission and first task launch
        scheduling_delay_ms = None
        if tasks and info.get("submission_time"):
            first_launch = min(
                t["launch_time"] for t in tasks if t.get("launch_time")
            )
            scheduling_delay_ms = first_launch - info["submission_time"]

        stage_entry = {
            "stage_id": info.get("stage_id", key[0]),
            "stage_attempt_id": info.get("stage_attempt_id", key[1]),
            "stage_name": info.get("stage_name"),
            "num_tasks": info.get("num_tasks"),
            "submission_time_iso": info.get("submission_time_iso"),
            "completion_time_iso": info.get("completion_time_iso"),
            "duration_ms": info.get("duration_ms"),
            "scheduling_delay_ms": scheduling_delay_ms,
            "task_summary": {
                "total_tasks": len(tasks),
                "failed_tasks": failed_count,
                "killed_tasks": killed_count,
                "speculative_tasks": speculative_count,
                "run_time_ms": summarize_values(run_times),
                "cpu_time_ns": summarize_values(cpu_times),
                "gc_time_ms": summarize_values(gc_times),
                "gc_pct_of_runtime": safe_div(total_gc_time, total_run_time) * 100,
                "cpu_utilization_pct": safe_div(
                    sum(cpu_times) / 1e6, total_run_time
                )
                * 100
                if cpu_times
                else 0,
                "peak_execution_memory": summarize_values(peak_mems),
                "memory_bytes_spilled": summarize_values(mem_spills),
                "disk_bytes_spilled": summarize_values(disk_spills),
            },
            "shuffle": {
                "read_bytes": sum(t["shuffle_read_bytes"] for t in tasks),
                "read_records": sum(t["shuffle_read_records"] for t in tasks),
                "remote_bytes": sum(t["shuffle_remote_bytes"] for t in tasks),
                "local_bytes": sum(t["shuffle_local_bytes"] for t in tasks),
                "fetch_wait_ms": sum(t["shuffle_fetch_wait_time"] for t in tasks),
                "write_bytes": sum(t["shuffle_write_bytes"] for t in tasks),
                "write_records": sum(t["shuffle_write_records"] for t in tasks),
                "write_time_ns": sum(t["shuffle_write_time"] for t in tasks),
            },
            "io": {
                "input_bytes": sum(t["input_bytes"] for t in tasks),
                "input_records": sum(t["input_records"] for t in tasks),
                "output_bytes": sum(t["output_bytes"] for t in tasks),
                "output_records": sum(t["output_records"] for t in tasks),
            },
            "cloud_storage": {
                "request_count": accums.get("cloud storage request count", 0),
                "request_duration_ms": accums.get(
                    "cloud storage request duration", 0
                ),
                "request_size_bytes": accums.get(
                    "cloud storage request size", 0
                ),
                "response_size_bytes": accums.get(
                    "cloud storage response size", 0
                ),
                "retry_count": accums.get("cloud storage retry count", 0),
                "retry_duration_ms": accums.get(
                    "cloud storage retry duration", 0
                ),
            },
            "locality": locality_counts,
            "spill": {
                "spill_size": accums.get("spill size", 0),
                "spill_write_time": accums.get("spill write time", 0),
            },
            "cache": {
                "hits_bytes": accums.get("cache hits size", 0),
                "misses_bytes": accums.get("cache misses size", 0),
            },
        }

        stages.append(stage_entry)

    return stages


def extract_sql_queries(events):
    """Extract SQL execution timings and descriptions."""
    sql_starts = {}
    sql_results = []

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
            exec_id = ev.get("executionId")
            sql_starts[exec_id] = {
                "execution_id": exec_id,
                "description": ev.get("description", ""),
                "start_time": ev.get("time"),
                "start_time_iso": ts_to_iso(ev.get("time")),
            }

        elif evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
            exec_id = ev.get("executionId")
            if exec_id in sql_starts:
                entry = sql_starts[exec_id]
                end_time = ev.get("time")
                entry["end_time"] = end_time
                entry["end_time_iso"] = ts_to_iso(end_time)
                entry["duration_ms"] = (
                    end_time - entry["start_time"]
                    if end_time and entry["start_time"]
                    else None
                )
                sql_results.append(entry)

    # Also add any that started but didn't end (possibly still running or failed)
    for exec_id, entry in sql_starts.items():
        if not any(r["execution_id"] == exec_id for r in sql_results):
            entry["end_time"] = None
            entry["end_time_iso"] = None
            entry["duration_ms"] = None
            entry["status"] = "incomplete"
            sql_results.append(entry)

    sql_results.sort(key=lambda x: x.get("execution_id", 0))
    return sql_results


def extract_job_results(events):
    """Extract job start/end for job-level overview."""
    jobs = {}

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "SparkListenerJobStart":
            job_id = ev.get("Job ID")
            stage_ids = [
                si.get("Stage ID")
                for si in ev.get("Stage Infos", [])
            ]
            jobs[job_id] = {
                "job_id": job_id,
                "submission_time": ev.get("Submission Time"),
                "submission_time_iso": ts_to_iso(ev.get("Submission Time")),
                "stage_ids": stage_ids,
                "sql_execution_id": None,
            }
            # Check properties for SQL execution ID
            props = ev.get("Properties", {})
            if "spark.sql.execution.id" in props:
                jobs[job_id]["sql_execution_id"] = int(
                    props["spark.sql.execution.id"]
                )

        elif evt == "SparkListenerJobEnd":
            job_id = ev.get("Job ID")
            if job_id in jobs:
                comp_time = ev.get("Completion Time")
                jobs[job_id]["completion_time"] = comp_time
                jobs[job_id]["completion_time_iso"] = ts_to_iso(comp_time)
                jobs[job_id]["result"] = ev.get("Job Result", {}).get(
                    "Result", "Unknown"
                )
                sub = jobs[job_id].get("submission_time", 0)
                if sub and comp_time:
                    jobs[job_id]["duration_ms"] = comp_time - sub

    return sorted(jobs.values(), key=lambda x: x.get("job_id", 0))


def extract_pending_task_timeline(events):
    """
    Build a timeline of pending (queued but not yet finished) tasks.

    Increments on SparkListenerStageSubmitted (by Number of Tasks),
    decrements on each successful SparkListenerTaskEnd.
    Returns sorted list of {timestamp, pending}.
    """
    deltas = []  # (timestamp, delta)

    for ev in events:
        evt = ev.get("Event", "")

        if evt == "SparkListenerStageSubmitted":
            si = ev.get("Stage Info", {})
            ts = si.get("Submission Time")
            num_tasks = si.get("Number of Tasks", 0)
            if ts and num_tasks:
                deltas.append((ts, num_tasks))

        elif evt == "SparkListenerTaskEnd":
            reason = ev.get("Task End Reason", {}).get("Reason", "")
            if reason == "Success":
                task_info = ev.get("Task Info", {})
                finish_time = task_info.get("Finish Time")
                if finish_time:
                    deltas.append((finish_time, -1))

    # Sort by timestamp, then by delta (additions before subtractions at same ts)
    deltas.sort(key=lambda x: (x[0], -x[1]))

    pending = 0
    timeline = []
    for ts, delta in deltas:
        pending = max(0, pending + delta)
        timeline.append({"timestamp": ts, "pending": pending})

    return timeline


def extract_executor_task_distribution(events):
    """
    Build per-executor task metrics for the Task Distribution chart.

    Groups by Executor ID and computes:
    - tasks_processed: count of successfully completed tasks
    - avg_active_cores: total_compute_time / executor_lifespan, rounded up
    """
    # executor_id -> list of (launch_time, finish_time)
    executor_tasks = {}

    for ev in events:
        if ev.get("Event") != "SparkListenerTaskEnd":
            continue
        reason = ev.get("Task End Reason", {}).get("Reason", "")
        if reason != "Success":
            continue

        task_info = ev.get("Task Info", {})
        exec_id = task_info.get("Executor ID", "")
        launch = task_info.get("Launch Time")
        finish = task_info.get("Finish Time")
        if not launch or not finish:
            continue

        if exec_id not in executor_tasks:
            executor_tasks[exec_id] = []
        executor_tasks[exec_id].append((launch, finish))

    result = []
    for exec_id in sorted(executor_tasks.keys(), key=lambda x: (len(x), x)):
        tasks = executor_tasks[exec_id]
        tasks_processed = len(tasks)

        total_compute_ms = sum(f - l for l, f in tasks)
        first_launch = min(l for l, f in tasks)
        last_finish = max(f for l, f in tasks)
        lifespan_ms = last_finish - first_launch

        if lifespan_ms > 0:
            avg_cores = total_compute_ms / lifespan_ms
            avg_active_cores = math.ceil(avg_cores)
        else:
            avg_active_cores = 1

        result.append({
            "executor_id": exec_id,
            "tasks_processed": tasks_processed,
            "avg_active_cores": avg_active_cores,
        })

    return result


def extract_stage_task_bins(events, bin_size=20):
    """
    Build binned task breakdown per stage for the Stage Task Breakdown chart.

    For each stage, sorts successful tasks by duration, chunks into bins,
    and computes per-bin averages for duration, GC time, and disk spill.

    Returns:
        {"longest_stage_id": int, "stages": {stage_id: [bins]}}
    """
    # Collect successful tasks per stage
    stage_tasks = {}  # stage_id -> [(duration_ms, gc_ms, spill_bytes)]

    for ev in events:
        if ev.get("Event") != "SparkListenerTaskEnd":
            continue
        reason = ev.get("Task End Reason", {}).get("Reason", "")
        if reason != "Success":
            continue

        stage_id = ev.get("Stage ID")
        task_info = ev.get("Task Info", {})
        task_metrics = ev.get("Task Metrics", {})

        launch = task_info.get("Launch Time", 0)
        finish = task_info.get("Finish Time", 0)
        duration_ms = finish - launch if finish and launch else 0
        gc_ms = task_metrics.get("JVM GC Time", 0)
        spill_bytes = task_metrics.get("Disk Bytes Spilled", 0)

        if stage_id is not None:
            if stage_id not in stage_tasks:
                stage_tasks[stage_id] = []
            stage_tasks[stage_id].append((duration_ms, gc_ms, spill_bytes))

    # Build bins per stage
    stages_binned = {}
    longest_stage_id = None
    longest_duration_total = 0

    for stage_id in sorted(stage_tasks.keys()):
        tasks = stage_tasks[stage_id]
        # Sort by duration
        tasks.sort(key=lambda t: t[0])

        total_duration = sum(t[0] for t in tasks)
        if total_duration > longest_duration_total:
            longest_duration_total = total_duration
            longest_stage_id = stage_id

        bins = []
        for i in range(0, len(tasks), bin_size):
            chunk = tasks[i : i + bin_size]
            start_idx = i + 1
            end_idx = i + len(chunk)
            label = f"P{start_idx}-{end_idx}"

            avg_duration = sum(t[0] for t in chunk) / len(chunk)
            avg_gc = sum(t[1] for t in chunk) / len(chunk)
            avg_spill = sum(t[2] for t in chunk) / len(chunk)

            bins.append({
                "label": label,
                "avg_duration_ms": round(avg_duration, 1),
                "avg_gc_ms": round(avg_gc, 1),
                "avg_spill_bytes": round(avg_spill, 1),
            })

        stages_binned[str(stage_id)] = bins

    return {
        "longest_stage_id": longest_stage_id,
        "stages": stages_binned,
    }


def compute_overall_summary(metadata, stages, executor_timeline, sql_queries):
    """Compute top-level summary statistics for quick overview."""
    total_tasks = sum(s["task_summary"]["total_tasks"] for s in stages)
    total_failed = sum(s["task_summary"]["failed_tasks"] for s in stages)
    total_input_bytes = sum(s["io"]["input_bytes"] for s in stages)
    total_output_bytes = sum(s["io"]["output_bytes"] for s in stages)
    total_shuffle_read = sum(s["shuffle"]["read_bytes"] for s in stages)
    total_shuffle_write = sum(s["shuffle"]["write_bytes"] for s in stages)
    total_spill_memory = sum(
        s["task_summary"]["memory_bytes_spilled"]["total"] for s in stages
    )
    total_spill_disk = sum(
        s["task_summary"]["disk_bytes_spilled"]["total"] for s in stages
    )
    total_gc_ms = sum(
        s["task_summary"]["gc_time_ms"]["total"] for s in stages
    )
    total_runtime_ms = sum(
        s["task_summary"]["run_time_ms"]["total"] for s in stages
    )

    # Executor scaling summary
    adds = [e for e in executor_timeline if e["event"] == "added"]
    removes = [
        e for e in executor_timeline
        if e["event"] in ("removed", "block_manager_removed")
    ]
    peak_executors = 0
    current = 0
    for e in executor_timeline:
        if e["event"] == "added":
            current += 1
            peak_executors = max(peak_executors, current)
        elif e["event"] == "removed":
            current = max(0, current - 1)

    # Longest stage
    longest_stage = max(stages, key=lambda s: s.get("duration_ms", 0) or 0) if stages else None

    # App duration
    app_duration_ms = None
    if stages:
        first_sub = min(
            s.get("submission_time_iso", "") for s in stages if s.get("submission_time_iso")
        )
        last_comp = max(
            s.get("completion_time_iso", "") for s in stages if s.get("completion_time_iso")
        )
        if first_sub and last_comp:
            app_duration_ms = None  # Will compute from raw timestamps below

    # Use raw timestamps for accurate duration
    all_stage_starts = [
        s.get("duration_ms", 0) for s in stages if s.get("duration_ms")
    ]

    return {
        "total_stages": len(stages),
        "total_tasks": total_tasks,
        "total_failed_tasks": total_failed,
        "total_sql_queries": len(sql_queries),
        "peak_executors": peak_executors,
        "executors_added": len(adds),
        "executors_removed": len(removes),
        "total_input_bytes": total_input_bytes,
        "total_output_bytes": total_output_bytes,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "shuffle_to_input_ratio": safe_div(total_shuffle_read, total_input_bytes),
        "total_spill_memory_bytes": total_spill_memory,
        "total_spill_disk_bytes": total_spill_disk,
        "total_gc_ms": total_gc_ms,
        "total_task_runtime_ms": total_runtime_ms,
        "gc_pct_of_total_runtime": safe_div(total_gc_ms, total_runtime_ms) * 100,
        "longest_stage": {
            "stage_id": longest_stage["stage_id"] if longest_stage else None,
            "stage_name": longest_stage.get("stage_name") if longest_stage else None,
            "duration_ms": longest_stage.get("duration_ms") if longest_stage else None,
        } if longest_stage else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def analyze(eventlog_path, output_path=None):
    """Run full analysis and write compressed JSON."""
    print(f"📂 Reading event log: {eventlog_path}")
    events = parse_eventlog(eventlog_path)
    print(f"   Found {len(events)} events")

    if not events:
        print("   ❌ No events found. Aborting.", file=sys.stderr)
        return None

    print("   Extracting metadata...")
    metadata = extract_metadata(events)

    print("   Extracting config snapshot...")
    config = extract_config_snapshot(events)

    print("   Extracting resource profiles...")
    resource_profiles = extract_resource_profiles(events)

    print("   Building executor timeline...")
    executor_timeline = extract_executor_timeline(events)

    print("   Aggregating stage & task metrics...")
    stages = extract_stages(events)

    print("   Extracting SQL query timings...")
    sql_queries = extract_sql_queries(events)

    print("   Extracting job results...")
    jobs = extract_job_results(events)

    print("   Building pending task timeline...")
    pending_timeline = extract_pending_task_timeline(events)

    print("   Building executor task distribution...")
    executor_distribution = extract_executor_task_distribution(events)

    print("   Building stage task bins...")
    stage_task_bins = extract_stage_task_bins(events)

    print("   Computing overall summary...")
    summary = compute_overall_summary(
        metadata, stages, executor_timeline, sql_queries
    )

    # Build tuning inputs from resource profiles and config
    print("   Computing tuning inputs...")
    tuning_inputs = {}
    if resource_profiles:
        rp = resource_profiles[0]
        exec_mem_mb = rp.get("executor_memory_mb", 0)
        offheap_mb = rp.get("executor_offheap_mb", 0)
        unified_mb = exec_mem_mb + offheap_mb
        unified_gb = round(unified_mb / 1024, 2)

        # Cores: prefer total_cores from executor timeline, then config, then task_cpus
        cores = 0
        for ev in executor_timeline:
            if ev.get("event") == "added" and ev.get("total_cores"):
                cores = int(ev["total_cores"])
                break
        if cores == 0:
            cores = int(config.get("spark.executor.cores", 0))
        if cores == 0:
            cores = max(1, int(rp.get("task_cpus", 1)))

        per_core_gb = round(unified_gb / cores, 2) if cores > 0 else 0

        tuning_inputs = {
            "executor_memory_mb": exec_mem_mb,
            "executor_offheap_mb": offheap_mb,
            "unified_memory_mb": unified_mb,
            "unified_memory_gb": unified_gb,
            "cores_per_executor": cores,
            "per_core_gb": per_core_gb,
        }

    analysis = {
        "analysis_version": "2.1.0",
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_file": os.path.basename(eventlog_path),
        "metadata": metadata,
        "summary": summary,
        "config_snapshot": config,
        "resource_profiles": resource_profiles,
        "tuning_inputs": tuning_inputs,
        "executor_timeline": executor_timeline,
        "pending_task_timeline": pending_timeline,
        "executor_task_distribution": executor_distribution,
        "stage_task_bins": stage_task_bins,
        "jobs": jobs,
        "stages": stages,
        "sql_queries": sql_queries,
    }

    # Determine output path
    if output_path is None:
        output_dir = os.path.dirname(os.path.abspath(eventlog_path))
        output_path = os.path.join(output_dir, "analysis.json")

    print(f"💾 Writing analysis to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2)

    # Print compression stats
    input_size = os.path.getsize(eventlog_path)
    output_size = os.path.getsize(output_path)
    ratio = safe_div(output_size, input_size) * 100
    print(f"   📊 Input:  {input_size:>10,} bytes")
    print(f"   📊 Output: {output_size:>10,} bytes ({ratio:.1f}% of original)")
    print(f"   ✅ Done!")

    return analysis


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Databricks Spark event logs and extract tuning metrics."
    )
    parser.add_argument(
        "eventlog",
        help="Path to the raw Spark event log file",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output path for the analysis JSON (default: analysis.json next to input)",
        default=None,
    )

    args = parser.parse_args()

    if not os.path.isfile(args.eventlog):
        print(f"❌ File not found: {args.eventlog}", file=sys.stderr)
        sys.exit(1)

    analyze(args.eventlog, args.output)


if __name__ == "__main__":
    main()
