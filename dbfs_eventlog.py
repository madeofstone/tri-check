"""
DBFS Event Log Client — Find and download Spark event logs from Databricks DBFS.

Trifacta stores Spark event logs under:
    /trifacta/logs/<cluster_id>/eventlog/<rand1>/<rand2>/

Larger jobs split the log across multiple files:
    eventlog              – current / most recent events (plain text)
    eventlog-YYYY-MM-DD--HH-MM.gz  – older events (gzip compressed)

This module discovers ALL files in that directory, downloads them,
decompresses .gz archives, and concatenates everything (oldest → newest)
into a single unified eventlog file for analysis.
"""

import gzip
import logging
import re
import shutil
from pathlib import Path
from typing import Optional, List, Tuple

from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)


class EventLogError(Exception):
    """Raised when event log discovery or download fails."""
    pass


def _make_client(host: str, token: str) -> WorkspaceClient:
    """Create a Databricks WorkspaceClient."""
    return WorkspaceClient(host=host, token=token)


def find_eventlog_dir(client: WorkspaceClient, cluster_id: str) -> str:
    """
    Recursively search for the directory containing eventlog files under
    /trifacta/logs/<cluster_id>/eventlog/.

    The path structure is:
        /trifacta/logs/<cluster_id>/eventlog/<rand1>/<rand2>/
            eventlog
            eventlog-2026-02-23--13-30.gz
            eventlog-2026-02-23--13-40.gz

    Returns the full DBFS path to the directory that contains eventlog files.
    Raises EventLogError if not found.
    """
    base_path = f"/trifacta/logs/{cluster_id}/eventlog"
    log.info(f"Searching for event log directory under: {base_path}")

    try:
        items = list(client.dbfs.list(base_path))
    except Exception as e:
        raise EventLogError(
            f"Cannot access DBFS path '{base_path}': {e}"
        ) from e

    if not items:
        raise EventLogError(
            f"No contents found at '{base_path}'. "
            f"Cluster '{cluster_id}' may not have event logs."
        )

    # Recursively search for a directory containing an 'eventlog' file
    def _search(path: str, depth: int = 0) -> Optional[str]:
        if depth > 5:  # Safety limit
            return None
        try:
            children = list(client.dbfs.list(path))
            for item in children:
                item_name = Path(item.path).name
                if not item.is_dir and item_name.startswith("eventlog"):
                    # Found an eventlog file — return its parent directory
                    return path
            # No eventlog file here; recurse into subdirectories
            for item in children:
                if item.is_dir:
                    result = _search(item.path, depth + 1)
                    if result:
                        return result
        except Exception as e:
            log.warning(f"Error listing '{path}': {e}")
        return None

    eventlog_dir = _search(base_path)
    if not eventlog_dir:
        raise EventLogError(
            f"Could not find eventlog files under '{base_path}'. "
            f"Directory exists but no eventlog files were found."
        )

    log.info(f"Found event log directory: {eventlog_dir}")
    return eventlog_dir


# Regex to extract timestamp from filenames like eventlog-2026-02-23--13-30.gz
_TS_RE = re.compile(r"eventlog-(\d{4}-\d{2}-\d{2}--\d{2}-\d{2})")


def _sort_key(filename: str) -> Tuple[int, str]:
    """
    Sort key so that timestamped .gz files come first (oldest → newest),
    and the plain 'eventlog' file comes last.
    """
    m = _TS_RE.search(filename)
    if m:
        return (0, m.group(1))  # timestamped archives first, sorted by time
    return (1, filename)        # plain 'eventlog' last


def _download_file(client: WorkspaceClient, dbfs_path: str, local_path: Path) -> None:
    """Download a single file from DBFS to a local path."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with client.dbfs.download(dbfs_path) as remote_file:
        with open(local_path, "wb") as f:
            chunk_size = 1024 * 1024  # 1 MB chunks
            while True:
                chunk = remote_file.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)


def _decompress_gz(gz_path: Path, out_path: Path) -> None:
    """Decompress a .gz file to out_path."""
    with gzip.open(gz_path, "rb") as f_in:
        with open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def download_all_eventlogs(
    client: WorkspaceClient,
    cluster_id: str,
    local_dir: str,
) -> str:
    """
    Discover, download, decompress, and concatenate ALL Spark event log
    files for a given cluster into a single unified eventlog.

    Steps:
        1. Find the DBFS directory containing eventlog files.
        2. List all files in that directory.
        3. Download each file to a local temp area.
        4. Decompress any .gz files.
        5. Concatenate all files oldest → newest into <local_dir>/eventlog.
        6. Clean up temp files.

    Returns:
        Path to the final unified eventlog file.

    Raises:
        EventLogError on failure.
    """
    # Step 1: Find directory
    eventlog_dir = find_eventlog_dir(client, cluster_id)

    # Step 2: List all files
    try:
        items = list(client.dbfs.list(eventlog_dir))
    except Exception as e:
        raise EventLogError(f"Cannot list eventlog directory '{eventlog_dir}': {e}") from e

    files = [item for item in items if not item.is_dir]
    if not files:
        raise EventLogError(f"No files found in eventlog directory '{eventlog_dir}'.")

    log.info(f"Found {len(files)} eventlog file(s) in {eventlog_dir}")

    # Step 3 & 4: Download and decompress
    local_dir_path = Path(local_dir)
    local_dir_path.mkdir(parents=True, exist_ok=True)
    tmp_dir = local_dir_path / "_eventlog_parts"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    decompressed_files: List[Tuple[str, Path]] = []  # (sort_key_name, local_path)

    for item in files:
        filename = Path(item.path).name
        local_file = tmp_dir / filename
        log.info(f"Downloading {item.path} ({item.file_size or 0} bytes)")

        try:
            _download_file(client, item.path, local_file)
        except Exception as e:
            raise EventLogError(f"Failed to download '{item.path}': {e}") from e

        # Decompress .gz files
        if filename.endswith(".gz"):
            decompressed_name = filename[:-3]  # strip .gz
            decompressed_path = tmp_dir / decompressed_name
            log.info(f"Decompressing {filename} → {decompressed_name}")
            try:
                _decompress_gz(local_file, decompressed_path)
            except Exception as e:
                raise EventLogError(f"Failed to decompress '{filename}': {e}") from e
            # Remove the .gz file, keep decompressed
            local_file.unlink()
            decompressed_files.append((filename, decompressed_path))
        else:
            decompressed_files.append((filename, local_file))

    # Step 5: Sort oldest → newest and concatenate
    decompressed_files.sort(key=lambda x: _sort_key(x[0]))

    final_eventlog = local_dir_path / "eventlog"
    log.info(f"Concatenating {len(decompressed_files)} file(s) into {final_eventlog}")

    total_bytes = 0
    with open(final_eventlog, "wb") as out_f:
        for filename, part_path in decompressed_files:
            part_size = part_path.stat().st_size
            total_bytes += part_size
            log.info(f"  Appending {filename} ({part_size:,} bytes)")
            with open(part_path, "rb") as in_f:
                shutil.copyfileobj(in_f, out_f)
            # Ensure newline between files so JSON lines don't merge
            out_f.write(b"\n")

    log.info(f"Unified eventlog: {total_bytes:,} bytes → {final_eventlog}")

    # Step 6: Clean up temp directory
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return str(final_eventlog)


def download_eventlog(
    host: str,
    token: str,
    cluster_id: str,
    local_dir: str,
) -> str:
    """
    Find and download ALL Spark event log files for a given cluster,
    decompress .gz archives, and concatenate into a single eventlog.

    Args:
        host: Databricks workspace URL
        token: Databricks PAT token
        cluster_id: Databricks cluster ID
        local_dir: Local directory to save the unified event log to

    Returns:
        Path to the unified event log file.

    Raises:
        EventLogError on failure.
    """
    client = _make_client(host, token)
    return download_all_eventlogs(client, cluster_id, local_dir)
