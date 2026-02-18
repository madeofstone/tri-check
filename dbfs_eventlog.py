"""
DBFS Event Log Client — Find and download Spark event logs from Databricks DBFS.

Trifacta stores Spark event logs under:
    /trifacta/logs/<cluster_id>/eventlog/<random_id>/<random_id>/eventlog

This module recursively searches that path tree and downloads the event log file.
"""

import logging
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)


class EventLogError(Exception):
    """Raised when event log discovery or download fails."""
    pass


def _make_client(host: str, token: str) -> WorkspaceClient:
    """Create a Databricks WorkspaceClient."""
    return WorkspaceClient(host=host, token=token)


def find_eventlog_path(client: WorkspaceClient, cluster_id: str) -> str:
    """
    Recursively search for the 'eventlog' file under
    /trifacta/logs/<cluster_id>/eventlog/.

    The path structure is:
        /trifacta/logs/<cluster_id>/eventlog/<rand1>/<rand2>/eventlog

    Returns the full DBFS path to the eventlog file.
    Raises EventLogError if not found.
    """
    base_path = f"/trifacta/logs/{cluster_id}/eventlog"
    log.info(f"Searching for event log under: {base_path}")

    try:
        # Walk down through the random directory levels
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

    # Recursively search for a file named 'eventlog'
    def _search(path: str, depth: int = 0) -> Optional[str]:
        if depth > 5:  # Safety limit
            return None
        try:
            for item in client.dbfs.list(path):
                item_name = Path(item.path).name
                if not item.is_dir and item_name == "eventlog":
                    return item.path
                elif item.is_dir:
                    result = _search(item.path, depth + 1)
                    if result:
                        return result
        except Exception as e:
            log.warning(f"Error listing '{path}': {e}")
        return None

    eventlog_path = _search(base_path)
    if not eventlog_path:
        raise EventLogError(
            f"Could not find 'eventlog' file under '{base_path}'. "
            f"Directory exists but no eventlog file was found."
        )

    log.info(f"Found event log: {eventlog_path}")
    return eventlog_path


def download_eventlog(
    host: str,
    token: str,
    cluster_id: str,
    local_dir: str,
) -> str:
    """
    Find and download the Spark event log for a given cluster.

    Args:
        host: Databricks workspace URL
        token: Databricks PAT token
        cluster_id: Databricks cluster ID
        local_dir: Local directory to save the event log to

    Returns:
        Path to the downloaded event log file.

    Raises:
        EventLogError on failure.
    """
    client = _make_client(host, token)

    # Find the event log path on DBFS
    dbfs_path = find_eventlog_path(client, cluster_id)

    # Prepare local destination
    local_dir_path = Path(local_dir)
    local_dir_path.mkdir(parents=True, exist_ok=True)
    local_file = local_dir_path / "eventlog"

    log.info(f"Downloading {dbfs_path} -> {local_file}")

    try:
        with client.dbfs.download(dbfs_path) as remote_file:
            with open(local_file, "wb") as f:
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = remote_file.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
    except Exception as e:
        raise EventLogError(f"Failed to download event log: {e}") from e

    log.info(f"Event log saved to: {local_file}")
    return str(local_file)
