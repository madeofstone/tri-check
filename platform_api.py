"""
Alteryx Cloud Platform API client for Job Library data.
"""

import requests
from typing import Optional
from urllib.parse import urlencode
from config import Config


class PlatformAPIError(Exception):
    """Exception raised for Platform API errors."""
    pass


class PlatformAPI:
    """Client for interacting with the Alteryx Cloud Platform Job Library API."""
    
    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None, verify_ssl: bool = True, timeout: int = 10):
        """
        Initialize the Platform API client.
        
        Args:
            token: API bearer token (uses Config if not provided)
            base_url: API base URL (uses Config if not provided)
            verify_ssl: Whether to verify SSL certificates (disable for on-prem with self-signed certs)
            timeout: Request timeout in seconds (default 10)
        """
        self.token = token or Config.PLATFORM_API_TOKEN
        self.base_url = base_url or Config.PLATFORM_API_BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}"
        })
    
    def get_jobs_for_flow(
        self, 
        flow_name: str, 
        limit: Optional[int] = None,
        ranfor: Optional[str] = None
    ) -> dict:
        """
        Fetch jobs for a single flow.
        
        Args:
            flow_name: Name of the flow to query
            limit: Maximum number of jobs to return
            ranfor: Filter for job type (e.g., "recipe,plan")
            
        Returns:
            API response as dictionary
            
        Raises:
            PlatformAPIError: If the API request fails
        """
        params = {
            "limit": limit or Config.DEFAULT_LIMIT,
            "filter": flow_name,
            "ranfor": ranfor or Config.RANFOR_FILTER
        }
        
        url = f"{self.base_url}/jobLibrary?{urlencode(params)}"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise PlatformAPIError(f"Trifacta unreachable — timed out after {self.timeout}s fetching jobs for '{flow_name}'")
        except requests.exceptions.ConnectionError:
            raise PlatformAPIError(f"Trifacta unreachable — cannot connect to {self.base_url}")
        except requests.exceptions.RequestException as e:
            raise PlatformAPIError(f"Failed to fetch jobs for flow '{flow_name}': {e}")
    
    def get_jobs_for_flows(
        self, 
        flow_names: list[str], 
        limit: Optional[int] = None,
        ranfor: Optional[str] = None
    ) -> list[dict]:
        """
        Fetch jobs for multiple flows.
        
        Args:
            flow_names: List of flow names to query
            limit: Maximum number of jobs per flow
            ranfor: Filter for job type (e.g., "recipe,plan")
            
        Returns:
            List of API responses, one per flow
        """
        results = []
        for flow_name in flow_names:
            try:
                response = self.get_jobs_for_flow(flow_name, limit, ranfor)
                results.append({
                    "flow_name": flow_name,
                    "success": True,
                    "data": response
                })
            except PlatformAPIError as e:
                results.append({
                    "flow_name": flow_name,
                    "success": False,
                    "error": str(e),
                    "data": None
                })
        return results

    def get_all_jobs(
        self,
        limit: int = 25,
        offset: int = 0,
        stop_at_id: Optional[int] = None,
    ) -> tuple[list[dict], bool]:
        """
        Fetch all jobs sorted by createdAt descending.

        Args:
            limit: Page size (default 25)
            offset: Starting offset for pagination
            stop_at_id: If set, stop fetching when this job ID is found (for incremental refresh)

        Returns:
            (list_of_raw_jobs, has_more) — raw API entries, and whether more pages exist
        """
        params = {
            "limit": limit,
            "offset": offset,
            "sort": "-createdAt",
        }

        url = f"{self.base_url}/jobLibrary?{urlencode(params)}"

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            raise PlatformAPIError(f"Timed out after {self.timeout}s fetching all jobs")
        except requests.exceptions.ConnectionError:
            raise PlatformAPIError(f"Cannot connect to {self.base_url}")
        except requests.exceptions.RequestException as e:
            raise PlatformAPIError(f"Failed to fetch jobs: {e}")

        raw_jobs = data.get("data", [])

        # Determine if there are more pages
        has_more = len(raw_jobs) == limit

        # If we have a stop_at_id, trim the results
        if stop_at_id is not None:
            trimmed = []
            for j in raw_jobs:
                if j.get("id") == stop_at_id:
                    return trimmed, False  # We've caught up
                trimmed.append(j)
            return trimmed, has_more

        return raw_jobs, has_more
