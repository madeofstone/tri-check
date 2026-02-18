"""
Configuration management for Job Performance Monitor.
Loads settings from .env file with support for CLI overrides.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from the same directory as this script
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)


class Config:
    """Configuration settings for the job performance monitor."""
    
    # Platform API Settings
    PLATFORM_API_BASE_URL: str = os.getenv(
        "PLATFORM_API_BASE_URL", 
        "https://eu1.alteryxcloud.com/v4"
    )
    PLATFORM_API_TOKEN: str = os.getenv("PLATFORM_API_TOKEN", "")
    
    # On-Prem Trifacta API Settings
    ONPREM_API_BASE_URL: str = os.getenv("ONPREM_API_BASE_URL", "")
    ONPREM_API_TOKEN: str = os.getenv("ONPREM_API_TOKEN", "")
    
    # Platform API Query Parameters
    RANFOR_FILTER: str = os.getenv("RANFOR_FILTER", "recipe,plan")
    DEFAULT_LIMIT: int = int(os.getenv("DEFAULT_LIMIT", "25"))
    MATCH_WINDOW_MINUTES: int = int(os.getenv("MATCH_WINDOW_MINUTES", "10"))
    
    # Databricks Settings
    DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN", "")
    
    # Job Naming Pattern
    DATABRICKS_JOB_NAME_PREFIX: str = os.getenv(
        "DATABRICKS_JOB_NAME_PREFIX", 
        "AAC-Transform-Job-"
    )
    
    # Output Settings
    DEFAULT_OUTPUT_FILE: str = os.getenv("DEFAULT_OUTPUT_FILE", "job_report.csv")
    
    @classmethod
    def validate(cls) -> list[str]:
        """Validate required configuration. Returns list of missing fields."""
        missing = []
        if not cls.PLATFORM_API_TOKEN:
            missing.append("PLATFORM_API_TOKEN")
        if not cls.DATABRICKS_HOST:
            missing.append("DATABRICKS_HOST")
        if not cls.DATABRICKS_TOKEN:
            missing.append("DATABRICKS_TOKEN")
        return missing
    
    @classmethod
    def update_from_args(cls, **kwargs):
        """Update configuration from CLI arguments."""
        if kwargs.get("platform_token"):
            cls.PLATFORM_API_TOKEN = kwargs["platform_token"]
        if kwargs.get("databricks_host"):
            cls.DATABRICKS_HOST = kwargs["databricks_host"]
        if kwargs.get("databricks_token"):
            cls.DATABRICKS_TOKEN = kwargs["databricks_token"]
        if kwargs.get("limit"):
            cls.DEFAULT_LIMIT = kwargs["limit"]
        if kwargs.get("ranfor"):
            cls.RANFOR_FILTER = kwargs["ranfor"]
