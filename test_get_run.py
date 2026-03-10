import sys
sys.path.append("/Users/travis.stone/Library/CloudStorage/OneDrive-alteryx.com/Documents/migrations/Dentsu/issues/sparkTunning/tri-track")
from dotenv import load_dotenv

load_dotenv("/Users/travis.stone/Library/CloudStorage/OneDrive-alteryx.com/Documents/migrations/Dentsu/issues/sparkTunning/tri-track/.env")
import os
import json
from databricks_client import DatabricksClient

def test():
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    client = DatabricksClient(host=host, token=token)
    run_details = client.get_run_details(run_id=1032465)
    print(json.dumps(run_details, indent=2))

test()
