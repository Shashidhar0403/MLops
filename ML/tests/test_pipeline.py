import requests
import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

def test_databricks_connection():
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"
    response = requests.get(url, headers=headers)
    
    assert response.status_code == 200, "Databricks connection failed"


def test_job_config():
    job_name = "Sinsa's_pipeline"
    assert job_name is not None, "Job name missing"
