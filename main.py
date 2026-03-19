import requests
import json
import os

# --------------------------------
# Databricks Configuration
# --------------------------------

DATABRICKS_HOST = "https://dbc-7a28e21d-1f11.cloud.databricks.com"

TOKEN = os.getenv("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


# Job Configuration


job_config = {
    "name": "Sinsa's_pipeline",
    "tasks": [
        {
            "task_key": "ingest_data",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/sinsinboy123@gmail.com/01_ingest_data"
            }
        },
        {
            "task_key": "clean_data",
            "depends_on": [{"task_key": "ingest_data"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/sinsinboy123@gmail.com/02_clean_data"
            }
        },
        {
            "task_key": "sales_summary",
            "depends_on": [{"task_key": "clean_data"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Users/sinsinboy123@gmail.com/03_sales_summary"
            }
        }
    ]
}


# Function: Get Existing Job ID


def get_existing_job_id(job_name):

    jobs_list_url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"

    response = requests.get(jobs_list_url, headers=headers)

    if response.status_code != 200:
        print("Failed to fetch jobs:", response.text)
        return None

    jobs = response.json().get("jobs", [])

    for job in jobs:
        if job["settings"]["name"] == job_name:
            return job["job_id"]

    return None



# Check if Job Exists


job_name = job_config["name"]

existing_job_id = get_existing_job_id(job_name)

if existing_job_id:
    print(f"Job already exists. Using job_id: {existing_job_id}")
    job_id = existing_job_id

else:

    print("Creating new job...")

    create_job_url = f"{DATABRICKS_HOST}/api/2.1/jobs/create"

    response = requests.post(
        create_job_url,
        headers=headers,
        data=json.dumps(job_config)
    )

    print("Create job response:", response.text)

    data = response.json()

    if "job_id" not in data:
        print("Job creation failed:", data)
        exit()

    job_id = data["job_id"]

    print(f"Job created successfully: {job_id}")



# Run the Job


run_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"

run_payload = {
    "job_id": job_id
}

run_response = requests.post(
    run_url,
    headers=headers,
    data=json.dumps(run_payload)
)
print("Databricks Job Url:- ",run_url)
print("Run response:", run_response.text)

print("Job triggered successfully.")
