from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import requests
import yaml
import json

LITELLM_URL = "http://192.168.0.151:32200"
API_KEY = "sk-1234"

default_args = {
    "owner": "airflow",
}

dag = DAG(
    "litellm_teams_provisioning",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

def read_config(**context):
    with open("/opt/airflow/dags/configs/litellm_teams.yaml") as f:
        config = yaml.safe_load(f)
    return config["teams"]

def fetch_existing_teams(**context):
    response = requests.get(
        f"{LITELLM_URL}/team/list",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    return response.json()

def branch_team(team, existing_teams):
    existing_names = [t["name"] for t in existing_teams]

    if team["name"] not in existing_names:
        return "create_team"

    # Compare config
    existing_team = next(t for t in existing_teams if t["name"] == team["name"])

    if existing_team["budget_per_week"] != team["budget_per_week"]:
        return "update_team"

    return "skip_team"

def create_team(team):
    requests.post(
        f"{LITELLM_URL}/team/new",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=team
    )
def update_team(team):
    requests.post(
        f"{LITELLM_URL}/team/update",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=team
    )
def skip_team(team):
    print(f"Skipping team {team['name']} — no changes.")

read_task = PythonOperator(
    task_id="read_config",
    python_callable=read_config,
    dag=dag,
)

fetch_task = PythonOperator(
    task_id="fetch_existing_teams",
    python_callable=fetch_existing_teams,
    dag=dag,
)

def process_teams(**context):
    teams = context["ti"].xcom_pull(task_ids="read_config")
    existing = context["ti"].xcom_pull(task_ids="fetch_existing_teams")

    for team in teams:
        action = branch_team(team, existing)

        if action == "create_team":
            create_team(team)
        elif action == "update_team":
            update_team(team)
        else:
            skip_team(team)

process_task = PythonOperator(
    task_id="process_teams",
    python_callable=process_teams,
    provide_context=True,
    dag=dag,
)

read_task >> fetch_task >> process_task


