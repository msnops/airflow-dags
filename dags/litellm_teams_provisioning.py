from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import yaml
import logging
import pendulum

# Config
LITELLM_URL = Variable.get("LITELLM_URL")
API_KEY = Variable.get("LITELLM_API_KEY")

CONFIG_PATH = "/opt/airflow/dags/configs/litellm_teams.yaml"

default_args = {
    "owner": "platform",
    "retries": 2,
}

dag = DAG(
    dag_id="litellm_teams_provisioning",
    default_args=default_args,
    start_date=pendulum.now("UTC").subtract(days=1),  # compatible with Airflow 2.6+
    schedule_interval=None,
    catchup=False,
    tags=["litellm", "provisioning"],
)


def provision_teams(**context):
    logging.info("Loading config file...")

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    teams = config.get("teams", [])

    logging.info("Fetching existing teams from LiteLLM...")

    response = requests.get(
        f"{LITELLM_URL}/team/list",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    response.raise_for_status()
    existing_teams = response.json()

    existing_map = {t["name"]: t for t in existing_teams}

    for team in teams:
        name = team["name"]

        if name not in existing_map:
            logging.info(f"Creating team: {name}")
            create_team(team)
        else:
            if has_changes(team, existing_map[name]):
                logging.info(f"Updating team: {name}")
                update_team(team)
            else:
                logging.info(f"Skipping team (no changes): {name}")


def has_changes(desired, current):
    compare_fields = ["budget_per_week", "models"]
    for field in compare_fields:
        if desired.get(field) != current.get(field):
            return True
    return False


def create_team(team):
    response = requests.post(
        f"{LITELLM_URL}/team/new",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=team
    )
    response.raise_for_status()


def update_team(team):
    response = requests.post(
        f"{LITELLM_URL}/team/update",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=team
    )
    response.raise_for_status()


provision_task = PythonOperator(
    task_id="provision_litellm_teams",
    python_callable=provision_teams,
    dag=dag,
)
