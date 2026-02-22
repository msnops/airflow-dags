from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import requests
import yaml
import logging
import pendulum
import os

# Config
LITELLM_URL = Variable.get("LITELLM_URL")
API_KEY = Variable.get("LITELLM_API_KEY")
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "configs", "litellm_teams.yaml")

default_args = {
    "owner": "platform",
    "retries": 2,
}

dag = DAG(
    dag_id="litellm_teams_provisioning",
    default_args=default_args,
    start_date=pendulum.now("UTC").subtract(days=1),
    schedule=None,
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

    # LiteLLM returns team_alias, not name
    existing_map = {t.get("team_alias"): t for t in existing_teams}

    for team in teams:
        # Fall back to 'name' if 'team_alias' is not set
        alias = team.get("team_alias") or team.get("name")
        if not alias:
            raise ValueError(f"Team entry missing both 'team_alias' and 'name': {team}")

        # Normalise so downstream functions always see team_alias
        team = {**team, "team_alias": alias}

        if alias not in existing_map:
            logging.info(f"Creating team: {alias}")
            create_team(team)
        else:
            if has_changes(team, existing_map[alias]):
                logging.info(f"Updating team: {alias}")
                update_team(team)
            else:
                logging.info(f"Skipping team (no changes): {alias}")

    return "done"


def has_changes(desired, current):
    compare_fields = ["budget_per_week", "models"]
    for field in compare_fields:
        if desired.get(field) != current.get(field):
            return True
    return False


def create_team(team):
    payload = {
        "team_alias": team.get("team_alias"),
        "budget_per_week": team.get("budget_per_week"),
        "models": team.get("models", []),
    }
    response = requests.post(
        f"{LITELLM_URL}/team/new",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=payload
    )
    response.raise_for_status()


def update_team(team):
    payload = {
        "team_alias": team.get("team_alias"),
        "budget_per_week": team.get("budget_per_week"),
        "models": team.get("models", []),
    }
    response = requests.post(
        f"{LITELLM_URL}/team/update",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=payload
    )
    response.raise_for_status()


provision_task = PythonOperator(
    task_id="provision_litellm_teams",
    python_callable=provision_teams,
    dag=dag,
)
