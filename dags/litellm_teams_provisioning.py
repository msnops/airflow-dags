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
    # ------------------------------------------------------------------ setup
    logging.info("=" * 60)
    logging.info("Starting LiteLLM teams provisioning")
    logging.info(f"LiteLLM URL : {LITELLM_URL}")
    logging.info(f"Config path : {CONFIG_PATH}")
    logging.info("=" * 60)

    # ------------------------------------------------------------------ load config
    logging.info("Loading config file...")
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    teams = config.get("teams", [])
    logging.info(f"Found {len(teams)} team(s) in config: {[t.get('team_alias') or t.get('name') for t in teams]}")

    # ------------------------------------------------------------------ fetch existing
    logging.info("Fetching existing teams from LiteLLM...")
    response = requests.get(
        f"{LITELLM_URL}/team/list",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    response.raise_for_status()
    existing_teams = response.json()

    # LiteLLM returns team_alias, not name
    existing_map = {t.get("team_alias"): t for t in existing_teams}
    logging.info(f"Found {len(existing_map)} existing team(s) in LiteLLM: {list(existing_map.keys())}")

    # ------------------------------------------------------------------ reconcile
    created, updated, skipped, errors = [], [], [], []

    for team in teams:
        # Fall back to 'name' if 'team_alias' is not set
        alias = team.get("team_alias") or team.get("name")
        if not alias:
            msg = f"Team entry missing both 'team_alias' and 'name': {team}"
            logging.error(msg)
            errors.append(team)
            continue  # skip bad entry rather than aborting everything

        # Normalise so downstream functions always see team_alias
        team = {**team, "team_alias": alias}

        logging.info(f"--- Processing team: '{alias}' ---")

        if alias not in existing_map:
            logging.info(f"  [CREATE] Team '{alias}' does not exist in LiteLLM, creating...")
            create_team(team)
            logging.info(f"  [CREATE] Team '{alias}' created successfully")
            created.append(alias)
        else:
            changes = get_changes(team, existing_map[alias])
            if changes:
                logging.info(f"  [UPDATE] Team '{alias}' has changes: {changes}")
                update_team(team)
                logging.info(f"  [UPDATE] Team '{alias}' updated successfully")
                updated.append(alias)
            else:
                logging.info(f"  [SKIP] Team '{alias}' is up to date, nothing to do")
                skipped.append(alias)

    # ------------------------------------------------------------------ summary
    logging.info("=" * 60)
    logging.info("Provisioning complete — summary:")
    logging.info(f"  Created : {created or 'none'}")
    logging.info(f"  Updated : {updated or 'none'}")
    logging.info(f"  Skipped : {skipped or 'none'}")
    if errors:
        logging.warning(f"  Errors  : {errors}")
    logging.info("=" * 60)

    if errors:
        raise ValueError(f"{len(errors)} team entry/entries had validation errors — see logs above")

    return "done"


def get_changes(desired, current):
    """Return a dict of {field: {current, desired}} for fields that differ."""
    compare_fields = ["budget_per_week", "models"]
    changes = {}
    for field in compare_fields:
        d, c = desired.get(field), current.get(field)
        if d != c:
            changes[field] = {"current": c, "desired": d}
    return changes


def create_team(team):
    payload = {
        "team_alias": team.get("team_alias"),
        "budget_per_week": team.get("budget_per_week"),
        "models": team.get("models", []),
    }
    logging.debug(f"  POST {LITELLM_URL}/team/new | payload: {payload}")
    response = requests.post(
        f"{LITELLM_URL}/team/new",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=payload
    )
    logging.debug(f"  Response [{response.status_code}]: {response.text}")
    response.raise_for_status()


def update_team(team):
    payload = {
        "team_alias": team.get("team_alias"),
        "budget_per_week": team.get("budget_per_week"),
        "models": team.get("models", []),
    }
    logging.debug(f"  POST {LITELLM_URL}/team/update | payload: {payload}")
    response = requests.post(
        f"{LITELLM_URL}/team/update",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json=payload
    )
    logging.debug(f"  Response [{response.status_code}]: {response.text}")
    response.raise_for_status()


provision_task = PythonOperator(
    task_id="provision_litellm_teams",
    python_callable=provision_teams,
    dag=dag,
)
