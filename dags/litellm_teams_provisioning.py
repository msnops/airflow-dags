from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import requests
import yaml
import logging
import pendulum
from kubernetes import client, config as k8s_config

# Config
LITELLM_URL = Variable.get("LITELLM_URL")
API_KEY = Variable.get("LITELLM_API_KEY")

# ConfigMap settings
CONFIGMAP_NAME = "litellm-teams-config"
CONFIGMAP_NAMESPACE = "airflow"
CONFIGMAP_KEY = "litellm_teams.yaml"

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


def load_config_from_configmap():
    """Load the teams YAML from a Kubernetes ConfigMap."""
    try:
        # Load in-cluster config (works when running inside Kubernetes)
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        # Fallback to local kubeconfig for local testing
        k8s_config.load_kube_config()

    v1 = client.CoreV1Api()
    configmap = v1.read_namespaced_config_map(
        name=CONFIGMAP_NAME,
        namespace=CONFIGMAP_NAMESPACE,
    )

    raw_yaml = configmap.data.get(CONFIGMAP_KEY)
    if not raw_yaml:
        raise ValueError(
            f"Key '{CONFIGMAP_KEY}' not found in ConfigMap '{CONFIGMAP_NAME}'. "
            f"Available keys: {list(configmap.data.keys())}"
        )

    return yaml.safe_load(raw_yaml)


def provision_teams(**context):
    # ------------------------------------------------------------------ setup
    logging.info("=" * 60)
    logging.info("Starting LiteLLM teams provisioning")
    logging.info(f"LiteLLM URL  : {LITELLM_URL}")
    logging.info(f"ConfigMap    : {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME} → {CONFIGMAP_KEY}")
    logging.info("=" * 60)

    # ------------------------------------------------------------------ load config
    logging.info("Loading config from Kubernetes ConfigMap...")
    config = load_config_from_configmap()

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

    existing_map = {t.get("team_alias"): t for t in existing_teams}
    logging.info(f"Found {len(existing_map)} existing team(s) in LiteLLM: {list(existing_map.keys())}")

    if existing_map:
        sample = next(iter(existing_map.values()))
        logging.debug(f"Existing team sample fields: {list(sample.keys())}")

    # ------------------------------------------------------------------ reconcile
    created, updated, skipped, errors = [], [], [], []

    for team in teams:
        alias = team.get("team_alias") or team.get("name")
        if not alias:
            msg = f"Team entry missing both 'team_alias' and 'name': {team}"
            logging.error(msg)
            errors.append(team)
            continue

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
                update_team(team, existing_map[alias])
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
    """
    Return a dict of {field: {current, desired}} for fields that differ.

    Mapping:
      config: budget_per_week  →  LiteLLM: max_budget  (+ budget_duration="7d")
      config: models           →  LiteLLM: models
    """
    changes = {}

    desired_budget = desired.get("budget_per_week")
    current_budget = current.get("max_budget")
    if desired_budget != current_budget:
        changes["budget"] = {"current": current_budget, "desired": desired_budget}

    desired_models = desired.get("models", [])
    current_models = current.get("models", [])
    if desired_models != current_models:
        changes["models"] = {"current": current_models, "desired": desired_models}

    return changes


def create_team(team):
    payload = {
        "team_alias": team.get("team_alias"),
        "max_budget": team.get("budget_per_week"),
        "budget_duration": "7d",
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


def update_team(team, existing_team):
    """
    Update a LiteLLM team. team_id (the internal UUID) is required by the
    /team/update endpoint — team_alias alone returns a 422.

    budget_per_week in config maps to max_budget + budget_duration="7d" in LiteLLM.
    """
    team_id = existing_team.get("team_id") or existing_team.get("id")
    if not team_id:
        raise ValueError(
            f"Cannot update team '{team.get('team_alias')}': no team_id found in "
            f"existing team data. Available fields: {list(existing_team.keys())}"
        )

    payload = {
        "team_id": team_id,
        "team_alias": team.get("team_alias"),
        "max_budget": team.get("budget_per_week"),
        "budget_duration": "7d",
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
