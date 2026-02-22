"""
Implement litellm_teams_provisioning DAG with Branching Logic

Objective:
    Automate LiteLLM team provisioning with intelligent branching logic.
    Only create/update when config changes; skip if unchanged.

Task Flow (per team):
    task_read_config
    task_validate_config
        → for each team:
            branching_check_team_exists_{alias}
                → create path  : task_create_team_{alias} → task_audit_created_{alias}
                → update path  : branching_check_config_changed_{alias}
                                    → config changed : task_update_team_{alias} → task_audit_updated_{alias}
                                    → unchanged      : task_skip_{alias}
"""

from __future__ import annotations

import logging
from typing import Any

import pendulum
import requests
import yaml
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LITELLM_URL = Variable.get("LITELLM_URL")
API_KEY = Variable.get("LITELLM_API_KEY")

CONFIGMAP_NAME = "litellm-teams-config"
CONFIGMAP_NAMESPACE = "airflow"
CONFIGMAP_KEY = "litellm_teams.yaml"

XCOM_TEAMS_KEY = "teams"
XCOM_EXISTING_KEY = "existing_map"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_configmap() -> dict:
    """Read the teams YAML from a Kubernetes ConfigMap."""
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    v1 = k8s_client.CoreV1Api()
    cm = v1.read_namespaced_config_map(name=CONFIGMAP_NAME, namespace=CONFIGMAP_NAMESPACE)
    raw = cm.data.get(CONFIGMAP_KEY)
    if not raw:
        raise ValueError(
            f"Key '{CONFIGMAP_KEY}' not found in ConfigMap '{CONFIGMAP_NAME}'. "
            f"Available keys: {list(cm.data.keys())}"
        )
    return yaml.safe_load(raw)


def _litellm_headers() -> dict:
    return {"Authorization": f"Bearer {API_KEY}"}


def _fetch_existing_teams() -> dict[str, Any]:
    """Return {team_alias: team_object} from LiteLLM."""
    resp = requests.get(f"{LITELLM_URL}/team/list", headers=_litellm_headers())
    resp.raise_for_status()
    return {t.get("team_alias"): t for t in resp.json()}


def _config_has_changed(desired: dict, current: dict) -> bool:
    """Return True if budget or models differ."""
    if desired.get("budget_per_week") != current.get("max_budget"):
        return True
    if desired.get("models", []) != current.get("models", []):
        return True
    return False


def _get_parse_time_teams() -> list[dict]:
    """
    Load teams at DAG parse time for task generation.
    Returns empty list on any failure — tasks will still be
    generated correctly at runtime via XCom.
    """
    try:
        k8s_config.load_incluster_config()
    except Exception:
        try:
            k8s_config.load_kube_config()
        except Exception:
            logging.warning("Could not load k8s config at parse time")
            return []
    try:
        v1 = k8s_client.CoreV1Api()
        cm = v1.read_namespaced_config_map(
            name=CONFIGMAP_NAME, namespace=CONFIGMAP_NAMESPACE
        )
        raw = cm.data.get(CONFIGMAP_KEY, "teams: []")
        teams = yaml.safe_load(raw).get("teams", [])
        logging.info(f"[parse-time] Loaded {len(teams)} team(s) from ConfigMap")
        return teams
    except Exception as e:
        logging.warning(f"[parse-time] Could not load ConfigMap: {e}")
        return []


# ---------------------------------------------------------------------------
# Per-team task factory — defined OUTSIDE the DAG context so closures work
# ---------------------------------------------------------------------------

def make_team_tasks(dag: DAG, team: dict):
    """Create the full branch task graph for a single team."""

    alias = team.get("team_alias") or team.get("name")
    team = {**team, "team_alias": alias}

    # Task IDs
    tid_branch_exists  = f"branching_check_team_exists_{alias}"
    tid_create         = f"task_create_team_{alias}"
    tid_audit_created  = f"task_audit_created_{alias}"
    tid_branch_changed = f"branching_check_config_changed_{alias}"
    tid_update         = f"task_update_team_{alias}"
    tid_audit_updated  = f"task_audit_updated_{alias}"
    tid_skip           = f"task_skip_{alias}"

    # capture alias in closure
    _alias = alias
    _team = team

    # -------------------------------------------------------------------
    # Branch 1 — does the team exist?
    # -------------------------------------------------------------------
    def _branch_exists(**context):
        existing_map = context["ti"].xcom_pull(
            task_ids="task_validate_config", key=XCOM_EXISTING_KEY
        )
        if _alias not in existing_map:
            logging.info(f"[{_alias}] Team does not exist → CREATE path")
            return tid_create
        logging.info(f"[{_alias}] Team exists → CHECK CONFIG CHANGED path")
        return tid_branch_changed

    branch_exists = BranchPythonOperator(
        task_id=tid_branch_exists,
        python_callable=_branch_exists,
        dag=dag,
    )

    # -------------------------------------------------------------------
    # Create path
    # -------------------------------------------------------------------
    def _create_team(**context):
        logging.info(f"[{_alias}] Creating team in LiteLLM...")
        payload = {
            "team_alias": _alias,
            "max_budget": _team.get("budget_per_week"),
            "budget_duration": "7d",
            "models": _team.get("models", []),
        }
        logging.debug(f"  Payload: {payload}")
        resp = requests.post(
            f"{LITELLM_URL}/team/new",
            headers=_litellm_headers(),
            json=payload,
        )
        logging.debug(f"  Response [{resp.status_code}]: {resp.text}")
        resp.raise_for_status()
        logging.info(f"[{_alias}] Team created successfully.")
        return resp.json()

    task_create = PythonOperator(
        task_id=tid_create,
        python_callable=_create_team,
        dag=dag,
    )

    def _audit_created(**context):
        result = context["ti"].xcom_pull(task_ids=tid_create)
        logging.info("=" * 60)
        logging.info(f"AUDIT — CREATED team '{_alias}'")
        logging.info(f"  team_id      : {result.get('team_id') if result else 'n/a'}")
        logging.info(f"  team_alias   : {_alias}")
        logging.info(f"  models       : {_team.get('models')}")
        logging.info(f"  max_budget   : {_team.get('budget_per_week')}")
        logging.info(f"  budget_cycle : 7d")
        logging.info("=" * 60)

    task_audit_created = PythonOperator(
        task_id=tid_audit_created,
        python_callable=_audit_created,
        dag=dag,
    )

    # -------------------------------------------------------------------
    # Branch 2 — has config changed?
    # -------------------------------------------------------------------
    def _branch_config_changed(**context):
        existing_map = context["ti"].xcom_pull(
            task_ids="task_validate_config", key=XCOM_EXISTING_KEY
        )
        current = existing_map.get(_alias, {})
        if _config_has_changed(_team, current):
            logging.info(f"[{_alias}] Config changed → UPDATE path")
            return tid_update
        logging.info(f"[{_alias}] Config unchanged → SKIP path")
        return tid_skip

    branch_changed = BranchPythonOperator(
        task_id=tid_branch_changed,
        python_callable=_branch_config_changed,
        dag=dag,
    )

    # -------------------------------------------------------------------
    # Update path
    # -------------------------------------------------------------------
    def _update_team(**context):
        existing_map = context["ti"].xcom_pull(
            task_ids="task_validate_config", key=XCOM_EXISTING_KEY
        )
        current = existing_map.get(_alias, {})
        team_id = current.get("team_id") or current.get("id")
        if not team_id:
            raise ValueError(
                f"[{_alias}] Cannot update: no team_id in existing data. "
                f"Fields: {list(current.keys())}"
            )
        payload = {
            "team_id": team_id,
            "team_alias": _alias,
            "max_budget": _team.get("budget_per_week"),
            "budget_duration": "7d",
            "models": _team.get("models", []),
        }
        logging.info(f"[{_alias}] Updating team in LiteLLM...")
        logging.debug(f"  Payload: {payload}")
        resp = requests.post(
            f"{LITELLM_URL}/team/update",
            headers=_litellm_headers(),
            json=payload,
        )
        logging.debug(f"  Response [{resp.status_code}]: {resp.text}")
        resp.raise_for_status()
        logging.info(f"[{_alias}] Team updated successfully.")
        changes = {}
        if _team.get("budget_per_week") != current.get("max_budget"):
            changes["max_budget"] = {
                "from": current.get("max_budget"),
                "to": _team.get("budget_per_week"),
            }
        if _team.get("models", []) != current.get("models", []):
            changes["models"] = {
                "from": current.get("models"),
                "to": _team.get("models"),
            }
        return changes

    task_update = PythonOperator(
        task_id=tid_update,
        python_callable=_update_team,
        dag=dag,
    )

    def _audit_updated(**context):
        changes = context["ti"].xcom_pull(task_ids=tid_update)
        logging.info("=" * 60)
        logging.info(f"AUDIT — UPDATED team '{_alias}'")
        for field, diff in (changes or {}).items():
            logging.info(f"  {field}: {diff['from']} → {diff['to']}")
        logging.info("=" * 60)

    task_audit_updated = PythonOperator(
        task_id=tid_audit_updated,
        python_callable=_audit_updated,
        dag=dag,
    )

    # -------------------------------------------------------------------
    # Skip path
    # -------------------------------------------------------------------
    def _skip(**context):
        logging.info("=" * 60)
        logging.info(f"SKIP — Team '{_alias}' is up to date. No API call made.")
        logging.info("=" * 60)

    task_skip = PythonOperator(
        task_id=tid_skip,
        python_callable=_skip,
        dag=dag,
    )

    # -------------------------------------------------------------------
    # Wire up dependencies for this team
    # -------------------------------------------------------------------
    branch_exists >> task_create >> task_audit_created
    branch_exists >> branch_changed >> task_update >> task_audit_updated
    branch_changed >> task_skip

    return branch_exists  # return entry point


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="litellm_teams_with_branch_and_scheduling",
    default_args={"owner": "platform", "retries": 2},
    start_date=pendulum.datetime(2026, 2, 22, tz="UTC"),
    schedule="0 9 * * *",
    catchup=False,
    tags=["litellm", "provisioning"],
    description="RDX-610 — LiteLLM team provisioning with branching logic",
)

# ---------------------------------------------------------------------------
# Task 1 — read_config
# ---------------------------------------------------------------------------
def _read_config(**context):
    logging.info("=" * 60)
    logging.info("Task: read_config")
    logging.info(f"ConfigMap : {CONFIGMAP_NAMESPACE}/{CONFIGMAP_NAME} → {CONFIGMAP_KEY}")
    config = _load_configmap()
    teams = config.get("teams", [])
    logging.info(f"Teams in config ({len(teams)}): "
                 f"{[t.get('team_alias') or t.get('name') for t in teams]}")
    context["ti"].xcom_push(key=XCOM_TEAMS_KEY, value=teams)
    logging.info("Pushed teams to XCom")


task_read_config = PythonOperator(
    task_id="task_read_config",
    python_callable=_read_config,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task 2 — validate_config
# ---------------------------------------------------------------------------
def _validate_config(**context):
    logging.info("=" * 60)
    logging.info("Task: validate_config")

    teams = context["ti"].xcom_pull(task_ids="task_read_config", key=XCOM_TEAMS_KEY)
    if not teams:
        raise ValueError("No teams found in config — aborting.")

    errors = []
    for i, team in enumerate(teams):
        alias = team.get("team_alias") or team.get("name")
        if not alias:
            errors.append(f"Team[{i}] missing 'team_alias' and 'name': {team}")
        if not isinstance(team.get("models"), list) or len(team.get("models", [])) == 0:
            errors.append(f"Team '{alias}' has no models defined.")
        if (team.get("budget_per_week") is not None
                and not isinstance(team.get("budget_per_week"), (int, float))):
            errors.append(f"Team '{alias}' budget_per_week must be a number.")

    if errors:
        for e in errors:
            logging.error(f"  Validation error: {e}")
        raise ValueError(f"Config validation failed with {len(errors)} error(s).")

    logging.info(f"Validation passed for {len(teams)} team(s).")

    logging.info("Fetching existing teams from LiteLLM...")
    existing_map = _fetch_existing_teams()
    logging.info(f"Found {len(existing_map)} existing team(s): {list(existing_map.keys())}")
    context["ti"].xcom_push(key=XCOM_EXISTING_KEY, value=existing_map)


task_validate_config = PythonOperator(
    task_id="task_validate_config",
    python_callable=_validate_config,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Build per-team task graphs at parse time
# ---------------------------------------------------------------------------
task_read_config >> task_validate_config

_parse_time_teams = _get_parse_time_teams()

for _team in _parse_time_teams:
    _alias = _team.get("team_alias") or _team.get("name")
    if _alias:
        _team = {**_team, "team_alias": _alias}
        _entry = make_team_tasks(dag, _team)
        task_validate_config >> _entry
