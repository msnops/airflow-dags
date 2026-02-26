# pylint: disable=import-error,no-name-in-module
"""
LiteLLM Teams Provisioning DAG with Branching Logic

Objective:
    Automate LiteLLM team provisioning with intelligent branching logic.
    Only create/update when config changes; skip if unchanged.

Notes:
    - models field is excluded from provisioning (managed separately)
    - Teams are blocked by default unless enabled: true is set in config

Task Flow (per team):
    task_read_config
    task_validate_config
        for each team:
            branching_check_team_exists_{alias}
                create path  : task_create_team_{alias} → task_audit_created_{alias}
                update path  : branching_check_config_changed_{alias}
                                   config changed : task_update_team_{alias} → task_audit_updated_{alias}
                                   unchanged      : task_skip_{alias}
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
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LITELLM_URL = Variable.get("LITELLM_URL")
LITELLM_VAULT_FIELD = "litellm_api_key"  # field name within the CSM secret

CONFIGMAP_NAME      = "litellm-teams-config"
CONFIGMAP_NAMESPACE = "airflow"
CONFIGMAP_KEY       = "teams-config.yaml"

XCOM_TEAMS_KEY    = "teams"
XCOM_EXISTING_KEY = "existing_map"

SEPARATOR = "=" * 60

# Mapping of config file keys → LiteLLM API keys
# NOTE: models is intentionally excluded — managed by separate automation
FIELD_MAP = {
    "budget_per_week"       : "max_budget",
    "rpm_limit"             : "rpm_limit",
    "tpm_limit"             : "tpm_limit",
    "max_parallel_requests" : "max_parallel_requests",
    "budget_duration"       : "budget_duration",
    "blocked"               : "blocked",
    "metadata"              : "metadata",
}


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
    cm = v1.read_namespaced_config_map(
        name=CONFIGMAP_NAME,
        namespace=CONFIGMAP_NAMESPACE,
    )
    raw = cm.data.get(CONFIGMAP_KEY)
    if not raw:
        raise ValueError(
            f"Key '{CONFIGMAP_KEY}' not found in ConfigMap "
            f"'{CONFIGMAP_NAME}'. Available keys: {list(cm.data.keys())}"
        )
    return yaml.safe_load(raw)


def _get_api_key() -> str:
    """Fetch LiteLLM API key from Airflow Variable."""
    return Variable.get(LITELLM_VAULT_FIELD)


def _litellm_headers() -> dict:
    """Build LiteLLM auth headers."""
    return {"Authorization": f"Bearer {_get_api_key()}"}


def _fetch_existing_teams() -> dict[str, Any]:
    """Return {team_alias: team_object} from LiteLLM."""
    resp = requests.get(
        f"{LITELLM_URL}/team/list",
        headers=_litellm_headers(),
    )
    resp.raise_for_status()
    return {t.get("team_alias"): t for t in resp.json()}


def _is_team_enabled(team: dict) -> bool:
    """
    Return True only if enabled is explicitly set to True.
    Teams are blocked by default unless enabled: true is set.
    """
    return team.get("enabled", False) is True


def _build_payload(team: dict, team_id: str | None = None) -> dict:
    """
    Build the LiteLLM API payload from config values.

    Notes:
        - models is excluded (managed by separate automation)
        - blocked defaults to True unless enabled: true in config
        - budget_duration defaults to 7d if not specified
    """
    is_enabled = _is_team_enabled(team)
    blocked    = not is_enabled

    payload = {
        "team_alias"           : team.get("team_alias"),
        "max_budget"           : team.get("budget_per_week"),
        "budget_duration"      : team.get("budget_duration", "7d"),
        "rpm_limit"            : team.get("rpm_limit"),
        "tpm_limit"            : team.get("tpm_limit"),
        "max_parallel_requests": team.get("max_parallel_requests"),
        "blocked"              : blocked,
        "metadata"             : {
            **team.get("metadata", {}),
            "enabled": is_enabled,
        },
    }

    if team_id:
        payload["team_id"] = team_id

    return payload


def _config_has_changed(desired: dict, current: dict) -> bool:
    """
    Return True if any tracked field differs between
    desired config and current LiteLLM state.
    Note: models is excluded from comparison.
    """
    desired_payload = _build_payload(desired)
    desired_payload.pop("team_id", None)

    for api_key, desired_val in desired_payload.items():
        current_val = current.get(api_key)

        if api_key == "metadata":
            for meta_key, meta_val in (desired_val or {}).items():
                if current.get("metadata", {}).get(meta_key) != meta_val:
                    return True
            continue

        if desired_val != current_val:
            return True

    return False


def _get_changes(desired: dict, current: dict) -> dict:
    """Return a dict of {field: {current, desired}} for all fields that differ."""
    changes = {}
    desired_payload = _build_payload(desired)
    desired_payload.pop("team_id", None)

    for api_key, desired_val in desired_payload.items():
        current_val = current.get(api_key)
        if api_key == "metadata":
            for meta_key, meta_val in (desired_val or {}).items():
                current_meta_val = current.get("metadata", {}).get(meta_key)
                if current_meta_val != meta_val:
                    changes[f"metadata.{meta_key}"] = {
                        "current": current_meta_val,
                        "desired": meta_val,
                    }
            continue
        if desired_val != current_val:
            changes[api_key] = {
                "current": current_val,
                "desired": desired_val,
            }
    return changes


def _get_parse_time_teams() -> list[dict]:
    """
    Load teams at DAG parse time for task generation.
    Returns empty list on any failure.
    """
    try:
        k8s_config.load_incluster_config()
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            k8s_config.load_kube_config()
        except Exception:  # pylint: disable=broad-exception-caught
            logging.warning("[parse-time] Could not load k8s config")
            return []
    try:
        v1 = k8s_client.CoreV1Api()
        cm = v1.read_namespaced_config_map(
            name=CONFIGMAP_NAME,
            namespace=CONFIGMAP_NAMESPACE,
        )
        raw = cm.data.get(CONFIGMAP_KEY, "litellm:\n  teams: []")
        data = yaml.safe_load(raw)
        teams = (
            data.get("litellm", {}).get("teams")
            or data.get("teams")
            or []
        )
        logging.info(
            "[parse-time] Loaded %d team(s) from ConfigMap", len(teams)
        )
        return teams
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.warning("[parse-time] Could not load ConfigMap: %s", e)
        return []


# ---------------------------------------------------------------------------
# Per-team task factory
# ---------------------------------------------------------------------------

def make_team_tasks(airflow_dag: DAG, team: dict):  # pylint: disable=too-many-locals,too-many-statements
    """Create the full branch task graph for a single team."""

    alias = team.get("team_alias") or team.get("name")
    team  = {**team, "team_alias": alias}

    tid_branch_exists  = f"branching_check_team_exists_{alias}"
    tid_create         = f"task_create_team_{alias}"
    tid_audit_created  = f"task_audit_created_{alias}"
    tid_branch_changed = f"branching_check_config_changed_{alias}"
    tid_update         = f"task_update_team_{alias}"
    tid_audit_updated  = f"task_audit_updated_{alias}"
    tid_skip           = f"task_skip_{alias}"

    _alias = alias
    _team  = team

    # -------------------------------------------------------------------
    # Branch 1 — does the team exist?
    # -------------------------------------------------------------------
    def _branch_exists(**context):
        """Branch based on whether team exists in LiteLLM."""
        existing_map = context["ti"].xcom_pull(
            task_ids="task_validate_config", key=XCOM_EXISTING_KEY
        )
        if _alias not in existing_map:
            logging.info("[%s] Team does not exist → CREATE path", _alias)
            return tid_create
        logging.info(
            "[%s] Team exists → CHECK CONFIG CHANGED path", _alias
        )
        return tid_branch_changed

    branch_exists = BranchPythonOperator(
        task_id=tid_branch_exists,
        python_callable=_branch_exists,
        dag=airflow_dag,
    )

    # -------------------------------------------------------------------
    # Create path
    # -------------------------------------------------------------------
    def _create_team(**context):
        """Create a new team in LiteLLM."""
        is_enabled = _is_team_enabled(_team)
        payload    = _build_payload(_team)

        logging.info("[%s] Creating team in LiteLLM...", _alias)
        logging.info(
            "[%s] Team will be %s (enabled=%s)",
            _alias,
            "ACTIVE" if is_enabled else "BLOCKED",
            is_enabled,
        )
        logging.debug("  Payload: %s", payload)

        resp = requests.post(
            f"{LITELLM_URL}/team/new",
            headers=_litellm_headers(),
            json=payload,
        )
        logging.debug("  Response [%d]: %s", resp.status_code, resp.text)
        resp.raise_for_status()
        logging.info("[%s] Team created successfully.", _alias)
        return resp.json()

    task_create = PythonOperator(
        task_id=tid_create,
        python_callable=_create_team,
        dag=airflow_dag,
    )

    def _audit_created(**context):
        """Audit log for team creation."""
        result     = context["ti"].xcom_pull(task_ids=tid_create)
        is_enabled = _is_team_enabled(_team)
        payload    = _build_payload(_team)

        logging.info(SEPARATOR)
        logging.info("AUDIT — CREATED team '%s'", _alias)
        logging.info(
            "  status              : %s",
            "ACTIVE" if is_enabled else "BLOCKED (set enabled: true to activate)",
        )
        logging.info(
            "  team_id             : %s",
            result.get("team_id") if result else "n/a",
        )
        for k, v in payload.items():
            logging.info("  %-22s: %s", k, v)
        logging.info(SEPARATOR)

    task_audit_created = PythonOperator(
        task_id=tid_audit_created,
        python_callable=_audit_created,
        dag=airflow_dag,
    )

    # -------------------------------------------------------------------
    # Branch 2 — has config changed?
    # -------------------------------------------------------------------
    def _branch_config_changed(**context):
        """Branch based on whether team config has changed."""
        existing_map = context["ti"].xcom_pull(
            task_ids="task_validate_config", key=XCOM_EXISTING_KEY
        )
        current = existing_map.get(_alias, {})
        if _config_has_changed(_team, current):
            logging.info("[%s] Config changed → UPDATE path", _alias)
            return tid_update
        logging.info("[%s] Config unchanged → SKIP path", _alias)
        return tid_skip

    branch_changed = BranchPythonOperator(
        task_id=tid_branch_changed,
        python_callable=_branch_config_changed,
        dag=airflow_dag,
    )

    # -------------------------------------------------------------------
    # Update path
    # -------------------------------------------------------------------
    def _update_team(**context):
        """Update an existing team in LiteLLM."""
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

        payload    = _build_payload(_team, team_id=team_id)
        is_enabled = _is_team_enabled(_team)

        logging.info("[%s] Updating team in LiteLLM...", _alias)
        logging.info(
            "[%s] Team will be %s (enabled=%s)",
            _alias,
            "ACTIVE" if is_enabled else "BLOCKED",
            is_enabled,
        )
        logging.debug("  Payload: %s", payload)

        resp = requests.post(
            f"{LITELLM_URL}/team/update",
            headers=_litellm_headers(),
            json=payload,
        )
        logging.debug("  Response [%d]: %s", resp.status_code, resp.text)
        resp.raise_for_status()
        logging.info("[%s] Team updated successfully.", _alias)
        return _get_changes(_team, current)

    task_update = PythonOperator(
        task_id=tid_update,
        python_callable=_update_team,
        dag=airflow_dag,
    )

    def _audit_updated(**context):
        """Audit log for team update."""
        changes    = context["ti"].xcom_pull(task_ids=tid_update)
        is_enabled = _is_team_enabled(_team)

        logging.info(SEPARATOR)
        logging.info("AUDIT — UPDATED team '%s'", _alias)
        logging.info(
            "  status : %s",
            "ACTIVE" if is_enabled else "BLOCKED (set enabled: true to activate)",
        )
        for field, diff in (changes or {}).items():
            logging.info(
                "  %-22s: %s → %s",
                field,
                diff["current"],
                diff["desired"],
            )
        logging.info(SEPARATOR)

    task_audit_updated = PythonOperator(
        task_id=tid_audit_updated,
        python_callable=_audit_updated,
        dag=airflow_dag,
    )

    # -------------------------------------------------------------------
    # Skip path
    # -------------------------------------------------------------------
    def _skip(**context):
        """Log skip when team config is unchanged."""
        logging.info(SEPARATOR)
        logging.info(
            "SKIP — Team '%s' is up to date. No API call made.", _alias
        )
        logging.info(SEPARATOR)

    task_skip = PythonOperator(
        task_id=tid_skip,
        python_callable=_skip,
        dag=airflow_dag,
    )

    # -------------------------------------------------------------------
    # Wire up dependencies for this team
    # -------------------------------------------------------------------
    branch_exists >> task_create >> task_audit_created
    branch_exists >> branch_changed >> task_update >> task_audit_updated
    branch_changed >> task_skip

    return branch_exists


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="litellm_teams_config",
    default_args={"owner": "platform", "retries": 2},
    start_date=pendulum.datetime(2026, 2, 22, tz="UTC"),
    schedule="0 9 * * *",
    catchup=False,
    tags=["litellm", "provisioning"],
    description="LiteLLM team provisioning with branching logic",
)


# ---------------------------------------------------------------------------
# Task 1 — read_config
# ---------------------------------------------------------------------------
def _read_config(**context):
    """Read teams configuration from Kubernetes ConfigMap."""
    logging.info(SEPARATOR)
    logging.info("Task: read_config")
    logging.info(
        "ConfigMap : %s/%s → %s",
        CONFIGMAP_NAMESPACE,
        CONFIGMAP_NAME,
        CONFIGMAP_KEY,
    )

    data  = _load_configmap()
    teams = (
        data.get("litellm", {}).get("teams")
        or data.get("teams")
        or []
    )

    logging.info(
        "Teams in config (%d): %s",
        len(teams),
        [t.get("team_alias") or t.get("name") for t in teams],
    )
    for team in teams:
        t_alias    = team.get("team_alias") or team.get("name")
        is_enabled = _is_team_enabled(team)
        logging.info(
            "  [%s] enabled=%s → will be %s",
            t_alias,
            is_enabled,
            "ACTIVE" if is_enabled else "BLOCKED",
        )

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
def _validate_config(**context):  # pylint: disable=too-many-branches
    """Validate teams configuration and fetch existing teams."""
    logging.info(SEPARATOR)
    logging.info("Task: validate_config")

    teams = context["ti"].xcom_pull(
        task_ids="task_read_config", key=XCOM_TEAMS_KEY
    )
    if not teams:
        raise ValueError("No teams found in config — aborting.")

    errors = []
    for i, team in enumerate(teams):
        t_alias = team.get("team_alias") or team.get("name")
        if not t_alias:
            errors.append(
                f"Team[{i}] missing 'team_alias' and 'name': {team}"
            )
        # models validation removed — managed by separate automation
        budget = team.get("budget_per_week")
        if budget is not None and not isinstance(budget, (int, float)):
            errors.append(
                f"Team '{t_alias}' budget_per_week must be a number."
            )
        for int_field in ("rpm_limit", "tpm_limit", "max_parallel_requests"):
            val = team.get(int_field)
            if val is not None and not isinstance(val, int):
                errors.append(
                    f"Team '{t_alias}' {int_field} must be an integer."
                )
        enabled_val = team.get("enabled")
        if enabled_val is not None and not isinstance(enabled_val, bool):
            errors.append(
                f"Team '{t_alias}' enabled must be a boolean (true/false)."
            )

    if errors:
        for e in errors:
            logging.error("  Validation error: %s", e)
        raise ValueError(
            f"Config validation failed with {len(errors)} error(s)."
        )

    logging.info("Validation passed for %d team(s).", len(teams))
    logging.info("Fetching existing teams from LiteLLM...")

    existing_map = _fetch_existing_teams()
    logging.info(
        "Found %d existing team(s): %s",
        len(existing_map),
        list(existing_map.keys()),
    )

    context["ti"].xcom_push(key=XCOM_EXISTING_KEY, value=existing_map)


task_validate_config = PythonOperator(
    task_id="task_validate_config",
    python_callable=_validate_config,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Wire shared task dependencies
# ---------------------------------------------------------------------------
task_read_config >> task_validate_config

# ---------------------------------------------------------------------------
# Build per-team task graphs at parse time
# ---------------------------------------------------------------------------
_parse_time_teams = _get_parse_time_teams()

for _team in _parse_time_teams:
    _alias = _team.get("team_alias") or _team.get("name")
    if _alias:
        _team  = {**_team, "team_alias": _alias}
        _entry = make_team_tasks(dag, _team)
        task_validate_config >> _entry
