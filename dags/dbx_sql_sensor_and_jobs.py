"""
Airflow 3.x — Databricks SQL Sensor + Job Runner  (DAG Factory)
================================================================

DAGs produced by this module
─────────────────────────────
  dbx_sql_sensor_dag
    • Triggered manually (or by an upstream scheduler)
    • Params: date_mode (Today | Yesterday | Custom), custom_date (YYYYMMDD)
    • Resolves YYYYMMDD and injects it into every SQL sensor via XCom
    • Runs one DatabricksSqlSensor per sensors.checks entry
    • Each sensor emits an Airflow Asset on success

  dbx_job_runner__<job.name>   [one DAG per job in config]
    • Auto-triggered by Airflow's asset scheduler:
        operation: AND  →  schedule = AssetAll(*depends_on assets)
        operation: OR   →  schedule = AssetAny(*depends_on assets)
    • Params: date_mode / custom_date (same defaults as sensor DAG)
    • Looks up the Databricks job_id at runtime from job_name
    • Substitutes {YYYYMMDD} in notebook_params / python_params
    • Triggers the run via DatabricksHook and waits for completion

Connection
──────────
  A single Airflow Databricks connection (databricks_conn_id from config)
  is used by every sensor and every job.  No per-task connection override.

Date modes
──────────
  date_mode is NOT stored in config — pass it as a DAG param at trigger time.
  Default: "Yesterday"
    Today     → current calendar date
    Yesterday → previous calendar date  (default)
    Custom    → explicit YYYYMMDD supplied in custom_date param

Config file: <project_root>/config/dag_config.yaml
"""

from __future__ import annotations

from datetime import datetime, timedelta
from datetime import date as date_cls
from pathlib import Path
from typing import Any

import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.sdk import Asset, AssetAll, AssetAny
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor


# ─────────────────────────────────────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).parents[1]        # dags/../  →  project root
_CONFIG_PATH = _PROJECT_ROOT / "config" / "dag_config.yaml"

with _CONFIG_PATH.open() as _fh:
    _cfg: dict[str, Any] = yaml.safe_load(_fh)

# Single fixed connection for the entire module — no per-task override
DATABRICKS_CONN_ID: str = _cfg["databricks_conn_id"]

_SENSOR_CHECKS: list[dict[str, Any]] = _cfg["sensors"]["checks"]
_JOB_CONFIGS: list[dict[str, Any]] = _cfg.get("jobs", [])

# ── Validate at import time so Airflow surfaces config errors early ───────────
if not _SENSOR_CHECKS:
    raise ValueError(f"sensors.checks must not be empty — check {_CONFIG_PATH}")

for _jc in _JOB_CONFIGS:
    if "job_name" not in _jc:
        raise ValueError(
            f"Job {_jc.get('name')!r} is missing required field 'job_name'"
        )
    if "depends_on" not in _jc or not _jc["depends_on"]:
        raise ValueError(
            f"Job {_jc.get('name')!r} must have a non-empty 'depends_on' list"
        )
    _jop = _jc.get("operation", "AND").upper()
    if _jop not in ("AND", "OR"):
        raise ValueError(
            f"Job {_jc.get('name')!r}: operation must be 'AND' or 'OR', got {_jop!r}"
        )
    for _dep in _jc["depends_on"]:
        _known = {c["name"] for c in _SENSOR_CHECKS}
        if _dep not in _known:
            raise ValueError(
                f"Job {_jc.get('name')!r}: depends_on entry {_dep!r} "
                f"does not match any sensor check name. Known: {sorted(_known)}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Helpers
# ─────────────────────────────────────────────────────────────────────────────

# date_mode param defaults — NOT read from config
_DATE_PARAMS: dict[str, str] = {
    "date_mode": "Yesterday",   # Today | Yesterday | Custom
    "custom_date": "",          # YYYYMMDD — required only for Custom
}


def _resolve_date(date_mode: str, custom_date: str | None) -> str:
    """Return a YYYYMMDD string for the given mode."""
    today = date_cls.today()
    if date_mode == "Today":
        return today.strftime("%Y%m%d")
    if date_mode == "Yesterday":
        return (today - timedelta(days=1)).strftime("%Y%m%d")
    if date_mode == "Custom":
        if not custom_date:
            raise ValueError("custom_date must be set when date_mode='Custom'")
        datetime.strptime(custom_date, "%Y%m%d")   # validate format
        return custom_date
    raise ValueError(
        f"Unknown date_mode={date_mode!r}. Valid values: Today | Yesterday | Custom"
    )


def _find_job_id_by_name(hook: DatabricksHook, job_name: str) -> int:
    """
    Look up the integer job_id for a Databricks job by its display name.

    Calls GET /api/2.1/jobs/list with a server-side name filter and iterates
    pages until an exact name match is found.

    Raises
    ------
    AirflowException
        When no job with that exact display name exists in the workspace.
    """
    params: dict[str, Any] = {
        "name": job_name,       # server-side filter (may be prefix-based)
        "limit": 25,
        "expand_tasks": False,
    }
    while True:
        response = hook._do_api_call(("GET", "api/2.1/jobs/list"), params)
        for job in response.get("jobs", []):
            if job.get("settings", {}).get("name") == job_name:   # exact match
                return int(job["job_id"])
        if not response.get("has_more", False):
            break
        params["page_token"] = response["next_page_token"]

    raise AirflowException(
        f"Databricks job {job_name!r} not found. "
        "Verify the display name and that the connection has Jobs list permission."
    )


def _subst(value: str, yyyymmdd: str) -> str:
    """Replace the {YYYYMMDD} placeholder with the resolved date string."""
    return value.replace("{YYYYMMDD}", yyyymmdd)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Asset registry  (one Asset per sensor check)
# ─────────────────────────────────────────────────────────────────────────────

_assets: dict[str, Asset] = {
    check["name"]: Asset(uri=f"dbx+sql://{check['asset_name']}")
    for check in _SENSOR_CHECKS
}

_SHARED_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────────────────────
# 4. DAG 1: dbx_sql_sensor_dag
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dbx_sql_sensor_dag",
    description=(
        "Runs Databricks SQL sensors and emits Airflow Assets when data "
        "conditions are satisfied."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_SHARED_ARGS,
    tags=["databricks", "sensor", "assets"],
    params=_DATE_PARAMS,
    doc_md=__doc__,
) as sensor_dag:

    @task(task_id="resolve_date")
    def resolve_date(**context: Any) -> str:
        """
        Resolve the run date from DAG params.
        Returns YYYYMMDD string pushed to XCom for use by every sensor.
        """
        p = context.get("params", {})
        mode = p.get("date_mode") or "Yesterday"
        cdate = p.get("custom_date") or None
        resolved = _resolve_date(mode, cdate)
        print(f"[resolve_date] mode={mode!r}  custom_date={cdate!r}  → {resolved!r}")
        return resolved

    _date_task = resolve_date()

    # One DatabricksSqlSensor per check — all run in parallel after resolve_date.
    # {YYYYMMDD} is replaced via Jinja XCom pull so the date is injected at
    # execution time (not at DAG-parse time).
    for _check in _SENSOR_CHECKS:
        _sql = _check["sql"].replace(
            "{YYYYMMDD}",
            "{{ ti.xcom_pull(task_ids='resolve_date') }}",
        )

        _sensor = DatabricksSqlSensor(
            task_id=f"sensor__{_check['name']}",
            databricks_conn_id=DATABRICKS_CONN_ID,
            sql=_sql,
            mode="reschedule",           # frees worker slot between polls
            poke_interval=_check.get("poke_interval", 60),
            timeout=_check.get("timeout", 3600),
            outlets=[_assets[_check["name"]]],   # emits Asset on success
        )

        _date_task >> _sensor


# ─────────────────────────────────────────────────────────────────────────────
# 5. DAG Factory: one job runner DAG per configured job
#    Each DAG has its own schedule (AssetAll / AssetAny based on job.operation)
#    and looks up the Databricks job_id from job_name at runtime.
# ─────────────────────────────────────────────────────────────────────────────

def _build_job_runner_dag(job_cfg: dict[str, Any]) -> DAG:
    """
    Create and return a job runner DAG for a single configured Databricks job.

    The DAG is scheduled on the subset of Assets listed in job_cfg['depends_on'],
    combined with AND (AssetAll) or OR (AssetAny) per job_cfg['operation'].
    """
    _jname: str = job_cfg["name"]
    _job_display_name: str = job_cfg["job_name"]
    _op: str = job_cfg.get("operation", "AND").upper()
    _dep_assets: list[Asset] = [_assets[d] for d in job_cfg["depends_on"]]
    _schedule = AssetAll(*_dep_assets) if _op == "AND" else AssetAny(*_dep_assets)

    # Capture param dicts for the closure — evaluated once at parse time
    _nb_params: dict[str, str] = {
        k: str(v) for k, v in job_cfg.get("notebook_params", {}).items()
    }
    _py_params: list[str] = [str(v) for v in job_cfg.get("python_params", [])]
    _spark_params: list[str] = [str(v) for v in job_cfg.get("spark_submit_params", [])]

    dag = DAG(
        dag_id=f"dbx_job_runner__{_jname}",
        description=(
            f"Runs Databricks job '{_job_display_name}'. "
            f"Triggered when {_op} of: {', '.join(job_cfg['depends_on'])}."
        ),
        schedule=_schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=3,
        default_args=_SHARED_ARGS,
        tags=["databricks", "jobs", "assets"],
        params=_DATE_PARAMS,   # date_mode defaults to Yesterday; override at trigger
    )

    with dag:

        @task(task_id="resolve_job_date")
        def resolve_job_date(**context: Any) -> str:
            """Resolve YYYYMMDD from DAG params (same logic as sensor DAG)."""
            p = context.get("params", {})
            mode = p.get("date_mode") or "Yesterday"
            cdate = p.get("custom_date") or None
            resolved = _resolve_date(mode, cdate)
            print(f"[{_jname}] resolve_job_date mode={mode!r} → {resolved!r}")
            return resolved

        _jdate = resolve_job_date()

        @task(task_id=f"run__{_jname}")
        def run_dbx_job(yyyymmdd: str) -> int:
            """
            1. Look up Databricks job_id from the display name.
            2. Substitute {YYYYMMDD} in all parameter values.
            3. Trigger the run via DatabricksHook.run_now().
            4. Block until the run reaches a terminal state.
            Returns the Databricks run_id.
            """
            hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)

            # ── Step 1: resolve job_id ────────────────────────────────────────
            job_id = _find_job_id_by_name(hook, _job_display_name)
            print(f"[{_jname}] resolved job_id={job_id} for {_job_display_name!r}")

            # ── Step 2: substitute date placeholder ───────────────────────────
            nb = {k: _subst(v, yyyymmdd) for k, v in _nb_params.items()}
            py = [_subst(v, yyyymmdd) for v in _py_params]
            spark = [_subst(v, yyyymmdd) for v in _spark_params]

            # ── Step 3: build API payload ──────────────────────────────────────
            payload: dict[str, Any] = {"job_id": job_id}
            if nb:
                payload["notebook_params"] = nb
            if py:
                payload["python_params"] = py
            if spark:
                payload["spark_submit_params"] = spark

            print(f"[{_jname}] triggering run — payload: {payload}")

            # ── Step 4: trigger and wait ───────────────────────────────────────
            run_id = hook.run_now(payload)
            print(f"[{_jname}] run_id={run_id}, waiting for completion …")
            hook.wait_for_run(run_id, verbose=True)

            return run_id

        # Passing _jdate (XComArg) as the argument automatically sets
        # the task dependency: resolve_job_date → run__<name>
        run_dbx_job(yyyymmdd=_jdate)

    return dag


# Build every job runner DAG and register it as a module-level global so that
# Airflow's DAG loader discovers each one automatically.
for _jcfg in _JOB_CONFIGS:
    _jdag = _build_job_runner_dag(_jcfg)
    globals()[_jdag.dag_id] = _jdag
