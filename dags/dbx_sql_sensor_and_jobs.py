"""
Airflow 3.x — Databricks SQL Sensor + Job Runner
=================================================

Two cooperating DAGs defined in this single module:

┌─────────────────────────────────────────────────────────────────────────────┐
│  dbx_sql_sensor_dag                                                         │
│  • Triggered manually or via an external scheduler                          │
│  • Accepts params: date_mode (Today | Yesterday | Custom), custom_date      │
│  • Resolves the YYYYMMDD date and passes it to every SQL sensor             │
│  • Runs one DatabricksSqlSensor per entry in config sensors.checks          │
│  • Each sensor emits an Airflow Asset on success (outlets=[asset])          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  Airflow Assets
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  dbx_job_runner_dag                                                         │
│  • Scheduled automatically by Airflow when sensor assets arrive:            │
│      operation: AND  →  schedule = AssetAll(*assets)                        │
│      operation: OR   →  schedule = AssetAny(*assets)                        │
│  • Runs one DatabricksRunNowOperator per entry in config jobs               │
│  • All jobs run in parallel after the date-resolution task                  │
│  • Replaces {YYYYMMDD} in notebook_params / python_params with resolved date│
└─────────────────────────────────────────────────────────────────────────────┘

Date resolution
───────────────
  Today     → today's date in YYYYMMDD format
  Yesterday → yesterday's date (default)
  Custom    → uses the custom_date value from config or DAG run params

The sensor DAG accepts override params at trigger time:
  {
      "date_mode":   "Custom",
      "custom_date": "20240315"
  }

Config file: <project_root>/config/dag_config.yaml
"""

from __future__ import annotations

from datetime import datetime, timedelta
from datetime import date as date_cls
from pathlib import Path
from typing import Any

import yaml

from airflow import DAG
from airflow.sdk import Asset, AssetAll, AssetAny
from airflow.decorators import task
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ─────────────────────────────────────────────────────────────────────────────
# 1. Load configuration
# ─────────────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).parents[1]   # dags/../  →  project root
_CONFIG_PATH = _PROJECT_ROOT / "config" / "dag_config.yaml"

with _CONFIG_PATH.open() as _fh:
    _cfg: dict[str, Any] = yaml.safe_load(_fh)

# Top-level date settings (can be overridden per DAG-run via params)
_CFG_DATE_MODE: str = _cfg.get("date_mode", "Yesterday")
_CFG_CUSTOM_DATE: str | None = _cfg.get("custom_date") or None

# Sensor block
_OPERATION: str = _cfg["sensors"].get("operation", "AND").upper()
_SENSOR_CHECKS: list[dict[str, Any]] = _cfg["sensors"]["checks"]

# Job block
_JOB_CONFIGS: list[dict[str, Any]] = _cfg.get("jobs", [])

# ── Validate at import time so Airflow surfaces config errors early ───────────
if _OPERATION not in ("AND", "OR"):
    raise ValueError(
        f"sensors.operation must be 'AND' or 'OR', got {_OPERATION!r}. "
        f"Check {_CONFIG_PATH}"
    )
if not _SENSOR_CHECKS:
    raise ValueError(
        f"sensors.checks must contain at least one entry. Check {_CONFIG_PATH}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_date(date_mode: str, custom_date: str | None) -> str:
    """
    Return a date string in YYYYMMDD format.

    Parameters
    ----------
    date_mode:
        "Today"     → current calendar date
        "Yesterday" → yesterday
        "Custom"    → uses *custom_date* verbatim (must be YYYYMMDD)
    custom_date:
        Required when date_mode == "Custom".
    """
    today = date_cls.today()
    if date_mode == "Today":
        return today.strftime("%Y%m%d")
    if date_mode == "Yesterday":
        return (today - timedelta(days=1)).strftime("%Y%m%d")
    if date_mode == "Custom":
        if not custom_date:
            raise ValueError(
                "custom_date must be set when date_mode='Custom'"
            )
        # Validate format; raises ValueError on bad input
        datetime.strptime(custom_date, "%Y%m%d")
        return custom_date
    raise ValueError(
        f"Unknown date_mode={date_mode!r}. Valid values: Today | Yesterday | Custom"
    )


def _tmpl_sensor(text: str) -> str:
    """Replace {YYYYMMDD} with a Jinja XCom pull from the resolve_date task."""
    return text.replace(
        "{YYYYMMDD}",
        "{{ ti.xcom_pull(task_ids='resolve_date') }}",
    )


def _tmpl_job(text: str) -> str:
    """Replace {YYYYMMDD} with a Jinja XCom pull from the resolve_job_date task."""
    return text.replace(
        "{YYYYMMDD}",
        "{{ ti.xcom_pull(task_ids='resolve_job_date') }}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Build Airflow Asset objects  (one per SQL sensor check)
#    Assets are identified by URI; the same URI must be used in both DAGs.
# ─────────────────────────────────────────────────────────────────────────────

_assets: dict[str, Asset] = {
    check["name"]: Asset(uri=f"dbx+sql://{check['asset_name']}")
    for check in _SENSOR_CHECKS
}


# ─────────────────────────────────────────────────────────────────────────────
# 4. DAG 1: dbx_sql_sensor_dag
# ─────────────────────────────────────────────────────────────────────────────

_SHARED_DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbx_sql_sensor_dag",
    description=(
        "Runs Databricks SQL checks and emits Airflow Assets when conditions "
        "are met. Supports AND/OR multi-sensor configurations."
    ),
    # Trigger manually; or wire to a schedule / external DAG trigger
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_SHARED_DEFAULT_ARGS,
    tags=["databricks", "sensor", "assets"],
    # Params allow per-run overrides at trigger time via Airflow UI / API
    params={
        "date_mode": _CFG_DATE_MODE,
        "custom_date": _CFG_CUSTOM_DATE or "",
    },
    doc_md=__doc__,
) as sensor_dag:

    @task(task_id="resolve_date")
    def resolve_date(**context: Any) -> str:
        """
        Resolve the YYYYMMDD date from DAG-run params (highest priority)
        falling back to the config file defaults.

        Returns
        -------
        str
            Date string in YYYYMMDD format, pushed to XCom automatically.
        """
        params = context.get("params", {})
        mode = params.get("date_mode") or _CFG_DATE_MODE
        cdate = params.get("custom_date") or _CFG_CUSTOM_DATE
        resolved = _resolve_date(mode, cdate)
        print(f"[resolve_date] mode={mode!r}  custom_date={cdate!r}  → {resolved!r}")
        return resolved

    date_task = resolve_date()

    # ── Create one DatabricksSqlSensor per check ─────────────────────────────
    # All sensors run in parallel after the date is resolved.
    # When a sensor's SQL condition is met it succeeds and emits its Asset.
    for _check in _SENSOR_CHECKS:
        _sql_rendered = _tmpl_sensor(_check["sql"])
        _asset = _assets[_check["name"]]

        _sensor_task = DatabricksSqlSensor(
            task_id=f"sensor__{_check['name']}",
            databricks_conn_id=_check.get("connection_id", "databricks_default"),
            sql=_sql_rendered,
            # reschedule mode: worker slot is freed between polls (recommended
            # for long-running waits to avoid exhausting the worker pool)
            mode="reschedule",
            poke_interval=_check.get("poke_interval", 60),
            timeout=_check.get("timeout", 3600),
            # Emitting this asset on task success is what wakes the job runner
            outlets=[_asset],
        )

        date_task >> _sensor_task


# ─────────────────────────────────────────────────────────────────────────────
# 5. DAG 2: dbx_job_runner_dag
#    Scheduled automatically by Airflow's asset-based triggering
# ─────────────────────────────────────────────────────────────────────────────

_all_assets: list[Asset] = list(_assets.values())

# AND → every sensor must succeed;  OR → any one sensor succeeding is enough
_job_schedule = (
    AssetAll(*_all_assets) if _OPERATION == "AND" else AssetAny(*_all_assets)
)

with DAG(
    dag_id="dbx_job_runner_dag",
    description=(
        f"Runs Databricks jobs when sensor assets become available. "
        f"Logic: {_OPERATION} across {len(_all_assets)} asset(s)."
    ),
    schedule=_job_schedule,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,
    default_args=_SHARED_DEFAULT_ARGS,
    tags=["databricks", "jobs", "assets"],
) as job_dag:

    @task(task_id="resolve_job_date")
    def resolve_job_date() -> str:
        """
        Recompute the YYYYMMDD date for job parameters.
        Uses the same date_mode / custom_date as the sensor DAG config.
        """
        resolved = _resolve_date(_CFG_DATE_MODE, _CFG_CUSTOM_DATE)
        print(f"[resolve_job_date] mode={_CFG_DATE_MODE!r}  → {resolved!r}")
        return resolved

    job_date_task = resolve_job_date()

    # ── Create one DatabricksRunNowOperator per job ───────────────────────────
    # All jobs run in parallel after the date resolution task completes.
    for _job_cfg in _JOB_CONFIGS:
        # Substitute {YYYYMMDD} placeholder in every string parameter value
        _nb_params: dict[str, str] = {
            k: _tmpl_job(str(v))
            for k, v in _job_cfg.get("notebook_params", {}).items()
        }
        _py_params: list[str] = [
            _tmpl_job(str(p)) for p in _job_cfg.get("python_params", [])
        ]
        _spark_params: list[str] = [
            _tmpl_job(str(p)) for p in _job_cfg.get("spark_submit_params", [])
        ]

        _run_op = DatabricksRunNowOperator(
            task_id=f"run__{_job_cfg['name']}",
            databricks_conn_id=_job_cfg.get("connection_id", "databricks_default"),
            job_id=str(_job_cfg["job_id"]),
            notebook_params=_nb_params if _nb_params else None,
            python_params=_py_params if _py_params else None,
            spark_submit_params=_spark_params if _spark_params else None,
            # Block until the Databricks run reaches a terminal state
            wait_for_termination=True,
        )

        job_date_task >> _run_op
