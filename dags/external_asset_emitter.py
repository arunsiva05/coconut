"""
External Asset Emitter DAGs
===========================
Generated DAGs:  emit_asset__<name>   (one per sensors.checks entry in dag_config.yaml)

These DAGs are designed to be triggered from external systems — Databricks
notebooks, CI/CD pipelines, custom loaders, or any HTTP client — to emit
Airflow Assets with arbitrary metadata.

─────────────────────────────────────────────────────────────────────────────
Trigger via the Airflow REST API (from Databricks or any REST client)
─────────────────────────────────────────────────────────────────────────────

  POST /api/v2/dags/emit_asset__sales_data_ready/dagRuns
  Authorization: Basic <base64(user:password)>
  Content-Type: application/json

  {
    "conf": {
      "source":      "databricks_job_42",
      "row_count":   15000,
      "environment": "prod",
      "date":        "20240101"
    }
  }

─────────────────────────────────────────────────────────────────────────────
Alternative: emit directly via the Airflow Assets REST API (no DAG run)
─────────────────────────────────────────────────────────────────────────────

  POST /api/v2/assets/events
  Authorization: Basic <base64(user:password)>
  Content-Type: application/json

  {
    "uri":   "dbx+sql://sales_data_ready",
    "extra": { "source": "databricks_job_42", "row_count": 15000 }
  }

See tools/emit_asset.py for a ready-made Python helper that wraps both
approaches and requires no third-party libraries.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset


# ─────────────────────────────────────────────────────────────────────────────
# Config — reuse the same source of truth as the sensor DAG
# ─────────────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).parents[1]
_CONFIG_PATH = _PROJECT_ROOT / "config" / "dag_config.yaml"

with _CONFIG_PATH.open() as _fh:
    _cfg: dict[str, Any] = yaml.safe_load(_fh)

_SENSOR_CHECKS: list[dict[str, Any]] = _cfg["sensors"]["checks"]

_SHARED_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 0,
}


# ─────────────────────────────────────────────────────────────────────────────
# DAG factory — one emitter DAG per asset
# ─────────────────────────────────────────────────────────────────────────────

def _build_emitter_dag(check: dict[str, Any]) -> DAG:
    """
    Create and return an emitter DAG for a single configured asset.

    The DAG is schedule=None so it only runs when triggered explicitly.
    Its params mirror the asset's metadata_fields plus a mandatory 'source'
    field so callers can identify what system emitted the event.
    """
    _asset = Asset(uri=f"dbx+sql://{check['asset_name']}")
    _metadata_fields: list[str] = check.get("metadata_fields", [])

    # DAG params: 'source' is always present; configured metadata_fields are optional.
    _params: dict[str, Any] = {"source": "external"}
    for _field in _metadata_fields:
        _params[_field] = None

    dag = DAG(
        dag_id=f"emit_asset__{check['name']}",
        description=(
            f"Emits Airflow asset '{check['asset_name']}' with metadata. "
            "Trigger from Databricks or any external system via the REST API."
        ),
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=5,
        default_args=_SHARED_ARGS,
        tags=["assets", "external", "databricks"],
        params=_params,
        doc_md=__doc__,
    )

    # Capture asset in closure so the inner task always references the right one.
    asset = _asset

    with dag:

        @task(task_id="emit_asset", outlets=[asset])
        def emit_asset(*, outlet_events, **context: Any) -> None:
            """
            Emit the asset event carrying all DAG run conf/params as metadata.

            Every key/value pair passed in the trigger conf is forwarded as the
            asset event's ``extra`` dict, making it visible in the Airflow UI
            (Assets → <asset name> → Recent events) and queryable via the API.
            """
            params: dict[str, Any] = context.get("params", {})
            # Drop None values so the extra dict stays clean
            extra = {k: v for k, v in params.items() if v is not None}
            extra.setdefault("source", "external")
            outlet_events[asset].extra = extra
            print(f"[{dag.dag_id}] asset emitted — extra={extra}")

        emit_asset()

    return dag


# Build one emitter DAG per sensor check and register as module globals so
# Airflow's DAG file processor discovers each one automatically.
for _check in _SENSOR_CHECKS:
    _edag = _build_emitter_dag(_check)
    globals()[_edag.dag_id] = _edag
