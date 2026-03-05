"""
Register Assets — utility DAG
==============================

Manually-triggered DAG that emits a registration event for every asset
defined in config/dag_config.yaml.

Run this once after deployment to ensure all assets appear in the Airflow
Assets UI before any external system (Databricks, loaders) emits real events.

One task per asset — each task sets outlets so Airflow records the asset
and marks the event as a registration (source="asset_registration").
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset


_PROJECT_ROOT = Path(__file__).parents[1]
_CONFIG_PATH = _PROJECT_ROOT / "config" / "dag_config.yaml"

with _CONFIG_PATH.open() as _fh:
    _cfg: dict[str, Any] = yaml.safe_load(_fh)

_ASSET_CONFIGS: list[dict[str, Any]] = _cfg.get("assets", [])

if not _ASSET_CONFIGS:
    raise ValueError(f"No assets defined in {_CONFIG_PATH}")

_assets: dict[str, Asset] = {
    ac["name"]: Asset(uri=ac["uri"])
    for ac in _ASSET_CONFIGS
}


def _make_register_task(ac: dict[str, Any], asset: Asset):
    @task(task_id=f"register__{ac['name']}", outlets=[asset])
    def _register(*, outlet_events, **_ctx: Any) -> None:
        outlet_events[asset].extra = {
            "source": "asset_registration",
            **{field: None for field in ac.get("metadata_fields", [])},
        }
        print(f"Registered  uri={ac['uri']!r}  name={ac['name']!r}")
    return _register


with DAG(
    dag_id="register_assets",
    description=(
        "Registers all assets from config/dag_config.yaml. "
        "Trigger manually once after deployment."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["assets", "setup"],
) as dag:
    for _ac in _ASSET_CONFIGS:
        _make_register_task(_ac, _assets[_ac["name"]])()
