"""
Airflow Asset Emitter — for Databricks notebooks and external loaders
=====================================================================

Use this to emit Airflow asset events with metadata from any Python
environment (Databricks notebooks, scripts, CI pipelines, etc.).

No third-party libraries required — standard library only.

─────────────────────────────────────────────────────────────────────────────
How it works
─────────────────────────────────────────────────────────────────────────────

Calls POST /api/v2/assets/events on the Airflow webserver.
Airflow records the event and automatically triggers any DAG that is
scheduled on the matching asset (AssetAll / AssetAny).

─────────────────────────────────────────────────────────────────────────────
Quick start
─────────────────────────────────────────────────────────────────────────────

    from emit_asset import AirflowAssetEmitter

    emitter = AirflowAssetEmitter(
        base_url="https://your-airflow.example.com",
        username="airflow_api_user",
        password="<password-or-api-token>",
    )

    emitter.emit(
        asset_uri="dbx+sql://sales_data_ready",
        row_count=15_000,
        source="databricks_job_42",
        date="20240101",
        environment="prod",
    )

─────────────────────────────────────────────────────────────────────────────
Asset URIs defined in this project  (config/dag_config.yaml)
─────────────────────────────────────────────────────────────────────────────

    dbx+sql://sales_data_ready
    dbx+sql://inventory_data_ready

─────────────────────────────────────────────────────────────────────────────
Databricks notebook usage
─────────────────────────────────────────────────────────────────────────────

    import sys
    sys.path.insert(0, "/dbfs/FileStore/shared_uploads/your_team/")
    from emit_asset import AirflowAssetEmitter

    emitter = AirflowAssetEmitter(
        base_url=dbutils.secrets.get("airflow", "base_url"),
        username=dbutils.secrets.get("airflow", "username"),
        password=dbutils.secrets.get("airflow", "password"),
    )

    emitter.emit(
        asset_uri="dbx+sql://sales_data_ready",
        source=f"databricks_job_{spark.conf.get('spark.databricks.job.id', 'local')}",
        row_count=df.count(),
        date="20240101",
    )
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from base64 import b64encode
from typing import Any


class AirflowAssetEmitter:
    """
    Lightweight client for emitting Airflow 3.x asset events via REST API.

    Parameters
    ----------
    base_url:
        Airflow webserver URL, e.g. ``"https://airflow.example.com"``.
    username:
        Airflow username for Basic Auth.
    password:
        Airflow password or API token.
    timeout:
        HTTP request timeout in seconds (default: 30).
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        timeout: int = 30,
    ) -> None:
        self._base = base_url.rstrip("/")
        _creds = b64encode(f"{username}:{password}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {_creds}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout

    def emit(self, asset_uri: str, **metadata: Any) -> dict[str, Any]:
        """
        Emit an asset event with metadata via POST /api/v2/assets/events.

        Airflow records the event and triggers any downstream asset-scheduled
        DAG (AssetAll / AssetAny) that depends on this asset.

        Parameters
        ----------
        asset_uri:
            Asset URI as defined in Airflow, e.g. ``"dbx+sql://sales_data_ready"``.
        **metadata:
            Key/value pairs stored in the event's ``extra`` field.
            All values must be JSON-serialisable.

        Returns
        -------
        dict
            Parsed JSON response from the Airflow API.

        Raises
        ------
        RuntimeError
            If Airflow returns a non-2xx status code.

        Example
        -------
        >>> emitter.emit(
        ...     "dbx+sql://sales_data_ready",
        ...     row_count=15_000,
        ...     source="databricks_job_42",
        ...     date="20240101",
        ... )
        """
        payload: dict[str, Any] = {"uri": asset_uri, "extra": metadata}
        url = f"{self._base}/api/v2/assets/events"
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=body, headers=self._headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode(errors="replace")
            raise RuntimeError(
                f"Airflow API error {exc.code} for POST {url}: {error_body}"
            ) from exc
