"""
Airflow Asset Emitter — for Databricks notebooks and external loaders
=====================================================================

Copy this file into your Databricks workspace (or install it as part of a
shared library) to emit Airflow asset events with metadata from any Python
environment.

No third-party libraries are required — only the Python standard library.

─────────────────────────────────────────────────────────────────────────────
Two approaches
─────────────────────────────────────────────────────────────────────────────

1. Direct REST API  (recommended — fastest, no DAG run overhead)
   Calls POST /api/v2/assets/events on the Airflow webserver.
   The asset event is recorded immediately and downstream asset-triggered
   DAGs are scheduled automatically.

2. DAG trigger  (use when you need an auditable Airflow run with task logs)
   Triggers the emit_asset__<name> DAG via POST /api/v2/dags/.../dagRuns.
   Requires the external_asset_emitter.py DAG file to be deployed in Airflow.

─────────────────────────────────────────────────────────────────────────────
Quick start (Databricks notebook or plain Python script)
─────────────────────────────────────────────────────────────────────────────

    from emit_asset import AirflowAssetEmitter

    emitter = AirflowAssetEmitter(
        base_url="https://your-airflow.example.com",
        username="airflow_api_user",
        password="<password-or-api-token>",
    )

    # ── Approach 1: direct REST API ───────────────────────────────────────────
    emitter.emit_via_api(
        asset_uri="dbx+sql://sales_data_ready",
        source="databricks_job_42",
        row_count=15_000,
        environment="prod",
        date="20240101",
    )

    # ── Approach 2: trigger the Airflow DAG ───────────────────────────────────
    emitter.emit_via_dag(
        asset_name="sales_data_ready",
        source="databricks_job_42",
        row_count=15_000,
        environment="prod",
        date="20240101",
    )

─────────────────────────────────────────────────────────────────────────────
Asset URIs defined in this project
─────────────────────────────────────────────────────────────────────────────

    dbx+sql://sales_data_ready
    dbx+sql://inventory_data_ready

(Add more as you add sensor checks to config/dag_config.yaml)
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from base64 import b64encode
from typing import Any


class AirflowAssetEmitter:
    """
    Lightweight HTTP client for emitting Airflow 3.x asset events.

    Uses only the Python standard library so it runs everywhere —
    Databricks notebooks, AWS Lambda, plain shell scripts.

    Parameters
    ----------
    base_url:
        Airflow webserver base URL, e.g. ``"https://airflow.example.com"``.
        A trailing slash is stripped automatically.
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

    # ── Public API ────────────────────────────────────────────────────────────

    def emit_via_api(self, asset_uri: str, **metadata: Any) -> dict[str, Any]:
        """
        Emit an asset event directly via the Airflow REST API.

        This is the fastest path — no DAG run is created.  The asset event is
        recorded in Airflow's database and any downstream asset-scheduled DAGs
        are queued automatically.

        Parameters
        ----------
        asset_uri:
            Full asset URI as defined in your Airflow DAGs, e.g.
            ``"dbx+sql://sales_data_ready"``.
        **metadata:
            Arbitrary key/value pairs stored as the event's ``extra`` field.
            All values must be JSON-serialisable.

        Returns
        -------
        dict
            Parsed JSON response from the Airflow API.

        Raises
        ------
        RuntimeError
            If the Airflow API returns a non-2xx status code.

        Example
        -------
        >>> emitter.emit_via_api(
        ...     "dbx+sql://sales_data_ready",
        ...     source="databricks_job_42",
        ...     row_count=15_000,
        ...     environment="prod",
        ...     date="20240101",
        ... )
        """
        payload: dict[str, Any] = {"uri": asset_uri, "extra": metadata}
        return self._post("/api/v2/assets/events", payload)

    def emit_via_dag(self, asset_name: str, **metadata: Any) -> dict[str, Any]:
        """
        Emit an asset event by triggering the ``emit_asset__<asset_name>`` DAG.

        This creates a real Airflow DAG run with task logs and an audit trail.
        Use this when you need full observability of the emission step itself.

        Requires the ``external_asset_emitter.py`` DAG file to be deployed and
        active in your Airflow environment.

        Parameters
        ----------
        asset_name:
            Sensor check name as defined in ``dag_config.yaml``, e.g.
            ``"sales_data_ready"``.  The DAG
            ``emit_asset__sales_data_ready`` must exist in Airflow.
        **metadata:
            Metadata forwarded as DAG run conf (becomes asset event extra).
            All values must be JSON-serialisable.

        Returns
        -------
        dict
            Parsed JSON response from the Airflow dagRuns API.

        Raises
        ------
        RuntimeError
            If the Airflow API returns a non-2xx status code.

        Example
        -------
        >>> emitter.emit_via_dag(
        ...     "sales_data_ready",
        ...     source="databricks_job_42",
        ...     row_count=15_000,
        ...     environment="prod",
        ...     date="20240101",
        ... )
        """
        dag_id = f"emit_asset__{asset_name}"
        payload: dict[str, Any] = {"conf": metadata}
        return self._post(f"/api/v2/dags/{dag_id}/dagRuns", payload)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base}{path}"
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


# ─────────────────────────────────────────────────────────────────────────────
# Databricks notebook usage example
# ─────────────────────────────────────────────────────────────────────────────
#
# Run this in a Databricks notebook cell after copying emit_asset.py to DBFS
# or your workspace files:
#
#   import sys
#   sys.path.insert(0, "/dbfs/FileStore/shared_uploads/your_team/")
#   from emit_asset import AirflowAssetEmitter
#
#   emitter = AirflowAssetEmitter(
#       base_url=dbutils.secrets.get("airflow", "base_url"),
#       username=dbutils.secrets.get("airflow", "username"),
#       password=dbutils.secrets.get("airflow", "password"),
#   )
#
#   # Emit once your data processing is done:
#   emitter.emit_via_api(
#       asset_uri="dbx+sql://sales_data_ready",
#       source=f"databricks_job_{spark.conf.get('spark.databricks.job.id', 'local')}",
#       row_count=df.count(),
#       environment="prod",
#       date="20240101",
#   )
