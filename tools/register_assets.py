#!/usr/bin/env python3
"""
Register Airflow assets from config/dag_config.yaml
====================================================

Reads the assets section from config and ensures each asset exists in
Airflow.  Airflow creates assets implicitly on first event, so this script
emits a one-time registration event for any asset not yet known.

Already-registered assets are left untouched.

Usage
-----
    python tools/register_assets.py \\
        --airflow-url https://airflow.example.com \\
        --username admin \\
        --password secret

    # Preview without writing anything
    python tools/register_assets.py \\
        --airflow-url https://airflow.example.com \\
        --username admin \\
        --password secret \\
        --dry-run

    # Point at a different config
    python tools/register_assets.py \\
        --airflow-url https://airflow.example.com \\
        --username admin \\
        --password secret \\
        --config path/to/dag_config.yaml

Dependencies
------------
    stdlib only + PyYAML (pip install pyyaml)
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from pathlib import Path
from typing import Any

import yaml


# ─────────────────────────────────────────────────────────────────────────────
# Airflow REST client (minimal — GET assets list + POST asset event)
# ─────────────────────────────────────────────────────────────────────────────

class _AirflowClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 30) -> None:
        self._base = base_url.rstrip("/")
        creds = b64encode(f"{username}:{password}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = f"{self._base}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=self._headers)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"GET {url} → {exc.code}: {body}") from exc

    def _post(self, path: str, payload: dict[str, Any]) -> Any:
        url = f"{self._base}{path}"
        body = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=body, headers=self._headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"POST {url} → {exc.code}: {body}") from exc

    def list_assets(self) -> set[str]:
        """Return the set of all asset URIs currently registered in Airflow."""
        uris: set[str] = set()
        offset = 0
        limit = 100
        while True:
            resp = self._get("/api/v2/assets", {"limit": str(limit), "offset": str(offset)})
            for asset in resp.get("assets", []):
                uris.add(asset["uri"])
            total = resp.get("total_entries", 0)
            offset += limit
            if offset >= total:
                break
        return uris

    def emit_event(self, uri: str, extra: dict[str, Any]) -> None:
        """Emit an asset event — creates the asset in Airflow if it doesn't exist."""
        self._post("/api/v2/assets/events", {"uri": uri, "extra": extra})


# ─────────────────────────────────────────────────────────────────────────────
# Config loader
# ─────────────────────────────────────────────────────────────────────────────

def _load_assets(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open() as fh:
        cfg = yaml.safe_load(fh)
    assets = cfg.get("assets", [])
    if not assets:
        raise ValueError(f"No assets found in {config_path}. Add an 'assets' section.")
    for entry in assets:
        if "name" not in entry or "uri" not in entry:
            raise ValueError(f"Each asset must have 'name' and 'uri'. Got: {entry}")
    return assets


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    default_config = Path(__file__).parents[1] / "config" / "dag_config.yaml"

    parser = argparse.ArgumentParser(
        description="Register Airflow assets from config/dag_config.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--airflow-url", required=True, help="Airflow webserver URL")
    parser.add_argument("--username", required=True, help="Airflow username")
    parser.add_argument("--password", required=True, help="Airflow password or API token")
    parser.add_argument(
        "--config",
        default=str(default_config),
        help=f"Path to dag_config.yaml (default: {default_config})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be registered without making any API calls",
    )
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: config file not found: {config_path}", file=sys.stderr)
        return 1

    # Load assets from config
    try:
        asset_configs = _load_assets(config_path)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Config: {config_path}")
    print(f"Assets in config: {len(asset_configs)}")
    print()

    if args.dry_run:
        print("DRY RUN — no API calls will be made\n")
        for ac in asset_configs:
            print(f"  would register  {ac['uri']!r}  ({ac['name']})")
        return 0

    client = _AirflowClient(args.airflow_url, args.username, args.password)

    # Fetch assets already known to Airflow
    print("Fetching registered assets from Airflow …")
    try:
        existing_uris = client.list_assets()
    except RuntimeError as exc:
        print(f"ERROR fetching assets: {exc}", file=sys.stderr)
        return 1
    print(f"Already registered: {len(existing_uris)}\n")

    registered = 0
    skipped = 0
    errors = 0

    for ac in asset_configs:
        uri: str = ac["uri"]
        name: str = ac["name"]

        if uri in existing_uris:
            print(f"  [exists]     {uri!r}  ({name})")
            skipped += 1
            continue

        # Build registration event extra from metadata_fields (all None at registration)
        extra: dict[str, Any] = {
            "source": "asset_registration",
            **{field: None for field in ac.get("metadata_fields", [])},
        }

        try:
            client.emit_event(uri, extra)
            print(f"  [registered] {uri!r}  ({name})")
            registered += 1
        except RuntimeError as exc:
            print(f"  [ERROR]      {uri!r}  ({name}): {exc}", file=sys.stderr)
            errors += 1

    print(f"\nDone — registered: {registered}, already existed: {skipped}, errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
