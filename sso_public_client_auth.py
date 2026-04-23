"""
SSO Public Client Authentication - Access Token Acquisition via Azure AD / Entra ID.

Uses MSAL's PublicClientApplication (no client secret required).
Supports two flows:
  - Device Code Flow  (default, works in headless/CI environments)
  - Interactive Browser Flow (opens a browser window)

Usage:
    python sso_public_client_auth.py
    python sso_public_client_auth.py --flow interactive
    python sso_public_client_auth.py --scopes "User.Read" "https://storage.azure.com/.default"

Environment variables (override config defaults):
    SSO_CLIENT_ID    - Azure AD application (client) ID  [required]
    SSO_TENANT_ID    - Azure AD tenant ID or "common"/"organizations"  [required]
    SSO_SCOPES       - Space-separated scopes  [optional, default: User.Read]
    SSO_CACHE_FILE   - Path to persist the token cache  [optional]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import msal

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default configuration — override via environment variables or CLI args
# ---------------------------------------------------------------------------
DEFAULT_CLIENT_ID: str = os.environ.get("SSO_CLIENT_ID", "")
DEFAULT_TENANT_ID: str = os.environ.get("SSO_TENANT_ID", "common")
DEFAULT_SCOPES: list[str] = os.environ.get("SSO_SCOPES", "User.Read").split()
DEFAULT_CACHE_FILE: str = os.environ.get("SSO_CACHE_FILE", ".token_cache.json")

AUTHORITY_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}"


def _load_cache(cache_path: str) -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    path = Path(cache_path)
    if path.exists():
        cache.deserialize(path.read_text())
    return cache


def _save_cache(cache: msal.SerializableTokenCache, cache_path: str) -> None:
    if cache.has_state_changed:
        Path(cache_path).write_text(cache.serialize())


def _build_app(
    client_id: str,
    tenant_id: str,
    cache: msal.SerializableTokenCache,
) -> msal.PublicClientApplication:
    return msal.PublicClientApplication(
        client_id=client_id,
        authority=AUTHORITY_TEMPLATE.format(tenant_id=tenant_id),
        token_cache=cache,
    )


def _get_cached_token(
    app: msal.PublicClientApplication,
    scopes: list[str],
) -> dict | None:
    accounts = app.get_accounts()
    if not accounts:
        return None
    result = app.acquire_token_silent(scopes, account=accounts[0])
    if result and "access_token" in result:
        logger.info("Token served from cache for account: %s", accounts[0]["username"])
        return result
    return None


def acquire_via_device_code(
    app: msal.PublicClientApplication,
    scopes: list[str],
) -> dict:
    flow = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in flow:
        raise RuntimeError(
            f"Failed to initiate device code flow: {flow.get('error_description', flow)}"
        )
    # Print the instruction so the user knows what to do
    print(flow["message"], flush=True)
    result = app.acquire_token_by_device_flow(flow)
    return result


def acquire_via_interactive(
    app: msal.PublicClientApplication,
    scopes: list[str],
) -> dict:
    result = app.acquire_token_interactive(scopes=scopes)
    return result


def get_access_token(
    client_id: str,
    tenant_id: str,
    scopes: list[str],
    flow: str = "device_code",
    cache_file: str = DEFAULT_CACHE_FILE,
) -> str:
    """Return a valid access token, using cache when possible.

    Args:
        client_id:  Azure AD app (client) ID.
        tenant_id:  Azure AD tenant ID, "common", or "organizations".
        scopes:     List of OAuth scopes to request.
        flow:       "device_code" or "interactive".
        cache_file: Path to persist token cache across calls.

    Returns:
        Access token string.

    Raises:
        ValueError:  If client_id is missing or flow is invalid.
        RuntimeError: If token acquisition fails.
    """
    if not client_id:
        raise ValueError(
            "client_id is required. Set SSO_CLIENT_ID env var or pass --client-id."
        )
    if flow not in ("device_code", "interactive"):
        raise ValueError(f"Unsupported flow '{flow}'. Use 'device_code' or 'interactive'.")

    cache = _load_cache(cache_file)
    app = _build_app(client_id, tenant_id, cache)

    result = _get_cached_token(app, scopes)

    if result is None:
        if flow == "device_code":
            result = acquire_via_device_code(app, scopes)
        else:
            result = acquire_via_interactive(app, scopes)

    _save_cache(cache, cache_file)

    if "access_token" not in result:
        error = result.get("error", "unknown_error")
        description = result.get("error_description", "No description provided.")
        raise RuntimeError(f"Token acquisition failed [{error}]: {description}")

    return result["access_token"]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Obtain an SSO access token using Azure AD public client auth.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--client-id",
        default=DEFAULT_CLIENT_ID,
        help="Azure AD application (client) ID  [env: SSO_CLIENT_ID]",
    )
    parser.add_argument(
        "--tenant-id",
        default=DEFAULT_TENANT_ID,
        help="Azure AD tenant ID or 'common'/'organizations'  [env: SSO_TENANT_ID]",
    )
    parser.add_argument(
        "--scopes",
        nargs="+",
        default=DEFAULT_SCOPES,
        metavar="SCOPE",
        help="OAuth scopes to request  [env: SSO_SCOPES]",
    )
    parser.add_argument(
        "--flow",
        choices=["device_code", "interactive"],
        default="device_code",
        help="Authentication flow to use (default: device_code)",
    )
    parser.add_argument(
        "--cache-file",
        default=DEFAULT_CACHE_FILE,
        help="File path for token cache persistence  [env: SSO_CACHE_FILE]",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output the full token response as JSON instead of just the access token",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    cache = _load_cache(args.cache_file)
    app = _build_app(args.client_id, args.tenant_id, cache)
    result = _get_cached_token(app, args.scopes)

    if result is None:
        if args.flow == "device_code":
            result = acquire_via_device_code(app, args.scopes)
        else:
            result = acquire_via_interactive(app, args.scopes)

    _save_cache(cache, args.cache_file)

    if "access_token" not in result:
        error = result.get("error", "unknown_error")
        description = result.get("error_description", "No description.")
        print(f"ERROR [{error}]: {description}", file=sys.stderr)
        sys.exit(1)

    if args.output_json:
        safe_result = {k: v for k, v in result.items() if k != "access_token"}
        safe_result["access_token"] = result["access_token"]
        print(json.dumps(safe_result, indent=2))
    else:
        print(result["access_token"])


if __name__ == "__main__":
    main()
