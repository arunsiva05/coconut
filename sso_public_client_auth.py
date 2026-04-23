"""
SSO Public Client Authentication - Access Token Acquisition via Azure AD / Entra ID.

Uses MSAL's PublicClientApplication (no client secret required).
Supports two flows:
  - Device Code Flow  (default, works in headless/CI environments)
  - Interactive Browser Flow (opens a browser window)

Usage:
    python sso_public_client_auth.py
    python sso_public_client_auth.py --flow interactive
    python sso_public_client_auth.py --flow interactive --redirect-port 8400
    python sso_public_client_auth.py --scopes "User.Read" "https://storage.azure.com/.default"
    python sso_public_client_auth.py --groups            # also fetch group claims
    python sso_public_client_auth.py --groups --resolve  # resolve group IDs to display names

Environment variables (override config defaults):
    SSO_CLIENT_ID    - Azure AD application (client) ID  [required]
    SSO_TENANT_ID    - Azure AD tenant ID or "common"/"organizations"  [required]
    SSO_SCOPES       - Space-separated scopes  [optional, default: User.Read]
    SSO_CACHE_FILE   - Path to persist the token cache  [optional]

Interactive browser flow — redirect URI setup:
  MSAL starts a local HTTP server to receive the auth code after login.
  You must register the redirect URI in your Azure AD app registration:

    Azure Portal → App registrations → <your app>
    → Authentication → Add a platform → Mobile and desktop applications
    → Add:  http://localhost

  Registering bare "http://localhost" covers all ports automatically for
  public clients.  If your policy requires a fixed port, register the exact
  URI (e.g. http://localhost:8400) and pass --redirect-port 8400.

  Device code flow does NOT use a redirect URI — use it for headless/CI.

Group claims notes:
  Azure AD embeds group object IDs in the token only when the user belongs to
  <=200 groups. Above that threshold ("overage"), the token contains a
  _claim_names.groups hint and groups must be fetched from Microsoft Graph.
  The --groups flag handles both cases automatically.  To resolve group IDs to
  display names add --resolve (requires GroupMember.Read.All or
  Directory.Read.All scope).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]

# Scopes required for group membership lookups via Graph
GROUP_READ_SCOPES = ["User.Read", "GroupMember.Read.All"]


# ---------------------------------------------------------------------------
# Token cache helpers
# ---------------------------------------------------------------------------

def _load_cache(cache_path: str) -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    path = Path(cache_path)
    if path.exists():
        cache.deserialize(path.read_text())
    return cache


def _save_cache(cache: msal.SerializableTokenCache, cache_path: str) -> None:
    if cache.has_state_changed:
        Path(cache_path).write_text(cache.serialize())


# ---------------------------------------------------------------------------
# MSAL app & token acquisition
# ---------------------------------------------------------------------------

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
    print(flow["message"], flush=True)
    return app.acquire_token_by_device_flow(flow)


def acquire_via_interactive(
    app: msal.PublicClientApplication,
    scopes: list[str],
    redirect_port: int | None = None,
) -> dict:
    # MSAL spawns a local HTTP server on `redirect_port` (random if None) and
    # opens the browser to the Azure AD login page.  The registered redirect URI
    # must be "http://localhost" (covers all ports) or the exact
    # "http://localhost:<port>" if a fixed port is required by policy.
    kwargs: dict = {}
    if redirect_port is not None:
        kwargs["port"] = redirect_port
    return app.acquire_token_interactive(scopes=scopes, **kwargs)


def get_access_token(
    client_id: str,
    tenant_id: str,
    scopes: list[str],
    flow: str = "device_code",
    cache_file: str = DEFAULT_CACHE_FILE,
    redirect_port: int | None = None,
) -> str:
    """Return a valid access token, using cache when possible.

    Args:
        client_id:     Azure AD app (client) ID.
        tenant_id:     Azure AD tenant ID, "common", or "organizations".
        scopes:        List of OAuth scopes to request.
        flow:          "device_code" or "interactive".
        cache_file:    Path to persist token cache across calls.
        redirect_port: Local port for the interactive flow's redirect server.
                       None lets MSAL pick a random available port.
                       Ignored for device_code flow.

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
            result = acquire_via_interactive(app, scopes, redirect_port=redirect_port)

    _save_cache(cache, cache_file)

    if "access_token" not in result:
        error = result.get("error", "unknown_error")
        description = result.get("error_description", "No description provided.")
        raise RuntimeError(f"Token acquisition failed [{error}]: {description}")

    return result["access_token"]


# ---------------------------------------------------------------------------
# JWT decoding (no signature verification — for inspecting claims only)
# ---------------------------------------------------------------------------

def _decode_jwt_payload(token: str) -> dict[str, Any]:
    """Decode the payload section of a JWT without verifying the signature."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Token does not appear to be a JWT (expected 3 dot-separated parts).")
    payload_b64 = parts[1]
    # Add padding if needed
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload_bytes = base64.urlsafe_b64decode(payload_b64)
    return json.loads(payload_bytes)


# ---------------------------------------------------------------------------
# Microsoft Graph helpers
# ---------------------------------------------------------------------------

def _graph_get(endpoint: str, bearer_token: str) -> dict[str, Any]:
    """Perform a GET request against Microsoft Graph and return parsed JSON."""
    req = Request(
        f"{GRAPH_BASE}{endpoint}",
        headers={"Authorization": f"Bearer {bearer_token}", "Accept": "application/json"},
    )
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(
            f"Graph API error {exc.code} for {endpoint}: {body}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling Graph API: {exc.reason}") from exc


def _graph_get_all_pages(endpoint: str, bearer_token: str) -> list[dict]:
    """Fetch all pages of a Graph list endpoint (follows @odata.nextLink)."""
    items: list[dict] = []
    url = endpoint
    while url:
        # For nextLink the URL is absolute; strip the base for our helper
        if url.startswith(GRAPH_BASE):
            url = url[len(GRAPH_BASE):]
        page = _graph_get(url, bearer_token)
        items.extend(page.get("value", []))
        url = page.get("@odata.nextLink", "")
    return items


def _get_graph_token(
    client_id: str,
    tenant_id: str,
    flow: str,
    cache_file: str,
) -> str:
    """Acquire a Graph-scoped token (re-uses cache when possible)."""
    return get_access_token(
        client_id=client_id,
        tenant_id=tenant_id,
        scopes=GROUP_READ_SCOPES,
        flow=flow,
        cache_file=cache_file,
    )


# ---------------------------------------------------------------------------
# Group claims — main public function
# ---------------------------------------------------------------------------

def get_group_claims(
    access_token: str,
    client_id: str = "",
    tenant_id: str = DEFAULT_TENANT_ID,
    flow: str = "device_code",
    cache_file: str = DEFAULT_CACHE_FILE,
    resolve_names: bool = False,
) -> list[dict[str, str]]:
    """Return the group memberships for the authenticated user.

    Strategy:
      1. Decode the JWT and look for an embedded ``groups`` claim.
      2. If the ``_claim_names`` claim contains ``groups`` (overage indicator),
         the user belongs to >200 groups — fetch them from Graph API instead.
      3. Optionally resolve group object IDs to display names via Graph.

    Args:
        access_token:   A valid Azure AD access token for the signed-in user.
        client_id:      Azure AD app client ID (needed for Graph token if overage).
        tenant_id:      Azure AD tenant ID.
        flow:           Auth flow for acquiring Graph token if needed.
        cache_file:     Token cache path.
        resolve_names:  When True, resolve each group ID to its display name.

    Returns:
        List of dicts with at least ``id`` and optionally ``displayName``.
    """
    try:
        claims = _decode_jwt_payload(access_token)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not decode access token: {exc}") from exc

    overage = "groups" in claims.get("_claim_names", {})

    if overage:
        # Groups omitted from token — must call Graph
        if not client_id:
            raise ValueError(
                "Group overage detected (user in >200 groups). "
                "Provide client_id so a Graph token can be acquired."
            )
        print(
            "[info] Group overage indicator found — fetching memberships from Microsoft Graph.",
            file=sys.stderr,
        )
        graph_token = _get_graph_token(client_id, tenant_id, flow, cache_file)
        members = _graph_get_all_pages(
            "/me/transitiveMemberOf/microsoft.graph.group"
            "?$select=id,displayName,mailNickname,groupTypes",
            graph_token,
        )
        return [
            {
                "id": g["id"],
                "displayName": g.get("displayName", ""),
                "mailNickname": g.get("mailNickname", ""),
            }
            for g in members
        ]

    group_ids: list[str] = claims.get("groups", [])
    if not group_ids:
        return []

    if not resolve_names:
        return [{"id": gid} for gid in group_ids]

    # Resolve IDs → display names via Graph (batch by 20 using $filter)
    if not client_id:
        raise ValueError(
            "client_id is required to resolve group names via Microsoft Graph."
        )
    graph_token = _get_graph_token(client_id, tenant_id, flow, cache_file)

    resolved: list[dict[str, str]] = []
    for i in range(0, len(group_ids), 20):
        batch = group_ids[i : i + 20]
        ids_filter = " or ".join(f"id eq '{gid}'" for gid in batch)
        page = _graph_get(
            f"/groups?$filter={ids_filter}&$select=id,displayName,mailNickname",
            graph_token,
        )
        resolved.extend(
            {
                "id": g["id"],
                "displayName": g.get("displayName", ""),
                "mailNickname": g.get("mailNickname", ""),
            }
            for g in page.get("value", [])
        )
    return resolved


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Obtain an SSO access token (and optionally group claims) via Azure AD public client auth.",
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
        "--redirect-port",
        type=int,
        default=None,
        metavar="PORT",
        help=(
            "Local port for the interactive flow redirect server "
            "(e.g. 8400). Random if omitted. "
            "Register 'http://localhost:<PORT>' in your Azure AD app if fixed. "
            "Ignored for device_code flow."
        ),
    )
    parser.add_argument(
        "--cache-file",
        default=DEFAULT_CACHE_FILE,
        help="File path for token cache persistence  [env: SSO_CACHE_FILE]",
    )
    parser.add_argument(
        "--groups",
        action="store_true",
        help="Fetch and print group claims for the signed-in user",
    )
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Resolve group object IDs to display names (requires --groups; needs GroupMember.Read.All scope)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output results as JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # --- Acquire access token ---
    cache = _load_cache(args.cache_file)
    app = _build_app(args.client_id, args.tenant_id, cache)
    result = _get_cached_token(app, args.scopes)

    if result is None:
        if args.flow == "device_code":
            result = acquire_via_device_code(app, args.scopes)
        else:
            result = acquire_via_interactive(app, args.scopes, redirect_port=args.redirect_port)

    _save_cache(cache, args.cache_file)

    if "access_token" not in result:
        error = result.get("error", "unknown_error")
        description = result.get("error_description", "No description.")
        print(f"ERROR [{error}]: {description}", file=sys.stderr)
        sys.exit(1)

    access_token = result["access_token"]

    # --- Output token ---
    if not args.groups:
        if args.output_json:
            print(json.dumps(result, indent=2))
        else:
            print(access_token)
        return

    # --- Fetch group claims ---
    try:
        groups = get_group_claims(
            access_token=access_token,
            client_id=args.client_id,
            tenant_id=args.tenant_id,
            flow=args.flow,
            cache_file=args.cache_file,
            resolve_names=args.resolve,
        )
    except (ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.output_json:
        print(json.dumps({"access_token": access_token, "groups": groups}, indent=2))
    else:
        print(f"\nGroup memberships ({len(groups)} total):")
        for g in groups:
            name = g.get("displayName") or g.get("mailNickname") or ""
            suffix = f"  ({name})" if name else ""
            print(f"  {g['id']}{suffix}")


if __name__ == "__main__":
    main()
