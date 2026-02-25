# dags/blob_dag_factory.py
# Airflow 3.1.6 — Dynamic DAG Factory (Astronomer Runtime compatible)
# Azure Blob Storage → Asset → Databricks Jobs (parallel) → Coordinator → Re-trigger
#
# ─── Architecture ─────────────────────────────────────────────────────────────
#
#   PRODUCER DAG (blob_sensor__{file_id})
#   ┌──────────────────────────────────────────┐
#   │  wait_for_blob  ──→  emit_blob_asset     │
#   │  (WasbBlobSensor)    (outlets=[blob_asset])│
#   │  deferrable=True      yields blob Asset  │
#   │  soft_fail=True                          │
#   └──────────────┬───────────────────────────┘
#                  │ blob Asset event (fan-out to ALL job consumers in parallel)
#        ┌─────────┼─────────┐
#        ▼         ▼         ▼
#   CONSUMER      CONSUMER   ...
#   databricks__{file_id}__{alias_1}
#   databricks__{file_id}__{alias_2}
#   ┌──────────────────────────────┐
#   │ log_metadata                 │   Each consumer independently:
#   │ → trigger_databricks_job     │   - reads blob metadata from Asset event
#   │ → emit_job_done Asset        │   - runs its Databricks job
#   └──────────────────────────────┘   - emits a job-done Asset on success
#        │ job_done Asset (alias_1)
#        │ job_done Asset (alias_2)  ← ALL must fire (AND logic, Airflow native)
#        ▼
#   COORDINATOR DAG (coordinator__{file_id})
#   ┌──────────────────────────────────────────────┐
#   │ verify_file_moved → re_trigger_producer      │
#   └──────────────────────────────────────────────┘
#        │
#        ▼
#   PRODUCER DAG (new run — waits for next file)
#
#   Eventually:
#     wait_for_blob times out → soft_fail=True → SKIP
#     emit_blob_asset skipped → no blob Asset → job consumers not triggered
#     Chain stops naturally. No failure.
#
# ─── date_mode / custom_date ─────────────────────────────────────────────────
#
#   DAG Run params, NOT YAML fields. Passed at trigger time:
#     {"date_mode": "today"}                             (default)
#     {"date_mode": "yesterday"}                         (previous weekday)
#     {"date_mode": "custom_date", "custom_date": "20240315"}
#
# ─── YAML config keys ────────────────────────────────────────────────────────
#
#   file_id, display_name, container, folder_pattern, file_pattern,
#   schedule, timeout_hours,
#   poke_interval_seconds (optional, default 300)
#   databricks_jobs:                          <- LIST of one or more jobs
#     - job_alias: <short_name>               <- used in DAG id + asset URI
#       job_id: <int>
#       job_parameters:
#         catalog_name, label_data, volume_basepath, event_type
#
# ─── Example YAML ────────────────────────────────────────────────────────────
#
#   files:
#     - file_id: sales_report
#       display_name: Daily Sales Report
#       container: landing
#       folder_pattern: sales/{yyyymmdd}
#       file_pattern: sales_{yyyymmdd}.csv
#       schedule: "0 7 * * 1-5"
#       timeout_hours: 4
#       databricks_jobs:
#         - job_alias: transform
#           job_id: 12345
#           job_parameters:
#             catalog_name: prod
#             label_data: sales
#             volume_basepath: /mnt/sales
#             event_type: daily_load
#         - job_alias: publish
#           job_id: 67890
#           job_parameters:
#             catalog_name: prod
#             label_data: sales_pub
#             volume_basepath: /mnt/publish
#             event_type: daily_publish

from __future__ import annotations

import functools
import logging
import re
import yaml
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Generator

# ── Airflow 3.x SDK imports (Astronomer Runtime compatible) ────────────────
# All core symbols (DAG, task, Asset, Param, etc.) live in airflow.sdk in Airflow 3.x
from airflow.sdk import DAG, Asset, Metadata, Variable, Param, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AZURE_CONN_ID      = "azure_blob_conn"
# Replace with your actual Azure Storage account name.
# Used for Airflow Asset URIs and as a fallback account name when the
# Airflow connection's 'host' field is not set (prevents the
# 'None.dfs.core.windows.net' ADLS connection error).
STORAGE_ACCOUNT    = "your_storage_account"
DATABRICKS_CONN_ID = "databricks_default"

VALID_DATE_MODES          = frozenset({"today", "yesterday", "custom_date"})
DEFAULT_DATE_MODE         = "today"
DEFAULT_POKE_INTERVAL_SEC = 300

REQUIRED_CONFIG_FIELDS = frozenset({
    "file_id",
    "display_name",
    "container",
    "folder_pattern",
    "file_pattern",
    "databricks_jobs",       # list of {job_alias, job_id, job_parameters}
    "schedule",
    "timeout_hours",
    # date_mode / custom_date intentionally NOT here -- they are DAG run params
    # use_dbx_archive_job intentionally NOT here -- optional, defaults to True
})

REQUIRED_DATABRICKS_JOB_KEYS = frozenset({
    "job_alias",
    "job_id",
    "job_parameters",
})

REQUIRED_DATABRICKS_JOB_PARAM_KEYS = frozenset({
    "catalog_name",
    "label_data",
    "volume_basepath",
    "event_type",
})

REQUIRED_METADATA_KEYS = frozenset({
    "file_id",
    "file_name",
    "file_path",
    "file_location",
    "blob_url",
    "file_timestamp",
    "size_bytes",
    "etag",
    "valuation_date",
    "execution_date",
    "container",
})


# ─────────────────────────────────────────────────────────────────────────────
# DAG-level Params definition (shared across every producer DAG)
# ─────────────────────────────────────────────────────────────────────────────

DATE_MODE_PARAMS = {
    "date_mode": Param(
        default=DEFAULT_DATE_MODE,
        type="string",
        enum=["today", "yesterday", "custom_date"],
        description=(
            "Which date to substitute into {yyyymmdd} placeholders.\n"
            "  today       -> logical_date\n"
            "  yesterday   -> previous weekday (Mon->Fri, Sat->Fri, Sun->Fri, else -1 day)\n"
            "  custom_date -> value of the 'custom_date' param below"
        ),
    ),
    "custom_date": Param(
        default=None,
        type=["null", "string"],
        description=(
            "Only required when date_mode='custom_date'. "
            "Must be exactly 8 digits: YYYYMMDD (e.g. '20240315'). "
            "Leave blank/null for today or yesterday modes."
        ),
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Jinja template -- resolves {yyyymmdd} for WasbBlobSensor at runtime
# ─────────────────────────────────────────────────────────────────────────────

_YYYYMMDD_JINJA = (
    # yesterday -> previous weekday (Mon->Fri skip 3, Sat->Fri skip 1, Sun->Fri skip 2, else skip 1)
    "{{ (logical_date - macros.timedelta(days=3)).strftime('%Y%m%d') "
    "if params.date_mode == 'yesterday' and logical_date.weekday() == 0 "
    "else (logical_date - macros.timedelta(days=2)).strftime('%Y%m%d') "
    "if params.date_mode == 'yesterday' and logical_date.weekday() == 6 "
    "else (logical_date - macros.timedelta(days=1)).strftime('%Y%m%d') "
    "if params.date_mode == 'yesterday' "
    "else (params.custom_date if params.date_mode == 'custom_date' "
    "else logical_date.strftime('%Y%m%d')) }}"
)


def _make_blob_name_template(folder_pattern: str, file_pattern: str) -> str:
    """
    Replaces {yyyymmdd} in folder_pattern and file_pattern with a Jinja
    expression so WasbBlobSensor resolves the dated blob path at execution time.
    """
    folder_tpl = folder_pattern.replace("{yyyymmdd}", _YYYYMMDD_JINJA)
    file_tpl   = file_pattern.replace("{yyyymmdd}", _YYYYMMDD_JINJA)
    return f"{folder_tpl}/{file_tpl}"


# ─────────────────────────────────────────────────────────────────────────────
# Date resolution helper (used by @task at runtime -- not Jinja)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_date_str(date_mode: str, logical_date: Any, custom_date: str | None) -> str:
    """
    Returns the effective YYYYMMDD string based on date_mode from DAG run params.

    date_mode="today"       -> logical_date as YYYYMMDD
    date_mode="yesterday"   -> previous weekday as YYYYMMDD
                               Mon->Fri (-3), Sat->Fri (-1), Sun->Fri (-2), else -1
    date_mode="custom_date" -> custom_date param value (static, run-independent)
    """
    if date_mode not in VALID_DATE_MODES:
        raise ValueError(
            f"date_mode='{date_mode}' is not valid. "
            f"Must be one of: {sorted(VALID_DATE_MODES)}"
        )

    if date_mode == "today":
        if logical_date is None:
            raise ValueError(
                "date_mode='today' requires logical_date but it is None. "
                "Ensure the DAG has a schedule configured."
            )
        return logical_date.strftime("%Y%m%d")

    if date_mode == "yesterday":
        if logical_date is None:
            raise ValueError(
                "date_mode='yesterday' requires logical_date but it is None. "
                "Ensure the DAG has a schedule configured."
            )
        # Return the previous WEEKDAY (Mon-Fri), skipping weekends.
        day_of_week = logical_date.weekday()  # Mon=0 ... Sun=6
        if day_of_week == 0:    # Monday -> Friday
            delta = 3
        elif day_of_week == 6:  # Sunday -> Friday
            delta = 2
        elif day_of_week == 5:  # Saturday -> Friday
            delta = 1
        else:                   # Tue-Fri -> previous calendar day
            delta = 1
        return (logical_date - timedelta(days=delta)).strftime("%Y%m%d")

    # custom_date
    date_str = str(custom_date or "").strip()
    if not date_str:
        raise ValueError(
            "date_mode='custom_date' requires the 'custom_date' run param "
            "to be set (e.g. '20240315'). It is currently empty."
        )
    if not re.fullmatch(r"\d{8}", date_str):
        raise ValueError(
            f"'custom_date' must be exactly 8 digits (YYYYMMDD), got: '{date_str}'"
        )
    return date_str


# ─────────────────────────────────────────────────────────────────────────────
# Validator Decorators
# ─────────────────────────────────────────────────────────────────────────────

def validate_config(fn: Callable) -> Callable:
    """
    Decorator -- validates the config dict at parse time (when factory is called).
    date_mode / custom_date are NOT validated here -- they arrive at runtime as params.
    """
    @functools.wraps(fn)
    def wrapper(cfg: dict, *args, **kwargs):
        fid = cfg.get("file_id", "UNKNOWN")

        missing = REQUIRED_CONFIG_FIELDS - cfg.keys()
        if missing:
            raise ValueError(
                f"[{fid}] Config is missing required fields: {sorted(missing)}"
            )
        if not isinstance(cfg["file_id"], str) or not cfg["file_id"].strip():
            raise TypeError(f"[{fid}] 'file_id' must be a non-empty string.")
        if not isinstance(cfg["container"], str) or not cfg["container"].strip():
            raise TypeError(f"[{fid}] 'container' must be a non-empty string.")
        if "{yyyymmdd}" not in cfg["folder_pattern"]:
            raise ValueError(
                f"[{fid}] 'folder_pattern' must contain the {{yyyymmdd}} placeholder. "
                f"Got: '{cfg['folder_pattern']}'"
            )
        if "{yyyymmdd}" not in cfg["file_pattern"]:
            raise ValueError(
                f"[{fid}] 'file_pattern' must contain the {{yyyymmdd}} placeholder. "
                f"Got: '{cfg['file_pattern']}'"
            )
        if "*" in cfg["file_pattern"]:
            raise ValueError(
                f"[{fid}] 'file_pattern' must NOT contain wildcards (*). "
                f"Got: '{cfg['file_pattern']}'. "
                "Rename files to use exact names."
            )
        if not isinstance(cfg["timeout_hours"], (int, float)) or cfg["timeout_hours"] <= 0:
            raise TypeError(
                f"[{fid}] 'timeout_hours' must be a positive number. "
                f"Got: {cfg['timeout_hours']!r}"
            )

        # ── Validate databricks_jobs list ─────────────────────────────────
        dbx_jobs = cfg["databricks_jobs"]
        if not isinstance(dbx_jobs, list) or len(dbx_jobs) == 0:
            raise TypeError(
                f"[{fid}] 'databricks_jobs' must be a non-empty list. "
                f"Got: {type(dbx_jobs).__name__}"
            )
        seen_aliases: set[str] = set()
        for i, job in enumerate(dbx_jobs):
            if not isinstance(job, dict):
                raise TypeError(
                    f"[{fid}] databricks_jobs[{i}] must be a mapping, "
                    f"got: {type(job).__name__}"
                )
            missing_job = REQUIRED_DATABRICKS_JOB_KEYS - job.keys()
            if missing_job:
                raise ValueError(
                    f"[{fid}] databricks_jobs[{i}] missing keys: {sorted(missing_job)}"
                )
            alias = job["job_alias"]
            if not isinstance(alias, str) or not alias.strip():
                raise TypeError(
                    f"[{fid}] databricks_jobs[{i}] 'job_alias' must be a non-empty string."
                )
            if alias in seen_aliases:
                raise ValueError(
                    f"[{fid}] Duplicate job_alias '{alias}' in databricks_jobs."
                )
            seen_aliases.add(alias)
            if not isinstance(job["job_id"], int) or job["job_id"] <= 0:
                raise TypeError(
                    f"[{fid}] databricks_jobs[{i}] 'job_id' must be a positive integer. "
                    f"Got: {job['job_id']!r}"
                )
            params = job["job_parameters"]
            if not isinstance(params, dict):
                raise TypeError(
                    f"[{fid}] databricks_jobs[{i}] 'job_parameters' must be a mapping."
                )
            missing_params = REQUIRED_DATABRICKS_JOB_PARAM_KEYS - params.keys()
            if missing_params:
                raise ValueError(
                    f"[{fid}] databricks_jobs[{i}] (alias='{alias}') "
                    f"'job_parameters' missing keys: {sorted(missing_params)}"
                )

        # ── Validate poke_interval vs timeout ─────────────────────────────
        poke = cfg.get("poke_interval_seconds", DEFAULT_POKE_INTERVAL_SEC)
        if not isinstance(poke, (int, float)) or poke <= 0:
            raise TypeError(
                f"[{fid}] 'poke_interval_seconds' must be a positive number. "
                f"Got: {poke!r}"
            )
        timeout_seconds = cfg["timeout_hours"] * 3600
        if poke >= timeout_seconds:
            raise ValueError(
                f"[{fid}] 'poke_interval_seconds' ({poke}s) must be less than "
                f"timeout in seconds ({timeout_seconds}s)."
            )

        return fn(cfg, *args, **kwargs)

    return wrapper


def validate_blob_properties(fn: Callable) -> Callable:
    """
    Decorator -- validates Azure blob properties before metadata is built.
    Signature of wrapped fn: fn(props, blob_name, file_id, **kwargs)
    """
    @functools.wraps(fn)
    def wrapper(props: Any, blob_name: str, file_id: str, **kwargs):
        if props is None:
            raise ValueError(
                f"[{file_id}] get_blob_properties() returned None for: '{blob_name}'"
            )
        if not getattr(props, "etag", None) or not str(props.etag).strip():
            raise ValueError(
                f"[{file_id}] ETag missing/empty for blob '{blob_name}'."
            )
        if getattr(props, "last_modified", None) is None:
            raise ValueError(
                f"[{file_id}] 'last_modified' is None for blob: '{blob_name}'"
            )
        if getattr(props, "size", None) is None or props.size < 0:
            raise ValueError(
                f"[{file_id}] Blob size invalid ({props.size!r}) for: '{blob_name}'"
            )
        if props.size == 0:
            raise ValueError(
                f"[{file_id}] Blob is empty (size=0): '{blob_name}'."
            )
        return fn(props, blob_name, file_id, **kwargs)

    return wrapper


def validate_metadata(fn: Callable) -> Callable:
    """
    Decorator -- validates a metadata dict before it is yielded as an Asset event.
    Signature of wrapped fn: fn(metadata, **kwargs)
    """
    @functools.wraps(fn)
    def wrapper(metadata: dict, **kwargs):
        if not isinstance(metadata, dict):
            raise TypeError(f"metadata must be a dict, got: {type(metadata).__name__}")
        missing = REQUIRED_METADATA_KEYS - metadata.keys()
        if missing:
            raise ValueError(f"Metadata missing required keys: {sorted(missing)}")
        if not str(metadata.get("etag", "")).strip():
            raise ValueError("Metadata 'etag' is empty.")
        if not isinstance(metadata.get("size_bytes"), int) or metadata["size_bytes"] <= 0:
            raise ValueError(
                f"Metadata 'size_bytes' must be a positive int, "
                f"got: {metadata.get('size_bytes')!r}"
            )
        return fn(metadata, **kwargs)

    return wrapper


def validate_xcom_metadata(fn: Callable) -> Callable:
    """
    Decorator -- validates XCom metadata in the coordinator DAG before Azure call.
    Signature of wrapped fn: fn(metadata, file_id, **kwargs)
    """
    @functools.wraps(fn)
    def wrapper(metadata: dict | None, file_id: str, **kwargs):
        if metadata is None:
            raise ValueError(
                f"[{file_id}] XCom returned None for asset metadata."
            )
        if not isinstance(metadata, dict):
            raise TypeError(
                f"[{file_id}] asset_metadata must be a dict, got: {type(metadata).__name__}"
            )
        for key in ("container", "file_path"):
            if not str(metadata.get(key, "")).strip():
                raise ValueError(
                    f"[{file_id}] asset_metadata['{key}'] is missing or empty."
                )
        return fn(metadata, file_id, **kwargs)

    return wrapper


# ─────────────────────────────────────────────────────────────────────────────
# Config loading & parse-time validation
# ─────────────────────────────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent / "config" / "blob_files.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(
        f"blob_files.yaml not found at: {CONFIG_PATH}."
    )

with open(CONFIG_PATH) as _f:
    _raw = yaml.safe_load(_f)

if not isinstance(_raw, dict) or "files" not in _raw:
    raise ValueError("blob_files.yaml must be a mapping with a top-level 'files' key.")

FILE_CONFIGS: list[dict] = _raw["files"]

if not isinstance(FILE_CONFIGS, list) or len(FILE_CONFIGS) == 0:
    raise ValueError("'files' in blob_files.yaml must be a non-empty list.")

if len(FILE_CONFIGS) > 50:
    raise ValueError(f"Found {len(FILE_CONFIGS)} configs -- maximum is 50.")

_errors: list[str] = []
_seen_ids: set[str] = set()

for _i, _cfg in enumerate(FILE_CONFIGS):
    if not isinstance(_cfg, dict):
        _errors.append(f"files[{_i}]: expected mapping, got {type(_cfg).__name__}")
        continue
    _fid = _cfg.get("file_id", f"[index {_i}]")
    _missing = REQUIRED_CONFIG_FIELDS - _cfg.keys()
    if _missing:
        _errors.append(f"[{_fid}] missing fields: {sorted(_missing)}")
    if str(_fid) in _seen_ids:
        _errors.append(f"Duplicate file_id: '{_fid}'")
    _seen_ids.add(str(_fid))

if _errors:
    raise ValueError(
        "blob_files.yaml validation errors:\n  " + "\n  ".join(_errors)
    )

log.info("blob_dag_factory: loaded %d configs from %s", len(FILE_CONFIGS), CONFIG_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# Asset registry
# ─────────────────────────────────────────────────────────────────────────────

# Blob Assets -- one per file_id, emitted by the producer
BLOB_ASSETS: dict[str, Asset] = {
    cfg["file_id"]: Asset(
        uri=f"azure://{STORAGE_ACCOUNT}/{cfg['container']}/{cfg['file_id']}",
        extra={
            "description":    cfg["display_name"],
            "container":      cfg["container"],
            "file_pattern":   cfg["file_pattern"],
            "folder_pattern": cfg["folder_pattern"],
        },
    )
    for cfg in FILE_CONFIGS
}

# Job-done Assets -- one per (file_id, job_alias), emitted by each job consumer.
# The coordinator DAG schedules on ALL of them (AND logic).
# Each event carries the full blob metadata so the coordinator can verify without
# bridging XCom across DAG runs.
JOB_DONE_ASSETS: dict[tuple[str, str], Asset] = {
    (cfg["file_id"], job["job_alias"]): Asset(
        uri=f"job_done://{cfg['file_id']}/{job['job_alias']}",
        extra={
            "description": (
                f"Databricks job '{job['job_alias']}' complete for {cfg['file_id']}"
            ),
            "file_id":   cfg["file_id"],
            "job_alias": job["job_alias"],
        },
    )
    for cfg in FILE_CONFIGS
    for job in cfg["databricks_jobs"]
}

if len(BLOB_ASSETS) != len(FILE_CONFIGS):
    raise RuntimeError("BLOB_ASSETS size mismatch -- check for duplicate file_id values.")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build metadata dict from Azure blob properties
# ─────────────────────────────────────────────────────────────────────────────

@validate_blob_properties
def _build_blob_metadata(
    props: Any,
    blob_name: str,
    file_id: str,
    *,
    cfg: dict,
    blob_client: Any,
    date_str: str,
    date_mode: str,
    execution_date: str,
) -> dict:
    """
    Builds the full metadata dict. @validate_blob_properties runs first.

    valuation_date  = resolved date from date_mode (the data date)
    execution_date  = actual DAG logical_date (the run date)
    """
    content_md5_raw = getattr(props.content_settings, "content_md5", None)
    content_md5_hex = (
        content_md5_raw.hex()
        if isinstance(content_md5_raw, (bytes, bytearray))
        else "N/A"
    )
    file_name = blob_name.split("/")[-1]
    folder    = "/".join(blob_name.split("/")[:-1])

    return {
        "file_id":         file_id,
        "display_name":    cfg["display_name"],
        "file_name":       file_name,
        "file_path":       blob_name,
        "file_location":   folder,
        "container":       cfg["container"],
        "storage_account": STORAGE_ACCOUNT,
        "blob_url":        blob_client.url,
        "file_timestamp":  props.last_modified.isoformat(),
        "size_bytes":      props.size,
        "etag":            str(props.etag),
        "content_md5":     content_md5_hex,
        "detected_at":     datetime.now(tz=timezone.utc).isoformat(),
        "valuation_date":  date_str,       # resolved date from date_mode
        "execution_date":  execution_date, # actual DAG logical_date
        "date_mode":       date_mode,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helper: validate metadata then yield blob Asset event
# ─────────────────────────────────────────────────────────────────────────────

@validate_metadata
def _yield_asset_event(metadata: dict, *, asset_obj: Asset) -> Generator[Metadata, None, None]:
    """
    Validated by @validate_metadata before the Metadata object is yielded.

    IMPORTANT -- Airflow 3.x extra metadata on asset EVENTS requires:
        yield Metadata(asset_object, extra_dict)
    NOT:
        yield Asset(uri=..., extra=metadata)
    """
    yield Metadata(asset_obj, metadata)


# ─────────────────────────────────────────────────────────────────────────────
# @task factory -- Producer: emit blob Asset event
# ─────────────────────────────────────────────────────────────────────────────

@validate_config
def make_emit_fn(cfg: dict) -> Callable:
    """
    Returns a @task that fetches blob properties, validates, and emits a blob
    Asset event with full metadata. The WasbBlobSensor upstream guarantees
    the blob exists.
    """
    file_id        = cfg["file_id"]
    container      = cfg["container"]
    folder_pattern = cfg["folder_pattern"]
    file_pattern   = cfg["file_pattern"]
    asset_obj      = BLOB_ASSETS[file_id]

    @task(task_id="emit_blob_asset", outlets=[BLOB_ASSETS[file_id]])
    def emit_blob_asset(**context) -> Generator[Metadata, None, None]:
        _file_id        = file_id
        _container      = container
        _folder_pattern = folder_pattern
        _file_pattern   = file_pattern
        _asset_obj      = asset_obj

        params      = context.get("params") or {}
        date_mode   = str(params.get("date_mode", DEFAULT_DATE_MODE)).strip()
        custom_date = str(params.get("custom_date", "")).strip() or None

        date_str  = resolve_date_str(date_mode, context.get("logical_date"), custom_date)
        folder    = _folder_pattern.replace("{yyyymmdd}", date_str)
        fname     = _file_pattern.replace("{yyyymmdd}", date_str)
        blob_name = f"{folder}/{fname}"

        if "//" in blob_name:
            raise ValueError(
                f"[{_file_id}] Resolved blob_name contains double slashes: '{blob_name}'."
            )

        log.info(
            "[%s] emit_blob_asset | date_mode=%s | date=%s | blob=%s",
            _file_id, date_mode, date_str, blob_name,
        )

        hook        = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        blob_client = hook.get_conn().get_blob_client(
            container=_container, blob=blob_name,
        )
        props = blob_client.get_blob_properties()

        metadata = _build_blob_metadata(
            props, blob_name, _file_id,
            cfg=cfg, blob_client=blob_client,
            date_str=date_str, date_mode=date_mode,
            execution_date=context["logical_date"].strftime("%Y%m%d"),
        )

        yield from _yield_asset_event(metadata, asset_obj=_asset_obj)

        log.info(
            "[OK] Blob Asset emitted | file=%s | blob=%s | size=%s bytes | etag=%s",
            _file_id, blob_name, f"{metadata['size_bytes']:,}", metadata["etag"],
        )

    return emit_blob_asset


# ─────────────────────────────────────────────────────────────────────────────
# @task factory -- Job consumer: read blob Asset metadata from triggering event
# ─────────────────────────────────────────────────────────────────────────────

@validate_config
def make_log_metadata_fn(cfg: dict) -> Callable:
    """
    Returns a @task that reads the triggering blob Asset event's extra metadata
    and auto-XComs it for downstream tasks within the same consumer DAG.
    """
    file_id    = cfg["file_id"]
    blob_asset = BLOB_ASSETS[file_id]

    @task(task_id="log_asset_metadata")
    def log_and_store_metadata(
        _file_id: str  = file_id,
        _asset: Asset  = blob_asset,
        **context,
    ) -> dict:
        """
        triggering_asset_events is keyed by Asset object (not URI string).
        Its values are lazy sequence-like accessors -- use len() and [-1],
        never isinstance(..., list).
        """
        asset_events = context.get("triggering_asset_events") or {}

        if not asset_events:
            log.warning(
                "[%s] triggering_asset_events is empty -- DAG may have been triggered manually.",
                _file_id,
            )

        metadata: dict = {}

        if _asset in asset_events:
            events = asset_events[_asset]
            if len(events) == 0:
                raise ValueError(
                    f"[{_file_id}] Asset event accessor for '{_asset.uri}' is empty."
                )
            raw_extra = events[-1].extra
            if not isinstance(raw_extra, dict):
                raise TypeError(
                    f"[{_file_id}] Asset event .extra must be a dict, "
                    f"got: {type(raw_extra).__name__}"
                )
            metadata = raw_extra
        else:
            log.warning(
                "[%s] Blob Asset '%s' not in triggering_asset_events. "
                "Databricks params will be empty.",
                _file_id, _asset.uri,
            )

        log.info("[%s] Triggering blob asset metadata: %s", _file_id, metadata)
        return metadata

    return log_and_store_metadata


# ─────────────────────────────────────────────────────────────────────────────
# Airflow Variable key for the shared archive Databricks job ID
# ─────────────────────────────────────────────────────────────────────────────

ARCHIVE_JOB_ID_VAR = "archive_job_id"
# Set this in Airflow UI / CLI:  airflow variables set archive_job_id <job_id>


# ─────────────────────────────────────────────────────────────────────────────
# @task factory -- Coordinator: verify blob was moved after all jobs complete
# ─────────────────────────────────────────────────────────────────────────────

@validate_config
def make_verify_fn(cfg: dict) -> Callable:
    """
    Returns a @task that reads blob metadata from a job-done Asset event and
    checks whether the source blob has been removed.

    Returns a dict with:
      "verified"  : bool   — True if blob is gone, False if still present
      + all blob metadata fields (for downstream archive / re-trigger tasks)
    """
    file_id             = cfg["file_id"]
    dbx_jobs            = cfg["databricks_jobs"]
    job_done_asset_list = [
        JOB_DONE_ASSETS[(file_id, job["job_alias"])] for job in dbx_jobs
    ]

    @task(task_id="verify_file_moved")
    def verify_file_moved(**context) -> dict:
        # Read frozen closure values
        _file_id         = file_id
        _job_done_assets = job_done_asset_list

        asset_events = context.get("triggering_asset_events") or {}

        # Read blob metadata from the first available job-done Asset event.
        # All job-done events carry identical blob metadata.
        metadata: dict = {}
        for asset_obj in _job_done_assets:
            if asset_obj in asset_events:
                events = asset_events[asset_obj]
                if len(events) > 0 and isinstance(events[-1].extra, dict):
                    metadata = events[-1].extra
                    log.info(
                        "[%s] Coordinator: read blob metadata from job-done Asset '%s'.",
                        _file_id, asset_obj.uri,
                    )
                    break

        if not metadata:
            raise ValueError(
                f"[{_file_id}] Coordinator: could not read blob metadata from any "
                f"job-done Asset event. Cannot verify file removal."
            )

        hook         = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        still_exists = hook.check_for_blob(
            container_name=metadata["container"],
            blob_name=metadata["file_path"],
        )

        if still_exists:
            log.warning(
                "[%s] Verification FAILED -- blob still present: container=%r path=%r. "
                "Archive job will be triggered.",
                _file_id, metadata["container"], metadata["file_path"],
            )
        else:
            log.info(
                "[%s] Verification PASSED -- blob removed from source. All jobs complete.",
                _file_id,
            )

        return {**metadata, "verified": not still_exists}

    return verify_file_moved


# ─────────────────────────────────────────────────────────────────────────────
# @task -- Coordinator: branch on verify result
# ─────────────────────────────────────────────────────────────────────────────

@task.branch(task_id="route_after_verify")
def route_after_verify(**context) -> str:
    """
    Branches based on the 'verified' flag returned by verify_file_moved.
      verified=True  → proceed to re_trigger_producer
      verified=False → proceed to archive_file

    verify_result is pulled from XCom — typed params are NOT used on @task
    in Airflow 3.x to avoid decorator introspection errors.
    """
    ti            = context["ti"]
    verify_result = ti.xcom_pull(task_ids="verify_file_moved") or {}
    if verify_result.get("verified"):
        return "re_trigger_producer"
    return "archive_file"


# ─────────────────────────────────────────────────────────────────────────────
# DAG Factory
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# DAG-level failure callback — marks DAG run SKIPPED when triggered by operator
# ─────────────────────────────────────────────────────────────────────────────

def _skip_dag_if_retriggered(context: dict) -> None:
    """
    on_failure_callback on the producer DAG.

    When any task fails (sensor timeout, connectivity error, etc.):
      triggered_by="operator" → TriggerDagRunOperator re-triggered this run
                                to watch for the next file. Mark DAG run
                                SKIPPED so the chain stops cleanly without
                                firing alerts.
      anything else           → scheduled/manual/API trigger. File genuinely
                                never arrived. Leave as FAILED so alerts fire.

    Note: run_type is NOT used — TriggerDagRunOperator sets run_type="manual"
    in Airflow 3.x, making it indistinguishable from a real manual trigger.
    dag_run.triggered_by is the reliable field: it is set exclusively to
    "operator" by TriggerDagRunOperator.
    triggered_by may be a string or an enum — both are handled below.
    """
    dag_run      = context["dag_run"]
    triggered_by = dag_run.triggered_by
    # triggered_by is a DagRunTriggeredByType enum in Airflow 3.x;
    # fall back to plain string comparison for safety.
    triggered_by_val = (
        triggered_by.value
        if hasattr(triggered_by, "value")
        else str(triggered_by)
    )

    if triggered_by_val == "operator":
        log.info(
            "on_failure_callback: triggered_by='operator' — marking DAG run SKIPPED "
            "instead of FAILED. Round circuit stops cleanly."
        )
        dag_run.set_state("skipped")
    else:
        log.info(
            "on_failure_callback: triggered_by=%r — leaving DAG run as FAILED.",
            triggered_by_val,
        )


if not FILE_CONFIGS:
    raise RuntimeError("FILE_CONFIGS is empty -- factory would produce zero DAGs.")

_total_consumer_dags = 0

for cfg in FILE_CONFIGS:
    file_id    = cfg["file_id"]
    blob_asset = BLOB_ASSETS[file_id]
    dbx_jobs   = cfg["databricks_jobs"]

    if file_id not in BLOB_ASSETS:
        raise RuntimeError(f"Blob Asset missing for file_id='{file_id}'.")

    _blob_name_tpl      = _make_blob_name_template(cfg["folder_pattern"], cfg["file_pattern"])
    _poke_interval      = int(cfg.get("poke_interval_seconds", DEFAULT_POKE_INTERVAL_SEC))
    _sensor_timeout     = int(cfg["timeout_hours"] * 3600)
    _producer_dag_id    = f"blob_sensor__{file_id}"
    _coordinator_dag_id = f"coordinator__{file_id}"
    _job_aliases_str    = ", ".join(j["job_alias"] for j in dbx_jobs)

    # All job-done Assets for this file -- coordinator schedules on all (AND logic)
    _job_done_assets = [JOB_DONE_ASSETS[(file_id, job["job_alias"])] for job in dbx_jobs]

    # ── PRODUCER DAG ──────────────────────────────────────────────────────
    with DAG(
        dag_id=_producer_dag_id,
        # schedule=cfg["schedule"],  # <- commented out for manual testing; re-enable when ready
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        params=DATE_MODE_PARAMS,
        max_active_runs=1,
        on_failure_callback=_skip_dag_if_retriggered,
        default_args={
            "retries": 0,  # sensor timeout is not retryable — fail fast or skip
        },
        tags=["producer", "azure", "blob", file_id],
        doc_md=(
            f"**Producer DAG** for `{cfg['display_name']}`.\n\n"
            f"Waits for blob then emits blob Asset -- fans out to all job consumers in parallel.\n\n"
            f"**Task 1** -- `wait_for_blob`: WasbBlobSensor (deferrable)\n"
            f"- Blob: `{cfg['container']}/{cfg['folder_pattern']}/{cfg['file_pattern']}`\n"
            f"- Poke: {_poke_interval}s | Timeout: {cfg['timeout_hours']}h | soft_fail=True\n\n"
            f"**Task 2** -- `emit_blob_asset`: emits `{blob_asset.uri}`\n\n"
            f"Triggers job consumers (parallel): `{_job_aliases_str}`\n\n"
            f"Re-triggered by coordinator `{_coordinator_dag_id}` after all jobs + verify.\n\n"
            "**Date mode** param -- pass at trigger time:\n\n"
            "```json\n"
            '{"date_mode": "today"}                          // default\n'
            '{"date_mode": "yesterday"}                      // previous weekday\n'
            '{"date_mode": "custom_date", "custom_date": "20240315"}\n'
            "```"
        ),
    ) as producer_dag:

        wait_for_blob = WasbBlobSensor(
            task_id="wait_for_blob",
            container_name=cfg["container"],
            blob_name=_blob_name_tpl,
            wasb_conn_id=AZURE_CONN_ID,
            deferrable=True,
            poke_interval=_poke_interval,
            timeout=_sensor_timeout,
            mode="poke",
            soft_fail=True,
        )

        emit_task = make_emit_fn(cfg)()

        wait_for_blob >> emit_task

    globals()[_producer_dag_id] = producer_dag

    # ── JOB CONSUMER DAGS -- one per databricks_jobs entry ────────────────
    # Each consumer:
    #   1. Triggered by the blob Asset
    #   2. Reads blob metadata from the triggering Asset event
    #   3. Runs its specific Databricks job
    #   4. Emits a job-done Asset (carrying blob metadata) for the coordinator
    for _job in dbx_jobs:
        _job_alias       = _job["job_alias"]
        _job_id          = _job["job_id"]
        _job_params      = _job["job_parameters"]
        _consumer_dag_id = f"databricks__{file_id}__{_job_alias}"
        _job_done_asset  = JOB_DONE_ASSETS[(file_id, _job_alias)]

        # Freeze loop variables as default args to prevent closure-capture bugs.
        # Variables used inside @task must be frozen here, not referenced by closure.
        _frozen_file_id    = file_id
        _frozen_job_alias  = _job_alias
        _frozen_done_asset = _job_done_asset

        @task(task_id="emit_job_done", outlets=[_frozen_done_asset])
        def _emit_job_done(**context) -> Generator[Metadata, None, None]:
            """
            Emits a job-done Asset carrying the full blob metadata dict.
            The coordinator reads this to get container + file_path for verification
            and date_mode for re-triggering the producer -- no cross-DAG XCom needed.
            metadata is pulled from XCom (log_asset_metadata task).
            """
            _file_id    = _frozen_file_id
            _alias      = _frozen_job_alias
            _done_asset = _frozen_done_asset

            ti       = context["ti"]
            metadata = ti.xcom_pull(task_ids="log_asset_metadata") or {}

            if not metadata:
                raise ValueError(
                    f"[{_file_id}/{_alias}] _emit_job_done: XCom from "
                    f"log_asset_metadata is empty. Cannot emit job-done Asset."
                )
            log.info(
                "[%s/%s] Databricks job complete -- emitting job-done Asset '%s'.",
                _file_id, _alias, _done_asset.uri,
            )
            yield Metadata(_done_asset, metadata)

        with DAG(
            dag_id=_consumer_dag_id,
            schedule=[blob_asset],   # triggered by blob Asset
            start_date=datetime(2024, 1, 1),
            catchup=False,
            max_active_runs=1,
            tags=["consumer", "databricks", file_id, _job_alias],
            doc_md=(
                f"**Job Consumer DAG** for `{cfg['display_name']}` -- `{_job_alias}`.\n\n"
                f"Triggered by blob Asset : `{blob_asset.uri}`\n\n"
                f"Databricks Job ID       : `{_job_id}`\n\n"
                f"On success emits job-done Asset: `{_job_done_asset.uri}`\n\n"
                f"Coordinator `{_coordinator_dag_id}` triggers when ALL job-done Assets fire.\n\n"
                f"Job Parameters:\n"
                f"  catalog_name   : `{_job_params['catalog_name']}`\n"
                f"  label_data     : `{_job_params['label_data']}`\n"
                f"  volume_basepath: `{_job_params['volume_basepath']}`\n"
                f"  event_type     : `{_job_params['event_type']}`\n"
            ),
        ) as consumer_dag:

            _metadata_task = make_log_metadata_fn(cfg)()

            _run_databricks_job = DatabricksRunNowOperator(
                task_id="trigger_databricks_job",
                databricks_conn_id=DATABRICKS_CONN_ID,
                job_id=_job_id,
                job_parameters={
                    # ── From blob Asset event metadata (XCom) ─────────────
                    "file_id":        file_id,
                    "job_alias":      _job_alias,
                    "date_mode":      "{{ ti.xcom_pull(task_ids='log_asset_metadata')['date_mode'] }}",
                    "valuation_date": "{{ ti.xcom_pull(task_ids='log_asset_metadata')['valuation_date'] }}",
                    "execution_date": "{{ ti.xcom_pull(task_ids='log_asset_metadata')['execution_date'] }}",
                    "file_name":      "{{ ti.xcom_pull(task_ids='log_asset_metadata')['file_name'] }}",
                    "file_path":      "{{ ti.xcom_pull(task_ids='log_asset_metadata')['file_path'] }}",
                    "file_location":  "{{ ti.xcom_pull(task_ids='log_asset_metadata')['file_location'] }}",
                    "blob_url":       "{{ ti.xcom_pull(task_ids='log_asset_metadata')['blob_url'] }}",
                    "file_timestamp": "{{ ti.xcom_pull(task_ids='log_asset_metadata')['file_timestamp'] }}",
                    "size_bytes":     "{{ ti.xcom_pull(task_ids='log_asset_metadata')['size_bytes'] }}",
                    "etag":           "{{ ti.xcom_pull(task_ids='log_asset_metadata')['etag'] }}",
                    # ── From YAML config (static per job) ─────────────────
                    "catalog_name":    str(_job_params["catalog_name"]),
                    "label_data":      str(_job_params["label_data"]),
                    "volume_basepath": str(_job_params["volume_basepath"]),
                    "event_type":      str(_job_params["event_type"]),
                },
                wait_for_termination=True,
                polling_period_seconds=30,
            )

            # Pass metadata to emit_job_done so the coordinator can read it
            # from the job-done Asset event (no cross-DAG XCom needed)
            _emit_done_task = _emit_job_done()

            _metadata_task >> _run_databricks_job >> _emit_done_task

        globals()[_consumer_dag_id] = consumer_dag
        _total_consumer_dags += 1

# ─────────────────────────────────────────────────────────────────────────────
# @task factory -- Coordinator: archive file (WasbHook or Databricks job)
# ─────────────────────────────────────────────────────────────────────────────

@validate_config
def make_archive_fn(cfg: dict) -> Callable:
    """
    Returns a @task that archives the source blob when verification fails.

    Behaviour controlled by YAML flag use_dbx_archive_job (default: True):

      True  → Run the shared Databricks job (job ID from Airflow Variable
               archive_job_id). Parameters: volume_base_path + filename.

      False → Use DataLakeServiceClient directly (ADLS Gen2 rename):
               Atomically moves the file by replacing 'toAzure' with
               'archive' in the path. rename_file() is a pure metadata
               operation — no data copied, no egress cost, no delete step.
               The archive container must be ADLS Gen2 cold/archive tier.

    verify_result is read from XCom (task_id='verify_file_moved') inside
    the task body — NOT passed as a typed parameter to avoid Airflow 3.x
    task decorator introspection errors (same pattern as other @task factories).
    """
    # Freeze closure variables — do NOT use typed default params on @task.
    # Airflow 3.x introspects annotated params and tries to inject context
    # values for them, causing import/type errors at registration time.
    _frozen_file_id           = cfg["file_id"]
    _frozen_use_dbx           = bool(cfg.get("use_dbx_archive_job", True))
    _frozen_archive_container = cfg.get("archive_container", cfg["container"])

    @task(task_id="archive_file")
    def archive_file(**context) -> None:
        # Read frozen closure values
        _file_id           = _frozen_file_id
        _use_dbx           = _frozen_use_dbx
        _archive_container = _frozen_archive_container

        # Pull verify_result from XCom — produced by verify_file_moved task
        ti            = context["ti"]
        verify_result = ti.xcom_pull(task_ids="verify_file_moved") or {}

        file_location = verify_result.get("file_location", "")
        file_name     = verify_result.get("file_name", "")
        container     = verify_result.get("container", "")
        file_path     = verify_result.get("file_path", "")

        if not file_path:
            raise ValueError(
                f"[{_file_id}] archive_file: verify_result XCom is empty or missing "
                f"'file_path'. Cannot archive. verify_result={verify_result!r}"
            )

        if _use_dbx:
            # ── Databricks archive job ─────────────────────────────────────
            from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
            from airflow.sdk import Variable

            job_id = Variable.get(ARCHIVE_JOB_ID_VAR)
            log.info(
                "[%s] archive_file: triggering Databricks job %s | "
                "volume_base_path=%s | filename=%s",
                _file_id, job_id, file_location, file_name,
            )
            op = DatabricksRunNowOperator(
                task_id="archive_file_dbx",
                databricks_conn_id=DATABRICKS_CONN_ID,
                job_id=job_id,
                job_parameters={
                    "volume_base_path": file_location,
                    "filename":         file_name,
                },
                wait_for_termination=True,
                polling_period_seconds=30,
            )
            op.execute(context={})

        else:
            # ── ADLS Gen2 atomic rename/move (no copy+delete) ──────────────
            # ADLS Gen2 supports a native rename operation via the
            # DataLakeFileClient.rename_file() API. This is:
            #   - Atomic: no window where the file exists in both locations
            #   - Instant: metadata operation, no data physically copied
            #   - Free: no egress/ingress cost unlike copy+delete
            #
            # Derive archive path:
            #   replace the FIRST occurrence of 'toAzure' in file_location
            #   with 'archive', then append filename.
            # Example:
            #   file_location = "data/toAzure/sales/20240315"
            #   archive_path  = "data/archive/sales/20240315/sales_20240315.csv"
            if "toAzure" not in file_location:
                raise ValueError(
                    f"[{_file_id}] archive_file: 'toAzure' not found in "
                    f"file_location={file_location!r}. Cannot derive archive path."
                )
            archive_folder = file_location.replace("toAzure", "archive", 1)
            archive_path   = f"{archive_folder}/{file_name}"

            log.info(
                "[%s] archive_file: moving via ADLS Gen2 rename | "
                "src=(%s/%s) → dst=(%s/%s) [cold/archive tier]",
                _file_id, container, file_path, _archive_container, archive_path,
            )

            # Build DataLakeServiceClient directly so we can fall back to the
            # module-level STORAGE_ACCOUNT constant when conn.host is not set.
            # AzureDataLakeStorageV2Hook.get_conn() derives the account URL from
            # conn.host; if that field is None the URL becomes
            # "None.dfs.core.windows.net", causing the connection to fail.
            # Constructing the client here mirrors the hook's logic while
            # honouring all supported auth types (connection string, service
            # principal, SAS token, account key).
            from airflow.hooks.base import BaseHook
            from azure.storage.filedatalake import DataLakeServiceClient

            _conn    = BaseHook.get_connection(AZURE_CONN_ID)
            _extra   = _conn.extra_dejson or {}
            _conn_str = (
                _extra.get("connection_string")
                or _extra.get("extra__azure_data_lake__connection_string")
            )
            if _conn_str:
                adls_client = DataLakeServiceClient.from_connection_string(_conn_str)
            else:
                # Prefer the connection's host; fall back to STORAGE_ACCOUNT.
                _acct_name   = _conn.host or STORAGE_ACCOUNT
                _account_url = (
                    _extra.get("account_url")
                    or f"https://{_acct_name}.dfs.core.windows.net"
                )
                _tenant = _extra.get("tenant_id") or _extra.get("extra__azure__tenant_id")
                if _tenant:
                    from azure.identity import ClientSecretCredential
                    _credential = ClientSecretCredential(
                        tenant_id=_tenant,
                        client_id=_conn.login,
                        client_secret=_conn.password,
                    )
                else:
                    _sas        = _extra.get("sas_token") or _extra.get("extra__adls__sas_token")
                    _credential = _sas or _conn.password
                adls_client = DataLakeServiceClient(account_url=_account_url, credential=_credential)

            src_file_client = adls_client.get_file_client(
                file_system=container,
                file_path=file_path,
            )

            # rename_file() atomically moves the file — no copy, no delete.
            # new_name format: "{destination_filesystem}/{destination_path}"
            src_file_client.rename_file(
                new_name=f"{_archive_container}/{archive_path}"
            )

            log.info(
                "[%s] archive_file: ADLS Gen2 rename complete — "
                "file atomically moved to archive. No copy cost incurred.",
                _file_id,
            )

    return archive_file


    # ── COORDINATOR DAG -- triggered when ALL job-done Assets have fired ───
    # schedule=_job_done_assets uses Airflow's AND logic:
    # all listed Assets must emit an event before this DAG triggers.
    with DAG(
        dag_id=_coordinator_dag_id,
        schedule=_job_done_assets,   # AND logic: every job must complete first
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["coordinator", file_id],
        doc_md=(
            f"**Coordinator DAG** for `{cfg['display_name']}`.\n\n"
            f"Triggered when ALL job-done Assets have fired (AND logic):\n"
            + "".join(f"  - `{a.uri}`\n" for a in _job_done_assets) +
            "\n**Flow**:\n\n"
            "```\n"
            "verify_file_moved\n"
            "  → route_after_verify\n"
            "      ├─ verified=True  → re_trigger_producer\n"
            "      └─ verified=False → archive_file → re_trigger_producer\n"
            "```\n\n"
            f"`archive_file` behaviour set by `use_dbx_archive_job` in YAML (default: true):\n  - true  → Databricks job (ID from Variable `{ARCHIVE_JOB_ID_VAR}`)\n  - false → ADLS Gen2 rename (toAzure → archive path, atomic move, no copy cost)\n\n"
            "Blob metadata is read from the job-done Asset events -- no cross-DAG XCom needed."
        ),
    ) as coordinator_dag:

        _verify_task = make_verify_fn(cfg)()

        _route_task = route_after_verify()

        # ── Path A: verification passed → re_trigger_producer (directly) ───
        # ── Path B: verification failed → archive_file → re_trigger_producer
        #
        # re_trigger_producer is shared by both paths.
        # trigger_rule="none_failed_min_one_success" ensures it runs after
        # whichever branch succeeded while gracefully skipping the other branch.

        # Path B: archive file when verify fails.
        # Behaviour controlled by use_dbx_archive_job in YAML (default True):
        #   True  → Databricks archive job (job ID from Airflow Variable)
        #   False → WasbHook copy source→archive (replace 'toAzure' with 'archive')
        #           then delete source. Archive container is cold/archive tier.
        _archive_file = make_archive_fn(cfg)()

        # Shared terminal task — executes after verify-pass OR after archive succeeds.
        # "none_failed_min_one_success": skipped upstream branches are ignored,
        # only a genuine failure in archive_file would block this from running.
        _re_trigger_producer = TriggerDagRunOperator(
            task_id="re_trigger_producer",
            trigger_dag_id=_producer_dag_id,
            conf={
                "date_mode":   "{{ ti.xcom_pull(task_ids='verify_file_moved')['date_mode'] }}",
                "custom_date": "{{ (ti.xcom_pull(task_ids='verify_file_moved') or {}).get('custom_date', '') }}",
            },
            wait_for_completion=False,
            reset_dag_run=True,
            allowed_states=["success"],
            trigger_rule="none_failed_min_one_success",
        )

        # Path A: verify passed → route → re_trigger_producer
        # Path B: verify failed → route → archive_file → re_trigger_producer
        _verify_task >> _route_task >> [_re_trigger_producer, _archive_file]
        _archive_file >> _re_trigger_producer

    globals()[_coordinator_dag_id] = coordinator_dag


log.info(
    "blob_dag_factory: registered %d producer + %d job consumer + %d coordinator DAGs (%d total).",
    len(FILE_CONFIGS),
    _total_consumer_dags,
    len(FILE_CONFIGS),
    len(FILE_CONFIGS) * 2 + _total_consumer_dags,
)
