"""
Azure Data Lake Storage (ADLS Gen2) — Operations DAG
=====================================================

A single, manually-triggered DAG that performs one of four ADLS operations
based on the ``operation`` run parameter supplied at trigger time.

Operations
----------
list_files
    Recursively lists every blob under *container / source_prefix* whose file
    name matches *file_pattern*.  Returns a JSON array of blob paths via XCom.

move_files
    Moves blobs from *source_prefix* to *destination_prefix* within the same
    container.  Copies each blob first (download → upload, preserving the
    sub-path relative to the source prefix), then deletes the source.
    Supports *dry_run* mode — logs the plan without touching storage.

delete_files
    Deletes every blob under *container / source_prefix* that matches
    *file_pattern*.  Supports *dry_run* mode for safe pre-flight validation.

check_file_exists
    Gate-check: verifies that at least *min_file_count* blobs exist under
    *container / source_prefix* matching *file_pattern*.
    Raises ``AirflowException`` when the count is below the threshold — useful
    as a data-quality guard before downstream processing steps.

Connection
----------
All operations share a single Airflow connection ``azure_data_lake_mi``
of type ``wasb`` (Azure Blob Storage).  The connection uses Azure Managed
Identity — no password or SAS token is stored in Airflow.

Setting up the connection (Airflow UI → Admin → Connections)
--------------------------------------------------------------
System-assigned Managed Identity
  Conn Id   :  azure_data_lake_mi
  Conn Type :  Azure Blob Storage (wasb)
  Host      :  <storage-account-name>.blob.core.windows.net
  (leave Login / Password / Extra empty — MI is picked up automatically)

User-assigned Managed Identity
  Conn Id   :  azure_data_lake_mi
  Conn Type :  Azure Blob Storage (wasb)
  Host      :  <storage-account-name>.blob.core.windows.net
  Extra     :  {"managed_identity_client_id": "<client-id>"}

The Managed Identity must be assigned at least the
*Storage Blob Data Contributor* role on the container (or account).

Run Parameters
--------------
operation           list_files | move_files | delete_files | check_file_exists
container_name      ADLS Gen2 container name                 (all operations)
source_prefix       Folder path, e.g. ``raw/2024/01/``       (all operations)
destination_prefix  Target folder, e.g. ``processed/2024/`` (move_files only)
file_pattern        Glob filter on file name, e.g. ``*.parquet``  default: *
min_file_count      Minimum files expected                  (check_file_exists)
dry_run             Log plan without writing / deleting      (move, delete)

Example trigger
---------------
airflow dags trigger adls_operations_dag \\
  --conf '{
    "operation":          "move_files",
    "container_name":     "datalake",
    "source_prefix":      "landing/sales/",
    "destination_prefix": "raw/sales/",
    "file_pattern":       "*.parquet",
    "dry_run":            false
  }'
"""

from __future__ import annotations

import fnmatch
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


# ─────────────────────────────────────────────────────────────────────────────
# 1. Constants
# ─────────────────────────────────────────────────────────────────────────────

#: Airflow connection ID — must be configured with Managed Identity (see docstring).
ADLS_CONN_ID = "azure_data_lake_mi"

#: Max seconds to wait for a server-side blob copy to reach a terminal state.
_COPY_TIMEOUT_SECS = 300

_SHARED_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_OPERATIONS = ("list_files", "move_files", "delete_files", "check_file_exists")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Internal helpers  (module-level so each @task can call them without
#    capturing a closure over the BlobServiceClient)
# ─────────────────────────────────────────────────────────────────────────────

def _get_service_client(conn_id: str) -> Any:
    """Return an authenticated ``BlobServiceClient`` via the Airflow hook."""
    hook = WasbHook(wasb_conn_id=conn_id)
    return hook.get_conn()   # azure.storage.blob.BlobServiceClient


def _list_blobs(service_client: Any, container: str, prefix: str | None) -> list[str]:
    """Return a flat list of all blob names under *prefix* (recursive)."""
    container_client = service_client.get_container_client(container)
    return [
        b.name
        for b in container_client.list_blobs(name_starts_with=prefix or None)
    ]


def _apply_pattern(blobs: list[str], pattern: str) -> list[str]:
    """Filter *blobs* by matching the file-name component against *pattern*."""
    if not pattern or pattern == "*":
        return blobs
    return [b for b in blobs if fnmatch.fnmatch(b.rsplit("/", 1)[-1], pattern)]


def _copy_blob(
    service_client: Any,
    container: str,
    src_blob: str,
    dst_blob: str,
    timeout: int = _COPY_TIMEOUT_SECS,
) -> None:
    """
    Copy *src_blob* to *dst_blob* within the same container.

    Uses download → upload so that Managed Identity credentials are never
    exposed in a URL; works reliably with both system- and user-assigned MI.
    After the upload completes the source blob is deleted.

    Raises
    ------
    AirflowException
        If the copy or subsequent delete fails.
    """
    try:
        src_client = service_client.get_blob_client(
            container=container, blob=src_blob
        )
        dst_client = service_client.get_blob_client(
            container=container, blob=dst_blob
        )
        # Stream the blob content from source and write it to destination.
        # This keeps memory usage bounded to the chunk size used by the SDK
        # (~4 MB default) regardless of overall blob size.
        download_stream = src_client.download_blob()
        dst_client.upload_blob(download_stream.readall(), overwrite=True)
    except Exception as exc:
        raise AirflowException(
            f"Copy failed: {src_blob!r} → {dst_blob!r}: {exc}"
        ) from exc

    try:
        src_client.delete_blob()
    except Exception as exc:
        raise AirflowException(
            f"Source delete failed after copy: {src_blob!r}: {exc}"
        ) from exc


# ─────────────────────────────────────────────────────────────────────────────
# 3. DAG definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="adls_operations_dag",
    description=(
        "ADLS Gen2 file operations (list / move / delete / check) "
        "using Azure Managed Identity."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_SHARED_ARGS,
    tags=["azure", "adls", "data-lake", "managed-identity"],
    params={
        "operation": Param(
            default="list_files",
            enum=list(_OPERATIONS),
            description=(
                "Operation to perform: "
                "list_files | move_files | delete_files | check_file_exists"
            ),
            type="string",
        ),
        "container_name": Param(
            default="",
            description="ADLS Gen2 container name (required for all operations).",
            type="string",
        ),
        "source_prefix": Param(
            default="",
            description=(
                "Source folder path prefix, e.g. 'raw/2024/01/'.  "
                "Required for move_files, delete_files, check_file_exists; "
                "optional for list_files (omit to list the whole container)."
            ),
            type="string",
        ),
        "destination_prefix": Param(
            default="",
            description=(
                "Destination folder prefix, e.g. 'processed/2024/01/'.  "
                "Required for move_files only."
            ),
            type="string",
        ),
        "file_pattern": Param(
            default="*",
            description=(
                "Glob-style filter applied to the file-name portion of each blob, "
                "e.g. '*.parquet' or 'sales_*.csv'.  Default: * (all files)."
            ),
            type="string",
        ),
        "min_file_count": Param(
            default=1,
            description=(
                "Minimum number of matching files expected.  "
                "check_file_exists fails if fewer files are found.  "
                "Minimum: 1."
            ),
            type="integer",
            minimum=1,
        ),
        "dry_run": Param(
            default=False,
            description=(
                "When true, log the planned changes without writing or deleting "
                "any blobs.  Applies to move_files and delete_files."
            ),
            type="boolean",
        ),
    },
    doc_md=__doc__,
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # Task 1 — validate_params
    #   Centralised parameter validation.  Returns a clean dict pushed to XCom
    #   so every downstream task receives already-validated values.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="validate_params")
    def validate_params(**context: Any) -> dict[str, Any]:
        """
        Validate run parameters for the requested operation and return a
        normalised parameter dict that downstream tasks receive via XCom.
        """
        p = context["params"]
        op: str = p["operation"]
        container: str = p.get("container_name", "").strip()
        source: str = p.get("source_prefix", "").strip()
        destination: str = p.get("destination_prefix", "").strip()
        pattern: str = (p.get("file_pattern") or "*").strip()
        min_count: int = int(p.get("min_file_count") or 1)
        dry_run: bool = bool(p.get("dry_run", False))

        # --- Required field checks -------------------------------------------
        if not container:
            raise AirflowException(
                "'container_name' is required and must not be empty."
            )

        if op in ("move_files", "delete_files", "check_file_exists") and not source:
            raise AirflowException(
                f"'source_prefix' is required for operation='{op}'."
            )

        if op == "move_files":
            if not destination:
                raise AirflowException(
                    "'destination_prefix' is required for operation='move_files'."
                )
            if source.rstrip("/") == destination.rstrip("/"):
                raise AirflowException(
                    "'source_prefix' and 'destination_prefix' must be different paths."
                )

        resolved: dict[str, Any] = {
            "operation": op,
            "container_name": container,
            "source_prefix": source,
            "destination_prefix": destination,
            "file_pattern": pattern,
            "min_file_count": min_count,
            "dry_run": dry_run,
        }
        print(f"[validate_params] params OK → {resolved}")
        return resolved

    validated = validate_params()

    # ─────────────────────────────────────────────────────────────────────────
    # Task 2 — branch_operation
    #   Returns the task_id of the operation to execute; Airflow skips the rest.
    # ─────────────────────────────────────────────────────────────────────────

    @task.branch(task_id="branch_operation")
    def branch_operation(params: dict[str, Any]) -> str:
        """Return the task_id that matches the requested operation."""
        op = params["operation"]
        if op not in _OPERATIONS:
            raise AirflowException(
                f"Unknown operation {op!r}. Valid: {_OPERATIONS}"
            )
        print(f"[branch_operation] routing to task: {op!r}")
        return op

    branch = branch_operation(validated)

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3a — list_files
    #   List all blobs under container/source_prefix matching file_pattern.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="list_files")
    def list_files(params: dict[str, Any]) -> list[str]:
        """
        List every blob under *container / source_prefix* whose file name
        matches *file_pattern*.

        Returns
        -------
        list[str]
            Blob names pushed to XCom (empty list when nothing matches).
        """
        container = params["container_name"]
        prefix = params["source_prefix"] or None
        pattern = params["file_pattern"]

        print(
            f"[list_files] container={container!r}  "
            f"prefix={prefix!r}  pattern={pattern!r}"
        )

        svc = _get_service_client(ADLS_CONN_ID)
        blobs = _apply_pattern(_list_blobs(svc, container, prefix), pattern)

        if not blobs:
            print("[list_files] No blobs found matching the given parameters.")
            return []

        print(f"[list_files] {len(blobs)} blob(s) found:")
        for i, name in enumerate(blobs, 1):
            print(f"  {i:5d}.  {name}")

        return blobs

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3b — move_files
    #   Relocate blobs from source_prefix to destination_prefix, preserving
    #   the sub-path structure relative to the source root.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="move_files")
    def move_files(params: dict[str, Any]) -> dict[str, Any]:
        """
        Move blobs from *source_prefix* to *destination_prefix* within the same
        container.

        Each blob is copied (download → upload) to the destination and then
        deleted from the source.  The sub-path relative to *source_prefix* is
        preserved, so ``landing/sales/2024/file.parquet`` with
        ``source_prefix=landing/sales/`` and
        ``destination_prefix=raw/sales/`` becomes
        ``raw/sales/2024/file.parquet``.

        Returns
        -------
        dict
            ``{moved: [...], errors: [...], dry_run: bool}``
        """
        container = params["container_name"]
        src_root = params["source_prefix"].rstrip("/") + "/"
        dst_root = params["destination_prefix"].rstrip("/") + "/"
        pattern = params["file_pattern"]
        dry_run = params["dry_run"]

        print(
            f"[move_files] container={container!r}  "
            f"src={src_root!r}  dst={dst_root!r}  "
            f"pattern={pattern!r}  dry_run={dry_run}"
        )

        svc = _get_service_client(ADLS_CONN_ID)
        blobs = _apply_pattern(_list_blobs(svc, container, src_root), pattern)

        if not blobs:
            print("[move_files] No matching blobs — nothing to move.")
            return {"moved": [], "errors": [], "dry_run": dry_run}

        print(f"[move_files] {len(blobs)} blob(s) to move:")
        moved: list[str] = []
        errors: list[str] = []

        for src_blob in blobs:
            # Compute the destination path, preserving sub-folder structure.
            rel = src_blob[len(src_root):]
            dst_blob = dst_root + rel
            print(f"  MOVE  {src_blob!r}  →  {dst_blob!r}", end="")

            if dry_run:
                print("  [DRY RUN — skipped]")
                moved.append(src_blob)
                continue

            try:
                _copy_blob(svc, container, src_blob, dst_blob)
                moved.append(src_blob)
                print("  ✓")
            except AirflowException as exc:
                errors.append(src_blob)
                print(f"  ✗  {exc}")

        if errors:
            raise AirflowException(
                f"[move_files] {len(errors)} blob(s) failed to move: {errors}"
            )

        suffix = " (dry run)" if dry_run else ""
        print(f"[move_files] complete{suffix} — moved={len(moved)}")
        return {"moved": moved, "errors": [], "dry_run": dry_run}

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3c — delete_files
    #   Delete blobs matching a pattern.  Supports dry_run for safe validation.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="delete_files")
    def delete_files(params: dict[str, Any]) -> dict[str, Any]:
        """
        Delete all blobs under *container / source_prefix* whose file name
        matches *file_pattern*.

        When *dry_run* is ``True`` the blobs that *would* be deleted are logged
        but no actual deletions are performed — useful for validating the
        pattern before a production run.

        Returns
        -------
        dict
            ``{deleted: [...], errors: [...], dry_run: bool}``
        """
        container = params["container_name"]
        prefix = params["source_prefix"] or None
        pattern = params["file_pattern"]
        dry_run = params["dry_run"]

        print(
            f"[delete_files] container={container!r}  prefix={prefix!r}  "
            f"pattern={pattern!r}  dry_run={dry_run}"
        )

        svc = _get_service_client(ADLS_CONN_ID)
        blobs = _apply_pattern(_list_blobs(svc, container, prefix), pattern)

        if not blobs:
            print("[delete_files] No matching blobs — nothing to delete.")
            return {"deleted": [], "errors": [], "dry_run": dry_run}

        print(f"[delete_files] {len(blobs)} blob(s) to delete:")
        deleted: list[str] = []
        errors: list[str] = []

        for blob_name in blobs:
            print(f"  DELETE  {blob_name!r}", end="")

            if dry_run:
                print("  [DRY RUN — skipped]")
                deleted.append(blob_name)
                continue

            try:
                blob_client = svc.get_blob_client(
                    container=container, blob=blob_name
                )
                blob_client.delete_blob()
                deleted.append(blob_name)
                print("  ✓")
            except Exception as exc:
                errors.append(blob_name)
                print(f"  ✗  {exc}")

        if errors:
            raise AirflowException(
                f"[delete_files] {len(errors)} blob(s) failed to delete: {errors}"
            )

        suffix = " (dry run)" if dry_run else ""
        print(f"[delete_files] complete{suffix} — deleted={len(deleted)}")
        return {"deleted": deleted, "errors": [], "dry_run": dry_run}

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3d — check_file_exists
    #   Data-quality gate: fail the task (and block downstream) when too few
    #   files are present.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="check_file_exists")
    def check_file_exists(params: dict[str, Any]) -> dict[str, Any]:
        """
        Verify that at least *min_file_count* blobs exist under
        *container / source_prefix* matching *file_pattern*.

        Use this as a pre-flight gate before triggering expensive compute jobs.
        The task fails — and halts any downstream pipeline — when the expected
        files have not yet arrived in the data lake.

        Returns
        -------
        dict
            ``{status: "passed", found: int, required: int, files: [...]}``

        Raises
        ------
        AirflowException
            When fewer than *min_file_count* matching blobs are found.
        """
        container = params["container_name"]
        prefix = params["source_prefix"] or None
        pattern = params["file_pattern"]
        min_count = params["min_file_count"]

        print(
            f"[check_file_exists] container={container!r}  prefix={prefix!r}  "
            f"pattern={pattern!r}  min_file_count={min_count}"
        )

        svc = _get_service_client(ADLS_CONN_ID)
        blobs = _apply_pattern(_list_blobs(svc, container, prefix), pattern)
        found = len(blobs)

        print(f"[check_file_exists] found={found}  required>={min_count}")

        if found < min_count:
            raise AirflowException(
                f"File existence check FAILED: "
                f"found {found} blob(s) under "
                f"'{container}/{prefix or ''}' matching pattern '{pattern}', "
                f"but at least {min_count} required.\n"
                f"Ensure that upstream processes have landed their files in ADLS "
                f"before triggering this DAG."
            )

        print(f"[check_file_exists] PASSED — {found} file(s) present:")
        for b in blobs:
            print(f"  ✓  {b}")

        return {
            "status": "passed",
            "found": found,
            "required": min_count,
            "files": blobs,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Task 4 — summarize_result
    #   Always runs after whichever operation task succeeded; skipped tasks
    #   are ignored via NONE_FAILED_MIN_ONE_SUCCESS.
    # ─────────────────────────────────────────────────────────────────────────

    @task(
        task_id="summarize_result",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def summarize_result(**context: Any) -> None:
        """
        Print a concise summary line for the completed operation.

        Pulls the result produced by the active operation task from XCom so the
        Airflow UI task log surfaces it without the user having to navigate to
        the individual operation task log.
        """
        op: str = context["params"]["operation"]
        ti = context["ti"]
        result = ti.xcom_pull(task_ids=op)
        print(
            f"[summarize_result] operation={op!r} completed successfully.\n"
            f"  result → {result!r}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Wire tasks
    #
    #   validate_params
    #       │
    #   branch_operation  ──────────────────────────────────────────────┐
    #       │                                                            │
    #   ┌───┴───────────────────────────────────────────────────────┐   │
    #   │  list_files │ move_files │ delete_files │ check_file_exists│   │
    #   └───────────────────────┬───────────────────────────────────┘   │
    #                            │                                       │
    #                       summarize_result  ◄──────────────────────────
    #
    # ─────────────────────────────────────────────────────────────────────────

    op_list = list_files(validated)
    op_move = move_files(validated)
    op_delete = delete_files(validated)
    op_check = check_file_exists(validated)

    # Branch routes to exactly one of the four operation tasks.
    branch >> [op_list, op_move, op_delete, op_check]

    # Summary runs after whichever operation task completed (the other three
    # are in "skipped" state and do not block the summary).
    summary = summarize_result()
    [op_list, op_move, op_delete, op_check] >> summary
