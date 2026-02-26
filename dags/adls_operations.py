"""
Azure Data Lake Storage Gen2 — Operations DAG  (ADLS Gen2 hook)
================================================================

Implements the same four storage operations as ``adls_wasb_operations.py``
but drives them exclusively through **AzureDataLakeStorageV2Hook**
(``azure-storage-file-datalake`` SDK / ``DataLakeServiceClient``) instead
of ``WasbHook`` (``azure-storage-blob`` SDK / ``BlobServiceClient``).

ADLS Gen2 advantages used in this DAG
--------------------------------------
list_files
    ``get_paths()`` returns ``PathProperties`` objects that carry rich
    metadata per file — size, last-modified timestamp, creation time,
    owner, group, POSIX permissions — not just blob names.

move_files
    ADLS Gen2 supports an **atomic, server-side rename** via
    ``DataLakeFileClient.rename_file()``.  Unlike the WASB download →
    upload → delete cycle, a rename is:

      * O(1) regardless of file size — no data is transferred
      * Atomic — there is no window where the file exists in both locations
        or in neither
      * Works across directories within the same file system

delete_files
    ``delete_file()`` targets individual file paths precisely, with
    ``dry_run`` mode for safe pre-production validation.

check_file_exists
    ``get_paths()`` with a configurable minimum-count threshold acts as a
    data-quality gate before triggering expensive downstream compute jobs.
    Raises ``AirflowException`` when fewer files than required are present.

Connection
----------
Conn Id   : azure_datalake_gen2_mi
Conn Type : Azure Data Lake Storage Gen2  (adls_v2)
Host      : <storage-account-name>

System-assigned MI  → leave Login / Password / Extra empty
User-assigned   MI  → Extra: {"managed_identity_client_id": "<client-id>"}

The Managed Identity must hold at least the **Storage Blob Data Contributor**
role on the file system (container) or storage account.

Note on terminology
-------------------
Azure Data Lake Gen2 calls what Azure Portal shows as a "container" a
**file system**.  The ``container_name`` trigger parameter maps to the
file system name — so trigger configurations are interchangeable between
this DAG and ``adls_wasb_operations_dag``.

Run Parameters (identical surface to adls_wasb_operations_dag)
--------------------------------------------------------------
operation            list_files | move_files | delete_files | check_file_exists
container_name       ADLS Gen2 file system name               (all operations)
source_prefix        Directory path, e.g. ``landing/sales/``  (all operations)
destination_prefix   Target directory, e.g. ``raw/sales/``    (move_files only)
file_pattern         Glob filter on file name, e.g. ``*.parquet``   default: *
min_file_count       Minimum files expected                   (check_file_exists)
dry_run              Log plan without writing / deleting       (move, delete)

Example trigger
---------------
airflow dags trigger adls_gen2_operations_dag \\
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
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.microsoft.azure.hooks.data_lake import (
    AzureDataLakeStorageV2Hook,
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Constants
# ─────────────────────────────────────────────────────────────────────────────

#: Airflow connection ID — conn type must be ``adls_v2`` (Azure Data Lake
#: Storage Gen2).  Configure with Managed Identity: no password required.
ADLS_CONN_ID = "azure_datalake_gen2_mi"

_SHARED_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_OPERATIONS = ("list_files", "move_files", "delete_files", "check_file_exists")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_hook() -> AzureDataLakeStorageV2Hook:
    """Return an authenticated AzureDataLakeStorageV2Hook via Managed Identity."""
    return AzureDataLakeStorageV2Hook(adls_conn_id=ADLS_CONN_ID)


def _get_matching_paths(
    hook: AzureDataLakeStorageV2Hook,
    file_system: str,
    directory: str | None,
    pattern: str,
) -> list[Any]:   # list[azure.storage.filedatalake.PathProperties]
    """
    Return PathProperties for all non-directory entries under *directory*
    whose file-name portion matches *pattern*.

    Uses ``AzureDataLakeStorageV2Hook.get_paths()`` which performs a
    recursive, flat listing of the ADLS Gen2 hierarchical namespace.
    """
    all_paths = hook.get_paths(
        file_system=file_system,
        directory=directory or "",
        recursive=True,
    )
    # Exclude directory entries — keep files only.
    files = [p for p in all_paths if not p.is_directory]

    if pattern and pattern != "*":
        files = [
            p for p in files
            if fnmatch.fnmatch(p.name.rsplit("/", 1)[-1], pattern)
        ]
    return files


def _ensure_directory(
    hook: AzureDataLakeStorageV2Hook,
    file_system: str,
    directory: str,
) -> None:
    """
    Create *directory* (and any intermediate parent directories) in the
    ADLS Gen2 file system.  The operation is idempotent — calling it on
    an existing directory is a no-op.
    """
    try:
        hook.create_directory(
            file_system_name=file_system,
            directory_name=directory,
        )
    except Exception as exc:
        # Swallow "already exists" errors; re-raise everything else.
        err = str(exc).lower()
        if "pathexists" not in err and "already exist" not in err:
            raise


# ─────────────────────────────────────────────────────────────────────────────
# 3. DAG definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="adls_gen2_operations_dag",
    description=(
        "ADLS Gen2 file operations (list / move / delete / check) using "
        "AzureDataLakeStorageV2Hook and Azure Managed Identity."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_SHARED_ARGS,
    tags=["azure", "adls", "adls-gen2", "data-lake", "managed-identity"],
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
            description=(
                "ADLS Gen2 file system name (what Azure Portal calls a container). "
                "Required for all operations."
            ),
            type="string",
        ),
        "source_prefix": Param(
            default="",
            description=(
                "Source directory path, e.g. 'landing/sales/2024/'.  "
                "Required for move_files, delete_files, check_file_exists; "
                "optional for list_files (omit to list the entire file system)."
            ),
            type="string",
        ),
        "destination_prefix": Param(
            default="",
            description=(
                "Destination directory path, e.g. 'raw/sales/2024/'.  "
                "Required for move_files only."
            ),
            type="string",
        ),
        "file_pattern": Param(
            default="*",
            description=(
                "Glob-style filter applied to the file-name part of each path, "
                "e.g. '*.parquet' or 'sales_*.csv'.  Default: * (all files)."
            ),
            type="string",
        ),
        "min_file_count": Param(
            default=1,
            description=(
                "Minimum number of matching files expected.  "
                "check_file_exists fails when fewer files are found.  "
                "Minimum value: 1."
            ),
            type="integer",
            minimum=1,
        ),
        "dry_run": Param(
            default=False,
            description=(
                "When true, log the planned changes without performing any "
                "renames or deletes.  Applies to move_files and delete_files."
            ),
            type="boolean",
        ),
    },
    doc_md=__doc__,
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # Task 1 — validate_params
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="validate_params")
    def validate_params(**context: Any) -> dict[str, Any]:
        """
        Gate-check all run parameters before any storage calls are made.
        Returns a normalised dict pushed to XCom for every downstream task.
        """
        p = context["params"]
        op: str = p["operation"]
        file_system: str = p.get("container_name", "").strip()
        source: str = p.get("source_prefix", "").strip()
        destination: str = p.get("destination_prefix", "").strip()
        pattern: str = (p.get("file_pattern") or "*").strip()
        min_count: int = int(p.get("min_file_count") or 1)
        dry_run: bool = bool(p.get("dry_run", False))

        if not file_system:
            raise AirflowException(
                "'container_name' (file system name) is required and must not be empty."
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
            "file_system": file_system,
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
    # ─────────────────────────────────────────────────────────────────────────

    @task.branch(task_id="branch_operation")
    def branch_operation(conf: dict[str, Any]) -> str:
        """Return the task_id of the operation to execute; Airflow skips the rest."""
        op = conf["operation"]
        if op not in _OPERATIONS:
            raise AirflowException(
                f"Unknown operation {op!r}. Valid: {_OPERATIONS}"
            )
        print(f"[branch_operation] routing → '{op}'")
        return op

    branch = branch_operation(validated)

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3a — list_files
    #
    #   AzureDataLakeStorageV2Hook.get_paths() returns PathProperties objects
    #   that expose rich metadata (size, timestamps, permissions) — a key
    #   advantage over the flat blob names returned by WasbHook.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="list_files")
    def list_files(conf: dict[str, Any]) -> list[str]:
        """
        Recursively list all files under *file_system / source_prefix* that
        match *file_pattern*.

        Uses ``AzureDataLakeStorageV2Hook.get_paths()`` which returns rich
        ``PathProperties`` metadata — size, last-modified time, creation time,
        owner, POSIX permissions — printed in the task log.

        Returns
        -------
        list[str]
            File paths (relative to the file system root) pushed to XCom.
        """
        file_system = conf["file_system"]
        prefix = conf["source_prefix"] or None
        pattern = conf["file_pattern"]

        print(
            f"[list_files] file_system={file_system!r}  "
            f"prefix={prefix!r}  pattern={pattern!r}"
        )

        hook = _get_hook()
        files = _get_matching_paths(hook, file_system, prefix, pattern)

        if not files:
            print("[list_files] No files found matching the given parameters.")
            return []

        # ── Print inventory with rich ADLS Gen2 PathProperties metadata ──────
        print(f"\n[list_files] {len(files)} file(s) found:")
        print(f"  {'#':>4}  {'SIZE (bytes)':>14}  {'LAST MODIFIED':>26}  PATH")
        print(f"  {'-'*4}  {'-'*14}  {'-'*26}  {'-'*50}")
        for i, p in enumerate(files, 1):
            size = getattr(p, "content_length", "?")
            mtime = getattr(p, "last_modified", "?")
            print(f"  {i:4d}  {size:>14}  {str(mtime):>26}  {p.name}")

        return [p.name for p in files]

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3b — move_files
    #
    #   Uses DataLakeFileClient.rename_file() — an atomic, O(1) server-side
    #   operation unique to ADLS Gen2.  The WASB equivalent requires a full
    #   download → upload → delete cycle for every file.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="move_files")
    def move_files(conf: dict[str, Any]) -> dict[str, Any]:
        """
        Move (rename) files from *source_prefix* to *destination_prefix*
        within the same ADLS Gen2 file system.

        **ADLS Gen2 advantage**: each file is moved via
        ``DataLakeFileClient.rename_file()`` — an atomic, O(1) server-side
        rename.  No data is transferred over the network; there is no window
        in which the file exists in both locations or in neither.

        Sub-paths are preserved relative to *source_prefix*, e.g.:

            landing/sales/2024/01/orders.parquet  →  raw/sales/2024/01/orders.parquet

        When *dry_run* is ``True`` the plan is logged without any renames.

        Returns
        -------
        dict
            ``{moved: [...], errors: [...], dry_run: bool}``
        """
        file_system = conf["file_system"]
        src_root = conf["source_prefix"].rstrip("/") + "/"
        dst_root = conf["destination_prefix"].rstrip("/") + "/"
        pattern = conf["file_pattern"]
        dry_run = conf["dry_run"]

        print(
            f"[move_files] file_system={file_system!r}  "
            f"src={src_root!r}  dst={dst_root!r}  "
            f"pattern={pattern!r}  dry_run={dry_run}"
        )

        hook = _get_hook()
        files = _get_matching_paths(hook, file_system, src_root, pattern)

        if not files:
            print("[move_files] No matching files — nothing to move.")
            return {"moved": [], "errors": [], "dry_run": dry_run}

        print(f"[move_files] {len(files)} file(s) to move:")

        moved: list[str] = []
        errors: list[str] = []

        # ── Pre-create unique destination directories (idempotent) ────────────
        # rename_file() requires the destination parent directory to exist.
        if not dry_run:
            dst_dirs = {
                (dst_root + p.name[len(src_root):]).rsplit("/", 1)[0]
                for p in files
            }
            for dst_dir in dst_dirs:
                _ensure_directory(hook, file_system, dst_dir)

        # ── Atomic server-side rename per file ────────────────────────────────
        for path_props in files:
            src_path = path_props.name                    # e.g. landing/sales/2024/01/orders.parquet
            rel = src_path[len(src_root):]                # e.g. 2024/01/orders.parquet
            dst_path = dst_root + rel                     # e.g. raw/sales/2024/01/orders.parquet

            print(f"  RENAME  {src_path!r}  →  {dst_path!r}", end="")

            if dry_run:
                print("  [DRY RUN — skipped]")
                moved.append(src_path)
                continue

            try:
                file_client = hook.get_file_client(
                    file_system_name=file_system,
                    file_path=src_path,
                )
                # Atomic O(1) rename — unique to ADLS Gen2 hierarchical namespace.
                # new_name format: "{file_system}/{destination_path}"
                file_client.rename_file(
                    new_name=f"{file_system}/{dst_path}"
                )
                moved.append(src_path)
                print("  ✓ (atomic rename)")
            except Exception as exc:
                errors.append(src_path)
                print(f"  ✗  {exc}")

        if errors:
            raise AirflowException(
                f"[move_files] {len(errors)} file(s) failed to rename: {errors}"
            )

        suffix = " (dry run)" if dry_run else ""
        print(f"[move_files] complete{suffix} — moved={len(moved)}")
        return {"moved": moved, "errors": [], "dry_run": dry_run}

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3c — delete_files
    #
    #   Deletes individual ADLS Gen2 file paths using get_file_client().
    #   Supports dry_run for safe pre-production validation.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="delete_files")
    def delete_files(conf: dict[str, Any]) -> dict[str, Any]:
        """
        Delete all files under *file_system / source_prefix* whose file name
        matches *file_pattern*.

        Uses ``AzureDataLakeStorageV2Hook.get_file_client()`` for precise,
        per-file deletion via the ADLS Gen2 hierarchical namespace API.

        When *dry_run* is ``True`` the files that *would* be deleted are
        listed without any actual deletions — safe for pre-production checks.

        Returns
        -------
        dict
            ``{deleted: [...], errors: [...], dry_run: bool}``
        """
        file_system = conf["file_system"]
        prefix = conf["source_prefix"] or None
        pattern = conf["file_pattern"]
        dry_run = conf["dry_run"]

        print(
            f"[delete_files] file_system={file_system!r}  prefix={prefix!r}  "
            f"pattern={pattern!r}  dry_run={dry_run}"
        )

        hook = _get_hook()
        files = _get_matching_paths(hook, file_system, prefix, pattern)

        if not files:
            print("[delete_files] No matching files — nothing to delete.")
            return {"deleted": [], "errors": [], "dry_run": dry_run}

        print(f"[delete_files] {len(files)} file(s) to delete:")
        deleted: list[str] = []
        errors: list[str] = []

        for path_props in files:
            file_path = path_props.name
            print(f"  DELETE  {file_path!r}", end="")

            if dry_run:
                print("  [DRY RUN — skipped]")
                deleted.append(file_path)
                continue

            try:
                file_client = hook.get_file_client(
                    file_system_name=file_system,
                    file_path=file_path,
                )
                file_client.delete_file()
                deleted.append(file_path)
                print("  ✓")
            except Exception as exc:
                errors.append(file_path)
                print(f"  ✗  {exc}")

        if errors:
            raise AirflowException(
                f"[delete_files] {len(errors)} file(s) failed to delete: {errors}"
            )

        suffix = " (dry run)" if dry_run else ""
        print(f"[delete_files] complete{suffix} — deleted={len(deleted)}")
        return {"deleted": deleted, "errors": [], "dry_run": dry_run}

    # ─────────────────────────────────────────────────────────────────────────
    # Task 3d — check_file_exists
    #
    #   Data-quality gate: verifies a minimum number of files are present
    #   before expensive downstream compute jobs are triggered.
    # ─────────────────────────────────────────────────────────────────────────

    @task(task_id="check_file_exists")
    def check_file_exists(conf: dict[str, Any]) -> dict[str, Any]:
        """
        Gate-check: verify that at least *min_file_count* files exist under
        *file_system / source_prefix* matching *file_pattern*.

        Raises ``AirflowException`` — failing the task and blocking any
        downstream pipeline — when fewer files than required are found.

        Use this as a **pre-flight guard** before triggering compute jobs so
        that processing never starts against an incomplete dataset.

        The ADLS Gen2 ``get_paths()`` response includes ``PathProperties``
        with creation time and size, which are surfaced in the task log to
        aid debugging when files are unexpectedly absent.

        Returns
        -------
        dict
            ``{status: "passed", found: int, required: int, files: [...]}``

        Raises
        ------
        AirflowException
            When ``found < min_file_count``.
        """
        file_system = conf["file_system"]
        prefix = conf["source_prefix"] or None
        pattern = conf["file_pattern"]
        min_count = conf["min_file_count"]

        print(
            f"[check_file_exists] file_system={file_system!r}  "
            f"prefix={prefix!r}  pattern={pattern!r}  "
            f"min_file_count={min_count}"
        )

        hook = _get_hook()
        files = _get_matching_paths(hook, file_system, prefix, pattern)
        found = len(files)

        print(f"[check_file_exists] found={found}  required>={min_count}")

        if found < min_count:
            raise AirflowException(
                f"File existence check FAILED: "
                f"found {found} file(s) under "
                f"'{file_system}/{prefix or ''}' matching pattern '{pattern}', "
                f"but at least {min_count} required.\n"
                f"Verify that upstream processes have landed their data in "
                f"'{file_system}' before triggering this DAG."
            )

        # ── Print inventory with ADLS Gen2 metadata for transparency ──────────
        print(f"[check_file_exists] PASSED — {found} file(s) present:")
        for p in files:
            size = getattr(p, "content_length", "?")
            ctime = getattr(p, "creation_time", "?")
            print(f"  ✓  {p.name}  ({size} bytes, created {ctime})")

        return {
            "status": "passed",
            "found": found,
            "required": min_count,
            "files": [p.name for p in files],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Task 4 — summarize_result
    # ─────────────────────────────────────────────────────────────────────────

    @task(
        task_id="summarize_result",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def summarize_result(**context: Any) -> None:
        """
        Pull the result of the completed operation from XCom and print a
        concise summary line.  The other three skipped tasks do not block
        this task (``NONE_FAILED_MIN_ONE_SUCCESS`` trigger rule).
        """
        op: str = context["params"]["operation"]
        result = context["ti"].xcom_pull(task_ids=op)
        print(
            f"[summarize_result] operation={op!r}  completed successfully.\n"
            f"  result → {result!r}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Wire tasks
    #
    #   validate_params
    #        │
    #   branch_operation  ─────────────────────────────────────────────────┐
    #        │                                                              │
    #   ┌────┴──────────────────────────────────────────────────────────┐  │
    #   │  list_files │ move_files │ delete_files │ check_file_exists   │  │
    #   └──────────────────────────┬────────────────────────────────────┘  │
    #                               │                                       │
    #                          summarize_result  ◄───────────────────────────
    # ─────────────────────────────────────────────────────────────────────────

    op_list = list_files(validated)
    op_move = move_files(validated)
    op_delete = delete_files(validated)
    op_check = check_file_exists(validated)

    branch >> [op_list, op_move, op_delete, op_check]

    summary = summarize_result()
    [op_list, op_move, op_delete, op_check] >> summary
