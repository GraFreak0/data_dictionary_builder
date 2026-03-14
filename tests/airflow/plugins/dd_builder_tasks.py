"""
dd_builder_tasks.py
===================
Reusable Airflow task functions for the data_dictionary_builder pipeline.

Place this file in your ``include/`` folder and import its callables into
your DAG using ``PythonOperator(python_callable=run_..., op_kwargs={...})``.

──────────────────────────────────────────────────────────────────────────────
Data flow — source vs. destination
──────────────────────────────────────────────────────────────────────────────

  SOURCE databases
      Operational / production databases spread across one or more servers.
      Metadata is extracted from source and written to dbt YAML model files.
      Source is the *baseline* for comparison — what "should" exist.

  DESTINATION (warehouse)
      One or more data warehouse instances (e.g. BigQuery, Snowflake, ClickHouse
      cloud, Redshift).  Different warehouse instances may hold different domains
      of data.  Destination is compared *against* the source to surface missing
      tables, missing columns, type mismatches, and documentation gaps.

  Direction of truth
      Source  ──extracts──>  dbt YAML model files
      Source  ──compared to──>  Destination warehouse
      Findings reported: what is in source but missing or changed in destination.

──────────────────────────────────────────────────────────────────────────────
Connection resolution order (highest priority first)
──────────────────────────────────────────────────────────────────────────────
1. Explicit ``**overrides`` passed at call time (e.g. from ``op_kwargs``)
2. Airflow Connection (looked up by ``conn_id``)
3. Environment variables prefixed by ``env_prefix`` (e.g. ``SOURCE_DB_HOST``)
4. Built-in defaults (``db_type=postgres``, ``port=5432``)

──────────────────────────────────────────────────────────────────────────────
Airflow Connection setup
──────────────────────────────────────────────────────────────────────────────
Create connections in Admin → Connections or via the CLI:

    airflow connections add source_postgres \\
        --conn-type postgres \\
        --host prod-db.example.com \\
        --port 5432 \\
        --login readonly \\
        --password <secret> \\
        --schema my_database

For ClickHouse, store driver/transport options as JSON in the "Extra" field:

    {"db_type": "clickhouse", "transport": "native", "secure": true}

──────────────────────────────────────────────────────────────────────────────
Environment variable fallback
──────────────────────────────────────────────────────────────────────────────
<env_prefix>_DB_TYPE      e.g.  SOURCE_DB_TYPE=postgres
<env_prefix>_HOST         e.g.  SOURCE_HOST=prod-db.example.com
<env_prefix>_PORT         e.g.  SOURCE_PORT=5432
<env_prefix>_DATABASE     e.g.  SOURCE_DATABASE=my_db
<env_prefix>_USER         e.g.  SOURCE_USER=readonly
<env_prefix>_PASSWORD     e.g.  SOURCE_PASSWORD=secret
<env_prefix>_TRANSPORT    e.g.  SOURCE_TRANSPORT=native   (ClickHouse only)
<env_prefix>_SECURE       e.g.  SOURCE_SECURE=true        (ClickHouse only)
<env_prefix>_PROJECT_ID   e.g.  SOURCE_PROJECT_ID=my-gcp  (Spanner only)
<env_prefix>_INSTANCE_ID  e.g.  SOURCE_INSTANCE_ID=prod   (Spanner only)

──────────────────────────────────────────────────────────────────────────────
Available task functions
──────────────────────────────────────────────────────────────────────────────
  resolve_db_config               Build a db config dict from conn / env / overrides
  run_connection_test             Test one or more database connections
  run_list_schemas                List all schemas in a database
  run_list_tables                 List all tables in a schema
  run_extract_metadata            Extract full SOURCE metadata → push to XCom
  run_extract_destination_metadata  Extract DESTINATION (warehouse) metadata → XCom
  run_extract_single_schema       Extract a single source schema → push to XCom
  run_extract_single_table        Extract a single source table → push to XCom
  run_generate_yaml_files         Write per-schema dbt YAML from source XCom metadata
  run_generate_combined_yaml      Write a single all_models.yml from source XCom metadata
  run_detect_documentation_gaps   Find source tables/columns missing descriptions
  run_compare_single_schema       Compare one schema: source (baseline) vs destination
  run_compare_schemas             Compare multiple schemas: source vs destination
  run_compile_pdf                 Compile comparison report JSON → PDF
  run_send_notification           Send the PDF report via email, Slack, or both
  run_export_metadata             Export source metadata to JSON + validate round-trip
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Library imports
# ---------------------------------------------------------------------------
from data_dictionary_builder import (
    DatabaseMetadata,
    DDHelper,
    ExecutionTimer,
    MetadataExtractor,
    SchemaComparator,
    YAMLGenerator,
)


# ===========================================================================
# Internal helpers
# ===========================================================================

def _get_airflow_conn(conn_id: Optional[str]) -> Optional[Any]:
    """
    Load an Airflow Connection by ``conn_id``.

    Returns ``None`` silently when conn_id is falsy, Airflow is not installed,
    or the connection does not exist in the Airflow meta-database.
    """
    if not conn_id:
        return None
    try:
        from airflow.sdk.bases.hook import BaseHook
        return BaseHook.get_connection(conn_id)
    except Exception as exc:
        log.debug("Could not load Airflow connection '%s': %s", conn_id, exc)
        return None


def _bool(value: Any) -> bool:
    """Coerce a value (string, int, bool) to a Python bool."""
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _pull_db_metadata(ti: Any, task_id: str, xcom_key: str = "db_metadata") -> DatabaseMetadata:
    """
    Pull a ``DatabaseMetadata`` dict from XCom and deserialise it.

    Parameters
    ----------
    ti        : TaskInstance (injected via ``**context``)
    task_id   : upstream task_id that pushed the metadata
    xcom_key  : XCom key used by the upstream task
    """
    raw = ti.xcom_pull(task_ids=task_id, key=xcom_key)
    if not raw:
        raise ValueError(
            f"No metadata found in XCom (task_id='{task_id}', key='{xcom_key}'). "
            "Did the upstream extraction task succeed?"
        )
    return DatabaseMetadata.from_dict(raw)


def _safe_task_id(name: str) -> str:
    """Convert a schema / label name to a safe Airflow task_id segment."""
    return name.replace(".", "__").replace("-", "_").replace(" ", "_")


# ===========================================================================
# resolve_db_config
# ===========================================================================

def resolve_db_config(
    conn_id: Optional[str] = None,
    db_type: Optional[str] = None,
    env_prefix: Optional[str] = None,
    **overrides,
) -> Dict[str, Any]:
    """
    Build a database config dict suitable for ``MetadataExtractor(**config)``.

    Resolution order (highest priority wins):
    1. Keyword ``overrides`` passed to this call
    2. Airflow Connection (``conn_id``)
    3. Environment variables with ``env_prefix`` (e.g. ``SOURCE``)
    4. Hard-coded defaults (db_type=postgres, port=5432)

    Parameters
    ----------
    conn_id    : Airflow connection ID to look up.
    db_type    : Override the database type (``"postgres"``, ``"mysql"``,
                 ``"clickhouse"``, ``"sqlite"``, ``"spanner"``, ``"oracle"``,
                 ``"sqlserver"``).
    env_prefix : Env-var prefix, e.g. ``"SOURCE"`` reads ``SOURCE_HOST``,
                 ``SOURCE_PORT``, ``SOURCE_DATABASE``, ``SOURCE_USER``, etc.
    **overrides: Any additional keyword arguments — highest priority and
                 merged into the final config last.

    Returns
    -------
    dict — ready to unpack into ``MetadataExtractor(**config)``.

    Examples
    --------
    ::

        # From an Airflow connection
        cfg = resolve_db_config(conn_id="source_postgres")

        # From environment variables
        cfg = resolve_db_config(env_prefix="SOURCE")

        # Explicit overrides (useful for local testing)
        cfg = resolve_db_config(env_prefix="SOURCE", host="localhost", port=5432)

        # ClickHouse with explicit transport
        cfg = resolve_db_config(conn_id="source_clickhouse", transport="native")
    """
    prefix = (env_prefix or "").rstrip("_").upper()

    # ── Step 1: env-var base ─────────────────────────────────────────────────
    cfg: Dict[str, Any] = {}

    if prefix:
        _db_type = os.getenv(f"{prefix}_DB_TYPE") or os.getenv(f"{prefix}_TYPE")
        if _db_type:
            cfg["db_type"] = _db_type

        for key, env_var in [
            ("host",     f"{prefix}_HOST"),
            ("database", f"{prefix}_DATABASE"),
            ("database", f"{prefix}_DB"),        # alias
            ("user",     f"{prefix}_USER"),
            ("password", f"{prefix}_PASSWORD"),
        ]:
            val = os.getenv(env_var)
            if val and key not in cfg:
                cfg[key] = val

        _port = _int_or_none(os.getenv(f"{prefix}_PORT"))
        if _port:
            cfg["port"] = _port

        # ClickHouse extras
        _transport = os.getenv(f"{prefix}_TRANSPORT")
        if _transport:
            cfg["transport"] = _transport

        _secure = os.getenv(f"{prefix}_SECURE")
        if _secure is not None:
            cfg["secure"] = _bool(_secure)

        # Spanner extras
        _project = os.getenv(f"{prefix}_PROJECT_ID")
        if _project:
            cfg["project_id"] = _project

        _instance = os.getenv(f"{prefix}_INSTANCE_ID")
        if _instance:
            cfg["instance_id"] = _instance

    # ── Step 2: Airflow connection (higher priority than env vars) ───────────
    conn = _get_airflow_conn(conn_id)
    if conn:
        extra = {}
        try:
            extra = conn.extra_dejson or {}
        except Exception:
            pass

        if conn.host:
            cfg["host"] = conn.host
        if conn.login:
            cfg["user"] = conn.login
        if conn.password:
            cfg["password"] = conn.password
        if conn.schema:
            cfg["database"] = conn.schema

        _conn_port = _int_or_none(conn.port)
        if _conn_port:
            cfg["port"] = _conn_port

        _conn_type_map = {
            "postgres":             "postgres",
            "postgresql":           "postgres",
            "mysql":                "mysql",
            "clickhouse":           "clickhouse",
            "sqlite":               "sqlite",
            "spanner":              "spanner",
            "google_cloud_spanner": "spanner",
            "oracle":               "oracle",
            "sqlserver":            "sqlserver",
            "mssql":                "sqlserver",
        }
        _ct = getattr(conn, "conn_type", None) or ""
        if _ct.lower() in _conn_type_map:
            cfg["db_type"] = _conn_type_map[_ct.lower()]

        # Extras stored as JSON in the connection's "Extra" field
        for k, v in extra.items():
            cfg.setdefault(k, v)

    # ── Step 3: caller overrides (highest priority) ──────────────────────────
    if db_type:
        overrides["db_type"] = db_type

    for k, v in overrides.items():
        if v is not None:
            cfg[k] = v

    # ── Step 4: sensible defaults ────────────────────────────────────────────
    cfg.setdefault("db_type", "postgres")

    _type_port_defaults = {
        "postgres":  5432,
        "mysql":     3306,
        "sqlite":    None,
        "spanner":   None,
    }
    if "port" not in cfg:
        default_port = _type_port_defaults.get(cfg["db_type"])
        if default_port:
            cfg["port"] = default_port

    log.info(
        "Resolved DB config — type=%s  host=%s  db=%s  port=%s",
        cfg.get("db_type"), cfg.get("host"), cfg.get("database"), cfg.get("port"),
    )
    return cfg


# ===========================================================================
# Connection test
# ===========================================================================

def run_connection_test(
    # Option A: pre-built config dicts (used by the multi-pipeline DAG)
    configs: Optional[List[Dict[str, Any]]] = None,
    # Option B: separate source / dest configs (single-pipeline convenience)
    source_conn_id: Optional[str] = None,
    dest_conn_id:   Optional[str] = None,
    source_config:  Optional[Dict[str, Any]] = None,
    dest_config:    Optional[Dict[str, Any]] = None,
    source_env_prefix: str = "SOURCE",
    dest_env_prefix:   str = "DEST",
    **context,
) -> str:
    """
    Verify that one or more databases are reachable.

    Raises ``RuntimeError`` on any failure so Airflow marks the task as failed
    and triggers retries / alerts before any extraction begins.

    Parameters
    ----------
    configs       : List of pre-built config dicts to test (multi-pipeline mode).
                    Each dict must contain at least ``db_type`` and ``host``.
                    When supplied, ``source_*`` / ``dest_*`` params are ignored.
    source_config : Pre-built source config dict (single-pipeline mode).
    dest_config   : Pre-built destination config dict (single-pipeline mode).
    source_conn_id / dest_conn_id : Airflow connection IDs (single-pipeline mode).
    source_env_prefix / dest_env_prefix : Env-var prefixes (single-pipeline mode).
    """
    timer = ExecutionTimer()

    # Build the list of (label, config) pairs to test
    to_test: List[tuple] = []

    if configs:
        for i, cfg in enumerate(configs):
            label = cfg.pop("_label", None) or cfg.get("host") or f"connection_{i + 1}"
            to_test.append((label, cfg))
    else:
        src_cfg  = source_config or resolve_db_config(conn_id=source_conn_id, env_prefix=source_env_prefix)
        dest_cfg = dest_config   or resolve_db_config(conn_id=dest_conn_id,   env_prefix=dest_env_prefix)
        to_test  = [("SOURCE", src_cfg), ("DESTINATION", dest_cfg)]

    results: Dict[str, bool] = {}
    with timer.task("Connection tests"):
        for label, cfg in to_test:
            ok = MetadataExtractor(**cfg).test_connection()
            results[label] = ok
            status = "OK" if ok else "FAILED"
            log.info(
                "  [%s] %s — type=%s  host=%s  db=%s",
                status, label, cfg.get("db_type"), cfg.get("host"), cfg.get("database"),
            )

    failed = [label for label, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(
            f"Connection test failed for: {', '.join(failed)}. "
            "Check your connection config, credentials, and network access."
        )

    timer.summary("Connection Test")
    passed = [label for label, ok in results.items() if ok]
    return f"All {len(passed)} connection(s) OK: {', '.join(passed)}"


# ===========================================================================
# List schemas / tables  (utility tasks)
# ===========================================================================

def run_list_schemas(
    conn_id:    Optional[str] = None,
    db_config:  Optional[Dict[str, Any]] = None,
    env_prefix: str = "SOURCE",
    xcom_key:   str = "schemas",
    **context,
) -> List[str]:
    """
    List all schemas in a source database and push them to XCom.

    Parameters
    ----------
    conn_id    : Airflow connection ID.
    db_config  : Pre-built config dict (bypasses resolve_db_config).
    env_prefix : Env-var prefix (default ``"SOURCE"``).
    xcom_key   : XCom key to push the schema list under (default ``"schemas"``).
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task("List schemas"):
        with MetadataExtractor(**cfg) as ext:
            schemas = ext.get_schemas_list()

    log.info(
        "Source [%s @ %s] — found %d schema(s): %s",
        cfg.get("db_type"), cfg.get("host"), len(schemas), schemas,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=schemas)

    timer.summary("List Schemas")
    return schemas


def run_list_tables(
    schema_name: str,
    conn_id:     Optional[str] = None,
    db_config:   Optional[Dict[str, Any]] = None,
    env_prefix:  str = "SOURCE",
    xcom_key:    str = "tables",
    **context,
) -> List[str]:
    """
    List all tables in a source schema and push them to XCom.

    Parameters
    ----------
    schema_name : Schema to inspect.
    conn_id     : Airflow connection ID.
    db_config   : Pre-built config dict (bypasses resolve_db_config).
    env_prefix  : Env-var prefix (default ``"SOURCE"``).
    xcom_key    : XCom key to push the table list under (default ``"tables"``).
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task(f"List tables in '{schema_name}'"):
        with MetadataExtractor(**cfg) as ext:
            tables = ext.get_tables_list(schema_name)

    log.info(
        "Source schema '%s' [%s @ %s] — found %d table(s): %s",
        schema_name, cfg.get("db_type"), cfg.get("host"), len(tables), tables,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=tables)

    timer.summary("List Tables")
    return tables


# ===========================================================================
# Extract source metadata
# ===========================================================================

def run_extract_metadata(
    conn_id:          Optional[str] = None,
    db_config:        Optional[Dict[str, Any]] = None,
    env_prefix:       str = "SOURCE",
    schema_filter:    Optional[List[str]] = None,
    parallel_workers: int = 8,
    xcom_key:         str = "source_metadata",
    **context,
) -> str:
    """
    Extract full metadata from the SOURCE database and push it to XCom.

    This is the primary extraction step.  The resulting ``DatabaseMetadata``
    is used by every downstream task: YAML generation, documentation gap
    detection, and schema comparison (as the baseline).

    ``schema_filter`` supports all filtering strategies:
    - Exact names:       ``["public", "analytics"]``
    - Glob wildcards:    ``["stg_%", "raw_*"]``
    - Prefix/suffix:     ``["prefix:stg_", "suffix:_prod"]``
    - Contains:          ``["contains:analytics"]``
    - Regex:             ``["regex:^tmp_\\\\d+$"]``
    - Mixed:             ``["public", "prefix:stg_", "regex:^raw_\\\\d{4}$"]``
    - All schemas:       ``None``

    Parameters
    ----------
    conn_id          : Airflow connection ID for the source database.
    db_config        : Pre-built config dict (bypasses resolve_db_config).
    env_prefix       : Env-var prefix (default ``"SOURCE"``).
    schema_filter    : Which schemas to extract (None = all schemas).
    parallel_workers : Max concurrent extraction threads per schema (default 8).
    xcom_key         : XCom key to push the serialised metadata under.
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    log.info(
        "Extracting SOURCE metadata — type=%s  host=%s  db=%s  filter=%s",
        cfg.get("db_type"), cfg.get("host"), cfg.get("database"), schema_filter,
    )

    with timer.task("Extract source metadata"):
        with MetadataExtractor(**cfg) as ext:
            db_meta = ext.extract_all_schemas(
                schema_filter=schema_filter,
                parallel_workers=parallel_workers,
            )

    schema_names  = [s.name for s in db_meta.schemas]
    schema_count  = len(schema_names)
    table_count   = sum(len(s.tables) for s in db_meta.schemas)
    col_count     = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)

    log.info(
        "SOURCE extracted — db=%s  schemas=%d %s  tables=%d  columns=%d",
        db_meta.database_name, schema_count, schema_names, table_count, col_count,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=db_meta.to_dict())

    timer.summary("Extract Source Metadata")
    return (
        f"SOURCE: {schema_count} schema(s) {schema_names}, "
        f"{table_count} table(s), {col_count} column(s) "
        f"from {db_meta.database_name}"
    )


# ===========================================================================
# Extract destination (warehouse) metadata
# ===========================================================================

def run_extract_destination_metadata(
    conn_id:          Optional[str] = None,
    db_config:        Optional[Dict[str, Any]] = None,
    env_prefix:       str = "DEST",
    schema_filter:    Optional[List[str]] = None,
    parallel_workers: int = 8,
    xcom_key:         str = "dest_metadata",
    **context,
) -> str:
    """
    Extract metadata from the DESTINATION (warehouse) database and push to XCom.

    Run this once per pipeline and reuse the result across all per-schema
    comparison tasks — avoids repeated connections to the warehouse.

    The destination snapshot is what source schemas are compared *against*.
    Differences found (missing tables, columns, type mismatches) represent
    drift between the source of truth and the warehouse.

    Parameters
    ----------
    conn_id          : Airflow connection ID for the destination warehouse.
    db_config        : Pre-built config dict (bypasses resolve_db_config).
    env_prefix       : Env-var prefix (default ``"DEST"``).
    schema_filter    : Which schemas to extract from the warehouse for comparison.
                       Should match the schemas being compared.
    parallel_workers : Max concurrent extraction threads (default 8).
    xcom_key         : XCom key to push the serialised metadata under
                       (default ``"dest_metadata"``).
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    log.info(
        "Extracting DESTINATION (warehouse) metadata — type=%s  host=%s  db=%s  filter=%s",
        cfg.get("db_type"), cfg.get("host"), cfg.get("database"), schema_filter,
    )

    with timer.task("Extract destination metadata"):
        with MetadataExtractor(**cfg) as ext:
            db_meta = ext.extract_all_schemas(
                schema_filter=schema_filter,
                parallel_workers=parallel_workers,
            )

    schema_names = [s.name for s in db_meta.schemas]
    table_count  = sum(len(s.tables) for s in db_meta.schemas)

    log.info(
        "DESTINATION extracted — db=%s  schemas=%d %s  tables=%d",
        db_meta.database_name, len(schema_names), schema_names, table_count,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=db_meta.to_dict())

    timer.summary("Extract Destination Metadata")
    return (
        f"DESTINATION: {len(schema_names)} schema(s) {schema_names}, "
        f"{table_count} table(s) from {db_meta.database_name}"
    )


# ===========================================================================
# Extract single schema / single table  (utility tasks)
# ===========================================================================

def run_extract_single_schema(
    schema_name: str,
    conn_id:     Optional[str] = None,
    db_config:   Optional[Dict[str, Any]] = None,
    env_prefix:  str = "SOURCE",
    xcom_key:    str = "schema_metadata",
    **context,
) -> str:
    """
    Extract metadata for a single source schema and push it to XCom.

    Useful for lightweight inspection or debugging a specific schema without
    running a full database extraction.

    Parameters
    ----------
    schema_name : Name of the schema to extract.
    conn_id     : Airflow connection ID.
    db_config   : Pre-built config dict (bypasses resolve_db_config).
    env_prefix  : Env-var prefix (default ``"SOURCE"``).
    xcom_key    : XCom key (default ``"schema_metadata"``).
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task(f"Extract schema '{schema_name}'"):
        with MetadataExtractor(**cfg) as ext:
            schema = ext.extract_schema(schema_name)

    log.info("Source schema '%s' — %d table(s)", schema.name, len(schema.tables))

    # Wrap in DatabaseMetadata so downstream tasks can deserialise uniformly
    db_meta = DatabaseMetadata(
        database_name=cfg.get("database", schema_name),
        database_type=cfg.get("db_type", "unknown"),
    )
    db_meta.add_schema(schema)

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=db_meta.to_dict())

    timer.summary("Extract Single Schema")
    return f"SOURCE schema '{schema_name}': {len(schema.tables)} table(s)"


def run_extract_single_table(
    schema_name: str,
    table_name:  str,
    conn_id:     Optional[str] = None,
    db_config:   Optional[Dict[str, Any]] = None,
    env_prefix:  str = "SOURCE",
    xcom_key:    str = "table_metadata",
    **context,
) -> str:
    """
    Extract metadata for a single source table and push it to XCom.

    Parameters
    ----------
    schema_name : Schema containing the table.
    table_name  : Table to inspect.
    conn_id     : Airflow connection ID.
    db_config   : Pre-built config dict.
    env_prefix  : Env-var prefix (default ``"SOURCE"``).
    xcom_key    : XCom key (default ``"table_metadata"``).
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task(f"Extract table '{schema_name}.{table_name}'"):
        with MetadataExtractor(**cfg) as ext:
            table = ext.extract_table(schema_name, table_name)

    pk_list  = table.primary_keys
    fk_count = sum(1 for c in table.columns if c.is_foreign_key)

    log.info(
        "Source table '%s.%s' — %d cols, %d rows, PKs=%s, FKs=%d",
        schema_name, table_name, len(table.columns), table.row_count, pk_list, fk_count,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value={
            "schema":       schema_name,
            "table":        table_name,
            "column_count": len(table.columns),
            "row_count":    table.row_count,
            "primary_keys": pk_list,
            "fk_count":     fk_count,
            "columns": [
                {
                    "name":           c.name,
                    "data_type":      c.data_type,
                    "is_nullable":    c.is_nullable,
                    "is_primary_key": c.is_primary_key,
                    "is_foreign_key": c.is_foreign_key,
                }
                for c in table.columns
            ],
        })

    timer.summary("Extract Single Table")
    return (
        f"SOURCE table '{schema_name}.{table_name}': "
        f"{len(table.columns)} cols, {table.row_count} rows, PKs={pk_list}"
    )


# ===========================================================================
# Generate dbt YAML files  (source metadata → model files)
# ===========================================================================

def run_generate_yaml_files(
    yaml_output_dir:   str,
    metadata_task_id:  str,
    metadata_xcom_key: str = "source_metadata",
    **context,
) -> List[str]:
    """
    Pull SOURCE ``DatabaseMetadata`` from XCom and write one dbt YAML file per schema.

    YAML files document the source database structure.  On re-runs, existing
    user-written descriptions, dbt tests, and ``meta`` blocks are preserved
    (smart merge — nothing you've written by hand is overwritten).

    Parameters
    ----------
    yaml_output_dir   : Directory where YAML files are written (e.g. ``models/``).
    metadata_task_id  : task_id of the upstream source extraction task.
    metadata_xcom_key : XCom key used by that task (default ``"source_metadata"``).
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    schema_names = [s.name for s in db_meta.schemas]
    log.info("Generating YAML for %d schema(s): %s", len(schema_names), schema_names)

    with timer.task("Generate per-schema YAML"):
        gen   = YAMLGenerator(output_dir=yaml_output_dir)
        files = gen.generate_yaml_files(db_meta)

    for f in files:
        log.info("  YAML → %s  (%d bytes)", f, os.path.getsize(f))

    timer.summary("Generate YAML Files")
    return files


def run_generate_combined_yaml(
    yaml_output_dir:   str,
    metadata_task_id:  str,
    metadata_xcom_key: str = "source_metadata",
    combined_filename: str = "all_models.yml",
    xcom_key:          str = "combined_yaml_path",
    **context,
) -> str:
    """
    Pull SOURCE ``DatabaseMetadata`` from XCom and write a single combined YAML file.

    Produces ``all_models.yml`` containing every schema and table in one file.
    Useful when you prefer a single file over per-schema files.

    Parameters
    ----------
    yaml_output_dir   : Directory where the YAML file is written.
    metadata_task_id  : task_id of the upstream source extraction task.
    metadata_xcom_key : XCom key (default ``"source_metadata"``).
    combined_filename : Output filename (default ``"all_models.yml"``).
    xcom_key          : XCom key to push the output path under.
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    schema_names = [s.name for s in db_meta.schemas]
    log.info("Generating combined YAML for schemas: %s", schema_names)

    with timer.task("Generate combined YAML"):
        filepath = YAMLGenerator(output_dir=yaml_output_dir).generate_single_yaml(
            db_meta, filename=combined_filename
        )

    log.info("Combined YAML → %s  (%d bytes)", filepath, os.path.getsize(filepath))

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=filepath)

    timer.summary("Generate Combined YAML")
    return filepath


# ===========================================================================
# Documentation gap detection  (source coverage check)
# ===========================================================================

def run_detect_documentation_gaps(
    metadata_task_id:  str,
    metadata_xcom_key: str = "source_metadata",
    xcom_key:          str = "doc_gaps",
    **context,
) -> Dict[str, Any]:
    """
    Find source tables and columns that have no description in the metadata.

    Surfaces coverage percentages and lists the undocumented items.  Push
    results to XCom so downstream tasks can branch, alert, or include the gap
    counts in the comparison report.

    Parameters
    ----------
    metadata_task_id  : task_id of the upstream source extraction task.
    metadata_xcom_key : XCom key (default ``"source_metadata"``).
    xcom_key          : XCom key to push gap results under (default ``"doc_gaps"``).

    Returns
    -------
    dict with keys:
        ``tables_without_descriptions``  — list of ``"schema.table"`` strings
        ``columns_without_descriptions`` — list of ``{"schema", "table", "column"}`` dicts
        ``tables_gap_count``             — int
        ``columns_gap_count``            — int
        ``tables_coverage_pct``          — float 0–100
        ``columns_coverage_pct``         — float 0–100
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    schema_names = [s.name for s in db_meta.schemas]
    log.info("Checking documentation coverage for schemas: %s", schema_names)

    with timer.task("Documentation gap detection"):
        gen             = YAMLGenerator(output_dir=".")
        tables_no_desc  = gen.get_tables_without_descriptions(db_meta)
        columns_no_desc = gen.get_columns_without_descriptions(db_meta)

    total_tables  = sum(len(s.tables) for s in db_meta.schemas)
    total_columns = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)

    t_pct = round(100 * (total_tables  - len(tables_no_desc))  / max(total_tables,  1), 1)
    c_pct = round(100 * (total_columns - len(columns_no_desc)) / max(total_columns, 1), 1)

    log.info(
        "SOURCE documentation coverage — tables: %.1f%% (%d/%d missing) | "
        "columns: %.1f%% (%d/%d missing)",
        t_pct, len(tables_no_desc),  total_tables,
        c_pct, len(columns_no_desc), total_columns,
    )

    result = {
        "schemas_checked":              schema_names,
        "tables_without_descriptions":  tables_no_desc,
        "columns_without_descriptions": columns_no_desc,
        "tables_gap_count":             len(tables_no_desc),
        "columns_gap_count":            len(columns_no_desc),
        "tables_coverage_pct":          t_pct,
        "columns_coverage_pct":         c_pct,
    }

    if ti:
        ti.xcom_push(key=xcom_key, value=result)

    timer.summary("Documentation Gap Detection")
    return result


# ===========================================================================
# Schema comparison — single schema  (source baseline vs destination warehouse)
# ===========================================================================

def run_compare_single_schema(
    schema_name:              str,
    yaml_output_dir:          str,
    report_base_dir:          str,
    source_conn_id:           Optional[str] = None,
    dest_conn_id:             Optional[str] = None,
    source_config:            Optional[Dict[str, Any]] = None,
    dest_config:              Optional[Dict[str, Any]] = None,
    source_env_prefix:        str = "SOURCE",
    dest_env_prefix:          str = "DEST",
    include_yaml_gaps:        bool = True,
    # Reuse pre-extracted metadata from XCom to avoid repeated DB queries
    source_metadata_task_id:  Optional[str] = None,
    source_metadata_xcom_key: str = "source_metadata",
    dest_metadata_task_id:    Optional[str] = None,
    dest_metadata_xcom_key:   str = "dest_metadata",
    xcom_key:                 str = "comparison_report",
    **context,
) -> str:
    """
    Compare a single schema: SOURCE (baseline) vs DESTINATION (warehouse).

    This task is designed to run in parallel across schemas — one task per schema
    within a pipeline TaskGroup.  The task_id includes the schema name so each
    schema's comparison status is visible separately in the Airflow UI.

    Source is the baseline (what *should* exist in the warehouse).
    Findings surface tables, columns, and type differences that are present in
    the source but missing or changed in the destination.

    Optimisations
    -------------
    - Pass ``source_metadata_task_id`` to reuse already-extracted source metadata
      from XCom — avoids a second connection to the source database.
    - Pass ``dest_metadata_task_id`` to reuse already-extracted destination metadata
      from XCom — avoids a second connection to the warehouse (critical when
      multiple schemas are being compared in parallel).

    Parameters
    ----------
    schema_name               : Schema to compare (must exist in source).
    yaml_output_dir           : YAML output directory for documentation gap detection.
    report_base_dir           : Base directory for DDHelper (JSON + PDF output).
    source_conn_id            : Airflow connection ID for the source database.
    dest_conn_id              : Airflow connection ID for the destination warehouse.
    source_config             : Pre-built source config dict.
    dest_config               : Pre-built destination config dict.
    source_env_prefix         : Env-var prefix for source (default ``"SOURCE"``).
    dest_env_prefix           : Env-var prefix for destination (default ``"DEST"``).
    include_yaml_gaps         : Include documentation gap analysis in the report.
    source_metadata_task_id   : task_id that pushed source metadata to XCom.
    source_metadata_xcom_key  : XCom key for source metadata.
    dest_metadata_task_id     : task_id that pushed destination metadata to XCom.
    dest_metadata_xcom_key    : XCom key for destination metadata.
    xcom_key                  : XCom key to push the comparison result under.
    """
    timer = ExecutionTimer()
    ti    = context.get("task_instance") or context.get("ti")

    src_cfg  = source_config or resolve_db_config(conn_id=source_conn_id, env_prefix=source_env_prefix)
    dest_cfg = dest_config   or resolve_db_config(conn_id=dest_conn_id,   env_prefix=dest_env_prefix)

    log.info(
        "Comparing schema '%s' — SOURCE [%s @ %s] vs DESTINATION [%s @ %s]",
        schema_name,
        src_cfg.get("db_type"),  src_cfg.get("host"),
        dest_cfg.get("db_type"), dest_cfg.get("host"),
    )

    # ── Reuse pre-extracted source metadata if available ─────────────────────
    source_db_meta: Optional[DatabaseMetadata] = None
    if source_metadata_task_id and ti:
        try:
            source_db_meta = _pull_db_metadata(ti, source_metadata_task_id, source_metadata_xcom_key)
            log.info("Reusing source metadata from XCom (task '%s')", source_metadata_task_id)
        except Exception as exc:
            log.warning("Could not reuse source metadata: %s — will re-extract", exc)

    # ── Reuse pre-extracted destination metadata if available ─────────────────
    dest_db_meta: Optional[DatabaseMetadata] = None
    if dest_metadata_task_id and ti:
        try:
            dest_db_meta = _pull_db_metadata(ti, dest_metadata_task_id, dest_metadata_xcom_key)
            log.info("Reusing destination metadata from XCom (task '%s')", dest_metadata_task_id)
        except Exception as exc:
            log.warning("Could not reuse destination metadata: %s — will re-extract for this schema", exc)

    comparator = SchemaComparator(
        source_config=src_cfg,
        destination_config=dest_cfg,
        yaml_output_dir=yaml_output_dir,
    )

    with timer.task(f"Compare schema '{schema_name}'"):
        report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            include_yaml_gaps=include_yaml_gaps,
            source_db_metadata=source_db_meta,
            dest_db_metadata=dest_db_meta,
        )

    summary = report.get("summary", {})
    log.info(
        "Schema '%s' comparison — missing tables: %d  missing columns: %d  "
        "type mismatches: %d",
        schema_name,
        summary.get("missing_tables_count", 0),
        summary.get("missing_columns_count", 0),
        summary.get("type_mismatches_count", 0),
    )

    # Save to JSON and push to XCom
    helper    = DDHelper(report_base_dir)
    json_path = helper.save_report(report)
    log.info("Report saved → %s", json_path)

    if ti:
        ti.xcom_push(key=xcom_key,          value=report)
        ti.xcom_push(key="report_json_path", value=str(json_path))

    timer.summary(f"Compare Schema '{schema_name}'")
    return (
        f"Schema '{schema_name}': "
        f"missing tables={summary.get('missing_tables_count', 0)}, "
        f"missing columns={summary.get('missing_columns_count', 0)}, "
        f"type mismatches={summary.get('type_mismatches_count', 0)}"
    )


# ===========================================================================
# Schema comparison — multiple schemas  (combined, single task)
# ===========================================================================

def run_compare_schemas(
    schema_names:              List[str],
    yaml_output_dir:           str,
    report_base_dir:           str,
    source_conn_id:            Optional[str] = None,
    dest_conn_id:              Optional[str] = None,
    source_config:             Optional[Dict[str, Any]] = None,
    dest_config:               Optional[Dict[str, Any]] = None,
    source_env_prefix:         str = "SOURCE",
    dest_env_prefix:           str = "DEST",
    include_yaml_gaps:         bool = True,
    parallel_workers:          int = 8,
    source_metadata_task_id:   Optional[str] = None,
    source_metadata_xcom_key:  str = "source_metadata",
    dest_metadata_task_id:     Optional[str] = None,
    dest_metadata_xcom_key:    str = "dest_metadata",
    xcom_key:                  str = "comparison_report",
    **context,
) -> str:
    """
    Compare multiple schemas in a single task: SOURCE (baseline) vs DESTINATION.

    Use this when you want one combined comparison task covering all schemas.
    For per-schema visibility in the Airflow UI, use ``run_compare_single_schema``
    instead — it creates one task per schema with the schema name in the task_id.

    Source schemas are the baseline — what *should* exist in the warehouse.
    Results surface what is in the source but missing or changed in the destination.

    Optimisations
    -------------
    - Reuses already-extracted source metadata from XCom when
      ``source_metadata_task_id`` is supplied.
    - Reuses pre-extracted destination metadata from XCom when
      ``dest_metadata_task_id`` is supplied.  If neither is provided,
      destination metadata is extracted once and reused across all schemas.

    Parameters
    ----------
    schema_names               : Schemas to compare (from source).
    yaml_output_dir            : YAML directory for documentation gap detection.
    report_base_dir            : Base dir for DDHelper (JSON + PDF output).
    source_conn_id             : Airflow connection ID for the source database.
    dest_conn_id               : Airflow connection ID for the destination warehouse.
    source_config / dest_config: Pre-built config dicts.
    include_yaml_gaps          : Include documentation gap analysis.
    parallel_workers           : Threads for any live destination extraction.
    source_metadata_task_id    : Upstream task_id for source metadata XCom.
    source_metadata_xcom_key   : XCom key for source metadata.
    dest_metadata_task_id      : Upstream task_id for destination metadata XCom.
    dest_metadata_xcom_key     : XCom key for destination metadata.
    xcom_key                   : XCom key for the combined report.
    """
    timer = ExecutionTimer()
    ti    = context.get("task_instance") or context.get("ti")

    src_cfg  = source_config or resolve_db_config(conn_id=source_conn_id, env_prefix=source_env_prefix)
    dest_cfg = dest_config   or resolve_db_config(conn_id=dest_conn_id,   env_prefix=dest_env_prefix)

    log.info(
        "Comparing %d schema(s) %s — SOURCE [%s @ %s] vs DESTINATION [%s @ %s]",
        len(schema_names), schema_names,
        src_cfg.get("db_type"),  src_cfg.get("host"),
        dest_cfg.get("db_type"), dest_cfg.get("host"),
    )

    # ── Reuse pre-extracted source metadata ───────────────────────────────────
    source_db_meta: Optional[DatabaseMetadata] = None
    if source_metadata_task_id and ti:
        try:
            source_db_meta = _pull_db_metadata(ti, source_metadata_task_id, source_metadata_xcom_key)
            log.info("Reusing source metadata from XCom (task '%s')", source_metadata_task_id)
        except Exception as exc:
            log.warning("Could not reuse source metadata: %s — will re-extract", exc)

    # ── Reuse pre-extracted destination metadata, or extract it now ───────────
    dest_db_meta: Optional[DatabaseMetadata] = None
    if dest_metadata_task_id and ti:
        try:
            dest_db_meta = _pull_db_metadata(ti, dest_metadata_task_id, dest_metadata_xcom_key)
            log.info("Reusing destination metadata from XCom (task '%s')", dest_metadata_task_id)
        except Exception as exc:
            log.warning("Could not reuse destination metadata: %s — will extract now", exc)

    if dest_db_meta is None:
        with timer.task("Extract destination snapshot"):
            with MetadataExtractor(**dest_cfg) as ext:
                dest_db_meta = ext.extract_all_schemas(
                    schema_filter=schema_names,
                    parallel_workers=parallel_workers,
                )
        log.info(
            "DESTINATION extracted — %d schema(s), %d table(s)",
            len(dest_db_meta.schemas),
            sum(len(s.tables) for s in dest_db_meta.schemas),
        )

    # ── Per-schema comparison ─────────────────────────────────────────────────
    comparator = SchemaComparator(
        source_config=src_cfg,
        destination_config=dest_cfg,
        yaml_output_dir=yaml_output_dir,
    )

    combined: Dict[str, Any] = {
        "summary": {
            "missing_tables_count":               0,
            "missing_columns_count":              0,
            "type_mismatches_count":              0,
            "tables_without_descriptions_count":  0,
            "columns_without_descriptions_count": 0,
        },
        "comparison": {
            "missing_tables":  [],
            "missing_columns": [],
            "type_mismatches": [],
        },
        "yaml_gaps": {
            "tables_without_descriptions":  [],
            "columns_without_descriptions": [],
        },
        "schemas_compared": [],
    }

    with timer.task("Compare schemas"):
        for schema_name in schema_names:
            log.info("  Comparing schema: %s", schema_name)
            report = comparator.compare_and_generate_report(
                source_schema_name=schema_name,
                include_yaml_gaps=include_yaml_gaps,
                source_db_metadata=source_db_meta,
                dest_db_metadata=dest_db_meta,
            )

            for key in combined["summary"]:
                combined["summary"][key] += report["summary"].get(key, 0)

            for key in ("missing_tables", "missing_columns", "type_mismatches"):
                combined["comparison"][key].extend(report["comparison"].get(key, []))

            for key in ("tables_without_descriptions", "columns_without_descriptions"):
                combined["yaml_gaps"][key].extend(report.get("yaml_gaps", {}).get(key, []))

            combined["schemas_compared"].append(schema_name)

    # ── Persist to JSON ───────────────────────────────────────────────────────
    helper    = DDHelper(report_base_dir)
    json_path = helper.save_report(combined)

    s = combined["summary"]
    log.info(
        "Comparison complete — schemas: %s | missing tables: %d  "
        "missing columns: %d  type mismatches: %d",
        schema_names,
        s["missing_tables_count"], s["missing_columns_count"], s["type_mismatches_count"],
    )
    log.info("Report saved → %s", json_path)

    if ti:
        ti.xcom_push(key=xcom_key,          value=combined)
        ti.xcom_push(key="report_json_path", value=str(json_path))

    timer.summary("Compare Schemas")
    return (
        f"Compared {len(schema_names)} schema(s) {schema_names} — "
        f"missing tables={s['missing_tables_count']}, "
        f"missing columns={s['missing_columns_count']}, "
        f"type mismatches={s['type_mismatches_count']}"
    )


# ===========================================================================
# Compile PDF
# ===========================================================================

def run_compile_pdf(
    report_base_dir:    str,
    report_task_id:     str,
    report_xcom_key:    str = "comparison_report",
    json_path_xcom_key: str = "report_json_path",
    pdf_xcom_key:       str = "pdf_path",
    **context,
) -> Optional[str]:
    """
    Compile the saved JSON comparison report into a formatted PDF.

    Reads the JSON file path pushed by the comparison task and produces a
    paginated PDF with cover page, summary table, and detail sections for
    missing tables, missing columns, type mismatches, and documentation gaps.

    Requires ``reportlab`` (``pip install reportlab``).  Returns ``None``
    gracefully when reportlab is not installed.

    Parameters
    ----------
    report_base_dir    : Base directory used by DDHelper.
    report_task_id     : task_id that pushed the comparison report to XCom.
    report_xcom_key    : XCom key for the report dict.
    json_path_xcom_key : XCom key for the JSON file path (avoids re-saving).
    pdf_xcom_key       : XCom key to push the compiled PDF path under.
    """
    timer = ExecutionTimer()
    ti = context.get("task_instance") or context.get("ti")

    # Prefer the JSON path that the comparison task already saved
    json_path: Optional[str] = None
    if ti:
        json_path = ti.xcom_pull(task_ids=report_task_id, key=json_path_xcom_key)

    if not json_path:
        # Fallback: pull the report dict and save a fresh JSON file
        raw = ti.xcom_pull(task_ids=report_task_id, key=report_xcom_key) if ti else None
        if raw:
            helper    = DDHelper(report_base_dir)
            json_path = str(helper.save_report(raw))
        else:
            raise ValueError(
                f"No report or JSON path found in XCom from task '{report_task_id}'. "
                "Ensure the comparison task ran successfully."
            )

    with timer.task("Compile PDF"):
        helper   = DDHelper(report_base_dir)
        pdf_path = helper.compile_pdf(source_json=Path(json_path))

    if pdf_path:
        log.info("PDF compiled → %s  (%d bytes)", pdf_path, os.path.getsize(pdf_path))
        if ti:
            ti.xcom_push(key=pdf_xcom_key, value=str(pdf_path))
    else:
        log.warning("PDF compilation skipped — reportlab may not be installed.")

    timer.summary("Compile PDF")
    return str(pdf_path) if pdf_path else None


# ===========================================================================
# Send email
# ===========================================================================

def run_send_notification(
    report_base_dir:      str,
    report_task_id:       str,
    report_xcom_key:      str = "comparison_report",
    pdf_task_id:          Optional[str] = None,
    pdf_xcom_key:         str = "pdf_path",
    notification_type:    Optional[str] = None,
    email_to:             Optional[str] = None,
    subject:              Optional[str] = None,
    smtp_conn_id:         Optional[str] = None,
    smtp_host:            Optional[str] = None,
    smtp_port:            int = 587,
    smtp_user:            Optional[str] = None,
    smtp_password:        Optional[str] = None,
    use_tls:              bool = True,
    slack_token:          Optional[str] = None,
    slack_target:         Optional[str] = None,
    slack_pipeline_label: Optional[str] = None,
    **context,
) -> Dict[str, bool]:
    """
    Send the schema comparison report via email, Slack, or both.

    Notification type resolution order
    -----------------------------------
    1. Explicit ``notification_type`` parameter.
    2. ``NOTIFICATION_TYPE`` environment variable.
    3. Default: ``"email"``.

    SMTP credentials resolution order
    ----------------------------------
    1. Explicit ``smtp_host / smtp_user / smtp_password`` parameters.
    2. Airflow Connection (``smtp_conn_id``).
    3. Environment variables: ``SMTP_HOST``, ``SMTP_PORT``, ``SMTP_USER``,
       ``SMTP_PASSWORD``, ``EMAIL_TO``.

    Slack credentials resolution order
    ------------------------------------
    1. Explicit ``slack_token / slack_target`` parameters.
    2. Environment variables: ``SLACK_BOT_TOKEN``, ``SLACK_NOTIFY_TARGET``.

    Both channels skip gracefully when credentials are missing — the task
    succeeds without sending rather than failing the pipeline.

    Parameters
    ----------
    report_base_dir      : Base directory for DDHelper.
    report_task_id       : task_id that pushed the comparison report to XCom.
    report_xcom_key      : XCom key for the report dict.
    pdf_task_id          : task_id that pushed the PDF path to XCom (optional).
    pdf_xcom_key         : XCom key for the PDF path.
    notification_type    : ``"email"``, ``"slack"``, or ``"both"`` (default: ``"email"``).
    email_to             : Recipient email address.
    subject              : Notification subject / title.
    smtp_conn_id         : Airflow connection ID for the SMTP server.
    smtp_host / smtp_user / smtp_password : Explicit SMTP credentials.
    use_tls              : Use STARTTLS (default ``True``).
    slack_token          : Slack Bot User OAuth Token (``xoxb-…``).
    slack_target         : Slack channel (``#name`` / ``C…``) or user (``U…``).
    slack_pipeline_label : Pipeline label shown in the Slack message header.
    """
    timer = ExecutionTimer()
    ti = context.get("task_instance") or context.get("ti")

    report = ti.xcom_pull(task_ids=report_task_id, key=report_xcom_key) if ti else None
    if not report:
        raise ValueError(
            f"No report in XCom (task_id='{report_task_id}', key='{report_xcom_key}'). "
            "Ensure the comparison task ran successfully."
        )

    pdf_path: Optional[str] = None
    if ti and pdf_task_id:
        pdf_path = ti.xcom_pull(task_ids=pdf_task_id, key=pdf_xcom_key)

    # ── Resolve notification type ─────────────────────────────────────────────
    nt = (notification_type or os.getenv("NOTIFICATION_TYPE", "email")).lower().strip()

    # ── Resolve SMTP config ───────────────────────────────────────────────────
    smtp_conn = _get_airflow_conn(smtp_conn_id)
    if smtp_conn:
        smtp_host     = smtp_host     or smtp_conn.host
        smtp_port     = smtp_port     or smtp_conn.port or 587
        smtp_user     = smtp_user     or smtp_conn.login
        smtp_password = smtp_password or smtp_conn.password

    smtp_host     = smtp_host     or os.getenv("SMTP_HOST")
    smtp_port     = smtp_port     or int(os.getenv("SMTP_PORT", 587))
    smtp_user     = smtp_user     or os.getenv("SMTP_USER", "")
    smtp_password = smtp_password or os.getenv("SMTP_PASSWORD")
    email_to      = email_to      or os.getenv("EMAIL_TO")

    # ── Resolve Slack config ──────────────────────────────────────────────────
    slack_token  = slack_token  or os.getenv("SLACK_BOT_TOKEN")
    slack_target = slack_target or os.getenv("SLACK_NOTIFY_TARGET")

    with timer.task("Send notification"):
        helper  = DDHelper(report_base_dir)
        results = helper.send_notification(
            notification_type=nt,
            report=report,
            pdf_path=Path(pdf_path) if pdf_path else None,
            subject=subject or "Database Schema Comparison Report",
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            email_to=email_to,
            use_tls=use_tls,
            slack_token=slack_token,
            slack_target=slack_target,
            slack_pipeline_label=slack_pipeline_label,
        )

    log.info(
        "Notification results — email=%s  slack=%s  type=%s",
        results.get("email"), results.get("slack"), nt,
    )

    timer.summary("Send Notification")
    return results


# ---------------------------------------------------------------------------
# Backward-compatible alias
# ---------------------------------------------------------------------------
def run_send_email(
    report_base_dir:  str,
    report_task_id:   str,
    report_xcom_key:  str = "comparison_report",
    pdf_task_id:      Optional[str] = None,
    pdf_xcom_key:     str = "pdf_path",
    email_to:         Optional[str] = None,
    subject:          Optional[str] = None,
    smtp_conn_id:     Optional[str] = None,
    smtp_host:        Optional[str] = None,
    smtp_port:        int = 587,
    smtp_user:        Optional[str] = None,
    smtp_password:    Optional[str] = None,
    use_tls:          bool = True,
    **context,
) -> bool:
    """Backward-compatible wrapper — calls ``run_send_notification`` with ``notification_type='email'``."""
    result = run_send_notification(
        report_base_dir=report_base_dir,
        report_task_id=report_task_id,
        report_xcom_key=report_xcom_key,
        pdf_task_id=pdf_task_id,
        pdf_xcom_key=pdf_xcom_key,
        notification_type="email",
        email_to=email_to,
        subject=subject,
        smtp_conn_id=smtp_conn_id,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        use_tls=use_tls,
        **context,
    )
    return result.get("email", False)


# ===========================================================================
# Export metadata + round-trip validation
# ===========================================================================

def run_export_metadata(
    report_base_dir:   str,
    metadata_task_id:  str,
    metadata_xcom_key: str = "source_metadata",
    export_filename:   Optional[str] = None,
    xcom_key:          str = "metadata_export_path",
    **context,
) -> str:
    """
    Export SOURCE ``DatabaseMetadata`` to a JSON file and validate the
    ``to_dict()`` / ``from_dict()`` serialisation round-trip.

    This confirms the metadata object is safe to pass through Airflow XCom,
    between tasks, and to downstream systems such as data catalog APIs.

    Parameters
    ----------
    report_base_dir   : Base directory for DDHelper (JSON goes in reports/json/).
    metadata_task_id  : task_id of the upstream source extraction task.
    metadata_xcom_key : XCom key (default ``"source_metadata"``).
    export_filename   : Custom output filename. Defaults to
                        ``"<database_name>_metadata.json"``.
    xcom_key          : XCom key to push the export file path under.
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    schema_names = [s.name for s in db_meta.schemas]
    log.info(
        "Exporting SOURCE metadata for db=%s  schemas=%s",
        db_meta.database_name, schema_names,
    )

    with timer.task("Export metadata + round-trip validation"):
        helper    = DDHelper(report_base_dir)
        filename  = export_filename or f"{db_meta.database_name}_metadata.json"
        safe_name = filename.replace("/", "_").replace("\\", "_")
        out_path  = helper.reports_json_dir / safe_name

        out_path.write_text(
            json.dumps(db_meta.to_dict(), indent=2, default=str),
            encoding="utf-8",
        )

        # Round-trip validation — confirm the object survives XCom serialisation
        restored    = DatabaseMetadata.from_dict(db_meta.to_dict())
        orig_tables = {t.name for s in db_meta.schemas  for t in s.tables}
        rest_tables = {t.name for s in restored.schemas for t in s.tables}
        if orig_tables != rest_tables:
            raise AssertionError(
                f"Metadata round-trip mismatch — "
                f"diff: {orig_tables.symmetric_difference(rest_tables)}"
            )

    log.info(
        "Metadata exported → %s  (%d bytes)  |  round-trip OK — %d table(s)",
        out_path, os.path.getsize(out_path), len(orig_tables),
    )

    if ti:
        ti.xcom_push(key=xcom_key, value=str(out_path))

    timer.summary("Export Metadata")
    return str(out_path)
