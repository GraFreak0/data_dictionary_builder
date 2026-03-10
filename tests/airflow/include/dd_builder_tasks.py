"""
dd_builder_tasks.py
===================
Importable task functions for using data_dictionary_builder inside Apache Airflow.

Place this file in your Airflow ``include/`` folder and import its functions
directly inside your DAG using ``op_kwargs`` on PythonOperator tasks.

Connection resolution order (highest priority first)
-----------------------------------------------------
1. Explicit ``**overrides`` passed at call time (e.g. from op_kwargs in the DAG)
2. Airflow Connection (looked up by ``conn_id``)
3. Environment variables (prefixed by ``env_prefix``, e.g. ``SOURCE_DB_HOST``)
4. Built-in defaults

Airflow Connection setup
------------------------
Create connections in the Airflow UI (Admin → Connections) or via the CLI:

    airflow connections add source_postgres \\
        --conn-type postgres \\
        --host prod-db.example.com \\
        --port 5432 \\
        --login readonly \\
        --password <secret> \\
        --schema my_database

For ClickHouse (custom conn-type), store extra fields as JSON in the
"Extra" field of the connection:

    {"db_type": "clickhouse", "clickhouse_transport": "http", "secure": true}

Environment variable fallback
------------------------------
If no Airflow connection is configured, the function reads:

    <env_prefix>_DB_TYPE      e.g.  SOURCE_DB_TYPE=postgres
    <env_prefix>_HOST         e.g.  SOURCE_HOST=prod-db.example.com
    <env_prefix>_PORT         e.g.  SOURCE_PORT=5432
    <env_prefix>_DATABASE     e.g.  SOURCE_DATABASE=my_db
    <env_prefix>_USER         e.g.  SOURCE_USER=readonly
    <env_prefix>_PASSWORD     e.g.  SOURCE_PASSWORD=secret
    <env_prefix>_TRANSPORT    e.g.  SOURCE_TRANSPORT=http      (ClickHouse only)
    <env_prefix>_SECURE       e.g.  SOURCE_SECURE=true         (ClickHouse only)
    <env_prefix>_PROJECT_ID   e.g.  SOURCE_PROJECT_ID=my-gcp   (Spanner only)
    <env_prefix>_INSTANCE_ID  e.g.  SOURCE_INSTANCE_ID=prod    (Spanner only)

Available task functions
-------------------------
    resolve_db_config           Build a db config dict from conn / env / overrides
    run_connection_test         Verify source and destination connections
    run_list_schemas            List all schemas in a database
    run_list_tables             List all tables in a schema
    run_extract_metadata        Extract full metadata → push to XCom
    run_extract_single_schema   Extract a single schema → push to XCom
    run_extract_single_table    Extract a single table → push to XCom
    run_generate_yaml_files     Generate per-schema dbt YAML from XCom metadata
    run_generate_combined_yaml  Generate a single all_models.yml from XCom metadata
    run_detect_documentation_gaps  Find tables/columns missing descriptions
    run_compare_schemas         Compare source vs destination schemas → push report to XCom
    run_compile_pdf             Compile comparison report JSON → PDF
    run_send_email              Email report PDF (reads SMTP from Airflow conn or env)
    run_export_metadata         Export metadata to JSON + validate round-trip
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
    Try to load an Airflow Connection by ``conn_id``.

    Returns ``None`` silently when:
    - ``conn_id`` is falsy
    - Airflow is not installed
    - The connection does not exist in the Airflow meta-database
    """
    if not conn_id:
        return None
    try:
        from airflow.hooks.base import BaseHook
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
    ti        : TaskInstance (from ``**context``)
    task_id   : upstream task_id that pushed the metadata
    xcom_key  : XCom key used by the upstream task (default: ``"db_metadata"``)
    """
    raw = ti.xcom_pull(task_ids=task_id, key=xcom_key)
    if not raw:
        raise ValueError(
            f"No metadata found in XCom (task_id='{task_id}', key='{xcom_key}'). "
            "Did the upstream extraction task succeed?"
        )
    return DatabaseMetadata.from_dict(raw)


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
    1. Keyword ``overrides``
    2. Airflow Connection (``conn_id``)
    3. Environment variables with ``env_prefix`` (e.g. ``SOURCE``)
    4. Hard-coded defaults (db_type=postgres, port=5432)

    Parameters
    ----------
    conn_id    : Airflow connection ID to look up.
    db_type    : Override the database type (``"postgres"``, ``"mysql"``,
                 ``"clickhouse"``, ``"sqlite"``, ``"spanner"``).
    env_prefix : Env-var prefix, e.g. ``"SOURCE"`` reads ``SOURCE_HOST``,
                 ``SOURCE_PORT``, ``SOURCE_DATABASE``, ``SOURCE_USER``,
                 ``SOURCE_PASSWORD``, ``SOURCE_DB_TYPE``, etc.
    **overrides: Any additional keyword arguments take highest priority and
                 are merged into the final config.

    Returns
    -------
    dict  — ready to be unpacked into ``MetadataExtractor(**config)``.

    Examples
    --------
    ::

        # From Airflow connection:
        cfg = resolve_db_config(conn_id="source_postgres")

        # From env vars:
        cfg = resolve_db_config(env_prefix="SOURCE")

        # Explicit overrides (useful for testing / local runs):
        cfg = resolve_db_config(
            env_prefix="SOURCE",
            host="localhost",
            port=5432,
            database="my_db",
        )

        # ClickHouse with transport override:
        cfg = resolve_db_config(
            conn_id="source_clickhouse",
            clickhouse_transport="native",
        )
    """
    prefix = (env_prefix or "").rstrip("_").upper()

    # ── Step 1: env-var base ─────────────────────────────────────────────
    cfg: Dict[str, Any] = {}

    if prefix:
        _db_type = os.getenv(f"{prefix}_DB_TYPE") or os.getenv(f"{prefix}_TYPE")
        if _db_type:
            cfg["db_type"] = _db_type

        for key, env_var in [
            ("host",     f"{prefix}_HOST"),
            ("database", f"{prefix}_DATABASE"),
            ("database", f"{prefix}_DB"),       # alias
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
            cfg["clickhouse_transport"] = _transport

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

    # ── Step 2: Airflow connection (higher priority than env vars) ───────
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

        # conn_type → db_type mapping
        _conn_type_map = {
            "postgres":   "postgres",
            "postgresql": "postgres",
            "mysql":      "mysql",
            "clickhouse": "clickhouse",
            "sqlite":     "sqlite",
            "spanner":    "spanner",
            "google_cloud_spanner": "spanner",
        }
        _ct = getattr(conn, "conn_type", None) or ""
        if _ct.lower() in _conn_type_map:
            cfg["db_type"] = _conn_type_map[_ct.lower()]

        # Merge extras (allows storing db_type, clickhouse_transport, etc. in Extra field)
        for k, v in extra.items():
            cfg.setdefault(k, v)

    # ── Step 3: caller overrides (highest priority) ──────────────────────
    if db_type:
        overrides["db_type"] = db_type

    for k, v in overrides.items():
        if v is not None:
            cfg[k] = v

    # ── Step 4: sensible defaults ────────────────────────────────────────
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
# Task 1 — Connection test
# ===========================================================================

def run_connection_test(
    source_conn_id: Optional[str] = None,
    dest_conn_id:   Optional[str] = None,
    source_config:  Optional[Dict[str, Any]] = None,
    dest_config:    Optional[Dict[str, Any]] = None,
    source_env_prefix: str = "SOURCE",
    dest_env_prefix:   str = "DEST",
    **context,
) -> str:
    """
    Verify that both the source and destination databases are reachable.

    Raises ``RuntimeError`` if either connection fails so Airflow marks
    the task as failed and triggers retries / alerts.

    Parameters
    ----------
    source_conn_id     : Airflow connection ID for the source DB.
    dest_conn_id       : Airflow connection ID for the destination DB.
    source_config      : Fully-built config dict (skips resolve_db_config).
    dest_config        : Fully-built config dict (skips resolve_db_config).
    source_env_prefix  : Env-var prefix for source (default ``"SOURCE"``).
    dest_env_prefix    : Env-var prefix for destination (default ``"DEST"``).
    """
    timer = ExecutionTimer()

    src_cfg  = source_config or resolve_db_config(conn_id=source_conn_id, env_prefix=source_env_prefix)
    dest_cfg = dest_config   or resolve_db_config(conn_id=dest_conn_id,   env_prefix=dest_env_prefix)

    results = {}
    with timer.task("Connection test"):
        for label, cfg in [("SOURCE", src_cfg), ("DEST", dest_cfg)]:
            ok = MetadataExtractor(**cfg).test_connection()
            results[label] = ok
            status = "OK" if ok else "FAILED"
            log.info("%s connection: %s (%s @ %s)", label, status, cfg.get("db_type"), cfg.get("host"))

    failed = [label for label, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(
            f"Connection test failed for: {', '.join(failed)}. "
            "Check your connection config, credentials, and network access."
        )

    timer.summary("Connection Test")
    return f"Both connections OK — source={src_cfg.get('host')}, dest={dest_cfg.get('host')}"


# ===========================================================================
# Task 2 — List schemas
# ===========================================================================

def run_list_schemas(
    conn_id:    Optional[str] = None,
    db_config:  Optional[Dict[str, Any]] = None,
    env_prefix: str = "SOURCE",
    xcom_key:   str = "schemas",
    **context,
) -> List[str]:
    """
    List all schemas in a database and push them to XCom.

    Parameters
    ----------
    conn_id    : Airflow connection ID.
    db_config  : Pre-built config dict (bypasses resolve_db_config).
    env_prefix : Env-var prefix (default ``"SOURCE"``).
    xcom_key   : XCom key to push the schema list under.
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task("List schemas"):
        with MetadataExtractor(**cfg) as ext:
            schemas = ext.get_schemas_list()

    log.info("Found %d schema(s): %s", len(schemas), schemas)

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=schemas)

    timer.summary("List Schemas")
    return schemas


# ===========================================================================
# Task 3 — List tables
# ===========================================================================

def run_list_tables(
    schema_name: str,
    conn_id:     Optional[str] = None,
    db_config:   Optional[Dict[str, Any]] = None,
    env_prefix:  str = "SOURCE",
    xcom_key:    str = "tables",
    **context,
) -> List[str]:
    """
    List all tables in a specific schema and push them to XCom.

    Parameters
    ----------
    schema_name : Schema to inspect.
    conn_id     : Airflow connection ID.
    db_config   : Pre-built config dict (bypasses resolve_db_config).
    env_prefix  : Env-var prefix (default ``"SOURCE"``).
    xcom_key    : XCom key to push the table list under.
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task("List tables"):
        with MetadataExtractor(**cfg) as ext:
            tables = ext.get_tables_list(schema_name)

    log.info("Schema '%s' — found %d table(s): %s", schema_name, len(tables), tables)

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=tables)

    timer.summary("List Tables")
    return tables


# ===========================================================================
# Task 4 — Extract full metadata
# ===========================================================================

def run_extract_metadata(
    conn_id:          Optional[str] = None,
    db_config:        Optional[Dict[str, Any]] = None,
    env_prefix:       str = "SOURCE",
    schema_filter:    Optional[List[str]] = None,
    parallel_workers: int = 8,
    xcom_key:         str = "db_metadata",
    **context,
) -> str:
    """
    Extract full database metadata and push the serialised result to XCom.

    ``schema_filter`` supports all filtering strategies:
    - Exact names:       ``["public", "analytics"]``
    - Glob wildcards:    ``["stg_%", "raw_*"]``
    - Prefix:            ``["prefix:stg_"]``
    - Suffix:            ``["suffix:_prod"]``
    - Contains:          ``["contains:analytics"]``
    - Regex:             ``["regex:^tmp_\\\\d+$"]``
    - Mixed:             ``["public", "prefix:stg_", "regex:^raw_\\\\d{4}$"]``
    - All schemas:       ``None``

    Parameters
    ----------
    conn_id          : Airflow connection ID for the source database.
    db_config        : Pre-built config dict (bypasses resolve_db_config).
    env_prefix       : Env-var prefix (default ``"SOURCE"``).
    schema_filter    : Which schemas to extract (None = all).
    parallel_workers : Max concurrent extraction threads (default 8).
    xcom_key         : XCom key to push the serialised metadata under.
    """
    timer = ExecutionTimer()
    cfg = db_config or resolve_db_config(conn_id=conn_id, env_prefix=env_prefix)

    with timer.task("Extract metadata"):
        with MetadataExtractor(**cfg) as ext:
            db_meta = ext.extract_all_schemas(
                schema_filter=schema_filter,
                parallel_workers=parallel_workers,
            )

    schema_count = len(db_meta.schemas)
    table_count  = sum(len(s.tables) for s in db_meta.schemas)
    col_count    = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)

    log.info(
        "Extracted — db=%s  schemas=%d  tables=%d  columns=%d",
        db_meta.database_name, schema_count, table_count, col_count,
    )

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=db_meta.to_dict())

    timer.summary("Extract Metadata")
    return (
        f"Extracted {schema_count} schema(s), {table_count} table(s), "
        f"{col_count} column(s) from {db_meta.database_name}"
    )


# ===========================================================================
# Task 5 — Extract single schema
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
    Extract metadata for a single schema and push it to XCom.

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

    with timer.task("Extract single schema"):
        with MetadataExtractor(**cfg) as ext:
            schema = ext.extract_schema(schema_name)

    log.info("Schema '%s' — %d table(s)", schema.name, len(schema.tables))

    # Wrap in a minimal DatabaseMetadata so downstream tasks can deserialise it
    db_meta = DatabaseMetadata(
        database_name=cfg.get("database", schema_name),
        database_type=cfg.get("db_type", "unknown"),
    )
    db_meta.add_schema(schema)

    ti = context.get("task_instance") or context.get("ti")
    if ti:
        ti.xcom_push(key=xcom_key, value=db_meta.to_dict())

    timer.summary("Extract Single Schema")
    return f"Schema '{schema_name}': {len(schema.tables)} table(s)"


# ===========================================================================
# Task 6 — Extract single table
# ===========================================================================

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
    Extract metadata for a single table and push it to XCom.

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

    with timer.task("Extract single table"):
        with MetadataExtractor(**cfg) as ext:
            table = ext.extract_table(schema_name, table_name)

    pk_list  = table.primary_keys
    fk_count = sum(1 for c in table.columns if c.is_foreign_key)

    log.info(
        "Table '%s.%s' — %d cols, %d rows, PKs=%s, FKs=%d",
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
        f"Table '{schema_name}.{table_name}': {len(table.columns)} cols, "
        f"{table.row_count} rows, PKs={pk_list}"
    )


# ===========================================================================
# Task 7 — Generate per-schema YAML files
# ===========================================================================

def run_generate_yaml_files(
    yaml_output_dir:     str,
    metadata_task_id:    str,
    metadata_xcom_key:   str = "db_metadata",
    **context,
) -> List[str]:
    """
    Pull ``DatabaseMetadata`` from XCom and write one dbt YAML file per schema.

    Uses smart merge: existing user-written descriptions, dbt tests, and
    ``meta`` blocks are preserved; new tables/columns are appended.

    Parameters
    ----------
    yaml_output_dir   : Directory where YAML files are written.
    metadata_task_id  : task_id that pushed the ``DatabaseMetadata`` to XCom.
    metadata_xcom_key : XCom key used by the upstream task (default ``"db_metadata"``).
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    with timer.task("Generate per-schema YAML"):
        gen   = YAMLGenerator(output_dir=yaml_output_dir)
        files = gen.generate_yaml_files(db_meta)

    for f in files:
        size = os.path.getsize(f)
        log.info("  YAML → %s  (%d bytes)", f, size)

    timer.summary("Generate YAML Files")
    return files


# ===========================================================================
# Task 8 — Generate combined YAML (all_models.yml)
# ===========================================================================

def run_generate_combined_yaml(
    yaml_output_dir:   str,
    metadata_task_id:  str,
    metadata_xcom_key: str = "db_metadata",
    combined_filename: str = "all_models.yml",
    xcom_key:          str = "combined_yaml_path",
    **context,
) -> str:
    """
    Pull ``DatabaseMetadata`` from XCom and write a single combined YAML file
    containing all schemas and tables.

    Parameters
    ----------
    yaml_output_dir   : Directory where the YAML file is written.
    metadata_task_id  : task_id that pushed the metadata to XCom.
    metadata_xcom_key : XCom key (default ``"db_metadata"``).
    combined_filename : Output filename (default ``"all_models.yml"``).
    xcom_key          : XCom key to push the output path under.
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    with timer.task("Generate combined YAML"):
        filepath = YAMLGenerator(output_dir=yaml_output_dir).generate_single_yaml(
            db_meta, filename=combined_filename
        )

    size = os.path.getsize(filepath)
    log.info("Combined YAML → %s  (%d bytes)", filepath, size)

    if ti:
        ti.xcom_push(key=xcom_key, value=filepath)

    timer.summary("Generate Combined YAML")
    return filepath


# ===========================================================================
# Task 9 — Detect documentation gaps
# ===========================================================================

def run_detect_documentation_gaps(
    metadata_task_id:  str,
    metadata_xcom_key: str = "db_metadata",
    xcom_key:          str = "doc_gaps",
    **context,
) -> Dict[str, Any]:
    """
    Find tables and columns that have no description in the extracted metadata.

    Results are pushed to XCom and also returned for use in downstream
    branch operators or alerts.

    Parameters
    ----------
    metadata_task_id  : task_id that pushed the metadata to XCom.
    metadata_xcom_key : XCom key (default ``"db_metadata"``).
    xcom_key          : XCom key to push gap results under.

    Returns
    -------
    dict with keys:
        ``tables_without_descriptions``  — list of ``"schema.table"`` strings
        ``columns_without_descriptions`` — list of ``{"schema", "table", "column"}`` dicts
        ``tables_gap_count``             — int
        ``columns_gap_count``            — int
        ``tables_coverage_pct``          — float  0–100
        ``columns_coverage_pct``         — float  0–100
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    with timer.task("Documentation gap detection"):
        gen              = YAMLGenerator(output_dir=".")  # output_dir not used here
        tables_no_desc   = gen.get_tables_without_descriptions(db_meta)
        columns_no_desc  = gen.get_columns_without_descriptions(db_meta)

    total_tables  = sum(len(s.tables) for s in db_meta.schemas)
    total_columns = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)

    t_pct = round(100 * (total_tables  - len(tables_no_desc))  / max(total_tables,  1), 1)
    c_pct = round(100 * (total_columns - len(columns_no_desc)) / max(total_columns, 1), 1)

    log.info(
        "Documentation coverage — tables: %.1f%%  (%d/%d missing)  |  "
        "columns: %.1f%%  (%d/%d missing)",
        t_pct, len(tables_no_desc),  total_tables,
        c_pct, len(columns_no_desc), total_columns,
    )

    result = {
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
# Task 10 — Compare schemas
# ===========================================================================

def run_compare_schemas(
    schema_names:          List[str],
    yaml_output_dir:       str,
    report_base_dir:       str,
    source_conn_id:        Optional[str] = None,
    dest_conn_id:          Optional[str] = None,
    source_config:         Optional[Dict[str, Any]] = None,
    dest_config:           Optional[Dict[str, Any]] = None,
    source_env_prefix:     str = "SOURCE",
    dest_env_prefix:       str = "DEST",
    include_yaml_gaps:     bool = True,
    parallel_workers:      int = 8,
    source_metadata_task_id:  Optional[str] = None,
    source_metadata_xcom_key: str = "db_metadata",
    xcom_key:              str = "comparison_report",
    **context,
) -> str:
    """
    Compare one or more schemas between source and destination databases.

    Results are accumulated into a combined report and pushed to XCom.
    The report is also saved as a JSON file via ``DDHelper``.

    Optimisations
    -------------
    - If ``source_metadata_task_id`` is supplied the already-extracted source
      ``DatabaseMetadata`` is reused (no second source DB query).
    - Destination metadata is extracted once and reused for every schema in
      ``schema_names`` (no repeated destination DB queries).

    Parameters
    ----------
    schema_names              : List of schema names to compare.
    yaml_output_dir           : YAML directory used for gap detection.
    report_base_dir           : Base dir for DDHelper (JSON + PDF output).
    source_conn_id            : Airflow connection ID for source.
    dest_conn_id              : Airflow connection ID for destination.
    source_config             : Pre-built source config (bypasses resolve).
    dest_config               : Pre-built destination config (bypasses resolve).
    source_env_prefix         : Env-var prefix for source (default ``"SOURCE"``).
    dest_env_prefix           : Env-var prefix for destination (default ``"DEST"``).
    include_yaml_gaps         : Include documentation gap analysis in report.
    parallel_workers          : Threads for destination metadata extraction.
    source_metadata_task_id   : Upstream task_id that pushed source metadata.
    source_metadata_xcom_key  : XCom key for source metadata.
    xcom_key                  : XCom key to push the combined report under.
    """
    timer = ExecutionTimer()

    ti = context.get("task_instance") or context.get("ti")

    src_cfg  = source_config or resolve_db_config(conn_id=source_conn_id, env_prefix=source_env_prefix)
    dest_cfg = dest_config   or resolve_db_config(conn_id=dest_conn_id,   env_prefix=dest_env_prefix)

    # ── Reuse already-extracted source metadata if available ────────────
    source_db_meta: Optional[DatabaseMetadata] = None
    if source_metadata_task_id and ti:
        try:
            source_db_meta = _pull_db_metadata(ti, source_metadata_task_id, source_metadata_xcom_key)
            log.info("Reusing source metadata from XCom (task_id='%s')", source_metadata_task_id)
        except Exception as exc:
            log.warning("Could not reuse source metadata from XCom: %s — will re-extract", exc)

    # ── Extract destination metadata once ────────────────────────────────
    with timer.task("Extract destination snapshot"):
        with MetadataExtractor(**dest_cfg) as ext:
            dest_db_meta = ext.extract_all_schemas(
                schema_filter=schema_names,
                parallel_workers=parallel_workers,
            )

    log.info(
        "Destination snapshot — %d schema(s), %d table(s)",
        len(dest_db_meta.schemas),
        sum(len(s.tables) for s in dest_db_meta.schemas),
    )

    # ── Per-schema comparison ─────────────────────────────────────────────
    comparator = SchemaComparator(
        source_config=src_cfg,
        destination_config=dest_cfg,
        yaml_output_dir=yaml_output_dir,
    )

    combined: Dict[str, Any] = {
        "summary": {
            "missing_tables_count":              0,
            "missing_columns_count":             0,
            "type_mismatches_count":             0,
            "tables_without_descriptions_count": 0,
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
            log.info("Comparing schema: %s", schema_name)
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

    # ── Persist to JSON ───────────────────────────────────────────────────
    helper    = DDHelper(report_base_dir)
    json_path = helper.save_report(combined)

    log.info(
        "Comparison complete — missing tables: %d, columns: %d, mismatches: %d",
        combined["summary"]["missing_tables_count"],
        combined["summary"]["missing_columns_count"],
        combined["summary"]["type_mismatches_count"],
    )
    log.info("Report saved → %s", json_path)

    if ti:
        ti.xcom_push(key=xcom_key,          value=combined)
        ti.xcom_push(key="report_json_path", value=str(json_path))

    timer.summary("Compare Schemas")
    return f"Compared {len(schema_names)} schema(s) — report: {json_path}"


# ===========================================================================
# Task 11 — Compile PDF
# ===========================================================================

def run_compile_pdf(
    report_base_dir:   str,
    report_task_id:    str,
    report_xcom_key:   str = "comparison_report",
    json_path_xcom_key: str = "report_json_path",
    pdf_xcom_key:      str = "pdf_path",
    **context,
) -> Optional[str]:
    """
    Compile the saved JSON comparison report into a formatted PDF.

    Requires ``reportlab`` (``pip install reportlab``).  Returns ``None``
    gracefully when reportlab is not installed.

    Parameters
    ----------
    report_base_dir    : Base directory used by DDHelper.
    report_task_id     : task_id that pushed the report to XCom.
    report_xcom_key    : XCom key for the report dict.
    json_path_xcom_key : XCom key for the JSON file path pushed by compare task.
    pdf_xcom_key       : XCom key to push the compiled PDF path under.
    """
    timer = ExecutionTimer()

    ti = context.get("task_instance") or context.get("ti")

    # Prefer the JSON file path pushed by run_compare_schemas (avoids re-saving)
    json_path: Optional[str] = None
    if ti:
        json_path = ti.xcom_pull(task_ids=report_task_id, key=json_path_xcom_key)

    if not json_path:
        # Fallback: pull the report dict and save a fresh JSON
        raw = ti.xcom_pull(task_ids=report_task_id, key=report_xcom_key) if ti else None
        if raw:
            helper    = DDHelper(report_base_dir)
            json_path = str(helper.save_report(raw))
        else:
            raise ValueError(
                f"No report or JSON path found in XCom from task '{report_task_id}'. "
                "Ensure run_compare_schemas ran successfully."
            )

    with timer.task("Compile PDF"):
        helper   = DDHelper(report_base_dir)
        pdf_path = helper.compile_pdf(source_json=Path(json_path))

    if pdf_path:
        size = os.path.getsize(pdf_path)
        log.info("PDF compiled → %s  (%d bytes)", pdf_path, size)
        if ti:
            ti.xcom_push(key=pdf_xcom_key, value=str(pdf_path))
    else:
        log.warning("PDF compilation skipped — reportlab may not be installed.")

    timer.summary("Compile PDF")
    return str(pdf_path) if pdf_path else None


# ===========================================================================
# Task 12 — Send email
# ===========================================================================

def run_send_email(
    report_base_dir:   str,
    report_task_id:    str,
    report_xcom_key:   str = "comparison_report",
    pdf_task_id:       Optional[str] = None,
    pdf_xcom_key:      str = "pdf_path",
    email_to:          Optional[str] = None,
    subject:           Optional[str] = None,
    smtp_conn_id:      Optional[str] = None,
    smtp_host:         Optional[str] = None,
    smtp_port:         int = 587,
    smtp_user:         Optional[str] = None,
    smtp_password:     Optional[str] = None,
    use_tls:           bool = True,
    **context,
) -> bool:
    """
    Email the schema comparison report with the compiled PDF attached.

    SMTP credentials resolution order
    ----------------------------------
    1. Explicit ``smtp_host / smtp_user / smtp_password`` parameters.
    2. Airflow Connection (``smtp_conn_id``).
    3. Environment variables: ``SMTP_HOST``, ``SMTP_PORT``, ``SMTP_USER``,
       ``SMTP_PASSWORD``, ``EMAIL_TO``.

    Parameters
    ----------
    report_base_dir  : Base directory for DDHelper.
    report_task_id   : task_id that pushed the comparison report to XCom.
    report_xcom_key  : XCom key for the report dict.
    pdf_task_id      : task_id that pushed the PDF path to XCom.
    pdf_xcom_key     : XCom key for the PDF path.
    email_to         : Recipient email address.
    subject          : Email subject line.
    smtp_conn_id     : Airflow connection ID for SMTP (conn_type=``"smtp"``).
    smtp_host/user/password : Explicit SMTP credentials.
    use_tls          : Whether to use STARTTLS (default ``True``).
    """
    timer = ExecutionTimer()

    ti = context.get("task_instance") or context.get("ti")

    # ── Pull report from XCom ─────────────────────────────────────────────
    report = ti.xcom_pull(task_ids=report_task_id, key=report_xcom_key) if ti else None
    if not report:
        raise ValueError(
            f"No report in XCom (task_id='{report_task_id}', key='{report_xcom_key}'). "
            "Ensure run_compare_schemas ran successfully."
        )

    # ── Pull PDF path from XCom (optional) ────────────────────────────────
    pdf_path: Optional[str] = None
    if ti and pdf_task_id:
        pdf_path = ti.xcom_pull(task_ids=pdf_task_id, key=pdf_xcom_key)

    # ── Resolve SMTP config ────────────────────────────────────────────────
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

    with timer.task("Send email"):
        helper = DDHelper(report_base_dir)
        sent   = helper.send_report_email(
            report=report,
            pdf_path=Path(pdf_path) if pdf_path else None,
            subject=subject or "Database Schema Comparison Report",
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            email_to=email_to,
            use_tls=use_tls,
        )

    status = f"sent to {email_to}" if sent else "skipped (SMTP not configured)"
    log.info("Email %s", status)

    timer.summary("Send Email")
    return sent


# ===========================================================================
# Task 13 — Export metadata + round-trip validation
# ===========================================================================

def run_export_metadata(
    report_base_dir:   str,
    metadata_task_id:  str,
    metadata_xcom_key: str = "db_metadata",
    export_filename:   Optional[str] = None,
    xcom_key:          str = "metadata_export_path",
    **context,
) -> str:
    """
    Export ``DatabaseMetadata`` to a JSON file and validate the
    ``to_dict()`` / ``from_dict()`` serialisation round-trip.

    This confirms the metadata is safe to pass through Airflow XCom and
    to downstream systems (e.g. a data catalog API).

    Parameters
    ----------
    report_base_dir   : Base directory for DDHelper (JSON goes in reports/json/).
    metadata_task_id  : task_id that pushed the metadata to XCom.
    metadata_xcom_key : XCom key (default ``"db_metadata"``).
    export_filename   : Custom filename (default: ``"<database_name>_metadata.json"``).
    xcom_key          : XCom key to push the export path under.
    """
    timer = ExecutionTimer()

    ti      = context.get("task_instance") or context.get("ti")
    db_meta = _pull_db_metadata(ti, metadata_task_id, metadata_xcom_key)

    with timer.task("Export metadata + round-trip validation"):
        helper    = DDHelper(report_base_dir)
        filename  = export_filename or f"{db_meta.database_name}_metadata.json"
        safe_name = filename.replace("/", "_").replace("\\", "_")
        out_path  = helper.reports_json_dir / safe_name

        out_path.write_text(
            json.dumps(db_meta.to_dict(), indent=2, default=str),
            encoding="utf-8",
        )

        # Round-trip validation
        restored     = DatabaseMetadata.from_dict(db_meta.to_dict())
        orig_tables  = {t.name for s in db_meta.schemas  for t in s.tables}
        rest_tables  = {t.name for s in restored.schemas for t in s.tables}
        if orig_tables != rest_tables:
            raise AssertionError(
                f"Metadata round-trip mismatch — "
                f"diff: {orig_tables.symmetric_difference(rest_tables)}"
            )

    size = os.path.getsize(out_path)
    log.info(
        "Metadata exported → %s  (%d bytes)  |  round-trip OK — %d table(s)",
        out_path, size, len(orig_tables),
    )

    if ti:
        ti.xcom_push(key=xcom_key, value=str(out_path))

    timer.summary("Export Metadata")
    return str(out_path)
