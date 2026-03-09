"""
db_metadata_dag.py
==================
Airflow DAG — full data_dictionary_builder pipeline.

Task pipeline
-------------
    1.  connection_test          Verify source + destination DB connections
    2.  list_schemas             List all schemas on source
    3.  list_tables              List tables in the target schema
    4a. extract_metadata         Full parallel metadata extraction → XCom
    4b. extract_single_schema    Extract one schema → XCom (runs in parallel with 4a)
    4c. extract_single_table     Extract one table  → XCom (runs in parallel with 4a)
    5.  generate_yaml_files      Write per-schema dbt YAML files
    6.  generate_combined_yaml   Write a single all_models.yml
    7.  detect_doc_gaps          Find tables / columns without descriptions
    8.  compare_schemas          Source vs destination diff → combined report
    9.  compile_pdf              Compile report JSON → PDF
    10. send_email               Email PDF report
    11. export_metadata          Export metadata JSON + round-trip validation

Configuration
-------------
All parameters are read from Airflow Variables (set them in Admin → Variables)
or from environment variables / a ``.env`` file in the worker's working directory.

Connections
-----------
Create connections in Admin → Connections (or via ``airflow connections add``):

    source_db_conn    — source database
    dest_db_conn      — destination / staging database
    smtp_conn         — SMTP email server  (conn_type=smtp, optional)

See ``include/dd_builder_tasks.py`` for the full connection resolution logic
and the list of supported environment variable names.

Variable reference
------------------
Variable name               Default value               Description
─────────────────────────── ─────────────────────────── ──────────────────────────────
dd_source_conn_id           source_db_conn              Airflow conn ID for source DB
dd_dest_conn_id             dest_db_conn                Airflow conn ID for dest DB
dd_smtp_conn_id             smtp_conn                   Airflow conn ID for SMTP
dd_schema_filter            <none — all schemas>        Comma-separated schema filter
dd_target_schema            public                      Schema used for single-schema tasks
dd_target_table             <first table listed>        Table used for single-table task
dd_parallel_workers         8                           Extraction thread count
dd_yaml_output_dir          /opt/airflow/models         dbt YAML output path
dd_report_base_dir          /opt/airflow/reports        DDHelper base directory
dd_alert_email              <none>                      Report recipient email address
dd_email_subject            Database Schema Report      Email subject line
dd_schedule                 0 2 * * *                   Cron schedule (daily 02:00)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── Add the include/ directory to sys.path so we can import dd_builder_tasks ──
_DAG_DIR     = Path(__file__).parent          # .../dags/
_INCLUDE_DIR = _DAG_DIR.parent / "include"   # .../include/
if str(_INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(_INCLUDE_DIR))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import all task callables from the include script
from dd_builder_tasks import (
    resolve_db_config,
    run_connection_test,
    run_list_schemas,
    run_list_tables,
    run_extract_metadata,
    run_extract_single_schema,
    run_extract_single_table,
    run_generate_yaml_files,
    run_generate_combined_yaml,
    run_detect_documentation_gaps,
    run_compare_schemas,
    run_compile_pdf,
    run_send_email,
    run_export_metadata,
)


# ===========================================================================
# Helper: read Airflow Variable with a fallback
# ===========================================================================

def _var(key: str, default: str = "") -> str:
    """Read an Airflow Variable; fall back to an env var, then ``default``."""
    try:
        return Variable.get(key, default_var=None) or os.getenv(key.upper(), default)
    except Exception:
        return os.getenv(key.upper(), default)


def _var_int(key: str, default: int) -> int:
    try:
        return int(_var(key, str(default)))
    except (TypeError, ValueError):
        return default


# ===========================================================================
# Pipeline configuration
# (all values are read at DAG-parse time from Airflow Variables / env vars)
# ===========================================================================

SOURCE_CONN_ID     = _var("dd_source_conn_id",  "source_db_conn")
DEST_CONN_ID       = _var("dd_dest_conn_id",    "dest_db_conn")
SMTP_CONN_ID       = _var("dd_smtp_conn_id",    "smtp_conn")

# Schema / table targeting
_schema_filter_raw = _var("dd_schema_filter", "")
SCHEMA_FILTER: list = (
    [s.strip() for s in _schema_filter_raw.split(",") if s.strip()]
    if _schema_filter_raw
    else None          # None → extract ALL schemas
)
TARGET_SCHEMA  = _var("dd_target_schema",  "public")   # used for single-schema tasks
TARGET_TABLE   = _var("dd_target_table",   "")         # used for single-table task; set via Variable

# Extraction options
PARALLEL_WORKERS = _var_int("dd_parallel_workers", 8)

# Output directories
YAML_OUTPUT_DIR = _var("dd_yaml_output_dir",  "/opt/airflow/models")
REPORT_BASE_DIR = _var("dd_report_base_dir",  "/opt/airflow/reports")

# Email
ALERT_EMAIL   = _var("dd_alert_email",   "")
EMAIL_SUBJECT = _var("dd_email_subject", "Database Schema Comparison Report")

# DAG schedule
SCHEDULE = _var("dd_schedule", "0 2 * * *")   # daily at 02:00 UTC by default

# ---------------------------------------------------------------------------
# Resolve DB configs once at DAG-parse time so op_kwargs are plain dicts
# (avoids importing Airflow hooks inside worker processes on remote executors)
# ---------------------------------------------------------------------------
SOURCE_DB_CONFIG = resolve_db_config(conn_id=SOURCE_CONN_ID, env_prefix="SOURCE")
DEST_DB_CONFIG   = resolve_db_config(conn_id=DEST_CONN_ID,   env_prefix="DEST")


# ===========================================================================
# DAG definition
# ===========================================================================

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email":            [ALERT_EMAIL] if ALERT_EMAIL else [],
    "email_on_failure": bool(ALERT_EMAIL),
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="db_metadata_pipeline",
    default_args=default_args,
    description=(
        "Extract database metadata, generate dbt YAML, compare schemas, "
        "compile a PDF report, and email it."
    ),
    schedule=SCHEDULE,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["database", "metadata", "dbt", "data_dictionary_builder"],
) as dag:

    # ── Task 1: Connection test ────────────────────────────────────────────
    t_connection_test = PythonOperator(
        task_id="connection_test",
        python_callable=run_connection_test,
        op_kwargs={
            "source_config": SOURCE_DB_CONFIG,
            "dest_config":   DEST_DB_CONFIG,
        },
        doc_md="""
        **Connection Test**
        Verifies that both source and destination databases are reachable.
        Fails the pipeline early if either connection is down.
        """,
    )

    # ── Task 2: List schemas ───────────────────────────────────────────────
    t_list_schemas = PythonOperator(
        task_id="list_schemas",
        python_callable=run_list_schemas,
        op_kwargs={
            "db_config": SOURCE_DB_CONFIG,
            "xcom_key":  "schemas",
        },
        doc_md="""
        **List Schemas**
        Lists all available schemas on the source database and pushes
        the result to XCom under key ``schemas``.
        """,
    )

    # ── Task 3: List tables ────────────────────────────────────────────────
    t_list_tables = PythonOperator(
        task_id="list_tables",
        python_callable=run_list_tables,
        op_kwargs={
            "db_config":   SOURCE_DB_CONFIG,
            "schema_name": TARGET_SCHEMA,
            "xcom_key":    "tables",
        },
        doc_md="""
        **List Tables**
        Lists all tables in ``TARGET_SCHEMA`` and pushes them to XCom
        under key ``tables``.  Set ``dd_target_schema`` Variable to change
        which schema is inspected.
        """,
    )

    # ── Task 4a: Full metadata extraction ─────────────────────────────────
    t_extract_metadata = PythonOperator(
        task_id="extract_metadata",
        python_callable=run_extract_metadata,
        op_kwargs={
            "db_config":        SOURCE_DB_CONFIG,
            "schema_filter":    SCHEMA_FILTER,
            "parallel_workers": PARALLEL_WORKERS,
            "xcom_key":         "db_metadata",
        },
        doc_md="""
        **Extract Full Metadata**
        Connects to the source database and extracts metadata for all schemas
        matching ``dd_schema_filter`` (or all schemas when unset).
        Uses ``parallel_workers`` threads for concurrent extraction.
        Pushes serialised ``DatabaseMetadata`` to XCom under ``db_metadata``.

        ``dd_schema_filter`` examples:
        - ``"public,analytics"``           — exact names
        - ``"prefix:stg_,suffix:_prod"``   — prefix + suffix mix
        - ``"regex:^raw_\\d{4}$"``         — regex
        """,
    )

    # ── Task 4b: Extract single schema ────────────────────────────────────
    t_extract_single_schema = PythonOperator(
        task_id="extract_single_schema",
        python_callable=run_extract_single_schema,
        op_kwargs={
            "db_config":   SOURCE_DB_CONFIG,
            "schema_name": TARGET_SCHEMA,
            "xcom_key":    "schema_metadata",
        },
        doc_md="""
        **Extract Single Schema**
        Extracts metadata for ``TARGET_SCHEMA`` only.
        Pushes serialised ``DatabaseMetadata`` to XCom under ``schema_metadata``.
        Runs in parallel with ``extract_metadata``.
        """,
    )

    # ── Task 4c: Extract single table ─────────────────────────────────────
    t_extract_single_table = PythonOperator(
        task_id="extract_single_table",
        python_callable=run_extract_single_table,
        op_kwargs={
            "db_config":   SOURCE_DB_CONFIG,
            "schema_name": TARGET_SCHEMA,
            "table_name":  TARGET_TABLE or "orders",   # fallback name for demo
            "xcom_key":    "table_metadata",
        },
        doc_md="""
        **Extract Single Table**
        Extracts column-level metadata (PK, FK, nullability, types) for one
        table.  Set ``dd_target_table`` Variable to control which table is
        inspected.  Runs in parallel with ``extract_metadata``.
        """,
    )

    # ── Task 5: Generate per-schema YAML ──────────────────────────────────
    t_generate_yaml = PythonOperator(
        task_id="generate_yaml_files",
        python_callable=run_generate_yaml_files,
        op_kwargs={
            "yaml_output_dir":   YAML_OUTPUT_DIR,
            "metadata_task_id":  "extract_metadata",
            "metadata_xcom_key": "db_metadata",
        },
        doc_md="""
        **Generate YAML Files**
        Pulls ``DatabaseMetadata`` from XCom and writes one dbt-compatible
        YAML file per schema into ``YAML_OUTPUT_DIR``.
        Smart merge: existing user descriptions and dbt tests are preserved.
        """,
    )

    # ── Task 6: Generate combined YAML ────────────────────────────────────
    t_generate_combined_yaml = PythonOperator(
        task_id="generate_combined_yaml",
        python_callable=run_generate_combined_yaml,
        op_kwargs={
            "yaml_output_dir":   YAML_OUTPUT_DIR,
            "metadata_task_id":  "extract_metadata",
            "metadata_xcom_key": "db_metadata",
            "combined_filename": "all_models.yml",
            "xcom_key":          "combined_yaml_path",
        },
        doc_md="""
        **Generate Combined YAML**
        Writes a single ``all_models.yml`` containing all schemas and tables.
        Useful when you prefer one file over per-schema files.
        """,
    )

    # ── Task 7: Documentation gap detection ───────────────────────────────
    t_detect_gaps = PythonOperator(
        task_id="detect_documentation_gaps",
        python_callable=run_detect_documentation_gaps,
        op_kwargs={
            "metadata_task_id":  "extract_metadata",
            "metadata_xcom_key": "db_metadata",
            "xcom_key":          "doc_gaps",
        },
        doc_md="""
        **Detect Documentation Gaps**
        Scans extracted metadata for tables and columns that have no description.
        Outputs coverage percentages and pushes the gap lists to XCom under
        ``doc_gaps`` for downstream alerting or reporting.
        """,
    )

    # ── Task 8: Schema comparison ──────────────────────────────────────────
    t_compare_schemas = PythonOperator(
        task_id="compare_schemas",
        python_callable=run_compare_schemas,
        op_kwargs={
            "source_config":             SOURCE_DB_CONFIG,
            "dest_config":               DEST_DB_CONFIG,
            "schema_names":              SCHEMA_FILTER or [TARGET_SCHEMA],
            "yaml_output_dir":           YAML_OUTPUT_DIR,
            "report_base_dir":           REPORT_BASE_DIR,
            "include_yaml_gaps":         True,
            "parallel_workers":          PARALLEL_WORKERS,
            # Reuse already-extracted source metadata — avoids a second DB query
            "source_metadata_task_id":   "extract_metadata",
            "source_metadata_xcom_key":  "db_metadata",
            "xcom_key":                  "comparison_report",
        },
        doc_md="""
        **Compare Schemas**
        Diffs each schema in ``schema_names`` between source and destination.
        Reuses the XCom metadata from ``extract_metadata`` for the source so
        the source DB is not queried again.  Extracts the destination snapshot
        once and reuses it across all schemas.

        Produces a combined report with:
        - Missing tables / columns
        - Data type mismatches
        - Documentation gap counts

        Saves the report as JSON via DDHelper and pushes it to XCom.
        """,
    )

    # ── Task 9: Compile PDF ────────────────────────────────────────────────
    t_compile_pdf = PythonOperator(
        task_id="compile_pdf",
        python_callable=run_compile_pdf,
        op_kwargs={
            "report_base_dir":    REPORT_BASE_DIR,
            "report_task_id":     "compare_schemas",
            "report_xcom_key":    "comparison_report",
            "json_path_xcom_key": "report_json_path",
            "pdf_xcom_key":       "pdf_path",
        },
        doc_md="""
        **Compile PDF**
        Reads the JSON report saved by ``compare_schemas`` and compiles it
        into a formatted PDF with cover page, summary table, missing-table
        list, missing-column list, type-mismatch list, and documentation
        gap tables.  Requires ``reportlab`` (``pip install reportlab``).
        """,
    )

    # ── Task 10: Send email ────────────────────────────────────────────────
    t_send_email = PythonOperator(
        task_id="send_email",
        python_callable=run_send_email,
        op_kwargs={
            "report_base_dir":  REPORT_BASE_DIR,
            "report_task_id":   "compare_schemas",
            "report_xcom_key":  "comparison_report",
            "pdf_task_id":      "compile_pdf",
            "pdf_xcom_key":     "pdf_path",
            "email_to":         ALERT_EMAIL or None,
            "subject":          EMAIL_SUBJECT,
            "smtp_conn_id":     SMTP_CONN_ID or None,
        },
        doc_md="""
        **Send Email**
        Emails the comparison report with the compiled PDF attached.
        SMTP credentials are resolved from the Airflow ``smtp_conn``
        connection, or from ``SMTP_HOST`` / ``SMTP_PORT`` / ``SMTP_USER``
        / ``SMTP_PASSWORD`` environment variables.
        Skipped gracefully when no SMTP config is present.
        """,
    )

    # ── Task 11: Export metadata + round-trip validation ──────────────────
    t_export_metadata = PythonOperator(
        task_id="export_metadata",
        python_callable=run_export_metadata,
        op_kwargs={
            "report_base_dir":   REPORT_BASE_DIR,
            "metadata_task_id":  "extract_metadata",
            "metadata_xcom_key": "db_metadata",
            "xcom_key":          "metadata_export_path",
        },
        doc_md="""
        **Export Metadata + Round-Trip Validation**
        Serialises the ``DatabaseMetadata`` to a JSON file in
        ``reports/json/`` via DDHelper.  Then restores it via
        ``DatabaseMetadata.from_dict()`` and asserts that all tables are
        preserved — confirming the object is safe for XCom / Airflow task
        boundaries and downstream catalog APIs.
        """,
    )


    # =========================================================================
    # Task dependencies
    # =========================================================================
    #
    # connection_test
    #   ├── list_schemas
    #   ├── list_tables
    #   ├── extract_metadata ──────────────────────────┐
    #   │     ├── generate_yaml_files                  │
    #   │     ├── generate_combined_yaml               │
    #   │     ├── detect_documentation_gaps            │
    #   │     ├── compare_schemas ──────────────────── │ ──> compile_pdf ──> send_email
    #   │     └── export_metadata                      │
    #   ├── extract_single_schema (parallel w/ 4a)     │
    #   └── extract_single_table  (parallel w/ 4a)     │
    #                                                   │
    #   list_tables ──────────────────────────────── feeds TARGET_TABLE Variable
    #

    # Gate everything behind connection_test
    t_connection_test >> [
        t_list_schemas,
        t_list_tables,
        t_extract_metadata,
        t_extract_single_schema,
        t_extract_single_table,
    ]

    # Full extraction feeds YAML, gap detection, comparison, and export
    t_extract_metadata >> [
        t_generate_yaml,
        t_generate_combined_yaml,
        t_detect_gaps,
        t_compare_schemas,
        t_export_metadata,
    ]

    # Comparison → PDF → email
    t_compare_schemas >> t_compile_pdf >> t_send_email
