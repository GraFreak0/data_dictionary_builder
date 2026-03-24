"""
db_metadata_dag.py
==================
Airflow DAG — data_dictionary_builder multi-pipeline metadata pipeline.

What this DAG does
------------------
For each configured pipeline (source database → destination warehouse):

  1.  Extract source metadata      Pull column/table/schema info from the source DB.
  2.  Extract destination metadata Pull a snapshot from the warehouse (run in parallel
                                    with step 1 so both connections happen simultaneously).
  3.  Generate dbt YAML files      Write one model YAML per source schema.
  4.  Detect documentation gaps    Report undocumented tables and columns in the source.
  5.  Compare [schema] × N         One task per schema — source (baseline) vs warehouse.
                                    All schema comparisons run in parallel within a pipeline.
  6.  Compile PDF                  Combine all comparison reports into a single PDF.
  7.  Send notification            Deliver the PDF report via email, Slack, or both.
  8.  Export metadata              Write source metadata to JSON + validate round-trip.

Multiple pipelines run in parallel so different source servers and warehouse
instances are processed simultaneously without blocking each other.

──────────────────────────────────────────────────────────────────────────────
Airflow UI — what you'll see
──────────────────────────────────────────────────────────────────────────────
Each pipeline appears as a collapsed TaskGroup named ``pipeline__{label}``.
Inside each group you'll see individual tasks for every schema being compared:

    pipeline__prod_to_analytics
    ├── test_connections
    ├── extract_source
    ├── extract_destination
    ├── generate_yaml
    ├── detect_doc_gaps
    ├── compare__public          ← one node per schema, run in parallel
    ├── compare__analytics
    ├── compare__reporting
    ├── compile_pdf
    ├── send_notification
    └── export_metadata

──────────────────────────────────────────────────────────────────────────────
Configuration — Airflow Variables
──────────────────────────────────────────────────────────────────────────────
Set these in Admin → Variables (or as environment variables):

dd_pipelines  (JSON array — primary config)
    A list of pipeline objects.  Each pipeline connects one source database
    to one destination warehouse.  Example:

    [
      {
        "label":          "prod_to_analytics",
        "source_conn_id": "source_postgres_prod",
        "dest_conn_id":   "warehouse_analytics",
        "schemas":        ["public", "analytics", "reporting"],
        "schema_filter":  null,
        "parallel_workers": 8,
        "email_to":       "analytics-team@company.com",
        "email_subject":  "Prod → Analytics Comparison"
      },
      {
        "label":          "mysql_to_reporting",
        "source_conn_id": "source_mysql_ops",
        "dest_conn_id":   "warehouse_reporting",
        "schemas":        ["orders", "inventory"],
        "parallel_workers": 4
      }
    ]

    Pipeline object fields:
      label           (required) Unique identifier — used in task group names
                                  and output file names.
      source_conn_id  (required) Airflow connection ID for the source database.
      dest_conn_id    (required) Airflow connection ID for the destination warehouse.
      schemas         (required) Explicit list of schema names to process.
                                  Each schema gets its own compare task in the UI.
      schema_filter   (optional) Filter string (comma-separated) passed to
                                  extract_all_schemas().  Overrides ``schemas``
                                  when provided for the extraction steps; the
                                  explicit ``schemas`` list is still used to
                                  generate per-schema compare tasks.
      parallel_workers (optional, default 8)  Extraction thread count.
      email_to           (optional) Override the recipient for this pipeline.
      email_subject      (optional) Override the email subject for this pipeline.
      notification_type  (optional) "email" | "slack" | "both" — overrides the
                                     NOTIFICATION_TYPE env var for this pipeline.
      slack_target       (optional) Override SLACK_NOTIFY_TARGET for this pipeline.

dd_yaml_output_dir   /opt/airflow/models    dbt YAML output path
dd_report_base_dir   /opt/airflow/reports   DDHelper base directory
dd_alert_email       <none>                 Default report recipient email
dd_email_subject     Database Schema Report Default email subject
dd_smtp_conn_id      smtp_conn              Airflow SMTP connection ID
dd_schedule          0 2 * * *              Cron schedule (daily 02:00 UTC)

Environment variables (notification):
  NOTIFICATION_TYPE    email | slack | both   (default: email)
  SLACK_BOT_TOKEN      xoxb-… Bot User OAuth Token
  SLACK_NOTIFY_TARGET  #channel-name, C…, or U… user ID

──────────────────────────────────────────────────────────────────────────────
Single-pipeline fallback (backward compatible)
──────────────────────────────────────────────────────────────────────────────
If ``dd_pipelines`` is not set, the DAG falls back to the original single-pipeline
behaviour using these Variables:

  dd_source_conn_id   source_db_conn   Airflow conn ID for the source database
  dd_dest_conn_id     dest_db_conn     Airflow conn ID for the destination warehouse
  dd_schema_filter    <none>           Comma-separated schema filter
  dd_schemas          <none>           Comma-separated explicit schema list
  dd_parallel_workers 8                Extraction thread count

──────────────────────────────────────────────────────────────────────────────
Airflow Connection setup
──────────────────────────────────────────────────────────────────────────────
Create connections in Admin → Connections (or ``airflow connections add``):

  source_postgres_prod   — source PostgreSQL production database
  source_mysql_ops       — source MySQL operations database
  warehouse_analytics    — ClickHouse / BigQuery / Redshift analytics warehouse
  warehouse_reporting    — separate warehouse instance for reporting data
  smtp_conn              — SMTP server for emailing reports (conn_type=smtp)

See ``include/dd_builder_tasks.py`` for the full connection resolution logic.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from dotenv import load_dotenv


_DAG_DIR     = Path(__file__).parent
_INCLUDE_DIR = Path(__file__).parents[3] / "plugins"
if str(_INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(_INCLUDE_DIR))

from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from dd_builder_tasks import (
    resolve_db_config,
    run_connection_test,
    run_extract_metadata,
    run_extract_destination_metadata,
    run_generate_yaml_files,
    run_detect_documentation_gaps,
    run_compare_single_schema,
    run_compile_pdf,
    run_send_notification,
    run_export_metadata,
)

log = logging.getLogger(__name__)


# ===========================================================================
# Helpers — read Airflow Variables with env-var fallback
# ===========================================================================

def _var(key: str, default: str = "") -> str:
    """Read an Airflow Variable, fall back to an env var, then ``default``."""
    try:
        return Variable.get(key, default_var=None) or os.getenv(key.upper(), default)
    except Exception:
        return os.getenv(key.upper(), default)


def _var_int(key: str, default: int) -> int:
    try:
        return int(_var(key, str(default)))
    except (TypeError, ValueError):
        return default


def _var_json(key: str, default: Any = None) -> Any:
    """Read an Airflow Variable and parse it as JSON."""
    raw = _var(key, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.error("Could not parse Variable '%s' as JSON: %s", key, exc)
        return default


def _safe_id(name: str) -> str:
    """Convert a name to a safe Airflow task/group ID segment."""
    return name.replace(".", "__").replace("-", "_").replace(" ", "_")


# ===========================================================================
# Pipeline configuration
# ===========================================================================

# Global output directories (shared across all pipelines)
YAML_OUTPUT_DIR = _var("dd_yaml_output_dir", "/opt/airflow/models")
REPORT_BASE_DIR = _var("dd_report_base_dir", "/opt/airflow/reports")
MODELS_DIR      = _var("dd_models_dir",      None)
REPORTS_DIR     = _var("dd_reports_dir",     None)
DEFAULT_SUBJECT = _var("dd_email_subject",   "Database Schema Comparison Report")
SCHEDULE        = _var("dd_schedule",        "0 2 * * *")

# SMTP — credentials resolved from .env at parse time
SMTP_HOST     = os.getenv("SMTP_HOST",     "")
SMTP_PORT     = os.getenv("SMTP_PORT")          # None → DDHelper reads env at send time
SMTP_USER     = os.getenv("SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

# Recipients — comma-separated list supported
DEFAULT_EMAIL_RECIPIENTS = [
    e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()
]
DEFAULT_EMAIL = ", ".join(DEFAULT_EMAIL_RECIPIENTS)   # kept for doc_md strings

# Notification
DEFAULT_NOTIFICATION_TYPE = os.getenv("NOTIFICATION_TYPE", "email")
SLACK_BOT_TOKEN           = os.getenv("SLACK_BOT_TOKEN", "")
DEFAULT_SLACK_TARGETS     = [
    t.strip() for t in os.getenv("SLACK_NOTIFY_TARGET", "").split(",") if t.strip()
]
SLACK_NOTIFY_TARGET = ", ".join(DEFAULT_SLACK_TARGETS)   # kept for doc_md strings

def _load_pipelines() -> List[Dict[str, Any]]:
    """
    Return the single hardcoded pipeline:
        source  — demo_clickhouse   (credentials from Airflow Connection)
        dest    — clickhouse_local  (credentials from Airflow Connection)
        schema  — 'default'         (hardcoded)
    """
    pl: Dict[str, Any] = {
        "label":            "demo_to_local",
        "source_conn_id":   "demo_clickhouse",
        "dest_conn_id":     "clickhouse_local",
        "schemas":          ["default"],
        "schema_filter":    ["default"],
        "parallel_workers": 8,
        "email_to":         DEFAULT_EMAIL_RECIPIENTS,
        "email_subject":    DEFAULT_SUBJECT,
    }

    pl["source_config"] = resolve_db_config(conn_id=pl["source_conn_id"], db_type="clickhouse", database="default")
    pl["dest_config"]   = resolve_db_config(conn_id=pl["dest_conn_id"],   db_type="clickhouse", database="default")

    log.info("Pipeline: %s → %s, schema='default'", pl["source_conn_id"], pl["dest_conn_id"])
    return [pl]


PIPELINES = _load_pipelines()

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email":            [DEFAULT_EMAIL] if DEFAULT_EMAIL else [],
    "email_on_failure": bool(DEFAULT_EMAIL),
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

with DAG(
    dag_id="db_metadata_pipeline",
    default_args=default_args,
    description=(
        "Extract metadata from source databases, generate dbt YAML model files, "
        "compare schemas against destination warehouses, compile PDF reports, and email them. "
        f"Pipelines: {[p['label'] for p in PIPELINES]}"
    ),
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["database", "metadata", "dbt", "data_dictionary_builder"],
) as dag:

    all_configs = []
    for _pl in PIPELINES:
        all_configs.append({**_pl["source_config"], "_label": f"SOURCE:{_pl['label']}"})
        all_configs.append({**_pl["dest_config"],   "_label": f"DEST:{_pl['label']}"})

    t_global_connection_test = PythonOperator(
        task_id="test_all_connections",
        python_callable=run_connection_test,
        op_kwargs={"configs": all_configs},
        doc_md=f"""
        **Test All Connections**

        Verifies every source database and destination warehouse before any
        extraction begins.  Fails the entire DAG run immediately if any
        connection is down.

        Connections tested:
        {chr(10).join(f'- **SOURCE** `{p["label"]}`: {p["source_config"].get("db_type")} @ {p["source_config"].get("host")}' for p in PIPELINES)}
        {chr(10).join(f'- **DEST**   `{p["label"]}`: {p["dest_config"].get("db_type")} @ {p["dest_config"].get("host")}' for p in PIPELINES)}
        """,
    )

    # =========================================================================
    # One TaskGroup per pipeline — all pipeline groups run in parallel
    # =========================================================================

    pipeline_groups = []

    for pipeline in PIPELINES:
        label            = pipeline["label"]
        safe_label       = _safe_id(label)
        source_cfg       = pipeline["source_config"]
        dest_cfg         = pipeline["dest_config"]
        schemas          = pipeline["schemas"]          # explicit list for per-schema tasks
        schema_filter    = pipeline.get("schema_filter")
        parallel_workers    = pipeline["parallel_workers"]
        # email_to / slack_target: accept list or comma-separated string from pipeline config
        _raw_email   = pipeline["email_to"]
        email_to     = _raw_email if isinstance(_raw_email, list) else [e.strip() for e in str(_raw_email).split(",") if e.strip()]
        email_subject       = pipeline["email_subject"]
        notification_type   = pipeline.get("notification_type", DEFAULT_NOTIFICATION_TYPE)
        _raw_slack   = pipeline.get("slack_target", DEFAULT_SLACK_TARGETS)
        pipeline_slack_targets = _raw_slack if isinstance(_raw_slack, list) else [t.strip() for t in str(_raw_slack).split(",") if t.strip()]
        # Display strings for doc_md
        email_to_display = ", ".join(email_to)
        slack_targets_display = ", ".join(pipeline_slack_targets)

        extraction_filter = schema_filter or schemas

        schema_list_md = "\n".join(f"- `{s}`" for s in schemas)

        with TaskGroup(
            group_id=f"pipeline__{safe_label}",
            tooltip=(
                f"Pipeline: {label} | "
                f"SOURCE: {source_cfg.get('db_type')} @ {source_cfg.get('host')} | "
                f"DEST: {dest_cfg.get('db_type')} @ {dest_cfg.get('host')} | "
                f"Schemas: {schemas}"
            ),
        ) as pipeline_group:

            t_extract_source = PythonOperator(
                task_id="extract_source",
                python_callable=run_extract_metadata,
                op_kwargs={
                    "db_config":        source_cfg,
                    "schema_filter":    extraction_filter,
                    "parallel_workers": parallel_workers,
                    "xcom_key":         "source_metadata",
                },
                doc_md=f"""
                **Extract Source Metadata** — pipeline `{label}`

                Connects to the source database and extracts column-level metadata
                for all schemas listed below.  The result is pushed to XCom and
                reused by every downstream task in this pipeline — no second query
                to the source DB is needed.

                Source: `{source_cfg.get("db_type")}` @ `{source_cfg.get("host")}`

                Schemas extracted:
                {schema_list_md}
                """,
            )

            t_extract_dest = PythonOperator(
                task_id="extract_destination",
                python_callable=run_extract_destination_metadata,
                op_kwargs={
                    "db_config":        dest_cfg,
                    "schema_filter":    extraction_filter,
                    "parallel_workers": parallel_workers,
                    "xcom_key":         "dest_metadata",
                },
                doc_md=f"""
                **Extract Destination (Warehouse) Metadata** — pipeline `{label}`

                Connects to the destination warehouse and takes a snapshot of the
                same schemas listed below.  This snapshot is reused by all
                per-schema comparison tasks so the warehouse is only queried once
                per pipeline run.

                Destination: `{dest_cfg.get("db_type")}` @ `{dest_cfg.get("host")}`

                Schemas extracted:
                {schema_list_md}
                """,
            )

            t_generate_yaml = PythonOperator(
                task_id="generate_yaml",
                python_callable=run_generate_yaml_files,
                op_kwargs={
                    "metadata_task_id":  f"pipeline__{safe_label}.extract_source",
                    "yaml_output_dir":   YAML_OUTPUT_DIR,
                    "models_dir":        MODELS_DIR,
                    "metadata_xcom_key": "source_metadata",
                },
                doc_md=f"""
                **Generate dbt YAML Model Files** — pipeline `{label}`

                Pulls source metadata from XCom and writes one dbt-compatible YAML
                file per schema into ``{YAML_OUTPUT_DIR}``.

                Smart merge is applied: user-written descriptions, dbt tests, and
                ``meta`` blocks in existing YAML files are always preserved.

                Schemas documented:
                {schema_list_md}
                """,
            )

            t_detect_gaps = PythonOperator(
                task_id="detect_doc_gaps",
                python_callable=run_detect_documentation_gaps,
                op_kwargs={
                    "metadata_task_id":  f"pipeline__{safe_label}.extract_source",
                    "metadata_xcom_key": "source_metadata",
                    "xcom_key":          "doc_gaps",
                },
                doc_md=f"""
                **Detect Documentation Gaps** — pipeline `{label}`

                Scans source metadata for tables and columns that have no description.
                Reports table and column coverage percentages.  Results are pushed to
                XCom for downstream reporting or alerting.

                Schemas checked:
                {schema_list_md}
                """,
            )

            compare_tasks = []
            for schema in schemas:
                safe_schema = _safe_id(schema)
                t_compare = PythonOperator(
                    task_id=f"compare__{safe_schema}",
                    python_callable=run_compare_single_schema,
                    op_kwargs={
                        "schema_name":              schema,
                        "yaml_output_dir":          YAML_OUTPUT_DIR,
                        "report_base_dir":          REPORT_BASE_DIR,
                        "models_dir":               MODELS_DIR,
                        "reports_dir":              REPORTS_DIR,
                        "source_config":            source_cfg,
                        "dest_config":              dest_cfg,
                        "include_yaml_gaps":        True,
                        "source_metadata_task_id":  f"pipeline__{safe_label}.extract_source",
                        "source_metadata_xcom_key": "source_metadata",
                        "dest_metadata_task_id":    f"pipeline__{safe_label}.extract_destination",
                        "dest_metadata_xcom_key":   "dest_metadata",
                        "xcom_key":                 f"comparison__{safe_schema}",
                    },
                    doc_md=f"""
                    **Compare Schema `{schema}`** — pipeline `{label}`

                    Compares schema `{schema}` between source (baseline) and destination
                    warehouse.  Surfaces tables, columns, and data types that exist in
                    the source but are missing or changed in the warehouse.

                    Source      : `{source_cfg.get("db_type")}` @ `{source_cfg.get("host")}`
                    Destination : `{dest_cfg.get("db_type")}` @ `{dest_cfg.get("host")}`
                    Schema      : `{schema}`

                    Both source and destination metadata are pulled from XCom —
                    no additional database connections are opened by this task.
                    """,
                )
                compare_tasks.append(t_compare)

            first_schema_safe = _safe_id(schemas[0])

            t_compile_pdf = PythonOperator(
                task_id="compile_pdf",
                python_callable=run_compile_pdf,
                op_kwargs={
                    "report_task_id":     f"pipeline__{safe_label}.compare__{first_schema_safe}",
                    "report_base_dir":    REPORT_BASE_DIR,
                    "reports_dir":        REPORTS_DIR,
                    "report_xcom_key":    f"comparison__{first_schema_safe}",
                    "json_path_xcom_key": "report_json_path",
                    "pdf_xcom_key":       "pdf_path",
                },
                doc_md=f"""
                **Compile PDF Report** — pipeline `{label}`

                Compiles the comparison report JSON into a paginated PDF with a
                cover page, summary table, missing-table list, missing-column list,
                type-mismatch list, and documentation gap tables.

                Requires ``reportlab``.  Skips gracefully if not installed.
                """,
            )

            t_send_notification = PythonOperator(
                task_id="send_notification",
                python_callable=run_send_notification,
                op_kwargs={
                    "report_task_id":       f"pipeline__{safe_label}.compare__{first_schema_safe}",
                    "report_base_dir":      REPORT_BASE_DIR,
                    "reports_dir":          REPORTS_DIR,
                    "report_xcom_key":      f"comparison__{first_schema_safe}",
                    "pdf_task_id":          f"pipeline__{safe_label}.compile_pdf",
                    "pdf_xcom_key":         "pdf_path",
                    "notification_type":    notification_type,
                    "email_to":             email_to or None,
                    "subject":              email_subject,
                    "smtp_host":            SMTP_HOST or None,
                    "smtp_port":            int(SMTP_PORT) if SMTP_PORT else None,
                    "smtp_user":            SMTP_USER or None,
                    "smtp_password":        SMTP_PASSWORD or None,
                    "slack_token":          SLACK_BOT_TOKEN or None,
                    "slack_target":         pipeline_slack_targets or None,
                    "slack_pipeline_label": label,
                },
                doc_md=f"""
                **Send Notification** — pipeline `{label}`

                Delivers the comparison PDF report via ``{notification_type}``.

                - **email** — sends to {len(email_to)} recipient(s): ``{email_to_display or "(EMAIL_TO)"}``
                  using SMTP credentials from ``SMTP_HOST`` / ``SMTP_PORT`` / ``SMTP_USER`` /
                  ``SMTP_PASSWORD``.
                - **slack** — posts a Block Kit summary to {len(pipeline_slack_targets)} target(s):
                  ``{slack_targets_display or "(SLACK_NOTIFY_TARGET)"}``
                  using the ``SLACK_BOT_TOKEN`` (``xoxb-…``).
                - **both** — sends email and Slack; each channel skips independently
                  if credentials are missing.

                Set ``NOTIFICATION_TYPE`` (or the pipeline ``notification_type`` field)
                to ``"email"``, ``"slack"``, or ``"both"``.
                """,
            )

            t_export_metadata = PythonOperator(
                task_id="export_metadata",
                python_callable=run_export_metadata,
                op_kwargs={
                    "metadata_task_id":  f"pipeline__{safe_label}.extract_source",
                    "report_base_dir":   REPORT_BASE_DIR,
                    "reports_dir":       REPORTS_DIR,
                    "metadata_xcom_key": "source_metadata",
                    "export_filename":   f"{label}_source_metadata.json",
                    "xcom_key":          "metadata_export_path",
                },
                doc_md=f"""
                **Export Source Metadata** — pipeline `{label}`

                Serialises the source ``DatabaseMetadata`` object to a JSON file
                in ``reports/json/`` and validates the round-trip (``to_dict()`` →
                ``from_dict()``) to confirm it is safe for XCom and catalog APIs.

                Output file: ``{label}_source_metadata.json``
                """,
            )

            t_extract_source >> [t_generate_yaml, t_detect_gaps, t_export_metadata]

            for t_compare in compare_tasks:
                [t_generate_yaml, t_extract_dest] >> t_compare

            compare_tasks >> t_compile_pdf >> t_send_notification

        pipeline_groups.append(pipeline_group)

    t_global_connection_test >> pipeline_groups
