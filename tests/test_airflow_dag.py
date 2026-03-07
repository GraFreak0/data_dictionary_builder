"""
Airflow DAG: db_metadata_extraction_and_comparison

Demonstrates how to use data_dictionary_builder in a production Airflow pipeline:

  1. extract_metadata      — connect to the source database, extract all schema
                             metadata, and push it to XCom as a serialised dict.
  2. generate_yaml_files   — pull the dict from XCom, deserialise it, and write
                             dbt-compatible YAML files to the configured output dir.
  3. compare_schemas       — compare each target schema between the source and
                             destination databases and push a combined report to XCom.
  4. send_report_email     — pull the report from XCom, compile a PDF, and email
                             the report with the PDF attached.

Credentials are read from Airflow Variables (or environment variables) — no
secrets are hardcoded in this file.

Usage:
    Copy this file to your Airflow DAGs directory.
    Set the Airflow Variables listed under "Configuration" below, or export the
    corresponding environment variables before starting the Airflow scheduler.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from data_dictionary_builder import (
    MetadataExtractor,
    DatabaseMetadata,
    YAMLGenerator,
    SchemaComparator,
    DDHelper,
    ExecutionTimer,
)


# ── Configuration ─────────────────────────────────────────────────────────────
# Replace the placeholder strings with Airflow Variable lookups in production:
#   from airflow.models import Variable
#   Variable.get("source_db_host")

SOURCE_DB_CONFIG = {
    "db_type":  os.getenv("SOURCE_DB_TYPE",  "postgres"),
    "host":     os.getenv("SOURCE_DB_HOST",  "prod-db.example.com"),
    "port":     int(os.getenv("SOURCE_DB_PORT", "5432")),
    "database": os.getenv("SOURCE_DB_NAME",  "source_db"),
    "user":     os.getenv("SOURCE_DB_USER",  "readonly"),
    "password": os.getenv("SOURCE_DB_PASSWORD", ""),
}

DEST_DB_CONFIG = {
    "db_type":  os.getenv("DEST_DB_TYPE",  "postgres"),
    "host":     os.getenv("DEST_DB_HOST",  "staging-db.example.com"),
    "port":     int(os.getenv("DEST_DB_PORT", "5432")),
    "database": os.getenv("DEST_DB_NAME",  "dest_db"),
    "user":     os.getenv("DEST_DB_USER",  "readonly"),
    "password": os.getenv("DEST_DB_PASSWORD", ""),
}

SCHEMAS_TO_EXTRACT = os.getenv("TARGET_SCHEMAS", "public,analytics").split(",")
YAML_OUTPUT_DIR    = os.getenv("YAML_OUTPUT_DIR", "/opt/airflow/dbt/models")
REPORT_BASE_DIR    = os.getenv("REPORT_BASE_DIR", "/opt/airflow/reports")
ALERT_EMAIL        = os.getenv("ALERT_EMAIL", "data-team@example.com")


# ── Task functions ─────────────────────────────────────────────────────────────

def extract_metadata(**context):
    """
    Connect to the source database, extract schema metadata for all configured
    schemas, serialise the result, and push it to XCom.
    """
    timer = ExecutionTimer()

    with timer.task("Extract metadata"):
        with MetadataExtractor(**SOURCE_DB_CONFIG) as ext:
            if not ext.test_connection():
                raise RuntimeError(
                    f"Cannot connect to source database at {SOURCE_DB_CONFIG['host']}"
                )

            db_meta = ext.extract_all_schemas(
                schema_filter=SCHEMAS_TO_EXTRACT,
                parallel_workers=8,
            )

    schema_count = len(db_meta.schemas)
    table_count  = sum(len(s.tables) for s in db_meta.schemas)
    print(f"Extracted {schema_count} schemas with {table_count} tables total.")

    # Serialise for XCom — DatabaseMetadata is not directly JSON-serialisable
    context["task_instance"].xcom_push(key="db_metadata", value=db_meta.to_dict())

    timer.summary("1. Extract metadata")
    return f"Extracted {schema_count} schemas, {table_count} tables"


def generate_yaml_files(**context):
    """
    Pull the serialised DatabaseMetadata from XCom, deserialise it, and write
    dbt-compatible YAML files.  Uses smart merge: existing descriptions and
    test definitions are preserved; new tables/columns are appended.
    """
    timer = ExecutionTimer()

    db_meta_dict = context["task_instance"].xcom_pull(
        task_ids="extract_metadata", key="db_metadata"
    )
    if not db_meta_dict:
        raise ValueError("No metadata found in XCom — did extract_metadata succeed?")

    with timer.task("Generate YAML"):
        db_meta = DatabaseMetadata.from_dict(db_meta_dict)
        helper  = DDHelper(REPORT_BASE_DIR)
        gen     = YAMLGenerator(output_dir=YAML_OUTPUT_DIR)
        files   = gen.generate_yaml_files(db_meta)

    print(f"Generated {len(files)} YAML file(s):")
    for f in files:
        print(f"  {f}")

    timer.summary("2. Generate YAML")
    return f"Generated {len(files)} YAML files"


def compare_schemas(**context):
    """
    Compare each target schema between the source and destination databases.
    Builds a combined report across all schemas and pushes it to XCom.
    """
    timer = ExecutionTimer()

    # Reuse the already-extracted source metadata from XCom so we don't
    # hit the source database again.
    db_meta_dict = context["task_instance"].xcom_pull(
        task_ids="extract_metadata", key="db_metadata"
    )
    source_db_meta = DatabaseMetadata.from_dict(db_meta_dict) if db_meta_dict else None

    comparator = SchemaComparator(
        source_config=SOURCE_DB_CONFIG,
        destination_config=DEST_DB_CONFIG,
        yaml_output_dir=YAML_OUTPUT_DIR,
    )

    # Extract destination metadata once, then reuse for every schema comparison.
    with timer.task("Extract destination snapshot"):
        with MetadataExtractor(**DEST_DB_CONFIG) as ext:
            dest_db_meta = ext.extract_all_schemas(
                schema_filter=SCHEMAS_TO_EXTRACT,
                parallel_workers=8,
            )

    # Accumulate per-schema reports into a combined structure.
    combined = {
        "summary": {
            "missing_tables_count": 0,
            "missing_columns_count": 0,
            "type_mismatches_count": 0,
            "tables_without_descriptions_count": 0,
            "columns_without_descriptions_count": 0,
        },
        "comparison": {"missing_tables": [], "missing_columns": [], "type_mismatches": []},
        "yaml_gaps":  {"tables_without_descriptions": [], "columns_without_descriptions": []},
        "schemas_compared": [],
    }

    with timer.task("Compare schemas"):
        for schema_name in SCHEMAS_TO_EXTRACT:
            print(f"Comparing schema: {schema_name}")
            report = comparator.compare_and_generate_report(
                source_schema_name=schema_name,
                include_yaml_gaps=True,
                dest_db_metadata=dest_db_meta,
                source_db_metadata=source_db_meta,
            )

            # Merge summary counts
            for key in combined["summary"]:
                combined["summary"][key] += report["summary"].get(key, 0)

            # Merge detail lists
            for key in ("missing_tables", "missing_columns", "type_mismatches"):
                combined["comparison"][key].extend(report["comparison"].get(key, []))

            for key in ("tables_without_descriptions", "columns_without_descriptions"):
                combined["yaml_gaps"][key].extend(report.get("yaml_gaps", {}).get(key, []))

            combined["schemas_compared"].append(schema_name)

    context["task_instance"].xcom_push(key="comparison_report", value=combined)

    summary = combined["summary"]
    print(
        f"Comparison complete — "
        f"missing tables: {summary['missing_tables_count']}, "
        f"missing columns: {summary['missing_columns_count']}, "
        f"type mismatches: {summary['type_mismatches_count']}"
    )

    timer.summary("3. Compare schemas")
    return "Schema comparison complete"


def send_report_email(**context):
    """
    Pull the combined comparison report from XCom, save it as JSON, compile a
    PDF, and email the report with the PDF attached.
    """
    timer = ExecutionTimer()

    report = context["task_instance"].xcom_pull(
        task_ids="compare_schemas", key="comparison_report"
    )
    if not report:
        raise ValueError("No report found in XCom — did compare_schemas succeed?")

    with timer.task("Save, compile PDF, and send email"):
        helper    = DDHelper(REPORT_BASE_DIR)
        json_path = helper.save_report(report)
        pdf_path  = helper.compile_pdf(source_json=json_path)

        sent = helper.send_report_email(
            report=report,
            pdf_path=pdf_path,
            subject=f"Schema comparison report — {datetime.now().strftime('%Y-%m-%d')}",
            email_to=ALERT_EMAIL,
        )

    status = "sent" if sent else "skipped (SMTP not configured)"
    print(f"Email {status}. Report saved to {json_path}")
    if pdf_path:
        print(f"PDF compiled at {pdf_path}")

    timer.summary("4. Report delivery")
    return f"Email {status}"


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="db_metadata_extraction_and_comparison",
    default_args=default_args,
    description="Extract database metadata, generate dbt YAML, compare schemas, and email a report.",
    schedule="0 2 * * *",   # daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["database", "metadata", "dbt", "data_dictionary_builder"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_metadata",
        python_callable=extract_metadata,
    )

    task_yaml = PythonOperator(
        task_id="generate_yaml_files",
        python_callable=generate_yaml_files,
    )

    task_compare = PythonOperator(
        task_id="compare_schemas",
        python_callable=compare_schemas,
    )

    task_email = PythonOperator(
        task_id="send_report_email",
        python_callable=send_report_email,
    )

    task_extract >> task_yaml >> task_compare >> task_email
