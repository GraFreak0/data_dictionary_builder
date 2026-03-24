"""
data_dictionary_builder CLI
============================
Entry points: ddgen  |  data-dictionary-builder

Run  ddgen --help              for a command overview.
Run  ddgen features            for a full module reference.
Run  ddgen <command> --help    for per-command options and examples.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import click

from . import __version__


# ── Width / style helpers ─────────────────────────────────────────────────────

W = 88   # preferred terminal width

def _hr(char: str = "─", width: int = W) -> str:
    return char * width

def _section(title: str) -> None:
    click.echo()
    click.echo(click.style(f"  {title}", fg="cyan", bold=True))
    click.echo("  " + _hr("─", W - 2))

def _ok(msg: str)   -> None: click.echo(click.style(f"  ✓  {msg}", fg="green"))
def _err(msg: str)  -> None: click.echo(click.style(f"  ✗  {msg}", fg="red"))
def _warn(msg: str) -> None: click.echo(click.style(f"  ⚠  {msg}", fg="yellow"))
def _info(msg: str) -> None: click.echo(f"     {msg}")


# ── Connector registry ────────────────────────────────────────────────────────

CONNECTORS = {
    "sqlite": {
        "label":       "SQLite",
        "import_mod":  None,
        "pip_package": None,
        "pip_extra":   None,
        "notes":       "built-in — no install needed",
        "transport":   "file-based",
        "default_port": None,
    },
    "postgres": {
        "label":       "PostgreSQL",
        "import_mod":  "psycopg2",
        "pip_package": "psycopg2-binary",
        "pip_extra":   "postgres",
        "notes":       "",
        "transport":   "TCP",
        "default_port": 5432,
    },
    "mysql": {
        "label":       "MySQL / MariaDB",
        "import_mod":  "pymysql",
        "pip_package": "PyMySQL",
        "pip_extra":   "mysql",
        "notes":       "",
        "transport":   "TCP",
        "default_port": 3306,
    },
    "clickhouse": {
        "label":       "ClickHouse",
        "import_mod":  "clickhouse_connect",
        "pip_package": "clickhouse-connect",
        "pip_extra":   "clickhouse",
        "notes":       "HTTP(S) via clickhouse-connect; native TCP via clickhouse-driver",
        "transport":   "HTTP / native TCP",
        "default_port": 8123,
    },
    "oracle": {
        "label":       "Oracle Database",
        "import_mod":  "oracledb",
        "pip_package": "oracledb",
        "pip_extra":   "oracle",
        "notes":       "thin mode — no Oracle Client needed; --database = service name",
        "transport":   "TCP",
        "default_port": 1521,
    },
    "sqlserver": {
        "label":       "SQL Server / Azure SQL",
        "import_mod":  "pymssql",
        "pip_package": "pymssql",
        "pip_extra":   "sqlserver",
        "notes":       "pure-Python pymssql; also accepts --db-type mssql",
        "transport":   "TCP",
        "default_port": 1433,
    },
    "spanner": {
        "label":       "Google Cloud Spanner",
        "import_mod":  "google.cloud.spanner",
        "pip_package": "google-cloud-spanner",
        "pip_extra":   "spanner",
        "notes":       "requires Application Default Credentials (gcloud auth)",
        "transport":   "gRPC",
        "default_port": None,
    },
    "mongodb": {
        "label":       "MongoDB",
        "import_mod":  "pymongo",
        "pip_package": "pymongo",
        "pip_extra":   "mongodb",
        "notes":       "NoSQL — infers schema by sampling documents; --database = database name",
        "transport":   "TCP",
        "default_port": 27017,
    },
}

INSTALLABLE = [k for k, v in CONNECTORS.items() if v["pip_extra"] is not None]


def _is_installed(import_mod: Optional[str]) -> bool:
    if import_mod is None:
        return True
    return importlib.util.find_spec(import_mod) is not None


# ── Shared connection options (reused by extract + compare) ───────────────────

def _db_options(prefix: str = ""):
    """Return a list of Click decorators for DB connection parameters."""
    p = prefix  # e.g. "source-" or "dest-"
    def decorator(fn):
        opts = [
            click.option(f"--{p}db-type",   f"--{p}type",  default="postgres",
                         show_default=True, metavar="TYPE",
                         help="Database type: sqlite | postgres | mysql | clickhouse | oracle | sqlserver | spanner | mongodb"),
            click.option(f"--{p}host",       default="localhost",   show_default=True,
                         metavar="HOST",    help="Database server hostname or IP."),
            click.option(f"--{p}port",       default=None,  type=int,
                         metavar="PORT",    help="Server port (defaults: pg=5432, mysql=3306, ch=8123)."),
            click.option(f"--{p}database",  f"--{p}db",  default=None,
                         metavar="DB",     help="Database / schema name. Omit for server-mode scan."),
            click.option(f"--{p}user",       default=None, metavar="USER",
                         help="Login username."),
            click.option(f"--{p}password",   default=None, metavar="PASS", hide_input=True,
                         help="Login password (or set via env var / .env file)."),
            click.option(f"--{p}transport",  default=None, metavar="TRANSPORT",
                         help="ClickHouse only: http | native (default: auto)."),
            click.option(f"--{p}secure",     is_flag=True, default=False,
                         help="ClickHouse only: enable TLS (port auto-adjusts to 8443/9440)."),
            click.option(f"--{p}project-id", default=None, metavar="GCP_PROJECT",
                         help="Spanner only: GCP project ID."),
            click.option(f"--{p}instance-id",default=None, metavar="SPANNER_INSTANCE",
                         help="Spanner only: Cloud Spanner instance ID."),
            click.option(f"--{p}connection-string", default=None, metavar="URI",
                         help="MongoDB only: Full connection URI."),
        ]
        for opt in reversed(opts):
            fn = opt(fn)
        return fn
    return decorator


def _build_config(prefix: str, kwargs: dict) -> dict:
    """Pull prefixed kwargs into a clean connector config dict."""
    p = prefix.replace("-", "_")
    cfg = {}
    for field in ("db_type", "host", "port", "database", "user", "password",
                  "transport", "secure", "project_id", "instance_id"):
        key = f"{p}{field}" if p else field
        val = kwargs.get(key)
        if val is not None and val != "" and val is not False:
            out_key = "clickhouse_transport" if field == "transport" else field
            cfg[out_key] = val
    
    # Handle connection_string explicitly as it doesn't fit the standard pattern easily
    conn_str_key = f"{p}connection_string" if p else "connection_string"
    if kwargs.get(conn_str_key):
        cfg["connection_string"] = kwargs[conn_str_key]

    cfg.setdefault("db_type", "postgres")
    return cfg


# ── Root group ────────────────────────────────────────────────────────────────

MAIN_HELP = (
    "Extract database metadata, generate dbt-compatible YAML, compare schemas "
    "across environments, compile PDF reports, and deliver them by email.\n"
    "\n\b\n"
    "  Supported databases\n"
    "  ───────────────────\n"
    "    sqlite      File-based, no server — ideal for local testing\n"
    "    postgres    PostgreSQL / Amazon Aurora / AlloyDB\n"
    "    mysql       MySQL 5.7+ and MariaDB 10.3+\n"
    "    clickhouse  ClickHouse Cloud and self-hosted (HTTP + native TCP)\n"
    "    oracle      Oracle Database (thin mode — no Instant Client needed)\n"
    "    sqlserver   SQL Server / Azure SQL Database  (alias: mssql)\n"
    "    spanner     Google Cloud Spanner (Application Default Credentials)\n"
    "    mongodb     MongoDB (NoSQL — infers schema by sampling documents)\n"
    "\n\b\n"
    "  Quick start\n"
    "  ───────────\n"
    "    ddgen install postgres\n"
    "    ddgen extract --db-type postgres  --host prod.db.io --database mydb\n"
    "    ddgen extract --db-type oracle    --host ora.prod.io --database XEPDB1\n"
    "    ddgen extract --db-type sqlserver --host sql.prod.io --database MyDB\n"
    "    ddgen compare --source-host prod.db.io --dest-host staging.db.io\n"
    "    ddgen features             # full module and API reference\n"
    "    ddgen <command> --help     # options and examples for any command\n"
)


@click.group(
    help=MAIN_HELP,
    context_settings=dict(
        max_content_width=W + 4,
        help_option_names=["--help", "-h"],
    ),
    invoke_without_command=True,
)
@click.version_option(version=__version__, prog_name="ddgen", message="ddgen v%(version)s")
@click.pass_context
def main(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ── ddgen features  ──────────────────────────────────────────────────────────

@main.command("features")
def features():
    """Full module reference — classes, methods, parameters, and examples."""

    click.echo()
    click.echo(click.style("  data_dictionary_builder — Full Feature Reference", bold=True, fg="white"))
    click.echo(click.style("  " + _hr("═"), fg="cyan"))
    click.echo(f"  Version {__version__}  |  Python {sys.version.split()[0]}")
    click.echo()

    # ── MetadataExtractor ─────────────────────────────────────────────────
    _section("MODULE 1 — MetadataExtractor   (metadata/extractor.py)")
    click.echo("""
  Connects to a database and extracts schema metadata.
  Always use as a context manager (with) to ensure connections are closed.

  Constructor
  ───────────
    MetadataExtractor(db_type, **connection_params)

    db_type             str    "sqlite" | "postgres" | "mysql" | "clickhouse" | "oracle" | "sqlserver" | "spanner" | "mongodb"
    host                str    Server hostname or IP address
    port                int    Server port (auto-defaulted per db_type if omitted)
    database            str    Database name (omit for server-mode — scans all databases)
    user                str    Login username
    password            str    Login password
    clickhouse_transport str   "http" | "native" | None (auto-detect)
    secure              bool   Enable TLS — auto-adjusts port (8443 / 9440)
    project_id          str    Spanner: GCP project ID
    instance_id         str    Spanner: Cloud Spanner instance ID
    connection_string   str    MongoDB: Full connection URI (overrides other params)

  Methods
  ───────
    .test_connection()                          → bool
        Returns True if the database is reachable.

    .get_schemas_list()                         → List[str]
        Lists all schema names in the current database.

    .get_tables_list(schema_name)               → List[str]
        Lists all table names in a schema.

    .extract_schema(schema_name)                → SchemaMetadata
        Extracts full column-level metadata for all tables in one schema.

    .extract_table(schema_name, table_name)     → TableMetadata
        Extracts column-level metadata for a single table including PK/FK info.

    .extract_all_schemas(                       → DatabaseMetadata
        schema_filter=None,
        parallel_workers=5,
    )
        Extracts all schemas in parallel using ThreadPoolExecutor.
        Each worker creates its own connection — fully thread-safe.

        schema_filter   List[str] | None   See "Schema Filter Strategies" below.
        parallel_workers int               Max concurrent threads (default 5).
                                           Automatically capped at schema count.

  Schema Filter Strategies
  ────────────────────────
    Pass schema_filter=[ ... ] to extract_all_schemas().
    Strategies can be mixed freely in one list.

    Strategy          Example entry           Matches
    ──────────────    ──────────────────────  ──────────────────────────────────
    Exact name        "public"                only "public"
    SQL-LIKE glob     "stg_%"                 stg_orders, stg_customers, ...
    Glob wildcard     "raw_*"                 raw_events, raw_logs, ...
    prefix:           "prefix:stg_"           any schema starting with stg_
    suffix:           "suffix:_prod"          any schema ending with _prod
    contains:         "contains:analytics"    any schema containing analytics
    regex:            "regex:^tmp_\\d+$"      tmp_1, tmp_42, ...
    None (all)        schema_filter=None      every schema in the database

    All matching is case-insensitive.
    Results are returned in original schema order (no sorting).

  Server mode
  ───────────
    Omit 'database' from connection params to scan every database on the server.
    Supported for MySQL, ClickHouse, and PostgreSQL.
    MongoDB: all databases on the server are enumerated automatically.
    Oracle: all non-system schemas (users) are enumerated automatically.
    SQL Server: all non-system databases on the instance are enumerated.

    with MetadataExtractor(db_type="mysql", host="prod", user="ro", password="…") as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["prefix:app_"])

  Example
  ───────
    from data_dictionary_builder import MetadataExtractor

    with MetadataExtractor(db_type="postgres", host="localhost",
                           database="mydb", user="readonly", password="secret") as ext:
        ok      = ext.test_connection()
        schemas = ext.get_schemas_list()
        tables  = ext.get_tables_list("public")
        table   = ext.extract_table("public", "orders")
        db_meta = ext.extract_all_schemas(
            schema_filter=["public", "prefix:stg_"],
            parallel_workers=8,
        )
""")

    # ── Metadata Models ───────────────────────────────────────────────────
    _section("MODULE 2 — Metadata Models   (metadata/models.py)")
    click.echo("""
  Dataclass tree returned by MetadataExtractor.

  DatabaseMetadata
  ────────────────
    .database_name       str         Name of the database
    .database_type       str         Connector type used
    .version             str         Database server version string
    .host                str
    .port                int
    .schemas             List[SchemaMetadata]
    .add_schema(schema)              Append a SchemaMetadata
    .to_dict()           dict        Serialise to a plain dict (XCom / JSON safe)
    .from_dict(d)        classmethod Restore from a dict — validates round-trip

  SchemaMetadata
  ──────────────
    .name                str
    .tables              List[TableMetadata]
    .add_table(table)

  TableMetadata
  ─────────────
    .name                str
    .schema_name         str
    .table_type          str         "TABLE" | "VIEW"
    .row_count           int
    .description         str         Populated from DB COMMENT (where supported)
    .primary_keys        List[str]   Column names forming the PK
    .columns             List[ColumnMetadata]

  ColumnMetadata
  ──────────────
    .name                str
    .data_type           str
    .is_nullable         bool
    .is_primary_key      bool
    .is_foreign_key      bool
    .foreign_key_table   str | None
    .foreign_key_column  str | None
    .description         str         Populated from DB COMMENT (where supported)
    .default_value       str | None

  Serialisation (for Airflow XCom / REST APIs)
  ─────────────────────────────────────────────
    serial = db_meta.to_dict()           # → plain dict, JSON-safe
    restored = DatabaseMetadata.from_dict(serial)   # full round-trip
""")

    # ── YAMLGenerator ─────────────────────────────────────────────────────
    _section("MODULE 3 — YAMLGenerator   (yaml_generator/generator.py)")
    click.echo("""
  Generates dbt v2-compatible YAML source files from DatabaseMetadata.
  Smart merge: on re-run, existing user-written descriptions, dbt tests,
  and meta blocks are preserved; new tables/columns are appended.

  Constructor
  ───────────
    YAMLGenerator(output_dir="./output")

  Methods
  ───────
    .generate_yaml_files(db_metadata)           → List[str]
        Writes one  <schema_name>.yml  per schema.  Returns file paths.

    .generate_single_yaml(db_metadata,          → str
        filename="models.yml")
        Writes all schemas into one combined YAML file.

    .generate_schema_yaml(schema, filename=None) → str
        Generates YAML for a single SchemaMetadata object.

    .get_tables_without_descriptions(db_meta)   → List[str]
        Returns "schema.table" strings for tables missing a description.

    .get_columns_without_descriptions(db_meta)  → List[dict]
        Returns {"schema", "table", "column"} dicts for undescribed columns.

  YAML output format (dbt v2)
  ───────────────────────────
    version: 2
    models:
      - name: orders
        description: ""          ← preserved across re-runs if you fill it in
        meta:
          schema: public
          table_type: TABLE
          row_count: 84201
        columns:
          - name: order_id
            data_type: integer
            meta: {is_primary_key: true, is_nullable: false}
            tests: [unique, not_null]
          - name: customer_id
            data_type: integer
            meta: {is_foreign_key: true, foreign_key_table: customers}
            tests: [not_null]

  Example
  ───────
    from data_dictionary_builder import YAMLGenerator

    gen   = YAMLGenerator(output_dir="./models")
    files = gen.generate_yaml_files(db_meta)      # per-schema
    path  = gen.generate_single_yaml(db_meta, filename="all_models.yml")

    tables_missing = gen.get_tables_without_descriptions(db_meta)
    cols_missing   = gen.get_columns_without_descriptions(db_meta)
""")

    # ── SchemaComparator ──────────────────────────────────────────────────
    _section("MODULE 4 — SchemaComparator   (comparison/comparator.py)")
    click.echo("""
  Compares schemas between a source and a destination database.
  Detects missing tables, missing columns, and data type mismatches.
  Type comparison is normalised across different database systems
  (e.g.  "character varying" == "varchar",  "int4" == "int").

  Constructor
  ───────────
    SchemaComparator(
        source_config:      dict,   # {db_type, host, port, database, user, password, …}
        destination_config: dict,
        yaml_output_dir:    str = "./output",
    )

  Methods
  ───────
    .compare_schemas(                           → ComparisonResult
        source_schema_name,
        destination_schema_name=None,
        dest_db_metadata=None,
    )
        Core diff method.  Pass dest_db_metadata to skip a destination DB
        round-trip when metadata was already extracted earlier.

    .compare_and_generate_report(               → dict
        source_schema_name,
        destination_schema_name=None,
        include_yaml_gaps=True,
        source_db_metadata=None,   # reuse — avoids re-querying source
        dest_db_metadata=None,     # reuse — avoids re-querying destination
    )
        Full report dict with keys:
          comparison.missing_tables     list of {schema, table, column_count}
          comparison.missing_columns    list of {schema, table, column, data_type}
          comparison.type_mismatches    list of {schema, table, column, source_type, dest_type}
          summary.missing_tables_count
          summary.missing_columns_count
          summary.type_mismatches_count
          summary.tables_without_descriptions_count
          summary.columns_without_descriptions_count
          yaml_gaps.tables_without_descriptions
          yaml_gaps.columns_without_descriptions

  Type normalisation map (built-in)
  ──────────────────────────────────
    "integer" / "int4"              → int
    "int8"                          → bigint
    "character varying"             → varchar
    "timestamp without time zone"   → timestamp
    "timestamp with time zone"      → timestamptz
    "double precision"              → double

  Example
  ───────
    from data_dictionary_builder import SchemaComparator

    comparator = SchemaComparator(
        source_config={"db_type": "postgres", "host": "prod", "database": "app"},
        destination_config={"db_type": "postgres", "host": "staging", "database": "app"},
        yaml_output_dir="./models",
    )
    report = comparator.compare_and_generate_report(
        source_schema_name="public",
        include_yaml_gaps=True,
        source_db_metadata=already_extracted_db_meta,   # skip re-query
    )
    print(report["summary"])
""")

    # ── DDHelper ──────────────────────────────────────────────────────────
    _section("MODULE 5 — DDHelper   (DDHelper.py)")
    click.echo("""
  High-level convenience class that manages the standard output layout,
  persists comparison reports as JSON, compiles PDFs, and sends emails.

  Directory layout created automatically
  ───────────────────────────────────────
    <base_dir>/
        models/               dbt YAML files   (<schema>.yml, all_models.yml)
        reports/
            json/             Metadata_comparison_YYYY-MM-DD_HH-MM-SS.json
            pdf/              Metadata_comparison_YYYY-MM-DD_HH-MM-SS.pdf

  Constructor
  ───────────
    DDHelper(base_dir=".")

  Properties
  ──────────
    .base_dir             Path   Root directory
    .dirs                 dict   {"models", "reports", "reports_json", "reports_pdf"}
    .models_dir           Path
    .reports_json_dir     Path
    .reports_pdf_dir      Path

  Methods
  ───────
    .save_report(report, dt=None)               → Path
        Serialise a comparison report dict to a timestamped JSON file.

    .compile_pdf(source_json=None,              → Path | None
                 output_pdf=None)
        Compile one JSON file (or all in reports/json/) into a formatted PDF.
        Requires  pip install reportlab.
        Returns None gracefully when reportlab is not installed.

    .send_report_email(                         → bool
        report,
        pdf_path=None,
        subject=None,
        smtp_host=None, smtp_port=587,
        smtp_user=None, smtp_password=None,
        email_to=None,
        use_tls=True,
    )
        Email the report with the PDF attached.
        All SMTP params fall back to env vars:
            SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
        Returns False gracefully when SMTP is not configured.

  Example
  ───────
    from data_dictionary_builder import DDHelper

    helper    = DDHelper("/opt/airflow/reports")
    json_path = helper.save_report(report)
    pdf_path  = helper.compile_pdf(source_json=json_path)
    helper.send_report_email(
        report=report,
        pdf_path=pdf_path,
        subject="Nightly drift report — production",
        email_to="data-team@company.com",
    )
""")

    # ── ExecutionTimer ─────────────────────────────────────────────────────
    _section("MODULE 6 — ExecutionTimer   (timer.py)")
    click.echo("""
  Lightweight wall-clock timer for measuring and reporting task durations.

  Constructor
  ───────────
    ExecutionTimer()     ← clock starts immediately

  Methods
  ───────
    .task(name)          context manager — measures a named block
    .elapsed             float — seconds since construction
    .totals()            → (List[Tuple[str, float]], float)
    .summary(title="Execution Summary")   prints a formatted table

  Example
  ───────
    from data_dictionary_builder import ExecutionTimer

    timer = ExecutionTimer()

    with timer.task("Extract metadata"):
        db_meta = ext.extract_all_schemas(parallel_workers=8)

    with timer.task("Generate YAML"):
        gen.generate_yaml_files(db_meta)

    timer.summary("Nightly pipeline")
    # ─────────────────────────────────────
    #   Nightly pipeline
    # ─────────────────────────────────────
    #   Extract metadata        4.231s
    #   Generate YAML           0.094s
    # ─────────────────────────────────────
    #   TOTAL                   4.325s
    # ─────────────────────────────────────
""")

    # ── Connectors ────────────────────────────────────────────────────────
    _section("MODULE 7 — Connectors   (connectors/)")
    click.echo("""
  Database-specific driver implementations behind a factory function.
  You should not need to use connectors directly — use MetadataExtractor.

  Factory
  ───────
    from data_dictionary_builder.connectors import get_connector
    connector = get_connector(db_type, **connection_params)

  All connectors extend BaseConnector (connectors/base.py) and implement:
    .connect()                           open connection
    .disconnect()                        close connection
    .test_connection()       → bool
    .get_schemas()           → List[str]
    .get_tables(schema)      → List[str]
    .get_table_metadata(schema, table)   → TableMetadata
    .extract_schema_metadata(schema)     → SchemaMetadata
    .get_database_version()  → str

  Connector notes
  ───────────────
    Connector      Driver               Default port   Install
    ─────────────  ───────────────────  ─────────────  ───────────────────────
    sqlite         built-in sqlite3     —              (none needed)
    postgres       psycopg2-binary      5432           ddgen install postgres
    mysql          PyMySQL              3306           ddgen install mysql
    clickhouse     clickhouse-connect   8123 / 8443*   ddgen install clickhouse
                   clickhouse-driver    9000 / 9440*
    oracle         oracledb             1521           ddgen install oracle
    sqlserver      pymssql              1433           ddgen install sqlserver
    spanner        google-cloud-spanner —              ddgen install spanner
    mongodb        pymongo              27017          ddgen install mongodb

    * With secure=True the port auto-adjusts: HTTP → 8443, native TCP → 9440.
    Oracle uses thin mode — no Oracle Instant Client installation required.
    SQL Server also accepts --db-type mssql as an alias.

  ClickHouse transport selection
  ──────────────────────────────
    transport       Port     When to use
    ─────────────   ──────   ──────────────────────────────────────────────────
    http (default)  8123     Standard self-hosted, ClickHouse Cloud (use 8443)
    http + secure   8443     ClickHouse Cloud / any TLS HTTP endpoint
    native          9000     High-throughput self-hosted
    native + secure 9440     Self-hosted with TLS or Altinity Cloud
""")

    # ── EmailSender ───────────────────────────────────────────────────────
    _section("MODULE 8 — EmailSender   (notifications/email_sender.py)")
    click.echo("""
  Low-level SMTP email sender (used internally by DDHelper.send_report_email).
  Prefer DDHelper for normal use.

  Constructor
  ───────────
    EmailSender(
        smtp_host,
        smtp_port=587,
        sender_email="",
        sender_password=None,
        use_tls=True,
    )

  Methods
  ───────
    .send_comparison_report(
        recipient_emails: List[str],
        report:           dict,
        subject:          str = "Database Schema Comparison Report",
        attachments:      List[str] = [],
    ) → bool

  Environment variables (read automatically when params are omitted)
  ──────────────────────────────────────────────────────────────────
    SMTP_HOST       SMTP_PORT       SMTP_USER       SMTP_PASSWORD       EMAIL_TO
""")

    # ── Airflow integration ───────────────────────────────────────────────
    _section("INTEGRATION — Apache Airflow")
    click.echo("""
  The library ships two Airflow-ready files in  airflow/ :

    airflow/include/dd_builder_tasks.py
        13 importable task functions (one per feature).
        Reads credentials from Airflow Connections, env vars, or explicit params.
        All functions accept **context and work with XCom out of the box.

    airflow/dags/db_metadata_dag.py
        Full production DAG using PythonOperator + op_kwargs.
        All behaviour configurable via Airflow Variables — no code edits needed.

  Airflow Variables (set in Admin → Variables)
  ────────────────────────────────────────────
    dd_source_conn_id       Airflow connection ID for source DB
    dd_dest_conn_id         Airflow connection ID for destination DB
    dd_smtp_conn_id         Airflow connection ID for SMTP
    dd_schema_filter        Comma-separated schema filter (e.g. "public,prefix:stg_")
    dd_target_schema        Schema for single-schema / table tasks (default: public)
    dd_yaml_output_dir      dbt YAML output directory
    dd_report_base_dir      DDHelper base directory (JSON + PDF output)
    dd_alert_email          Report recipient email address
    dd_parallel_workers     Extraction thread count (default: 8)

  Quick DAG setup
  ───────────────
    1. Copy airflow/include/dd_builder_tasks.py  → $AIRFLOW_HOME/include/
    2. Copy airflow/dags/db_metadata_dag.py      → $AIRFLOW_HOME/dags/
    3. Set Airflow Variables (above) in the UI
    4. Create Airflow Connections for source/dest/smtp
    5. Trigger the DAG
""")

    # ── Environment variables ─────────────────────────────────────────────
    _section("ENVIRONMENT VARIABLES  (.env file or shell export)")
    click.echo("""
  Variable               Description
  ─────────────────────  ───────────────────────────────────────────────────
  SOURCE_DB_TYPE         postgres | mysql | clickhouse | spanner | sqlite
  SOURCE_HOST            source database hostname
  SOURCE_PORT            source port
  SOURCE_DATABASE        source database name
  SOURCE_USER            source login user
  SOURCE_PASSWORD        source password
  SOURCE_TRANSPORT       clickhouse: http | native
  SOURCE_SECURE          clickhouse: true | false

  DEST_DB_TYPE           same as SOURCE_* but for the destination database
  DEST_HOST
  DEST_PORT
  DEST_DATABASE
  DEST_USER
  DEST_PASSWORD

  SMTP_HOST              SMTP server hostname
  SMTP_PORT              SMTP port (default 587)
  SMTP_USER              SMTP login
  SMTP_PASSWORD          SMTP password
  EMAIL_TO               report recipient address

  Loaded automatically from a .env file in the working directory.
""")

    # ── Installation ──────────────────────────────────────────────────────
    _section("INSTALLATION")
    click.echo("""
  # Full install (all connectors + PDF + email)
  pip install data-dictionary-builder

  # Minimal install + selective connectors
  pip install data-dictionary-builder
  ddgen install postgres
  ddgen install clickhouse
  ddgen install oracle
  ddgen install sqlserver
  ddgen install all       ← installs all optional drivers

  # Or install extras directly with pip
  pip install "data-dictionary-builder[oracle]"
  pip install "data-dictionary-builder[sqlserver]"
  pip install "data-dictionary-builder[all]"

  # Editable install from source
  git clone https://github.com/GraFreak0/data_dictionary_builder
  cd data_dictionary_builder
  pip install -e ".[dev]"
""")

    click.echo(click.style("  " + _hr("═"), fg="cyan"))
    click.echo(f"  Run  ddgen <command> --help  for per-command flags.")
    click.echo(f"  Docs: https://github.com/GraFreak0/data_dictionary_builder")
    click.echo()


# ── ddgen extract ─────────────────────────────────────────────────────────────

@main.command("extract")
@click.option("--db-type", "--type", "-t", default="postgres", show_default=True,
              type=click.Choice(
                  ["sqlite", "postgres", "mysql", "clickhouse", "oracle", "sqlserver", "mssql", "spanner", "mongodb"],
                  case_sensitive=False,
              ),
              help="Database type to connect to.")
@click.option("--host",         "-H", default="localhost",  show_default=True, metavar="HOST",
              help="Database server hostname or IP.")
@click.option("--port",         "-p", default=None, type=int, metavar="PORT",
              help="Server port (auto-defaulted per db-type if omitted).")
@click.option("--database",     "-d", default=None, metavar="DB",
              help="Database name. Omit to enable server-mode (scan all databases).")
@click.option("--user",         "-u", default=None, metavar="USER", help="Login username.")
@click.option("--password",     "-P", default=None, metavar="PASS", hide_input=False,
              help="Login password (or use SOURCE_PASSWORD env var).")
@click.option("--service",      default=None, metavar="SERVICE",
              help="Oracle only: service name (e.g. XEPDB1, ORCL). Alias for --database.")
@click.option("--schema-filter","-s", default=None, multiple=True, metavar="FILTER",
              help=(
                  "Schema filter — repeat for multiple entries. "
                  'Examples: -s public  -s "prefix:stg_"  -s "regex:^raw_\\d+$"'
              ))
@click.option("--parallel",     "-w", default=8, show_default=True, metavar="N",
              help="Number of parallel extraction threads.")
@click.option("--output-dir",   "-o", default="./models", show_default=True, metavar="DIR",
              help="Directory to write dbt YAML files.")
@click.option("--format", "fmt", default="yaml",
              type=click.Choice(["yaml", "json", "both"], case_sensitive=False),
              show_default=True,
              help="Output format: yaml (per-schema), json (metadata file), or both.")
@click.option("--combined",     is_flag=True, default=False,
              help="Also write a single all_models.yml containing all schemas.")
@click.option("--transport",    default=None, metavar="TRANSPORT",
              type=click.Choice(["http", "native"], case_sensitive=False),
              help="ClickHouse transport: http or native (default: auto-detect).")
@click.option("--secure",       is_flag=True, default=False,
              help="ClickHouse: enable TLS (port auto-adjusts to 8443/9440).")
@click.option("--project-id",   default=None, metavar="GCP_PROJECT",
              help="Spanner: GCP project ID.")
@click.option("--instance-id",  default=None, metavar="SPANNER_INSTANCE",
              help="Spanner: Cloud Spanner instance ID.")
@click.option("--report-dir",   default=None, metavar="DIR",
              help="Base directory for DDHelper (JSON report output). Defaults to output-dir parent.")
def extract(db_type, host, port, database, user, password, service, schema_filter,
            parallel, output_dir, fmt, combined, transport, secure,
            project_id, instance_id, report_dir):
    """
    Extract database metadata and write dbt YAML files.

    \b
    Reads credentials from:  explicit options → env vars (SOURCE_*) → .env file

    \b
    Examples:
      ddgen extract --db-type postgres   --host prod.db.io --database mydb --user ro
      ddgen extract --db-type sqlite     --database ./local.db
      ddgen extract --db-type mysql      --host 127.0.0.1 --database app -s "prefix:stg_"
      ddgen extract --db-type clickhouse --host my.cloud.clickhouse.com \\
                    --transport http --secure --schema-filter default
      ddgen extract --db-type oracle     --host ora.prod.io --database XEPDB1 \\
                    --user hr --schema-filter HR --schema-filter OE
      ddgen extract --db-type sqlserver  --host sql.prod.io --database AdventureWorks \\
                    --user sa --schema-filter dbo --schema-filter sales
      ddgen extract --db-type postgres --host prod.db.io --database mydb \\
                    --schema-filter public --schema-filter "prefix:stg_" \\
                    --parallel 16 --output-dir ./dbt/models --combined
    """
    from . import MetadataExtractor, DatabaseMetadata, YAMLGenerator, DDHelper, ExecutionTimer

    # ── Build config ──────────────────────────────────────────────────────
    cfg: dict = {"db_type": db_type.lower()}
    if host:        cfg["host"]     = host
    if port:        cfg["port"]     = port
    # Oracle: --service is an alias for --database
    effective_database = database or (service if db_type.lower() == "oracle" else None)
    if effective_database: cfg["database"] = effective_database
    if user:        cfg["user"]     = user or os.getenv("SOURCE_USER")
    if password:    cfg["password"] = password or os.getenv("SOURCE_PASSWORD")
    if transport:   cfg["clickhouse_transport"] = transport
    if secure:      cfg["secure"]   = True
    if project_id:  cfg["project_id"]  = project_id
    if instance_id: cfg["instance_id"] = instance_id

    # Env-var fallbacks for user/password
    cfg["user"]     = cfg.get("user")     or os.getenv("SOURCE_USER")
    cfg["password"] = cfg.get("password") or os.getenv("SOURCE_PASSWORD")

    _sf = list(schema_filter) if schema_filter else None

    click.echo()
    click.echo(click.style("  Extracting metadata…", fg="cyan", bold=True))
    _info(f"db_type  : {db_type}")
    _info(f"host     : {host}:{port or 'default'}")
    _info(f"database : {database or '(server mode — all databases)'}")
    _info(f"filter   : {_sf or 'all schemas'}")
    _info(f"workers  : {parallel}")
    click.echo()

    timer = ExecutionTimer()

    try:
        with timer.task("Connection test"):
            ext = MetadataExtractor(**cfg)
            if not ext.test_connection():
                _err("Connection failed. Check host, credentials, and network.")
                sys.exit(1)
        _ok("Connection established.")

        with timer.task("Metadata extraction"):
            with MetadataExtractor(**cfg) as ext:
                db_meta = ext.extract_all_schemas(
                    schema_filter=_sf,
                    parallel_workers=parallel,
                )

        schema_count = len(db_meta.schemas)
        table_count  = sum(len(s.tables) for s in db_meta.schemas)
        col_count    = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
        _ok(f"Extracted {schema_count} schema(s), {table_count} table(s), {col_count} column(s).")
        click.echo()

        # ── YAML output ───────────────────────────────────────────────────
        if fmt in ("yaml", "both"):
            with timer.task("YAML generation"):
                gen   = YAMLGenerator(output_dir=output_dir)
                files = gen.generate_yaml_files(db_meta)

            for f in files:
                _ok(f"YAML  → {f}  ({os.path.getsize(f):,} bytes)")

            if combined:
                with timer.task("Combined YAML"):
                    path = gen.generate_single_yaml(db_meta, filename="all_models.yml")
                _ok(f"Combined → {path}  ({os.path.getsize(path):,} bytes)")

        # ── JSON metadata export ───────────────────────────────────────────
        if fmt in ("json", "both"):
            with timer.task("JSON metadata export"):
                base     = report_dir or str(Path(output_dir).parent)
                helper   = DDHelper(base)
                out_path = helper.reports_json_dir / f"{db_meta.database_name}_metadata.json"
                out_path.write_text(
                    json.dumps(db_meta.to_dict(), indent=2, default=str),
                    encoding="utf-8",
                )
            _ok(f"JSON  → {out_path}  ({os.path.getsize(out_path):,} bytes)")

        click.echo()

        # ── Doc gaps ──────────────────────────────────────────────────────
        gen = YAMLGenerator(output_dir=output_dir)
        t_missing = gen.get_tables_without_descriptions(db_meta)
        c_missing = gen.get_columns_without_descriptions(db_meta)
        total_t   = sum(len(s.tables) for s in db_meta.schemas)
        total_c   = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
        t_pct     = round(100 * (total_t - len(t_missing)) / max(total_t, 1), 1)
        c_pct     = round(100 * (total_c - len(c_missing)) / max(total_c, 1), 1)

        _section("Documentation coverage")
        click.echo(f"     Tables  : {t_pct}%  ({len(t_missing)}/{total_t} missing descriptions)")
        click.echo(f"     Columns : {c_pct}%  ({len(c_missing)}/{total_c} missing descriptions)")
        click.echo()

    except Exception as exc:
        _err(str(exc))
        sys.exit(1)

    timer.summary("Extract")


# ── ddgen compare ─────────────────────────────────────────────────────────────

@main.command("compare")
@click.option("--source-db-type",  default="postgres", show_default=True,
              type=click.Choice(
                  ["sqlite","postgres","mysql","clickhouse","oracle","sqlserver","mssql","spanner", "mongodb"],
                  case_sensitive=False,
              ),
              help="Source database type.")
@click.option("--source-host",     default="localhost", show_default=True, metavar="HOST")
@click.option("--source-port",     default=None, type=int, metavar="PORT")
@click.option("--source-database", "--source-db", default=None, metavar="DB")
@click.option("--source-service",  default=None, metavar="SERVICE",
              help="Oracle source: service name (alias for --source-database).")
@click.option("--source-user",     default=None, metavar="USER")
@click.option("--source-password", default=None, metavar="PASS")
@click.option("--source-transport",default=None, type=click.Choice(["http","native"]),
              help="ClickHouse source transport.")
@click.option("--source-secure",   is_flag=True, default=False)
@click.option("--dest-db-type",    default="postgres", show_default=True,
              type=click.Choice(
                  ["sqlite","postgres","mysql","clickhouse","oracle","sqlserver","mssql","spanner", "mongodb"],
                  case_sensitive=False,
              ),
              help="Destination database type.")
@click.option("--dest-host",       default="localhost", show_default=True, metavar="HOST")
@click.option("--dest-port",       default=None, type=int, metavar="PORT")
@click.option("--dest-database",   "--dest-db", default=None, metavar="DB")
@click.option("--dest-service",    default=None, metavar="SERVICE",
              help="Oracle destination: service name (alias for --dest-database).")
@click.option("--dest-user",       default=None, metavar="USER")
@click.option("--dest-password",   default=None, metavar="PASS")
@click.option("--dest-transport",  default=None, type=click.Choice(["http","native"]),
              help="ClickHouse destination transport.")
@click.option("--dest-secure",     is_flag=True, default=False)
@click.option("--schema",          "-s", multiple=True, metavar="SCHEMA",
              help="Schema(s) to compare. Repeat for multiple. Supports all filter strategies.")
@click.option("--output-dir",      "-o", default="./models", show_default=True, metavar="DIR",
              help="dbt YAML directory (used for gap detection).")
@click.option("--report-dir",      "-r", default="./reports", show_default=True, metavar="DIR",
              help="DDHelper base directory — JSON and PDF reports are written here.")
@click.option("--pdf",             is_flag=True, default=False,
              help="Compile the report JSON into a formatted PDF.")
@click.option("--email-to",        default=None, metavar="EMAIL",
              help="Send the report PDF to this address (requires SMTP_* env vars).")
@click.option("--parallel",        "-w", default=8, show_default=True, metavar="N",
              help="Extraction threads for destination snapshot.")
def compare(source_db_type, source_host, source_port, source_database, source_service,
            source_user, source_password, source_transport, source_secure,
            dest_db_type, dest_host, dest_port, dest_database, dest_service,
            dest_user, dest_password, dest_transport, dest_secure,
            schema, output_dir, report_dir, pdf, email_to, parallel):
    """
    Compare schemas between a source and a destination database.

    \b
    Detects: missing tables, missing columns, data type mismatches,
             and documentation gaps (tables/columns without descriptions).

    \b
    Examples:
      ddgen compare \\
          --source-host prod.db.io   --source-database app \\
          --dest-host   stage.db.io  --dest-database   app \\
          --schema public --schema analytics --pdf

      ddgen compare \\
          --source-db-type clickhouse --source-host ch.cloud.io --source-secure \\
          --dest-db-type   postgres   --dest-host   pg.prod.io  \\
          --schema default --report-dir ./reports --pdf --email-to team@co.com

      ddgen compare \\
          --source-db-type oracle --source-host ora.prod.io --source-service ORCL \\
          --dest-db-type   oracle --dest-host   ora.stage.io --dest-service  ORCL \\
          --schema HR --schema OE

      ddgen compare \\
          --source-db-type sqlserver --source-host sql.prod.io --source-database MyDB \\
          --dest-db-type   sqlserver --dest-host   sql.stage.io --dest-database MyDB \\
          --schema dbo --schema sales --pdf
    """
    from . import MetadataExtractor, SchemaComparator, DDHelper, ExecutionTimer

    def _cfg(db_type, host, port, database, service, user, password, transport, secure, prefix):
        c = {"db_type": db_type.lower()}
        if host:        c["host"]     = host
        if port:        c["port"]     = port
        # Oracle: --service is an alias for --database
        effective_db = database or (service if db_type.lower() == "oracle" else None)
        if effective_db: c["database"] = effective_db
        c["user"]     = user     or os.getenv(f"{prefix}_USER")
        c["password"] = password or os.getenv(f"{prefix}_PASSWORD")
        if transport:   c["clickhouse_transport"] = transport
        if secure:      c["secure"] = True
        return c

    src_cfg  = _cfg(source_db_type, source_host, source_port, source_database, source_service,
                    source_user, source_password, source_transport, source_secure, "SOURCE")
    dest_cfg = _cfg(dest_db_type, dest_host, dest_port, dest_database, dest_service,
                    dest_user, dest_password, dest_transport, dest_secure, "DEST")

    schema_names = list(schema) if schema else None

    click.echo()
    click.echo(click.style("  Comparing schemas…", fg="cyan", bold=True))
    _info(f"source : {source_db_type} @ {source_host} / {source_database or 'all'}")
    _info(f"dest   : {dest_db_type} @ {dest_host} / {dest_database or 'all'}")
    _info(f"schemas: {schema_names or 'all'}")
    click.echo()

    timer = ExecutionTimer()

    try:
        # ── Test connections ───────────────────────────────────────────────
        with timer.task("Connection tests"):
            for label, cfg in [("SOURCE", src_cfg), ("DEST", dest_cfg)]:
                ok = MetadataExtractor(**cfg).test_connection()
                if not ok:
                    _err(f"{label} connection failed.")
                    sys.exit(1)
                _ok(f"{label} connection OK.")

        # ── Extract source metadata ────────────────────────────────────────
        with timer.task("Source extraction"):
            with MetadataExtractor(**src_cfg) as ext:
                if not schema_names:
                    schema_names = ext.get_schemas_list()
                source_db_meta = ext.extract_all_schemas(
                    schema_filter=schema_names, parallel_workers=parallel
                )
        _ok(f"Source: {len(source_db_meta.schemas)} schema(s), "
            f"{sum(len(s.tables) for s in source_db_meta.schemas)} table(s).")

        # ── Extract destination metadata once ──────────────────────────────
        with timer.task("Destination extraction"):
            with MetadataExtractor(**dest_cfg) as ext:
                dest_db_meta = ext.extract_all_schemas(
                    schema_filter=schema_names, parallel_workers=parallel
                )
        _ok(f"Dest  : {len(dest_db_meta.schemas)} schema(s), "
            f"{sum(len(s.tables) for s in dest_db_meta.schemas)} table(s).")

        # ── Compare each schema ────────────────────────────────────────────
        comparator = SchemaComparator(
            source_config=src_cfg,
            destination_config=dest_cfg,
            yaml_output_dir=output_dir,
        )

        combined: dict = {
            "summary":    {k: 0 for k in (
                "missing_tables_count", "missing_columns_count",
                "type_mismatches_count", "tables_without_descriptions_count",
                "columns_without_descriptions_count",
            )},
            "comparison": {"missing_tables": [], "missing_columns": [], "type_mismatches": []},
            "yaml_gaps":  {"tables_without_descriptions": [], "columns_without_descriptions": []},
            "schemas_compared": [],
        }

        with timer.task("Schema comparison"):
            for sn in schema_names:
                report = comparator.compare_and_generate_report(
                    source_schema_name=sn,
                    include_yaml_gaps=True,
                    source_db_metadata=source_db_meta,
                    dest_db_metadata=dest_db_meta,
                )
                for k in combined["summary"]:
                    combined["summary"][k] += report["summary"].get(k, 0)
                for k in ("missing_tables", "missing_columns", "type_mismatches"):
                    combined["comparison"][k].extend(report["comparison"].get(k, []))
                for k in ("tables_without_descriptions", "columns_without_descriptions"):
                    combined["yaml_gaps"][k].extend(report.get("yaml_gaps", {}).get(k, []))
                combined["schemas_compared"].append(sn)

        # ── Print summary ──────────────────────────────────────────────────
        s = combined["summary"]
        click.echo()
        _section("Comparison Results")
        status_color = "red" if (s["missing_tables_count"] or s["missing_columns_count"]) else "green"
        click.echo(click.style(
            f"     Missing tables  : {s['missing_tables_count']}", fg=status_color
        ))
        click.echo(click.style(
            f"     Missing columns : {s['missing_columns_count']}", fg=status_color
        ))
        click.echo(click.style(
            f"     Type mismatches : {s['type_mismatches_count']}",
            fg="yellow" if s["type_mismatches_count"] else "green",
        ))
        click.echo(f"     Tables  missing descriptions : {s['tables_without_descriptions_count']}")
        click.echo(f"     Columns missing descriptions : {s['columns_without_descriptions_count']}")
        click.echo()

        # ── Save JSON report ───────────────────────────────────────────────
        helper    = DDHelper(report_dir)
        json_path = helper.save_report(combined)
        _ok(f"Report JSON → {json_path}")

        # ── Compile PDF ────────────────────────────────────────────────────
        if pdf:
            with timer.task("PDF compilation"):
                pdf_path = helper.compile_pdf(source_json=json_path)
            if pdf_path:
                _ok(f"Report PDF  → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
            else:
                _warn("PDF skipped — install reportlab:  pip install reportlab")
        else:
            pdf_path = None

        # ── Email ──────────────────────────────────────────────────────────
        if email_to:
            with timer.task("Email delivery"):
                sent = helper.send_report_email(
                    report=combined,
                    pdf_path=pdf_path,
                    email_to=email_to,
                )
            if sent:
                _ok(f"Report emailed to {email_to}")
            else:
                _warn("Email skipped — set SMTP_HOST, SMTP_USER, SMTP_PASSWORD env vars.")

        click.echo()

    except Exception as exc:
        _err(str(exc))
        sys.exit(1)

    timer.summary("Compare")


# ── ddgen connectors ──────────────────────────────────────────────────────────

@main.command("connectors")
def connectors_status():
    """Show which database connectors are installed and how to install missing ones."""
    col_name   = 26
    col_status = 16

    click.echo()
    click.echo(f"  {'Connector':<{col_name}} {'Status':<{col_status}} {'Port':<8}  Notes / Install command")
    click.echo("  " + _hr("─", W - 2))

    for key, info in CONNECTORS.items():
        installed = _is_installed(info["import_mod"])
        port_str  = str(info["default_port"]) if info["default_port"] else "—"

        if installed:
            if info["pip_extra"] is None:
                status_str = click.style("✓ built-in ", fg="cyan")
                detail     = info["notes"]
            else:
                status_str = click.style("✓ installed", fg="green")
                detail     = info["notes"] or ""
        else:
            status_str = click.style("✗ missing  ", fg="red")
            detail     = f"ddgen install {key}"

        click.echo(f"  {info['label']:<{col_name}} {status_str}  {port_str:<8}  {detail}")

    click.echo()
    installed_count = sum(1 for i in CONNECTORS.values() if _is_installed(i["import_mod"]))
    click.echo(f"  {installed_count}/{len(CONNECTORS)} connectors available.")
    click.echo()
    click.echo("  To install missing connectors:")
    click.echo("    ddgen install <name>   — install one connector")
    click.echo("    ddgen install all      — install all optional connectors")
    click.echo()


# ── ddgen install ─────────────────────────────────────────────────────────────

@main.command("install")
@click.argument(
    "connector",
    type=click.Choice(INSTALLABLE + ["all"], case_sensitive=False),
)
def install_connector(connector: str):
    """
    Install the driver package for a database connector.

    \b
    CONNECTOR is one of: postgres, mysql, clickhouse, oracle, sqlserver, spanner, all

    \b
    Examples:
        ddgen install postgres
        ddgen install clickhouse
        ddgen install oracle
        ddgen install sqlserver
        ddgen install all
    """
    targets = INSTALLABLE if connector == "all" else [connector.lower()]

    to_install    = []
    already_have  = []

    for key in targets:
        info = CONNECTORS[key]
        if _is_installed(info["import_mod"]):
            already_have.append(info["label"])
        else:
            to_install.append(info["pip_package"])

    click.echo()
    for label in already_have:
        _ok(f"{label} is already installed.")

    if not to_install:
        click.echo("  Nothing to install.\n")
        return

    click.echo(f"  Installing: {', '.join(to_install)}\n")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install"] + to_install,
        check=False,
    )
    click.echo()

    if result.returncode == 0:
        for pkg in to_install:
            _ok(f"{pkg} installed successfully.")
        click.echo()
        click.echo("  Run  ddgen connectors  to verify.\n")
    else:
        _err("Installation failed. Check the pip output above.")
        sys.exit(result.returncode)


# ── ddgen info ────────────────────────────────────────────────────────────────

@main.command("info")
def info():
    """Show library version, Python info, and installed connector status."""
    click.echo()
    click.echo(click.style(f"  data_dictionary_builder  v{__version__}", bold=True))
    click.echo(f"  Python {sys.version.split()[0]}  |  {sys.executable}")
    click.echo()
    click.echo("  Connectors:")

    for key, cinfo in CONNECTORS.items():
        installed = _is_installed(cinfo["import_mod"])
        marker    = click.style("✓", fg="green") if installed else click.style("✗", fg="red")
        port      = f"  (port {cinfo['default_port']})" if cinfo["default_port"] else ""
        click.echo(f"    {marker}  {cinfo['label']:<26}{port}")

    click.echo()
    click.echo("  Commands:  extract | compare | connectors | install | features | info")
    click.echo("  Help:      ddgen --help | ddgen features | ddgen <command> --help")
    click.echo()
