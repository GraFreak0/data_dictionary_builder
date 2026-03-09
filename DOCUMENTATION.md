# data_dictionary_builder — User Guide

## Table of Contents

1. [Installation](#1-installation)
2. [Quick Start](#2-quick-start)
3. [Connecting to Your Database](#3-connecting-to-your-database)
4. [Extracting Metadata](#4-extracting-metadata)
5. [Filtering Schemas](#5-filtering-schemas)
6. [Parallel Extraction](#6-parallel-extraction)
7. [Generating dbt YAML](#7-generating-dbt-yaml)
8. [Comparing Schemas](#8-comparing-schemas)
9. [Reports: JSON, PDF, and Email](#9-reports-json-pdf-and-email)
10. [Timing Your Pipeline](#10-timing-your-pipeline)
11. [Airflow Integration](#11-airflow-integration)
12. [CLI Reference](#12-cli-reference)
13. [API Reference](#13-api-reference)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Installation

```bash
# Full install — all connectors included by default
pip install data-dictionary-builder

# Install from source (also installs all connectors)
git clone https://github.com/GraFreak0/data_dictionary_builder.git
cd data_dictionary_builder
pip install -e .
```

For a **minimal install**, use `--no-deps` then add only what you need:

```bash
pip install psycopg2-binary          # PostgreSQL
pip install PyMySQL                  # MySQL / MariaDB
pip install clickhouse-connect        # ClickHouse
pip install google-cloud-spanner     # Google Cloud Spanner
pip install reportlab                # PDF report generation
```

---

## 2. Quick Start

The fastest path from zero to documented database:

```python
from data_dictionary_builder import (
    MetadataExtractor,
    YAMLGenerator,
    DDHelper,
    ExecutionTimer,
)

timer  = ExecutionTimer()
helper = DDHelper(".")          # creates models/, reports/json/, reports/pdf/

with timer.task("Extract"):
    with MetadataExtractor(
        db_type="postgres",
        host="localhost",
        port=5432,
        database="mydb",
        user="readonly",
        password="secret",
    ) as ext:
        db_meta = ext.extract_all_schemas(
            schema_filter=["public", "analytics"],
            parallel_workers=8,
        )

with timer.task("Generate YAML"):
    gen = YAMLGenerator(output_dir=str(helper.models_dir))
    gen.generate_yaml_files(db_meta)

timer.summary()
# ────────────────────────────────────
#   Extract              2.341s
#   Generate YAML        0.087s
# ────────────────────────────────────
#   TOTAL                2.428s
# ────────────────────────────────────
```

Your schemas are now documented in dbt-compatible YAML files under `./models/`.

---

## 3. Connecting to Your Database

All connections go through `MetadataExtractor`. Always use it as a context manager — it opens and closes the connection automatically.

### SQLite

```python
with MetadataExtractor(db_type="sqlite", database="/path/to/file.db") as ext:
    ...
```

No additional packages needed. Use `:memory:` for in-memory databases.

### PostgreSQL

```python
with MetadataExtractor(
    db_type="postgres",
    host="localhost",
    port=5432,
    database="mydb",
    user="readonly",
    password="secret",
) as ext:
    ...
```

Requires `psycopg2-binary`. Row counts use `pg_class` estimates for speed.

### MySQL / MariaDB

```python
with MetadataExtractor(
    db_type="mysql",
    host="localhost",
    port=3306,
    database="mydb",
    user="root",
    password="secret",
) as ext:
    ...
```

Requires `PyMySQL`. Omit `database` to scan all databases on the server.

### ClickHouse

Two transports are supported. Pass `transport` explicitly or omit it to auto-detect (HTTP preferred when both drivers are installed).

**Port defaults** — if you don't pass `port`, the connector picks automatically:

| Transport | `secure=True` | `secure` not set |
|---|---|---|
| HTTP (`clickhouse-connect`) | **8443** | 8123 |
| Native TCP (`clickhouse-driver`) | **9440** | 9000 |

**HTTP / HTTPS — `clickhouse-connect` (recommended, default)**

```python
with MetadataExtractor(
    db_type="clickhouse",
    host="my-cluster.clickhouse.cloud",
    # port omitted — defaults to 8443 when secure=True
    database="default",
    user="default",
    password="secret",
    secure=True,
) as ext:
    ...
```

**Native TCP — `clickhouse-driver` (optional)**

```python
with MetadataExtractor(
    db_type="clickhouse",
    host="self-hosted.internal",
    # port omitted — defaults to 9440 when secure=True
    database="default",
    user="default",
    password="secret",
    transport="native",     # requires: pip install clickhouse-driver
    secure=True,
) as ext:
    ...
```

`transport` accepts `"http"`, `"native"`, or `None` (auto-detect). Auto-detect prefers HTTP when `clickhouse-connect` is installed. Install HTTP support with `ddgen install clickhouse`. Metadata is read from `system.columns` using a 2-query bulk approach.

### Google Cloud Spanner

```python
with MetadataExtractor(
    db_type="spanner",
    instance_id="my-instance",
    database_id="my-database",
    project_id="my-gcp-project",   # optional if ADC is configured
) as ext:
    ...
```

Requires `google-cloud-spanner` and Application Default Credentials. Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.

### Server Mode

Omit `database` to scan **all databases on the server** at once. Supported for MySQL, ClickHouse, and PostgreSQL.

```python
with MetadataExtractor(db_type="mysql", host="db.internal",
                       user="readonly", password="secret") as ext:
    db_meta = ext.extract_all_schemas()    # every DB on this server
```

### Testing Your Connection

```python
with MetadataExtractor(**config) as ext:
    if not ext.test_connection():
        print("Connection failed")
```

---

## 4. Extracting Metadata

Once connected, you can extract at any level of granularity:

```python
with MetadataExtractor(**config) as ext:

    # List schema names without extracting metadata
    schemas = ext.get_schemas_list()

    # List table names in a schema without extracting metadata
    tables = ext.get_tables_list("public")

    # Extract a single table
    table = ext.extract_table("public", "orders")

    # Extract a single schema (all tables in it)
    schema = ext.extract_schema("public")

    # Extract multiple schemas (recommended — supports filtering and parallelism)
    db_meta = ext.extract_all_schemas(
        schema_filter=["public", "analytics"],
        parallel_workers=8,
    )
```

### The Data Model

Extracted metadata is organised as a nested object hierarchy:

```
DatabaseMetadata
  └── schemas: List[SchemaMetadata]
        └── tables: List[TableMetadata]
              ├── columns: List[ColumnMetadata]
              ├── primary_keys: List[str]
              ├── row_count: int
              └── description: str
```

Each `ColumnMetadata` contains: `name`, `data_type`, `is_nullable`, `is_primary_key`, `is_foreign_key`, `foreign_key_table`, `foreign_key_column`, `ordinal_position`, `default_value`, `character_maximum_length`, `numeric_precision`, `numeric_scale`, and `description`.

### Serialising for Storage or XCom

```python
# Convert to a plain dict (JSON-serialisable)
d = db_meta.to_dict()

# Restore the full object from a dict
from data_dictionary_builder import DatabaseMetadata
db_meta = DatabaseMetadata.from_dict(d)
```

This is used for Airflow XCom passing and for saving metadata to JSON files.

---

## 5. Filtering Schemas

Pass `schema_filter` to `extract_all_schemas()` to choose which schemas to extract. Filtering happens **after** fetching the live schema list — every match is guaranteed to exist.

### Filter Strategies

You can mix all six strategies in a single list:

```python
db_meta = ext.extract_all_schemas(
    schema_filter=[
        "public",                      # exact name (case-insensitive)
        "stg_%",                       # SQL-LIKE glob (* and ? also work)
        "prefix:raw_",                 # any schema starting with raw_
        "suffix:_prod",                # any schema ending with _prod
        "contains:analytics",          # any schema containing analytics
        "regex:^tmp_\\d{4}_\\w+$",    # full Python regex (re.fullmatch)
    ]
)
```

Pass `schema_filter=None` (the default) to extract all schemas.

---

## 6. Parallel Extraction

`extract_all_schemas()` uses a `ThreadPoolExecutor` internally. Each worker gets its own database connection, so threads never share state.

```python
db_meta = ext.extract_all_schemas(
    schema_filter=["public", "analytics", "raw", "staging"],
    parallel_workers=10,    # up to 10 schemas extracted concurrently
                            # automatically capped at the number of schemas
)
```

**Tips:**
- Start with `parallel_workers=8` and tune up/down based on your database's connection limit.
- Use `parallel_workers=1` for sequential extraction when debugging or with connectors that don't support concurrent connections.
- A single worker failure is logged and skipped; the remaining workers continue normally.

**Performance (bulk queries):**
The ClickHouse and PostgreSQL connectors use bulk queries that cover the entire schema in a fixed number of round-trips regardless of table count:

| Database | Queries for N tables |
|---|---|
| ClickHouse | 2 (one `system.tables` + one `system.columns`) |
| PostgreSQL | 5 (one per metadata type: columns, PKs, FKs, row counts, table info) |

---

## 7. Generating dbt YAML

### Per-Schema Files (recommended)

```python
from data_dictionary_builder import YAMLGenerator

gen   = YAMLGenerator(output_dir="./models")
files = gen.generate_yaml_files(db_meta)
# → models/public.yml, models/analytics.yml, …
```

### Single Combined File

```python
filepath = gen.generate_single_yaml(db_meta, filename="all_models.yml")
```

### What the Output Looks Like

```yaml
version: 2
models:
  - name: orders
    meta:
      schema: public
      table_type: BASE TABLE
      row_count: 4821903
    columns:
      - name: order_id
        data_type: integer
        meta:
          is_primary_key: true
          is_nullable: false
        tests:
          - unique
          - not_null
```

### Smart Merge — Your Descriptions Are Never Overwritten

On every re-run, `generate_yaml_files()` loads any existing YAML and **preserves**:

- User-written table and column descriptions
- dbt test definitions you've added
- Custom `meta` blocks

New tables and columns are added automatically. Changed data types are updated. Nothing you've written by hand is touched.

### Checking Documentation Coverage

```python
tables_missing  = gen.get_tables_without_descriptions(db_meta)
columns_missing = gen.get_columns_without_descriptions(db_meta)

total_cols = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
coverage   = 100 * (total_cols - len(columns_missing)) // max(total_cols, 1)
print(f"Column documentation coverage: {coverage}%")
```

**How gap detection works with YAML files:** The gap detection methods check your existing `{schema}.yml` files first (where hand-written descriptions live), then fall back to the in-memory metadata description field. This means descriptions you've written in YAML are always recognised as documented — the report will never incorrectly flag them as missing. The `YAMLGenerator` must be initialised with the same `output_dir` where your YAML files are stored.

---

## 8. Comparing Schemas

Use `SchemaComparator` to detect drift between environments (e.g. production vs. staging).

### Basic Comparison

```python
from data_dictionary_builder import SchemaComparator

comparator = SchemaComparator(
    source_config={"db_type": "postgres", "host": "prod-db", ...},
    destination_config={"db_type": "postgres", "host": "staging-db", ...},
    yaml_output_dir="./models",     # optional — used for gap detection
)

report = comparator.compare_and_generate_report(
    source_schema_name="public",
    include_yaml_gaps=True,         # also report undocumented tables/columns
)

print(report["summary"])
# {
#   "missing_tables_count": 3,
#   "missing_columns_count": 17,
#   "type_mismatches_count": 2,
#   "tables_without_descriptions_count": 24,
#   "columns_without_descriptions_count": 312,
# }
```

### Report Structure

```python
report["comparison"]["missing_tables"]     # [{schema, table, column_count}, ...]
report["comparison"]["missing_columns"]    # [{schema, table, column, data_type}, ...]
report["comparison"]["type_mismatches"]    # [{schema, table, column, source_type, destination_type}, ...]
report["yaml_gaps"]["tables_without_descriptions"]
report["yaml_gaps"]["columns_without_descriptions"]
```

### Reusing Already-Extracted Metadata

If you've already extracted metadata earlier in your pipeline, pass it directly to avoid a second database round-trip:

```python
# Extract once
with MetadataExtractor(**dest_config) as ext:
    dest_db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)

# Compare without re-querying the destination database
report = comparator.compare_and_generate_report(
    source_schema_name="public",
    dest_db_metadata=dest_db_meta,
    source_db_metadata=dest_db_meta,    # also reuse for YAML gap detection
)
```

### Cross-Database Comparison

Source and destination can be **different database engines**. Type normalisation maps vendor-specific aliases before comparing, so `character varying` (PostgreSQL) and `varchar` (MySQL) do not produce a false mismatch.

```python
comparator = SchemaComparator(
    source_config={"db_type": "postgres", ...},
    destination_config={"db_type": "clickhouse", ...},
)
```

### Batch Comparison

```python
results = comparator.extract_and_compare_all(
    source_schemas=["public", "analytics"],
    destination_schemas=["public", "analytics"],
)
```

---

## 9. Reports: JSON, PDF, and Email

`DDHelper` manages the standard output directory layout and handles all three report delivery mechanisms.

### Setting Up DDHelper

```python
from data_dictionary_builder import DDHelper

helper = DDHelper(".")          # or pass a custom base directory

# Directories created automatically:
# ./models/
# ./reports/json/
# ./reports/pdf/
```

### Save a Report to JSON

```python
json_path = helper.save_report(report)
# → reports/json/Metadata_comparison_2024-03-05_10-30-42.json
```

### Compile a PDF

```python
# Compile only the report from this run
pdf_path = helper.compile_pdf(source_json=json_path)

# Compile all JSON files in reports/json/ into one PDF
pdf_path = helper.compile_pdf()
```

The PDF is paginated with a table of contents, summary tables, and complete data — no row limits, no truncation. Requires `reportlab` (`pip install reportlab`).

### Send by Email

```python
helper.send_report_email(
    report=report,
    pdf_path=pdf_path,                      # PDF is attached
    subject="Nightly schema drift — prod",
    email_to="data-team@company.com",
)
```

SMTP credentials are read from environment variables when not passed explicitly:

| Env var | Parameter | Default |
|---|---|---|
| `SMTP_HOST` | `smtp_host` | — |
| `SMTP_PORT` | `smtp_port` | `587` |
| `SMTP_USER` | `smtp_user` | `""` |
| `SMTP_PASSWORD` | `smtp_password` | — |
| `EMAIL_TO` | `email_to` | — |

If `SMTP_HOST` or the recipient address is missing, the call returns `False` silently — it will not raise an exception. This keeps pipelines that run without email configured from failing.

### Override Credentials Explicitly

```python
helper.send_report_email(
    report=report,
    pdf_path=pdf_path,
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    smtp_user="sender@gmail.com",
    smtp_password="app-password",
    email_to="recipient@example.com",
    use_tls=True,
)
```

### Using `EmailSender` Directly

For full control without `DDHelper`:

```python
from data_dictionary_builder import EmailSender

sender = EmailSender(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    sender_email="you@gmail.com",
    sender_password="app-password",
    use_tls=True,
)

sender.send_comparison_report(
    recipient_emails=["team@example.com"],
    report=report,
    subject="Weekly drift report",
    attachments=[str(pdf_path)],
)
```

### SMTP Provider Quick Reference

| Provider | Host | Port | Notes |
|---|---|---|---|
| Gmail | `smtp.gmail.com` | `587` | Use an [App Password](https://support.google.com/accounts/answer/185833), not your account password |
| Office 365 | `smtp.office365.com` | `587` | — |
| AWS SES | `email-smtp.<region>.amazonaws.com` | `587` | Use SES SMTP credentials |
| SSL (any) | your host | `465` | Set `use_tls=False, use_ssl=True` |

---

## 10. Timing Your Pipeline

Wrap any block of code in `timer.task()` to measure and report its duration:

```python
from data_dictionary_builder import ExecutionTimer

timer = ExecutionTimer()

with timer.task("Connection test"):
    ok = ext.test_connection()

with timer.task("Metadata extraction"):
    db_meta = ext.extract_all_schemas(parallel_workers=8)

with timer.task("YAML generation"):
    gen.generate_yaml_files(db_meta)

with timer.task("Schema comparison"):
    report = comparator.compare_and_generate_report("public")

with timer.task("PDF + email"):
    json_path = helper.save_report(report)
    pdf_path  = helper.compile_pdf(source_json=json_path)
    helper.send_report_email(report=report, pdf_path=pdf_path)

timer.summary("Nightly Pipeline")
```

```
────────────────────────────────────────────────
  Nightly Pipeline
────────────────────────────────────────────────
  Connection test               0.312s
  Metadata extraction           1.984s
  YAML generation               0.091s
  Schema comparison             0.743s
  PDF + email                   2.103s
────────────────────────────────────────────────
  TOTAL                         5.233s
────────────────────────────────────────────────
```

Durations auto-format: `0.123s` / `1m 4.5s` / `2h 3m 15s`.

To access timings programmatically:

```python
task_timings, overall_seconds = timer.totals()
# task_timings → [("Connection test", 0.312), ("Metadata extraction", 1.984), ...]
```

---

## 11. Airflow Integration

`DatabaseMetadata` objects serialise to and from plain dicts via `to_dict()` / `from_dict()`, making them compatible with Airflow XCom out of the box.

```python
from airflow.decorators import dag, task
from data_dictionary_builder import (
    MetadataExtractor, DatabaseMetadata, YAMLGenerator,
    SchemaComparator, DDHelper,
)

@dag(schedule="@daily")
def metadata_pipeline():

    @task
    def extract():
        with MetadataExtractor(
            db_type="postgres", host="{{ var.value.db_host }}",
            database="{{ var.value.db_name }}", user="{{ var.value.db_user }}",
            password="{{ var.value.db_pass }}",
        ) as ext:
            db_meta = ext.extract_all_schemas(
                schema_filter=["public", "analytics"],
                parallel_workers=8,
            )
        return db_meta.to_dict()     # XCom-serialisable dict

    @task
    def generate_yaml(db_meta_dict: dict):
        helper  = DDHelper("/data/dbt")
        db_meta = DatabaseMetadata.from_dict(db_meta_dict)   # deserialise
        gen     = YAMLGenerator(output_dir=str(helper.models_dir))
        gen.generate_yaml_files(db_meta)

    @task
    def compare_and_report(db_meta_dict: dict):
        helper    = DDHelper("/data/dbt")
        dest_meta = DatabaseMetadata.from_dict(db_meta_dict)
        comparator = SchemaComparator(
            source_config={"db_type": "postgres", "host": "{{ var.value.prod_host }}", ...},
            destination_config={"db_type": "postgres", "host": "{{ var.value.staging_host }}", ...},
        )
        report    = comparator.compare_and_generate_report(
            "public",
            dest_db_metadata=dest_meta,
            include_yaml_gaps=True,
        )
        json_path = helper.save_report(report)
        pdf_path  = helper.compile_pdf(source_json=json_path)
        helper.send_report_email(
            report=report, pdf_path=pdf_path,
            email_to="{{ var.value.alert_email }}",
        )

    raw = extract()
    generate_yaml(raw)
    compare_and_report(raw)

metadata_pipeline()
```

See [`tests/airflow_dag_example.py`](tests/airflow_dag_example.py) for a complete implementation using `PythonOperator`.

---

## 12. CLI Reference

The CLI entry points are `ddgen` and `data-dictionary-builder`.

```bash
ddgen --help              # command overview
ddgen features            # full module and API reference (in-terminal)
ddgen <command> --help    # options and examples for any command
```

### `ddgen install <connector>`

Install a database driver.

```bash
ddgen install postgres
ddgen install mysql
ddgen install clickhouse      # installs clickhouse-connect (HTTP transport)
ddgen install spanner
ddgen install all
```

### `ddgen connectors`

List all supported connectors and whether each driver is currently installed.

### `ddgen info`

Show the library version, Python version, and connector status.

### `ddgen extract`

Extract metadata and generate YAML files in one command.

```bash
ddgen extract \
  --db-type postgres \
  --host prod.db.io \
  --database mydb \
  --user readonly \
  --output-dir ./models \
  --schema-filter "public" "prefix:stg_"
```

Key flags:

| Flag | Default | Description |
|---|---|---|
| `--db-type` | `postgres` | Database type |
| `--host` | `localhost` | Server hostname |
| `--port` | *(auto)* | Server port |
| `--database` | — | Database name (omit for server mode) |
| `--user` / `--password` | — | Credentials |
| `--transport` | *(auto)* | ClickHouse only: `http` or `native` |
| `--secure` | `false` | ClickHouse only: enable TLS |
| `--output-dir` | `./models` | Where to write YAML files |
| `--schema-filter` | *(all)* | One or more filter strings (space-separated) |
| `--workers` | `5` | Parallel extraction threads |

### `ddgen compare`

Compare two environments and save a JSON + PDF report.

```bash
ddgen compare \
  --source-host prod.db.io   --source-database mydb \
  --dest-host staging.db.io  --dest-database mydb \
  --source-db-type postgres  --dest-db-type postgres \
  --schema public \
  --output-dir ./reports
```

### `ddgen features`

Prints a full in-terminal reference covering all classes, methods, parameters, and code examples — no browser needed.

```bash
ddgen features
```

---

## 13. API Reference

### `MetadataExtractor(db_type, **connection_params)`

Key constructor parameters:

| Parameter | Type | Description |
|---|---|---|
| `db_type` | `str` | `"sqlite"` \| `"postgres"` \| `"mysql"` \| `"clickhouse"` \| `"spanner"` |
| `host` | `str` | Server hostname or IP |
| `port` | `int` | Server port — auto-defaulted per db_type if omitted |
| `database` | `str` | Database name — omit for server-mode scan |
| `user` / `password` | `str` | Credentials |
| `transport` | `str` | ClickHouse only: `"http"` \| `"native"` \| `None` (auto) |
| `secure` | `bool` | ClickHouse only: enable TLS (auto-adjusts port to 8443/9440) |
| `project_id` | `str` | Spanner only: GCP project ID |
| `instance_id` | `str` | Spanner only: Cloud Spanner instance ID |

| Method | Returns | Description |
|---|---|---|
| `test_connection()` | `bool` | Verify connectivity |
| `get_schemas_list()` | `List[str]` | All schema names |
| `get_tables_list(schema_name)` | `List[str]` | Table names in a schema |
| `extract_table(schema, table)` | `TableMetadata` | Single table |
| `extract_schema(schema_name)` | `SchemaMetadata` | Full schema |
| `extract_all_schemas(schema_filter, parallel_workers)` | `DatabaseMetadata` | All filtered schemas in parallel |

---

### `YAMLGenerator(output_dir)`

| Method | Returns | Description |
|---|---|---|
| `generate_yaml_files(db_meta)` | `List[str]` | One YAML per schema; smart merge |
| `generate_single_yaml(db_meta, filename)` | `str` | One combined file |
| `generate_schema_yaml(schema, filename)` | `str` | YAML for one schema |
| `get_tables_without_descriptions(db_meta)` | `List[str]` | Tables missing a description — checks existing YAML files first, then in-memory metadata |
| `get_columns_without_descriptions(db_meta)` | `List[dict]` | Columns missing a description — checks existing YAML files first, then in-memory metadata |

---

### `SchemaComparator(source_config, destination_config, yaml_output_dir=None)`

| Method | Returns | Description |
|---|---|---|
| `compare_schemas(source_schema, dest_schema=None, dest_db_metadata=None)` | `ComparisonResult` | Structured diff |
| `compare_and_generate_report(source_schema_name, destination_schema_name=None, include_yaml_gaps=False, source_db_metadata=None, dest_db_metadata=None)` | `dict` | Full report dict |
| `extract_and_compare_all(source_schemas, destination_schemas)` | `List[ComparisonResult]` | Batch diff |

---

### `DDHelper(base_dir=".")`

| Attribute / Method | Description |
|---|---|
| `models_dir` | `Path` to the YAML output directory |
| `reports_json_dir` | `Path` to the JSON report directory |
| `reports_pdf_dir` | `Path` to the PDF report directory |
| `dirs` | Dict with all four paths |
| `save_report(report, dt=None)` | Write report to JSON; returns `Path` |
| `compile_pdf(source_json=None, output_pdf=None)` | JSON → PDF; returns `Path` or `None` |
| `send_report_email(report, pdf_path=None, subject=None, *, smtp_host, smtp_port, smtp_user, smtp_password, email_to, use_tls)` | Send email; all SMTP params fall back to env vars |

---

### `ExecutionTimer()`

| Attribute / Method | Description |
|---|---|
| `task(name)` | Context manager recording a named duration |
| `elapsed` | Seconds since timer was created (read-only) |
| `totals()` | `([(name, secs), …], overall_secs)` |
| `summary(title="Execution Summary")` | Print formatted table |

---

### `EmailSender(smtp_host, smtp_port, sender_email, sender_password=None, use_tls=True, use_ssl=False)`

| Method | Returns | Description |
|---|---|---|
| `send_comparison_report(recipient_emails, report, subject=None, attachments=None)` | `bool` | Send formatted HTML report |
| `send_email(recipient_emails, subject, text_body, html_body=None, attachments=None)` | `bool` | Send custom email |

---

### `DatabaseMetadata`

| Method | Description |
|---|---|
| `to_dict()` | Serialise to a plain dict (JSON-safe) |
| `from_dict(data)` *(classmethod)* | Reconstruct from a dict produced by `to_dict()` |
| `get_schema(schema_name)` | Look up a `SchemaMetadata` by name |
| `add_schema(schema)` | Append a `SchemaMetadata` |

---

## 14. Troubleshooting

### Connection Problems

**ClickHouse — `Connection refused` or timeout**
The port depends on transport:
- HTTP (`clickhouse-connect`): `8123` plain, `8443` TLS. Pass `secure=True` to auto-select `8443`.
- Native TCP (`clickhouse-driver`): `9000` plain, `9440` TLS. Pass `secure=True` to auto-select `9440`.

If you omit `port` entirely, the connector picks the right default based on your `transport` and `secure` values. For ClickHouse Cloud, always use `secure=True` and the HTTP transport (default). Pass `verify=False` for self-signed certificates.

**ClickHouse — `No ClickHouse driver found`**
At least one driver must be installed. Install the HTTP driver (recommended):
```bash
pip install clickhouse-connect
# or
ddgen install clickhouse
```
For native TCP:
```bash
pip install clickhouse-driver
```

**Google Cloud Spanner — `google.auth.exceptions.DefaultCredentialsError`**
Run `gcloud auth application-default login`, or set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.

**PostgreSQL / MySQL — `permission denied`**
The database user needs `SELECT` privileges on `information_schema` and all target schemas. A read-only role is sufficient.

---

### Slow Extraction

- Always use `schema_filter` to limit extraction to the schemas you need.
- Increase `parallel_workers` — the default is conservative. Try `parallel_workers=10` and tune based on your database's connection limit.
- Use `ExecutionTimer` to find which step is the bottleneck.
- PostgreSQL row counts use `pg_class.reltuples` estimates — they are fast but approximate.

---

### YAML Issues

**My descriptions were overwritten.**
They should not be — `generate_yaml_files()` always merges with existing files. Check that you are writing to the same `output_dir` you read from. If you generate to `./models/` but the existing file is somewhere else, a new blank file is created instead of merging.

**The report still shows my tables/columns as undocumented, even though I added descriptions to the YAML.**
The gap detection methods (`get_tables_without_descriptions` / `get_columns_without_descriptions`) read descriptions from the existing YAML files on disk — make sure the `YAMLGenerator` is initialised with the same `output_dir` where your YAML files live. Descriptions that exist only in the YAML (not in the database as COMMENTs) are recognised correctly as long as the paths match.

**YAML validation errors after generation.**
Run `yamllint ./models/*.yml`. Descriptions containing YAML special characters (`:`, `#`, `{`) are handled automatically — if you see errors, they likely come from manual edits.

---

### Email Issues

**`send_report_email()` returns `False` with no error.**
Check that `SMTP_HOST` is set and that either `email_to` (parameter) or `EMAIL_TO` (env var) is set. Missing either causes a silent skip.

**Gmail authentication failure.**
Use an [App Password](https://support.google.com/accounts/answer/185833) — not your Google account password. App Passwords require 2-Step Verification to be enabled on your account.

**Wrong port / SSL mismatch.**
Port `587` uses STARTTLS: set `use_tls=True, use_ssl=False`.
Port `465` uses implicit SSL: set `use_tls=False, use_ssl=True`.

---

### PDF Issues

**`compile_pdf()` returns `None`.**
Either `reportlab` is not installed (`pip install reportlab`) or there are no `*.json` files in `reports/json/`.

**PDF is missing some rows.**
The PDF always includes all rows. If rows appear cut off, verify you are using `compile_pdf(source_json=json_path)` to compile only the current run's JSON rather than picking up an older, partial file.
