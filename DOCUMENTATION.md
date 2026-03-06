# data_dictionary_builder — Complete Documentation

## Table of Contents

1. [Overview](#1-overview)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Database Connectors](#4-database-connectors)
5. [Metadata Extraction](#5-metadata-extraction)
6. [Schema Filtering](#6-schema-filtering)
7. [Parallel Extraction](#7-parallel-extraction)
8. [YAML Generation](#8-yaml-generation)
9. [Schema Comparison](#9-schema-comparison)
10. [DDHelper — Output & Reporting Utility](#10-ddhelper--output--reporting-utility)
11. [ExecutionTimer — Performance Timing](#11-executiontimer--performance-timing)
12. [Email Notifications](#12-email-notifications)
13. [Airflow Integration](#13-airflow-integration)
14. [API Reference](#14-api-reference)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Overview

**data_dictionary_builder** is a Python library for extracting database metadata, generating dbt-compatible YAML documentation, comparing schemas across databases, and delivering structured reports — all with built-in timing and email delivery.

### Key Features

| Feature | Description |
|---|---|
| **Multi-database** | SQLite, PostgreSQL, MySQL, ClickHouse, Google Cloud Spanner |
| **Metadata extraction** | Tables, columns, constraints, row counts, FK relationships |
| **Parallel extraction** | ThreadPoolExecutor; configurable worker count; thread-safe |
| **Server mode** | Scan all databases on a server when no `database` is specified |
| **Schema filtering** | Exact, glob, prefix, suffix, contains, regex — mix freely |
| **YAML generation** | dbt v2-compatible; smart merge preserves existing descriptions |
| **Schema comparison** | Cross-database diff; type normalisation |
| **DDHelper** | Manages output dirs, JSON/PDF reports, email delivery |
| **ExecutionTimer** | Named-task timing with formatted summary table |
| **Email** | SMTP delivery with HTML+text body and PDF attachment |

---

## 2. Installation

```bash
# From source
git clone https://github.com/GraFreak0/data_dictionary_builder.git
cd data_dictionary_builder
pip install -r requirements.txt
pip install -e .

# With dev tools (pytest, black, flake8, mypy)
pip install -e ".[dev]"
```

---

## 3. Quick Start

```python
from data_dictionary_builder import (
    MetadataExtractor,
    YAMLGenerator,
    SchemaComparator,
    DDHelper,
    ExecutionTimer,
)

timer  = ExecutionTimer()
helper = DDHelper(".")          # creates models/, reports/json/, reports/pdf/

config = {
    "db_type": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "readonly",
    "password": "secret",
}

with timer.task("Extract metadata"):
    with MetadataExtractor(**config) as ext:
        db_meta = ext.extract_all_schemas(
            schema_filter=["public", "analytics"],
            parallel_workers=8,
        )

with timer.task("Generate YAML"):
    gen = YAMLGenerator(output_dir=str(helper.models_dir))
    gen.generate_yaml_files(db_meta)

with timer.task("Compare schemas"):
    report = SchemaComparator(
        source_config=config,
        destination_config={**config, "host": "staging-db"},
    ).compare_and_generate_report("public", include_yaml_gaps=True)

with timer.task("Save & email report"):
    json_path = helper.save_report(report)
    pdf_path  = helper.compile_pdf(source_json=json_path)
    helper.send_report_email(report=report, pdf_path=pdf_path,
                             email_to="team@example.com")

timer.summary()
```

---

## 4. Database Connectors

Connectors are created via the factory function `get_connector(db_type, **kwargs)` or indirectly through `MetadataExtractor`. You do not need to instantiate them directly.

### SQLite

```python
config = {
    "db_type":  "sqlite",
    "database": "/path/to/database.db",
}
```

### PostgreSQL

```python
config = {
    "db_type":  "postgres",
    "host":     "localhost",
    "port":     5432,
    "database": "mydb",
    "user":     "postgres",
    "password": "password",
}
```

Row counts use `pg_class` estimates for performance.

### MySQL / MariaDB

```python
config = {
    "db_type":  "mysql",
    "host":     "localhost",
    "port":     3306,
    "database": "mydb",       # omit for server mode
    "user":     "root",
    "password": "password",
}
```

### ClickHouse

```python
config = {
    "db_type":  "clickhouse",
    "host":     "my-cluster.clickhouse.cloud",
    "port":     9440,          # native protocol (not HTTP 8123)
    "database": "default",     # omit for server mode
    "user":     "default",
    "password": "secret",
    "secure":   True,          # TLS
    "verify":   False,         # skip cert verification for self-signed certs
}
```

Metadata is read from ClickHouse's `system.columns` table.

### Google Cloud Spanner

```python
config = {
    "db_type":     "spanner",
    "instance_id": "my-instance",
    "database_id": "my-database",
    "project_id":  "my-gcp-project",   # optional; uses ADC if omitted
}
```

Requires Application Default Credentials (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`).

### Server Mode

When `database` is **omitted**, the extractor scans **all databases** on the server. Supported for MySQL, ClickHouse, and PostgreSQL.

```python
# Omit 'database' to enable server mode
config = {
    "db_type":  "mysql",
    "host":     "db.internal",
    "user":     "readonly",
    "password": "secret",
    # no 'database' key
}

with MetadataExtractor(**config) as ext:
    db_meta = ext.extract_all_schemas()    # scans every DB on the server
```

---

## 5. Metadata Extraction

### `MetadataExtractor`

Always use as a context manager — it manages the connection lifecycle automatically.

```python
with MetadataExtractor(**config) as ext:
    # Test connection
    ok = ext.test_connection()

    # List schemas / tables without extracting full metadata
    schemas = ext.get_schemas_list()
    tables  = ext.get_tables_list("public")

    # Extract a single table
    table = ext.extract_table("public", "orders")

    # Extract a single schema
    schema = ext.extract_schema("public")

    # Extract all schemas (with optional filter and parallelism)
    db_meta = ext.extract_all_schemas(
        schema_filter=["public", "analytics"],
        parallel_workers=10,
    )
```

### Data Model Hierarchy

```
DatabaseMetadata
  └── schemas: List[SchemaMetadata]
        └── tables: List[TableMetadata]
              ├── columns: List[ColumnMetadata]
              ├── primary_keys: List[str]
              ├── foreign_keys: List[dict]
              └── row_count: int
```

Each `ColumnMetadata` contains: `name`, `data_type`, `is_nullable`, `is_primary_key`, `ordinal_position`, `default_value`, `character_maximum_length`, `numeric_precision`, `numeric_scale`, `description`, and `foreign_key_info`.

### Serialisation (Airflow / XCom)

```python
# Serialise to dict for XCom or JSON storage
d = db_meta.to_dict()

# Reconstruct from dict
from data_dictionary_builder import DatabaseMetadata
db_meta = DatabaseMetadata.from_dict(d)
```

---

## 6. Schema Filtering

Pass `schema_filter` to `extract_all_schemas()`. Filtering happens **after** fetching the live schema list, so every match is guaranteed to exist.

### Filter Strategies

| Format | Example | Matches |
|---|---|---|
| Exact name | `"public"` | Only `public` |
| Glob / SQL-LIKE | `"stg_%"` | `stg_orders`, `stg_customers` |
| Prefix | `"prefix:stg_"` | Anything starting with `stg_` |
| Suffix | `"suffix:_prod"` | Anything ending with `_prod` |
| Contains | `"contains:analytics"` | Anything containing `analytics` |
| Regex | `"regex:^tmp_\\d+$"` | Full `re.fullmatch` (case-insensitive) |
| `None` | — | All schemas (no filtering) |

All matching is **case-insensitive**. Strategies can be **mixed** in a single list:

```python
db_meta = ext.extract_all_schemas(
    schema_filter=[
        "public",                    # exact
        "stg_%",                     # glob
        "prefix:raw_",               # prefix
        "suffix:_prod",              # suffix
        "contains:analytics",        # contains
        "regex:^tmp_\\d{4}_\\w+$",   # regex
    ]
)
```

---

## 7. Parallel Extraction

`extract_all_schemas()` extracts schemas concurrently using `ThreadPoolExecutor`. Each worker creates its own database connection so threads never share state.

```python
db_meta = ext.extract_all_schemas(
    schema_filter=["public", "analytics", "raw", "staging"],
    parallel_workers=10,    # up to 10 concurrent threads
                            # automatically capped at number of schemas
)
```

- Set `parallel_workers=1` for sequential extraction (useful for debugging or connectors that do not support concurrent connections).
- A worker failure is logged and skipped; it does not abort the remaining workers.
- Results are always returned in the **original schema order**.

---

## 8. YAML Generation

### Per-Schema Files

```python
gen   = YAMLGenerator(output_dir="./dbt_models")
files = gen.generate_yaml_files(db_meta)
# → dbt_models/public.yml, dbt_models/analytics.yml, …
```

### Single Combined File

```python
filepath = gen.generate_single_yaml(db_meta, filename="all_models.yml")
```

### Smart Merge

On re-runs, `YAMLGenerator` loads any existing YAML and **preserves**:
- User-written column and table descriptions
- dbt test definitions
- Custom `meta` blocks

Only new tables/columns are added; changed data types are updated. Nothing the user has written is overwritten.

### Documentation Gap Detection

```python
tables_missing_desc  = gen.get_tables_without_descriptions(db_meta)
columns_missing_desc = gen.get_columns_without_descriptions(db_meta)

total_tables  = sum(len(s.tables) for s in db_meta.schemas)
total_columns = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)

coverage_t = 100 * (total_tables  - len(tables_missing_desc))  // max(total_tables, 1)
coverage_c = 100 * (total_columns - len(columns_missing_desc)) // max(total_columns, 1)
print(f"Table coverage:  {coverage_t}%")
print(f"Column coverage: {coverage_c}%")
```

---

## 9. Schema Comparison

### Compare Two Schemas

```python
from data_dictionary_builder import SchemaComparator

comparator = SchemaComparator(
    source_config=source_config,
    destination_config=dest_config,
    yaml_output_dir="./dbt_models",    # optional; used for gap detection
)

# Compare schemas with the same name in source and destination
result = comparator.compare_schemas("public")
print(f"Missing tables:  {len(result.missing_tables)}")
print(f"Missing columns: {len(result.missing_columns)}")
print(f"Type mismatches: {len(result.type_mismatches)}")
```

### Generate a Full Report Dict

```python
report = comparator.compare_and_generate_report(
    source_schema_name="public",
    destination_schema_name="public",   # defaults to source_schema_name
    include_yaml_gaps=True,             # adds undocumented tables/columns
)

# report["summary"]["missing_tables_count"]
# report["summary"]["missing_columns_count"]
# report["summary"]["type_mismatches_count"]
# report["summary"]["tables_without_descriptions_count"]
# report["summary"]["columns_without_descriptions_count"]
# report["comparison"]["missing_tables"]    → list of {schema, table, column_count}
# report["comparison"]["missing_columns"]   → list of {schema, table, column, data_type}
# report["comparison"]["type_mismatches"]   → list of {schema, table, column, source_type, destination_type}
# report["yaml_gaps"]["tables_without_descriptions"]
# report["yaml_gaps"]["columns_without_descriptions"]
```

### Batch Comparison

```python
results = comparator.extract_and_compare_all(
    source_schemas=["public", "analytics"],
    destination_schemas=["public", "analytics"],
)
```

### Cross-Database Comparison

Source and destination can be **different database types**. Type normalisation maps vendor-specific aliases (e.g. PostgreSQL `character varying` → `varchar`, `int4` → `integer`) before comparing.

```python
comparator = SchemaComparator(
    source_config={"db_type": "postgres", "host": "prod-pg", ...},
    destination_config={"db_type": "mysql",    "host": "staging-mysql", ...},
)
```

---

## 10. DDHelper — Output & Reporting Utility

`DDHelper` manages the standard output directory layout, JSON report persistence, PDF compilation, and email delivery in one class.

### Directory Layout

```
<base_dir>/
    models/
    reports/
        json/    ← Metadata_comparison_YYYY-MM-DD_HH-MM-SS.json
        pdf/     ← Metadata_comparison_YYYY-MM-DD_HH-MM-SS.pdf
```

### Initialise

```python
from data_dictionary_builder import DDHelper

helper = DDHelper()                  # base = current working directory
helper = DDHelper(base_dir="/data")  # custom root

# Access directory paths
helper.models_dir        # Path object
helper.reports_json_dir  # Path object
helper.reports_pdf_dir   # Path object

# Or as a dict
dirs = helper.dirs
# dirs["models"], dirs["reports_json"], dirs["reports_pdf"]
```

### Save a JSON Report

```python
json_path = helper.save_report(report)
# → reports/json/Metadata_comparison_2024-03-05_10-30-42.json
```

### Compile PDF

```python
# Compile all JSON files in reports/json/ into one PDF
pdf_path = helper.compile_pdf()

# Compile only the most-recently saved report
pdf_path = helper.compile_pdf(source_json=json_path)

# Override output path
pdf_path = helper.compile_pdf(output_pdf=Path("/tmp/my_report.pdf"))
```

Requires `reportlab`. Install with `pip install reportlab`.

The PDF contains:
- Cover page with generation timestamp and report count
- Table of contents
- Per-report sections: summary table, missing tables, missing columns, type mismatches, documentation gaps

### Send Email

```python
helper.send_report_email(
    report=report,
    pdf_path=pdf_path,                      # PDF attached; optional
    subject="Nightly schema drift",
    email_to="recipient@example.com",
)
```

SMTP parameters can be supplied explicitly **or** read from environment variables:

| Parameter | Env var | Default |
|---|---|---|
| `smtp_host` | `SMTP_HOST` | — |
| `smtp_port` | `SMTP_PORT` | `587` |
| `smtp_user` | `SMTP_USER` | `""` |
| `smtp_password` | `SMTP_PASSWORD` | — |
| `email_to` | `EMAIL_TO` | — |

If `SMTP_HOST` or `email_to` / `EMAIL_TO` is not set, the email is silently skipped (returns `False`).

```python
# Explicit credentials (override env vars)
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

---

## 11. ExecutionTimer — Performance Timing

`ExecutionTimer` tracks wall-clock time for named tasks and prints a formatted summary.

### Basic Usage

```python
from data_dictionary_builder import ExecutionTimer

timer = ExecutionTimer()    # clock starts here

with timer.task("Connect & extract"):
    with MetadataExtractor(**config) as ext:
        db_meta = ext.extract_all_schemas(parallel_workers=8)

with timer.task("Generate YAML"):
    gen.generate_yaml_files(db_meta)

with timer.task("Schema comparison"):
    report = comparator.compare_and_generate_report("public")

timer.summary()
```

Example output:

```
────────────────────────────────────────────────
  Execution Summary
────────────────────────────────────────────────
  Connect & extract                      2.341s
  Generate YAML                          0.087s
  Schema comparison                      1.203s
────────────────────────────────────────────────
  TOTAL                                  3.631s
────────────────────────────────────────────────
```

Duration format: `0.123s` / `1m 4.5s` / `2h 3m 15s` depending on magnitude.

### Custom Title

```python
timer.summary("ClickHouse Test Suite — Execution Summary")
```

### Programmatic Access

```python
task_timings, overall_seconds = timer.totals()

for name, secs in task_timings:
    print(f"{name}: {secs:.3f}s")

print(f"Overall: {overall_seconds:.3f}s")
```

`timer.elapsed` returns the total seconds since the timer was created (read-only property, no side effects).

### Nested Tasks

`ExecutionTimer` supports any nesting depth — including tasks within tasks — and records each `with timer.task(...)` block independently:

```python
with timer.task("Full pipeline"):
    with timer.task("Extract"):
        ...
    with timer.task("Generate"):
        ...
```

> **Note:** Durations are measured in **wall-clock time** (not CPU time), so parallel threads contribute to a single task's elapsed time rather than multiplying it.

---

## 12. Email Notifications

### `EmailSender` (direct usage)

Use `EmailSender` directly when you need full control over SMTP settings without going through `DDHelper`.

```python
from data_dictionary_builder import EmailSender

sender = EmailSender(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    sender_email="you@gmail.com",
    sender_password="app-password",    # Gmail App Password
    use_tls=True,
    use_ssl=False,
)

# Send a formatted comparison report (HTML + plain text)
ok = sender.send_comparison_report(
    recipient_emails=["team@example.com", "manager@example.com"],
    report=report,
    subject="Weekly Schema Drift Report",
    attachments=["/path/to/report.pdf"],
)

# Send a completely custom email
ok = sender.send_email(
    recipient_emails=["user@example.com"],
    subject="Custom notification",
    text_body="Plain-text fallback.",
    html_body="<h1>HTML version</h1>",
    attachments=["/path/to/file.pdf"],
)
```

### SMTP Provider Examples

**Gmail (App Password)**
```python
EmailSender(smtp_host="smtp.gmail.com", smtp_port=587,
            sender_email="you@gmail.com", sender_password="xxxx xxxx xxxx xxxx",
            use_tls=True)
```

**Office 365**
```python
EmailSender(smtp_host="smtp.office365.com", smtp_port=587,
            sender_email="you@company.com", sender_password="password",
            use_tls=True)
```

**AWS SES**
```python
EmailSender(smtp_host="email-smtp.us-east-1.amazonaws.com", smtp_port=587,
            sender_email="verified@yourdomain.com",
            sender_password="SES-SMTP-password", use_tls=True)
```

**SSL (port 465)**
```python
EmailSender(smtp_host="mail.example.com", smtp_port=465,
            sender_email="you@example.com", sender_password="password",
            use_tls=False, use_ssl=True)
```

### Using Environment Variables

Set these in your `.env` file or shell environment for credential-free code:

```bash
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=xxxx xxxx xxxx xxxx
EMAIL_TO=recipient@example.com
```

Then call `helper.send_report_email(report=report, pdf_path=pdf_path)` with no other arguments.

---

## 13. Airflow Integration

### Using `DatabaseMetadata.to_dict()` / `from_dict()` for XCom

```python
from airflow.decorators import dag, task
from data_dictionary_builder import MetadataExtractor, DatabaseMetadata, YAMLGenerator, DDHelper

@dag(schedule="@daily")
def metadata_pipeline():

    @task
    def extract():
        with MetadataExtractor(db_type="postgres", host="prod-db",
                               database="mydb", user="ro", password="{{ var.value.db_pass }}") as ext:
            db_meta = ext.extract_all_schemas(
                schema_filter=["public", "analytics"],
                parallel_workers=8,
            )
        return db_meta.to_dict()   # XCom-serialisable

    @task
    def generate_yaml(db_meta_dict: dict):
        helper  = DDHelper("/data/dbt")
        db_meta = DatabaseMetadata.from_dict(db_meta_dict)
        gen     = YAMLGenerator(output_dir=str(helper.models_dir))
        gen.generate_yaml_files(db_meta)

    @task
    def compare_and_report(db_meta_dict: dict):
        helper = DDHelper("/data/dbt")
        report = SchemaComparator(
            source_config={"db_type": "postgres", "host": "prod-db", ...},
            destination_config={"db_type": "postgres", "host": "staging-db", ...},
        ).compare_and_generate_report("public", include_yaml_gaps=True)
        json_path = helper.save_report(report)
        pdf_path  = helper.compile_pdf(source_json=json_path)
        helper.send_report_email(report=report, pdf_path=pdf_path,
                                 email_to="data-team@example.com")

    raw = extract()
    generate_yaml(raw)
    compare_and_report(raw)

metadata_pipeline()
```

See `examples/airflow_dag_example.py` for a complete implementation.

---

## 14. API Reference

### `MetadataExtractor(db_type, **connection_params)`

| Method | Signature | Description |
|---|---|---|
| `connect()` | `→ None` | Open database connection |
| `disconnect()` | `→ None` | Close database connection |
| `test_connection()` | `→ bool` | Verify connectivity |
| `get_schemas_list()` | `→ List[str]` | List all schema names |
| `get_tables_list(schema_name)` | `→ List[str]` | List tables in a schema |
| `extract_schema(schema_name)` | `→ SchemaMetadata` | Full schema metadata |
| `extract_table(schema, table)` | `→ TableMetadata` | Single table metadata |
| `extract_all_schemas(schema_filter, parallel_workers)` | `→ DatabaseMetadata` | Extract all (or filtered) schemas; parallel |

Context-manager protocol: `__enter__` calls `connect()`, `__exit__` calls `disconnect()`.

---

### `YAMLGenerator(output_dir)`

| Method | Signature | Description |
|---|---|---|
| `generate_yaml_files(db_meta)` | `→ List[str]` | One YAML file per schema; smart merge |
| `generate_single_yaml(db_meta, filename)` | `→ str` | One combined YAML file |
| `generate_schema_yaml(schema, filename)` | `→ str` | YAML for a single schema |
| `get_tables_without_descriptions(db_meta)` | `→ List[str]` | Undocumented tables |
| `get_columns_without_descriptions(db_meta)` | `→ List[dict]` | Undocumented columns |

---

### `SchemaComparator(source_config, destination_config, yaml_output_dir=None)`

| Method | Signature | Description |
|---|---|---|
| `compare_schemas(source_schema, dest_schema=None)` | `→ ComparisonResult` | Structured diff object |
| `compare_and_generate_report(source_schema_name, destination_schema_name=None, include_yaml_gaps=False)` | `→ dict` | Full report dict |
| `extract_and_compare_all(source_schemas, destination_schemas)` | `→ List[ComparisonResult]` | Batch comparison |

---

### `DDHelper(base_dir=".")`

| Attribute / Method | Description |
|---|---|
| `base_dir` | `Path` — root directory |
| `dirs` | `dict` — `{models, reports, reports_json, reports_pdf}` |
| `models_dir` | `Path` shortcut |
| `reports_json_dir` | `Path` shortcut |
| `reports_pdf_dir` | `Path` shortcut |
| `save_report(report, dt=None)` | Serialise report to JSON; returns `Path` |
| `compile_pdf(source_json=None, output_pdf=None)` | Compile JSON → PDF; returns `Path` or `None` |
| `send_report_email(report, pdf_path=None, subject=None, *, smtp_host, smtp_port, smtp_user, smtp_password, email_to, use_tls)` | Send email; all SMTP params fall back to env vars |

---

### `ExecutionTimer()`

| Attribute / Method | Description |
|---|---|
| `task(name)` | Context manager; records named duration |
| `elapsed` | Property — seconds since timer was created |
| `totals()` | Returns `([(name, secs), …], overall_secs)` |
| `summary(title="Execution Summary")` | Print formatted table to stdout |

---

### `EmailSender(smtp_host, smtp_port, sender_email, sender_password=None, use_tls=True, use_ssl=False)`

| Method | Signature | Description |
|---|---|---|
| `send_comparison_report(recipient_emails, report, subject=None, attachments=None)` | `→ bool` | Formatted HTML+text report email |
| `send_email(recipient_emails, subject, text_body, html_body=None, attachments=None)` | `→ bool` | Generic email with optional attachments |

---

## 15. Troubleshooting

### Connection Issues

**ClickHouse:** Use the native protocol port (default `9440` for TLS, `9000` for plain). The HTTP port `8123` is not supported. Pass `secure=True, verify=False` for self-signed certificates.

**Google Cloud Spanner:** Set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` or run `gcloud auth application-default login`.

**PostgreSQL / MySQL:** Ensure the database user has `SELECT` on `information_schema` and the target schemas.

### Performance

- Always use `schema_filter` to limit extraction to relevant schemas.
- Increase `parallel_workers` to speed up large databases (tune to your DB's connection limit).
- Row counts in PostgreSQL use `pg_class` estimates, which are fast but approximate.
- Use `ExecutionTimer` to identify which step is the bottleneck.

### YAML Issues

- Descriptions containing colons (`:`) or special YAML characters are handled automatically.
- Re-running `generate_yaml_files` will **not** overwrite user-written descriptions; use a text editor to remove generated content if needed.
- Validate generated files with `yamllint ./dbt_models/*.yml`.

### Email Issues

- **Gmail:** Use an [App Password](https://support.google.com/accounts/answer/185833), not your account password. Requires 2-Step Verification enabled.
- **SSL vs TLS:** Port 587 uses STARTTLS (`use_tls=True, use_ssl=False`). Port 465 uses implicit SSL (`use_tls=False, use_ssl=True`).
- If `helper.send_report_email()` returns `False` with no error, verify that `SMTP_HOST` and either `email_to` (parameter) or `EMAIL_TO` (env var) are set.
- PDF attachment requires `reportlab` (`pip install reportlab`).

### PDF Compilation

`compile_pdf()` returns `None` when:
- `reportlab` is not installed.
- No `*.json` files exist in `reports/json/`.

Install reportlab: `pip install reportlab`.
