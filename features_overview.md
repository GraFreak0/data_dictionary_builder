# data_dictionary_builder

A Python library that extracts schema metadata from your databases and generates dbt-compatible YAML documentation. Connect it to any supported database, point it at the schemas you care about, and it produces ready-to-use `schema.yml` files — with parallel extraction to keep things fast even across large servers.

---

## Connectors

Five connectors are included. All implement the same `BaseConnector` interface, so switching databases requires no changes to your downstream code.

| Database | Driver | Default Port | Notes |
|---|---|---|---|
| SQLite | built-in `sqlite3` | — | File-based, no server needed |
| PostgreSQL | `psycopg2-binary` | 5432 | Row estimates via `pg_class` |
| MySQL / MariaDB | `PyMySQL` | 3306 | Server-mode scans all databases in one pass |
| ClickHouse | `clickhouse-driver` | 9000 | Native protocol; reads `system.columns` |
| Google Cloud Spanner | `google-cloud-spanner` | — | Application Default Credentials |

---

## Schema Filtering

Pass a `schema_filter` list to `extract_all_schemas()`. The extractor fetches all schema names from the database first, then resolves each entry against that live list — so filters always match real names, never guesses.

Entries can be mixed freely in a single call:

```python
db_meta = ext.extract_all_schemas(schema_filter=[
    "public",                    # exact name
    "monkeybook_%",              # SQL-LIKE / glob wildcard
    "prefix:stg_",               # anything starting with stg_
    "suffix:_prod",              # anything ending with _prod
    "contains:analytics",        # substring match
    "regex:^reporting_\\d{4}$",  # full regex
])
```

| Format | Example | Matches |
|---|---|---|
| Exact name | `"public"` | Only `public` |
| Glob / SQL-LIKE | `"monkeybook_%"` | `monkeybook_orders`, `monkeybook_customers` … |
| `prefix:` | `"prefix:stg_"` | `stg_orders`, `stg_customers` … |
| `suffix:` | `"suffix:_prod"` | `analytics_prod`, `raw_prod` … |
| `contains:` | `"contains:finance"` | `finance_reporting`, `corp_finance` … |
| `regex:` | `"regex:^tmp_\\d+$"` | `tmp_1`, `tmp_42` … |
| `None` | _(omit the argument)_ | Every schema |

Existing plain lists (`schema_filter=["public", "analytics"]`) continue to work unchanged.

---

## Parallel Extraction

Schemas are extracted in parallel using a `ThreadPoolExecutor`. The `parallel_workers` parameter controls how many schemas are processed concurrently, defaulting to `5`.

```python
# Default — 5 parallel workers
db_meta = ext.extract_all_schemas()

# Increase for servers with many schemas and a connection pool that supports it
db_meta = ext.extract_all_schemas(parallel_workers=10)

# Set to 1 to force sequential extraction (useful for debugging)
db_meta = ext.extract_all_schemas(parallel_workers=1)
```

Workers are capped at the number of schemas to extract, so setting a high value on a database with only three schemas is harmless. Errors in one worker are logged and do not abort the others.

---

## Metadata Extraction

The extractor returns a clean Python object hierarchy that can be inspected, serialised, or passed directly to the YAML generator or schema comparator.

```
DatabaseMetadata
└── SchemaMetadata          name
    └── TableMetadata       name, row_count, primary_keys
        └── ColumnMetadata  name, data_type, is_nullable,
                            is_primary_key, is_foreign_key,
                            foreign_key_table, foreign_key_column,
                            default_value, description, ordinal_position
```

Every model has a `.to_dict()` method for JSON serialisation.

Four extraction scopes are supported:

```python
# All schemas (with optional filter + parallel workers)
db_meta = ext.extract_all_schemas(schema_filter=["prefix:stg_"], parallel_workers=8)

# One schema
schema = ext.extract_schema("analytics")

# One table
table = ext.extract_table("analytics", "orders")
```

Server-mode is available for MySQL and ClickHouse: omit `database` from the connection config and `extract_all_schemas` will scan every database on the server, also in parallel.

---

## YAML Generation

Generates dbt-compatible YAML (`version: 2`) from any `DatabaseMetadata` object.

```python
from data_dictionary_builder import YAMLGenerator

gen = YAMLGenerator(output_dir="./dbt/models")

# One file per schema: public.yml, analytics.yml, …
gen.generate_yaml_files(db_meta)

# Or everything in one file
gen.generate_single_yaml(db_meta, filename="all_models.yml")
```

- **Smart merge** — re-running on an existing file preserves custom descriptions, dbt tests, and `meta` blocks. Only new tables/columns are added; changed types are updated.
- Primary key columns automatically get `unique` and `not_null` tests. Non-nullable columns get `not_null`.
- FK relationships, row counts, and table type are written into `meta` blocks.

---

## Schema Comparison

Compare any source schema against any destination schema, across any combination of supported databases.

```python
from data_dictionary_builder import SchemaComparator

with tempfile.TemporaryDirectory() as tmpdir:
    comparator = SchemaComparator(
        source_config={"db_type": "postgres", ...},
        destination_config={"db_type": "mysql", ...},
        yaml_output_dir=tmpdir,
    )
    report = comparator.compare_and_generate_report(
        source_schema_name="public",
        destination_schema_name="app_db",
        include_yaml_gaps=True,
    )

print(report["summary"])
# {missing_tables_count, missing_columns_count, type_mismatches_count, …}
```

- Detects missing tables, missing columns, and data type mismatches.
- Normalises type aliases across databases (`character varying` = `varchar`, `int4` = `integer`, etc.).
- Returns a structured dict with `summary` and `comparison` keys, suitable for XCom in Airflow.

---

## Documentation Gap Detection

```python
gen = YAMLGenerator(output_dir=tmpdir)

tables_missing = gen.get_tables_without_descriptions(db_meta)
cols_missing   = gen.get_columns_without_descriptions(db_meta)

total = sum(len(s.tables) for s in db_meta.schemas)
pct   = 100 * (total - len(tables_missing)) / max(total, 1)
print(f"Table documentation: {pct:.0f}%")
```

Results are structured (`schema.table` / `schema.table.column`) and easy to pipe into a report or email.

---

## Email Reporting

Send comparison reports via any standard SMTP server.

```python
from data_dictionary_builder.notifications.email_sender import EmailSender

sender = EmailSender(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    sender_email="ci@company.com",
    sender_password=os.getenv("SMTP_PASSWORD"),
    use_tls=True,
)
sender.send_comparison_report(
    recipient_emails=["data-team@company.com"],
    report=report,
    subject="Nightly schema drift report",
)
```

HTML and plain-text parts are both included. Compatible with Gmail App Passwords, Office 365, SendGrid, AWS SES, and any SMTP relay.

---

## Airflow Integration

All classes are stateless and context-manager safe, designed for production orchestration.

```python
@task
def extract():
    with MetadataExtractor(**config) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["prefix:stg_"], parallel_workers=8)
    return db_meta.to_dict()

@task
def generate_yaml(db_meta_dict):
    db_meta = DatabaseMetadata.from_dict(db_meta_dict)
    YAMLGenerator(output_dir="/tmp/dbt").generate_yaml_files(db_meta)

@task
def send_report(report):
    EmailSender(...).send_comparison_report(["team@company.com"], report)
```

Credentials can be managed through Airflow Variables and Connections. Return values from each task are XCom-compatible.

---

## CLI

```bash
data-dictionary extract --db-type postgres --host localhost --user readonly
data-dictionary info
data-dictionary --version
```

---

## Installation

```bash
pip install -r requirements.txt
pip install -e .
```

Requires Python 3.8 or higher.
