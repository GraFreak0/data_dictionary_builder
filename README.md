# data_dictionary_builder

A Python library that automates database documentation — extract live schema metadata, generate dbt-compatible YAML, compare schemas across environments, and deliver PDF reports, all in a single import.

[![PyPI](https://img.shields.io/pypi/v/data-dictionary-builder)](https://pypi.org/project/data-dictionary-builder/)
[![Python](https://img.shields.io/pypi/pyversions/data-dictionary-builder)](https://pypi.org/project/data-dictionary-builder/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

> **What's new in v0.1.4**
>
> - **Multi-recipient email delivery** — `email_to` now accepts a list of addresses (e.g. `["alice@example.com", "bob@example.com"]`) in addition to a single string. The `EMAIL_TO` environment variable supports comma-separated addresses (`EMAIL_TO=alice@example.com,bob@example.com`). All recipients receive the PDF attachment in one send.
> - **Multi-target Slack delivery** — `slack_target` now accepts a list of targets (e.g. `["#data-alerts", "U012AB3CD"]`). The `SLACK_NOTIFY_TARGET` environment variable supports comma-separated values. Each target receives the full Block Kit report and optional PDF upload independently.
> - **`send_notification` updated** — both `email_to` and `slack_target` parameters accept strings or lists; the method routes correctly for either form with no API change.

---

> **What's new in v0.1.3**
>
> - **Slack notifications** — deliver schema comparison reports directly to any Slack channel or DM alongside the existing email delivery. Use `notification_type="slack"` or `"both"` in `send_notification()`. Requires a Bot User OAuth Token (`xoxb-…`) and the `slack` extra: `pip install "data-dictionary-builder[slack]"`.
> - **Optimised metadata generation** — parallel extraction is faster with improved thread scheduling and reduced connection overhead across all supported databases.
> - **Smarter schema comparison** — type normalisation has been expanded to cover more cross-database equivalences, reducing false-positive mismatches in mixed-engine pipelines.
> - **Leaner exports** — JSON metadata exports are more compact, and the `to_dict()` / `from_dict()` round-trip is validated automatically to ensure safe use in Airflow XCom and downstream catalog APIs.

---

## Installation

**pip**

```bash
# Core library (SQLite works out of the box)
pip install data-dictionary-builder

# With the connectors you need
pip install "data-dictionary-builder[postgres]"
pip install "data-dictionary-builder[mysql]"
pip install "data-dictionary-builder[clickhouse]"         # ClickHouse HTTP/HTTPS
pip install "data-dictionary-builder[clickhouse-native]"  # ClickHouse native TCP
pip install "data-dictionary-builder[spanner]"
pip install "data-dictionary-builder[oracle]"
pip install "data-dictionary-builder[sqlserver]"

# Everything at once
pip install "data-dictionary-builder[all]"
```

**uv** *(recommended — faster resolver, built-in virtual environments)*

```bash
# Install uv
pip install uv
# or on macOS/Linux: curl -LsSf https://astral.sh/uv/install.sh | sh

# Add to your project
uv add data-dictionary-builder

# With specific connectors
uv add "data-dictionary-builder[postgres]"
uv add "data-dictionary-builder[mysql]"
uv add "data-dictionary-builder[clickhouse]"         # ClickHouse HTTP/HTTPS
uv add "data-dictionary-builder[clickhouse-native]"  # ClickHouse native TCP
uv add "data-dictionary-builder[oracle]"
uv add "data-dictionary-builder[sqlserver]"
uv add "data-dictionary-builder[spanner]"

# Everything at once
uv add "data-dictionary-builder[all]"
```

Or use the CLI to install connectors after the fact:

```bash
ddgen install postgres
ddgen install clickhouse
ddgen install oracle
ddgen install sqlserver
ddgen install all
```

---

## Supported Databases

| Database | Extra | Driver |
|---|---|---|
| **SQLite** | *(built-in)* | `sqlite3` (stdlib) |
| **PostgreSQL** | `[postgres]` | `psycopg2-binary` |
| **MySQL / MariaDB** | `[mysql]` | `PyMySQL` |
| **ClickHouse** | `[clickhouse]` | `clickhouse-connect` (HTTP/HTTPS) · `clickhouse-driver` (native TCP, optional) |
| **Oracle Database** | `[oracle]` | `oracledb` (thin mode — no Oracle Client needed) |
| **SQL Server / Azure SQL** | `[sqlserver]` | `pymssql` |
| **Google Cloud Spanner** | `[spanner]` | `google-cloud-spanner` |

---

## Quick Start

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator, DDHelper, ExecutionTimer

timer  = ExecutionTimer()
helper = DDHelper(".")      # creates models/, reports/json/, reports/pdf/

with timer.task("Extract"):
    with MetadataExtractor(
        db_type="postgres", host="localhost", port=5432,
        database="mydb", user="readonly", password="secret",
    ) as ext:
        db_meta = ext.extract_all_schemas(
            schema_filter=["public", "analytics"],
            parallel_workers=8,
        )

with timer.task("Generate YAML"):
    YAMLGenerator(output_dir=str(helper.models_dir)).generate_yaml_files(db_meta)

timer.summary()
```

---

## CLI

```bash
# Show all commands and supported databases
ddgen --help

# Full module and API reference
ddgen features

# Check which connectors are installed
ddgen connectors

# Install a connector
ddgen install postgres
ddgen install clickhouse
ddgen install all

# Extract metadata and generate YAML in one step
ddgen extract --db-type postgres --host prod.db.io --database mydb --user readonly

# Compare two environments
ddgen compare --source-host prod.db.io --dest-host staging.db.io --source-database mydb

# Show library version and connector summary
ddgen info

# Show version number
ddgen --version
```

---

## Schema Comparison

```python
from data_dictionary_builder import SchemaComparator, DDHelper

helper = DDHelper(".")
report = SchemaComparator(
    source_config={"db_type": "postgres", "host": "prod-db", ...},
    destination_config={"db_type": "postgres", "host": "staging-db", ...},
).compare_and_generate_report("public", include_yaml_gaps=True)

json_path = helper.save_report(report)
pdf_path  = helper.compile_pdf(source_json=json_path)

# Send via email, Slack, or both — credentials fall back to env vars
# email_to and slack_target accept a string or a list of recipients
helper.send_notification(
    notification_type="both",    # "email" | "slack" | "both"
    report=report,
    pdf_path=pdf_path,
    email_to=["alice@example.com", "bob@example.com"],
    slack_target=["#data-alerts", "#data-eng"],
)
```

---

## Airflow Integration

`DatabaseMetadata` serialises to/from plain dicts for XCom:

```python
@task
def extract():
    with MetadataExtractor(**config) as ext:
        return ext.extract_all_schemas(parallel_workers=8).to_dict()

@task
def generate_yaml(db_meta_dict):
    from data_dictionary_builder import DatabaseMetadata, YAMLGenerator
    YAMLGenerator("./models").generate_yaml_files(DatabaseMetadata.from_dict(db_meta_dict))
```

See [`tests/airflow_dag_example.py`](tests/airflow_dag_example.py) for a complete DAG.

---

## Key Features

- **Parallel extraction** — `ThreadPoolExecutor` with configurable workers; ClickHouse uses 2 queries and PostgreSQL uses 5 queries per schema regardless of table count
- **Dual ClickHouse transport** — HTTP/HTTPS via `clickhouse-connect` (default) or native TCP via `clickhouse-driver`; auto-detected, with dynamic port defaults based on transport and TLS
- **Schema filtering** — exact, glob, prefix, suffix, contains, regex — mix freely
- **Smart YAML merge** — re-running never overwrites descriptions you've written by hand
- **YAML-aware gap detection** — documentation coverage checks read from your existing YAML files, so descriptions you've added are always recognised
- **Cross-database comparison** — compare any two database types; type aliases normalised before diffing
- **PDF reports** — paginated, no row limits, table of contents (requires `reportlab`)
- **Unified notifications** — `send_notification(notification_type="email"|"slack"|"both", ...)` delivers PDF reports via SMTP email, Slack (channel or DM), or both simultaneously; all credentials fall back to environment variables
- **Email delivery** — SMTP with env-var credential fallback; PDF attached automatically
- **Slack delivery** — Block Kit–formatted comparison summaries; optional PDF file upload; supports `#channel`, `@user`, channel IDs, and user IDs
- **ExecutionTimer** — named task timing with a formatted summary table
- **Server mode** — omit `database` to scan all databases on a MySQL, ClickHouse, or PostgreSQL server
- **Rich CLI** — `ddgen extract`, `ddgen compare`, `ddgen features` (full API reference), `ddgen connectors`, `ddgen install`

---

## Environment Variables

Set these in a `.env` file (see `tests/.env.example`) or in your shell:

```bash
# ── Notification channel ───────────────────────────────────────────────
# "email" | "slack" | "both"  (default: email)
NOTIFICATION_TYPE=email

# ── SMTP — DDHelper.send_notification() / send_report_email() ──────────
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=xxxx xxxx xxxx xxxx
EMAIL_TO=alice@example.com,bob@example.com   # comma-separated for multiple recipients

# ── Slack — DDHelper.send_notification() ───────────────────────────────
# Bot Token Scopes required: chat:write, files:write, channels:read,
#                            users:read, im:write
SLACK_BOT_TOKEN=xoxb-your-token-here
SLACK_NOTIFY_TARGET=#data-alerts,#data-eng   # comma-separated for multiple targets
```

---

## Documentation

Full user guide, API reference, and troubleshooting: [DOCUMENTATION.md](https://github.com/GraFreak0/data_dictionary_builder/blob/main/DOCUMENTATION.md)

---

## License

[MIT](https://github.com/GraFreak0/data_dictionary_builder/blob/main/LICENSE) — free to use, modify, and distribute in personal and commercial projects.
