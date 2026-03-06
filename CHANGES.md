# CHANGES.md — data_dictionary_builder

This document records all changes implemented across two development sessions,
covering new features, behavioral improvements, and performance optimisations.

---

## Table of Contents

1. [New Features](#1-new-features)
2. [Test Improvements](#2-test-improvements)
3. [Performance Optimisations](#3-performance-optimisations)
4. [Bug Fixes](#4-bug-fixes)
5. [Feature Suggestions for Future Releases](#5-feature-suggestions-for-future-releases)
6. [Files Changed](#6-files-changed)

---

## 1. New Features

### 1.1 `ExecutionTimer` — Task-level performance timing

**File:** `src/data_dictionary_builder/timer.py` *(new)*
**Exported from:** `src/data_dictionary_builder/__init__.py`

A lightweight timing utility that measures wall-clock duration for named tasks
and prints a formatted summary table.

```python
from data_dictionary_builder import ExecutionTimer

timer = ExecutionTimer()

with timer.task("Extract metadata"):
    db_meta = ext.extract_all_schemas(parallel_workers=8)

with timer.task("Generate YAML"):
    gen.generate_yaml_files(db_meta)

timer.summary()
# ────────────────────────────────────────────
#   Execution Summary
# ────────────────────────────────────────────
#   Extract metadata                  2.341s
#   Generate YAML                     0.087s
# ────────────────────────────────────────────
#   TOTAL                             2.428s
# ────────────────────────────────────────────
```

**API:**
| Member | Description |
|---|---|
| `task(name)` | Context manager; records wall-clock time for a named block |
| `elapsed` | Property — total seconds since construction |
| `totals()` | Returns `([(name, secs), …], overall_secs)` |
| `summary(title=…)` | Prints formatted table; durations auto-format as `s` / `m s` / `h m s` |

---

### 1.2 `DDHelper.send_report_email` — SMTP env-var fallback

**File:** `src/data_dictionary_builder/DDHelper.py`

`send_report_email()` now reads all SMTP parameters from environment variables
when they are not supplied as keyword arguments, making credential-free code
possible.

| Parameter | Env var | Default |
|---|---|---|
| `smtp_host` | `SMTP_HOST` | — |
| `smtp_port` | `SMTP_PORT` | `587` |
| `smtp_user` | `SMTP_USER` | `""` |
| `smtp_password` | `SMTP_PASSWORD` | — |
| `email_to` | `EMAIL_TO` | — |

The method silently skips sending (returns `False`) when `SMTP_HOST` or the
recipient address is absent, so code that runs in environments without email
configured will not fail.

---

### 1.3 `SchemaComparator` — reingested destination support

**File:** `src/data_dictionary_builder/comparison/comparator.py`

Two optional keyword parameters were added to `compare_schemas()` and
`compare_and_generate_report()`:

| Parameter | Effect |
|---|---|
| `dest_db_metadata: DatabaseMetadata` | Destination DB is **not** queried; the matching schema is looked up in this pre-extracted object |
| `source_db_metadata: DatabaseMetadata` | Source DB is **not** re-queried for YAML gap detection; uses the pre-extracted object |

Both default to `None` for full backwards compatibility. When both are
supplied, the comparison makes **zero additional database connections**,
reusing metadata already in memory from an earlier extraction step.

```python
# Step 4 — extract once
with MetadataExtractor(**DEST_CONFIG) as ext:
    dest_db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)

# Step 10 — compare without re-querying the destination
report = comparator.compare_and_generate_report(
    source_schema_name="public",
    dest_db_metadata=dest_db_meta,     # ← reingested; no DB round-trip
    source_db_metadata=dest_db_meta,   # ← reingested for YAML gaps too
)
```

---

### 1.4 `DatabaseMetadata.from_dict()` — deserialisation

**File:** `src/data_dictionary_builder/metadata/models.py`

`DatabaseMetadata.from_dict(data)` is the inverse of `to_dict()`. It fully
reconstructs the object hierarchy (`DatabaseMetadata` → `SchemaMetadata` →
`TableMetadata` → `ColumnMetadata`) from a plain dictionary, enabling
round-trip serialisation for Airflow XCom and JSON file storage.

```python
# Airflow task 1 — serialise for XCom
return db_meta.to_dict()

# Airflow task 2 — deserialise
from data_dictionary_builder import DatabaseMetadata
db_meta = DatabaseMetadata.from_dict(ti.xcom_pull(task_ids="extract"))
```

---

## 2. Test Improvements

### 2.1 `tests/test_clickhouse.py` — comprehensive rework

#### Dynamic `TARGET_SCHEMAS`

The hardcoded `TARGET_SCHEMAS = ["default", "system"]` was removed.
`test_schema_filter_strategies()` (step 3) now returns the **live schema list**
directly from the destination database. In `__main__` this list is assigned:

```python
TARGET_SCHEMAS = test_schema_filter_strategies()
```

The full test suite therefore always operates on whatever schemas actually exist
in the connected instance — no manual updates needed when connecting to a
different ClickHouse cluster.

#### Separate `SOURCE_CONFIG` / `DEST_CONFIG`

A single `BASE_CONFIG` dict was replaced with two independently configurable
connection configs. Each defaults to the shared `clickhouse_*` environment
variables but can be overridden with dedicated prefixed vars:

| Purpose | Env var prefix | Example |
|---|---|---|
| Source | `SOURCE_CLICKHOUSE_*` | `SOURCE_CLICKHOUSE_HOST=prod-ch.example.com` |
| Destination | `DEST_CLICKHOUSE_*` | `DEST_CLICKHOUSE_HOST=staging-ch.example.com` |

Step 1 (`test_connection`) now tests **both** connections independently.

#### Destination reingested in comparisons

Step 10 (`test_schema_comparison`) no longer re-queries the destination
database. It iterates over every schema in `TARGET_SCHEMAS` and calls:

```python
comparator.compare_and_generate_report(
    source_schema_name=schema_name,
    dest_db_metadata=dest_db_meta,     # from step 4
    source_db_metadata=dest_db_meta,   # from step 4
)
```

Individual per-schema results are merged into one combined report with a
`schemas_compared` summary block before being saved as JSON.

#### Timer integration

Every test step is wrapped in `with timer.task("N. Step name"):` and
`timer.summary()` is called at the end:

```
────────────────────────────────────────────────────────────────
  ClickHouse Test Suite — Execution Summary
────────────────────────────────────────────────────────────────
  1. Connection test                                    0.312s
  2. Schema listing                                     0.198s
  3. Schema filter strategies                           1.043s
  4. Full metadata extraction (destination snapshot)    2.871s
  ...
────────────────────────────────────────────────────────────────
  TOTAL                                                12.450s
────────────────────────────────────────────────────────────────
```

#### Email to fixed recipient

`test_email_report` explicitly passes `email_to="j_oyin@yahoo.com"` and reads
SMTP credentials (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`) from
the `.env` file via `os.getenv`.

#### `helper` scope bug fixed

`helper` was previously referenced as a global inside `test_schema_comparison`,
`test_email_report`, and `test_metadata_export` but only created inside
`if __name__ == "__main__":`. All three functions now receive `helper` as an
explicit parameter.

---

## 3. Performance Optimisations

### 3.1 ClickHouse — bulk schema extraction (3N → 2 queries)

**File:** `src/data_dictionary_builder/connectors/clickhouse_connector.py`

`ClickHouseConnector` now overrides `extract_schema_metadata()` with a
bulk implementation.

| Approach | Queries for N tables |
|---|---|
| **Before** (base-class default) | 3N (1× `system.tables` metadata + 1× `system.tables` PKs + 1× `system.columns`, all per table) |
| **After** (bulk override) | **2** (1 `system.tables` query covering all tables + 1 `system.columns` query covering all tables) |

For a schema with 100 tables: **300 → 2 queries (-99.3%)**.

All table assembly (PKs, column type parsing, nullable detection) is done
in Python from the two in-memory result sets.

---

### 3.2 PostgreSQL — bulk schema extraction (4N → 5 queries)

**File:** `src/data_dictionary_builder/connectors/postgres_connector.py`

`PostgresConnector` now overrides `extract_schema_metadata()` with a bulk
implementation that issues one query per metadata type across the **entire
schema** rather than one per table.

| Query | Covers |
|---|---|
| 1 | All base tables + `pg_class` descriptions |
| 2 | All columns (`information_schema.columns`) |
| 3 | All primary keys (`pg_index`) |
| 4 | All foreign keys (`information_schema`) |
| 5 | All approximate row counts (`pg_class.reltuples`) |

| Approach | Queries for N tables |
|---|---|
| **Before** | 4N (columns + PKs + FKs + row count per table) |
| **After** | **5** (one per metadata type across all tables) |

For a schema with 100 tables: **400 → 5 queries (-98.75%)**.

---

### 3.3 Schema filter — pre-compiled patterns

**File:** `src/data_dictionary_builder/metadata/extractor.py`

`_resolve_schema_filter()` previously compiled regex patterns and transformed
glob strings on **every iteration** of the inner schema-matching loop.
The refactored implementation:

1. Walks the filter list **once**, compiling all patterns into a `compiled`
   list of `(kind, value)` tuples.
2. The inner loop over `available_schemas` only matches — no allocation,
   no `re.compile`, no string manipulation on each pass.

For F filter entries and S available schemas, compilation work drops from
O(F × S) to O(F + S).

---

### 3.4 Comparator — class-level type mapping constant

**File:** `src/data_dictionary_builder/comparison/comparator.py`

`_normalize_data_type()` previously constructed a new `type_mappings` dict on
every single call. The dict is now a class-level constant `_TYPE_MAPPINGS`,
built once at class load time and shared by all instances and all invocations.

For a comparison of two schemas each with 500 columns, this eliminates
~1000 unnecessary dict constructions per comparison run.

---

## 4. Bug Fixes

### 4.1 `helper` global scope in ClickHouse test

`test_schema_comparison`, `test_email_report`, and `test_metadata_export`
referenced `helper` as an implicit global. Since `helper` was only created
inside `if __name__ == "__main__":`, calling these functions from `pytest`
(or any other entry point that doesn't run `__main__`) would raise
`NameError: name 'helper' is not defined`. Fixed by passing `helper` as an
explicit parameter to all three functions.

---

## 5. Feature Suggestions for Future Releases

The following features would significantly expand the library's value and
adoption, moving it toward a category-defining tool for data engineers.

---

### 5.1 Column-level statistics & data profiling

Extend `TableMetadata` and each connector to optionally capture per-column
statistics: null percentage, distinct count, min/max/avg/median for numeric
types, and top-N most frequent values for low-cardinality columns.

This turns the library from a pure schema catalogue into a **data quality +
catalogue** tool — a major differentiator against alternatives like
`sqlglot` or raw dbt metadata.

```python
with MetadataExtractor(**config) as ext:
    db_meta = ext.extract_all_schemas(include_statistics=True)

# db_meta.schemas[0].tables[0].columns[0].stats.null_pct  → 0.04
# db_meta.schemas[0].tables[0].columns[0].stats.distinct  → 1842
```

---

### 5.2 Schema drift alerting (Slack / webhook / PagerDuty)

Automatically diff today's extraction against a stored previous snapshot and
dispatch alerts when tables or columns appear, disappear, or change type.
Currently only email is supported; adding webhook and Slack delivery would
allow integration with every modern incident-management workflow without
requiring email infrastructure.

```python
drift = DriftDetector(baseline=yesterday_db_meta, current=today_db_meta)
if drift.has_breaking_changes():
    drift.notify(channel="slack", webhook_url="https://hooks.slack.com/...")
```

---

### 5.3 Interactive HTML data catalog

Generate a self-contained, single-file HTML dashboard with a collapsible
tree (`database → schema → table → column`), full-text search, and column
statistics charts. Analogous to `pandas-profiling` / `ydata-profiling` for
DataFrames.

```python
from data_dictionary_builder import HTMLCatalogGenerator
gen = HTMLCatalogGenerator()
gen.export(db_meta, path="catalog.html")
```

A shareable, zero-dependency HTML file dramatically lowers the barrier for
non-technical stakeholders to explore data assets.

---

### 5.4 LLM-powered auto-descriptions

Integrate with OpenAI, Anthropic, or a local Ollama model to automatically
draft column and table descriptions based on name, data type, sample values,
and surrounding schema context. Users review and accept rather than write
from scratch — reducing documentation effort by 80–90%.

```python
from data_dictionary_builder.ai import AutoDescriber
describer = AutoDescriber(provider="anthropic", model="claude-opus-4-6")
db_meta = describer.enrich(db_meta)
# db_meta.schemas[0].tables[0].description
# → "Stores customer order records with payment and shipping status."
```

---

### 5.5 Schema versioning & snapshot diffing

Store timestamped snapshots of `DatabaseMetadata` as lightweight JSON files
and expose a Git-like diff API over any two snapshots. Teams could answer
questions like *"what changed in the last 30 days?"* without a running
database connection.

```python
registry = SchemaRegistry(path="~/.dd_snapshots/")
registry.save(db_meta)                       # snapshot today

diff = registry.diff(date_a="2026-01-01", date_b="2026-03-01")
diff.added_columns    # → [{"schema": "public", "table": "orders", "column": "refund_amount"}]
diff.removed_tables   # → []
diff.type_changes     # → [...]
```

---

### 5.6 Data catalog integrations

Push extracted metadata to external catalog platforms via their REST APIs:
**DataHub**, **OpenMetadata**, **Alation**, **AWS Glue Data Catalog**,
and **dbt Cloud**. This positions the library as a universal *ingestion adapter*
that any metadata platform can consume.

```python
from data_dictionary_builder.integrations import DataHubPublisher
pub = DataHubPublisher(server="http://datahub:8080", token="...")
pub.publish(db_meta)
```

---

### 5.7 REST API server mode

A `data-dictionary serve` CLI command that starts a lightweight FastAPI
server exposing extraction, comparison, and reporting as HTTP endpoints.
Enables integration with Airflow, Prefect, dbt Cloud webhooks, and custom
dashboards without importing the library directly.

```bash
data-dictionary serve --host 0.0.0.0 --port 8000
# POST /extract   { "db_type": "postgres", "host": "...", ... }
# POST /compare   { "source": {...}, "destination": {...} }
# GET  /report/{id}
```

---

### 5.8 dbt project awareness

Read an existing dbt project's `schema.yml` files and reconcile them with
live database metadata, producing a gap report: which models are documented
vs. undocumented, which columns exist in the DB but not the YAML, and which
YAML entries no longer have a matching DB object.

```python
from data_dictionary_builder.dbt import DbtReconciler
rec = DbtReconciler(dbt_project_dir="./dbt", db_meta=db_meta)
report = rec.reconcile()
# report.undocumented_models  → [...]
# report.stale_yaml_entries   → [...]
```

---

## 6. Files Changed

| File | Change type | Summary |
|---|---|---|
| `src/data_dictionary_builder/timer.py` | **New** | `ExecutionTimer` class |
| `src/data_dictionary_builder/__init__.py` | Modified | Export `ExecutionTimer` |
| `src/data_dictionary_builder/metadata/models.py` | Modified | Add `DatabaseMetadata.from_dict()` |
| `src/data_dictionary_builder/metadata/extractor.py` | Modified | Pre-compile filter patterns in `_resolve_schema_filter` |
| `src/data_dictionary_builder/connectors/clickhouse_connector.py` | Modified | Bulk `extract_schema_metadata` override (3N → 2 queries) |
| `src/data_dictionary_builder/connectors/postgres_connector.py` | Modified | Bulk `extract_schema_metadata` override (4N → 5 queries) |
| `src/data_dictionary_builder/comparison/comparator.py` | Modified | Class-level `_TYPE_MAPPINGS`; `dest_db_metadata` / `source_db_metadata` params |
| `src/data_dictionary_builder/DDHelper.py` | Modified | `send_report_email` env-var fallback for all SMTP params |
| `tests/test_clickhouse.py` | Modified | Dynamic schemas; dual configs; reingested comparison; timer; email; scope fix |
| `DOCUMENTATION.md` | Modified | Full rewrite covering all features |
| `CLAUDE.md` | **New** | Repository guidance for Claude Code |
