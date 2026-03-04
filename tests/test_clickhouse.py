"""
test_clickhouse.py
==================
Exercises every major feature of data_dictionary_builder against a live
ClickHouse instance, including all schema-filter strategies now built into
MetadataExtractor.extract_all_schemas().

Schema-filter formats demonstrated
------------------------------------
  Exact name    schema_filter=["default"]
  Glob/LIKE     schema_filter=["default%"]  or  "monkeybook_%"
  prefix:       schema_filter=["prefix:stg_"]
  suffix:       schema_filter=["suffix:_prod"]
  contains:     schema_filter=["contains:analytics"]
  regex:        schema_filter=["regex:^tmp_\\d+$"]
  Mixed list    any combination of the above in one call

Configuration – set these environment variables (or a .env file):
    clickhouse_host      default: localhost
    clickhouse_port      default: 9440
    clickhouse_user
    clickhouse_password
    clickhouse_db        optional – omit to scan all databases on the server

Optional email:
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import os
import json
import tempfile

from dotenv import load_dotenv

from data_dictionary_builder import MetadataExtractor, YAMLGenerator, SchemaComparator
from data_dictionary_builder.notifications.email_sender import EmailSender

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Connection config
# ─────────────────────────────────────────────────────────────────────────────

BASE_CONFIG = {
    "db_type":  "clickhouse",
    "host":     os.getenv("clickhouse_host", "localhost"),
    "port":     int(os.getenv("clickhouse_port", 9440)),
    "user":     os.getenv("clickhouse_user", "default"),
    "password": os.getenv("clickhouse_password", ""),
    "secure":   True,
    "verify":   False,
}

if os.getenv("clickhouse_db"):
    BASE_CONFIG["database"] = os.getenv("clickhouse_db")

# Primary schemas used for extraction and comparison steps
TARGET_SCHEMAS = ["default", "system"]


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Connection test
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    extractor = MetadataExtractor(**BASE_CONFIG)
    ok = extractor.test_connection()
    print(f"  Connection successful: {ok}")
    assert ok, "❌  Could not connect to ClickHouse – check env vars"
    print("  ✓ Connected")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Schema listing
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_listing():
    section("2. Schema / Database Listing")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Available schemas: {schemas}")
    assert isinstance(schemas, list)
    print(f"  ✓ Found {len(schemas)} schema(s)")
    return schemas


# ─────────────────────────────────────────────────────────────────────────────
# 3. Schema-filter strategies
#
# All entries are passed directly to extract_all_schemas(schema_filter=[...]).
# The extractor fetches the real schema list from the DB first, then resolves
# each entry against it — no pre-fetching or separate filter step needed.
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_filter_strategies():
    section("3. Schema-Filter Strategies")

    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas on server: {live}\n")

    cases = [
        # (label, schema_filter)
        ("3a. Exact names  (original behaviour)",
         ["default", "system"]),

        ("3b. Glob / SQL-LIKE  (default% matches 'default')",
         ["default%"]),

        ("3c. prefix: marker  — anything starting with 'def'",
         ["prefix:def"]),

        ("3d. suffix: marker  — anything ending with 'ault'",
         ["suffix:ault"]),

        ("3e. contains: marker  — anything containing 'sys'",
         ["contains:sys"]),

        ("3f. regex: marker  — full match ^def.*$",
         ["regex:^def.*$"]),

        ("3g. Mixed list  — exact + prefix + regex in one call",
         ["system", "prefix:def", "regex:^tmp_\\d+$"]),

        ("3h. No filter (None)  — extract everything",
         None),
    ]

    for label, sf in cases:
        with MetadataExtractor(**BASE_CONFIG) as ext:
            db_meta = ext.extract_all_schemas(schema_filter=sf)
        matched = [s.name for s in db_meta.schemas]
        print(f"  {label}")
        print(f"    filter  : {sf}")
        print(f"    matched : {matched}\n")

    print("  ✓ Schema-filter strategies demonstrated")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Full extraction with targeted filter
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_all_schemas():
    section("4. Full Metadata Extraction")

    with MetadataExtractor(**BASE_CONFIG) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)

    print(f"  Database : {db_meta.database_name}")
    print(f"  DB type  : {db_meta.database_type}")
    print(f"  Version  : {db_meta.version}")
    print(f"  Schemas  : {len(db_meta.schemas)}")

    for schema in db_meta.schemas:
        print(f"\n  [{schema.name}]  {len(schema.tables)} table(s)")
        for table in schema.tables[:5]:
            print(f"    • {table.name}  ({len(table.columns)} cols, "
                  f"{table.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables) - 5} more")

    print("\n  ✓ Extraction complete")
    return db_meta


# ─────────────────────────────────────────────────────────────────────────────
# 5. Single schema extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_schema():
    section("5. Extract Single Schema")
    schema_name = TARGET_SCHEMAS[0]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(schema_name)

    print(f"  Schema : {schema.name}")
    print(f"  Tables : {len(schema.tables)}")
    for t in schema.tables[:5]:
        print(f"    • {t.name}")
    print("  ✓ Single-schema extraction OK")
    return schema


# ─────────────────────────────────────────────────────────────────────────────
# 6. Single table extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_table(schema):
    section("6. Extract Single Table")
    if not schema.tables:
        print("  ⚠  No tables in schema – skipping")
        return

    table_name  = schema.tables[0].name
    schema_name = TARGET_SCHEMAS[0]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        table = ext.extract_table(schema_name, table_name)

    print(f"  Table      : {table.schema_name}.{table.name}")
    print(f"  Row count  : {table.row_count}")
    print(f"  Primary key: {table.primary_keys}")
    print(f"  Columns    : {len(table.columns)}")
    for col in table.columns[:8]:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type} {nullable}{pk}")
    if len(table.columns) > 8:
        print(f"    … and {len(table.columns) - 8} more")
    print("  ✓ Single-table extraction OK")


# ─────────────────────────────────────────────────────────────────────────────
# 7. YAML generation – per-schema
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_per_schema(db_meta):
    section("7. YAML Generation – Per-Schema Files")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen   = YAMLGenerator(output_dir=tmpdir)
        files = gen.generate_yaml_files(db_meta)
        print(f"  Generated {len(files)} YAML file(s):")
        for f in files:
            print(f"    • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML generation OK")


# ─────────────────────────────────────────────────────────────────────────────
# 8. YAML generation – single combined file
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_combined(db_meta):
    section("8. YAML Generation – Single Combined File")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen      = YAMLGenerator(output_dir=tmpdir)
        filepath = gen.generate_single_yaml(db_meta, filename="all_models.yml")
        print(f"  File: {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML generation OK")


# ─────────────────────────────────────────────────────────────────────────────
# 9. Documentation gap detection
# ─────────────────────────────────────────────────────────────────────────────

def test_documentation_gaps(db_meta):
    section("9. Documentation Gap Detection")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen            = YAMLGenerator(output_dir=tmpdir)
        tables_no_desc = gen.get_tables_without_descriptions(db_meta)
        cols_no_desc   = gen.get_columns_without_descriptions(db_meta)

    total_tables = sum(len(s.tables) for s in db_meta.schemas)
    total_cols   = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
    tbl_pct = 100 * (total_tables - len(tables_no_desc)) / max(total_tables, 1)
    col_pct = 100 * (total_cols   - len(cols_no_desc))   / max(total_cols, 1)

    print(f"  Tables  : {total_tables} total | "
          f"{len(tables_no_desc)} missing descriptions ({tbl_pct:.0f}% documented)")
    print(f"  Columns : {total_cols} total | "
          f"{len(cols_no_desc)} missing descriptions ({col_pct:.0f}% documented)")
    if tables_no_desc:
        print("  Sample tables needing descriptions (first 5):")
        for t in tables_no_desc[:5]:
            print(f"    • {t}")
    if cols_no_desc:
        print("  Sample columns needing descriptions (first 5):")
        for c in cols_no_desc[:5]:
            print(f"    • {c['schema']}.{c['table']}.{c['column']}")
    print("  ✓ Gap detection OK")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Schema comparison
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_comparison():
    section("10. Schema Comparison")

    if len(TARGET_SCHEMAS) < 2:
        print("  ⚠  Need at least 2 schemas in TARGET_SCHEMAS – skipping")
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        comparator = SchemaComparator(
            source_config=BASE_CONFIG,
            destination_config=BASE_CONFIG,
            yaml_output_dir=tmpdir,
        )
        report = comparator.compare_and_generate_report(
            source_schema_name=TARGET_SCHEMAS[0],
            destination_schema_name=TARGET_SCHEMAS[1],
            include_yaml_gaps=True,
        )

    s = report["summary"]
    print(f"  Missing tables  : {s['missing_tables_count']}")
    print(f"  Missing columns : {s['missing_columns_count']}")
    print(f"  Type mismatches : {s['type_mismatches_count']}")
    print(f"  Tables w/o desc : {s.get('tables_without_descriptions_count', 'n/a')}")
    print(f"  Cols w/o desc   : {s.get('columns_without_descriptions_count', 'n/a')}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(report, fh, indent=2, default=str)
        print(f"\n  Full report saved to: {fh.name}")

    print("  ✓ Schema comparison OK")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# 11. Email report (optional)
# ─────────────────────────────────────────────────────────────────────────────

def test_email_report(report):
    section("11. Email Report  (optional)")
    smtp_host = os.getenv("SMTP_HOST")
    email_to  = os.getenv("EMAIL_TO")

    if not smtp_host or not email_to or report is None:
        print("  ⚠  SMTP_HOST / EMAIL_TO not set – skipping email test")
        return

    sender = EmailSender(
        smtp_host=smtp_host,
        smtp_port=int(os.getenv("SMTP_PORT", 587)),
        sender_email=os.getenv("SMTP_USER", ""),
        sender_password=os.getenv("SMTP_PASSWORD"),
        use_tls=True,
    )
    ok = sender.send_comparison_report(
        recipient_emails=[email_to],
        report=report,
        subject="[ClickHouse Test] Schema Comparison Report",
    )
    print(f"  Email sent: {ok}")
    print("  ✓ Email test complete")


# ─────────────────────────────────────────────────────────────────────────────
# 12. Metadata export to JSON
# ─────────────────────────────────────────────────────────────────────────────

def test_metadata_export(db_meta):
    section("12. Metadata Export to JSON")
    data = db_meta.to_dict()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(data, fh, indent=2, default=str)
        print(f"  Exported to: {fh.name}")
        print(f"  Schemas in export: {len(data['schemas'])}")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "🔷 " * 30)
    print("  data_dictionary_builder — ClickHouse full feature test")
    print("🔷 " * 30)

    test_connection()
    test_schema_listing()
    test_schema_filter_strategies()

    db_meta = test_extract_all_schemas()
    schema  = test_extract_single_schema()

    test_extract_single_table(schema)
    test_yaml_per_schema(db_meta)
    test_yaml_combined(db_meta)
    test_documentation_gaps(db_meta)

    report = test_schema_comparison()
    test_email_report(report)
    test_metadata_export(db_meta)

    print("\n" + "✅ " * 30)
    print("  All ClickHouse feature tests passed!")
    print("✅ " * 30 + "\n")