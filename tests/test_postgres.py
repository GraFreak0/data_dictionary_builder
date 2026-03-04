"""
test_postgres.py
================
Exercises every major feature of data_dictionary_builder against a live
PostgreSQL instance, including all schema-filter strategies now built into
MetadataExtractor.extract_all_schemas().

Schema-filter formats demonstrated
------------------------------------
  Exact name    schema_filter=["public"]
  Glob/LIKE     schema_filter=["pub%"]  or  "my_app_%"
  prefix:       schema_filter=["prefix:stg_"]
  suffix:       schema_filter=["suffix:_prod"]
  contains:     schema_filter=["contains:analytics"]
  regex:        schema_filter=["regex:^app_\\w+$"]
  Mixed list    any combination of the above in one call

Configuration – set these environment variables (or a .env file):
    PG_HOST       default: localhost
    PG_PORT       default: 5432
    PG_DB         default: postgres
    PG_USER       default: postgres
    PG_PASSWORD
    PG_SCHEMAS    comma-separated list of schemas to use in tests
                  default: public

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
    "db_type":  "postgres",
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "postgres"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

TARGET_SCHEMAS = os.getenv("PG_SCHEMAS", "public").split(",")


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
    assert ok, "❌  Could not connect – check env vars"
    print("  ✓ Connected")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Schema listing
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_listing():
    section("2. Schema Listing")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Available schemas: {schemas}")
    assert isinstance(schemas, list)
    print(f"  ✓ Found {len(schemas)} schema(s)")
    return schemas


# ─────────────────────────────────────────────────────────────────────────────
# 3. Table listing
# ─────────────────────────────────────────────────────────────────────────────

def test_table_listing():
    section("3. Table Listing")
    schema_name = TARGET_SCHEMAS[0]
    with MetadataExtractor(**BASE_CONFIG) as ext:
        tables = ext.get_tables_list(schema_name)
    print(f"  Tables in '{schema_name}': {tables[:10]}")
    print(f"  ✓ Found {len(tables)} table(s)")
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# 4. Schema-filter strategies
#
# Entries are passed directly to extract_all_schemas(schema_filter=[...]).
# The extractor first fetches all schemas from Postgres, then resolves each
# entry against that live list — no separate filtering step required.
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies")

    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas in database: {live}\n")

    cases = [
        # (label, schema_filter)
        ("4a. Exact names  (original behaviour)",
         ["public"]),

        ("4b. Glob / SQL-LIKE  (pub% matches 'public')",
         ["pub%"]),

        ("4c. prefix: marker  — anything starting with 'pub'",
         ["prefix:pub"]),

        ("4d. suffix: marker  — anything ending with 'lic'",
         ["suffix:lic"]),

        ("4e. contains: marker  — anything containing 'pub'",
         ["contains:pub"]),

        ("4f. regex: marker  — full match ^pub.*$",
         ["regex:^pub.*$"]),

        ("4g. Mixed list  — exact + prefix + regex in one call",
         ["public", "prefix:stg_", "regex:^analytics_\\d{4}$"]),

        ("4h. No filter (None)  — extract everything",
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
# 5. Full extraction with targeted filter
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_all_schemas():
    section("5. Full Metadata Extraction")

    with MetadataExtractor(**BASE_CONFIG) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)

    print(f"  Database : {db_meta.database_name}")
    print(f"  DB type  : {db_meta.database_type}")
    print(f"  Version  : {db_meta.version}")

    for schema in db_meta.schemas:
        print(f"\n  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            pk = f"  PK: {t.primary_keys}" if t.primary_keys else ""
            print(f"    • {t.name}  ({len(t.columns)} cols){pk}")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables) - 5} more")

    print("\n  ✓ Extraction OK")
    return db_meta


# ─────────────────────────────────────────────────────────────────────────────
# 6. Single schema extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_schema():
    section("6. Extract Single Schema")
    schema_name = TARGET_SCHEMAS[0]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(schema_name)

    print(f"  Schema : {schema.name}")
    print(f"  Tables : {len(schema.tables)}")
    print("  ✓ Single-schema extraction OK")
    return schema


# ─────────────────────────────────────────────────────────────────────────────
# 7. Single table extraction (PK / FK detail)
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_table(schema):
    section("7. Extract Single Table  (PK / FK detail)")
    if not schema.tables:
        print("  ⚠  No tables – skipping")
        return

    table_name  = schema.tables[0].name
    schema_name = TARGET_SCHEMAS[0]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        table = ext.extract_table(schema_name, table_name)

    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  Primary keys  : {table.primary_keys}")

    fk_cols = [c for c in table.columns if c.is_foreign_key]
    print(f"  Foreign keys  : {len(fk_cols)}")
    for fk in fk_cols:
        print(f"    • {fk.name} → {fk.foreign_key_table}.{fk.foreign_key_column}")

    print(f"  Columns ({len(table.columns)}):")
    for col in table.columns[:10]:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type} {nullable}{pk}")
    print("  ✓ Single-table extraction OK")


# ─────────────────────────────────────────────────────────────────────────────
# 8. YAML generation – per-schema
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_per_schema(db_meta):
    section("8. YAML Generation – Per-Schema Files")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen   = YAMLGenerator(output_dir=tmpdir)
        files = gen.generate_yaml_files(db_meta)
        print(f"  Generated {len(files)} file(s):")
        for f in files:
            print(f"    • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


# ─────────────────────────────────────────────────────────────────────────────
# 9. YAML generation – single combined file
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_combined(db_meta):
    section("9. YAML Generation – Single Combined File")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen      = YAMLGenerator(output_dir=tmpdir)
        filepath = gen.generate_single_yaml(db_meta, filename="all_models.yml")
        print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Documentation gap detection
# ─────────────────────────────────────────────────────────────────────────────

def test_documentation_gaps(db_meta):
    section("10. Documentation Gap Detection")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen            = YAMLGenerator(output_dir=tmpdir)
        tables_no_desc = gen.get_tables_without_descriptions(db_meta)
        cols_no_desc   = gen.get_columns_without_descriptions(db_meta)

    total_tables = sum(len(s.tables) for s in db_meta.schemas)
    total_cols   = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
    tbl_pct      = 100 * (total_tables - len(tables_no_desc)) / max(total_tables, 1)
    col_pct      = 100 * (total_cols   - len(cols_no_desc))   / max(total_cols, 1)

    print(f"  Table documentation  : {tbl_pct:.0f}%  "
          f"({len(tables_no_desc)}/{total_tables} missing)")
    print(f"  Column documentation : {col_pct:.0f}%  "
          f"({len(cols_no_desc)}/{total_cols} missing)")
    if tables_no_desc:
        print("  Sample undocumented tables:")
        for t in tables_no_desc[:5]:
            print(f"    • {t}")
    print("  ✓ Gap detection OK")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Schema comparison
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_comparison():
    section("11. Schema Comparison")
    schema_name = TARGET_SCHEMAS[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        comparator = SchemaComparator(
            source_config=BASE_CONFIG,
            destination_config=BASE_CONFIG,
            yaml_output_dir=tmpdir,
        )
        report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            destination_schema_name=schema_name,
            include_yaml_gaps=True,
        )

    s = report["summary"]
    print(f"  Source / dest schema : {schema_name}")
    print(f"  Missing tables       : {s['missing_tables_count']}")
    print(f"  Missing columns      : {s['missing_columns_count']}")
    print(f"  Type mismatches      : {s['type_mismatches_count']}")
    print(f"  Tables w/o desc      : {s.get('tables_without_descriptions_count', 'n/a')}")
    print(f"  Cols w/o desc        : {s.get('columns_without_descriptions_count', 'n/a')}")
    print("  ✓ Schema comparison OK")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# 12. Email report (optional)
# ─────────────────────────────────────────────────────────────────────────────

def test_email_report(report):
    section("12. Email Report  (optional)")
    smtp_host = os.getenv("SMTP_HOST")
    email_to  = os.getenv("EMAIL_TO")

    if not smtp_host or not email_to:
        print("  ⚠  SMTP_HOST / EMAIL_TO not set – skipping")
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
        subject="[PostgreSQL Test] Schema Comparison Report",
    )
    print(f"  Email sent: {ok}")
    print("  ✓ Email test complete")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Metadata export to JSON
# ─────────────────────────────────────────────────────────────────────────────

def test_metadata_export(db_meta):
    section("13. Metadata Export to JSON")
    data = db_meta.to_dict()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(data, fh, indent=2, default=str)
        print(f"  Saved to: {fh.name}")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "🐘 " * 30)
    print("  data_dictionary_builder — PostgreSQL full feature test")
    print("🐘 " * 30)

    test_connection()
    test_schema_listing()
    test_table_listing()
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
    print("  All PostgreSQL feature tests passed!")
    print("✅ " * 30 + "\n")
