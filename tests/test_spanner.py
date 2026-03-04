"""
test_spanner.py
===============
Exercises every major feature of data_dictionary_builder against a live
Google Cloud Spanner instance, including all schema-filter strategies now
built into MetadataExtractor.extract_all_schemas().

Schema-filter formats demonstrated
------------------------------------
  Exact name    schema_filter=["public"]
  Glob/LIKE     schema_filter=["pub%"]
  prefix:       schema_filter=["prefix:pub"]
  suffix:       schema_filter=["suffix:lic"]
  contains:     schema_filter=["contains:pub"]
  regex:        schema_filter=["regex:^pub.*$"]
  Mixed list    any combination of the above in one call

Spanner always returns a single "public" schema, so the filter demos
confirm the formats work against that real schema name.

Configuration – set these environment variables (or a .env file):
    SPANNER_INSTANCE    e.g. my-instance
    SPANNER_DATABASE    e.g. my-database
    SPANNER_PROJECT     e.g. my-gcp-project   (optional if ADC configured)
    GOOGLE_APPLICATION_CREDENTIALS  path to service-account JSON key

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
    "db_type":     "spanner",
    "instance_id": os.getenv("SPANNER_INSTANCE", ""),
    "database_id": os.getenv("SPANNER_DATABASE", ""),
}

if os.getenv("SPANNER_PROJECT"):
    BASE_CONFIG["project_id"] = os.getenv("SPANNER_PROJECT")

TARGET_SCHEMA = "public"


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


def _skip_if_not_configured() -> bool:
    if not BASE_CONFIG["instance_id"] or not BASE_CONFIG["database_id"]:
        print("  ⚠  SPANNER_INSTANCE / SPANNER_DATABASE not set – skipping")
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# 1. Connection test
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    if _skip_if_not_configured():
        return False

    ok = MetadataExtractor(**BASE_CONFIG).test_connection()
    print(f"  Connection successful: {ok}")
    assert ok, "❌  Could not connect to Spanner – check env vars / credentials"
    print("  ✓ Connected")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 2. Schema listing  (Spanner always returns ['public'])
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_listing():
    section("2. Schema Listing")
    if _skip_if_not_configured():
        return ["public"]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        schemas = ext.get_schemas_list()

    print(f"  Schemas: {schemas}")
    assert schemas == ["public"], "Spanner should return a single 'public' schema"
    print("  ✓ Schema listing OK")
    return schemas


# ─────────────────────────────────────────────────────────────────────────────
# 3. Table listing
# ─────────────────────────────────────────────────────────────────────────────

def test_table_listing():
    section("3. Table Listing")
    if _skip_if_not_configured():
        return []

    with MetadataExtractor(**BASE_CONFIG) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMA)

    print(f"  Tables ({len(tables)}): {tables[:10]}")
    print(f"  ✓ Found {len(tables)} table(s)")
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# 4. Schema-filter strategies
#
# Entries are passed directly to extract_all_schemas(schema_filter=[...]).
# The extractor fetches Spanner's schema list first ("public"), then resolves
# each entry against it.
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies")

    if _skip_if_not_configured():
        return

    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")

    cases = [
        # (label, schema_filter, should_match_public)
        ("4a. Exact name  (original behaviour)",
         ["public"], True),

        ("4b. Glob / SQL-LIKE  (pub% matches 'public')",
         ["pub%"], True),

        ("4c. prefix: marker  — anything starting with 'pub'",
         ["prefix:pub"], True),

        ("4d. suffix: marker  — anything ending with 'lic'",
         ["suffix:lic"], True),

        ("4e. contains: marker  — anything containing 'pub'",
         ["contains:pub"], True),

        ("4f. regex: marker  — full match ^pub.*$",
         ["regex:^pub.*$"], True),

        ("4g. Mixed list  — exact + prefix + regex in one call",
         ["public", "prefix:stg_", "regex:^analytics_\\d{4}$"], True),

        ("4h. No filter (None)  — extract everything",
         None, True),

        ("4i. Non-matching filter  — no schemas returned",
         ["prefix:stg_"], False),
    ]

    for label, sf, expect_public in cases:
        with MetadataExtractor(**BASE_CONFIG) as ext:
            db_meta = ext.extract_all_schemas(schema_filter=sf)
        matched = [s.name for s in db_meta.schemas]
        status = "✓" if (("public" in matched) == expect_public) else "✗"
        print(f"  {status} {label}")
        print(f"      filter  : {sf}")
        print(f"      matched : {matched}\n")

    print("  ✓ Schema-filter strategies demonstrated")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Full extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_all_schemas():
    section("5. Full Metadata Extraction")
    if _skip_if_not_configured():
        return None

    with MetadataExtractor(**BASE_CONFIG) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["public"])

    print(f"  Instance : {db_meta.database_name}")
    print(f"  DB type  : {db_meta.database_type}")

    for schema in db_meta.schemas:
        print(f"\n  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables) - 5} more")

    print("\n  ✓ Extraction OK")
    return db_meta


# ─────────────────────────────────────────────────────────────────────────────
# 6. Single schema extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_schema():
    section("6. Extract Single Schema")
    if _skip_if_not_configured():
        return None

    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMA)

    print(f"  Schema : {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ Single-schema extraction OK")
    return schema


# ─────────────────────────────────────────────────────────────────────────────
# 7. Single table extraction (PK detail; Spanner has no FK enforcement)
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_table(schema):
    section("7. Extract Single Table  (PK detail)")
    if schema is None or not schema.tables:
        print("  ⚠  No schema/tables – skipping")
        return

    table_name = schema.tables[0].name

    with MetadataExtractor(**BASE_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMA, table_name)

    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  Primary keys : {table.primary_keys}")
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
    if db_meta is None:
        print("  ⚠  No metadata – skipping")
        return

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
    if db_meta is None:
        print("  ⚠  No metadata – skipping")
        return

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
    if db_meta is None:
        print("  ⚠  No metadata – skipping")
        return

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
    print("  ✓ Gap detection OK")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Schema comparison (self-comparison → 0 diffs)
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_comparison():
    section("11. Schema Comparison  (self-comparison → 0 diffs)")
    if _skip_if_not_configured():
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        comparator = SchemaComparator(
            source_config=BASE_CONFIG,
            destination_config=BASE_CONFIG,
            yaml_output_dir=tmpdir,
        )
        report = comparator.compare_and_generate_report(
            source_schema_name=TARGET_SCHEMA,
            destination_schema_name=TARGET_SCHEMA,
            include_yaml_gaps=True,
        )

    s = report["summary"]
    print(f"  Missing tables  : {s['missing_tables_count']}")
    print(f"  Missing columns : {s['missing_columns_count']}")
    print(f"  Type mismatches : {s['type_mismatches_count']}")
    print("  ✓ Schema comparison OK")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# 12. Email report (optional)
# ─────────────────────────────────────────────────────────────────────────────

def test_email_report(report):
    section("12. Email Report  (optional)")
    smtp_host = os.getenv("SMTP_HOST")
    email_to  = os.getenv("EMAIL_TO")

    if not smtp_host or not email_to or report is None:
        print("  ⚠  SMTP_HOST / EMAIL_TO not set or no report – skipping")
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
        subject="[Spanner Test] Schema Comparison Report",
    )
    print(f"  Email sent: {ok}")
    print("  ✓ Email test complete")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Metadata export to JSON
# ─────────────────────────────────────────────────────────────────────────────

def test_metadata_export(db_meta):
    section("13. Metadata Export to JSON")
    if db_meta is None:
        print("  ⚠  No metadata – skipping")
        return

    data = db_meta.to_dict()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(data, fh, indent=2, default=str)
        print(f"  Saved to: {fh.name}")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "☁️  " * 30)
    print("  data_dictionary_builder — Spanner full feature test")
    print("☁️  " * 30)

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
    print("  All Spanner feature tests completed!")
    print("✅ " * 30 + "\n")
