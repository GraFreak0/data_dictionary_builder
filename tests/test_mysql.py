"""
test_mysql.py
=============
Exercises every major feature of data_dictionary_builder against a live
MySQL (or MariaDB) instance, including all schema-filter strategies now
built into MetadataExtractor.extract_all_schemas().

Schema-filter formats demonstrated
------------------------------------
  Exact name    schema_filter=["myapp"]
  Glob/LIKE     schema_filter=["myapp_%"]  (SQL-LIKE style)
  prefix:       schema_filter=["prefix:stg_"]
  suffix:       schema_filter=["suffix:_prod"]
  contains:     schema_filter=["contains:shop"]
  regex:        schema_filter=["regex:^myapp_\\w+$"]
  Mixed list    any combination of the above in one call

In MySQL, "databases" and "schemas" are the same thing, so the filter
applies to database names returned by SHOW DATABASES.

Configuration – set these environment variables (or a .env file):
    MYSQL_HOST       default: localhost
    MYSQL_PORT       default: 3306
    MYSQL_DB         default: mysql
    MYSQL_USER       default: root
    MYSQL_PASSWORD
    MYSQL_SCHEMAS    comma-separated databases to use in tests
                     default: value of MYSQL_DB

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
    "db_type":  "mysql",
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DB", "mysql"),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
}

TARGET_SCHEMAS = os.getenv("MYSQL_SCHEMAS", BASE_CONFIG["database"]).split(",")


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
    section("2. Schema / Database Listing")
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
# The extractor fetches all databases from MySQL first, then resolves each
# entry against that live list.
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies")

    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live databases on server: {live}\n")

    db = BASE_CONFIG["database"]  # use actual DB name in examples below

    cases = [
        # (label, schema_filter)
        ("4a. Exact name  (original behaviour)",
         [db]),

        ("4b. Glob / SQL-LIKE  (db% matches databases starting with db name)",
         [f"{db}%"]),

        ("4c. prefix: marker  — any database starting with first 3 chars",
         [f"prefix:{db[:3]}"]),

        ("4d. suffix: marker  — any database ending with last 3 chars",
         [f"suffix:{db[-3:]}"]),

        ("4e. contains: marker  — any database containing first 3 chars",
         [f"contains:{db[:3]}"]),

        ("4f. regex: marker  — full match against the db name",
         [f"regex:^{db}$"]),

        ("4g. Mixed list  — exact + prefix convention + regex",
         [db, "prefix:stg_", "regex:^analytics_\\d{4}$"]),

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
    schema_name = TARGET_SCHEMAS[0]

    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(schema_name)

    print(f"  Schema : {schema.name}  ({len(schema.tables)} tables)")
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
    print(f"  Primary keys : {table.primary_keys}")

    fk_cols = [c for c in table.columns if c.is_foreign_key]
    if fk_cols:
        print(f"  Foreign keys ({len(fk_cols)}):")
        for fk in fk_cols:
            print(f"    • {fk.name} → {fk.foreign_key_table}.{fk.foreign_key_column}")

    print(f"  Columns ({len(table.columns)}):")
    for col in table.columns[:10]:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        pk = " [PK]" if col.is_primary_key else ""
        desc = f'  "{col.description}"' if col.description else ""
        print(f"    • {col.name}: {col.data_type} {nullable}{pk}{desc}")
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
        subject="[MySQL Test] Schema Comparison Report",
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
    print("\n" + "🐬 " * 30)
    print("  data_dictionary_builder — MySQL full feature test")
    print("🐬 " * 30)

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
    print("  All MySQL feature tests passed!")
    print("✅ " * 30 + "\n")
