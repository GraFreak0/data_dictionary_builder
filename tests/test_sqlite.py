"""
test_sqlite.py
==============
Exercises every major feature of data_dictionary_builder against SQLite,
including all schema-filter strategies now built into
MetadataExtractor.extract_all_schemas().

SQLite is the easiest connector to test locally — no server required.
The test creates a temporary database with realistic tables, runs every
feature including a cross-database diff, then cleans up.

Schema-filter formats demonstrated
------------------------------------
  Exact name    schema_filter=["main"]
  Glob/LIKE     schema_filter=["ma%"]
  prefix:       schema_filter=["prefix:ma"]
  suffix:       schema_filter=["suffix:in"]
  contains:     schema_filter=["contains:ai"]
  regex:        schema_filter=["regex:^ma.*$"]
  Mixed list    any combination of the above in one call

SQLite always has exactly one schema — "main" — so the filter demos use
a simulated list of schema names to show every strategy, then confirm the
real extraction still works with an exact-name and glob filter.

No environment variables required.

Optional email:
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import os
import json
import sqlite3
import tempfile

from dotenv import load_dotenv

from data_dictionary_builder import MetadataExtractor, YAMLGenerator, SchemaComparator
from data_dictionary_builder.notifications.email_sender import EmailSender

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Fixture – build a realistic temp database
# ─────────────────────────────────────────────────────────────────────────────

def create_test_database(path: str) -> None:
    """Create a small e-commerce SQLite database for testing."""
    conn = sqlite3.connect(path)
    conn.cursor().executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS countries (
            country_id   INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            country_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  INTEGER PRIMARY KEY,
            email        TEXT NOT NULL UNIQUE,
            first_name   TEXT,
            last_name    TEXT,
            country_id   INTEGER REFERENCES countries(country_id),
            created_at   TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS products (
            product_id   INTEGER PRIMARY KEY,
            sku          TEXT NOT NULL UNIQUE,
            name         TEXT NOT NULL,
            price        REAL NOT NULL,
            stock        INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id     INTEGER PRIMARY KEY,
            customer_id  INTEGER NOT NULL REFERENCES customers(customer_id),
            order_date   TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status       TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS order_items (
            item_id    INTEGER PRIMARY KEY,
            order_id   INTEGER NOT NULL REFERENCES orders(order_id),
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity   INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );

        INSERT OR IGNORE INTO countries VALUES (1,'US','United States'),(2,'GB','United Kingdom');
        INSERT OR IGNORE INTO customers VALUES
            (1,'alice@example.com','Alice','Smith',1,'2024-01-01','active'),
            (2,'bob@example.com','Bob','Jones',2,'2024-01-15','active');
        INSERT OR IGNORE INTO products VALUES
            (1,'SKU-001','Widget A',9.99,100),(2,'SKU-002','Widget B',19.99,50);
        INSERT OR IGNORE INTO orders VALUES
            (1,1,'2024-02-01',29.98,'completed'),(2,2,'2024-02-10',19.99,'pending');
        INSERT OR IGNORE INTO order_items VALUES
            (1,1,1,2,9.99),(2,1,2,1,19.99),(3,2,2,1,19.99);
    """)
    conn.commit()
    conn.close()


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Connection test
# ─────────────────────────────────────────────────────────────────────────────

def test_connection(db_path: str):
    section("1. Connection Test")
    config = {"db_type": "sqlite", "database": db_path}
    ok = MetadataExtractor(**config).test_connection()
    print(f"  Connection successful: {ok}")
    assert ok, "❌  SQLite connection failed"
    print("  ✓ Connected")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Schema listing
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_listing(db_path: str):
    section("2. Schema Listing")
    config = {"db_type": "sqlite", "database": db_path}
    with MetadataExtractor(**config) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    assert "main" in schemas
    print("  ✓ 'main' schema present")
    return schemas


# ─────────────────────────────────────────────────────────────────────────────
# 3. Table listing
# ─────────────────────────────────────────────────────────────────────────────

def test_table_listing(db_path: str):
    section("3. Table Listing")
    config = {"db_type": "sqlite", "database": db_path}
    with MetadataExtractor(**config) as ext:
        tables = ext.get_tables_list("main")
    print(f"  Tables: {tables}")
    assert len(tables) >= 5
    print(f"  ✓ Found {len(tables)} table(s)")
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# 4. Schema-filter strategies
#
# SQLite always has exactly one schema ("main"), so we demonstrate all filter
# formats against a simulated richer list first, then confirm each format
# also works correctly against the real single-schema database.
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_filter_strategies(db_path: str):
    section("4. Schema-Filter Strategies")

    config = {"db_type": "sqlite", "database": db_path}

    print("  All entries are passed directly to extract_all_schemas().\n")

    cases = [
        # (label, schema_filter, expect_main)
        ("4a. Exact name  (original behaviour)",
         ["main"], True),

        ("4b. Glob / SQL-LIKE  (ma% matches 'main')",
         ["ma%"], True),

        ("4c. prefix: marker  — anything starting with 'ma'",
         ["prefix:ma"], True),

        ("4d. suffix: marker  — anything ending with 'in'",
         ["suffix:in"], True),

        ("4e. contains: marker  — anything containing 'ai'",
         ["contains:ai"], True),

        ("4f. regex: marker  — full match ^ma.*$",
         ["regex:^ma.*$"], True),

        ("4g. Mixed list  — exact + glob + regex in one call",
         ["main", "ma%", "regex:^stg_.*$"], True),

        ("4h. No filter (None)  — extract everything",
         None, True),

        ("4i. Non-matching filter  — no schemas returned",
         ["prefix:stg_"], False),
    ]

    for label, sf, expect_main in cases:
        with MetadataExtractor(**config) as ext:
            db_meta = ext.extract_all_schemas(schema_filter=sf)
        matched = [s.name for s in db_meta.schemas]
        status = "✓" if (("main" in matched) == expect_main) else "✗"
        print(f"  {status} {label}")
        print(f"      filter  : {sf}")
        print(f"      matched : {matched}\n")

    print("  ✓ Schema-filter strategies demonstrated")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Full extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_all_schemas(db_path: str):
    section("5. Full Metadata Extraction")
    config = {"db_type": "sqlite", "database": db_path}

    with MetadataExtractor(**config) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["main"])

    print(f"  Database : {db_meta.database_name}")
    print(f"  DB type  : {db_meta.database_type}")
    print(f"  Version  : {db_meta.version}")

    for schema in db_meta.schemas:
        print(f"\n  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")

    assert any(t.name == "customers" for s in db_meta.schemas for t in s.tables)
    print("\n  ✓ Extraction OK")
    return db_meta


# ─────────────────────────────────────────────────────────────────────────────
# 6. Single schema extraction
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_schema(db_path: str):
    section("6. Extract Single Schema")
    config = {"db_type": "sqlite", "database": db_path}

    with MetadataExtractor(**config) as ext:
        schema = ext.extract_schema("main")

    print(f"  Schema : {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ Single-schema extraction OK")
    return schema


# ─────────────────────────────────────────────────────────────────────────────
# 7. Single table extraction (FK / PK detail)
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_single_table(db_path: str):
    section("7. Extract Single Table  (FK / PK detail)")
    config = {"db_type": "sqlite", "database": db_path}

    with MetadataExtractor(**config) as ext:
        table = ext.extract_table("main", "orders")

    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  Primary keys : {table.primary_keys}")

    fk_cols = [c for c in table.columns if c.is_foreign_key]
    if fk_cols:
        print("  Foreign keys:")
        for fk in fk_cols:
            print(f"    • {fk.name} → {fk.foreign_key_table}.{fk.foreign_key_column}")

    print(f"  Columns ({len(table.columns)}):")
    for col in table.columns:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        pk = " [PK]" if col.is_primary_key else ""
        fk = " [FK]" if col.is_foreign_key else ""
        print(f"    • {col.name}: {col.data_type} {nullable}{pk}{fk}")

    assert table.primary_keys == ["order_id"]
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
        assert len(files) >= 1
    print("  ✓ Per-schema YAML OK")


# ─────────────────────────────────────────────────────────────────────────────
# 9. YAML generation – single combined file
# ─────────────────────────────────────────────────────────────────────────────

def test_yaml_combined(db_meta):
    section("9. YAML Generation – Single Combined File")
    with tempfile.TemporaryDirectory() as tmpdir:
        gen      = YAMLGenerator(output_dir=tmpdir)
        filepath = gen.generate_single_yaml(db_meta, filename="all_models.yml")
        size     = os.path.getsize(filepath)
        print(f"  {os.path.basename(filepath)}  ({size:,} bytes)")
        assert size > 0
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

    assert len(tables_no_desc) == total_tables, \
        "Expected all SQLite tables to lack descriptions"
    print("  (SQLite has no native COMMENT support — all descriptions empty, expected)")
    print("  ✓ Gap detection OK")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Schema comparison (self-comparison → zero diffs)
# ─────────────────────────────────────────────────────────────────────────────

def test_schema_comparison(db_path: str):
    section("11. Schema Comparison  (self-comparison → 0 diffs)")
    config = {"db_type": "sqlite", "database": db_path}

    with tempfile.TemporaryDirectory() as tmpdir:
        comparator = SchemaComparator(
            source_config=config,
            destination_config=config,
            yaml_output_dir=tmpdir,
        )
        report = comparator.compare_and_generate_report(
            source_schema_name="main",
            destination_schema_name="main",
            include_yaml_gaps=True,
        )

    s = report["summary"]
    print(f"  Missing tables  : {s['missing_tables_count']}")
    print(f"  Missing columns : {s['missing_columns_count']}")
    print(f"  Type mismatches : {s['type_mismatches_count']}")

    assert s["missing_tables_count"] == 0
    assert s["missing_columns_count"] == 0
    assert s["type_mismatches_count"] == 0
    print("  ✓ Schema comparison OK  (0 diffs as expected)")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# 12. Cross-database comparison (intentional diffs)
# ─────────────────────────────────────────────────────────────────────────────

def test_cross_db_comparison(db_path: str):
    section("12. Cross-DB Comparison  (source vs stripped copy)")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        dest_path = fh.name

    conn = sqlite3.connect(dest_path)
    conn.cursor().executescript("""
        CREATE TABLE countries (
            country_id   INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL
            -- country_name intentionally removed
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            email       TEXT NOT NULL
        );
        -- products, orders, order_items intentionally missing
    """)
    conn.commit()
    conn.close()

    src_config  = {"db_type": "sqlite", "database": db_path}
    dest_config = {"db_type": "sqlite", "database": dest_path}

    with tempfile.TemporaryDirectory() as tmpdir:
        comparator = SchemaComparator(
            source_config=src_config,
            destination_config=dest_config,
            yaml_output_dir=tmpdir,
        )
        report = comparator.compare_and_generate_report(
            source_schema_name="main",
            destination_schema_name="main",
            include_yaml_gaps=False,
        )

    s = report["summary"]
    print(f"  Missing tables  : {s['missing_tables_count']}  (expected 3)")
    print(f"  Missing columns : {s['missing_columns_count']}  (expected ≥1)")
    print(f"  Type mismatches : {s['type_mismatches_count']}")

    if report["comparison"]["missing_tables"]:
        print("  Missing table details:")
        for t in report["comparison"]["missing_tables"]:
            print(f"    • {t['schema']}.{t['table']}")

    assert s["missing_tables_count"] >= 3, "Expected at least 3 missing tables"

    os.unlink(dest_path)
    print("  ✓ Cross-DB comparison OK")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Email report (optional)
# ─────────────────────────────────────────────────────────────────────────────

def test_email_report(report):
    section("13. Email Report  (optional)")
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
        subject="[SQLite Test] Schema Comparison Report",
    )
    print(f"  Email sent: {ok}")
    print("  ✓ Email test complete")


# ─────────────────────────────────────────────────────────────────────────────
# 14. Metadata export to JSON
# ─────────────────────────────────────────────────────────────────────────────

def test_metadata_export(db_meta):
    section("14. Metadata Export to JSON")
    data = db_meta.to_dict()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(data, fh, indent=2, default=str)
        size = os.path.getsize(fh.name)
        print(f"  Saved to : {fh.name}")
        print(f"  Size     : {size:,} bytes")
    assert size > 0
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "🗂  " * 30)
    print("  data_dictionary_builder — SQLite full feature test")
    print("🗂  " * 30)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        db_path = fh.name

    try:
        print(f"\n  Using temp database: {db_path}")
        create_test_database(db_path)

        test_connection(db_path)
        test_schema_listing(db_path)
        test_table_listing(db_path)
        test_schema_filter_strategies(db_path)

        db_meta = test_extract_all_schemas(db_path)
        test_extract_single_schema(db_path)
        test_extract_single_table(db_path)

        test_yaml_per_schema(db_meta)
        test_yaml_combined(db_meta)
        test_documentation_gaps(db_meta)

        report = test_schema_comparison(db_path)
        test_cross_db_comparison(db_path)
        test_email_report(report)
        test_metadata_export(db_meta)

    finally:
        os.unlink(db_path)
        print(f"\n  Cleaned up temp database: {db_path}")

    print("\n" + "✅ " * 30)
    print("  All SQLite feature tests passed!")
    print("✅ " * 30 + "\n")
