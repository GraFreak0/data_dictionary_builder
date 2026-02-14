# DB Metadata Generator - Complete User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [Database Connections](#database-connections)
6. [Metadata Extraction](#metadata-extraction)
7. [YAML File Generation](#yaml-file-generation)
8. [Schema Comparison](#schema-comparison)
9. [Email Notifications](#email-notifications)
10. [Airflow Integration](#airflow-integration)
11. [Advanced Usage](#advanced-usage)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)
14. [Complete Examples](#complete-examples)

---

## Introduction

### What is DB Metadata Generator?

DB Metadata Generator is a Python library designed to help data engineers and analytics teams build comprehensive data dictionaries by:

- **Extracting** complete metadata from databases (tables, columns, types, constraints)
- **Generating** dbt-compatible YAML files for documentation
- **Comparing** schemas between source and destination databases
- **Reporting** differences and missing documentation via email
- **Orchestrating** these tasks through Airflow

### Who Should Use This Library?

- **Data Engineers** building data dictionaries and documentation
- **Analytics Engineers** working with dbt and need automated schema documentation
- **Database Administrators** managing multiple databases and need schema comparison
- **Data Teams** maintaining data quality and documentation standards

### Key Benefits

✅ **Time Savings** - Automate documentation instead of manual YAML writing  
✅ **Accuracy** - Extract actual schema state, not outdated documentation  
✅ **Quality Control** - Identify missing descriptions and schema drift  
✅ **Multi-Database** - One tool for SQLite, PostgreSQL, MySQL, ClickHouse, Spanner  
✅ **dbt Integration** - Generate dbt-compatible YAML files automatically  
✅ **Airflow Ready** - Built for production orchestration  

---

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Access to the databases you want to document

### Step 1: Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### Step 2: Install the Library

```bash
# Install in development mode (if you downloaded the source)
pip install -e .

# Or install from pip (when published)
pip install data-dictionary-builder
```

### Step 3: Verify Installation

```python
python -c "from data_dictionary_builder import MetadataExtractor; print('✓ Installation successful!')"
```

### Database-Specific Dependencies

The library automatically installs drivers for all supported databases:

- **SQLite**: Built into Python (no extra install needed)
- **PostgreSQL**: psycopg2-binary
- **MySQL**: PyMySQL
- **ClickHouse**: clickhouse-driver
- **Google Spanner**: google-cloud-spanner

---

## Quick Start

### 5-Minute Tutorial

This example extracts metadata from a PostgreSQL database and generates YAML files:

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator

# Step 1: Configure your database connection
db_config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'postgres',
    'password': 'your_password'
}

# Step 2: Extract metadata
print("Extracting metadata...")
with MetadataExtractor(**db_config) as extractor:
    # Extract all schemas (or use schema_filter=['public'] for specific ones)
    db_metadata = extractor.extract_all_schemas()
    
    print(f"✓ Extracted {len(db_metadata.schemas)} schemas")
    for schema in db_metadata.schemas:
        print(f"  - {schema.name}: {len(schema.tables)} tables")

# Step 3: Generate YAML files for dbt
print("\nGenerating YAML files...")
yaml_gen = YAMLGenerator(output_dir='./dbt_models')
files = yaml_gen.generate_yaml_files(db_metadata)

print(f"✓ Generated {len(files)} YAML files:")
for file_path in files:
    print(f"  - {file_path}")

print("\n✨ Done! Your YAML files are ready for dbt.")
```

**Output:**
```
Extracting metadata...
✓ Extracted 2 schemas
  - public: 15 tables
  - analytics: 8 tables

Generating YAML files...
✓ Generated 2 YAML files:
  - ./dbt_models/schema_public.yml
  - ./dbt_models/schema_analytics.yml

✨ Done! Your YAML files are ready for dbt.
```

---

## Core Concepts

### Metadata Hierarchy

The library organizes metadata in a hierarchical structure:

```
DatabaseMetadata
├── SchemaMetadata (e.g., "public", "analytics")
│   ├── TableMetadata (e.g., "customers", "orders")
│   │   ├── ColumnMetadata (e.g., "customer_id", "email")
│   │   │   ├── name
│   │   │   ├── data_type
│   │   │   ├── is_nullable
│   │   │   ├── is_primary_key
│   │   │   ├── is_foreign_key
│   │   │   └── description
│   │   ├── primary_keys
│   │   └── row_count
│   └── description
```

### Key Classes

1. **MetadataExtractor** - Connects to databases and extracts schema information
2. **YAMLGenerator** - Converts metadata to dbt-compatible YAML files
3. **SchemaComparator** - Compares schemas between databases
4. **EmailSender** - Sends formatted email reports
5. **Data Models** - DatabaseMetadata, SchemaMetadata, TableMetadata, ColumnMetadata

---

## Database Connections

### Connection Configuration

Each database type requires specific connection parameters:

### SQLite

**Use Case:** Local development, small databases, testing

```python
config = {
    'db_type': 'sqlite',
    'database': '/path/to/your/database.db'
}

# Example
config = {
    'db_type': 'sqlite',
    'database': './ecommerce.db'
}
```

**Notes:**
- SQLite doesn't require username/password
- Use absolute or relative file paths
- Schema name is typically 'main'

### PostgreSQL

**Use Case:** Production databases, data warehouses

```python
config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'user': 'postgres',
    'password': 'secure_password'
}

# Real-world example
config = {
    'db_type': 'postgres',
    'host': 'prod-db.example.com',
    'port': 5432,
    'database': 'analytics_db',
    'user': 'readonly_user',
    'password': 'readonly_pass'
}
```

**Best Practices:**
- Use read-only user credentials
- Connect during off-peak hours for large databases
- Consider using connection pooling for repeated connections

### MySQL

**Use Case:** Web applications, MariaDB databases

```python
config = {
    'db_type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'database': 'mydb',
    'user': 'root',
    'password': 'password'
}

# Production example
config = {
    'db_type': 'mysql',
    'host': 'mysql-prod.example.com',
    'port': 3306,
    'database': 'app_database',
    'user': 'app_readonly',
    'password': 'readonly_password'
}
```

**Notes:**
- MySQL uses databases as schemas
- Default port is 3306
- Supports both MySQL and MariaDB

### ClickHouse

**Use Case:** OLAP databases, analytics workloads, time-series data

```python
config = {
    'db_type': 'clickhouse',
    'host': 'localhost',
    'port': 9000,  # Native protocol port, NOT HTTP port
    'database': 'default',
    'user': 'default',
    'password': ''
}

# Production example
config = {
    'db_type': 'clickhouse',
    'host': 'clickhouse.example.com',
    'port': 9000,
    'database': 'analytics',
    'user': 'readonly',
    'password': 'secure_password'
}
```

**Important:**
- Use port **9000** (native protocol), not 8123 (HTTP)
- ClickHouse doesn't enforce foreign keys (metadata only)
- Supports Nullable types

### Google Cloud Spanner

**Use Case:** Google Cloud databases, globally distributed databases

```python
config = {
    'db_type': 'spanner',
    'instance_id': 'my-instance',
    'database_id': 'my-database',
    'project_id': 'my-gcp-project'  # Optional if using default credentials
}

# Production example
config = {
    'db_type': 'spanner',
    'instance_id': 'prod-instance',
    'database_id': 'analytics-db',
    'project_id': 'my-company-prod'
}
```

**Authentication:**
```bash
# Set up Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Or use gcloud CLI
gcloud auth application-default login
```

### Testing Connections

Always test your connection before running full extractions:

```python
from data_dictionary_builder import MetadataExtractor

config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'user': 'postgres',
    'password': 'password'
}

extractor = MetadataExtractor(**config)

if extractor.test_connection():
    print("✓ Connection successful!")
else:
    print("✗ Connection failed!")
```

### Using Environment Variables

**Recommended approach for production:**

```python
import os
from dotenv import load_dotenv

# Load from .env file
load_dotenv()

config = {
    'db_type': os.getenv('DB_TYPE'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}
```

**.env file:**
```bash
DB_TYPE=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=postgres
DB_PASSWORD=your_secure_password
```

---

## Metadata Extraction

### Extract All Schemas

**Use Case:** Full database documentation

```python
from data_dictionary_builder import MetadataExtractor

with MetadataExtractor(**config) as extractor:
    # Extract everything
    db_metadata = extractor.extract_all_schemas()
    
    # Access the data
    print(f"Database: {db_metadata.database_name}")
    print(f"Type: {db_metadata.database_type}")
    print(f"Version: {db_metadata.version}")
    print(f"Schemas: {len(db_metadata.schemas)}")
    
    for schema in db_metadata.schemas:
        print(f"\n{schema.name}:")
        for table in schema.tables:
            print(f"  - {table.name} ({len(table.columns)} columns, {table.row_count} rows)")
```

**Output:**
```
Database: ecommerce_db
Type: postgresql
Version: PostgreSQL 14.5
Schemas: 2

public:
  - customers (8 columns, 15234 rows)
  - orders (12 columns, 45678 rows)
  - products (10 columns, 892 rows)

analytics:
  - daily_sales (6 columns, 365 rows)
  - customer_metrics (15 columns, 15234 rows)
```

### Extract Specific Schemas

**Use Case:** Focus on relevant schemas only (faster, more efficient)

```python
with MetadataExtractor(**config) as extractor:
    # Only extract 'public' and 'analytics' schemas
    db_metadata = extractor.extract_all_schemas(
        schema_filter=['public', 'analytics']
    )
    
    print(f"Extracted {len(db_metadata.schemas)} schemas")
```

**Why use schema_filter?**
- ⚡ **Faster** - Skip system schemas and irrelevant data
- 💾 **Less memory** - Only load what you need
- 🎯 **Focused** - Generate YAML only for schemas you care about

### Extract Single Schema

**Use Case:** Incremental updates, testing

```python
with MetadataExtractor(**config) as extractor:
    schema_metadata = extractor.extract_schema('public')
    
    print(f"Schema: {schema_metadata.name}")
    print(f"Tables: {len(schema_metadata.tables)}")
    
    for table in schema_metadata.tables:
        print(f"  - {table.name}")
```

### Extract Single Table

**Use Case:** Detailed inspection, debugging

```python
with MetadataExtractor(**config) as extractor:
    table = extractor.extract_table('public', 'customers')
    
    print(f"Table: {table.name}")
    print(f"Schema: {table.schema_name}")
    print(f"Rows: {table.row_count}")
    print(f"Primary Keys: {', '.join(table.primary_keys)}")
    
    print("\nColumns:")
    for col in table.columns:
        nullable = "NULL" if col.is_nullable else "NOT NULL"
        pk = " (PK)" if col.is_primary_key else ""
        fk = f" (FK → {col.foreign_key_table}.{col.foreign_key_column})" if col.is_foreign_key else ""
        print(f"  {col.name}: {col.data_type} {nullable}{pk}{fk}")
```

**Output:**
```
Table: customers
Schema: public
Rows: 15234
Primary Keys: customer_id

Columns:
  customer_id: integer NOT NULL (PK)
  email: varchar NOT NULL
  first_name: varchar NULL
  last_name: varchar NULL
  created_at: timestamp NOT NULL
  updated_at: timestamp NULL
  country_id: integer NULL (FK → countries.country_id)
  status: varchar NOT NULL
```

### List Available Schemas and Tables

**Use Case:** Exploration, validation

```python
with MetadataExtractor(**config) as extractor:
    # List all schemas
    schemas = extractor.get_schemas_list()
    print("Available schemas:", schemas)
    
    # List tables in a specific schema
    tables = extractor.get_tables_list('public')
    print("\nTables in 'public':")
    for table in tables:
        print(f"  - {table}")
```

### Understanding Extracted Metadata

#### Column Metadata

Each column includes:
- `name` - Column name
- `data_type` - Data type (e.g., varchar, integer, timestamp)
- `is_nullable` - Can it be NULL?
- `is_primary_key` - Is it part of primary key?
- `is_foreign_key` - Is it a foreign key?
- `foreign_key_table` - Referenced table (if FK)
- `foreign_key_column` - Referenced column (if FK)
- `default_value` - Default value
- `character_maximum_length` - Max length for strings
- `numeric_precision` - Precision for numbers
- `numeric_scale` - Scale for numbers
- `description` - Column description (from database comments)
- `ordinal_position` - Position in table

#### Table Metadata

Each table includes:
- `name` - Table name
- `schema_name` - Schema it belongs to
- `table_type` - BASE TABLE, VIEW, etc.
- `columns` - List of ColumnMetadata
- `description` - Table description (from database comments)
- `row_count` - Approximate row count
- `primary_keys` - List of primary key column names
- `indexes` - Index information

### Performance Considerations

**For Large Databases:**

```python
# 1. Use schema filtering
db_metadata = extractor.extract_all_schemas(
    schema_filter=['public']  # Only extract what you need
)

# 2. Row counts are approximate (from database statistics)
# This is much faster than COUNT(*) on large tables
table.row_count  # Uses pg_class for PostgreSQL, information_schema for others

# 3. Extract during off-peak hours
# Schedule in Airflow for nights/weekends
```

**Memory Management:**

```python
# For very large databases, process schema by schema
with MetadataExtractor(**config) as extractor:
    for schema_name in ['public', 'analytics', 'staging']:
        schema = extractor.extract_schema(schema_name)
        
        # Generate YAML immediately
        yaml_gen = YAMLGenerator(output_dir='./output')
        yaml_gen.generate_schema_yaml(schema)
        
        # schema goes out of scope and gets garbage collected
```

---

## YAML File Generation

### Generate dbt-Compatible YAML Files

The primary use case - create YAML files for your dbt project:

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator

# Extract metadata
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas(schema_filter=['public'])

# Generate YAML files
yaml_gen = YAMLGenerator(output_dir='./dbt_project/models')
files = yaml_gen.generate_yaml_files(db_metadata)

print(f"Generated {len(files)} YAML files for dbt")
```

### YAML File Structure

**Generated file: `schema_public.yml`**

```yaml
version: 2
models:
  - name: customers
    description: null
    meta:
      schema: public
      table_type: BASE TABLE
      row_count: 15234
    columns:
      - name: customer_id
        data_type: integer
        meta:
          is_primary_key: true
          is_nullable: false
        tests:
          - unique
          - not_null
      - name: email
        data_type: varchar
        tests:
          - not_null
      - name: first_name
        data_type: varchar
      - name: created_at
        data_type: timestamp
        tests:
          - not_null
      - name: country_id
        data_type: integer
        meta:
          is_foreign_key: true
          foreign_key_table: countries
          foreign_key_column: country_id
```

### Generate One File Per Schema (Default)

```python
yaml_gen = YAMLGenerator(output_dir='./dbt_models')
files = yaml_gen.generate_yaml_files(db_metadata)

# Creates:
# - schema_public.yml
# - schema_analytics.yml
# - schema_staging.yml
```

**When to use:**
- ✅ Multiple schemas with different purposes
- ✅ Want organized, modular YAML files
- ✅ Following dbt best practices

### Generate Single Combined YAML File

```python
yaml_gen = YAMLGenerator(output_dir='./dbt_models')
filepath = yaml_gen.generate_single_yaml(
    db_metadata, 
    filename='all_models.yml'
)

# Creates one file: all_models.yml with all schemas
```

**When to use:**
- ✅ Small projects with few tables
- ✅ Want everything in one place
- ✅ Single schema databases

### Generate YAML for Single Schema

```python
with MetadataExtractor(**config) as extractor:
    schema = extractor.extract_schema('public')

yaml_gen = YAMLGenerator(output_dir='./output')
filepath = yaml_gen.generate_schema_yaml(
    schema, 
    filename='custom_name.yml'
)
```

### Customizing Output Directory

```python
# Development
yaml_gen = YAMLGenerator(output_dir='./dev_models')

# Production (dbt project)
yaml_gen = YAMLGenerator(output_dir='/opt/dbt/ecommerce/models')

# Staging
yaml_gen = YAMLGenerator(output_dir='./staging/models')
```

### Finding Tables/Columns Without Descriptions

**Use Case:** Data quality checks, documentation gaps

```python
yaml_gen = YAMLGenerator(output_dir='./output')

# Find undocumented tables
tables_without_desc = yaml_gen.get_tables_without_descriptions(db_metadata)
print(f"Tables missing descriptions: {len(tables_without_desc)}")
for table in tables_without_desc:
    print(f"  - {table}")

# Find undocumented columns
columns_without_desc = yaml_gen.get_columns_without_descriptions(db_metadata)
print(f"\nColumns missing descriptions: {len(columns_without_desc)}")
for col in columns_without_desc[:10]:  # Show first 10
    print(f"  - {col['schema']}.{col['table']}.{col['column']}")
```

**Output:**
```
Tables missing descriptions: 12
  - public.customers
  - public.orders
  - public.products
  - analytics.daily_sales

Columns missing descriptions: 127
  - public.customers.customer_id
  - public.customers.email
  - public.customers.first_name
  - public.orders.order_id
  - public.orders.customer_id
```

### Adding Descriptions in Your Database

**To get descriptions in YAML files, add them to your database:**

**PostgreSQL:**
```sql
COMMENT ON TABLE customers IS 'Customer master data table';
COMMENT ON COLUMN customers.email IS 'Customer email address, used for login';
```

**MySQL:**
```sql
ALTER TABLE customers COMMENT = 'Customer master data table';
ALTER TABLE customers MODIFY email VARCHAR(255) COMMENT 'Customer email address';
```

Then re-run the extraction and the descriptions will appear in your YAML!

### YAML for Different dbt Use Cases

**Source definitions:**
```python
# Extract source database
source_config = {'db_type': 'postgres', 'host': 'source-db', ...}
with MetadataExtractor(**source_config) as extractor:
    db_metadata = extractor.extract_all_schemas()

yaml_gen = YAMLGenerator(output_dir='./models/staging')
yaml_gen.generate_yaml_files(db_metadata)
# Use these as source definitions in dbt
```

**Model documentation:**
```python
# Extract from your warehouse
warehouse_config = {'db_type': 'postgres', 'host': 'warehouse', ...}
with MetadataExtractor(**warehouse_config) as extractor:
    db_metadata = extractor.extract_all_schemas()

yaml_gen = YAMLGenerator(output_dir='./models/marts')
yaml_gen.generate_yaml_files(db_metadata)
# Use these to document your dbt models
```

---

## Schema Comparison

### Why Compare Schemas?

Common scenarios:
- **Migration validation** - Ensure source data made it to destination
- **Replication monitoring** - Verify replicated tables match
- **Data quality** - Detect schema drift between environments
- **Change detection** - Identify unexpected schema changes

### Basic Schema Comparison

```python
from data_dictionary_builder import SchemaComparator

# Configure source and destination
source_config = {
    'db_type': 'postgres',
    'host': 'source-db.example.com',
    'database': 'production',
    ...
}

destination_config = {
    'db_type': 'postgres',
    'host': 'warehouse.example.com',
    'database': 'analytics',
    ...
}

# Create comparator
comparator = SchemaComparator(
    source_config=source_config,
    destination_config=destination_config,
    yaml_output_dir='./output'
)

# Compare schemas
result = comparator.compare_schemas(
    source_schema_name='public',
    destination_schema_name='public'  # Optional, defaults to source_schema_name
)

# Check results
print(f"Missing tables: {len(result.missing_tables)}")
print(f"Missing columns: {len(result.missing_columns)}")
print(f"Type mismatches: {len(result.type_mismatches)}")
```

### Generate Comprehensive Report

```python
report = comparator.compare_and_generate_report(
    source_schema_name='public',
    destination_schema_name='public',
    include_yaml_gaps=True  # Also find tables/columns without descriptions
)

# Print summary
summary = report['summary']
print("=== Comparison Summary ===")
print(f"Missing Tables: {summary['missing_tables_count']}")
print(f"Missing Columns: {summary['missing_columns_count']}")
print(f"Type Mismatches: {summary['type_mismatches_count']}")
print(f"Tables Without Descriptions: {summary.get('tables_without_descriptions_count', 0)}")
print(f"Columns Without Descriptions: {summary.get('columns_without_descriptions_count', 0)}")

# Show missing tables
if report['comparison']['missing_tables']:
    print("\n=== Missing Tables ===")
    for table in report['comparison']['missing_tables']:
        print(f"  - {table['schema']}.{table['table']} ({table['column_count']} columns)")

# Show missing columns
if report['comparison']['missing_columns']:
    print("\n=== Missing Columns (first 10) ===")
    for col in report['comparison']['missing_columns'][:10]:
        print(f"  - {col['schema']}.{col['table']}.{col['column']} ({col['data_type']})")

# Show type mismatches
if report['comparison']['type_mismatches']:
    print("\n=== Type Mismatches ===")
    for mismatch in report['comparison']['type_mismatches']:
        print(f"  - {mismatch['schema']}.{mismatch['table']}.{mismatch['column']}")
        print(f"    Source: {mismatch['source_type']}")
        print(f"    Destination: {mismatch['destination_type']}")
```

**Output:**
```
=== Comparison Summary ===
Missing Tables: 3
Missing Columns: 15
Type Mismatches: 2
Tables Without Descriptions: 8
Columns Without Descriptions: 45

=== Missing Tables ===
  - public.new_customers (6 columns)
  - public.temp_orders (4 columns)
  - public.audit_log (8 columns)

=== Missing Columns (first 10) ===
  - public.customers.loyalty_points (integer)
  - public.customers.referral_code (varchar)
  - public.orders.discount_amount (decimal)

=== Type Mismatches ===
  - public.orders.total_amount
    Source: numeric
    Destination: decimal
```

### Compare Multiple Schemas

```python
# Compare all matching schemas
results = comparator.extract_and_compare_all(
    source_schemas=['public', 'analytics', 'staging'],
    destination_schemas=['public', 'analytics', 'staging']
)

# results is a dict: {'public': ComparisonResult, 'analytics': ComparisonResult, ...}
for schema_name, result in results.items():
    print(f"\n{schema_name}:")
    print(f"  Missing tables: {len(result.missing_tables)}")
    print(f"  Missing columns: {len(result.missing_columns)}")
```

### Cross-Database Comparison

**Compare PostgreSQL to MySQL:**

```python
source_config = {
    'db_type': 'postgres',
    'host': 'postgres-db',
    ...
}

destination_config = {
    'db_type': 'mysql',
    'host': 'mysql-db',
    ...
}

comparator = SchemaComparator(
    source_config=source_config,
    destination_config=destination_config
)

report = comparator.compare_and_generate_report('public', 'mydb')
```

**Note:** Type mismatches are normalized (e.g., `varchar` vs `character varying` are considered the same)

### Understanding Comparison Results

The `ComparisonResult` object contains:

```python
result.missing_tables          # Tables in source but not in destination
result.missing_columns         # Columns in source tables but not in destination
result.type_mismatches        # Columns with different data types
result.tables_without_descriptions    # Undocumented tables
result.columns_without_descriptions   # Undocumented columns
```

Each is a list of dictionaries:

```python
# missing_tables
[
    {'schema': 'public', 'table': 'customers', 'column_count': 8},
    ...
]

# missing_columns
[
    {'schema': 'public', 'table': 'orders', 'column': 'discount', 'data_type': 'decimal'},
    ...
]

# type_mismatches
[
    {
        'schema': 'public',
        'table': 'orders',
        'column': 'amount',
        'source_type': 'numeric',
        'destination_type': 'decimal'
    },
    ...
]
```

### Save Comparison Results

```python
import json

# Save report to JSON
with open('comparison_report.json', 'w') as f:
    json.dump(report, f, indent=2)

# Save just the comparison part
with open('differences.json', 'w') as f:
    json.dump(report['comparison'], f, indent=2)
```

---

## Email Notifications

### Why Email Reports?

- **Automated alerts** when schemas diverge
- **Share results** with team members
- **Track changes** over time
- **Documentation** of schema evolution

### Send Basic Comparison Report

```python
from data_dictionary_builder import SchemaComparator, EmailSender

# 1. Compare schemas
comparator = SchemaComparator(source_config, destination_config)
report = comparator.compare_and_generate_report('public')

# 2. Configure email
email_sender = EmailSender(
    smtp_host='smtp.gmail.com',
    smtp_port=587,
    sender_email='your-email@gmail.com',
    sender_password='your-app-password',  # Use app password for Gmail
    use_tls=True
)

# 3. Send report
success = email_sender.send_comparison_report(
    recipient_emails=['team@example.com', 'data-eng@example.com'],
    report=report,
    subject='Schema Comparison Report - Production vs Warehouse'
)

if success:
    print("✓ Email sent successfully!")
else:
    print("✗ Failed to send email")
```

### Email Content

The email includes:

**HTML Version:**
- Summary table with counts
- Missing tables (with column counts)
- Missing columns (first 50)
- Type mismatches
- Tables without descriptions (first 20)
- Columns without descriptions (first 30)

**Plain Text Version:**
- Summary
- Key highlights
- Link to full report

### Gmail Configuration

**Step 1: Enable 2-Factor Authentication**
1. Go to your Google Account settings
2. Security → 2-Step Verification
3. Turn it on

**Step 2: Generate App Password**
1. Security → App passwords
2. Select "Mail" and your device
3. Copy the 16-character password

**Step 3: Use in Code**

```python
email_sender = EmailSender(
    smtp_host='smtp.gmail.com',
    smtp_port=587,
    sender_email='your-email@gmail.com',
    sender_password='xxxx xxxx xxxx xxxx',  # App password
    use_tls=True
)
```

### Corporate Email (Office 365, Outlook)

```python
email_sender = EmailSender(
    smtp_host='smtp.office365.com',
    smtp_port=587,
    sender_email='you@company.com',
    sender_password='your-password',
    use_tls=True
)
```

### Other SMTP Providers

**SendGrid:**
```python
email_sender = EmailSender(
    smtp_host='smtp.sendgrid.net',
    smtp_port=587,
    sender_email='apikey',  # Literally the string "apikey"
    sender_password='your-sendgrid-api-key',
    use_tls=True
)
```

**AWS SES:**
```python
email_sender = EmailSender(
    smtp_host='email-smtp.us-east-1.amazonaws.com',
    smtp_port=587,
    sender_email='verified@email.com',
    sender_password='your-smtp-password',
    use_tls=True
)
```

### Send Custom Email

```python
# Plain text email
email_sender.send_email(
    recipient_emails=['user@example.com'],
    subject='Database Metadata Extraction Complete',
    text_body='The metadata extraction job completed successfully.\n\nSee attached YAML files.'
)

# HTML email
html_content = """
<html>
<body>
    <h1>Metadata Extraction Complete</h1>
    <p>Successfully extracted metadata from <strong>15 tables</strong>.</p>
    <ul>
        <li>customers: 8 columns</li>
        <li>orders: 12 columns</li>
        <li>products: 10 columns</li>
    </ul>
</body>
</html>
"""

email_sender.send_email(
    recipient_emails=['team@example.com'],
    subject='Extraction Complete',
    text_body='See HTML version',
    html_body=html_content
)
```

### Error Handling

```python
try:
    success = email_sender.send_comparison_report(
        recipient_emails=['team@example.com'],
        report=report
    )
    if success:
        print("Email sent")
    else:
        print("Email failed - check logs")
except Exception as e:
    print(f"Error sending email: {e}")
    # Continue with your workflow anyway
```

### Multiple Recipients

```python
# List of emails
recipients = [
    'data-team@example.com',
    'analytics@example.com',
    'john.doe@example.com',
    'jane.smith@example.com'
]

email_sender.send_comparison_report(
    recipient_emails=recipients,
    report=report
)
```

---

## Airflow Integration

### Why Use Airflow?

- **Schedule** regular metadata extractions
- **Orchestrate** complex workflows
- **Monitor** job execution
- **Retry** failed tasks
- **Alert** on failures

### Basic Airflow DAG

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from data_dictionary_builder import MetadataExtractor, YAMLGenerator

def extract_and_generate():
    """Extract metadata and generate YAML files."""
    config = {
        'db_type': 'postgres',
        'host': 'prod-db.example.com',
        'port': 5432,
        'database': 'production',
        'user': 'readonly',
        'password': 'password'
    }
    
    # Extract
    with MetadataExtractor(**config) as extractor:
        db_metadata = extractor.extract_all_schemas(
            schema_filter=['public', 'analytics']
        )
    
    # Generate YAML
    yaml_gen = YAMLGenerator(output_dir='/opt/dbt/models')
    files = yaml_gen.generate_yaml_files(db_metadata)
    
    print(f"Generated {len(files)} YAML files")
    return files

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['team@example.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metadata_extraction',
    default_args=default_args,
    description='Extract database metadata and generate YAML',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

task = PythonOperator(
    task_id='extract_and_generate',
    python_callable=extract_and_generate,
    dag=dag,
)
```

### Complete DAG with Comparison and Email

See `examples/airflow_dag_example.py` for a full implementation including:

1. **Extract metadata** from source database
2. **Generate YAML files** for dbt
3. **Compare schemas** between source and destination
4. **Send email report** with results

**Key features:**
- Uses XCom to pass data between tasks
- Proper error handling
- Email on failure
- Configurable retry logic
- Environment variable support

### Task Dependencies

```python
# Sequential workflow
task_extract >> task_generate_yaml >> task_compare >> task_send_email

# Parallel tasks
task_extract_source = PythonOperator(...)
task_extract_dest = PythonOperator(...)
task_compare = PythonOperator(...)

# Both extractions must complete before comparison
[task_extract_source, task_extract_dest] >> task_compare
```

### Using Airflow Variables

```python
from airflow.models import Variable

# Set in Airflow UI: Admin → Variables
db_host = Variable.get("PROD_DB_HOST")
db_password = Variable.get("PROD_DB_PASSWORD")

config = {
    'db_type': 'postgres',
    'host': db_host,
    'password': db_password,
    ...
}
```

### Using Airflow Connections

```python
from airflow.hooks.base import BaseHook

# Set in Airflow UI: Admin → Connections
connection = BaseHook.get_connection('postgres_prod')

config = {
    'db_type': 'postgres',
    'host': connection.host,
    'port': connection.port,
    'database': connection.schema,
    'user': connection.login,
    'password': connection.password,
}
```

### Scheduling Examples

```python
# Daily at 2 AM
schedule_interval='0 2 * * *'

# Every 6 hours
schedule_interval='0 */6 * * *'

# Weekly on Sunday at midnight
schedule_interval='0 0 * * 0'

# Monthly on the 1st at 3 AM
schedule_interval='0 3 1 * *'

# Using timedelta
schedule_interval=timedelta(hours=12)
```

### Best Practices for Airflow

1. **Use environment variables** for sensitive data
2. **Set appropriate timeouts** for database operations
3. **Enable email alerts** for failures
4. **Use schema_filter** to limit extraction scope
5. **Run during off-peak hours** for large databases
6. **Set retry logic** for transient failures
7. **Use XCom** sparingly (for small data only)
8. **Log extensively** for debugging

---

## Advanced Usage

### Working with Metadata Objects

```python
# Access metadata programmatically
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()

# Iterate through hierarchy
for schema in db_metadata.schemas:
    print(f"Schema: {schema.name}")
    
    for table in schema.tables:
        print(f"  Table: {table.name}")
        
        # Find primary key columns
        pk_columns = [col for col in table.columns if col.is_primary_key]
        print(f"    Primary keys: {[col.name for col in pk_columns]}")
        
        # Find foreign key relationships
        fk_columns = [col for col in table.columns if col.is_foreign_key]
        for fk in fk_columns:
            print(f"    FK: {fk.name} -> {fk.foreign_key_table}.{fk.foreign_key_column}")
        
        # Find nullable columns
        nullable = [col for col in table.columns if col.is_nullable]
        print(f"    Nullable columns: {len(nullable)}")
```

### Custom Data Analysis

```python
# Find all integer columns across all tables
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()

integer_columns = []
for schema in db_metadata.schemas:
    for table in schema.tables:
        for col in table.columns:
            if 'int' in col.data_type.lower():
                integer_columns.append({
                    'schema': schema.name,
                    'table': table.name,
                    'column': col.name,
                    'type': col.data_type
                })

print(f"Found {len(integer_columns)} integer columns")
```

### Export to Different Formats

**Export to JSON:**
```python
import json

with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()

# Convert to dictionary
metadata_dict = db_metadata.to_dict()

# Save to JSON
with open('metadata.json', 'w') as f:
    json.dump(metadata_dict, f, indent=2)
```

**Export to CSV:**
```python
import csv

# Extract flat table list
tables_data = []
for schema in db_metadata.schemas:
    for table in schema.tables:
        tables_data.append({
            'schema': schema.name,
            'table': table.name,
            'columns': len(table.columns),
            'rows': table.row_count,
            'has_description': bool(table.description)
        })

# Write to CSV
with open('tables.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['schema', 'table', 'columns', 'rows', 'has_description'])
    writer.writeheader()
    writer.writerows(tables_data)
```

### Data Quality Checks

```python
def check_naming_conventions(db_metadata):
    """Check if tables follow naming conventions."""
    violations = []
    
    for schema in db_metadata.schemas:
        for table in schema.tables:
            # Check: tables should be lowercase
            if table.name != table.name.lower():
                violations.append(f"{schema.name}.{table.name} - not lowercase")
            
            # Check: tables should be plural
            if not table.name.endswith('s'):
                violations.append(f"{schema.name}.{table.name} - not plural")
            
            for col in table.columns:
                # Check: columns should be snake_case
                if '-' in col.name or ' ' in col.name:
                    violations.append(f"{schema.name}.{table.name}.{col.name} - not snake_case")
    
    return violations

# Run checks
violations = check_naming_conventions(db_metadata)
if violations:
    print("Naming convention violations:")
    for v in violations:
        print(f"  - {v}")
```

### Incremental Updates

```python
import json
import os

def incremental_extraction(config, state_file='extraction_state.json'):
    """Only extract schemas that have changed."""
    
    # Load previous state
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            previous_state = json.load(f)
    else:
        previous_state = {}
    
    # Extract current metadata
    with MetadataExtractor(**config) as extractor:
        current_metadata = extractor.extract_all_schemas()
    
    # Compare and find changes
    current_state = current_metadata.to_dict()
    
    # Detect changes (simplified)
    changed_schemas = []
    for schema in current_metadata.schemas:
        schema_name = schema.name
        if schema_name not in previous_state.get('schemas', {}):
            changed_schemas.append(schema_name)
            continue
        
        # Check if table count changed
        prev_tables = len(previous_state['schemas'][schema_name].get('tables', []))
        curr_tables = len(schema.tables)
        if prev_tables != curr_tables:
            changed_schemas.append(schema_name)
    
    # Save current state
    with open(state_file, 'w') as f:
        json.dump(current_state, f)
    
    return changed_schemas

# Use it
changed = incremental_extraction(config)
print(f"Changed schemas: {changed}")
```

### Parallel Extraction

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_schema_parallel(config, schemas):
    """Extract multiple schemas in parallel."""
    
    def extract_one(schema_name):
        with MetadataExtractor(**config) as extractor:
            return extractor.extract_schema(schema_name)
    
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_schema = {
            executor.submit(extract_one, schema): schema 
            for schema in schemas
        }
        
        for future in as_completed(future_to_schema):
            schema_name = future_to_schema[future]
            try:
                schema_metadata = future.result()
                results[schema_name] = schema_metadata
                print(f"✓ Extracted {schema_name}")
            except Exception as e:
                print(f"✗ Failed to extract {schema_name}: {e}")
    
    return results

# Use it
schemas_to_extract = ['public', 'analytics', 'staging', 'marts']
results = extract_schema_parallel(config, schemas_to_extract)
```

---

## Best Practices

### Security

**1. Use Read-Only Database Users**
```sql
-- PostgreSQL
CREATE USER readonly_user WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE mydb TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
```

**2. Store Credentials Securely**
```python
# ✗ Bad - hardcoded credentials
config = {'password': 'my_password'}

# ✓ Good - environment variables
import os
config = {'password': os.getenv('DB_PASSWORD')}

# ✓ Better - secrets manager
from airflow.hooks.base import BaseHook
connection = BaseHook.get_connection('postgres_prod')
```

**3. Never Commit Credentials**
```bash
# Add to .gitignore
.env
config.yaml
secrets.json
*.credentials
```

### Performance

**1. Use Schema Filtering**
```python
# ✗ Slow - extracts everything including system schemas
db_metadata = extractor.extract_all_schemas()

# ✓ Fast - only what you need
db_metadata = extractor.extract_all_schemas(
    schema_filter=['public', 'analytics']
)
```

**2. Schedule During Off-Peak Hours**
```python
# In Airflow DAG
schedule_interval='0 2 * * *'  # 2 AM when database is less busy
```

**3. Use Connection Pooling for Multiple Extractions**
```python
# For one-off: use context manager
with MetadataExtractor(**config) as extractor:
    schema = extractor.extract_schema('public')

# For multiple operations: reuse connection
extractor = MetadataExtractor(**config)
extractor.connect()
try:
    schema1 = extractor.extract_schema('public')
    schema2 = extractor.extract_schema('analytics')
finally:
    extractor.disconnect()
```

### Code Organization

**1. Use Configuration Files**
```python
# config.yaml
databases:
  production:
    db_type: postgres
    host: prod-db.example.com
    port: 5432
    database: production
  warehouse:
    db_type: postgres
    host: warehouse.example.com
    port: 5432
    database: analytics

# Load in code
import yaml
with open('config.yaml') as f:
    config = yaml.safe_load(f)

prod_config = config['databases']['production']
```

**2. Create Reusable Functions**
```python
def extract_and_generate_yaml(db_config, schemas, output_dir):
    """Reusable extraction + YAML generation."""
    with MetadataExtractor(**db_config) as extractor:
        db_metadata = extractor.extract_all_schemas(schema_filter=schemas)
    
    yaml_gen = YAMLGenerator(output_dir=output_dir)
    return yaml_gen.generate_yaml_files(db_metadata)

# Use it
files = extract_and_generate_yaml(
    db_config=prod_config,
    schemas=['public', 'analytics'],
    output_dir='./dbt_models'
)
```

**3. Add Logging**
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

logger.info("Starting metadata extraction...")
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()
logger.info(f"Extracted {len(db_metadata.schemas)} schemas")
```

### Testing

**1. Test Connections First**
```python
def test_database_connection(config):
    """Test before running full extraction."""
    extractor = MetadataExtractor(**config)
    if extractor.test_connection():
        print("✓ Connection successful")
        return True
    else:
        print("✗ Connection failed")
        return False

# Use it
if test_database_connection(config):
    # Proceed with extraction
    pass
```

**2. Validate Generated YAML**
```python
import yaml

def validate_yaml_file(filepath):
    """Ensure YAML is valid."""
    try:
        with open(filepath, 'r') as f:
            yaml.safe_load(f)
        return True
    except yaml.YAMLError as e:
        print(f"Invalid YAML: {e}")
        return False

# After generation
files = yaml_gen.generate_yaml_files(db_metadata)
for file in files:
    if not validate_yaml_file(file):
        print(f"Warning: {file} is not valid YAML")
```

### Error Handling

**1. Graceful Degradation**
```python
def safe_extract_schemas(config, schemas):
    """Extract schemas with error handling."""
    results = {}
    errors = {}
    
    for schema_name in schemas:
        try:
            with MetadataExtractor(**config) as extractor:
                schema = extractor.extract_schema(schema_name)
                results[schema_name] = schema
                print(f"✓ {schema_name}")
        except Exception as e:
            errors[schema_name] = str(e)
            print(f"✗ {schema_name}: {e}")
    
    return results, errors

# Use it
results, errors = safe_extract_schemas(config, ['public', 'analytics', 'staging'])
if errors:
    print(f"Failed to extract {len(errors)} schemas")
```

**2. Retry Logic**
```python
import time

def extract_with_retry(config, max_retries=3):
    """Retry on transient failures."""
    for attempt in range(max_retries):
        try:
            with MetadataExtractor(**config) as extractor:
                return extractor.extract_all_schemas()
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
```

---

## Troubleshooting

### Connection Issues

**Problem: "Connection refused"**

```python
# Check:
# 1. Database is running
# 2. Host/port are correct
# 3. Firewall allows connection
# 4. Database accepts remote connections

# PostgreSQL: edit postgresql.conf
listen_addresses = '*'

# PostgreSQL: edit pg_hba.conf
host    all    all    0.0.0.0/0    md5
```

**Problem: "Authentication failed"**

```python
# Check:
# 1. Username/password are correct
# 2. User has permissions
# 3. For Gmail: use App Password, not regular password

# Test connection
extractor = MetadataExtractor(**config)
if not extractor.test_connection():
    print("Check credentials and permissions")
```

**Problem: "Database not found"**

```python
# List available databases first
with MetadataExtractor(**config) as extractor:
    schemas = extractor.get_schemas_list()
    print(f"Available schemas: {schemas}")
```

### Performance Issues

**Problem: Extraction is very slow**

```python
# Solution 1: Use schema filtering
db_metadata = extractor.extract_all_schemas(
    schema_filter=['public']  # Only extract what you need
)

# Solution 2: Extract during off-peak hours

# Solution 3: Skip row counts for very large tables
# Row counts use approximate statistics, but you can skip them:
# (Note: Current implementation always includes row counts)
```

**Problem: Out of memory**

```python
# Solution: Process schema by schema
for schema_name in ['public', 'analytics']:
    with MetadataExtractor(**config) as extractor:
        schema = extractor.extract_schema(schema_name)
    
    # Process immediately
    yaml_gen = YAMLGenerator(output_dir='./output')
    yaml_gen.generate_schema_yaml(schema)
    
    # schema goes out of scope and memory is freed
```

### YAML Issues

**Problem: Invalid YAML generated**

```python
# Check for special characters in descriptions
# Add validation after generation

import yaml

try:
    with open('schema_public.yml', 'r') as f:
        yaml.safe_load(f)
    print("✓ YAML is valid")
except yaml.YAMLError as e:
    print(f"✗ Invalid YAML: {e}")
```

**Problem: YAML not compatible with dbt**

```python
# Ensure you're using version 2 format (automatic in this library)
# Check the first line of generated files should be:
# version: 2
```

### Email Issues

**Problem: Email not sending (Gmail)**

```python
# 1. Enable 2FA in Google Account
# 2. Generate App Password (not regular password)
# 3. Use correct SMTP settings:

email_sender = EmailSender(
    smtp_host='smtp.gmail.com',  # Correct
    smtp_port=587,               # Correct (TLS)
    sender_email='you@gmail.com',
    sender_password='xxxx xxxx xxxx xxxx',  # 16-char app password
    use_tls=True,                # Important!
    use_ssl=False                # Don't use both TLS and SSL
)
```

**Problem: "SMTP AUTH extension not supported"**

```python
# Try without authentication for local SMTP servers
email_sender = EmailSender(
    smtp_host='localhost',
    smtp_port=25,
    sender_email='noreply@localhost',
    sender_password=None,  # No auth
    use_tls=False
)
```

### Database-Specific Issues

**ClickHouse: "Connection refused"**

```python
# Use port 9000 (native protocol), NOT 8123 (HTTP)
config = {
    'db_type': 'clickhouse',
    'port': 9000,  # Correct
    # NOT 8123
}
```

**Spanner: "Permission denied"**

```python
# Set up Google Cloud credentials
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/key.json'

# Or use gcloud
# gcloud auth application-default login
```

**MySQL: "Client does not support authentication protocol"**

```python
# Update user authentication
# mysql> ALTER USER 'user'@'host' IDENTIFIED WITH mysql_native_password BY 'password';
# mysql> FLUSH PRIVILEGES;
```

### Debugging

**Enable verbose logging:**

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,  # Show all messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Now all operations will log detailed information
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()
```

**Inspect metadata objects:**

```python
# Print metadata structure
import json

metadata_dict = db_metadata.to_dict()
print(json.dumps(metadata_dict, indent=2, default=str))
```

---

## Complete Examples

### Example 1: Daily dbt Documentation Update

**Scenario:** Update dbt YAML files every night with latest schema

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator
import os
from datetime import datetime

def daily_dbt_update():
    """Daily job to update dbt documentation."""
    
    # Configuration
    config = {
        'db_type': 'postgres',
        'host': os.getenv('WAREHOUSE_HOST'),
        'port': 5432,
        'database': 'analytics',
        'user': os.getenv('WAREHOUSE_USER'),
        'password': os.getenv('WAREHOUSE_PASSWORD')
    }
    
    print(f"Starting extraction at {datetime.now()}")
    
    # Extract only production schemas
    with MetadataExtractor(**config) as extractor:
        db_metadata = extractor.extract_all_schemas(
            schema_filter=['marts', 'staging']
        )
    
    print(f"Extracted {len(db_metadata.schemas)} schemas")
    
    # Generate YAML in dbt project
    dbt_models_dir = '/opt/dbt/ecommerce/models'
    yaml_gen = YAMLGenerator(output_dir=dbt_models_dir)
    files = yaml_gen.generate_yaml_files(db_metadata)
    
    print(f"Generated {len(files)} YAML files")
    for file in files:
        print(f"  - {file}")
    
    # Find documentation gaps
    tables_without_desc = yaml_gen.get_tables_without_descriptions(db_metadata)
    if tables_without_desc:
        print(f"\n⚠️  {len(tables_without_desc)} tables need descriptions:")
        for table in tables_without_desc[:5]:
            print(f"  - {table}")
    
    print(f"Completed at {datetime.now()}")

if __name__ == '__main__':
    daily_dbt_update()
```

### Example 2: Migration Validation

**Scenario:** Validate data migration from MySQL to PostgreSQL

```python
from data_dictionary_builder import SchemaComparator, EmailSender
import os

def validate_migration():
    """Validate MySQL to PostgreSQL migration."""
    
    # Source: MySQL
    source_config = {
        'db_type': 'mysql',
        'host': 'mysql-prod.example.com',
        'port': 3306,
        'database': 'ecommerce',
        'user': 'readonly',
        'password': os.getenv('MYSQL_PASSWORD')
    }
    
    # Destination: PostgreSQL
    dest_config = {
        'db_type': 'postgres',
        'host': 'postgres-warehouse.example.com',
        'port': 5432,
        'database': 'analytics',
        'user': 'readonly',
        'password': os.getenv('POSTGRES_PASSWORD')
    }
    
    # Compare
    comparator = SchemaComparator(
        source_config=source_config,
        destination_config=dest_config
    )
    
    report = comparator.compare_and_generate_report(
        source_schema_name='ecommerce',
        destination_schema_name='public',
        include_yaml_gaps=False
    )
    
    # Print results
    print("=== Migration Validation Report ===")
    print(f"Missing tables: {report['summary']['missing_tables_count']}")
    print(f"Missing columns: {report['summary']['missing_columns_count']}")
    print(f"Type mismatches: {report['summary']['type_mismatches_count']}")
    
    # Send email
    if report['summary']['missing_tables_count'] > 0 or \
       report['summary']['missing_columns_count'] > 0:
        
        email_sender = EmailSender(
            smtp_host='smtp.gmail.com',
            smtp_port=587,
            sender_email=os.getenv('SENDER_EMAIL'),
            sender_password=os.getenv('EMAIL_PASSWORD'),
            use_tls=True
        )
        
        email_sender.send_comparison_report(
            recipient_emails=['migration-team@example.com'],
            report=report,
            subject='⚠️ Migration Validation - Issues Found'
        )
        
        print("\n⚠️ Issues found - email sent to migration team")
    else:
        print("\n✓ Migration validated - all tables and columns present")

if __name__ == '__main__':
    validate_migration()
```

### Example 3: Multi-Database Documentation

**Scenario:** Document multiple databases and combine into single report

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator
import json

def document_all_databases():
    """Document all company databases."""
    
    databases = {
        'ecommerce_db': {
            'db_type': 'postgres',
            'host': 'ecommerce-db.example.com',
            'port': 5432,
            'database': 'ecommerce',
            'user': 'readonly',
            'password': 'password1'
        },
        'analytics_db': {
            'db_type': 'postgres',
            'host': 'analytics-db.example.com',
            'port': 5432,
            'database': 'analytics',
            'user': 'readonly',
            'password': 'password2'
        },
        'clickhouse_events': {
            'db_type': 'clickhouse',
            'host': 'clickhouse.example.com',
            'port': 9000,
            'database': 'events',
            'user': 'default',
            'password': 'password3'
        }
    }
    
    all_metadata = {}
    
    for db_name, config in databases.items():
        print(f"\nExtracting {db_name}...")
        
        try:
            with MetadataExtractor(**config) as extractor:
                db_metadata = extractor.extract_all_schemas()
                all_metadata[db_name] = db_metadata.to_dict()
                
                # Generate YAML per database
                yaml_gen = YAMLGenerator(output_dir=f'./docs/{db_name}')
                files = yaml_gen.generate_yaml_files(db_metadata)
                print(f"  Generated {len(files)} YAML files")
        
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            continue
    
    # Save combined metadata
    with open('all_databases_metadata.json', 'w') as f:
        json.dump(all_metadata, f, indent=2, default=str)
    
    print("\n✓ All databases documented")
    print(f"Total databases: {len(all_metadata)}")

if __name__ == '__main__':
    document_all_databases()
```

### Example 4: Data Quality Monitoring

**Scenario:** Monitor for schema changes and missing documentation

```python
from data_dictionary_builder import MetadataExtractor, YAMLGenerator, EmailSender
import json
import os
from datetime import datetime

def monitor_data_quality():
    """Monitor schema quality metrics."""
    
    config = {
        'db_type': 'postgres',
        'host': 'prod-db.example.com',
        'port': 5432,
        'database': 'production',
        'user': 'readonly',
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Extract metadata
    with MetadataExtractor(**config) as extractor:
        db_metadata = extractor.extract_all_schemas(
            schema_filter=['public', 'analytics']
        )
    
    # Generate YAML
    yaml_gen = YAMLGenerator(output_dir='./temp')
    
    # Quality checks
    tables_without_desc = yaml_gen.get_tables_without_descriptions(db_metadata)
    columns_without_desc = yaml_gen.get_columns_without_descriptions(db_metadata)
    
    # Calculate metrics
    total_tables = sum(len(s.tables) for s in db_metadata.schemas)
    total_columns = sum(
        sum(len(t.columns) for t in s.tables)
        for s in db_metadata.schemas
    )
    
    table_doc_coverage = (total_tables - len(tables_without_desc)) / total_tables * 100
    column_doc_coverage = (total_columns - len(columns_without_desc)) / total_columns * 100
    
    # Create report
    report = {
        'timestamp': datetime.now().isoformat(),
        'metrics': {
            'total_tables': total_tables,
            'total_columns': total_columns,
            'table_documentation_coverage': f'{table_doc_coverage:.1f}%',
            'column_documentation_coverage': f'{column_doc_coverage:.1f}%',
            'tables_missing_descriptions': len(tables_without_desc),
            'columns_missing_descriptions': len(columns_without_desc)
        },
        'tables_needing_docs': tables_without_desc[:10],  # Top 10
        'columns_needing_docs': columns_without_desc[:20]  # Top 20
    }
    
    # Save report
    with open(f'quality_report_{datetime.now():%Y%m%d}.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    # Print summary
    print("=== Data Quality Report ===")
    print(f"Table Documentation: {table_doc_coverage:.1f}%")
    print(f"Column Documentation: {column_doc_coverage:.1f}%")
    print(f"\n{len(tables_without_desc)} tables need descriptions")
    print(f"{len(columns_without_desc)} columns need descriptions")
    
    # Send alert if coverage is low
    if table_doc_coverage < 80 or column_doc_coverage < 50:
        # Send email alert
        print("\n⚠️ Documentation coverage below threshold!")

if __name__ == '__main__':
    monitor_data_quality()
```

---

## Summary

This user guide covered:

✅ **Installation and setup** - Getting started quickly  
✅ **Database connections** - All 5 supported database types  
✅ **Metadata extraction** - Complete and targeted extraction  
✅ **YAML generation** - dbt-compatible documentation  
✅ **Schema comparison** - Validate migrations and detect drift  
✅ **Email notifications** - Automated reporting  
✅ **Airflow integration** - Production orchestration  
✅ **Advanced usage** - Custom workflows and analysis  
✅ **Best practices** - Security, performance, code organization  
✅ **Troubleshooting** - Common issues and solutions  
✅ **Complete examples** - Real-world use cases  

### Getting Help

- **Documentation**: See DOCUMENTATION.md for API reference
- **Examples**: Check `examples/` directory for more code samples
- **Issues**: Report bugs or request features on GitHub

### Next Steps

1. ✅ Install the library
2. ✅ Test connection to your database
3. ✅ Run a simple extraction
4. ✅ Generate YAML files
5. ✅ Set up in Airflow (optional)
6. ✅ Schedule regular updates

**Happy documenting! 📚**
