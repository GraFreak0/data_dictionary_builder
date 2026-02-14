# DB Metadata Generator - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Database Connectors](#database-connectors)
5. [Metadata Extraction](#metadata-extraction)
6. [YAML Generation](#yaml-generation)
7. [Schema Comparison](#schema-comparison)
8. [Email Notifications](#email-notifications)
9. [Airflow Integration](#airflow-integration)
10. [API Reference](#api-reference)
11. [Examples](#examples)
12. [Troubleshooting](#troubleshooting)

## Overview

The DB Metadata Generator is a Python library designed to extract database metadata, generate dbt-compatible YAML files, and compare schemas between source and destination databases. It supports multiple database types including SQLite, PostgreSQL, MySQL, ClickHouse, and Google Cloud Spanner.

### Key Features

- **Multi-Database Support**: Works with SQLite, PostgreSQL, MySQL, ClickHouse, and Spanner
- **Metadata Extraction**: Extract complete schema metadata including tables, columns, constraints
- **YAML Generation**: Generate dbt-compatible YAML files for documentation
- **Schema Comparison**: Compare schemas between databases and identify differences
- **Email Reporting**: Send detailed comparison reports via email
- **Airflow Integration**: Ready-to-use DAG examples for orchestration

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/db-metadata-generator.git
cd db-metadata-generator

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

### Using pip (when published)

```bash
pip install db-metadata-generator
```

## Quick Start

### Basic Metadata Extraction

```python
from db_metadata_generator import MetadataExtractor, YAMLGenerator

# Configure database connection
config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'user': 'user',
    'password': 'password'
}

# Extract metadata
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()

# Generate YAML files
yaml_gen = YAMLGenerator(output_dir='./output')
yaml_gen.generate_yaml_files(db_metadata)
```

### Schema Comparison

```python
from db_metadata_generator import SchemaComparator

comparator = SchemaComparator(
    source_config={'db_type': 'postgres', 'host': 'source-db', ...},
    destination_config={'db_type': 'postgres', 'host': 'dest-db', ...}
)

report = comparator.compare_and_generate_report('public')
print(f"Missing tables: {report['summary']['missing_tables_count']}")
```

## Database Connectors

### SQLite

```python
config = {
    'db_type': 'sqlite',
    'database': '/path/to/database.db'
}
```

### PostgreSQL

```python
config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'user': 'postgres',
    'password': 'password'
}
```

### MySQL

```python
config = {
    'db_type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'database': 'mydb',
    'user': 'root',
    'password': 'password'
}
```

### ClickHouse

```python
config = {
    'db_type': 'clickhouse',
    'host': 'localhost',
    'port': 9000,  # Native protocol port
    'database': 'default',
    'user': 'default',
    'password': ''
}
```

### Google Cloud Spanner

```python
config = {
    'db_type': 'spanner',
    'instance_id': 'my-instance',
    'database_id': 'my-database',
    'project_id': 'my-gcp-project'  # Optional if using default credentials
}
```

## Metadata Extraction

### Extract All Schemas

```python
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas()
    
    for schema in db_metadata.schemas:
        print(f"Schema: {schema.name}")
        print(f"Tables: {len(schema.tables)}")
```

### Extract Specific Schemas

```python
with MetadataExtractor(**config) as extractor:
    db_metadata = extractor.extract_all_schemas(
        schema_filter=['public', 'analytics']
    )
```

### Extract Single Table

```python
with MetadataExtractor(**config) as extractor:
    table = extractor.extract_table('public', 'users')
    print(f"Columns: {len(table.columns)}")
    print(f"Primary Keys: {table.primary_keys}")
```

### Get Available Schemas and Tables

```python
with MetadataExtractor(**config) as extractor:
    schemas = extractor.get_schemas_list()
    tables = extractor.get_tables_list('public')
```

## YAML Generation

### Generate Per-Schema YAML Files

```python
yaml_gen = YAMLGenerator(output_dir='./dbt_models')
files = yaml_gen.generate_yaml_files(db_metadata)

# Files will be named: schema_public.yml, schema_analytics.yml, etc.
```

### Generate Single Combined YAML File

```python
yaml_gen = YAMLGenerator(output_dir='./dbt_models')
filepath = yaml_gen.generate_single_yaml(db_metadata, filename='models.yml')
```

### Find Missing Descriptions

```python
# Tables without descriptions
tables_without_desc = yaml_gen.get_tables_without_descriptions(db_metadata)

# Columns without descriptions
columns_without_desc = yaml_gen.get_columns_without_descriptions(db_metadata)
```

## Schema Comparison

### Compare Single Schema

```python
comparator = SchemaComparator(
    source_config=source_config,
    destination_config=dest_config,
    yaml_output_dir='./output'
)

result = comparator.compare_schemas('public')

print(f"Missing tables: {len(result.missing_tables)}")
print(f"Missing columns: {len(result.missing_columns)}")
print(f"Type mismatches: {len(result.type_mismatches)}")
```

### Generate Comprehensive Report

```python
report = comparator.compare_and_generate_report(
    source_schema_name='public',
    include_yaml_gaps=True
)

# Report includes:
# - Missing tables and columns
# - Type mismatches
# - Tables/columns without descriptions
```

### Compare All Schemas

```python
results = comparator.extract_and_compare_all(
    source_schemas=['public', 'analytics'],
    destination_schemas=['public', 'analytics']
)
```

## Email Notifications

### Send Comparison Report

```python
from db_metadata_generator import EmailSender

email_sender = EmailSender(
    smtp_host='smtp.gmail.com',
    smtp_port=587,
    sender_email='your-email@gmail.com',
    sender_password='your-app-password',
    use_tls=True
)

success = email_sender.send_comparison_report(
    recipient_emails=['team@example.com'],
    report=report,
    subject='Schema Comparison Report'
)
```

### Custom Email

```python
email_sender.send_email(
    recipient_emails=['user@example.com'],
    subject='Custom Subject',
    text_body='Plain text content',
    html_body='<h1>HTML content</h1>'
)
```

## Airflow Integration

### Basic DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from db_metadata_generator import MetadataExtractor, YAMLGenerator

def extract_metadata():
    with MetadataExtractor(**config) as extractor:
        return extractor.extract_all_schemas()

dag = DAG('metadata_extraction', ...)
task = PythonOperator(task_id='extract', python_callable=extract_metadata, dag=dag)
```

See `examples/airflow_dag_example.py` for a complete implementation.

## API Reference

### MetadataExtractor

**Methods:**
- `connect()`: Establish database connection
- `disconnect()`: Close database connection
- `extract_schema(schema_name)`: Extract single schema
- `extract_all_schemas(schema_filter=None)`: Extract all or filtered schemas
- `extract_table(schema_name, table_name)`: Extract single table
- `get_schemas_list()`: Get list of schema names
- `get_tables_list(schema_name)`: Get list of table names
- `test_connection()`: Test database connection

### YAMLGenerator

**Methods:**
- `generate_yaml_files(db_metadata)`: Generate per-schema YAML files
- `generate_single_yaml(db_metadata, filename)`: Generate single combined file
- `generate_schema_yaml(schema, filename)`: Generate YAML for single schema
- `get_tables_without_descriptions(db_metadata)`: Find undocumented tables
- `get_columns_without_descriptions(db_metadata)`: Find undocumented columns

### SchemaComparator

**Methods:**
- `compare_schemas(source_schema, dest_schema)`: Compare two schemas
- `compare_and_generate_report(source_schema, ...)`: Generate detailed report
- `extract_and_compare_all(source_schemas, dest_schemas)`: Batch comparison

### EmailSender

**Methods:**
- `send_comparison_report(recipients, report, subject)`: Send formatted report
- `send_email(recipients, subject, text_body, html_body)`: Send custom email

## Examples

See the `examples/` directory for complete working examples:

- `standalone_usage.py`: Various standalone usage patterns
- `airflow_dag_example.py`: Complete Airflow DAG implementation

## Troubleshooting

### Connection Issues

**PostgreSQL:**
```python
# Ensure PostgreSQL is running
# Check connection parameters
# Verify user has appropriate permissions
```

**MySQL:**
```python
# Common issue: Authentication plugin
# Solution: Use mysql_native_password or update PyMySQL
```

**ClickHouse:**
```python
# Use native protocol port (9000), not HTTP port (8123)
# Check firewall settings
```

**Spanner:**
```python
# Ensure Google Cloud credentials are configured
# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
```

### Performance Issues

For large databases:
- Use `schema_filter` to limit extraction
- Row count queries can be slow; they use approximate counts where possible
- Consider running during off-peak hours

### YAML Generation Issues

If YAML files are not valid:
- Check for special characters in descriptions
- Ensure table/column names don't conflict with dbt reserved words
- Validate YAML syntax using `yamllint`

### Email Sending Issues

**Gmail:**
- Use App Passwords, not regular password
- Enable "Less secure app access" (if not using 2FA)
- Check SMTP settings: smtp.gmail.com:587 with TLS

**Corporate Email:**
- Check firewall/proxy settings
- Verify SMTP server and port
- May need to whitelist application

## Best Practices

1. **Environment Variables**: Use `.env` file for sensitive credentials
2. **Schema Filtering**: Always filter to required schemas for performance
3. **Error Handling**: Wrap extraction in try/except for production use
4. **Logging**: Enable logging for debugging and monitoring
5. **Testing**: Test connections before full extraction
6. **Airflow**: Use XCom for passing data between tasks
7. **YAML**: Review generated files before committing to dbt project

## Support

For issues, questions, or contributions:
- GitHub Issues: [Create an issue](https://github.com/yourusername/db-metadata-generator/issues)
- Documentation: [Read the docs](https://github.com/yourusername/db-metadata-generator)
- Email: your-email@example.com

## License

MIT License - see LICENSE file for details
