# Database Metadata Generator

A Python library for extracting database metadata and generating dbt-compatible YAML files with schema comparison capabilities.

## Features

- Connect to multiple database types (SQLite, PostgreSQL, MySQL, ClickHouse, Spanner)
- Extract complete schema metadata (tables, columns, data types, constraints)
- Generate dbt-compatible YAML files (one per schema)
- Compare source and destination database schemas
- Email reporting for schema differences and missing descriptions
- Airflow-compatible design for orchestration

## Project Structure

```
db_metadata_generator/
├── README.md
├── requirements.txt
├── setup.py
├── src/
│   ├── __init__.py
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── sqlite_connector.py
│   │   ├── postgres_connector.py
│   │   ├── mysql_connector.py
│   │   ├── clickhouse_connector.py
│   │   └── spanner_connector.py
│   ├── metadata/
│   │   ├── __init__.py
│   │   ├── extractor.py
│   │   └── models.py
│   ├── yaml_generator/
│   │   ├── __init__.py
│   │   └── generator.py
│   ├── comparison/
│   │   ├── __init__.py
│   │   └── comparator.py
│   └── notifications/
│       ├── __init__.py
│       └── email_sender.py
├── examples/
│   ├── airflow_dag_example.py
│   └── standalone_usage.py
└── tests/
    └── __init__.py
```

## Installation

```bash
pip install -r requirements.txt
pip install -e .
```

## Usage

### Basic Usage

```python
from db_metadata_generator import MetadataExtractor, YAMLGenerator

# Extract metadata
extractor = MetadataExtractor(
    db_type='postgres',
    host='localhost',
    port=5432,
    database='mydb',
    user='user',
    password='pass'
)

metadata = extractor.extract_all_schemas()

# Generate YAML files
generator = YAMLGenerator(output_dir='./dbt_models')
generator.generate_yaml_files(metadata)
```

### With Schema Comparison

```python
from db_metadata_generator import SchemaComparator

comparator = SchemaComparator(
    source_config={...},
    destination_config={...},
    yaml_output_dir='./dbt_models'
)

report = comparator.compare_and_report(
    email_to='team@example.com',
    smtp_config={...}
)
```

## Airflow Integration

See `examples/airflow_dag_example.py` for complete DAG implementation.

## Configuration

Database connection configurations should include:
- `db_type`: sqlite, postgres, mysql, clickhouse, spanner
- `host`: Database host
- `port`: Database port
- `database`: Database name
- `user`: Username
- `password`: Password
- Additional driver-specific parameters

## License

MIT
