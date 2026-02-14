"""
DB Metadata Generator - A library for extracting database metadata and generating dbt-compatible YAML files.
"""

from .metadata.extractor import MetadataExtractor
from .metadata.models import DatabaseMetadata, SchemaMetadata, TableMetadata, ColumnMetadata
from .yaml_generator.generator import YAMLGenerator
from .comparison.comparator import SchemaComparator
from .notifications.email_sender import EmailSender

__version__ = "1.0.0"
__all__ = [
    "MetadataExtractor",
    "DatabaseMetadata",
    "SchemaMetadata",
    "TableMetadata",
    "ColumnMetadata",
    "YAMLGenerator",
    "SchemaComparator",
    "EmailSender",
]
