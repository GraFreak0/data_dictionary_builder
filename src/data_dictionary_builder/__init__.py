"""
DB Metadata Generator - A library for extracting database metadata and generating dbt-compatible YAML files.
"""

from .metadata.extractor import MetadataExtractor
from .metadata.models import DatabaseMetadata, SchemaMetadata, TableMetadata, ColumnMetadata
from .yaml_generator.generator import YAMLGenerator
from .comparison.comparator import SchemaComparator
from .notifications.email_sender import EmailSender
from .notifications.slack_notifier import SlackNotifier
from .DDHelper import DDHelper
from .timer import ExecutionTimer

__version__ = "0.1.4"
__all__ = [
    "MetadataExtractor",
    "DatabaseMetadata",
    "SchemaMetadata",
    "TableMetadata",
    "ColumnMetadata",
    "YAMLGenerator",
    "SchemaComparator",
    "EmailSender",
    "SlackNotifier",
    "DDHelper",
    "ExecutionTimer",
]
