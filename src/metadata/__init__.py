"""
Metadata module for database metadata extraction and models.
"""

from .extractor import MetadataExtractor
from .models import (
    DatabaseMetadata,
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    ComparisonResult
)

__all__ = [
    "MetadataExtractor",
    "DatabaseMetadata",
    "SchemaMetadata",
    "TableMetadata",
    "ColumnMetadata",
    "ComparisonResult",
]
