"""
Data models for database metadata structures.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ColumnMetadata:
    """Represents metadata for a database column."""
    
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    default_value: Optional[str] = None
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    description: Optional[str] = None
    ordinal_position: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert column metadata to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "is_nullable": self.is_nullable,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "foreign_key_table": self.foreign_key_table,
            "foreign_key_column": self.foreign_key_column,
            "default_value": self.default_value,
            "character_maximum_length": self.character_maximum_length,
            "numeric_precision": self.numeric_precision,
            "numeric_scale": self.numeric_scale,
            "description": self.description,
            "ordinal_position": self.ordinal_position,
        }


@dataclass
class TableMetadata:
    """Represents metadata for a database table."""
    
    name: str
    schema_name: str
    table_type: str = "BASE TABLE"  # BASE TABLE, VIEW, etc.
    columns: List[ColumnMetadata] = field(default_factory=list)
    description: Optional[str] = None
    row_count: Optional[int] = None
    created_date: Optional[datetime] = None
    last_modified_date: Optional[datetime] = None
    primary_keys: List[str] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_column(self, column: ColumnMetadata) -> None:
        """Add a column to the table."""
        self.columns.append(column)
    
    def get_column(self, column_name: str) -> Optional[ColumnMetadata]:
        """Get a column by name."""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert table metadata to dictionary."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "table_type": self.table_type,
            "columns": [col.to_dict() for col in self.columns],
            "description": self.description,
            "row_count": self.row_count,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "last_modified_date": self.last_modified_date.isoformat() if self.last_modified_date else None,
            "primary_keys": self.primary_keys,
            "indexes": self.indexes,
        }


@dataclass
class SchemaMetadata:
    """Represents metadata for a database schema."""
    
    name: str
    tables: List[TableMetadata] = field(default_factory=list)
    description: Optional[str] = None
    owner: Optional[str] = None
    
    def add_table(self, table: TableMetadata) -> None:
        """Add a table to the schema."""
        self.tables.append(table)
    
    def get_table(self, table_name: str) -> Optional[TableMetadata]:
        """Get a table by name."""
        for table in self.tables:
            if table.name == table_name:
                return table
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema metadata to dictionary."""
        return {
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
            "description": self.description,
            "owner": self.owner,
        }


@dataclass
class DatabaseMetadata:
    """Represents metadata for an entire database."""
    
    database_name: str
    database_type: str
    schemas: List[SchemaMetadata] = field(default_factory=list)
    version: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    extraction_timestamp: datetime = field(default_factory=datetime.now)
    
    def add_schema(self, schema: SchemaMetadata) -> None:
        """Add a schema to the database."""
        self.schemas.append(schema)

    def get_schema(self, schema_name: str) -> Optional[SchemaMetadata]:
        """Get a schema by name."""
        for schema in self.schemas:
            if schema.name == schema_name:
                return schema
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert database metadata to dictionary."""
        return {
            "database_name": self.database_name,
            "database_type": self.database_type,
            "schemas": [schema.to_dict() for schema in self.schemas],
            "version": self.version,
            "host": self.host,
            "port": self.port,
            "extraction_timestamp": self.extraction_timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseMetadata":
        """
        Reconstruct a ``DatabaseMetadata`` instance from a plain dictionary.

        This is the inverse of :meth:`to_dict` and is intended for deserialising
        objects that were serialised for Airflow XCom, JSON storage, or
        inter-process transfer.

        Parameters
        ----------
        data : dict
            Dictionary as produced by :meth:`to_dict`.

        Returns
        -------
        DatabaseMetadata
        """
        ts_raw = data.get("extraction_timestamp")
        try:
            ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now()
        except (ValueError, TypeError):
            ts = datetime.now()

        instance = cls(
            database_name=data["database_name"],
            database_type=data["database_type"],
            version=data.get("version"),
            host=data.get("host"),
            port=data.get("port"),
            extraction_timestamp=ts,
        )

        for schema_data in data.get("schemas", []):
            schema = SchemaMetadata(
                name=schema_data["name"],
                description=schema_data.get("description"),
                owner=schema_data.get("owner"),
            )
            for table_data in schema_data.get("tables", []):
                table = TableMetadata(
                    name=table_data["name"],
                    schema_name=table_data["schema_name"],
                    table_type=table_data.get("table_type", "BASE TABLE"),
                    description=table_data.get("description"),
                    row_count=table_data.get("row_count"),
                    primary_keys=table_data.get("primary_keys", []),
                    indexes=table_data.get("indexes", []),
                )
                for col_data in table_data.get("columns", []):
                    table.add_column(ColumnMetadata(
                        name=col_data["name"],
                        data_type=col_data["data_type"],
                        is_nullable=col_data.get("is_nullable", True),
                        is_primary_key=col_data.get("is_primary_key", False),
                        is_foreign_key=col_data.get("is_foreign_key", False),
                        foreign_key_table=col_data.get("foreign_key_table"),
                        foreign_key_column=col_data.get("foreign_key_column"),
                        default_value=col_data.get("default_value"),
                        character_maximum_length=col_data.get("character_maximum_length"),
                        numeric_precision=col_data.get("numeric_precision"),
                        numeric_scale=col_data.get("numeric_scale"),
                        description=col_data.get("description"),
                        ordinal_position=col_data.get("ordinal_position", 0),
                    ))
                schema.add_table(table)
            instance.add_schema(schema)

        return instance


@dataclass
class ComparisonResult:
    """Represents the result of a schema comparison."""
    
    missing_tables: List[Dict[str, str]] = field(default_factory=list)
    missing_columns: List[Dict[str, str]] = field(default_factory=list)
    type_mismatches: List[Dict[str, Any]] = field(default_factory=list)
    tables_without_descriptions: List[str] = field(default_factory=list)
    columns_without_descriptions: List[Dict[str, str]] = field(default_factory=list)
    
    def has_differences(self) -> bool:
        """Check if there are any differences."""
        return bool(
            self.missing_tables
            or self.missing_columns
            or self.type_mismatches
            or self.tables_without_descriptions
            or self.columns_without_descriptions
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert comparison result to dictionary."""
        return {
            "missing_tables": self.missing_tables,
            "missing_columns": self.missing_columns,
            "type_mismatches": self.type_mismatches,
            "tables_without_descriptions": self.tables_without_descriptions,
            "columns_without_descriptions": self.columns_without_descriptions,
        }
