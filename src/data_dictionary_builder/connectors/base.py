"""
Base connector class for database connections.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


class BaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    def __init__(self, **kwargs):
        """
        Initialize the connector with connection parameters.
        
        Args:
            **kwargs: Database-specific connection parameters
        """
        self.connection_params = kwargs
        self.connection = None
        self.db_type = "unknown"
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def get_schemas(self) -> List[str]:
        """
        Get list of all schemas in the database.
        
        Returns:
            List of schema names
        """
        pass
    
    @abstractmethod
    def get_tables(self, schema_name: str) -> List[str]:
        """
        Get list of all tables in a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of table names
        """
        pass
    
    @abstractmethod
    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get metadata for a specific table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        pass
    
    @abstractmethod
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """
        Get column metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of ColumnMetadata objects
        """
        pass
    
    @abstractmethod
    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        pass
    
    @abstractmethod
    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of foreign key dictionaries with column, referenced_table, referenced_column
        """
        pass
    
    def get_database_version(self) -> Optional[str]:
        """
        Get the database version.
        
        Returns:
            Database version string or None
        """
        return None
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get the approximate row count for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Row count or None if not available
        """
        try:
            if self.connection:
                cursor = self.connection.cursor()
                # This is a basic implementation, override in specific connectors for better performance
                full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                result = cursor.fetchone()
                cursor.close()
                return result[0] if result else None
        except Exception:
            return None
    
    def get_views(self, schema_name: str) -> List[str]:
        """
        Get list of all views in a schema.

        Override in subclasses to return real view names.  The default
        implementation returns an empty list so connectors that have not
        yet implemented view support degrade gracefully.

        Args:
            schema_name: Name of the schema

        Returns:
            List of view names
        """
        return []

    def extract_schema_metadata(
        self,
        schema_name: str,
        include_views: bool = False,
    ) -> SchemaMetadata:
        """
        Extract complete metadata for a schema.

        Args:
            schema_name: Name of the schema
            include_views: When True, views are extracted in addition to
                base tables.  Defaults to False.

        Returns:
            SchemaMetadata object
        """
        schema_metadata = SchemaMetadata(name=schema_name)

        for table_name in self.get_tables(schema_name):
            table_metadata = self.get_table_metadata(schema_name, table_name)
            schema_metadata.add_table(table_metadata)

        if include_views:
            for view_name in self.get_views(schema_name):
                view_metadata = self.get_table_metadata(schema_name, view_name)
                view_metadata.table_type = "VIEW"
                schema_metadata.add_table(view_metadata)

        return schema_metadata
    
    def test_connection(self) -> bool:
        """
        Test if the connection is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.connect()
            result = self.connection is not None
            self.disconnect()
            return result
        except Exception:
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
