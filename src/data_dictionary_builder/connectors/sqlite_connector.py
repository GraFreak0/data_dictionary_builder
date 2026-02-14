"""
SQLite database connector implementation.
"""

import sqlite3
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import TableMetadata, ColumnMetadata


class SQLiteConnector(BaseConnector):
    """Connector for SQLite databases."""
    
    def __init__(self, database: str, **kwargs):
        """
        Initialize SQLite connector.
        
        Args:
            database: Path to SQLite database file
            **kwargs: Additional connection parameters
        """
        super().__init__(database=database, **kwargs)
        self.database = database
        self.db_type = "sqlite"
    
    def connect(self) -> None:
        """Establish connection to SQLite database."""
        try:
            self.connection = sqlite3.connect(self.database)
            self.connection.row_factory = sqlite3.Row
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQLite database: {e}")
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def get_schemas(self) -> List[str]:
        """
        Get list of schemas (SQLite uses 'main' as default schema).
        
        Returns:
            List containing 'main' schema
        """
        # SQLite doesn't have multiple schemas in the same way as other databases
        # ATTACH DATABASE can create additional schemas, but we'll focus on main
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA database_list")
        schemas = [row[1] for row in cursor.fetchall()]
        cursor.close()
        return schemas if schemas else ['main']
    
    def get_tables(self, schema_name: str = 'main') -> List[str]:
        """
        Get list of tables in the schema.
        
        Args:
            schema_name: Name of the schema (default: 'main')
            
        Returns:
            List of table names
        """
        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT name 
            FROM {schema_name}.sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """
        Get column metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of ColumnMetadata objects
        """
        cursor = self.connection.cursor()
        
        # Get column information
        cursor.execute(f"PRAGMA {schema_name}.table_info('{table_name}')")
        columns_info = cursor.fetchall()
        
        # Get foreign key information
        cursor.execute(f"PRAGMA {schema_name}.foreign_key_list('{table_name}')")
        foreign_keys_info = cursor.fetchall()
        
        # Create a mapping of column names to foreign key info
        fk_map = {}
        for fk in foreign_keys_info:
            fk_map[fk[3]] = {  # fk[3] is the 'from' column
                'table': fk[2],      # fk[2] is the referenced table
                'column': fk[4]      # fk[4] is the referenced column
            }
        
        columns = []
        for col_info in columns_info:
            # SQLite PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            column = ColumnMetadata(
                name=col_info[1],
                data_type=col_info[2] if col_info[2] else 'TEXT',
                is_nullable=not bool(col_info[3]),
                is_primary_key=bool(col_info[5]),
                default_value=col_info[4],
                ordinal_position=col_info[0]
            )
            
            # Check if this column is a foreign key
            if col_info[1] in fk_map:
                column.is_foreign_key = True
                column.foreign_key_table = fk_map[col_info[1]]['table']
                column.foreign_key_column = fk_map[col_info[1]]['column']
            
            columns.append(column)
        
        cursor.close()
        return columns
    
    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA {schema_name}.table_info('{table_name}')")
        columns_info = cursor.fetchall()
        
        primary_keys = [col[1] for col in columns_info if col[5] > 0]  # col[5] is pk flag
        cursor.close()
        return primary_keys
    
    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of foreign key dictionaries
        """
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA {schema_name}.foreign_key_list('{table_name}')")
        fk_info = cursor.fetchall()
        
        foreign_keys = []
        for fk in fk_info:
            foreign_keys.append({
                'column': fk[3],              # from column
                'referenced_table': fk[2],    # table
                'referenced_column': fk[4]    # to column
            })
        
        cursor.close()
        return foreign_keys
    
    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get complete metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type='BASE TABLE'
        )
        
        # Get columns
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get row count
        table_metadata.row_count = self.get_table_row_count(schema_name, table_name)
        
        return table_metadata
    
    def get_database_version(self) -> Optional[str]:
        """
        Get SQLite version.
        
        Returns:
            SQLite version string
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]
        cursor.close()
        return version
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get row count for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Row count
        """
        try:
            cursor = self.connection.cursor()
            full_table_name = f"{schema_name}.{table_name}" if schema_name != 'main' else table_name
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception:
            return None
