"""
MySQL database connector implementation.
"""

import pymysql
from pymysql.cursors import DictCursor
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import TableMetadata, ColumnMetadata


class MySQLConnector(BaseConnector):
    """Connector for MySQL databases."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str, **kwargs):
        """
        Initialize MySQL connector.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Username
            password: Password
            **kwargs: Additional connection parameters
        """
        super().__init__(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.db_type = "mysql"
    
    def connect(self) -> None:
        """Establish connection to MySQL database."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                cursorclass=DictCursor,
                **{k: v for k, v in self.connection_params.items() 
                   if k not in ['host', 'port', 'database', 'user', 'password']}
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MySQL database: {e}")
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def get_schemas(self) -> List[str]:
        """
        Get list of all schemas (databases) in MySQL.
        
        Returns:
            List of schema names
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT SCHEMA_NAME 
            FROM information_schema.SCHEMATA 
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY SCHEMA_NAME
        """)
        schemas = [row['SCHEMA_NAME'] for row in cursor.fetchall()]
        cursor.close()
        return schemas if schemas else [self.database]
    
    def get_tables(self, schema_name: str) -> List[str]:
        """
        Get list of tables in a schema.
        
        Args:
            schema_name: Name of the schema (database)
            
        Returns:
            List of table names
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """, (schema_name,))
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
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
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                ORDINAL_POSITION,
                COLUMN_KEY,
                COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (schema_name, table_name))
        
        columns_info = cursor.fetchall()
        
        # Get primary keys
        primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get foreign keys
        foreign_keys = self.get_foreign_keys(schema_name, table_name)
        fk_map = {fk['column']: fk for fk in foreign_keys}
        
        columns = []
        for col_info in columns_info:
            column = ColumnMetadata(
                name=col_info['COLUMN_NAME'],
                data_type=col_info['DATA_TYPE'],
                is_nullable=col_info['IS_NULLABLE'] == 'YES',
                is_primary_key=col_info['COLUMN_NAME'] in primary_keys,
                default_value=col_info['COLUMN_DEFAULT'],
                character_maximum_length=col_info['CHARACTER_MAXIMUM_LENGTH'],
                numeric_precision=col_info['NUMERIC_PRECISION'],
                numeric_scale=col_info['NUMERIC_SCALE'],
                ordinal_position=col_info['ORDINAL_POSITION'],
                description=col_info['COLUMN_COMMENT'] if col_info['COLUMN_COMMENT'] else None
            )
            
            # Check if this column is a foreign key
            if col_info['COLUMN_NAME'] in fk_map:
                column.is_foreign_key = True
                column.foreign_key_table = fk_map[col_info['COLUMN_NAME']]['referenced_table']
                column.foreign_key_column = fk_map[col_info['COLUMN_NAME']]['referenced_column']
            
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
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """, (schema_name, table_name))
        
        primary_keys = [row['COLUMN_NAME'] for row in cursor.fetchall()]
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
        cursor.execute("""
            SELECT
                kcu.COLUMN_NAME AS column_name,
                kcu.REFERENCED_TABLE_NAME AS referenced_table,
                kcu.REFERENCED_COLUMN_NAME AS referenced_column
            FROM information_schema.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_SCHEMA = %s
            AND kcu.TABLE_NAME = %s
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """, (schema_name, table_name))
        
        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'column': row['column_name'],
                'referenced_table': row['referenced_table'],
                'referenced_column': row['referenced_column']
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
        cursor = self.connection.cursor()
        
        # Get table comment
        cursor.execute("""
            SELECT TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (schema_name, table_name))
        
        result = cursor.fetchone()
        description = result['TABLE_COMMENT'] if result and result['TABLE_COMMENT'] else None
        
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type='BASE TABLE',
            description=description
        )
        
        # Get columns
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get row count
        table_metadata.row_count = self.get_table_row_count(schema_name, table_name)
        
        cursor.close()
        return table_metadata
    
    def get_database_version(self) -> Optional[str]:
        """
        Get MySQL version.
        
        Returns:
            MySQL version string
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT VERSION()")
        result = cursor.fetchone()
        version = list(result.values())[0] if result else None
        cursor.close()
        return version
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get approximate row count for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Row count
        """
        try:
            cursor = self.connection.cursor()
            # Use information_schema for approximate count
            cursor.execute("""
                SELECT TABLE_ROWS
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (schema_name, table_name))
            
            result = cursor.fetchone()
            cursor.close()
            return result['TABLE_ROWS'] if result else None
        except Exception:
            return None
