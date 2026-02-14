"""
Database connectors module.
"""

from .base import BaseConnector
from .sqlite_connector import SQLiteConnector
from .postgres_connector import PostgresConnector
from .mysql_connector import MySQLConnector
from .clickhouse_connector import ClickHouseConnector
from .spanner_connector import SpannerConnector

__all__ = [
    "BaseConnector",
    "SQLiteConnector",
    "PostgresConnector",
    "MySQLConnector",
    "ClickHouseConnector",
    "SpannerConnector",
]


def get_connector(db_type: str, **kwargs) -> BaseConnector:
    """
    Factory function to get the appropriate connector based on database type.
    
    Args:
        db_type: Type of database ('sqlite', 'postgres', 'mysql', 'clickhouse', 'spanner')
        **kwargs: Connection parameters for the specific database type
        
    Returns:
        Instance of the appropriate connector
        
    Raises:
        ValueError: If database type is not supported
        
    Examples:
        >>> connector = get_connector('postgres', host='localhost', port=5432, 
        ...                          database='mydb', user='user', password='pass')
        >>> connector = get_connector('sqlite', database='/path/to/db.sqlite')
    """
    db_type = db_type.lower()
    
    connectors = {
        'sqlite': SQLiteConnector,
        'postgres': PostgresConnector,
        'postgresql': PostgresConnector,
        'mysql': MySQLConnector,
        'clickhouse': ClickHouseConnector,
        'spanner': SpannerConnector,
    }
    
    if db_type not in connectors:
        raise ValueError(
            f"Unsupported database type: {db_type}. "
            f"Supported types: {', '.join(connectors.keys())}"
        )
    
    return connectors[db_type](**kwargs)
