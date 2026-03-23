"""
MongoDB database connector implementation.
"""

from typing import List, Optional, Dict, Any, Union
import logging
from .base import BaseConnector
from ..metadata.models import TableMetadata, ColumnMetadata

# Lazy import for pymongo
pymongo = None

class MongoDBConnector(BaseConnector):
    """Connector for MongoDB databases."""

    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 27017, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None, 
                 auth_source: str = "admin", 
                 connection_string: Optional[str] = None,
                 database: Optional[str] = None,
                 **kwargs):
        """
        Initialize MongoDB connector.

        Args:
            host: MongoDB server host
            port: MongoDB server port
            username: Username for authentication
            password: Password for authentication
            auth_source: Database used for authentication
            connection_string: Full MongoDB URI (optional, overrides host/port/auth)
            database: Default database to use
            **kwargs: Additional connection parameters
        """
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.connection_string = connection_string
        self.default_database = database
        self.db_type = "mongodb"
        self.client = None

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        global pymongo
        if pymongo is None:
            try:
                import pymongo as pm
                pymongo = pm
            except ImportError:
                raise ImportError(
                    "MongoDB connector requires 'pymongo'. "
                    "Install it with: pip install pymongo"
                )

        try:
            if self.connection_string:
                self.client = pymongo.MongoClient(self.connection_string)
            else:
                params = {
                    "host": self.host,
                    "port": self.port,
                }
                if self.username and self.password:
                    params["username"] = self.username
                    params["password"] = self.password
                    params["authSource"] = self.auth_source
                
                # Merge additional kwargs into MongoClient params
                params.update(self.connection_params)
                self.client = pymongo.MongoClient(**params)
            
            self.connection = self.client
            # Trigger a simple command to verify connection
            self.client.admin.command('ping')
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

    def disconnect(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.connection = None

    def get_schemas(self) -> List[str]:
        """
        Get list of databases (mapped as schemas).

        Returns:
            List of database names
        """
        return self.client.list_database_names()

    def get_tables(self, schema_name: str) -> List[str]:
        """
        Get list of collections in a database (mapped as tables).

        Args:
            schema_name: Name of the database

        Returns:
            List of collection names
        """
        db = self.client[schema_name]
        return db.list_collection_names()

    def _infer_type_from_sample(self, val: Any) -> str:
        """Helper to infer a string type name from a MongoDB value."""
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "BOOLEAN"
        if isinstance(val, int):
            return "INTEGER"
        if isinstance(val, float):
            return "DOUBLE"
        if isinstance(val, str):
            return "STRING"
        if isinstance(val, list):
            return "ARRAY"
        if isinstance(val, dict):
            return "OBJECT"
        if hasattr(val, '__class__'):
            return val.__class__.__name__.upper()
        return "UNKNOWN"

    def get_columns(self, schema_name: str, table_name: str, sample_size: int = 100) -> List[ColumnMetadata]:
        """
        Get field metadata for a collection by sampling documents.

        Args:
            schema_name: Name of the database
            table_name: Name of the collection
            sample_size: Number of documents to scan to find fields

        Returns:
            List of ColumnMetadata objects
        """
        db = self.client[schema_name]
        collection = db[table_name]
        
        # We need to sample documents to discover the schema
        sample_docs = list(collection.find().limit(sample_size))
        
        field_info = {}
        
        # Scan fields and types
        for doc in sample_docs:
            for field_name, value in doc.items():
                if field_name not in field_info:
                    field_info[field_name] = {
                        "types": set(),
                        "nullable": False
                    }
                
                field_info[field_name]["types"].add(self._infer_type_from_sample(value))
                if value is None:
                    field_info[field_name]["nullable"] = True
        
        # If no documents, we might not find any fields
        if not field_info and "_id" not in field_info:
            # Default to at least _id if we know it exists (standard MongoDB)
            field_info["_id"] = {"types": {"OBJECTID"}, "nullable": False}

        columns = []
        for i, (field_name, info) in enumerate(field_info.items()):
            # Handle multiple types (common in schemaless DBs)
            types_list = sorted(list(info["types"]))
            data_type = " | ".join(types_list) if types_list else "UNKNOWN"
            
            column = ColumnMetadata(
                name=field_name,
                data_type=data_type,
                is_nullable=info["nullable"],
                is_primary_key=(field_name == "_id"),
                ordinal_position=i
            )
            columns.append(column)
            
        return columns

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns (always _id for MongoDB).

        Returns:
            List containing '_id'
        """
        return ["_id"]

    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships (not natively supported in MongoDB).

        Returns:
            Empty list
        """
        return []

    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get complete metadata for a collection.

        Args:
            schema_name: Name of the database
            table_name: Name of the collection

        Returns:
            TableMetadata object
        """
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type='COLLECTION'
        )
        
        # Get columns (fields)
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = ["_id"]
        
        # Get row count (document count)
        table_metadata.row_count = self.get_table_row_count(schema_name, table_name)
        
        return table_metadata

    def get_database_version(self) -> Optional[str]:
        """
        Get MongoDB version.

        Returns:
            Version string
        """
        server_info = self.client.server_info()
        return server_info.get("version")

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get document count for a collection.

        Args:
            schema_name: Name of the database
            table_name: Name of the collection

        Returns:
            Row count
        """
        try:
            db = self.client[schema_name]
            return db[table_name].estimated_document_count()
        except Exception:
            return None
