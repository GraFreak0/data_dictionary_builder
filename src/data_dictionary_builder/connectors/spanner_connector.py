"""
Google Cloud Spanner database connector implementation.
"""

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


class SpannerConnector(BaseConnector):
    """Connector for Google Cloud Spanner databases."""

    def __init__(
        self,
        instance_id: str,
        database_id: Optional[str] = None,
        databases: Optional[List[str]] = None,
        project_id: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize the Spanner connector.

        Args:
            instance_id: Cloud Spanner instance ID.
            database_id: Single database ID.  When provided the connector
                operates in single-database mode (backward-compatible default).
                Omit to use multi-database mode.
            databases: Explicit list of database IDs to extract.  Used in
                multi-database mode when ``database_id`` is not set.  If
                neither ``database_id`` nor ``databases`` is given the
                connector lists every database in the instance automatically.
            project_id: GCP project ID.  Falls back to Application Default
                Credentials when omitted.
        """
        super().__init__(
            instance_id=instance_id,
            database_id=database_id,
            databases=databases,
            project_id=project_id,
            **kwargs,
        )
        self.instance_id = instance_id
        self.database_id = database_id
        self.databases = databases          # explicit list for multi-db mode
        self.project_id = project_id
        self.db_type = "spanner"
        self.client = None
        self.instance = None
        self.database = None                # active single-db handle (single-db mode)

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Establish connection to the Spanner instance (and optionally a specific database)."""
        try:
            self.client = (
                spanner.Client(project=self.project_id)
                if self.project_id
                else spanner.Client()
            )
            self.instance = self.client.instance(self.instance_id)

            if self.database_id:
                # Single-database mode — connect immediately and verify
                self.database = self.instance.database(self.database_id)
                with self.database.snapshot() as snapshot:
                    list(snapshot.execute_sql("SELECT 1"))
                self.connection = self.database
            else:
                # Multi-database mode — just mark as connected via the instance handle
                self.connection = self.instance

        except Exception as exc:
            raise ConnectionError(f"Failed to connect to Spanner: {exc}") from exc

    def disconnect(self) -> None:
        """Close the Spanner client."""
        if self.client:
            self.client.close()
        self.client = None
        self.instance = None
        self.database = None
        self.connection = None

    def test_connection(self) -> bool:
        try:
            self.connect()
            ok = self.connection is not None
            self.disconnect()
            return ok
        except Exception:
            return False

    # ── Schema / database enumeration ─────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        """
        Return the list of Spanner databases to extract.

        Single-database mode (``database_id`` set):
            Returns ``['public']`` — backward-compatible with v0.1.5 and earlier.

        Multi-database mode (``databases`` list or no ``database_id``):
            Returns the explicit ``databases`` list when provided, otherwise
            lists every database in the instance via ``instance.list_databases()``.
        """
        if self.database_id:
            return ['public']

        if self.databases:
            return list(self.databases)

        # Auto-discover all databases in the instance
        try:
            return [db.database_id for db in self.instance.list_databases()]
        except Exception:
            return []

    # ── Schema metadata extraction ────────────────────────────────────────────

    def extract_schema_metadata(
        self,
        schema_name: str,
        include_views: bool = False,
    ) -> SchemaMetadata:
        """
        Extract complete metadata for a schema / database.

        In single-database mode ``schema_name`` is ``'public'`` and the
        already-connected ``self.database`` is used.

        In multi-database mode ``schema_name`` is a Spanner database ID; the
        method opens a fresh handle to that database for this call.
        """
        if self.database_id:
            active_db = self.database
        else:
            active_db = self.instance.database(schema_name)

        schema_metadata = SchemaMetadata(name=schema_name)

        for table_name in self._get_tables_from_db(active_db):
            table_meta = self._get_table_metadata_from_db(active_db, schema_name, table_name)
            schema_metadata.add_table(table_meta)

        return schema_metadata

    # ── Tables ────────────────────────────────────────────────────────────────

    def get_tables(self, schema_name: str = 'public') -> List[str]:
        """Return all base-table names.  Uses the active ``self.database``."""
        return self._get_tables_from_db(self.database)

    @staticmethod
    def _get_tables_from_db(db) -> List[str]:
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = ''
                ORDER BY TABLE_NAME
            """)
            return [row[0] for row in results]

    # ── Table metadata ────────────────────────────────────────────────────────

    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """Return metadata for a single table using ``self.database``."""
        return self._get_table_metadata_from_db(self.database, schema_name, table_name)

    def _get_table_metadata_from_db(self, db, schema_name: str, table_name: str) -> TableMetadata:
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type='BASE TABLE',
        )
        for col in self._get_columns_from_db(db, schema_name, table_name):
            table_metadata.add_column(col)
        table_metadata.primary_keys = self._get_primary_keys_from_db(db, table_name)
        table_metadata.row_count = self._get_row_count_from_db(db, table_name)
        return table_metadata

    # ── Columns ───────────────────────────────────────────────────────────────

    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Return column metadata using ``self.database``."""
        return self._get_columns_from_db(self.database, schema_name, table_name)

    @staticmethod
    def _get_columns_from_db(db, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE,
                       COLUMN_DEFAULT, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @table_name
                ORDER BY ORDINAL_POSITION
                """,
                params={'table_name': table_name},
                param_types={'table_name': param_types.STRING},
            )
            columns_info = list(results)

        pks = set(SpannerConnector._get_primary_keys_from_db(db, table_name))
        columns = []
        for col_name, spanner_type, is_nullable_str, default_value, ordinal in columns_info:
            columns.append(ColumnMetadata(
                name=col_name,
                data_type=spanner_type,
                is_nullable=is_nullable_str == 'YES',
                is_primary_key=col_name in pks,
                default_value=default_value,
                ordinal_position=ordinal,
            ))
        return columns

    # ── Primary keys ──────────────────────────────────────────────────────────

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Return primary key column names using ``self.database``."""
        return self._get_primary_keys_from_db(self.database, table_name)

    @staticmethod
    def _get_primary_keys_from_db(db, table_name: str) -> List[str]:
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.INDEX_COLUMNS
                WHERE TABLE_NAME = @table_name AND INDEX_NAME = 'PRIMARY_KEY'
                ORDER BY ORDINAL_POSITION
                """,
                params={'table_name': table_name},
                param_types={'table_name': param_types.STRING},
            )
            return [row[0] for row in results]

    # ── Foreign keys ──────────────────────────────────────────────────────────

    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """Return foreign key relationships using ``self.database``."""
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
                SELECT kcu.COLUMN_NAME,
                       kcu2.TABLE_NAME AS REFERENCED_TABLE,
                       kcu2.COLUMN_NAME AS REFERENCED_COLUMN
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                    ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
                WHERE kcu.TABLE_NAME = @table_name
                """,
                params={'table_name': table_name},
                param_types={'table_name': param_types.STRING},
            )
            return [
                {'column': row[0], 'referenced_table': row[1], 'referenced_column': row[2]}
                for row in results
            ]

    # ── Utilities ─────────────────────────────────────────────────────────────

    def get_database_version(self) -> Optional[str]:
        return "Google Cloud Spanner"

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        return self._get_row_count_from_db(self.database, table_name)

    @staticmethod
    def _get_row_count_from_db(db, table_name: str) -> Optional[int]:
        try:
            with db.snapshot() as snapshot:
                for row in snapshot.execute_sql(f"SELECT COUNT(*) FROM `{table_name}`"):
                    return row[0]
            return None
        except Exception:
            return None
