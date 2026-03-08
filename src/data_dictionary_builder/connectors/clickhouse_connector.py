"""
ClickHouse database connector implementation.

Uses the clickhouse-connect library (HTTP/HTTPS transport) instead of the
native TCP protocol driver. Install with:

    pip install data-dictionary-builder[clickhouse]
    # or
    ddgen install clickhouse
"""

import clickhouse_connect
from typing import List, Optional, Dict, Any

from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


def _http_available() -> bool:
    return importlib.util.find_spec("clickhouse_connect") is not None


def _native_available() -> bool:
    return importlib.util.find_spec("clickhouse_driver") is not None


class ClickHouseConnector(BaseConnector):
    """Connector for ClickHouse databases (HTTP/HTTPS transport via clickhouse-connect)."""

    def __init__(
        self,
        host: str,
        port: int = 8123,
        database: str = "default",
        user: str = "default",
        password: str = "",
        **kwargs,
    ):
        """
        Initialise the ClickHouse connector.

        Args:
            host:     ClickHouse server hostname or IP.
            port:     HTTP port — 8123 (plain) or 8443 (TLS). Default: 8123.
            database: Database name. Omit (or pass None) for server-mode scanning.
            user:     Username. Default: 'default'.
            password: Password. Default: empty string.
            **kwargs: Extra keyword arguments forwarded to
                      ``clickhouse_connect.get_client()``, e.g.
                      ``secure=True``, ``verify=False``.
        """
        self._transport = self._resolve_transport(transport)

        # Apply transport-appropriate default port if none was given.
        # HTTP: 8443 when secure=True (ClickHouse Cloud / TLS), else 8123.
        # Native TCP: 9440 when secure=True, else 9000.
        if port is None:
            if self._transport == "http":
                port = 8443 if kwargs.get("secure") else 8123
            else:
                port = 9440 if kwargs.get("secure") else 9000

        super().__init__(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs,
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.db_type = "clickhouse"
        self.server_mode = database is None
        self.connect_database = database if database else "default"
        self._extra_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ("host", "port", "database", "user", "password")
        }

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def connect(self) -> None:
        """Open an HTTP(S) connection to the ClickHouse server."""
        try:
            self.connection = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.connect_database,
                username=self.user,       # clickhouse-connect uses 'username'
                password=self.password,
                **self._extra_kwargs,
            )
            # Smoke test
            self.connection.query("SELECT 1")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ClickHouse: {e}")

    def disconnect(self) -> None:
        """Close the HTTP client."""
        if self.connection:
            self.connection.close()
            self.connection = None

    # ── Schema / table listing ────────────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        """Return all user-visible database names."""
        result = self.connection.query(
            "SELECT name FROM system.databases "
            "WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') "
            "ORDER BY name"
        )
        schemas = [row[0] for row in result.result_rows]
        return schemas if schemas else [self.database]

    def get_tables(self, schema_name: str) -> List[str]:
        """Return all base-table names in *schema_name*."""
        result = self.connection.query(
            "SELECT name FROM system.tables "
            "WHERE database = {db:String} "
            "AND engine NOT IN ('View', 'MaterializedView') "
            "ORDER BY name",
            parameters={"db": schema_name},
        )
        return [row[0] for row in result.result_rows]

    # ── Column / key metadata ─────────────────────────────────────────────────

    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Return column metadata for a single table."""
        result = self.connection.query(
            "SELECT name, type, default_kind, default_expression, comment, position "
            "FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            parameters={"db": schema_name, "tbl": table_name},
        )

        primary_keys = set(self.get_primary_keys(schema_name, table_name))
        columns = []
        for name, col_type, default_kind, default_expr, comment, position in result.result_rows:
            is_nullable = "Nullable" in col_type
            data_type = col_type.replace("Nullable(", "").rstrip(")") if is_nullable else col_type
            columns.append(ColumnMetadata(
                name=name,
                data_type=data_type,
                is_nullable=is_nullable,
                is_primary_key=name in primary_keys,
                default_value=default_expr if default_kind else None,
                description=comment or None,
                ordinal_position=position,
            ))
        return columns

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Return the list of primary-key column names for *table_name*."""
        try:
            result = self.connection.query(
                "SELECT primary_key FROM system.tables "
                "WHERE database = {db:String} AND name = {tbl:String}",
                parameters={"db": schema_name, "tbl": table_name},
            )
            if result.result_rows and result.result_rows[0][0]:
                pk_str = result.result_rows[0][0].strip("()")
                return [pk.strip() for pk in pk_str.split(",") if pk.strip()]
        except Exception:
            pass
        return []

    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """ClickHouse does not enforce foreign-key constraints; always returns []."""
        return []

    # ── Single-table extraction ───────────────────────────────────────────────

    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """Return complete metadata for a single table."""
        result = self.connection.query(
            "SELECT engine, comment, total_rows FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": schema_name, "tbl": table_name},
        )
        row = result.result_rows[0] if result.result_rows else ("Unknown", None, None)
        engine, comment, total_rows = row[0], row[1] or None, row[2]

        table_meta = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type=f"BASE TABLE ({engine})",
            description=comment,
            row_count=total_rows,
        )
        for col in self.get_columns(schema_name, table_name):
            table_meta.add_column(col)
        table_meta.primary_keys = self.get_primary_keys(schema_name, table_name)
        return table_meta

    # ── Bulk schema extraction (2 queries for any N tables) ──────────────────

    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """
        Bulk-optimised schema extraction for ClickHouse.

        Issues exactly **2 HTTP queries** regardless of table count:
          1. ``system.tables``  — engine, comment, row count, primary key
          2. ``system.columns`` — all columns for all tables at once

        For a schema with N tables this reduces round-trips from 3N → 2.
        """
        # ── Query 1: all tables ───────────────────────────────────────────────
        tables_result = self.connection.query(
            "SELECT name, engine, comment, total_rows, primary_key "
            "FROM system.tables "
            "WHERE database = {db:String} "
            "AND engine NOT IN ('View', 'MaterializedView') "
            "ORDER BY name",
            parameters={"db": schema_name},
        )

        if not tables_result.result_rows:
            return SchemaMetadata(name=schema_name)

        tables_info: Dict[str, Dict[str, Any]] = {}
        for name, engine, comment, total_rows, pk_str in tables_result.result_rows:
            pk_list: List[str] = []
            if pk_str:
                pk_list = [p.strip() for p in pk_str.strip("()").split(",") if p.strip()]
            tables_info[name] = {
                "engine":       engine,
                "comment":      comment or None,
                "total_rows":   total_rows,
                "primary_keys": pk_list,
            }

        # ── Query 2: all columns for all tables ───────────────────────────────
        # Table names come from system.tables (trusted source), so safe to inline.
        table_list = ", ".join(f"'{t}'" for t in tables_info.keys())
        columns_result = self.connection.query(
            f"SELECT table, name, type, default_kind, default_expression, "
            f"comment, position "
            f"FROM system.columns "
            f"WHERE database = {{db:String}} "
            f"AND table IN ({table_list}) "
            f"ORDER BY table, position",
            parameters={"db": schema_name},
        )

        # Group column rows by table
        cols_by_table: Dict[str, list] = {t: [] for t in tables_info}
        for row in columns_result.result_rows:
            tbl = row[0]
            if tbl in cols_by_table:
                cols_by_table[tbl].append(row)

        # ── Assemble SchemaMetadata ───────────────────────────────────────────
        schema_metadata = SchemaMetadata(name=schema_name)

        for table_name, tinfo in tables_info.items():
            pk_set = set(tinfo["primary_keys"])
            table_meta = TableMetadata(
                name=table_name,
                schema_name=schema_name,
                table_type=f'BASE TABLE ({tinfo["engine"]})',
                description=tinfo["comment"],
                row_count=tinfo["total_rows"],
            )
            table_meta.primary_keys = tinfo["primary_keys"]

            for _, col_name, col_type, default_kind, default_expr, col_comment, position in cols_by_table.get(table_name, []):
                is_nullable = "Nullable" in col_type
                data_type = col_type.replace("Nullable(", "").rstrip(")") if is_nullable else col_type
                table_meta.add_column(ColumnMetadata(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    is_primary_key=col_name in pk_set,
                    default_value=default_expr if default_kind else None,
                    description=col_comment or None,
                    ordinal_position=position,
                ))

            schema_metadata.add_table(table_meta)

    # ── Utility ───────────────────────────────────────────────────────────────

    # ── Utility ───────────────────────────────────────────────────────────────

    def get_database_version(self) -> Optional[str]:
        """Return the ClickHouse server version string."""
        result = self.connection.query("SELECT version()")
        return result.result_rows[0][0] if result.result_rows else None

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """Return the approximate row count for a table."""
        try:
            result = self.connection.query(
                "SELECT total_rows FROM system.tables "
                "WHERE database = {db:String} AND name = {tbl:String}",
                parameters={"db": schema_name, "tbl": table_name},
            )
            return result.result_rows[0][0] if result.result_rows else None
        except Exception:
            return None
