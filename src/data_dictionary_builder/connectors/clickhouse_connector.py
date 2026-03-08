"""
ClickHouse database connector.

Supports two transports — choose based on your setup or let the connector
pick whichever driver is installed:

  HTTP / HTTPS  (default, recommended for cloud)
      pip install "data-dictionary-builder[clickhouse]"
      # or: ddgen install clickhouse
      Uses: clickhouse-connect

  Native TCP  (legacy, required for some on-prem clusters)
      pip install "data-dictionary-builder[clickhouse-native]"
      # or: ddgen install clickhouse-native
      Uses: clickhouse-driver

Pass ``transport="http"`` or ``transport="native"`` explicitly, or omit it
to let the connector auto-detect which driver is installed (HTTP preferred).
"""

import re
import importlib.util
from typing import List, Optional, Dict, Any

from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


def _http_available() -> bool:
    return importlib.util.find_spec("clickhouse_connect") is not None


def _native_available() -> bool:
    return importlib.util.find_spec("clickhouse_driver") is not None


class ClickHouseConnector(BaseConnector):
    """
    Connector for ClickHouse databases.

    Supports both ``clickhouse-connect`` (HTTP/HTTPS) and ``clickhouse-driver``
    (native TCP) transports, selectable via the ``transport`` parameter.
    """

    def __init__(
        self,
        host: str,
        port: int = None,
        database: str = "default",
        user: str = "default",
        password: str = "",
        transport: str = None,
        **kwargs,
    ):
        """
        Initialise the ClickHouse connector.

        Args:
            host:      Server hostname or IP.
            port:      Port number. Defaults to 8443 (HTTPS/secure) or 8123 (HTTP)
                       for HTTP transport; 9440 (TLS) or 9000 for native TCP.
                       Automatically chosen based on ``secure`` kwarg if omitted.
            database:  Database name. Pass ``None`` for server-mode scanning.
            user:      Username. Default: ``'default'``.
            password:  Password.
            transport: ``"http"`` — clickhouse-connect (HTTP/HTTPS, recommended).
                       ``"native"`` — clickhouse-driver (native TCP).
                       ``None`` (default) — auto-detect: HTTP if clickhouse-connect
                       is installed, native TCP if only clickhouse-driver is installed.
            **kwargs:  Extra keyword arguments forwarded to the underlying client,
                       e.g. ``secure=True``, ``verify=False``.
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
            host=host, port=port, database=database,
            user=user, password=password, **kwargs,
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
            k: v for k, v in kwargs.items()
            if k not in ("host", "port", "database", "user", "password", "transport")
        }

    # ── Transport resolution ──────────────────────────────────────────────────

    @staticmethod
    def _resolve_transport(transport: Optional[str]) -> str:
        """Return 'http' or 'native', auto-detecting if not specified."""
        if transport == "http":
            if not _http_available():
                raise ImportError(
                    "transport='http' requires clickhouse-connect, which is not installed.\n"
                    "Install it with:  pip install data-dictionary-builder[clickhouse]\n"
                    "Or via the CLI:   ddgen install clickhouse"
                )
            return "http"

        if transport == "native":
            if not _native_available():
                raise ImportError(
                    "transport='native' requires clickhouse-driver, which is not installed.\n"
                    "Install it with:  pip install data-dictionary-builder[clickhouse-native]\n"
                    "Or via the CLI:   ddgen install clickhouse-native"
                )
            return "native"

        # Auto-detect
        if _http_available():
            return "http"
        if _native_available():
            return "native"

        raise ImportError(
            "No ClickHouse driver found. Install one:\n"
            "  HTTP (recommended):  pip install data-dictionary-builder[clickhouse]\n"
            "  Native TCP:          pip install data-dictionary-builder[clickhouse-native]\n"
            "  Or via the CLI:      ddgen install clickhouse"
        )

    # ── Unified query helper ──────────────────────────────────────────────────

    @staticmethod
    def _adapt_sql_for_native(sql: str) -> str:
        """
        Convert ``{param:Type}`` placeholders (clickhouse-connect style) to
        ``%(param)s`` placeholders (clickhouse-driver style).
        """
        return re.sub(r"\{(\w+):[^}]+\}", r"%(\1)s", sql)

    def _execute(self, sql: str, params: Dict[str, Any] = None) -> List[tuple]:
        """Execute *sql* and return a list of tuples, abstracting both drivers."""
        params = params or {}
        if self._transport == "http":
            result = self.connection.query(sql, parameters=params)
            return result.result_rows
        else:
            adapted = self._adapt_sql_for_native(sql)
            return self.connection.execute(adapted, params)

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def connect(self) -> None:
        """Open a connection using the resolved transport."""
        try:
            if self._transport == "http":
                import clickhouse_connect
                self.connection = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    database=self.connect_database,
                    username=self.user,          # clickhouse-connect uses 'username'
                    password=self.password,
                    **self._extra_kwargs,
                )
                self._execute("SELECT 1")
            else:
                from clickhouse_driver import Client
                self.connection = Client(
                    host=self.host,
                    port=self.port,
                    database=self.connect_database,
                    user=self.user,
                    password=self.password,
                    **self._extra_kwargs,
                )
                self._execute("SELECT 1")
        except (ImportError, ConnectionError):
            raise
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to ClickHouse ({self._transport}): {e}"
            )

    def disconnect(self) -> None:
        """Close the connection."""
        if self.connection:
            if self._transport == "http":
                self.connection.close()
            else:
                self.connection.disconnect()
            self.connection = None

    # ── Schema / table listing ────────────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        """Return all user-visible database names."""
        rows = self._execute(
            "SELECT name FROM system.databases "
            "WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') "
            "ORDER BY name"
        )
        schemas = [r[0] for r in rows]
        return schemas if schemas else [self.database]

    def get_tables(self, schema_name: str) -> List[str]:
        """Return all base-table names in *schema_name*."""
        rows = self._execute(
            "SELECT name FROM system.tables "
            "WHERE database = {db:String} "
            "AND engine NOT IN ('View', 'MaterializedView') "
            "ORDER BY name",
            {"db": schema_name},
        )
        return [r[0] for r in rows]

    # ── Column / key metadata ─────────────────────────────────────────────────

    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Return column metadata for a single table."""
        rows = self._execute(
            "SELECT name, type, default_kind, default_expression, comment, position "
            "FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} "
            "ORDER BY position",
            {"db": schema_name, "tbl": table_name},
        )
        primary_keys = set(self.get_primary_keys(schema_name, table_name))
        columns = []
        for name, col_type, default_kind, default_expr, comment, position in rows:
            is_nullable = "Nullable" in col_type
            data_type = (
                col_type.replace("Nullable(", "").rstrip(")") if is_nullable else col_type
            )
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
        """Return the primary-key column names for *table_name*."""
        try:
            rows = self._execute(
                "SELECT primary_key FROM system.tables "
                "WHERE database = {db:String} AND name = {tbl:String}",
                {"db": schema_name, "tbl": table_name},
            )
            if rows and rows[0][0]:
                return [p.strip() for p in rows[0][0].strip("()").split(",") if p.strip()]
        except Exception:
            pass
        return []

    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """ClickHouse does not enforce foreign-key constraints; always returns []."""
        return []

    # ── Single-table extraction ───────────────────────────────────────────────

    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """Return complete metadata for a single table."""
        rows = self._execute(
            "SELECT engine, comment, total_rows FROM system.tables "
            "WHERE database = {db:String} AND name = {tbl:String}",
            {"db": schema_name, "tbl": table_name},
        )
        engine = rows[0][0] if rows else "Unknown"
        comment = rows[0][1] or None if rows else None
        total_rows = rows[0][2] if rows else None

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

    # ── Bulk schema extraction (2 queries, any N tables) ─────────────────────

    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """
        Bulk-optimised schema extraction for ClickHouse.

        Issues exactly **2 queries** regardless of table count:
          1. ``system.tables``  — engine, comment, row count, primary key
          2. ``system.columns`` — all columns for all tables at once

        Works with both HTTP (clickhouse-connect) and native TCP
        (clickhouse-driver) transports.
        """
        # ── Query 1: all tables ───────────────────────────────────────────────
        table_rows = self._execute(
            "SELECT name, engine, comment, total_rows, primary_key "
            "FROM system.tables "
            "WHERE database = {db:String} "
            "AND engine NOT IN ('View', 'MaterializedView') "
            "ORDER BY name",
            {"db": schema_name},
        )

        if not table_rows:
            return SchemaMetadata(name=schema_name)

        tables_info: Dict[str, Dict[str, Any]] = {}
        for name, engine, comment, total_rows, pk_str in table_rows:
            pk_list: List[str] = []
            if pk_str:
                pk_list = [p.strip() for p in pk_str.strip("()").split(",") if p.strip()]
            tables_info[name] = {
                "engine":       engine,
                "comment":      comment or None,
                "total_rows":   total_rows,
                "primary_keys": pk_list,
            }

        # ── Query 2: all columns ──────────────────────────────────────────────
        # Table names originate from system.tables (trusted), safe to inline.
        table_list = ", ".join(f"'{t}'" for t in tables_info.keys())
        col_rows = self._execute(
            f"SELECT table, name, type, default_kind, default_expression, "
            f"comment, position "
            f"FROM system.columns "
            f"WHERE database = {{db:String}} "
            f"AND table IN ({table_list}) "
            f"ORDER BY table, position",
            {"db": schema_name},
        )

        cols_by_table: Dict[str, list] = {t: [] for t in tables_info}
        for row in col_rows:
            tbl = row[0]
            if tbl in cols_by_table:
                cols_by_table[tbl].append(row)

        # ── Assemble SchemaMetadata ───────────────────────────────────────────
        schema_meta = SchemaMetadata(name=schema_name)

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
                data_type = (
                    col_type.replace("Nullable(", "").rstrip(")") if is_nullable else col_type
                )
                table_meta.add_column(ColumnMetadata(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    is_primary_key=col_name in pk_set,
                    default_value=default_expr if default_kind else None,
                    description=col_comment or None,
                    ordinal_position=position,
                ))

            schema_meta.add_table(table_meta)

        return schema_meta

    # ── Utility ───────────────────────────────────────────────────────────────

    def get_database_version(self) -> Optional[str]:
        """Return the ClickHouse server version string."""
        rows = self._execute("SELECT version()")
        return rows[0][0] if rows else None

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """Return the approximate row count for *table_name*."""
        try:
            rows = self._execute(
                "SELECT total_rows FROM system.tables "
                "WHERE database = {db:String} AND name = {tbl:String}",
                {"db": schema_name, "tbl": table_name},
            )
            return rows[0][0] if rows else None
        except Exception:
            return None
