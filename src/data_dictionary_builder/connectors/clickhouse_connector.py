"""
ClickHouse database connector implementation.

Supports two transports, selected automatically or via the ``transport`` kwarg:

* **HTTP** (default)  — ``clickhouse-connect`` library (port 8443 TLS / 8123 plain).
* **Native TCP**       — ``clickhouse-driver`` library  (port 9440 TLS / 9000 plain).

Install with:

    pip install data-dictionary-builder[clickhouse]          # HTTP (default)
    pip install data-dictionary-builder[clickhouse-native]   # native TCP
    pip install clickhouse-driver                            # native TCP (direct)

Transport selection
-------------------
* ``transport=None`` (default) — auto-detect.  Tries HTTP first; if the
  connection fails **and** the native driver is installed, automatically
  retries on the native TCP port.  The reverse applies when only the native
  driver is present.
* ``transport="http"``  / ``transport="native"`` — explicit; no fallback.
"""

import importlib.util
from typing import List, Optional, Dict, Any

from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


# ── Transport availability helpers ────────────────────────────────────────────

def _http_available() -> bool:
    return importlib.util.find_spec("clickhouse_connect") is not None


def _native_available() -> bool:
    return importlib.util.find_spec("clickhouse_driver") is not None


# ── Native-transport adapter ──────────────────────────────────────────────────

class _NativeResult:
    """Makes clickhouse_driver rows look like a clickhouse_connect QueryResult."""

    __slots__ = ("result_rows",)

    def __init__(self, rows: list) -> None:
        self.result_rows = rows


class _NativeClient:
    """
    Thin wrapper around a ``clickhouse_driver.Client`` that exposes the same
    ``.query()`` / ``.close()`` surface as a ``clickhouse_connect`` client.

    Parameter binding: converts the ``{name:Type}`` placeholders used by
    clickhouse-connect into the ``%(name)s`` style expected by clickhouse-driver.
    """

    def __init__(self, client) -> None:
        self._client = client

    def query(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> _NativeResult:
        import re
        if parameters:
            sql = re.sub(r'\{(\w+):[^}]+\}', lambda m: f'%({m.group(1)})s', sql)
        rows = self._client.execute(sql, parameters or {})
        return _NativeResult(rows)

    def close(self) -> None:
        self._client.disconnect()


# ── Connector ─────────────────────────────────────────────────────────────────

class ClickHouseConnector(BaseConnector):
    """Connector for ClickHouse databases (HTTP or native TCP transport)."""

    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        database: str = "default",
        user: str = "default",
        password: str = "",
        transport: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialise the ClickHouse connector.

        Args:
            host:      ClickHouse server hostname or IP.
            port:      Override the default port.  When omitted the port is
                       chosen based on transport and the ``secure`` kwarg:
                       HTTP   → 8443 (TLS) / 8123 (plain)
                       Native → 9440 (TLS) / 9000 (plain)
            database:  Database name.  Pass ``None`` for server-mode scanning.
            user:      Username.  Default: ``'default'``.
            password:  Password.  Default: empty string.
            transport: ``'http'``, ``'native'``, or ``None`` (auto-detect).
                       Auto-detect tries the driver that is installed; if both
                       are installed it prefers HTTP but falls back to native
                       TCP (and vice-versa) when the first connection attempt
                       fails.  Passing an explicit value disables fallback.
            **kwargs:  Forwarded to the underlying driver, e.g.
                       ``secure=True``, ``verify=False``.
        """
        # Remember whether the caller pinned a transport or left it to auto.
        self._transport_auto: bool = transport is None
        self._transport: str = self._resolve_transport(transport)

        self._secure: bool = bool(kwargs.get("secure"))
        # Default ports per transport/TLS combination — used for fallback too.
        self._http_port: int  = 8443 if self._secure else 8123
        self._native_port: int = 9440 if self._secure else 9000

        # Remember whether the caller supplied an explicit port.
        self._port_auto: bool = port is None
        if port is None:
            port = self._http_port if self._transport == "http" else self._native_port

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

    # ── Transport resolution ──────────────────────────────────────────────────

    @staticmethod
    def _resolve_transport(transport: Optional[str]) -> str:
        """
        Return the **primary** transport to attempt: ``'http'`` or ``'native'``.

        When *transport* is ``None`` (auto) this picks the primary based on
        which drivers are installed.  ``connect()`` handles the runtime fallback
        to the alternate transport when the primary attempt fails.
        """
        if transport in ("http", "native"):
            return transport
        if transport is not None:
            raise ValueError(
                f"Unknown transport {transport!r}. Use 'http', 'native', or None (auto)."
            )
        # Auto-detect: prefer HTTP when clickhouse-connect is installed.
        if _http_available():
            return "http"
        if _native_available():
            return "native"
        raise ImportError(
            "No ClickHouse driver found. Install at least one:\n\n"
            "  HTTP/HTTPS transport (ClickHouse Cloud, default):\n"
            "    pip install clickhouse-connect\n"
            "    pip install \"data-dictionary-builder[clickhouse]\"\n"
            "    uv add \"data-dictionary-builder[clickhouse]\"\n\n"
            "  Native TCP transport (Altinity, on-prem):\n"
            "    pip install clickhouse-driver\n"
            "    pip install \"data-dictionary-builder[clickhouse-native]\"\n"
            "    uv add \"data-dictionary-builder[clickhouse-native]\"\n\n"
            "  Both transports:\n"
            "    pip install \"data-dictionary-builder[clickhouse-all]\"\n"
            "    uv add \"data-dictionary-builder[clickhouse-all]\""
        )

    # ── Connection lifecycle ──────────────────────────────────────────────────

    @staticmethod
    def _make_ssl_pool_manager(secure: bool, verify: bool):
        """
        Return a urllib3 PoolManager with a custom SSL context, or ``None``
        when a secure connection is not requested.

        The custom context applies ``ssl.OP_IGNORE_UNEXPECTED_EOF`` (Python
        ≥ 3.10) so that ClickHouse Cloud endpoints that close TLS without
        sending a ``close_notify`` alert do not raise ``SSLEOFError`` or
        ``SSLZeroReturnError``.
        """
        if not secure:
            return None
        import ssl
        import urllib3
        ctx = ssl.create_default_context()
        if not verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):   # Python 3.10+
            ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
        return urllib3.PoolManager(ssl_context=ctx)

    def _connect_transport(self, transport: str, port: int) -> None:
        """
        Open a raw connection using *transport* on *port* and verify it with
        ``SELECT 1``.  Sets ``self.connection`` on success; raises on failure.
        """
        if transport == "http":
            if not _http_available():
                raise ImportError(
                    "clickhouse-connect is not installed — required for HTTP/HTTPS transport.\n"
                    "Install with:\n"
                    "  pip install clickhouse-connect\n"
                    "  pip install \"data-dictionary-builder[clickhouse]\"\n"
                    "  uv add \"data-dictionary-builder[clickhouse]\""
                )

            import clickhouse_connect

            extra   = dict(self._extra_kwargs)
            secure  = extra.pop("secure", False)
            verify  = extra.pop("verify", True)

            # Pass a custom pool manager that applies the Python 3.10+
            # OP_IGNORE_UNEXPECTED_EOF fix for ClickHouse Cloud TLS handshakes.
            pool = self._make_ssl_pool_manager(secure=secure, verify=verify)
            if pool is not None and "pool_mgr" not in extra:
                extra["pool_mgr"] = pool

            try:
                client = clickhouse_connect.get_client(
                    host=self.host,
                    port=port,
                    database=self.connect_database,
                    username=self.user,       # clickhouse-connect uses 'username'
                    password=self.password,
                    secure=secure,
                    verify=verify,
                    **extra,
                )
                client.query("SELECT 1")
            except Exception as exc:
                msg = str(exc)
                if "SSL" in msg or "ssl" in msg or "certificate" in msg.lower():
                    raise ConnectionError(
                        f"SSL/TLS error connecting to ClickHouse "
                        f"({self.host}:{port}).\n"
                        "Common causes:\n"
                        "  • The server is paused or unreachable — verify it is running.\n"
                        "  • Wrong port — HTTP uses 8123 (plain) / 8443 (TLS); "
                        "native TCP uses 9000 / 9440.\n"
                        "  • Wrong transport — set transport=\"native\" for "
                        "Altinity / on-prem servers.\n"
                        "  • Pass verify=False to skip certificate verification "
                        "for self-signed certs.\n"
                        f"Original error: {exc}"
                    ) from exc
                raise
            self.connection = client

        else:
            if not _native_available():
                raise ImportError(
                    "clickhouse-driver is not installed — required for native TCP transport.\n"
                    "Install with:\n"
                    "  pip install clickhouse-driver\n"
                    "  pip install \"data-dictionary-builder[clickhouse-native]\"\n"
                    "  uv add \"data-dictionary-builder[clickhouse-native]\"\n"
                    "Or install both transports at once:\n"
                    "  pip install \"data-dictionary-builder[clickhouse-all]\"\n"
                    "  uv add \"data-dictionary-builder[clickhouse-all]\""
                )

            from clickhouse_driver import Client

            # clickhouse_driver manages its own SSL — pass secure/verify as-is.
            extra = dict(self._extra_kwargs)

            try:
                client = Client(
                    host=self.host,
                    port=port,
                    database=self.connect_database,
                    user=self.user,
                    password=self.password,
                    **extra,
                )
                client.execute("SELECT 1")
            except Exception as exc:
                msg = str(exc)
                if "ssl" in msg.lower() or "certificate" in msg.lower():
                    raise ConnectionError(
                        f"SSL/TLS error connecting to ClickHouse native TCP "
                        f"({self.host}:{port}).\n"
                        "Common causes:\n"
                        "  • The server is paused or unreachable — verify it is running.\n"
                        "  • Wrong port — native TCP uses 9000 (plain) / 9440 (TLS).\n"
                        "  • Wrong transport — set transport=\"http\" for ClickHouse Cloud.\n"
                        "  • Pass verify=False to skip certificate verification.\n"
                        f"Original error: {exc}"
                    ) from exc
                raise
            self.connection = _NativeClient(client)

    def connect(self) -> None:
        """
        Open a connection to the ClickHouse server.

        When *transport* was auto-detected (``transport=None``), a failed
        attempt on the primary transport is transparently retried on the
        alternate transport (if its driver is installed).  Explicit transport
        values never trigger a fallback.
        """
        try:
            self._connect_transport(self._transport, self.port)
            return
        except Exception as primary_err:
            if not self._transport_auto:
                raise ConnectionError(
                    f"Failed to connect to ClickHouse ({self._transport}): {primary_err}"
                )

        # ── Auto-mode fallback ────────────────────────────────────────────────
        fallback = "native" if self._transport == "http" else "http"
        if fallback == "http" and not _http_available():
            raise ConnectionError(
                f"Failed to connect to ClickHouse ({self._transport}, port {self.port}): {primary_err}\n"
                "HTTP/HTTPS fallback unavailable — install clickhouse-connect:\n"
                "  pip install clickhouse-connect\n"
                "  pip install \"data-dictionary-builder[clickhouse]\"\n"
                "  uv add \"data-dictionary-builder[clickhouse]\""
            )
        if fallback == "native" and not _native_available():
            raise ConnectionError(
                f"Failed to connect to ClickHouse ({self._transport}, port {self.port}): {primary_err}\n"
                "Native TCP fallback unavailable — install clickhouse-driver:\n"
                "  pip install clickhouse-driver\n"
                "  pip install \"data-dictionary-builder[clickhouse-native]\"\n"
                "  uv add \"data-dictionary-builder[clickhouse-native]\""
            )

        # Use the canonical default port for the fallback transport unless the
        # caller pinned an explicit port (in which case honour their choice).
        fallback_port = (
            (self._http_port if fallback == "http" else self._native_port)
            if self._port_auto
            else self.port
        )

        try:
            self._connect_transport(fallback, fallback_port)
            # Promote fallback to active transport so later queries are consistent.
            self._transport = fallback
            if self._port_auto:
                self.port = fallback_port
        except Exception as fallback_err:
            raise ConnectionError(
                f"Failed to connect to ClickHouse on both transports.\n"
                f"  {self._transport} (port {self.port}): {primary_err}\n"
                f"  {fallback} (port {fallback_port}): {fallback_err}"
            )

    def disconnect(self) -> None:
        """Close the connection."""
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

        Issues exactly **2 queries** regardless of table count:
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

        return schema_metadata

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
