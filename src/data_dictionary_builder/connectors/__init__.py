"""
Database connectors module.

Connector classes are imported lazily — the underlying driver package is only
required when you actually use that connector, not when you import the library.
"""

from .base import BaseConnector

__all__ = [
    "BaseConnector",
    "PostgresConnector",
    "MySQLConnector",
    "ClickHouseConnector",
    "SpannerConnector",
    "SQLiteConnector",
    "get_connector",
]

# Maps connector class names to their module and the pip extra needed
_CONNECTOR_MAP = {
    "PostgresConnector":    ("postgres_connector",    "psycopg2-binary",        "postgres"),
    "MySQLConnector":       ("mysql_connector",       "PyMySQL",                "mysql"),
    "ClickHouseConnector":  ("clickhouse_connector",  "clickhouse-driver",      "clickhouse"),
    "SpannerConnector":     ("spanner_connector",     "google-cloud-spanner",   "spanner"),
    "SQLiteConnector":      ("sqlite_connector",      None,                     None),
}


def __getattr__(name: str):
    """Lazily load connector classes so missing drivers don't break the import."""
    if name in _CONNECTOR_MAP:
        module_name, pip_package, pip_extra = _CONNECTOR_MAP[name]
        try:
            import importlib
            mod = importlib.import_module(f".{module_name}", package=__name__)
            return getattr(mod, name)
        except ImportError:
            install_hint = (
                f"pip install data-dictionary-builder[{pip_extra}]"
                if pip_extra else f"pip install {pip_package}"
            )
            raise ImportError(
                f"{name} requires '{pip_package}', which is not installed.\n"
                f"Install it with:  {install_hint}\n"
                f"Or via the CLI:   ddgen install {pip_extra or 'sqlite'}"
            )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_connector(db_type: str, **kwargs) -> BaseConnector:
    """
    Factory function — returns the appropriate connector for the given db_type.

    Drivers are imported lazily. If the required driver package is not
    installed, a clear ImportError is raised with installation instructions.

    Args:
        db_type: One of 'sqlite', 'postgres', 'postgresql', 'mysql',
                 'clickhouse', 'spanner'.
        **kwargs: Connection parameters passed directly to the connector.

    Returns:
        An instance of the appropriate BaseConnector subclass.

    Raises:
        ValueError:  Unsupported db_type.
        ImportError: Required driver package is not installed.
    """
    db_type_lower = db_type.lower()

    _routes = {
        "sqlite":      ("SQLiteConnector",      None,                 None),
        "postgres":    ("PostgresConnector",    "psycopg2-binary",    "postgres"),
        "postgresql":  ("PostgresConnector",    "psycopg2-binary",    "postgres"),
        "mysql":       ("MySQLConnector",       "PyMySQL",            "mysql"),
        "mariadb":     ("MySQLConnector",       "PyMySQL",            "mysql"),
        "clickhouse":  ("ClickHouseConnector",  "clickhouse-driver",  "clickhouse"),
        "spanner":     ("SpannerConnector",     "google-cloud-spanner", "spanner"),
    }

    if db_type_lower not in _routes:
        raise ValueError(
            f"Unsupported database type: '{db_type}'. "
            f"Supported types: {', '.join(sorted(set(k for k in _routes if k != 'postgresql' and k != 'mariadb')))}"
        )

    class_name, pip_package, pip_extra = _routes[db_type_lower]

    try:
        cls = __getattr__(class_name)
    except ImportError:
        install_hint = (
            f"pip install data-dictionary-builder[{pip_extra}]"
            if pip_extra else ""
        )
        raise ImportError(
            f"The '{db_type}' connector requires '{pip_package}', which is not installed.\n"
            f"Install it with:  {install_hint}\n"
            f"Or via the CLI:   ddgen install {pip_extra or 'sqlite'}"
        )

    return cls(**kwargs)
