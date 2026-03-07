"""
data_dictionary_builder CLI

Entry point: ddgen
"""

import sys
import subprocess
import importlib.util

import click

from . import __version__


# ── Connector registry ────────────────────────────────────────────────────────

CONNECTORS = {
    "sqlite": {
        "label":       "SQLite",
        "import_mod":  None,           # built-in, always available
        "pip_package": None,
        "pip_extra":   None,
        "notes":       "built-in — no install needed",
    },
    "postgres": {
        "label":       "PostgreSQL",
        "import_mod":  "psycopg2",
        "pip_package": "psycopg2-binary",
        "pip_extra":   "postgres",
        "notes":       "",
    },
    "mysql": {
        "label":       "MySQL / MariaDB",
        "import_mod":  "pymysql",
        "pip_package": "PyMySQL",
        "pip_extra":   "mysql",
        "notes":       "",
    },
    "clickhouse": {
        "label":       "ClickHouse (HTTP/HTTPS)",
        "import_mod":  "clickhouse_connect",
        "pip_package": "clickhouse-connect",
        "pip_extra":   "clickhouse",
        "notes":       "recommended for cloud instances",
    },
    "clickhouse-native": {
        "label":       "ClickHouse (Native TCP)",
        "import_mod":  "clickhouse_driver",
        "pip_package": "clickhouse-driver",
        "pip_extra":   "clickhouse-native",
        "notes":       "use transport='native' in MetadataExtractor",
    },
    "spanner": {
        "label":       "Google Cloud Spanner",
        "import_mod":  "google.cloud.spanner",
        "pip_package": "google-cloud-spanner",
        "pip_extra":   "spanner",
        "notes":       "requires Application Default Credentials",
    },
}

INSTALLABLE = [k for k, v in CONNECTORS.items() if v["pip_extra"] is not None]


def _is_installed(import_mod: str) -> bool:
    """Return True if the given module is importable."""
    if import_mod is None:
        return True
    return importlib.util.find_spec(import_mod) is not None


# ── CLI root ──────────────────────────────────────────────────────────────────

@click.group()
@click.version_option(version=__version__, prog_name="ddgen")
def main():
    """data_dictionary_builder — database metadata extraction and documentation."""


# ── connectors command ────────────────────────────────────────────────────────

@main.command("connectors")
def connectors_status():
    """Show which database connectors are installed."""
    col_name  = 24
    col_status = 14

    click.echo()
    click.echo(f"  {'Connector':<{col_name}} {'Status':<{col_status}} Install command")
    click.echo("  " + "─" * 74)

    for key, info in CONNECTORS.items():
        installed = _is_installed(info["import_mod"])

        if installed:
            status_str = click.style("✓ installed", fg="green")
            if info["pip_extra"] is None:
                status_str = click.style("✓ built-in", fg="cyan")
            install_cmd = info["notes"] if info["notes"] else ""
        else:
            status_str = click.style("✗ missing  ", fg="red")
            install_cmd = f"ddgen install {key}"

        # pad status manually (style adds ANSI bytes that confuse ljust)
        click.echo(f"  {info['label']:<{col_name}} {status_str}   {install_cmd}")

    click.echo()
    installed_count = sum(
        1 for info in CONNECTORS.values() if _is_installed(info["import_mod"])
    )
    total = len(CONNECTORS)
    click.echo(f"  {installed_count}/{total} connectors available.\n")


# ── install command ───────────────────────────────────────────────────────────

@main.command("install")
@click.argument(
    "connector",
    type=click.Choice(INSTALLABLE + ["all"], case_sensitive=False),
)
def install_connector(connector: str):
    """
    Install the driver package for a database connector.

    CONNECTOR is one of: postgres, mysql, clickhouse, spanner, all

    Examples:

    \b
        ddgen install postgres
        ddgen install clickhouse
        ddgen install all
    """
    if connector == "all":
        targets = INSTALLABLE
    else:
        targets = [connector.lower()]

    packages_to_install = []
    already_installed   = []

    for key in targets:
        info = CONNECTORS[key]
        if _is_installed(info["import_mod"]):
            already_installed.append(info["label"])
        else:
            packages_to_install.append(info["pip_package"])

    if already_installed:
        for label in already_installed:
            click.echo(click.style(f"  ✓ {label} is already installed.", fg="green"))

    if not packages_to_install:
        click.echo()
        click.echo("  Nothing to install.")
        return

    click.echo()
    click.echo(f"  Installing: {', '.join(packages_to_install)}")
    click.echo()

    cmd = [sys.executable, "-m", "pip", "install"] + packages_to_install

    result = subprocess.run(cmd, check=False)

    click.echo()
    if result.returncode == 0:
        for pkg in packages_to_install:
            click.echo(click.style(f"  ✓ {pkg} installed successfully.", fg="green"))
        click.echo()
        click.echo("  Run  ddgen connectors  to verify.")
    else:
        click.echo(
            click.style("  ✗ Installation failed. Check the pip output above.", fg="red")
        )
        sys.exit(result.returncode)

    click.echo()


# ── info command ──────────────────────────────────────────────────────────────

@main.command("info")
def info():
    """Show library version and a summary of installed connectors."""
    click.echo()
    click.echo(f"  data_dictionary_builder  v{__version__}")
    click.echo(f"  Python {sys.version.split()[0]}   {sys.executable}")
    click.echo()
    click.echo("  Connectors:")

    for key, cinfo in CONNECTORS.items():
        installed = _is_installed(cinfo["import_mod"])
        marker    = click.style("✓", fg="green") if installed else click.style("✗", fg="red")
        click.echo(f"    {marker}  {cinfo['label']}")

    click.echo()
    click.echo("  Run  ddgen connectors  for install commands.")
    click.echo()
