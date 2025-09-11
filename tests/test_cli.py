import pytest
from typer.testing import CliRunner
from py_load_pmda.cli import app

runner = CliRunner()

def test_cli_init_db(postgres_container, monkeypatch):
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname

    monkeypatch.setenv("PMDA_DB_TYPE", "postgres")
    monkeypatch.setenv("PMDA_DB_HOST", host)
    monkeypatch.setenv("PMDA_DB_PORT", str(port))
    monkeypatch.setenv("PMDA_DB_USER", user)
    monkeypatch.setenv("PMDA_DB_PASSWORD", password)
    monkeypatch.setenv("PMDA_DB_DBNAME", dbname)

    result = runner.invoke(app, ["init-db"])
    assert result.exit_code == 0
    assert "Database initialized successfully" in result.stdout

def test_cli_run(postgres_container, mock_pmda_pages, monkeypatch):
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname

    monkeypatch.setenv("PMDA_DB_TYPE", "postgres")
    monkeypatch.setenv("PMDA_DB_HOST", host)
    monkeypatch.setenv("PMDA_DB_PORT", str(port))
    monkeypatch.setenv("PMDA_DB_USER", user)
    monkeypatch.setenv("PMDA_DB_PASSWORD", password)
    monkeypatch.setenv("PMDA_DB_DBNAME", dbname)

    result = runner.invoke(app, ["init-db"])
    assert result.exit_code == 0

    result = runner.invoke(app, ["run", "approvals", "--year", "2025"])
    assert result.exit_code == 0
    assert "Successfully processed and loaded 1 records" in result.stdout

def test_cli_status(postgres_container, monkeypatch):
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname

    monkeypatch.setenv("PMDA_DB_TYPE", "postgres")
    monkeypatch.setenv("PMDA_DB_HOST", host)
    monkeypatch.setenv("PMDA_DB_PORT", str(port))
    monkeypatch.setenv("PMDA_DB_USER", user)
    monkeypatch.setenv("PMDA_DB_PASSWORD", password)
    monkeypatch.setenv("PMDA_DB_DBNAME", dbname)

    result = runner.invoke(app, ["init-db"])
    assert result.exit_code == 0

    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "No run history found" in result.stdout

def test_cli_check_config(postgres_container, monkeypatch):
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    dbname = postgres_container.dbname

    monkeypatch.setenv("PMDA_DB_TYPE", "postgres")
    monkeypatch.setenv("PMDA_DB_HOST", host)
    monkeypatch.setenv("PMDA_DB_PORT", str(port))
    monkeypatch.setenv("PMDA_DB_USER", user)
    monkeypatch.setenv("PMDA_DB_PASSWORD", password)
    monkeypatch.setenv("PMDA_DB_DBNAME", dbname)

    result = runner.invoke(app, ["check-config"])
    assert result.exit_code == 0
    assert "Database connection successful" in result.stdout
