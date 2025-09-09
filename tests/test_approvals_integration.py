import os

import psycopg2
import pytest
from typer.testing import CliRunner

from py_load_pmda.cli import app
from testcontainers.postgres import PostgresContainer

runner = CliRunner()


def test_approvals_pipeline_e2e(
    postgres_container: PostgresContainer, mock_pmda_pages, monkeypatch
):
    """
    End-to-end integration test for the approvals pipeline.

    This test uses a real PostgreSQL database (in a container) and a mocked
    PMDA website to verify the full ETL flow.
    """
    # 1. Set up environment variables to point to the test database
    db_info = postgres_container.get_connection_url()
    monkeypatch.setenv("PMDA_DB_TYPE", "postgres")
    monkeypatch.setenv("PMDA_DB_HOST", postgres_container.get_container_host_ip())
    monkeypatch.setenv("PMDA_DB_PORT", postgres_container.get_exposed_port(5432))
    monkeypatch.setenv("PMDA_DB_USER", postgres_container.username)
    monkeypatch.setenv("PMDA_DB_PASSWORD", postgres_container.password)
    monkeypatch.setenv("PMDA_DB_DBNAME", postgres_container.dbname)

    # 2. Initialize the database schema
    result = runner.invoke(app, ["init-db"])
    assert result.exit_code == 0, result.stdout

    # 3. Run the approvals pipeline for a test year
    result = runner.invoke(app, ["run", "--dataset", "approvals", "--year", "2025"])
    assert result.exit_code == 0, result.stdout
    assert "Successfully processed and loaded 1 records" in result.stdout

    # 4. Verify the data in the database
    conn = psycopg2.connect(db_info)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM public.pmda_approvals;")
    records = cursor.fetchall()

    assert len(records) == 1

    # Check some key values
    assert records[0][0] == "(302AMX00001000)"  # approval_id
    assert records[0][2] == "テストメディカル"  # brand_name_jp
    assert records[0][4] == "テスト製薬株式会社"  # applicant_name_jp

    cursor.close()
    conn.close()
