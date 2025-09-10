import os
import pytest
import psycopg2
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_pmda.cli import app

# Create a single, module-scoped instance of the CLI runner
runner = CliRunner()

@pytest.fixture(scope="module")
def postgres_container():
    """
    A module-scoped fixture that starts a PostgreSQL container,
    yields it, and then stops it after all tests in the module have run.
    """
    with PostgresContainer("postgres:16-alpine") as container:
        yield container

@pytest.fixture(scope="module")
def set_db_env_vars(postgres_container):
    """
    A module-scoped fixture that sets the necessary database connection
    environment variables before any tests in the module run.
    This uses the details from the running postgres_container.
    """
    os.environ["PMDA_DB_HOST"] = postgres_container.get_container_host_ip()
    os.environ["PMDA_DB_PORT"] = str(postgres_container.get_exposed_port(5432))
    os.environ["PMDA_DB_USER"] = postgres_container.username
    os.environ["PMDA_DB_PASSWORD"] = postgres_container.password
    os.environ["PMDA_DB_DBNAME"] = postgres_container.dbname
    # Ensure the 'type' is set for the get_db_adapter function
    os.environ["PMDA_DB_TYPE"] = "postgres"
    # Yield and then clean up the environment variables
    yield
    del os.environ["PMDA_DB_HOST"]
    del os.environ["PMDA_DB_PORT"]
    del os.environ["PMDA_DB_USER"]
    del os.environ["PMDA_DB_PASSWORD"]
    del os.environ["PMDA_DB_DBNAME"]
    del os.environ["PMDA_DB_TYPE"]


@pytest.mark.integration
def test_init_db_command_creates_table(set_db_env_vars, caplog):
    """
    Tests that the `init-db` CLI command successfully runs and creates
    the `ingestion_state` table in the test database.
    """
    # Act: Run the init-db command
    result = runner.invoke(app, ["init-db"])

    # Assert: Check that the command executed successfully
    assert result.exit_code == 0
    assert "✅ Database initialization complete." in caplog.text

    # Assert: Connect to the database and verify the table exists
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ["PMDA_DB_HOST"],
            port=int(os.environ["PMDA_DB_PORT"]),
            user=os.environ["PMDA_DB_USER"],
            password=os.environ["PMDA_DB_PASSWORD"],
            dbname=os.environ["PMDA_DB_DBNAME"],
        )
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'ingestion_state'
                );
            """)
            table_exists = cursor.fetchone()[0]
    finally:
        if conn:
            conn.close()

    assert table_exists, "The 'ingestion_state' table was not created by the init-db command."
