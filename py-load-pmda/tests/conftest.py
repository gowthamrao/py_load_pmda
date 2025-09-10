import importlib.util

import pytest
from testcontainers.postgres import PostgresContainer


def pytest_ignore_collect(path, config):
    """
    Conditionally ignore test files if their dependencies are not installed.
    This prevents ImportError during test collection if optional extras
    like `bigquery` or `redshift` are not installed.
    """
    path_str = str(path)
    if "test_bigquery_adapter.py" in path_str:
        # Check for the top-level 'google' namespace package.
        if not importlib.util.find_spec("google"):
            return True
    if "test_redshift_adapter.py" in path_str:
        # Redshift needs both the connector and boto3 for S3 operations.
        if not importlib.util.find_spec("redshift_connector") or not importlib.util.find_spec("boto3"):
            return True
    return False


import uuid
from typing import Generator, Tuple

from py_load_pmda.adapters.postgres import PostgreSQLAdapter


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Pytest fixture that starts a PostgreSQL container for the test session.
    The container will be automatically stopped at the end of the session.
    """
    # Using a specific, lightweight image for reproducibility
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="function")
def postgres_adapter(
    postgres_container: PostgresContainer,
) -> Generator[Tuple[PostgreSQLAdapter, str], None, None]:
    """
    Pytest fixture providing a connected PostgreSQLAdapter instance for a single test.

    This fixture ensures test isolation by creating a unique schema for each test
    function and dropping it during teardown.

    Yields:
        A tuple containing the connected adapter instance and the unique schema name.
    """
    conn_details = {
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "user": postgres_container.username,
        "password": postgres_container.password,
        "dbname": postgres_container.dbname,
    }
    adapter = PostgreSQLAdapter()
    adapter.connect(conn_details)

    schema_name = f"test_schema_{uuid.uuid4().hex}"

    assert adapter.conn is not None, "Connection should be established"
    with adapter.conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {schema_name};")
    adapter.conn.commit()

    try:
        yield adapter, schema_name
    finally:
        if adapter.conn:
            with adapter.conn.cursor() as cursor:
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
            adapter.conn.commit()
        adapter.disconnect()
