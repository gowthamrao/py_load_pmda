import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from py_load_pmda.adapters.postgres import PostgreSQLAdapter

# Mark all tests in this file as 'integration' to allow separating them from unit tests
pytestmark = pytest.mark.integration


def test_bulk_load_and_query(postgres_adapter: tuple[PostgreSQLAdapter, str]):
    """
    Tests the full cycle of schema creation, bulk loading data via COPY,
    and querying it back to verify integrity against a live PostgreSQL container.
    """
    adapter, schema_name = postgres_adapter

    # 1. Define sample data and table schema
    table_name = "test_users"
    data_to_load = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        }
    )

    schema_definition = {
        "schema_name": schema_name,
        "tables": {
            table_name: {
                "columns": {
                    "id": "INTEGER",  # The PRIMARY KEY is defined below
                    "name": "TEXT",
                    "email": "TEXT",
                },
                "primary_key": "id",  # This is the correct way for the adapter
            }
        },
    }

    # 2. Create the table using the adapter
    adapter.ensure_schema(schema_definition)

    # 3. Bulk load the data into the table
    adapter.bulk_load(data_to_load, table_name, schema_name)

    # The adapter methods are designed not to autocommit, so the test must commit.
    adapter.commit()

    # 4. Query the data back from the database using the live connection
    assert adapter.conn is not None, "Connection should still be active"
    loaded_data = pd.read_sql(
        f"SELECT * FROM {schema_name}.{table_name} ORDER BY id;", adapter.conn
    )

    # 5. Assert that the loaded data is identical to the original data
    assert_frame_equal(data_to_load, loaded_data)
