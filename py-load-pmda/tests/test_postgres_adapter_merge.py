from typing import Any

import pytest
from psycopg2 import sql

from py_load_pmda.adapters.postgres import PostgreSQLAdapter

# --- Test Fixtures ---


@pytest.fixture
def adapter(mocker: Any) -> PostgreSQLAdapter:
    """Provides a PostgreSQLAdapter with a mocked connection."""
    adapter = PostgreSQLAdapter()
    # Mock the connection object entirely
    adapter.conn = mocker.MagicMock()
    return adapter


# --- Test Cases ---


def test_execute_merge_constructs_correct_sql(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """
    Tests that execute_merge constructs the correct SQL statement.
    This is a unit test that verifies the SQL generation logic.
    """
    schema = "test_schema"
    target = "target_table"
    staging = "staging_table"
    pks = ["id"]

    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value
    column_names = [("id",), ("name",), ("value",)]
    mock_cursor.fetchall.return_value = column_names

    adapter.execute_merge(staging, target, pks, schema)

    # Check that execute was called for info schema and for the merge
    assert mock_cursor.execute.call_count == 2

    # Check that the second call was a Composed object for the merge
    actual_sql_obj = mock_cursor.execute.call_args[0][0]
    assert isinstance(actual_sql_obj, sql.Composed)

    adapter.conn.commit.assert_not_called()


def test_execute_merge_no_primary_keys_raises_error(adapter: PostgreSQLAdapter) -> None:
    """
    Tests that execute_merge raises a ValueError if no primary keys are provided.
    """
    with pytest.raises(ValueError, match="primary_keys must be provided"):
        adapter.execute_merge("staging", "target", [], "schema")


def test_execute_merge_no_update_columns_raises_error(
    adapter: PostgreSQLAdapter, mocker: Any
) -> None:
    """
    Tests that a ValueError is raised if all columns are primary keys,
    as this is likely a configuration error.
    """
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value
    mock_cursor.fetchall.return_value = [("id1",), ("id2",)]

    with pytest.raises(ValueError, match="No columns to update"):
        adapter.execute_merge("staging", "target", ["id1", "id2"], "schema")

    # The final merge SQL should NOT be executed
    mock_cursor.execute.assert_called_once()
    adapter.conn.commit.assert_not_called()
