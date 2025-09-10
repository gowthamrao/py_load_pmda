from typing import Any, Dict

import pandas as pd
import psycopg2
import pytest
from psycopg2 import sql
from py_load_pmda.adapters.postgres import PostgreSQLAdapter


@pytest.fixture
def adapter(mocker: Any) -> PostgreSQLAdapter:
    """Provides a PostgreSQLAdapter with a mocked connection."""
    adapter = PostgreSQLAdapter()
    adapter.conn = mocker.MagicMock()
    return adapter

@pytest.fixture
def db_details() -> Dict[str, Any]:
    """Provides a sample database connection details dictionary."""
    return {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "test",
        "password": "test",
        "dbname": "testdb",
    }

def test_connect_success(mocker: Any, db_details: Dict[str, Any]) -> None:
    """
    Tests a successful database connection.
    """
    mock_connect = mocker.patch("psycopg2.connect")
    # Instantiate a new adapter for this test to check connection logic
    new_adapter = PostgreSQLAdapter()
    new_adapter.connect(db_details)

    # Assert that psycopg2.connect was called once
    mock_connect.assert_called_once()

    # Assert that the 'type' key was removed before calling connect
    call_args = mock_connect.call_args[1]
    assert "type" not in call_args
    assert call_args["host"] == "localhost"

def test_connect_failure(mocker: Any, db_details: Dict[str, Any]) -> None:
    """
    Tests that a ConnectionError is raised when psycopg2.connect fails.
    """
    # Configure the mock to raise a psycopg2 OperationalError
    mocker.patch("psycopg2.connect", side_effect=psycopg2.OperationalError("Connection failed"))

    new_adapter = PostgreSQLAdapter()

    with pytest.raises(ConnectionError, match="Failed to connect to PostgreSQL."):
        new_adapter.connect(db_details)

def test_connect_is_idempotent(mocker: Any, db_details: Dict[str, Any]) -> None:
    """
    Tests that the connect method is idempotent and does not create new connections.
    """
    mock_connect = mocker.patch("psycopg2.connect")
    new_adapter = PostgreSQLAdapter()

    # First call should connect
    new_adapter.connect(db_details)
    mock_connect.assert_called_once()

    # Second call should do nothing
    new_adapter.connect(db_details)
    mock_connect.assert_called_once() # Should still be 1

def test_ensure_schema(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests that ensure_schema generates and executes correct SQL using sql module."""
    schema_def = {
        "schema_name": "test_schema",
        "tables": {
            "test_table": {
                "columns": {"id": "SERIAL PRIMARY KEY", "name": "TEXT"},
            }
        }
    }
    assert adapter.conn is not None
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value

    adapter.ensure_schema(schema_def)

    # Assert that execute was called twice (once for schema, once for table)
    assert mock_cursor.execute.call_count == 2
    # Assert that the calls were made with sql.Composed objects
    assert isinstance(mock_cursor.execute.call_args_list[0][0][0], sql.Composed)
    assert isinstance(mock_cursor.execute.call_args_list[1][0][0], sql.Composed)
    adapter.conn.commit.assert_not_called()

def test_bulk_load_append(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests bulk_load in 'append' mode, ensuring no TRUNCATE call."""
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value

    # Mock the as_string method to avoid the TypeError with mock cursors
    mocker.patch("psycopg2.sql.Composed.as_string", return_value='COPY "my_schema"."my_table" FROM STDIN WITH (FORMAT text, DELIMITER E\'\\t\', NULL \'\\N\')')

    adapter.bulk_load(df, "my_table", "my_schema", mode="append")

    # TRUNCATE should not be called
    mock_cursor.execute.assert_not_called()

    mock_cursor.copy_expert.assert_called_once()
    sql_arg = mock_cursor.copy_expert.call_args.kwargs['sql']
    assert 'COPY "my_schema"."my_table" FROM STDIN' in sql_arg
    adapter.conn.commit.assert_not_called()

def test_bulk_load_overwrite(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests bulk_load in 'overwrite' mode, ensuring TRUNCATE is called."""
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value
    mocker.patch("psycopg2.sql.Composed.as_string", return_value="TRUNCATE DUMMY")

    adapter.bulk_load(df, "my_table", "my_schema", mode="overwrite")

    # TRUNCATE should be called
    mock_cursor.execute.assert_called_once()
    assert isinstance(mock_cursor.execute.call_args[0][0], sql.Composed)

    mock_cursor.copy_expert.assert_called_once()
    adapter.conn.commit.assert_not_called()

def test_bulk_load_empty_df(adapter: PostgreSQLAdapter) -> None:
    """Tests that bulk_load exits gracefully for an empty DataFrame."""
    df = pd.DataFrame()
    adapter.bulk_load(df, "my_table", "my_schema")
    # A cursor is not even created if the dataframe is empty.
    adapter.conn.cursor.assert_not_called()
    adapter.conn.commit.assert_not_called()

def test_get_latest_state_found(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """
    Tests retrieving an existing state.
    This test is updated to check that a full dictionary is returned,
    not just a part of it.
    """
    assert adapter.conn is not None
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    # This is what the function *should* return
    full_expected_state = {
        "dataset_id": "my_dataset",
        "status": "SUCCESS",
        "last_watermark": {"last_run": "2025-01-01"},
    }

    # For the *fixed* implementation, a DictCursor would return a DictRow object.
    # We can simulate this by returning the dictionary directly, as dict(DictRow)
    # is the operation being performed.
    mock_cursor.fetchone.return_value = full_expected_state

    state = adapter.get_latest_state("my_dataset", schema="public")

    # Now, the fixed implementation should return the full state, and the test should pass.
    assert state == full_expected_state


def test_get_latest_state_not_found(adapter: PostgreSQLAdapter) -> None:
    """Tests retrieving a non-existing state."""
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore
    mock_cursor.fetchone.return_value = None

    state = adapter.get_latest_state("my_dataset", schema="public")

    assert state == {}

import json

def test_update_state_success(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """
    Tests updating a state with a SUCCESS status and verifies the watermark structure.
    """
    mocker.patch("py_load_pmda.adapters.postgres.version", return_value="0.1.0")
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    # This represents the full state object passed to the method
    state_to_save = {
        "pipeline_version": "0.1.0",
        "last_watermark": {"timestamp": "2025-09-09"},
    }
    adapter.update_state("my_dataset", state_to_save, "SUCCESS", schema="public")

    mock_cursor.execute.assert_called_once()
    args = mock_cursor.execute.call_args[0][1]

    # The 5th argument (index 4) should be the serialized `last_watermark` dict
    saved_watermark = json.loads(args[4])

    # The buggy implementation would save the whole `state_to_save` object.
    # The correct implementation saves only the nested `last_watermark`.
    assert saved_watermark == {"timestamp": "2025-09-09"}

    # Check other parameters too
    assert args[2] is not None # last_successful_ts
    assert args[3] == "SUCCESS"
    adapter.conn.commit.assert_not_called() # type: ignore

def test_update_state_failure(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests updating a state with a FAILED status."""
    mocker.patch("py_load_pmda.adapters.postgres.version", return_value="0.1.0")
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    adapter.update_state("my_dataset", {}, "FAILED", schema="public")

    mock_cursor.execute.assert_called_once()
    # Check that last_successful_ts is None
    args = mock_cursor.execute.call_args[0][1]
    assert args[2] is None # last_successful_ts
    assert args[3] == "FAILED"
    adapter.conn.commit.assert_not_called() # type: ignore
