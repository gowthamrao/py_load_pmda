from typing import Any, Dict

import pandas as pd
import psycopg2
import pytest
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
    """Tests that ensure_schema generates and executes correct SQL."""
    schema_def = {
        "schema_name": "test_schema",
        "tables": {
            "test_table": {
                "columns": {"id": "SERIAL", "name": "TEXT"},
                "primary_key": "id",
            }
        }
    }
    assert adapter.conn is not None
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    adapter.ensure_schema(schema_def)

    expected_calls = [
        mocker.call("CREATE SCHEMA IF NOT EXISTS test_schema;"),
        mocker.call(mocker.ANY), # The CREATE TABLE statement
    ]
    mock_cursor.execute.assert_has_calls(expected_calls)

    # Check the CREATE TABLE statement specifically
    create_table_call = mock_cursor.execute.call_args_list[1][0][0]
    assert "CREATE TABLE IF NOT EXISTS test_schema.test_table" in create_table_call
    assert "id SERIAL" in create_table_call
    assert "name TEXT" in create_table_call
    assert "PRIMARY KEY (id)" in create_table_call

    adapter.conn.commit.assert_not_called() # type: ignore

def test_bulk_load_append(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests bulk_load in 'append' mode."""
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    adapter.bulk_load(df, "my_table", "my_schema", mode="append")

    # TRUNCATE should not be called in append mode
    assert not any("TRUNCATE" in call[0][0] for call in mock_cursor.execute.call_args_list)

    mock_cursor.copy_expert.assert_called_once()
    adapter.conn.commit.assert_not_called() # type: ignore

def test_bulk_load_overwrite(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests bulk_load in 'overwrite' mode."""
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    adapter.bulk_load(df, "my_table", "my_schema", mode="overwrite")

    # TRUNCATE should be called in overwrite mode
    mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE my_schema.my_table RESTART IDENTITY;")

    mock_cursor.copy_expert.assert_called_once()
    adapter.conn.commit.assert_not_called() # type: ignore

def test_bulk_load_empty_df(adapter: PostgreSQLAdapter) -> None:
    """Tests that bulk_load exits gracefully for an empty DataFrame."""
    df = pd.DataFrame()
    adapter.bulk_load(df, "my_table", "my_schema")
    # No cursor should be created, no commit should be called
    adapter.conn.cursor.assert_not_called() # type: ignore
    adapter.conn.commit.assert_not_called() # type: ignore

def test_get_latest_state_found(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests retrieving an existing state."""
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore
    mock_cursor.fetchone.return_value = ({"last_run": "2025-01-01"},)

    state = adapter.get_latest_state("my_dataset", schema="public")

    mock_cursor.execute.assert_called_once_with(mocker.ANY, ("my_dataset",))
    assert state == {"last_run": "2025-01-01"}

def test_get_latest_state_not_found(adapter: PostgreSQLAdapter) -> None:
    """Tests retrieving a non-existing state."""
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore
    mock_cursor.fetchone.return_value = None

    state = adapter.get_latest_state("my_dataset", schema="public")

    assert state == {}

def test_update_state_success(adapter: PostgreSQLAdapter, mocker: Any) -> None:
    """Tests updating a state with a SUCCESS status."""
    mocker.patch("py_load_pmda.adapters.postgres.version", return_value="0.1.0")
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value # type: ignore

    state_to_save = {"new_watermark": "abc"}
    adapter.update_state("my_dataset", state_to_save, "SUCCESS", schema="public")

    mock_cursor.execute.assert_called_once()
    # Check that last_successful_ts is not None
    args = mock_cursor.execute.call_args[0][1]
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
