import pytest
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from psycopg2 import sql

# --- Test Fixtures ---

@pytest.fixture
def adapter(mocker):
    """Provides a PostgreSQLAdapter with a mocked connection."""
    adapter = PostgreSQLAdapter()
    # Mock the connection object entirely
    adapter.conn = mocker.MagicMock()
    return adapter

# --- Test Cases ---

def test_execute_merge_constructs_correct_sql(adapter, mocker):
    """
    Tests that execute_merge constructs the correct SQL statement.
    This is a unit test that verifies the SQL generation logic.
    """
    schema = "test_schema"
    target = "target_table"
    staging = "staging_table"
    pks = ["id"]

    # Mock the cursor and its methods
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value

    # Configure the mock to return column names when queried
    column_names = [("id",), ("name",), ("value",)]
    mock_cursor.fetchall.return_value = column_names

    # Call the method to be tested
    adapter.execute_merge(staging, target, pks, schema)

    # 1. Verify the query to information_schema to get columns
    info_schema_query = sql.SQL("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """)
    mock_cursor.execute.assert_any_call(info_schema_query, (schema, staging))

    # 2. Verify the final MERGE SQL statement

    # Expected construction
    expected_cols = ["id", "name", "value"]
    expected_update_cols = ["name", "value"]

    expected_update_clause = sql.SQL(', ').join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in expected_update_cols
    )

    expected_sql = sql.SQL("""
                    INSERT INTO {target} ({cols})
                    SELECT {cols} FROM {staging}
                    ON CONFLICT ({pks}) DO UPDATE
                    SET {update_clause};
                """).format(
        target=sql.Identifier(schema, target),
        cols=sql.SQL(', ').join(map(sql.Identifier, expected_cols)),
        staging=sql.Identifier(schema, staging),
        pks=sql.SQL(', ').join(map(sql.Identifier, pks)),
        update_clause=expected_update_clause
    )

    # Get the actual SQL passed to the second execute call
    actual_sql = mock_cursor.execute.call_args[0][0]

    # Compare the composed SQL objects. Direct comparison works on these objects.
    assert actual_sql == expected_sql

    # 3. Verify that the transaction was committed
    adapter.conn.commit.assert_called_once()

def test_execute_merge_no_primary_keys_raises_error(adapter):
    """
    Tests that execute_merge raises a ValueError if no primary keys are provided.
    """
    with pytest.raises(ValueError, match="primary_keys must be provided"):
        adapter.execute_merge("staging", "target", [], "schema")

def test_execute_merge_no_update_columns_skips_merge(adapter, mocker):
    """
    Tests that the merge is skipped if all columns are primary keys.
    """
    mock_cursor = adapter.conn.cursor.return_value.__enter__.return_value
    # All columns are primary keys
    mock_cursor.fetchall.return_value = [("id1",), ("id2",)]

    adapter.execute_merge("staging", "target", ["id1", "id2"], "schema")

    # The final merge SQL should NOT be executed
    # It should be called once for information_schema, but not again.
    mock_cursor.execute.assert_called_once()
    # Commit should not be called if the merge is skipped
    adapter.conn.commit.assert_not_called()
