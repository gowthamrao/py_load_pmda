from typing import Iterator

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from pytest_mock import MockerFixture

from py_load_pmda.adapters.redshift import RedshiftAdapter

# A sample DataFrame for testing
SAMPLE_DF = pd.DataFrame({"id": [1, 2], "data": ["test1", "test2"]})

# Sample connection details for Redshift
REDSHIFT_CONN_DETAILS = {
    "type": "redshift",
    "host": "test.redshift.amazonaws.com",
    "port": 5439,
    "database": "testdb",
    "user": "testuser",
    "password": "testpassword",
    "s3_staging_bucket": "test-staging-bucket",
    "iam_role": "arn:aws:iam::123456789012:role/test-redshift-role",
}


@pytest.fixture
def redshift_adapter(mocker: MockerFixture) -> RedshiftAdapter:
    """Fixture to provide a mocked RedshiftAdapter instance."""
    # Mock the redshift_connector to prevent actual DB calls
    mocker.patch("redshift_connector.connect")
    adapter = RedshiftAdapter()
    adapter.connect(REDSHIFT_CONN_DETAILS)
    return adapter


class TestRedshiftAdapter:
    """Test suite for the RedshiftAdapter."""

    @pytest.fixture(autouse=True)
    def setup_aws(self) -> Iterator[None]:
        """
        Auto-used fixture to set up mock credentials and S3 bucket using moto's
        context manager, which is more reliable than the decorator.
        """
        import os

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"

        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=REDSHIFT_CONN_DETAILS["s3_staging_bucket"])
            yield

    def test_connect(self, mocker: MockerFixture) -> None:
        """Test that connect establishes a connection and stores config."""
        mock_connect = mocker.patch("redshift_connector.connect")
        adapter = RedshiftAdapter()
        adapter.connect(REDSHIFT_CONN_DETAILS)

        mock_connect.assert_called_once()
        assert adapter.s3_staging_bucket == REDSHIFT_CONN_DETAILS["s3_staging_bucket"]
        assert adapter.iam_role == REDSHIFT_CONN_DETAILS["iam_role"]

    def test_ensure_schema(self, redshift_adapter: RedshiftAdapter) -> None:
        """Test the ensure_schema method generates correct SQL."""
        assert redshift_adapter.conn is not None
        mock_cursor = redshift_adapter.conn.cursor.return_value.__enter__.return_value
        schema_def = {
            "schema_name": "my_schema",
            "tables": {
                "my_table": {
                    "columns": {"id": "INTEGER", "data": "VARCHAR(255)"},
                    "primary_key": "id",
                }
            },
        }

        redshift_adapter.ensure_schema(schema_def)

        # Check that CREATE SCHEMA was called
        mock_cursor.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS my_schema;")

        # Check that CREATE TABLE was called with the correct, formatted SQL
        create_table_call = mock_cursor.execute.call_args_list[1][0][0]
        assert "CREATE TABLE IF NOT EXISTS my_schema.my_table" in create_table_call
        assert "id INTEGER" in create_table_call
        assert "data VARCHAR(255)" in create_table_call
        assert "PRIMARY KEY (id)" in create_table_call

    def test_bulk_load(self, redshift_adapter: RedshiftAdapter) -> None:
        """Test the full bulk_load flow: Parquet -> S3 -> COPY."""
        assert redshift_adapter.conn is not None
        mock_cursor = redshift_adapter.conn.cursor.return_value.__enter__.return_value
        s3_client = boto3.client("s3", region_name="us-east-1")

        redshift_adapter.bulk_load(SAMPLE_DF, "my_table", "my_schema", mode="append")

        # Verify S3 upload
        s3_objects = s3_client.list_objects_v2(Bucket=REDSHIFT_CONN_DETAILS["s3_staging_bucket"])
        # The object should be gone after cleanup, so we can't check for its existence.
        # Instead, we check that the COPY command was called correctly.
        assert s3_objects.get("KeyCount", 0) == 0

        # Verify the COPY command
        copy_sql = mock_cursor.execute.call_args[0][0]
        assert "COPY my_schema.my_table" in copy_sql
        assert (
            f"FROM 's3://{REDSHIFT_CONN_DETAILS['s3_staging_bucket']}/staging/my_schema_my_table"
            in copy_sql
        )
        assert f"IAM_ROLE '{REDSHIFT_CONN_DETAILS['iam_role']}'" in copy_sql
        assert "FORMAT AS PARQUET" in copy_sql

    def test_execute_merge(self, redshift_adapter: RedshiftAdapter) -> None:
        """Test the execute_merge method generates the correct MERGE SQL."""
        assert redshift_adapter.conn is not None
        mock_cursor = redshift_adapter.conn.cursor.return_value.__enter__.return_value
        # Mock the return of column names for the staging table
        mock_cursor.fetchall.return_value = [("id",), ("data",)]

        redshift_adapter.execute_merge("staging_table", "target_table", ["id"], "my_schema")

        # Verify the generated MERGE SQL
        merge_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "MERGE INTO my_schema.target_table AS target" in merge_sql
        assert "USING my_schema.staging_table AS source" in merge_sql
        assert "ON target.id = source.id" in merge_sql
        assert "WHEN MATCHED THEN" in merge_sql
        assert "UPDATE SET data = source.data" in merge_sql
        assert "WHEN NOT MATCHED THEN" in merge_sql
        assert "INSERT (id, data) VALUES (source.id, source.data)" in merge_sql

    def test_update_state(self, redshift_adapter: RedshiftAdapter) -> None:
        """Test that update_state issues a DELETE and INSERT."""
        assert redshift_adapter.conn is not None
        mock_cursor = redshift_adapter.conn.cursor.return_value.__enter__.return_value
        test_state = {"last_file": "file.zip"}

        redshift_adapter.update_state("my_dataset", test_state, "SUCCESS", "my_schema")

        # Check that it deletes the old state first
        delete_sql = mock_cursor.execute.call_args_list[0][0][0]
        delete_params = mock_cursor.execute.call_args_list[0][0][1]
        assert "DELETE FROM my_schema.ingestion_state" in delete_sql
        assert delete_params == ("my_dataset",)

        # Check that it inserts the new state
        insert_sql = mock_cursor.execute.call_args_list[1][0][0]
        insert_params = mock_cursor.execute.call_args_list[1][0][1]
        assert "INSERT INTO my_schema.ingestion_state" in insert_sql
        assert insert_params[0] == "my_dataset"
        assert insert_params[3] == "SUCCESS"
        assert insert_params[4] == '{"last_file": "file.zip"}'

    def test_get_latest_state(self, redshift_adapter: RedshiftAdapter) -> None:
        """Test retrieving the latest state."""
        assert redshift_adapter.conn is not None
        mock_cursor = redshift_adapter.conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = ('{"last_file": "file.zip"}',)

        state = redshift_adapter.get_latest_state("my_dataset", "my_schema")

        assert state == {"last_file": "file.zip"}
        mock_cursor.execute.assert_called_once_with(
            "SELECT last_watermark FROM my_schema.ingestion_state WHERE dataset_id = %s;",
            ("my_dataset",),
        )
