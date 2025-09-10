import json
import pytest
from unittest.mock import MagicMock, patch, call, ANY
import pandas as pd
from datetime import datetime
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from py_load_pmda.adapters.bigquery import BigQueryAdapter, STATE_TABLE_NAME


@pytest.fixture
def mock_google_clients():
    """Pytest fixture to mock the Google Cloud client classes."""
    with patch('google.cloud.bigquery.Client') as mock_bq_client_class, \
         patch('google.cloud.storage.Client') as mock_gcs_client_class:
        mock_bq_client = mock_bq_client_class.return_value
        mock_gcs_client = mock_gcs_client_class.return_value
        yield mock_bq_client, mock_gcs_client

@pytest.fixture
def adapter(mock_google_clients):
    """Pytest fixture to create a BigQueryAdapter instance with mocked clients."""
    mock_bq_client, mock_gcs_client = mock_google_clients
    adapter = BigQueryAdapter()
    adapter.client = mock_bq_client
    adapter.gcs_client = mock_gcs_client
    adapter.project_id = "test-project"
    adapter.gcs_bucket_name = "test-bucket"
    return adapter

def test_connect_success(mock_google_clients):
    mock_bq_client_class, mock_gcs_client_class = mock_google_clients
    # We need the class, not the instance for this test
    with patch('google.cloud.bigquery.Client') as mock_bq_client_class_local, \
         patch('google.cloud.storage.Client') as mock_gcs_client_class_local:

        adapter = BigQueryAdapter()
        connection_details = {"project": "test-project", "gcs_bucket": "test-bucket"}
        adapter.connect(connection_details)

        mock_bq_client_class_local.assert_called_with(project="test-project")
        mock_gcs_client_class_local.assert_called_with(project="test-project")
        assert adapter.client is not None
        assert adapter.gcs_client is not None

def test_ensure_schema_creates_all(adapter):
    adapter.client.get_dataset.side_effect = NotFound("Dataset not found")
    adapter.client.get_table.side_effect = NotFound("Table not found")
    schema_def = {"name": "my_dataset", "tables": {"my_table": {"columns": {"col1": "STRING"}}}}

    adapter.ensure_schema(schema_def)

    adapter.client.create_dataset.assert_called_once()
    assert adapter.client.create_table.call_count == 2 # State table and my_table

def test_ensure_schema_handles_json_type(adapter):
    """Verify that a 'JSONB' column type is correctly mapped to 'JSON'."""
    adapter.client.get_dataset.side_effect = NotFound("Dataset not found")
    adapter.client.get_table.side_effect = NotFound("Table not found")

    schema_def = {
        "name": "my_dataset",
        "tables": {
            "my_json_table": {
                "columns": {
                    "id": "INT64",
                    "data": "JSONB"
                }
            }
        }
    }

    adapter.ensure_schema(schema_def)

    # Find the call to create the json table
    create_table_call = None
    for call_args in adapter.client.create_table.call_args_list:
        table_arg = call_args.args[0]
        if "my_json_table" in table_arg.table_id:
            create_table_call = call_args
            break

    assert create_table_call is not None, "create_table for my_json_table was not called"

    # The 'table' object is the first positional argument
    created_table_obj = create_table_call.args[0]
    created_schema = created_table_obj.schema
    json_field = next((field for field in created_schema if field.name == "data"), None)

    assert json_field is not None, "Field 'data' not found in created schema"
    assert json_field.field_type == "JSON", "Field 'data' should have been mapped to JSON type"


def test_bulk_load_append(adapter):
    mock_bucket, mock_blob = MagicMock(), MagicMock()
    adapter.gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    adapter.client.load_table_from_uri.return_value = MagicMock()
    df = pd.DataFrame({"col1": ["a"]})

    adapter.bulk_load(df, "my_table", "my_dataset")

    mock_blob.upload_from_file.assert_called_once()
    adapter.client.load_table_from_uri.assert_called_once()
    mock_blob.delete.assert_called_once()

def test_execute_merge(adapter):
    mock_staging_table = MagicMock()
    mock_staging_table.schema = [bigquery.SchemaField("id", "INT64"), bigquery.SchemaField("value", "STRING")]
    adapter.client.get_table.return_value = mock_staging_table

    adapter.execute_merge("staging", "target", ["id"], "my_dataset")

    adapter.client.query.assert_called_once()
    query = adapter.client.query.call_args[0][0]
    assert "MERGE test-project.my_dataset.target T" in query

def test_get_latest_state_found(adapter):
    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.items.return_value = [("dataset_id", "my_dataset"), ("last_watermark", '{"key": "value"}')]
    mock_query_job.result.return_value = [mock_row]
    adapter.client.query.return_value = mock_query_job

    state = adapter.get_latest_state("my_dataset", "my_schema")
    assert state["last_watermark"] == {"key": "value"}

def test_update_state(adapter):
    state_details = {"last_watermark": {"ts": "2025-01-01"}, "pipeline_version": "1.0"}

    adapter.update_state("my_dataset", state_details, "SUCCESS", "my_schema")

    adapter.client.query.assert_called_once()
    query_params = adapter.client.query.call_args.kwargs["job_config"].query_parameters
    params = {p.name: p.value for p in query_params}
    assert params["dataset_id"] == "my_dataset"
    assert params["status"] == "SUCCESS"
