import json
import pytest
from unittest.mock import MagicMock, patch, call, ANY
import pandas as pd
from datetime import datetime
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from py_load_pmda.adapters.bigquery import BigQueryLoader, STATE_TABLE_NAME


@pytest.fixture
def mock_google_clients():
    """Pytest fixture to mock the Google Cloud client classes."""
    with patch('google.cloud.bigquery.Client') as mock_bq_client_class, \
         patch('google.cloud.storage.Client') as mock_gcs_client_class:
        yield mock_bq_client_class, mock_gcs_client_class

def test_connect_success(mock_google_clients):
    mock_bq_client_class, mock_gcs_client_class = mock_google_clients
    loader = BigQueryLoader()
    connection_details = {"project": "test-project", "gcs_bucket": "test-bucket"}
    loader.connect(connection_details)

    mock_bq_client_class.assert_called_with(project="test-project")
    mock_gcs_client_class.assert_called_with(project="test-project")
    assert loader.client is not None
    assert loader.gcs_client is not None

def test_ensure_schema_creates_all(mock_google_clients):
    mock_bq_client_class, _ = mock_google_clients
    client = mock_bq_client_class.return_value

    loader = BigQueryLoader()
    loader.client = client # Manually set the mocked client instance
    loader.project_id = "test-project"

    client.get_dataset.side_effect = NotFound("Dataset not found")
    client.get_table.side_effect = NotFound("Table not found")
    schema_def = {"name": "my_dataset", "tables": {"my_table": {"columns": {"col1": "STRING"}}}}

    loader.ensure_schema(schema_def)

    client.create_dataset.assert_called_once()
    assert client.create_table.call_count == 2

def test_bulk_load_append(mock_google_clients):
    mock_bq_client_class, mock_gcs_client_class = mock_google_clients
    client = mock_bq_client_class.return_value
    gcs_client = mock_gcs_client_class.return_value

    loader = BigQueryLoader()
    loader.client = client
    loader.gcs_client = gcs_client
    loader.project_id = "test-project"
    loader.gcs_bucket_name = "test-bucket"

    mock_bucket, mock_blob = MagicMock(), MagicMock()
    gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    client.load_table_from_uri.return_value = MagicMock()
    df = pd.DataFrame({"col1": ["a"]})

    loader.bulk_load(df, "my_table", "my_dataset")

    mock_blob.upload_from_file.assert_called_once()
    client.load_table_from_uri.assert_called_once()
    mock_blob.delete.assert_called_once()

def test_execute_merge(mock_google_clients):
    mock_bq_client_class, _ = mock_google_clients
    client = mock_bq_client_class.return_value

    loader = BigQueryLoader()
    loader.client = client
    loader.project_id = "test-project"

    mock_staging_table = MagicMock()
    mock_staging_table.schema = [bigquery.SchemaField("id", "INT64"), bigquery.SchemaField("value", "STRING")]
    client.get_table.return_value = mock_staging_table

    loader.execute_merge("staging", "target", ["id"], "my_dataset")

    client.query.assert_called_once()
    query = client.query.call_args[0][0]
    assert "MERGE test-project.my_dataset.target T" in query

def test_get_latest_state_found(mock_google_clients):
    mock_bq_client_class, _ = mock_google_clients
    client = mock_bq_client_class.return_value

    loader = BigQueryLoader()
    loader.client = client
    loader.project_id = "test-project"

    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.items.return_value = [("dataset_id", "my_dataset"), ("last_watermark", '{"key": "value"}')]
    mock_query_job.result.return_value = [mock_row]
    client.query.return_value = mock_query_job

    state = loader.get_latest_state("my_dataset", "my_schema")
    assert state["last_watermark"] == {"key": "value"}

def test_update_state(mock_google_clients):
    mock_bq_client_class, _ = mock_google_clients
    client = mock_bq_client_class.return_value

    loader = BigQueryLoader()
    loader.client = client
    loader.project_id = "test-project"

    state_details = {"last_watermark": {"ts": "2025-01-01"}, "pipeline_version": "1.0"}

    loader.update_state("my_dataset", state_details, "SUCCESS", "my_schema")

    client.query.assert_called_once()
    query_params = client.query.call_args.kwargs["job_config"].query_parameters
    params = {p.name: p.value for p in query_params}
    assert params["dataset_id"] == "my_dataset"
    assert params["status"] == "SUCCESS"
