import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock, patch
import pandas as pd

from py_load_pmda.cli import app
from py_load_pmda.interfaces import LoaderInterface

# A dummy PDF content to be returned by the mocked download
DUMMY_PDF_CONTENT = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"

class MockDBAdapter(LoaderInterface):
    """A mock database adapter that spies on method calls."""
    def __init__(self):
        self.connect_spy = MagicMock()
        self.ensure_schema_spy = MagicMock()
        self.bulk_load_spy = MagicMock()
        self.execute_merge_spy = MagicMock()
        self.get_latest_state_spy = MagicMock(return_value={})
        self.update_state_spy = MagicMock()
        self.commit_spy = MagicMock()
        self.close_spy = MagicMock()
        self.rollback_spy = MagicMock()
        self.get_all_states_spy = MagicMock(return_value=[])
        self.execute_sql_spy = MagicMock()

    def connect(self, connection_details: dict) -> None:
        self.connect_spy(connection_details)

    def ensure_schema(self, schema_definition: dict) -> None:
        self.ensure_schema_spy(schema_definition)

    def bulk_load(
        self, data: pd.DataFrame, target_table: str, schema: str, mode: str = "append"
    ) -> None:
        self.bulk_load_spy(data=data, target_table=target_table, schema=schema, mode=mode)

    def execute_merge(
        self, staging_table: str, target_table: str, primary_keys: list[str], schema: str
    ) -> None:
        self.execute_merge_spy(
            staging_table=staging_table,
            target_table=target_table,
            primary_keys=primary_keys,
            schema=schema,
        )

    def get_latest_state(self, dataset_id: str, schema: str) -> dict:
        return self.get_latest_state_spy(dataset_id=dataset_id, schema=schema)

    def update_state(self, dataset_id: str, state: dict, status: str, schema: str) -> None:
        self.update_state_spy(dataset_id=dataset_id, state=state, status=status, schema=schema)

    def get_all_states(self, schema: str) -> list[dict]:
        return self.get_all_states_spy(schema=schema)

    def commit(self) -> None:
        self.commit_spy()

    def close(self) -> None:
        self.close_spy()

    def rollback(self) -> None:
        self.rollback_spy()

    def execute_sql(self, query: str) -> None:
        self.execute_sql_spy(query)

@pytest.fixture
def mock_db_adapter():
    """Provides a mock database adapter for testing."""
    return MockDBAdapter()

class MockResponse:
    """Helper class to mock requests.Response objects."""
    def __init__(self, text="", status_code=200, headers=None, content=b""):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content
        self.apparent_encoding = "utf-8"
        self.encoding = None

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception("HTTP Error")

    def iter_content(self, chunk_size):
        for byte in self.content:
            yield bytes([byte])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@patch("py_load_pmda.cli.load_config")
@patch("py_load_pmda.cli.get_db_adapter")
@patch("py_load_pmda.extractor.requests.post")
@patch("py_load_pmda.extractor.requests.get")
@patch("py_load_pmda.parser.tabula.read_pdf")
def test_review_reports_etl_pipeline(
    mock_read_pdf, mock_get, mock_post, mock_get_db_adapter, mock_load_config, mock_db_adapter
):
    """
    Tests the end-to-end ETL pipeline for the 'review_reports' dataset.
    This test mocks all external dependencies (network, database, config).
    """
    # --- Arrange ---
    runner = CliRunner()

    # Mock the configuration file
    mock_load_config.return_value = {
        "database": {"type": "postgres"},
        "datasets": {
            "review_reports": {
                "extractor": "ReviewReportsExtractor",
                "parser": "ReviewReportsParser",
                "transformer": "ReviewReportsTransformer",
                "table_name": "pmda_review_reports",
                "schema_name": "public",
                "load_mode": "merge",
                "primary_key": ["document_id"],
            }
        },
    }

    # Mock the database adapter
    mock_get_db_adapter.return_value = mock_db_adapter

    # Mock the network requests
    # 1. The POST request to the search form
    mock_post.return_value = MockResponse(
        text='<html><div id="ContentMainArea"><a href="/drugs/review/report.pdf">PDF</a></div></html>'
    )
    # 2. The GET request to download the PDF
    mock_get.return_value = MockResponse(content=DUMMY_PDF_CONTENT, headers={"ETag": "dummy-etag"})

    # Mock the PDF parser
    mock_read_pdf.return_value = [pd.DataFrame({"column1": ["data1"], "column2": ["data2"]})]

    # --- Act ---
    result = runner.invoke(app, ["run", "--dataset", "review_reports", "--drug-name", "testdrug"])

    # --- Assert ---
    # Check that the command executed successfully
    assert result.exit_code == 0
    assert "ETL run for dataset 'review_reports' completed successfully" in result.stdout

    # Assert that the database adapter was used correctly
    mock_get_db_adapter.assert_called_once_with("postgres")
    mock_db_adapter.connect_spy.assert_called_once()
    mock_db_adapter.ensure_schema_spy.assert_called() # Called for target and staging
    mock_db_adapter.get_latest_state_spy.assert_called_once_with(dataset_id="review_reports", schema="public")

    # Assert that the POST request was made to the search portal
    mock_post.assert_called_once()
    assert "nameWord" in mock_post.call_args[1]["data"]
    assert mock_post.call_args[1]["data"]["nameWord"] == "testdrug"
    assert mock_post.call_args[1]["data"]["dispColumnsList[0]"] == "7"

    # Assert that the PDF was "downloaded"
    mock_get.assert_called_once_with(
        "https://www.pmda.go.jp/drugs/review/report.pdf", stream=True, timeout=30, headers={}
    )

    # Assert that the parser was called
    mock_read_pdf.assert_called_once()

    # Assert that the data was loaded via the staging/merge process
    # 1. Staging table is created (part of ensure_schema calls)
    # 2. Bulk load into staging table
    mock_db_adapter.bulk_load_spy.assert_called_once()
    assert mock_db_adapter.bulk_load_spy.call_args[1]["target_table"] == "staging_pmda_review_reports"
    # 3. Merge from staging to final table
    mock_db_adapter.execute_merge_spy.assert_called_once_with(
        staging_table="staging_pmda_review_reports",
        target_table="pmda_review_reports",
        primary_keys=["document_id"],
        schema="public",
    )

    # Assert that the final state was updated
    mock_db_adapter.update_state_spy.assert_called_once()
    assert mock_db_adapter.update_state_spy.call_args[1]["dataset_id"] == "review_reports"
    assert mock_db_adapter.update_state_spy.call_args[1]["status"] == "SUCCESS"
    assert "https://www.pmda.go.jp/drugs/review/report.pdf" in mock_db_adapter.update_state_spy.call_args[1]["state"]

    # Assert that the staging table was dropped
    mock_db_adapter.execute_sql_spy.assert_called_once_with("DROP TABLE IF EXISTS public.staging_pmda_review_reports;")

    # Assert that the connection was closed
    mock_db_adapter.commit_spy.assert_called_once()
    mock_db_adapter.close_spy.assert_called_once()
