import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path

from py_load_pmda.cli import app
from py_load_pmda.interfaces import LoaderInterface
from py_load_pmda.transformer import ReviewReportsTransformer
from py_load_pmda.utils import to_iso_date

# --- Re-usable Mocks and Fixtures ---

class MockDBAdapter(LoaderInterface):
    """A mock database adapter that spies on method calls with correct signatures."""
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

    def connect(self, connection_details: dict): self.connect_spy(connection_details)
    def ensure_schema(self, schema_definition: dict): self.ensure_schema_spy(schema_definition)
    def bulk_load(self, data, target_table: str, schema: str, mode: str): self.bulk_load_spy(data=data, target_table=target_table, schema=schema, mode=mode)
    def execute_merge(self, staging_table: str, target_table: str, primary_keys: list, schema: str): self.execute_merge_spy(staging_table=staging_table, target_table=target_table, primary_keys=primary_keys, schema=schema)
    def get_latest_state(self, dataset_id: str, schema: str) -> dict: return self.get_latest_state_spy(dataset_id=dataset_id, schema=schema)
    def update_state(self, dataset_id: str, state: dict, status: str, schema: str): self.update_state_spy(dataset_id=dataset_id, state=state, status=status, schema=schema)
    def get_all_states(self, schema: str) -> list: return self.get_all_states_spy(schema=schema)
    def commit(self): self.commit_spy()
    def close(self): self.close_spy()
    def rollback(self): self.rollback_spy()
    def execute_sql(self, query: str, params=None): self.execute_sql_spy(query, params)

@pytest.fixture
def mock_db_adapter(): return MockDBAdapter()

class MockResponse:
    def __init__(self, text="", status_code=200, headers=None, content=b""):
        self.text, self.status_code, self.headers, self.content = text, status_code, headers or {}, content
    def raise_for_status(self):
        if self.status_code >= 400: raise Exception("HTTP Error")
    def iter_content(self, cs): yield self.content
    def __enter__(self): return self
    def __exit__(self, *args): pass

# --- Unit Test for the Transformer ---
def test_review_reports_transformer_unit():
    """Unit test for the ReviewReportsTransformer to ensure it extracts data correctly."""
    mock_text = "販売名: テストドラッグ錠\n申請者名: テスト製薬株式会社\n申請年月日: 令和7年1月15日\n承認年月日: 2025年9月10日"
    parser_output = (mock_text, [pd.DataFrame({'colA': [1]})])
    transformer = ReviewReportsTransformer(source_url="http://example.com/report.pdf")
    df = transformer.transform(parser_output)
    assert df.iloc[0]['brand_name_jp'] == "テストドラッグ錠"
    assert df.iloc[0]['application_date'] == to_iso_date(pd.Series(["令和7年1月15日"]))[0]

# --- End-to-End Test for the CLI ---

@patch("py_load_pmda.cli.AVAILABLE_TRANSFORMERS")
@patch("py_load_pmda.cli.AVAILABLE_PARSERS")
@patch("py_load_pmda.cli.AVAILABLE_EXTRACTORS")
@patch("py_load_pmda.cli.schemas.DATASET_SCHEMAS")
@patch("py_load_pmda.cli.load_config")
@patch("py_load_pmda.cli.get_db_adapter")
def test_review_reports_etl_pipeline(
    mock_get_db, mock_load_config, mock_schemas, mock_extractors, mock_parsers, mock_transformers, mock_db_adapter
):
    """Tests the end-to-end ETL pipeline for the 'review_reports' dataset by mocking the ETL classes."""
    runner = CliRunner()
    mock_get_db.return_value = mock_db_adapter
    mock_load_config.return_value = {
        "database": {"type": "postgres"},
        "datasets": {"review_reports": {"extractor": "RRX", "parser": "RRP", "transformer": "RRT", "table_name": "pmda_review_reports", "schema_name": "public", "load_mode": "merge", "primary_key": ["document_id"]}},
    }
    mock_schemas.get.return_value = {"schema_name": "public", "tables": {"pmda_review_reports": {"columns": {"brand_name_jp": "TEXT"}}}}

    mock_extractor_inst = mock_extractors.get.return_value.return_value
    mock_file_path = MagicMock(spec=Path, name="MockPath")
    mock_file_path.name = "test.pdf"
    mock_extractor_inst.extract.return_value = ([(mock_file_path, "http://a.b/c.pdf")], {"etag": "new"})

    mock_parser_inst = mock_parsers.get.return_value.return_value
    mock_parser_inst.parse.return_value = ("販売名: E2Eテストドラッグ\n申請年月日: 2024年1月1日", [])

    mock_transformers.get.return_value = ReviewReportsTransformer

    result = runner.invoke(app, ["run", "--dataset", "review_reports", "--drug-name", "testdrug"])

    assert result.exit_code == 0, result.stdout

    mock_db_adapter.bulk_load_spy.assert_called_once()
    loaded_data = mock_db_adapter.bulk_load_spy.call_args.kwargs['data']
    assert loaded_data.iloc[0]['brand_name_jp'] == "E2Eテストドラッグ"
    assert loaded_data.iloc[0]['application_date'] == to_iso_date(pd.Series(["2024年1月1日"]))[0]
