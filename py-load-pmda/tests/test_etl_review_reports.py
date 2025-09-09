import hashlib
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from py_load_pmda.cli import app
from py_load_pmda.interfaces import LoaderInterface
from py_load_pmda.transformer import ReviewReportsTransformer
from py_load_pmda.utils import to_iso_date
from typer.testing import CliRunner

# A re-usable mock DB adapter to spy on calls to the database
class MockDBAdapter(LoaderInterface):
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
        self.execute_sql_spy = MagicMock()
        self.get_all_states_spy = MagicMock(return_value=[])

    def connect(self, connection_details): self.connect_spy(connection_details)
    def ensure_schema(self, schema_definition): self.ensure_schema_spy(schema_definition)
    def bulk_load(self, data, target_table, schema, mode='append'): self.bulk_load_spy(data=data, target_table=target_table, schema=schema, mode=mode)
    def execute_merge(self, staging_table, target_table, primary_keys, schema): self.execute_merge_spy(staging_table=staging_table, target_table=target_table, primary_keys=primary_keys, schema=schema)
    def get_latest_state(self, dataset_id, schema): return self.get_latest_state_spy(dataset_id=dataset_id, schema=schema)
    def update_state(self, dataset_id, state, status, schema): self.update_state_spy(dataset_id=dataset_id, state=state, status=status, schema=schema)
    def get_all_states(self, schema: str): return self.get_all_states_spy(schema=schema)
    def commit(self): self.commit_spy()
    def close(self): self.close_spy()
    def rollback(self): self.rollback_spy()
    def execute_sql(self, query, params=None): self.execute_sql_spy(query, params)

@pytest.fixture
def mock_db_adapter_fixture():
    return MockDBAdapter()

@pytest.fixture
def fixture_path() -> Path:
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def html_fixture(fixture_path: Path) -> str:
    with open(fixture_path / "sample_review_report_search.html", "r", encoding="utf-8") as f:
        return f.read()

# --- Unit Test for the Transformer (Restored) ---
def test_review_reports_transformer_unit():
    """Unit test for the ReviewReportsTransformer to ensure it extracts data correctly."""
    mock_text = "販売名: テストドラッグ錠\n申請者名: テスト製薬株式会社\n申請年月日: 令和7年1月15日\n承認年月日: 2025年9月10日"
    parser_output = (mock_text, [pd.DataFrame({'colA': [1]})])
    transformer = ReviewReportsTransformer(source_url="http://example.com/report.pdf")
    df = transformer.transform(parser_output)
    assert df.iloc[0]['brand_name_jp'] == "テストドラッグ錠"
    assert df.iloc[0]['application_date'] == to_iso_date(pd.Series(["令和7年1月15日"]))[0]


# --- End-to-End Test for the CLI ---
@patch("py_load_pmda.cli.get_db_adapter")
@patch("py_load_pmda.extractor.BaseExtractor._send_post_request")
@patch("py_load_pmda.extractor.BaseExtractor._download_file")
@patch("py_load_pmda.parser.pdfplumber.open")
def test_review_reports_pipeline_e2e(
    mock_pdfplumber_open, mock_download, mock_post, mock_get_db_adapter, mock_db_adapter_fixture, html_fixture
):
    """
    A true end-to-end integration test for the 'review_reports' pipeline.
    """
    runner = CliRunner()
    mock_get_db_adapter.return_value = mock_db_adapter_fixture
    mock_post.return_value.text = html_fixture

    source_url = "https://www.pmda.go.jp/drugs/2008/PDFofTempu/672260_22300AMX00557_C100_1.pdf"
    # This time, we don't need to mock Path.exists because we can return a real path
    # to a file that actually exists. The content doesn't matter since pdfplumber is mocked.
    dummy_pdf_path = Path(__file__).parent / "fixtures" / "sample_review_report.pdf"
    mock_download.return_value = dummy_pdf_path

    mock_pdf_page = MagicMock()
    mock_pdf_page.extract_text.return_value = "販売名: Test Drug 60mg\n一般的名称: Test-profen\n申請者名: Test Pharma Inc.\n申請年月日: 2024年1月1日\n承認年月日: 2025年2月2日\n\n審査の概要\nThis is the summary text."
    mock_pdf_page.extract_tables.return_value = []

    mock_pdf_object = MagicMock()
    mock_pdf_object.pages = [mock_pdf_page]
    mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf_object

    result = runner.invoke(
        app,
        ["run", "--dataset", "review_reports", "--drug-name", "ロキソニン錠６０ｍｇ"],
    )

    assert result.exit_code == 0, result.stdout
    assert mock_db_adapter_fixture.ensure_schema_spy.call_count == 2
    mock_db_adapter_fixture.execute_merge_spy.assert_called_once()
    loaded_df = mock_db_adapter_fixture.bulk_load_spy.call_args.kwargs['data']

    record = loaded_df.iloc[0]
    assert record["brand_name_jp"] == "Test Drug 60mg"
    assert record["application_date"] == date(2024, 1, 1)
    assert record["approval_date"] == date(2025, 2, 2)
    assert "This is the summary text." in record["review_summary_text"]
    assert record["_meta_source_url"] == source_url

    mock_db_adapter_fixture.update_state_spy.assert_called_once()
    mock_db_adapter_fixture.commit_spy.assert_called_once()
    mock_db_adapter_fixture.close_spy.assert_called_once()
