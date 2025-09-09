import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import MagicMock, patch

from py_load_pmda.extractor import PackageInsertsExtractor
from py_load_pmda.parser import PackageInsertsParser
from py_load_pmda.transformer import PackageInsertsTransformer

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
        yield self.content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_pmda_search(mocker):
    """Mocks the requests.post and requests.get calls for the extractor test."""

    # Mock the POST request to the search form
    mocker.patch(
        "requests.post",
        return_value=MockResponse(
            text="""
            <html>
                <div id="ContentMainArea">
                    <a href="/drugs/2023/dummy_insert.pdf">Download PDF</a>
                </div>
            </html>
            """
        )
    )

    # Mock the GET request for the PDF download
    mocker.patch(
        "requests.get",
        return_value=MockResponse(content=b"dummy pdf content")
    )


def test_package_inserts_extractor(mock_pmda_search, tmp_path):
    """Tests the PackageInsertsExtractor logic."""
    extractor = PackageInsertsExtractor(cache_dir=str(tmp_path))
    downloaded_data, new_state = extractor.extract(drug_names=["some_drug"], last_state={})

    assert len(downloaded_data) == 1
    file_path, source_url = downloaded_data[0]

    assert file_path.name == "dummy_insert.pdf"
    assert file_path.read_bytes() == b"dummy pdf content"
    assert source_url == "https://www.pmda.go.jp/drugs/2023/dummy_insert.pdf"
    assert "https://www.pmda.go.jp/drugs/2023/dummy_insert.pdf" in new_state


@patch("pdfplumber.open")
def test_package_inserts_parser(mock_pdfplumber_open, mocker):
    """Tests the PackageInsertsParser logic by mocking the pdfplumber library."""
    # Arrange
    mock_pdf = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "This is sample text."
    mock_page.extract_tables.return_value = [[['col1', 'col2'], ['A', 1], ['B', 2]]]
    mock_pdf.pages = [mock_page]
    mock_pdf.__enter__.return_value = mock_pdf
    mock_pdfplumber_open.return_value = mock_pdf

    parser = PackageInsertsParser()
    dummy_pdf_path = Path("dummy.pdf")
    mocker.patch.object(Path, 'exists', return_value=True)

    # Act
    full_text, tables = parser.parse(dummy_pdf_path)

    # Assert
    assert full_text == "This is sample text."
    assert len(tables) == 1
    assert isinstance(tables[0], pd.DataFrame)
    assert tables[0].columns.tolist() == ['col1', 'col2']
    mock_pdfplumber_open.assert_called_once_with(dummy_pdf_path)


def test_package_inserts_transformer():
    """Tests the PackageInsertsTransformer logic."""
    # Arrange
    source_url = "https://www.pmda.go.jp/drugs/2023/dummy_insert.pdf"
    raw_df = pd.DataFrame({'col1': ['A', 'B'], 'col2': [1, 2]})
    parser_output = ("This is the full text.", [raw_df])
    transformer = PackageInsertsTransformer(source_url=source_url)

    # Act
    transformed_df = transformer.transform(parser_output)

    # Assert
    assert len(transformed_df) == 1
    assert "document_id" in transformed_df.columns
    assert "raw_data_full" in transformed_df.columns
    assert "_meta_source_url" in transformed_df.columns
    assert "_meta_source_content_hash" in transformed_df.columns

    assert transformed_df.iloc[0]["_meta_source_url"] == source_url

    # Check that the raw data is correctly JSON-ified
    import json
    raw_data = json.loads(transformed_df.iloc[0]["raw_data_full"])
    assert raw_data["source_file_type"] == "pdf"
    assert raw_data["full_text"] == "This is the full text."
    assert len(raw_data["extracted_tables"]) == 1
    assert len(raw_data["extracted_tables"][0]) == 2
    assert raw_data["extracted_tables"][0][0]['col1'] == 'A'
