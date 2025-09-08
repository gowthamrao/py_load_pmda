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
    downloaded_files, new_state = extractor.extract(drug_names=["some_drug"], last_state={})

    assert len(downloaded_files) == 1
    assert downloaded_files[0].name == "dummy_insert.pdf"
    assert downloaded_files[0].read_bytes() == b"dummy pdf content"
    assert "https://www.pmda.go.jp/drugs/2023/dummy_insert.pdf" in new_state


@patch("tabula.read_pdf")
def test_package_inserts_parser(mock_read_pdf, mocker):
    """Tests the PackageInsertsParser logic by mocking the tabula library."""
    # Arrange
    # Mock Path.exists to prevent FileNotFoundError on a dummy path
    mocker.patch.object(Path, 'exists', return_value=True)

    sample_data = {'col1': ['A', 'B'], 'col2': [1, 2]}
    mock_df = pd.DataFrame(sample_data)
    mock_read_pdf.return_value = [mock_df] # tabula returns a list of DataFrames

    parser = PackageInsertsParser()
    dummy_pdf_path = Path("dummy.pdf")

    # Act
    result_df = parser.parse(dummy_pdf_path)

    # Assert
    assert not result_df.empty
    pd.testing.assert_frame_equal(result_df, mock_df)
    mock_read_pdf.assert_called_once_with(dummy_pdf_path, pages="all", multiple_tables=True, lattice=True)


def test_package_inserts_transformer():
    """Tests the PackageInsertsTransformer logic."""
    # Arrange
    source_url = "https://www.pmda.go.jp/drugs/2023/dummy_insert.pdf"
    raw_df = pd.DataFrame({'col1': ['A', 'B'], 'col2': [1, 2]})
    transformer = PackageInsertsTransformer(source_url=source_url)

    # Act
    transformed_df = transformer.transform(raw_df)

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
    assert len(raw_data["extracted_tables"]) == 2
    assert raw_data["extracted_tables"][0]['col1'] == 'A'
