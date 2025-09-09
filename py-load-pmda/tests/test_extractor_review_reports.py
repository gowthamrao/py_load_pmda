import pytest
import requests_mock
from py_load_pmda.extractor import ReviewReportsExtractor

# Mock HTML for search results page for Review Reports.
# This simulates finding multiple reports for different strengths of the same drug.
MOCK_SEARCH_RESULTS_HTML = """
<!DOCTYPE html>
<html>
<head><title>Search Results</title></head><body>
<div id="ContentMainArea">
    <h3>Search Results: 2 items</h3>
    <table class="results-table">
        <tbody>
            <tr>
                <td>Brand Name</td>
                <td>Generic Name</td>
                <td>Applicant</td>
                <td>Link 1</td>
                <td>PDF Link</td>
            </tr>
            <tr>
                <td>コレクチム軟膏0.25%</td>
                <td>デルゴシチニブ</td>
                <td>日本たばこ産業</td>
                <td><a href="/PmdaSearch/iyakuDetail/456_1">...</a></td>
                <td><a href="https://www.pmda.go.jp/drugs/review/report_a.pdf" target="_blank">PDF</a></td>
            </tr>
            <tr>
                <td>コレクチム軟膏0.5%</td>
                <td>デルゴシチニブ</td>
                <td>日本たばこ産業</td>
                <td><a href="/PmdaSearch/iyakuDetail/456_2">...</a></td>
                <td><a href="https://www.pmda.go.jp/drugs/review/report_b.pdf" target="_blank">PDF</a></td>
            </tr>
        </tbody>
    </table>
</div></body></html>
"""


@pytest.fixture
def mock_pmda_review_search(requests_mock: requests_mock.Mocker) -> None:
    """Fixture to mock PMDA search and download for review reports."""
    requests_mock.post(
        "https://www.pmda.go.jp/PmdaSearch/iyakuSearch",
        text=MOCK_SEARCH_RESULTS_HTML
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/review/report_a.pdf",
        content=b"Report A PDF content"
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/review/report_b.pdf",
        content=b"Report B PDF content"
    )


def test_review_report_extractor_finds_exact_match(tmp_path, mock_pmda_review_search):
    """
    GIVEN a search term for a review report that returns multiple results,
    WHEN the ReviewReportsExtractor is run,
    THEN it should download the PDF for the exact matching drug name.
    """
    cache_dir = tmp_path / "cache"
    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))

    # We want to find the 0.5% version specifically
    drug_to_find = "コレクチム軟膏0.5%"

    downloaded_data, new_state = extractor.extract(drug_names=[drug_to_find], last_state={})

    # This test will fail until the extractor is improved
    assert len(downloaded_data) == 1
    file_path, source_url = downloaded_data[0]

    assert source_url == "https://www.pmda.go.jp/drugs/review/report_b.pdf"
    assert file_path.name == "report_b.pdf"
    with open(file_path, "rb") as f:
        content = f.read()
    assert content == b"Report B PDF content"

    wrong_file_path = cache_dir / "report_a.pdf"
    assert not wrong_file_path.exists()
