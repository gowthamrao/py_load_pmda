import pytest
from requests_mock import Mocker

from py_load_pmda.extractor import ReviewReportsExtractor

# More realistic mock HTML for search results page for Review Reports.
# It includes multiple links in the same cell, a distractor link, and an unrelated row.
MOCK_SEARCH_RESULTS_HTML = """
<!DOCTYPE html>
<html>
<head><title>Search Results</title></head><body>
<div id="ContentMainArea">
    <table class="result_list_table">
        <tbody>
            <tr>
                <td>コレクチム軟膏0.5%</td>
                <td>デルゴシチニブ</td>
                <td>日本たばこ産業</td>
                <td><a href="/PmdaSearch/iyakuDetail/456_2">...</a></td>
                <td>
                    <a href="/drugs/2020/P20200123/report.pdf" target="_blank">審査報告書 (2020/01/23)</a>
                    <br>
                    <a href="/drugs/2022/P20220401/re-report.pdf" target="_blank">再審査報告書 (2022/04/01)</a>
                    <br>
                    <a href="/some/other/document.pdf" target="_blank">Other Document</a>
                </td>
            </tr>
            <tr>
                <td>無関係の薬剤</td>
                <td>...</td>
                <td>...</td>
                <td>...</td>
                <td><a href="/drugs/review/unrelated.pdf" target="_blank">審査報告書</a></td>
            </tr>
        </tbody>
    </table>
</div></body></html>
"""


@pytest.fixture
def mock_pmda_review_search(requests_mock: Mocker) -> None:
    """Fixture to mock PMDA search and download for review reports."""
    search_url = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"
    # Mock the initial GET to fetch the session token
    requests_mock.get(
        search_url, text='<html><body><input name="nccharset" value="DUMMY_TOKEN"></body></html>'
    )
    # Mock the POST request that returns the search results
    requests_mock.post(search_url, text=MOCK_SEARCH_RESULTS_HTML)
    # Mock the download endpoints for the valid PDFs
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/2020/P20200123/report.pdf", content=b"Report A PDF content"
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/2022/P20220401/re-report.pdf", content=b"Report B PDF content"
    )


def test_review_report_extractor_finds_all_valid_reports(tmp_path, mock_pmda_review_search):
    """
    GIVEN a search term that matches a drug with multiple review reports,
    WHEN the ReviewReportsExtractor is run,
    THEN it should download all valid report documents and ignore irrelevant ones.
    """
    cache_dir = tmp_path / "cache"
    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))

    # Search term is a substring of the full brand name, to test the 'in' logic
    drug_to_find = "コレクチム軟膏"

    downloaded_data, new_state = extractor.extract(drug_names=[drug_to_find], last_state={})

    # The extractor should find two valid links and download them
    assert len(downloaded_data) == 2, "Should have found two valid report links."

    # Sort results by URL to make assertions deterministic
    downloaded_data.sort(key=lambda x: x[1])

    # Check for the first report
    file_path_a, source_url_a = downloaded_data[0]
    assert source_url_a == "https://www.pmda.go.jp/drugs/2020/P20200123/report.pdf"
    assert file_path_a.name == "report.pdf"
    with open(file_path_a, "rb") as f:
        assert f.read() == b"Report A PDF content"

    # Check for the second report (re-examination)
    file_path_b, source_url_b = downloaded_data[1]
    assert source_url_b == "https://www.pmda.go.jp/drugs/2022/P20220401/re-report.pdf"
    assert file_path_b.name == "re-report.pdf"
    with open(file_path_b, "rb") as f:
        assert f.read() == b"Report B PDF content"

    # Ensure the other documents (distractor and unrelated) were not downloaded
    assert not (cache_dir / "document.pdf").exists()
    assert not (cache_dir / "unrelated.pdf").exists()


def test_review_report_extractor_no_matching_links(tmp_path, requests_mock):
    """
    GIVEN a search result where a drug is found but has no review report links,
    WHEN the extractor is run,
    THEN it should not download any files.
    """
    cache_dir = tmp_path / "cache"
    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))
    search_url = extractor.search_url

    mock_html = """
    <!DOCTYPE html><html><body><div id="ContentMainArea"><table class="result_list_table"><tbody>
        <tr>
            <td>コレクチム軟膏0.5%</td><td>...</td><td>...</td><td>...</td>
            <td><a href="/a.pdf">IF</a> <a href="/b.pdf">RMP</a></td>
        </tr>
    </tbody></table></div></body></html>
    """
    requests_mock.get(search_url, text='<html><body><input name="nccharset" value="DUMMY_TOKEN"></body></html>')
    requests_mock.post(search_url, text=mock_html)

    downloaded_data, _ = extractor.extract(drug_names=["コレクチム軟膏"], last_state={})

    assert len(downloaded_data) == 0


def test_review_report_extractor_no_matching_drug(tmp_path, mock_pmda_review_search):
    """
    GIVEN a search term that does not match any drug in the results,
    WHEN the extractor is run,
    THEN it should not download any files.
    """
    cache_dir = tmp_path / "cache"
    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))

    downloaded_data, _ = extractor.extract(drug_names=["NonExistentDrug"], last_state={})

    assert len(downloaded_data) == 0


def test_review_report_extractor_no_results_table(tmp_path, requests_mock):
    """
    GIVEN a search result page that is missing the results table,
    WHEN the extractor is run,
    THEN it should handle it gracefully and not download files.
    """
    cache_dir = tmp_path / "cache"
    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))
    search_url = extractor.search_url

    mock_html = '<html><body><input name="nccharset" value="DUMMY_TOKEN"><div id="ContentMainArea"><p>No results found.</p></div></body></html>'
    requests_mock.get(search_url, text='<html><body><input name="nccharset" value="DUMMY_TOKEN"></body></html>')
    requests_mock.post(search_url, text=mock_html)

    downloaded_data, _ = extractor.extract(drug_names=["コレクチム軟膏"], last_state={})

    assert len(downloaded_data) == 0
