import pytest
import requests_mock

from py_load_pmda.extractor import PackageInsertsExtractor

# This mock HTML simulates a search result page with multiple similar items.
# The goal is to ensure the extractor can pick the correct link from the table.
MOCK_SEARCH_RESULTS_HTML = """
<!DOCTYPE html>
<html>
<head><title>Search Results</title></head><body>
<div id="ContentMainArea">
    <h3>Search Results: 3 items</h3>
    <table class="result_list_table">
        <tbody>
            <tr>
                <td>Brand Name</td>
                <td>Generic Name</td>
                <td>Applicant</td>
                <td>Link 1</td>
                <td>PDF Link</td>
            </tr>
            <tr>
                <td>ロキソニンS</td>
                <td>ロキソプロフェンナトリウム水和物</td>
                <td>第一三共ヘルスケア</td>
                <td><a href="/PmdaSearch/iyakuDetail/123_1">...</a></td>
                <td><a href="https://www.pmda.go.jp/drugs/info/loxonin_s.pdf" target="_blank">PDF</a></td>
            </tr>
            <tr>
                <td>ロキソニンSプラス</td>
                <td>ロキソプロフェンナトリウム水和物</td>
                <td>第一三共ヘルスケア</td>
                <td><a href="/PmdaSearch/iyakuDetail/123_2">...</a></td>
                <td><a href="https://www.pmda.go.jp/drugs/info/loxonin_s_plus.pdf" target="_blank">PDF</a></td>
            </tr>
             <tr>
                <td>ロキソニンSプレミアム</td>
                <td>ロキソプロフェンナトリウム水和物</td>
                <td>第一三共ヘルスケア</td>
                <td><a href="/PmdaSearch/iyakuDetail/123_3">...</a></td>
                <td><a href="https://www.pmda.go.jp/drugs/info/loxonin_s_premium.pdf" target="_blank">PDF</a></td>
            </tr>
        </tbody>
    </table>
</div></body></html>
"""


@pytest.fixture
def mock_pmda_search(requests_mock: requests_mock.Mocker) -> None:
    """Fixture to mock PMDA search and download requests."""
    search_url = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"
    # Mock the initial GET request to fetch the token
    requests_mock.get(
        search_url, text='<html><body><input name="nccharset" value="DUMMY_TOKEN"></body></html>'
    )
    # Mock the search POST request
    requests_mock.post(search_url, text=MOCK_SEARCH_RESULTS_HTML)
    # Mock the download GET requests for each potential PDF
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/info/loxonin_s.pdf", content=b"Loxonin S PDF content"
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/info/loxonin_s_plus.pdf",
        content=b"Loxonin S Plus PDF content",
    )
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/info/loxonin_s_premium.pdf",
        content=b"Loxonin S Premium PDF content",
    )


def test_package_insert_extractor_finds_exact_match(tmp_path, mock_pmda_search):
    """
    GIVEN a search term that returns multiple results in a table,
    WHEN the PackageInsertsExtractor is run with a specific drug name,
    THEN it should download the PDF for the exact matching drug name from the table.
    """
    cache_dir = tmp_path / "cache"
    extractor = PackageInsertsExtractor(cache_dir=str(cache_dir))

    # We want to find "ロキソニンSプラス" specifically.
    # The current naive implementation finds the first link: loxonin_s.pdf
    # The improved implementation should find: loxonin_s_plus.pdf
    drug_to_find = "ロキソニンSプラス"

    downloaded_data, new_state = extractor.extract(drug_names=[drug_to_find], last_state={})

    # This test will fail until the extractor logic is improved
    assert len(downloaded_data) == 1, "Should have downloaded exactly one file."
    file_path, source_url = downloaded_data[0]

    # Assert that the correct URL was identified and the correct file was downloaded
    assert source_url == "https://www.pmda.go.jp/drugs/info/loxonin_s_plus.pdf"
    assert file_path.name == "loxonin_s_plus.pdf"

    # Assert that the content of the downloaded file is correct
    with open(file_path, "rb") as f:
        content = f.read()
    assert content == b"Loxonin S Plus PDF content"

    # Assert that the other, incorrect file was NOT downloaded
    wrong_file_path = cache_dir / "loxonin_s.pdf"
    assert not wrong_file_path.exists(), "Should not have downloaded the first PDF in the list."
