import pytest
from pathlib import Path
from typing import Any
from py_load_pmda.extractor import ApprovalsExtractor

# Helper function to get the path to the fixtures directory
def get_fixture_path() -> Path:
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def approvals_extractor(tmp_path: Path) -> ApprovalsExtractor:
    """Fixture to create an ApprovalsExtractor instance with a temporary cache directory."""
    return ApprovalsExtractor(cache_dir=str(tmp_path / "cache"))

def test_find_yearly_approval_url(approvals_extractor: ApprovalsExtractor, requests_mock: Any):
    """Test that the correct URL for a specific year's approval list is found."""
    html_content = (get_fixture_path() / 'approvals_main_page.html').read_text()
    requests_mock.get(approvals_extractor.approvals_list_url, text=html_content)
    soup = approvals_extractor._get_page_content(approvals_extractor.approvals_list_url)
    url = approvals_extractor._find_yearly_approval_url(soup, 2024)
    assert url == "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010_2024.html"

def test_find_yearly_approval_url_not_found(approvals_extractor: ApprovalsExtractor, requests_mock: Any):
    """Test that a ValueError is raised if the year link is not found."""
    html_content = (get_fixture_path() / 'approvals_main_page.html').read_text()
    requests_mock.get(approvals_extractor.approvals_list_url, text=html_content)
    soup = approvals_extractor._get_page_content(approvals_extractor.approvals_list_url)
    with pytest.raises(ValueError, match="Could not find link for year 2026"):
        approvals_extractor._find_yearly_approval_url(soup, 2026)

def test_find_excel_download_url(approvals_extractor: ApprovalsExtractor, requests_mock: Any):
    """Test that the correct Excel file download URL is found."""
    html_content = (get_fixture_path() / 'approvals_2024_page.html').read_text()
    requests_mock.get("http://dummy.com/2024_approvals.html", text=html_content)
    soup = approvals_extractor._get_page_content("http://dummy.com/2024_approvals.html")
    url = approvals_extractor._find_excel_download_url(soup)
    assert url == "https://www.pmda.go.jp/files/000263199.xlsx"

def test_find_excel_download_url_not_found(approvals_extractor: ApprovalsExtractor, requests_mock: Any):
    """Test that a ValueError is raised if the Excel link is not found."""
    html_content = "<html><body><p>No link here.</p></body></html>"
    requests_mock.get("http://dummy.com/no_link.html", text=html_content)
    soup = approvals_extractor._get_page_content("http://dummy.com/no_link.html")
    with pytest.raises(ValueError, match="Could not find the Excel file download link."):
        approvals_extractor._find_excel_download_url(soup)

def test_extract_approvals(approvals_extractor: ApprovalsExtractor, requests_mock: Any):
    """Test the full extract process for the ApprovalsExtractor."""
    main_page_html = (get_fixture_path() / 'approvals_main_page.html').read_text()
    yearly_page_html = (get_fixture_path() / 'approvals_2024_page.html').read_text()
    excel_content = b"dummy excel data"

    requests_mock.get(approvals_extractor.approvals_list_url, text=main_page_html)
    requests_mock.get("https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010_2024.html", text=yearly_page_html)
    requests_mock.get("https://www.pmda.go.jp/files/000263199.xlsx", content=excel_content)

    file_path, url, state = approvals_extractor.extract(year=2024, last_state={})

    assert file_path.name == "000263199.xlsx"
    assert file_path.read_bytes() == excel_content
    assert url == "https://www.pmda.go.jp/files/000263199.xlsx"
    assert state == {} # No ETag or Last-Modified in mock response
