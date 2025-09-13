import pytest
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from py_load_pmda.extractor import ApprovalsExtractor

# Helper function to get the path to the fixtures directory
def get_fixture_path() -> Path:
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def approvals_extractor(tmp_path: Path) -> ApprovalsExtractor:
    """Fixture to create an ApprovalsExtractor instance with a temporary cache directory."""
    return ApprovalsExtractor(cache_dir=str(tmp_path / "cache"))

@pytest.fixture
def approvals_main_page_html() -> BeautifulSoup:
    """Fixture to load the main approvals page HTML."""
    html_content = (get_fixture_path() / 'approvals_main_page.html').read_text(encoding='utf-8')
    return BeautifulSoup(html_content, "html.parser")

@pytest.fixture
def approvals_2025_page_html() -> BeautifulSoup:
    """Fixture to load the 2025 approvals page HTML."""
    html_content = (get_fixture_path() / 'approvals_2025_page.html').read_text(encoding='utf-8')
    return BeautifulSoup(html_content, "html.parser")

def test_find_yearly_approval_url_not_found(approvals_extractor: ApprovalsExtractor, approvals_main_page_html: BeautifulSoup):
    """
    Test that a ValueError is raised if the year link is not found.
    """
    with pytest.raises(ValueError, match="Could not find link for year 2099"):
        approvals_extractor._find_yearly_approval_url(approvals_main_page_html, 2099)


def test_find_excel_download_url_not_found(approvals_extractor: ApprovalsExtractor, approvals_2025_page_html: BeautifulSoup):
    """
    Test that a ValueError is raised if the excel download link is not found.
    """
    # Remove the excel link from the html
    approvals_2025_page_html.find("a", href=lambda href: href and ".xlsx" in href).decompose()
    with pytest.raises(ValueError, match="Could not find the Excel file download link."):
        approvals_extractor._find_excel_download_url(approvals_2025_page_html)


def test_extract_main_page_error(approvals_extractor: ApprovalsExtractor, requests_mock):
    """
    Test that an exception is raised if the main approvals page returns an error.
    """
    requests_mock.get(approvals_extractor.approvals_list_url, status_code=500)
    with pytest.raises(requests.exceptions.HTTPError):
        approvals_extractor.extract(year=2025, last_state={})


def test_extract_yearly_page_error(approvals_extractor: ApprovalsExtractor, requests_mock, approvals_main_page_html: BeautifulSoup):
    """
    Test that an exception is raised if the yearly approvals page returns an error.
    """
    requests_mock.get(approvals_extractor.approvals_list_url, text=str(approvals_main_page_html))
    yearly_url = approvals_extractor._find_yearly_approval_url(approvals_main_page_html, 2025)
    requests_mock.get(yearly_url, status_code=500)
    with pytest.raises(requests.exceptions.HTTPError):
        approvals_extractor.extract(year=2025, last_state={})


def test_extract_excel_download_error(approvals_extractor: ApprovalsExtractor, requests_mock, approvals_main_page_html: BeautifulSoup, approvals_2025_page_html: BeautifulSoup):
    """
    Test that an exception is raised if the excel download returns an error.
    """
    requests_mock.get(approvals_extractor.approvals_list_url, text=str(approvals_main_page_html))
    yearly_url = approvals_extractor._find_yearly_approval_url(approvals_main_page_html, 2025)
    requests_mock.get(yearly_url, text=str(approvals_2025_page_html))
    excel_url = approvals_extractor._find_excel_download_url(approvals_2025_page_html)
    requests_mock.get(excel_url, status_code=500)
    with pytest.raises(requests.exceptions.HTTPError):
        approvals_extractor.extract(year=2025, last_state={})
