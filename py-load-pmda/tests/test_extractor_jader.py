import pytest
from pathlib import Path
from typing import Any
from py_load_pmda.extractor import JaderExtractor

# Helper function to get the path to the fixtures directory
def get_fixture_path() -> Path:
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def jader_extractor(tmp_path: Path) -> JaderExtractor:
    """Fixture to create a JaderExtractor instance with a temporary cache directory."""
    return JaderExtractor(cache_dir=str(tmp_path / "cache"))

def test_find_jader_zip_url(jader_extractor: JaderExtractor, requests_mock: Any):
    """Test that the correct JADER zip file download URL is found."""
    html_content = (get_fixture_path() / 'jader_info_page.html').read_text()
    requests_mock.get(jader_extractor.jader_info_url, text=html_content)
    soup = jader_extractor._get_page_content(jader_extractor.jader_info_url)
    url = jader_extractor._find_jader_zip_url(soup)
    assert url == "https://www.pmda.go.jp/files/000251593_jader_202303.zip"

def test_find_jader_zip_url_not_found(jader_extractor: JaderExtractor, requests_mock: Any):
    """Test that a ValueError is raised if the JADER zip link is not found."""
    html_content = "<html><body><p>No link here.</p></body></html>"
    requests_mock.get(jader_extractor.jader_info_url, text=html_content)
    soup = jader_extractor._get_page_content(jader_extractor.jader_info_url)
    with pytest.raises(ValueError, match="Could not find the JADER zip file download link on the page."):
        jader_extractor._find_jader_zip_url(soup)

def test_extract_jader(jader_extractor: JaderExtractor, requests_mock: Any):
    """Test the full extract process for the JaderExtractor."""
    info_page_html = (get_fixture_path() / 'jader_info_page.html').read_text()
    zip_content = b"dummy zip data"

    requests_mock.get(jader_extractor.jader_info_url, text=info_page_html)
    requests_mock.get("https://www.pmda.go.jp/files/000251593_jader_202303.zip", content=zip_content)

    file_path, url, state = jader_extractor.extract(last_state={})

    assert file_path.name == "000251593_jader_202303.zip"
    assert file_path.read_bytes() == zip_content
    assert url == "https://www.pmda.go.jp/files/000251593_jader_202303.zip"
    assert state == {}
