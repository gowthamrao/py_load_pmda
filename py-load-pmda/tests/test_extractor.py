import pytest
import requests_mock
from pathlib import Path
from py_load_pmda.extractor import BaseExtractor

@pytest.fixture
def extractor(tmp_path):
    """Fixture to create a BaseExtractor instance with a temporary cache directory."""
    cache_dir = tmp_path / "cache"
    return BaseExtractor(cache_dir=str(cache_dir))

def test_download_file_initial(extractor, requests_mock):
    """Test downloading a file for the first time."""
    url = "http://test.com/file.txt"
    mock_content = b"Hello, world!"
    mock_headers = {
        "ETag": '"12345"',
        "Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"
    }
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert file_path.read_bytes() == mock_content
    assert extractor.new_state["etag"] == '"12345"'
    assert extractor.new_state["last_modified"] == "Tue, 15 Nov 1994 12:45:26 GMT"

def test_download_file_etag_match(extractor, requests_mock):
    """Test that the file is not re-downloaded when ETag matches."""
    url = "http://test.com/file.txt"
    last_state = {"etag": '"12345"'}

    # Create a dummy file in cache
    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("cached content")

    requests_mock.get(url, status_code=304)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_text() == "cached content" # Should not have changed
    assert requests_mock.last_request.headers["If-None-Match"] == '"12345"'
    assert extractor.new_state == last_state # State should be preserved

def test_download_file_last_modified_match(extractor, requests_mock):
    """Test that the file is not re-downloaded when Last-Modified matches."""
    url = "http://test.com/file.txt"
    last_state = {"last_modified": "Tue, 15 Nov 1994 12:45:26 GMT"}

    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("cached content")

    requests_mock.get(url, status_code=304)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_text() == "cached content"
    assert requests_mock.last_request.headers["If-Modified-Since"] == "Tue, 15 Nov 1994 12:45:26 GMT"
    assert extractor.new_state == last_state

def test_download_file_mismatch(extractor, requests_mock):
    """Test that the file is re-downloaded when headers do not match."""
    url = "http://test.com/file.txt"
    last_state = {"etag": '"old-etag"', "last_modified": "Mon, 14 Nov 1994 12:45:26 GMT"}

    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("old content")

    new_content = b"new content"
    new_headers = {
        "ETag": '"new-etag"',
        "Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"
    }
    requests_mock.get(url, content=new_content, headers=new_headers)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_bytes() == new_content
    assert extractor.new_state["etag"] == '"new-etag"'
    assert extractor.new_state["last_modified"] == "Tue, 15 Nov 1994 12:45:26 GMT"
    assert requests_mock.last_request.headers["If-None-Match"] == '"old-etag"'
    assert requests_mock.last_request.headers["If-Modified-Since"] == "Mon, 14 Nov 1994 12:45:26 GMT"

def test_download_file_only_etag_provided(extractor, requests_mock):
    """Test behavior when only ETag is provided by the server."""
    url = "http://test.com/file.txt"
    mock_content = b"content"
    mock_headers = {"ETag": '"etag-only"'}
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert extractor.new_state == {"etag": '"etag-only"'}

def test_download_file_only_last_modified_provided(extractor, requests_mock):
    """Test behavior when only Last-Modified is provided by the server."""
    url = "http://test.com/file.txt"
    mock_content = b"content"
    mock_headers = {"Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"}
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert extractor.new_state == {"last_modified": "Tue, 15 Nov 1994 12:45:26 GMT"}
