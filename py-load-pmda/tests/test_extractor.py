from pathlib import Path
from typing import Any
from unittest.mock import call

import pytest

from py_load_pmda.extractor import BaseExtractor


@pytest.fixture
def extractor(tmp_path: Path) -> BaseExtractor:
    """Fixture to create a BaseExtractor instance with a temporary cache directory."""
    cache_dir = tmp_path / "cache"
    return BaseExtractor(cache_dir=str(cache_dir))


def test_download_file_initial(extractor: BaseExtractor, requests_mock: Any) -> None:
    """Test downloading a file for the first time."""
    url = "http://test.com/file.txt"
    mock_content = b"Hello, world!"
    mock_headers = {"ETag": '"12345"', "Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"}
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert file_path.read_bytes() == mock_content
    assert extractor.new_state["etag"] == '"12345"'
    assert extractor.new_state["last_modified"] == "Tue, 15 Nov 1994 12:45:26 GMT"


def test_download_file_etag_match(extractor: BaseExtractor, requests_mock: Any) -> None:
    """Test that the file is not re-downloaded when ETag matches."""
    url = "http://test.com/file.txt"
    last_state = {"etag": '"12345"'}

    # Create a dummy file in cache
    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("cached content")

    requests_mock.get(url, status_code=304)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_text() == "cached content"  # Should not have changed
    assert requests_mock.last_request.headers["If-None-Match"] == '"12345"'
    assert extractor.new_state == last_state  # State should be preserved


def test_download_file_last_modified_match(extractor: BaseExtractor, requests_mock: Any) -> None:
    """Test that the file is not re-downloaded when Last-Modified matches."""
    url = "http://test.com/file.txt"
    last_state = {"last_modified": "Tue, 15 Nov 1994 12:45:26 GMT"}

    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("cached content")

    requests_mock.get(url, status_code=304)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_text() == "cached content"
    assert (
        requests_mock.last_request.headers["If-Modified-Since"] == "Tue, 15 Nov 1994 12:45:26 GMT"
    )
    assert extractor.new_state == last_state


def test_download_file_mismatch(extractor: BaseExtractor, requests_mock: Any) -> None:
    """Test that the file is re-downloaded when headers do not match."""
    url = "http://test.com/file.txt"
    last_state = {"etag": '"old-etag"', "last_modified": "Mon, 14 Nov 1994 12:45:26 GMT"}

    local_filepath = extractor.cache_dir / "file.txt"
    local_filepath.write_text("old content")

    new_content = b"new content"
    new_headers = {"ETag": '"new-etag"', "Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"}
    requests_mock.get(url, content=new_content, headers=new_headers)

    file_path = extractor._download_file(url, last_state=last_state)

    assert file_path.exists()
    assert file_path.read_bytes() == new_content
    assert extractor.new_state["etag"] == '"new-etag"'
    assert extractor.new_state["last_modified"] == "Tue, 15 Nov 1994 12:45:26 GMT"
    assert requests_mock.last_request.headers["If-None-Match"] == '"old-etag"'
    assert (
        requests_mock.last_request.headers["If-Modified-Since"] == "Mon, 14 Nov 1994 12:45:26 GMT"
    )


def test_download_file_only_etag_provided(extractor: BaseExtractor, requests_mock: Any) -> None:
    """Test behavior when only ETag is provided by the server."""
    url = "http://test.com/file.txt"
    mock_content = b"content"
    mock_headers = {"ETag": '"etag-only"'}
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert extractor.new_state == {"etag": '"etag-only"'}


def test_download_file_only_last_modified_provided(
    extractor: BaseExtractor, requests_mock: Any
) -> None:
    """Test behavior when only Last-Modified is provided by the server."""
    url = "http://test.com/file.txt"
    mock_content = b"content"
    mock_headers = {"Last-Modified": "Tue, 15 Nov 1994 12:45:26 GMT"}
    requests_mock.get(url, content=mock_content, headers=mock_headers)

    file_path = extractor._download_file(url)

    assert file_path.exists()
    assert extractor.new_state == {"last_modified": "Tue, 15 Nov 1994 12:45:26 GMT"}


def test_extractor_initialization_with_custom_settings(tmp_path: Path) -> None:
    """Test that the BaseExtractor can be initialized with custom settings."""
    custom_cache_dir = tmp_path / "custom_cache"
    extractor = BaseExtractor(
        cache_dir=str(custom_cache_dir),
        retries=5,
        backoff_factor=1.0,
        rate_limit_seconds=2.0,
    )
    assert extractor.cache_dir == custom_cache_dir
    assert extractor.retries == 5
    assert extractor.backoff_factor == 1.0
    assert extractor.rate_limit_seconds == 2.0


def test_request_with_retries_and_rate_limiting(
    extractor: BaseExtractor, requests_mock: Any, mocker: Any
) -> None:
    """
    Test the _request_with_retries method for correct rate limiting and retry logic.
    """
    url = "http://test.com/retry_endpoint"
    mock_sleep = mocker.patch("time.sleep")

    # Simulate a server that fails twice then succeeds
    requests_mock.get(
        url,
        [
            {"status_code": 500, "text": "Internal Server Error"},
            {"status_code": 503, "text": "Service Unavailable"},
            {"status_code": 200, "text": "Success!"},
        ],
    )

    # Custom settings for this test
    extractor.retries = 3
    extractor.rate_limit_seconds = 0.1
    extractor.backoff_factor = 0.2  # backoff will be 0.2 * (2**0) and 0.2 * (2**1)

    response = extractor._request_with_retries("get", url)

    assert response.status_code == 200
    assert response.text == "Success!"
    assert requests_mock.call_count == 3

    # Check the calls to time.sleep
    # Expected calls:
    # 1. Rate limit sleep before 1st attempt
    # 2. Backoff sleep after 1st failure
    # 3. Rate limit sleep before 2nd attempt
    # 4. Backoff sleep after 2nd failure
    # 5. Rate limit sleep before 3rd attempt
    sleep_calls = mock_sleep.call_args_list
    assert len(sleep_calls) == 5

    # Rate limit before 1st call
    assert sleep_calls[0] == call(0.1)
    # Backoff after 1st failure (0.2 * 2**0 = 0.2, plus random component)
    assert sleep_calls[1].args[0] >= 0.2
    # Rate limit before 2nd call
    assert sleep_calls[2] == call(0.1)
    # Backoff after 2nd failure (0.2 * 2**1 = 0.4, plus random component)
    assert sleep_calls[3].args[0] >= 0.4
    # Rate limit before 3rd (successful) call
    assert sleep_calls[4] == call(0.1)
