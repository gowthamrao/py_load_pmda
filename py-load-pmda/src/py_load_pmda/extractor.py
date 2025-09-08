import os
import requests
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
from typing import Tuple, List

class BaseExtractor:
    """
    A base class for extractors with robust request handling.

    Provides common functionality like rate limiting, retries with exponential
    backoff, and file caching.
    """
    def __init__(self, cache_dir: str = "./cache", retries: int = 3, backoff_factor: float = 0.5):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.base_url = "https://www.pmda.go.jp"

    def _send_request(self, url: str, stream: bool = False) -> requests.Response:
        """
        Sends an HTTP GET request with retries and exponential backoff.
        """
        for attempt in range(self.retries):
            try:
                # Simple rate limiting: wait before each request
                time.sleep(1)

                response = requests.get(url, stream=stream, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Request to {url} failed. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Request to {url} failed after {self.retries} attempts.")
                    raise e

    def _get_page_content(self, url: str) -> BeautifulSoup:
        """Fetches and parses the content of a given URL."""
        response = self._send_request(url)
        response.encoding = response.apparent_encoding
        return BeautifulSoup(response.text, "html.parser")

    def _download_file(self, url: str) -> Path:
        """Downloads a file and saves it to the cache, checking ETag."""
        local_filename = url.split('/')[-1]
        local_filepath = self.cache_dir / local_filename
        etag_path = self.cache_dir / f"{local_filename}.etag"

        headers = {}
        if local_filepath.exists() and etag_path.exists():
            headers["If-None-Match"] = etag_path.read_text()

        try:
            # The streaming request itself is handled by _send_request
            with self._send_request(url, stream=True) as r:
                if r.status_code == 304:
                    print(f"File '{local_filename}' is up to date (ETag match). Using cache.")
                    return local_filepath

                with open(local_filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"File '{local_filename}' downloaded successfully.")

                if "ETag" in r.headers:
                    etag_path.write_text(r.headers["ETag"])
            return local_filepath
        except requests.RequestException as e:
            print(f"Error downloading file from {url}: {e}")
            raise


class ApprovalsExtractor(BaseExtractor):
    """
    Extracts the New Drug Approvals list from the PMDA website.
    Inherits robust request handling from BaseExtractor.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.approvals_list_url = urljoin(self.base_url, "/review-services/drug-reviews/review-information/p-drugs/0010.html")
        self.excel_download_url = ""

    def _find_yearly_approval_url(self, soup: BeautifulSoup, year: int) -> str:
        """Finds the URL for a specific year's approval list."""
        year_text = f"{year}年度"
        link = soup.find("a", string=lambda text: text and year_text in text)
        if not link or not link.has_attr("href"):
            raise ValueError(f"Could not find link for year {year}")
        return urljoin(self.base_url, link["href"])

    def _find_excel_download_url(self, soup: BeautifulSoup) -> str:
        """Finds the download link for the Excel file on the page."""
        link = soup.find("a", href=lambda href: href and ".xlsx" in href)
        if not link or not link.has_attr("href"):
            raise ValueError("Could not find the Excel file download link.")
        self.excel_download_url = urljoin(self.base_url, link["href"])
        return self.excel_download_url

    def extract(self, year: int = 2025) -> Tuple[Path, str]:
        """
        Main extraction method for approvals.

        Args:
            year: The fiscal year to extract data for.

        Returns:
            A tuple containing the path to the downloaded Excel file and its source URL.
        """
        print("Step 1: Fetching the main approvals list page...")
        main_page_soup = self._get_page_content(self.approvals_list_url)

        print(f"Step 2: Finding the URL for fiscal year {year}...")
        yearly_url = self._find_yearly_approval_url(main_page_soup, year)

        print(f"Step 3: Fetching the page for fiscal year {year}...")
        yearly_page_soup = self._get_page_content(yearly_url)

        print("Step 4: Finding the Excel file download URL...")
        excel_url = self._find_excel_download_url(yearly_page_soup)

        print("Step 5: Downloading the Excel file...")
        file_path = self._download_file(excel_url)

        return file_path, excel_url


class JaderExtractor(BaseExtractor):
    """
    Finds the JADER (Japanese Adverse Drug Event Report) dataset files in the cache.

    NOTE: The PMDA website for JADER uses a CAPTCHA, which prevents automated
    downloads. This extractor does not download the files. Instead, it checks for
    their existence in the local cache directory and provides instructions for the
    user to download them manually if they are not found.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.jader_info_url = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0004.html"

    def extract(self) -> Tuple[List[Path], str]:
        """
        Checks for JADER .zip files in the cache and returns their paths.

        Raises:
            FileNotFoundError: If no JADER .zip files are found in the cache.

        Returns:
            A tuple containing a list of paths to the zip files and the source URL.
        """
        print("--- JADER Extractor ---")
        print("NOTE: JADER files must be downloaded manually due to CAPTCHA.")

        jader_zip_files = list(self.cache_dir.glob("*.zip"))

        if not jader_zip_files:
            error_message = f"""
            ========================================================================
            JADER FILES NOT FOUND!

            Automated download is not possible due to a CAPTCHA on the PMDA website.
            Please download the JADER dataset files manually.

            1. Go to: {self.jader_info_url}
            2. Follow the instructions to download the .zip files.
            3. Place all the downloaded .zip files into the cache directory:
               {self.cache_dir.resolve()}
            ========================================================================
            """
            raise FileNotFoundError(error_message)

        print(f"Found {len(jader_zip_files)} JADER .zip file(s) in cache.")
        return jader_zip_files, self.jader_info_url
