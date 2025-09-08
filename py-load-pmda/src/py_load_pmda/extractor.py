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
    """
    def __init__(self, cache_dir: str = "./cache", retries: int = 3, backoff_factor: float = 0.5):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.base_url = "https://www.pmda.go.jp"
        self.new_state = {}

    def _send_request(self, url: str, stream: bool = False, headers: dict = None) -> requests.Response:
        """
        Sends an HTTP GET request with retries and exponential backoff.
        """
        for attempt in range(self.retries):
            try:
                time.sleep(1)
                response = requests.get(url, stream=stream, timeout=30, headers=headers)
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

    def _download_file(self, url: str, last_state: dict = None) -> Path:
        """Downloads a file, saves it to cache, and uses ETag for delta-checking."""
        local_filename = url.split('/')[-1]
        local_filepath = self.cache_dir / local_filename

        headers = {}
        if last_state and "etag" in last_state:
            headers["If-None-Match"] = last_state["etag"]

        try:
            with self._send_request(url, stream=True, headers=headers) as r:
                if r.status_code == 304:
                    print(f"File '{local_filename}' is up to date (ETag match). Using cache.")
                    self.new_state = last_state # Preserve the old state
                    return local_filepath

                with open(local_filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"File '{local_filename}' downloaded successfully.")

                if "ETag" in r.headers:
                    self.new_state["etag"] = r.headers["ETag"]
            return local_filepath
        except requests.RequestException as e:
            print(f"Error downloading file from {url}: {e}")
            raise


class ApprovalsExtractor(BaseExtractor):
    """
    Extracts the New Drug Approvals list from the PMDA website.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.approvals_list_url = urljoin(self.base_url, "/review-services/drug-reviews/review-information/p-drugs/0010.html")

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
        return urljoin(self.base_url, link["href"])

    def extract(self, year: int, last_state: dict) -> Tuple[Path, str]:
        """
        Main extraction method for approvals.
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
        file_path = self._download_file(excel_url, last_state=last_state)

        return file_path, excel_url, self.new_state


class JaderExtractor(BaseExtractor):
    """
    Finds the JADER dataset files in the cache.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.jader_info_url = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0004.html"

    def extract(self, last_state: dict) -> Tuple[List[Path], str]:
        """
        Checks for JADER .zip files in the cache and returns their paths.
        """
        print("--- JADER Extractor ---")
        print("NOTE: JADER files must be downloaded manually due to CAPTCHA.")

        jader_zip_files = list(self.cache_dir.glob("*.zip"))

        if not jader_zip_files:
            error_message = f"""
            ========================================================================
            JADER FILES NOT FOUND!
            Please download the JADER dataset files manually.
            1. Go to: {self.jader_info_url}
            2. Download the .zip files.
            3. Place all .zip files into the cache directory:
               {self.cache_dir.resolve()}
            ========================================================================
            """
            raise FileNotFoundError(error_message)

        # For JADER, we can use file hashes as a delta-check mechanism
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in jader_zip_files}
        self.new_state = {"file_hashes": hashes}

        if last_state and last_state.get("file_hashes") == hashes:
            print("JADER files have not changed since last run. Skipping.")
            # This is where we could implement a mechanism to stop the pipeline
            # For now, we'll just log it.

        print(f"Found {len(jader_zip_files)} JADER .zip file(s) in cache.")
        return jader_zip_files, self.jader_info_url, self.new_state
