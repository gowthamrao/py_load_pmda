import os
import requests
import time
import random
import hashlib
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

    def _send_post_request(self, url: str, data: dict, headers: dict = None, stream: bool = False) -> requests.Response:
        """
        Sends an HTTP POST request with retries and exponential backoff.
        """
        for attempt in range(self.retries):
            try:
                time.sleep(1)
                response = requests.post(url, data=data, headers=headers, stream=stream, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                    print(f"POST request to {url} failed. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"POST request to {url} failed after {self.retries} attempts.")
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
    Extracts the JADER (Japanese Adverse Drug Event Report) dataset from the PMDA website.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # This is the landing page where the link to the JADER zip file is found.
        self.jader_info_url = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0005.html"

    def _find_jader_zip_url(self, soup: BeautifulSoup) -> str:
        """
        Finds the download link for the JADER zip file on the page.
        The link is identified by containing 'jader' and ending in '.zip'.
        """
        # A more robust selector might be needed if the site structure changes.
        link = soup.find("a", href=lambda href: href and "jader" in href.lower() and href.endswith(".zip"))
        if not link or not link.has_attr("href"):
            raise ValueError("Could not find the JADER zip file download link on the page.")

        # The URL in the href attribute is relative, so we join it with the base URL.
        return urljoin(self.base_url, link["href"])

    def extract(self, last_state: dict) -> Tuple[Path, str, dict]:
        """
        Main extraction method for the JADER dataset.
        It automates the download of the JADER zip file and uses ETags for delta detection.
        """
        print("--- JADER Extractor ---")
        print(f"Step 1: Fetching the JADER info page: {self.jader_info_url}")
        info_page_soup = self._get_page_content(self.jader_info_url)

        print("Step 2: Finding the JADER zip file download URL...")
        zip_url = self._find_jader_zip_url(info_page_soup)
        print(f"Found download URL: {zip_url}")

        print("Step 3: Downloading the JADER zip file...")
        # The _download_file method handles caching and ETag checking.
        # It will return the path to the cached file and set self.new_state.
        file_path = self._download_file(zip_url, last_state=last_state)

        # The CLI expects a 3-tuple return, so we match that signature.
        return file_path, zip_url, self.new_state


class PackageInsertsExtractor(BaseExtractor):
    """
    Extracts Package Inserts from the PMDA search portal.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # The POST request goes to a URL without a trailing slash.
        self.search_url = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"

    def extract(self, drug_names: List[str], last_state: dict) -> Tuple[List[Tuple[Path, str]], dict]:
        """
        Main extraction method for package inserts.
        It searches for each drug name and downloads the corresponding package insert PDF.

        Returns:
            A tuple containing:
            - A list of tuples, where each inner tuple is (file_path, source_url).
            - A dictionary containing the new state for delta checking.
        """
        print("--- Package Inserts Extractor ---")
        downloaded_data = []
        all_new_states = {}

        for name in drug_names:
            print(f"Searching for package insert for drug: '{name}'")

            # This payload is based on reverse-engineering the search form.
            form_data = {
                "nameWord": name,
                "dispColumnsList[0]": "1", # '1' is the value for '添付文書' (Package Insert)
                "_dispColumnsList[0]": "on",
                "nccharset": "EBBEE281", # This seems to be a required token
                "tglOpFlg": "",
                "isNewReleaseDisp": "true",
                "listCategory": ""
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"
            }

            try:
                # Step 1: POST to the search form to get the results page
                print(f"Submitting search form for '{name}'...")
                response = self._send_post_request(self.search_url, data=form_data, headers=headers)
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")

                # Step 2: Find the first PDF download link on the results page.
                # We assume the first PDF link in the main content area is the correct one.
                # This is a pragmatic approach as the page structure is complex.
                main_content = soup.find("div", id="ContentMainArea")
                if not main_content:
                    print(f"Could not find main content area on results page for '{name}'. Skipping.")
                    continue

                link = main_content.find("a", href=lambda href: href and ".pdf" in href)

                if not link or not link.has_attr("href"):
                    print(f"Could not find a PDF download link for '{name}'. Skipping.")
                    continue

                # The links are relative, so we need to join them with the base URL.
                download_url = urljoin("https://www.pmda.go.jp", link["href"])
                print(f"Found download link: {download_url}")

                # Step 3: Download the file using the robust method from BaseExtractor
                # ETag checking will prevent re-downloads if the file is unchanged.
                file_path = self._download_file(download_url, last_state=last_state.get(download_url, {}))
                if file_path and file_path.exists():
                    downloaded_data.append((file_path, download_url))
                    all_new_states[download_url] = self.new_state

            except requests.RequestException as e:
                print(f"Failed to process '{name}': {e}")
                continue

        print(f"Downloaded {len(downloaded_data)} package insert(s).")
        return downloaded_data, all_new_states


class ReviewReportsExtractor(BaseExtractor):
    """
    Extracts Review Reports from the PMDA search portal.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # The POST request goes to a URL without a trailing slash.
        self.search_url = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"

    def extract(self, drug_names: List[str], last_state: dict) -> Tuple[List[Tuple[Path, str]], dict]:
        """
        Main extraction method for review reports.
        It searches for each drug name and downloads the corresponding review report PDF.

        Returns:
            A tuple containing:
            - A list of tuples, where each inner tuple is (file_path, source_url).
            - A dictionary containing the new state for delta checking.
        """
        print("--- Review Reports Extractor ---")
        downloaded_data = []
        all_new_states = {}

        for name in drug_names:
            print(f"Searching for review report for drug: '{name}'")

            # This payload is based on reverse-engineering the search form.
            # "7" is the value for "審査報告書／再審査報告書／最適使用推進ガイドライン等"
            form_data = {
                "nameWord": name,
                "dispColumnsList[0]": "7",
                "_dispColumnsList[0]": "on",
                "nccharset": "EBBEE281",
                "tglOpFlg": "",
                "isNewReleaseDisp": "true",
                "listCategory": ""
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"
            }

            try:
                # Step 1: POST to the search form to get the results page
                print(f"Submitting search form for '{name}'...")
                response = self._send_post_request(self.search_url, data=form_data, headers=headers)
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")

                # Step 2: Find the first PDF download link on the results page.
                main_content = soup.find("div", id="ContentMainArea")
                if not main_content:
                    print(f"Could not find main content area on results page for '{name}'. Skipping.")
                    continue

                link = main_content.find("a", href=lambda href: href and ".pdf" in href)

                if not link or not link.has_attr("href"):
                    print(f"Could not find a PDF download link for '{name}'. Skipping.")
                    continue

                download_url = urljoin("https://www.pmda.go.jp", link["href"])
                print(f"Found download link: {download_url}")

                # Step 3: Download the file
                file_path = self._download_file(download_url, last_state=last_state.get(download_url, {}))
                if file_path and file_path.exists():
                    downloaded_data.append((file_path, download_url))
                    all_new_states[download_url] = self.new_state

            except requests.RequestException as e:
                print(f"Failed to process '{name}': {e}")
                continue

        print(f"Downloaded {len(downloaded_data)} review report(s).")
        return downloaded_data, all_new_states
