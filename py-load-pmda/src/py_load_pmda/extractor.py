import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
from typing import Tuple

class ApprovalsExtractor:
    """
    Extracts the New Drug Approvals list from the PMDA website.
    """
    def __init__(self, cache_dir: str = "./cache"):
        self.base_url = "https://www.pmda.go.jp"
        self.approvals_list_url = urljoin(self.base_url, "/review-services/drug-reviews/review-information/p-drugs/0010.html")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.excel_download_url = ""

    def _get_page_content(self, url: str) -> BeautifulSoup:
        """Fetches and parses the content of a given URL."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            print(f"Error fetching page {url}: {e}")
            raise

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

    def _download_file(self, url: str) -> Path:
        """Downloads a file and saves it to the cache, checking ETag."""
        local_filename = url.split('/')[-1]
        local_filepath = self.cache_dir / local_filename
        etag_path = self.cache_dir / f"{local_filename}.etag"

        headers = {}
        if local_filepath.exists() and etag_path.exists():
            headers["If-None-Match"] = etag_path.read_text()

        try:
            with requests.get(url, stream=True, headers=headers, timeout=30) as r:
                if r.status_code == 304:
                    print(f"File '{local_filename}' is up to date (ETag match). Using cache.")
                    return local_filepath

                r.raise_for_status()

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

    def extract(self, year: int = 2025) -> Tuple[Path, str]:
        """
        Main extraction method.

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
