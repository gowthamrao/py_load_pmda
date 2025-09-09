import hashlib
import logging
import os
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class BaseExtractor:
    """
    Base class for all extractors.

    Provides common functionality for downloading files, managing a cache,
    and handling HTTP requests.
    """

    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "py-load-pmda/0.1.0 (https://github.com/ohdsi/py-load-pmda; mailto:rao@ohdsi.org)"
            }
        )

    def _get(self, url: str) -> requests.Response:
        """
        Perform a GET request with appropriate error handling and logging.
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching URL {url}: {e}")
            raise

    def _download_file(self, url: str) -> Path:
        """
        Download a file and save it to the cache.

        Uses a hash of the URL as the filename to avoid collisions and
        re-downloads.
        """
        response = self._get(url)
        content = response.content
        # Use a hash of the URL for a unique, stable cache filename
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        file_extension = Path(url).suffix
        cached_path = self.cache_dir / f"{url_hash}{file_extension}"

        with open(cached_path, "wb") as f:
            f.write(content)
        logging.info(f"Downloaded and cached file from {url} to {cached_path}")
        return cached_path


class ApprovalsExtractor(BaseExtractor):
    """
    Extractor for New Drug Approvals.

    This extractor scrapes the PMDA website to find and download the
    Excel files containing the lists of newly approved drugs for a given year.
    """

    BASE_URL = "https://www.pmda.go.jp"
    YEAR_LIST_URL = "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html"

    def extract(self, year: int) -> List[Path]:
        """
        Find and download all approval Excel files for a specific year.

        Args:
            year: The fiscal year to extract data for.

        Returns:
            A list of local file paths to the downloaded Excel files.
        """
        logging.info(f"Starting extraction for approvals in year {year}...")
        year_page_url = self._find_year_page_url(year)
        if not year_page_url:
            logging.warning(f"No page found for year {year}. Skipping.")
            return []

        excel_urls = self._scrape_excel_links(year_page_url)
        if not excel_urls:
            logging.warning(f"No Excel files found on page {year_page_url}. Skipping.")
            return []

        downloaded_files = []
        for url in excel_urls:
            try:
                file_path = self._download_file(url)
                downloaded_files.append(file_path)
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download {url}: {e}")
                # Continue to the next file
                continue

        logging.info(f"Finished extraction for year {year}. Found {len(downloaded_files)} files.")
        return downloaded_files

    def _find_year_page_url(self, year: int) -> str | None:
        """
        Find the URL for the given year's approval list page.
        """
        logging.info(f"Scraping main approvals page to find URL for year {year}...")
        response = self._get(self.YEAR_LIST_URL)
        soup = BeautifulSoup(response.content, "html.parser")

        # The links are in a list, e.g., "<li><a href="...">2025年度</a></li>"
        year_link = soup.find("a", string=f"{year}年度")
        if year_link and year_link.has_attr("href"):
            relative_url = year_link["href"]
            absolute_url = urljoin(self.BASE_URL, relative_url)
            logging.info(f"Found page for year {year} at {absolute_url}")
            return absolute_url
        else:
            logging.warning(f"Could not find a link for year {year} on the main page.")
            return None

    def _scrape_excel_links(self, page_url: str) -> List[str]:
        """
        Scrape a year-specific page to find all .xlsx file download links.
        """
        logging.info(f"Scraping page {page_url} for Excel file links...")
        response = self._get(page_url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Links to Excel files typically end with .xlsx
        excel_links = soup.find_all("a", href=lambda href: href and href.endswith(".xlsx"))

        if not excel_links:
            return []

        absolute_urls = [urljoin(self.BASE_URL, link["href"]) for link in excel_links]
        logging.info(f"Found {len(absolute_urls)} Excel file links.")
        return absolute_urls
