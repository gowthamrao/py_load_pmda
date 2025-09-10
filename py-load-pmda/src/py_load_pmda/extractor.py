import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


class BaseExtractor:
    """
    A base class for extractors with robust request handling using a session.
    """
    def __init__(self, cache_dir: str = "./cache", retries: int = 3, backoff_factor: float = 0.5) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.base_url: str = "https://www.pmda.go.jp"
        self.new_state: Dict[str, Any] = {}
        self.session = requests.Session()
        # Set a default User-Agent for the session
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

    def _send_request(self, url: str, stream: bool = False, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """
        Sends an HTTP GET request with retries and exponential backoff using a session.
        """
        for attempt in range(self.retries):
            try:
                time.sleep(1)
                response = self.session.get(url, stream=stream, timeout=30, headers=headers)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"Request to {url} failed. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Request to {url} failed after {self.retries} attempts.")
                    raise e
        raise requests.RequestException(f"Request to {url} failed after {self.retries} attempts.")

    def _send_post_request(self, url: str, data: Dict[str, Any], headers: Optional[Dict[str, str]] = None, stream: bool = False) -> requests.Response:
        """
        Sends an HTTP POST request with retries and exponential backoff using a session.
        """
        for attempt in range(self.retries):
            try:
                time.sleep(1)
                response = self.session.post(url, data=data, headers=headers, stream=stream, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"POST request to {url} failed. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"POST request to {url} failed after {self.retries} attempts.")
                    raise e
        raise requests.RequestException(f"POST request to {url} failed after {self.retries} attempts.")

    def _get_page_content(self, url: str) -> BeautifulSoup:
        """Fetches and parses the content of a given URL."""
        response = self._send_request(url)
        response.encoding = response.apparent_encoding
        return BeautifulSoup(response.text, "html.parser")

    def _download_file(self, url: str, last_state: Optional[Dict[str, Any]] = None) -> Path:
        """
        Downloads a file, saves it to cache, and uses ETag and Last-Modified
        headers for delta-checking.
        """
        self.new_state = {}  # Reset state for this specific download
        local_filename = url.split("/")[-1]
        local_filepath = self.cache_dir / local_filename

        headers = {}
        if last_state:
            if "etag" in last_state:
                headers["If-None-Match"] = last_state["etag"]
            if "last_modified" in last_state:
                headers["If-Modified-Since"] = last_state["last_modified"]

        try:
            with self._send_request(url, stream=True, headers=headers) as r:
                if r.status_code == 304:
                    logging.info(
                        f"File '{local_filename}' is up to date (server returned 304 Not Modified). Using cache."
                    )
                    if last_state:
                        self.new_state = last_state  # Preserve the old state
                    return local_filepath

                # If we get here, it's a 200 OK, so we download the file
                with open(local_filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                logging.info(f"File '{local_filename}' downloaded successfully.")

                # Update the new state with the latest headers from the response
                if "ETag" in r.headers:
                    self.new_state["etag"] = r.headers["ETag"]
                if "Last-Modified" in r.headers:
                    self.new_state["last_modified"] = r.headers["Last-Modified"]

            return local_filepath
        except requests.RequestException as e:
            logging.error(f"Error downloading file from {url}: {e}", exc_info=True)
            raise


class ApprovalsExtractor(BaseExtractor):
    """
    Extracts the New Drug Approvals list from the PMDA website.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.approvals_list_url: str = urljoin(self.base_url, "/review-services/drug-reviews/review-information/p-drugs/0010.html")

    def _find_yearly_approval_url(self, soup: BeautifulSoup, year: int) -> str:
        """Finds the URL for a specific year's approval list."""
        year_text = f"{year}年度"
        link = soup.find("a", string=lambda text: text and year_text in text)
        if not isinstance(link, Tag) or not link.has_attr("href"):
            raise ValueError(f"Could not find link for year {year}")
        return urljoin(self.base_url, str(link["href"]))

    def _find_excel_download_url(self, soup: BeautifulSoup) -> str:
        """Finds the download link for the Excel file on the page."""
        link = soup.find("a", href=lambda href: href and ".xlsx" in href)
        if not isinstance(link, Tag) or not link.has_attr("href"):
            raise ValueError("Could not find the Excel file download link.")
        return urljoin(self.base_url, str(link["href"]))

    def extract(self, year: int, last_state: Dict[str, Any]) -> Tuple[Path, str, Dict[str, Any]]:
        """
        Main extraction method for approvals.
        """
        logging.info("Step 1: Fetching the main approvals list page...")
        main_page_soup = self._get_page_content(self.approvals_list_url)

        logging.info(f"Step 2: Finding the URL for fiscal year {year}...")
        yearly_url = self._find_yearly_approval_url(main_page_soup, year)

        logging.info(f"Step 3: Fetching the page for fiscal year {year}...")
        yearly_page_soup = self._get_page_content(yearly_url)

        logging.info("Step 4: Finding the Excel file download URL...")
        excel_url = self._find_excel_download_url(yearly_page_soup)

        logging.info("Step 5: Downloading the Excel file...")
        file_path = self._download_file(excel_url, last_state=last_state)

        return file_path, excel_url, self.new_state


class JaderExtractor(BaseExtractor):
    """
    Extracts the JADER (Japanese Adverse Drug Event Report) dataset from the PMDA website.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # This is the landing page where the link to the JADER zip file is found.
        self.jader_info_url: str = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0005.html"

    def _find_jader_zip_url(self, soup: BeautifulSoup) -> str:
        """
        Finds the download link for the JADER zip file on the page.
        The link is identified by containing 'jader' and ending in '.zip'.
        """
        # A more robust selector might be needed if the site structure changes.
        link = soup.find("a", href=lambda href: href and "jader" in href.lower() and href.endswith(".zip"))
        if not isinstance(link, Tag) or not link.has_attr("href"):
            raise ValueError("Could not find the JADER zip file download link on the page.")

        # The URL in the href attribute is relative, so we join it with the base URL.
        return urljoin(self.base_url, str(link["href"]))

    def extract(self, last_state: Dict[str, Any]) -> Tuple[Path, str, Dict[str, Any]]:
        """
        Main extraction method for the JADER dataset.
        It automates the download of the JADER zip file and uses ETags for delta detection.
        """
        logging.info("--- JADER Extractor ---")
        logging.info(f"Step 1: Fetching the JADER info page: {self.jader_info_url}")
        info_page_soup = self._get_page_content(self.jader_info_url)

        logging.info("Step 2: Finding the JADER zip file download URL...")
        zip_url = self._find_jader_zip_url(info_page_soup)
        logging.info(f"Found download URL: {zip_url}")

        logging.info("Step 3: Downloading the JADER zip file...")
        # The _download_file method handles caching and ETag checking.
        # It will return the path to the cached file and set self.new_state.
        file_path = self._download_file(zip_url, last_state=last_state)

        # The CLI expects a 3-tuple return, so we match that signature.
        return file_path, zip_url, self.new_state


class PackageInsertsExtractor(BaseExtractor):
    """
    Extracts Package Inserts from the PMDA search portal.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # The POST request goes to a URL without a trailing slash.
        self.search_url: str = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"

    def extract(self, drug_names: List[str], last_state: Dict[str, Any]) -> Tuple[List[Tuple[Path, str]], Dict[str, Any]]:
        """
        Main extraction method for package inserts.
        It searches for each drug name and downloads the corresponding package insert PDF.

        Returns:
            A tuple containing:
            - A list of tuples, where each inner tuple is (file_path, source_url).
            - A dictionary containing the new state for delta checking.
        """
        logging.info("--- Package Inserts Extractor ---")
        downloaded_data = []
        all_new_states = {}

        for name in drug_names:
            logging.info(f"Searching for package insert for drug: '{name}'")

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
                # Step 1: GET the search page to acquire a valid session token (nccharset)
                logging.info("Fetching search page to get a session token...")
                get_response = self._send_request(self.search_url)
                get_soup = BeautifulSoup(get_response.text, "html.parser")
                token_tag = get_soup.find("input", {"name": "nccharset"})
                if not isinstance(token_tag, Tag) or not token_tag.has_attr("value"):
                    raise ValueError("Could not find the 'nccharset' token on the search page.")

                nccharset_token = str(token_tag["value"])
                logging.info(f"Acquired nccharset token: {nccharset_token}")
                form_data["nccharset"] = nccharset_token

                # Step 2: POST to the search form with the valid token
                logging.info(f"Submitting search form for '{name}'...")
                post_response = self._send_post_request(self.search_url, data=form_data, headers=headers)
                post_response.encoding = post_response.apparent_encoding
                soup = BeautifulSoup(post_response.text, "html.parser")

                # Step 2: Intelligently parse the search results table to find the correct PDF.
                main_content = soup.find("div", id="ContentMainArea")
                if not isinstance(main_content, Tag):
                    logging.warning(f"Could not find main content area for '{name}'. Skipping.")
                    continue

                # The results table now has a specific class name.
                table = main_content.find("table", class_="result_list_table")
                if not isinstance(table, Tag):
                    logging.warning(f"Could not find results table for '{name}'. Skipping.")
                    continue

                download_url = None
                tbody = table.find("tbody")
                if not isinstance(tbody, Tag):
                    tbody = table  # Fallback to the table itself

                rows = tbody.find_all("tr")
                for row in rows:  # Iterate all rows in the body
                    cells = row.find_all("td")
                    # Expecting at least 5 columns: Brand, Generic, Applicant, Detail, PDF
                    if len(cells) < 5:
                        continue

                    # The brand name is in the first cell, based on the test case HTML.
                    brand_name = cells[0].get_text(strip=True)

                    if name == brand_name:
                        logging.info(f"Found exact match for '{name}' in results table.")
                        pdf_link_tag = cells[4].find("a", href=lambda href: href and ".pdf" in href)
                        if isinstance(pdf_link_tag, Tag) and pdf_link_tag.has_attr("href"):
                            # The URL can be relative or absolute. urljoin handles both.
                            download_url = urljoin(self.base_url, str(pdf_link_tag["href"]))
                            logging.info(f"Found download link: {download_url}")
                            break  # Stop after finding the first exact match

                if not download_url:
                    logging.warning(f"Could not find a matching PDF download link for '{name}'. Skipping.")
                    continue

                # Step 3: Download the file
                file_path = self._download_file(download_url, last_state=last_state.get(download_url, {}))
                if file_path and file_path.exists():
                    downloaded_data.append((file_path, download_url))
                    all_new_states[download_url] = self.new_state

            except requests.RequestException as e:
                logging.error(f"Failed to process '{name}': {e}", exc_info=True)
                continue

        logging.info(f"Downloaded {len(downloaded_data)} package insert(s).")
        return downloaded_data, all_new_states


class ReviewReportsExtractor(BaseExtractor):
    """
    Extracts Review Reports from the PMDA search portal.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # The POST request goes to a URL without a trailing slash.
        self.search_url: str = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch"

    def extract(self, drug_names: List[str], last_state: Dict[str, Any]) -> Tuple[List[Tuple[Path, str]], Dict[str, Any]]:
        """
        Main extraction method for review reports.
        It searches for each drug name, parses the results, finds links
        containing '審査報告書', and downloads the corresponding files.
        """
        logging.info("--- Review Reports Extractor ---")
        downloaded_data = []
        all_new_states = {}

        for name in drug_names:
            logging.info(f"Searching for review report for drug: '{name}'")

            # "7" is the value for "審査報告書／再審査報告書／最適使用推進ガイドライン等"
            form_data = {
                "nameWord": name,
                "dispColumnsList[0]": "7",
                "_dispColumnsList[0]": "on",
                "nccharset": "",  # Will be updated with a real token
                "tglOpFlg": "",
                "isNewReleaseDisp": "true",
                "listCategory": ""
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": f"{self.search_url}/"
            }

            try:
                logging.info("Fetching search page to get a session token...")
                get_response = self._send_request(self.search_url)
                get_soup = BeautifulSoup(get_response.text, "html.parser")
                token_tag = get_soup.find("input", {"name": "nccharset"})
                if not isinstance(token_tag, Tag) or not token_tag.has_attr("value"):
                    raise ValueError("Could not find the 'nccharset' token on the search page.")

                form_data["nccharset"] = str(token_tag["value"])
                logging.info(f"Acquired nccharset token: {form_data['nccharset']}")

                logging.info(f"Submitting search form for '{name}'...")
                post_response = self._send_post_request(self.search_url, data=form_data, headers=headers)
                post_response.encoding = post_response.apparent_encoding
                soup = BeautifulSoup(post_response.text, "html.parser")

                main_content = soup.find("div", id="ContentMainArea")
                if not isinstance(main_content, Tag):
                    logging.warning(f"Could not find main content area for '{name}'. Skipping.")
                    continue

                table = main_content.find("table", class_="result_list_table")
                if not isinstance(table, Tag):
                    logging.warning(f"Could not find results table for '{name}'. Skipping.")
                    continue

                tbody = table.find("tbody")
                if not isinstance(tbody, Tag):
                    tbody = table  # Fallback

                rows = tbody.find_all("tr")
                found_links = []
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 5:
                        continue

                    # Looser matching for the brand name
                    brand_name = cells[0].get_text(strip=True)
                    if name in brand_name:
                        logging.info(f"Found potential match for '{name}' in row with brand name '{brand_name}'.")

                        # Find all links in the 5th cell
                        link_cell = cells[4]
                        report_links = link_cell.find_all("a", href=True)

                        for link_tag in report_links:
                            # Check if the link text indicates it's a review report
                            if "審査報告書" in link_tag.get_text(strip=True):
                                download_url = urljoin(self.base_url, str(link_tag["href"]))
                                logging.info(f"Found review report link: {download_url}")
                                found_links.append(download_url)

                if not found_links:
                    logging.warning(f"Could not find any review report links for '{name}'. Skipping.")
                    continue

                # Download all found links
                for url in found_links:
                    # Check if we already processed this URL for this drug
                    if url in all_new_states:
                        continue

                    file_path = self._download_file(url, last_state=last_state.get(url, {}))
                    if file_path and file_path.exists():
                        downloaded_data.append((file_path, url))
                        all_new_states[url] = self.new_state

            except requests.RequestException as e:
                logging.error(f"Failed to process '{name}': {e}", exc_info=True)
                continue
            except ValueError as e:
                logging.error(f"A configuration or parsing error occurred for '{name}': {e}", exc_info=True)
                continue

        logging.info(f"Downloaded {len(downloaded_data)} review report(s).")
        return downloaded_data, all_new_states
