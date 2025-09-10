import io
import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pdfplumber

from py_load_pmda import utils


class BasePDFParser:
    """
    A base class for parsing PDF files using pdfplumber.
    Provides a generic method to extract all text and tables.
    """
    def __init__(self) -> None:
        pass

    def parse(self, file_path: Path) -> Tuple[str, List[pd.DataFrame]]:
        """
        Parses the PDF file, extracting full text and all tables.

        Args:
            file_path: The path to the PDF file.

        Returns:
            A tuple containing:
            - A string with the full text content of the PDF.
            - A list of DataFrames, where each DataFrame is a table from the PDF.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        logging.info(f"Parsing PDF with pdfplumber: {file_path}")
        full_text = []
        all_tables = []

        try:
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    logging.debug(f"  - Processing page {i + 1}/{len(pdf.pages)}")
                    # Extract text from the page
                    page_text = page.extract_text()
                    if page_text:
                        full_text.append(page_text)

                    # Extract tables from the page
                    tables = page.extract_tables()
                    for table in tables:
                        # Ensure the table is not empty and has a header row
                        if table and len(table) > 0 and any(table[0]):
                            df = pd.DataFrame(table[1:], columns=table[0])
                            all_tables.append(df)

            logging.info(f"Successfully extracted {len(all_tables)} tables and text from PDF.")
            return "\n".join(full_text), all_tables

        except Exception as e:
            logging.error(f"Error parsing PDF file {file_path} with pdfplumber: {e}", exc_info=True)
            raise


class PackageInsertsParser(BasePDFParser):
    """
    Parses downloaded Package Insert PDF files.
    Inherits the generic PDF parsing logic from BasePDFParser.
    """
    pass


class XMLParser:
    """
    Parses an XML file into a pandas DataFrame using an XPath expression.
    """
    def __init__(self) -> None:
        pass

    def parse(self, file_path: Path, xpath: str) -> pd.DataFrame:
        """
        Parses the XML file and returns a pandas DataFrame based on the XPath.

        Args:
            file_path: The path to the XML file.
            xpath: The XPath expression to select the nodes to be parsed into
                   the DataFrame. Each node will become a row.

        Returns:
            A pandas DataFrame containing the data extracted from the XML.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        if not xpath:
            raise ValueError("An XPath expression must be provided.")

        logging.info(f"Parsing XML file: {file_path} with XPath: '{xpath}'")
        try:
            # pandas.read_xml is a powerful function that uses lxml by default.
            # It can handle reading attributes and nested elements directly into columns.
            df = pd.read_xml(file_path, xpath=xpath, parser="lxml")
            logging.info(f"Successfully parsed XML file into a DataFrame with {len(df)} rows.")
            return df
        except Exception as e:
            logging.error(f"Error parsing XML file {file_path}: {e}", exc_info=True)
            raise


class ApprovalsParser:
    """
    Parses the downloaded New Drug Approvals Excel file.
    """
    def __init__(self) -> None:
        pass

    def _find_header_row(self, df: pd.DataFrame, keyword: str, search_limit: int = 10) -> int:
        """Finds the header row index by searching for a keyword."""
        for i, row in df.head(search_limit).iterrows():
            # Normalize the row content by removing whitespace before searching
            normalized_row = row.astype(str).str.replace(r'\s+', '', regex=True)
            if normalized_row.str.contains(keyword, na=False).any():
                return int(i)  # type: ignore
        raise ValueError(f"Could not find header row containing '{keyword}' within the first {search_limit} rows.")


    def parse(self, file_path: Path) -> List[pd.DataFrame]:
        """
        Parses the Excel file and returns a list containing a single pandas DataFrame.

        Args:
            file_path: The path to the Excel file.

        Returns:
            A list containing a single DataFrame with the raw data from the Excel file.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        try:
            logging.info(f"Parsing Excel file: {file_path}")
            # First, read without a header to inspect the content
            df_no_header = pd.read_excel(file_path, header=None)

            # Find the actual header row by looking for a known column name.
            header_row_index = self._find_header_row(df_no_header, "販売名")

            # Now, read the excel file again, using the correct header row
            logging.info(f"Found header at row index {header_row_index}. Re-parsing...")
            df = pd.read_excel(file_path, header=header_row_index)

            # Clean up column names (remove newlines and spaces)
            df.columns = df.columns.str.strip().str.replace(r'\s+', '', regex=True)

            # Drop rows that are entirely empty
            df.dropna(how="all", inplace=True)

            # Forward-fill the values in the first three columns to handle merged cells
            ffill_cols = df.columns[:3]
            df[ffill_cols] = df[ffill_cols].ffill()

            logging.info("Successfully parsed Excel file into DataFrame.")
            return [df]
        except Exception as e:
            logging.error(f"Error parsing Excel file {file_path}: {e}", exc_info=True)
            raise


class JaderParser:
    """
    Parses a downloaded JADER zip file.

    The JADER dataset is distributed in a zip file containing four CSV files:
    - DEMO.csv: Patient demographic information
    - DRUG.csv: Drug information
    - REAC.csv: Reaction (adverse event) information
    - HIST.csv: Patient history information
    """
    def __init__(self) -> None:
        pass
    # The four key files expected inside the JADER zip archive.
    JADER_FILENAMES = ["DEMO", "DRUG", "REAC", "HIST"]

    def _read_csv_from_zip(self, zf: zipfile.ZipFile, filename_stem: str) -> pd.DataFrame:
        """
        Reads a specific CSV from a zip file into a DataFrame, handling encoding
        by automatically detecting it.
        """
        # Find the actual filename in the zip, ignoring case. e.g., 'DEMO.csv' or 'demo.csv'
        try:
            target_filename = next(
                name for name in zf.namelist() if name.lower() == f"{filename_stem.lower()}.csv"
            )
        except StopIteration:
            logging.warning(f"Warning: '{filename_stem}.csv' not found in the zip file.")
            return pd.DataFrame()

        logging.debug(f"Reading '{target_filename}' from zip...")
        try:
            # Read the file as raw bytes first to detect encoding
            file_bytes = zf.read(target_filename)

            if not file_bytes:
                logging.warning(f"Warning: '{target_filename}' is empty.")
                return pd.DataFrame()

            # Use the utility to detect the encoding, with a fallback to Shift-JIS
            # for JADER files, as it's the most likely encoding.
            encoding = utils.detect_encoding(file_bytes, fallback='shift_jis')

            # Use the detected encoding to read the CSV into a DataFrame
            # We use io.BytesIO to treat the byte string as a file
            return pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)

        except Exception as e:
            logging.error(f"Error reading or parsing '{target_filename}' from zip: {e}", exc_info=True)
            return pd.DataFrame()

    def parse(self, file_path: Path) -> Dict[str, pd.DataFrame]:
        """
        Parses a single JADER zip file.

        Args:
            file_path: The path to the downloaded .zip file.

        Returns:
            A dictionary of DataFrames, where keys are the target table names
            (e.g., 'jader_demo', 'jader_drug').
        """
        parsed_data = {}
        if not file_path or not file_path.exists():
            raise FileNotFoundError(f"JADER zip file not found at {file_path}.")

        logging.info("--- JADER Parser ---")
        logging.info(f"Parsing JADER zip file: {file_path}")
        with zipfile.ZipFile(file_path) as zf:
            for file_stem in self.JADER_FILENAMES:
                df = self._read_csv_from_zip(zf, file_stem)
                # The key in the returned dictionary must match the table name in the schema.
                table_name = f"jader_{file_stem.lower()}"
                parsed_data[table_name] = df
                if not df.empty:
                    logging.info(f"Successfully parsed '{file_stem}.csv' into '{table_name}' with {len(df)} rows.")

        return parsed_data


class ReviewReportsParser(BasePDFParser):
    """
    Parses downloaded Review Report PDF files.
    Inherits the generic PDF parsing logic from BasePDFParser.
    """
    pass
