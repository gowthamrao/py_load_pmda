import pandas as pd
from pathlib import Path
import zipfile
import io
from typing import Dict, List
import tabula

class PackageInsertsParser:
    """
    Parses downloaded Package Insert PDF files using tabula-py.
    """
    def parse(self, file_path: Path) -> List[pd.DataFrame]:
        """
        Parses the PDF file and returns a list of pandas DataFrames.

        Args:
            file_path: The path to the PDF file.

        Returns:
            A list of DataFrames, where each DataFrame represents a table from the PDF.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        print(f"Parsing PDF file: {file_path}")
        try:
            # Read all tables from all pages of the PDF
            tables = tabula.read_pdf(file_path, pages="all", multiple_tables=True, lattice=True)

            if not tables:
                print(f"Warning: No tables found in {file_path}.")
                return []

            print(f"Successfully extracted {len(tables)} tables from PDF.")
            return tables

        except Exception as e:
            # This can catch various errors, including Java not being installed,
            # or the PDF being unparseable.
            print(f"Error parsing PDF file {file_path}: {e}")
            print("Please ensure you have Java installed and in your PATH for tabula-py to work.")
            raise


class ApprovalsParser:
    """
    Parses the downloaded New Drug Approvals Excel file.
    """

    def _find_header_row(self, df: pd.DataFrame, keyword: str, search_limit: int = 10) -> int:
        """Finds the header row index by searching for a keyword."""
        for i, row in df.head(search_limit).iterrows():
            # Normalize the row content by removing whitespace before searching
            normalized_row = row.astype(str).str.replace(r'\s+', '', regex=True)
            if normalized_row.str.contains(keyword, na=False).any():
                return i
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
            print(f"Parsing Excel file: {file_path}")
            # First, read without a header to inspect the content
            df_no_header = pd.read_excel(file_path, header=None)

            # Find the actual header row by looking for a known column name.
            header_row_index = self._find_header_row(df_no_header, "販売名")

            # Now, read the excel file again, using the correct header row
            print(f"Found header at row index {header_row_index}. Re-parsing...")
            df = pd.read_excel(file_path, header=header_row_index)

            # Clean up column names (remove newlines and spaces)
            df.columns = df.columns.str.strip().str.replace(r'\s+', '', regex=True)

            # Drop rows that are entirely empty
            df.dropna(how="all", inplace=True)

            # Forward-fill the values in the first three columns to handle merged cells
            ffill_cols = df.columns[:3]
            df[ffill_cols] = df[ffill_cols].ffill()

            print("Successfully parsed Excel file into DataFrame.")
            return [df]
        except Exception as e:
            print(f"Error parsing Excel file {file_path}: {e}")
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
    # The four key files expected inside the JADER zip archive.
    JADER_FILENAMES = ["DEMO", "DRUG", "REAC", "HIST"]

    def _read_csv_from_zip(self, zf: zipfile.ZipFile, filename_stem: str) -> pd.DataFrame:
        """Reads a specific CSV from a zip file into a DataFrame, handling encoding."""
        # Find the actual filename in the zip, ignoring case. e.g., 'DEMO.csv' or 'demo.csv'
        try:
            target_filename = next(
                name for name in zf.namelist() if name.lower() == f"{filename_stem.lower()}.csv"
            )
        except StopIteration:
            print(f"Warning: '{filename_stem}.csv' not found in the zip file.")
            return pd.DataFrame()

        try:
            with zf.open(target_filename) as csv_file:
                # Critical: JADER files use Shift-JIS encoding.
                return pd.read_csv(csv_file, encoding="shift_jis")
        except Exception as e:
            print(f"Error reading '{target_filename}' from zip: {e}")
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

        print(f"--- JADER Parser ---")
        print(f"Parsing JADER zip file: {file_path}")
        with zipfile.ZipFile(file_path) as zf:
            for file_stem in self.JADER_FILENAMES:
                df = self._read_csv_from_zip(zf, file_stem)
                # The key in the returned dictionary must match the table name in the schema.
                table_name = f"jader_{file_stem.lower()}"
                parsed_data[table_name] = df
                if not df.empty:
                    print(f"Successfully parsed '{file_stem}.csv' into '{table_name}' with {len(df)} rows.")

        return parsed_data


class ReviewReportsParser:
    """
    Parses downloaded Review Report PDF files using tabula-py.
    """
    def parse(self, file_path: Path) -> List[pd.DataFrame]:
        """
        Parses the PDF file and returns a list of pandas DataFrames, one for each table.

        Args:
            file_path: The path to the PDF file.

        Returns:
            A list of DataFrames, where each DataFrame is a table found in the PDF.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        print(f"Parsing PDF file: {file_path}")
        try:
            # Read all tables from all pages of the PDF.
            # `multiple_tables=True` ensures that we get a list of DataFrames
            # if multiple tables are on a single page.
            tables = tabula.read_pdf(file_path, pages="all", multiple_tables=True, lattice=True)

            if not tables:
                print(f"Warning: No tables found in {file_path}.")
                return []

            print(f"Successfully extracted {len(tables)} tables from PDF.")
            return tables

        except Exception as e:
            print(f"Error parsing PDF file {file_path}: {e}")
            print("Please ensure you have Java installed and in your PATH for tabula-py to work.")
            raise
