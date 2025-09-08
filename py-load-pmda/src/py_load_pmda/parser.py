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
    def parse(self, file_path: Path) -> pd.DataFrame:
        """
        Parses the PDF file and returns a pandas DataFrame containing table data.

        Args:
            file_path: The path to the PDF file.

        Returns:
            A DataFrame containing the concatenated table data from the PDF.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        print(f"Parsing PDF file: {file_path}")
        try:
            # Read all tables from all pages of the PDF
            # This returns a list of DataFrames
            tables = tabula.read_pdf(file_path, pages="all", multiple_tables=True, lattice=True)

            if not tables:
                print(f"Warning: No tables found in {file_path}.")
                return pd.DataFrame()

            # Concatenate all found tables into a single DataFrame
            full_df = pd.concat(tables, ignore_index=True)
            print(f"Successfully extracted and concatenated {len(tables)} tables from PDF.")
            return full_df

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


    def parse(self, file_path: Path) -> pd.DataFrame:
        """
        Parses the Excel file and returns a pandas DataFrame.

        Args:
            file_path: The path to the Excel file.

        Returns:
            A DataFrame containing the raw data from the Excel file.
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
            return df
        except Exception as e:
            print(f"Error parsing Excel file {file_path}: {e}")
            raise


class JaderParser:
    """
    Parses the downloaded JADER zip files.

    The JADER dataset is distributed in quarterly zip files, each containing
    four CSV files:
    - CASE.csv: Case information
    - DEMO.csv: Patient demographic information
    - DRUG.csv: Drug information
    - REAC.csv: Reaction (adverse event) information
    """
    JADER_FILES = ["CASE", "DEMO", "DRUG", "REAC"]

    def _read_csv_from_zip(self, zf: zipfile.ZipFile, filename: str) -> pd.DataFrame:
        """Reads a specific CSV from a zip file into a DataFrame."""
        try:
            # The filenames inside the zip are uppercase (e.g., 'CASE.csv')
            with zf.open(f"{filename.upper()}.csv") as csv_file:
                # Critical: JADER files use Shift-JIS encoding.
                return pd.read_csv(csv_file, encoding="shift_jis", low_memory=False)
        except KeyError:
            print(f"Warning: {filename}.csv not found in zip file.")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error reading {filename}.csv from zip: {e}")
            return pd.DataFrame()

    def parse(self, file_paths: List[Path]) -> Dict[str, pd.DataFrame]:
        """
        Parses multiple JADER zip files and concatenates the data.

        Args:
            file_paths: A list of paths to the downloaded .zip files.

        Returns:
            A dictionary of DataFrames, with keys 'case', 'demo', 'drug', 'reac'.
        """
        all_data = {key.lower(): [] for key in self.JADER_FILES}

        print(f"Parsing {len(file_paths)} JADER zip files...")
        for zip_path in file_paths:
            if not zip_path.exists():
                print(f"Warning: Zip file not found at {zip_path}. Skipping.")
                continue

            with zipfile.ZipFile(zip_path) as zf:
                for file_type in self.JADER_FILES:
                    df = self._read_csv_from_zip(zf, file_type)
                    if not df.empty:
                        all_data[file_type.lower()].append(df)

        # Concatenate all the dataframes for each type
        concatenated_data = {}
        for file_type, df_list in all_data.items():
            if df_list:
                concatenated_data[file_type] = pd.concat(df_list, ignore_index=True)
                print(f"Successfully concatenated {len(df_list)} files for '{file_type}', total rows: {len(concatenated_data[file_type])}.")
            else:
                concatenated_data[file_type] = pd.DataFrame()

        return concatenated_data
