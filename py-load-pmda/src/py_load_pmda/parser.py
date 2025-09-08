import pandas as pd
from pathlib import Path

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
            # "承認日" (Approval Date) is a good, clean keyword.
            # "販売名" (Brand Name) is also good, but we need to normalize whitespace.
            header_row_index = self._find_header_row(df_no_header, "販売名")

            # Now, read the excel file again, using the correct header row
            print(f"Found header at row index {header_row_index}. Re-parsing...")
            df = pd.read_excel(file_path, header=header_row_index)

            # Clean up column names (remove newlines and spaces)
            df.columns = df.columns.str.strip().str.replace(r'\s+', '', regex=True)

            # Drop rows that are entirely empty
            df.dropna(how="all", inplace=True)

            # Forward-fill the values in the first three columns ('分野', '承認日', 'No.')
            # This handles the merged cells where these values are only present on the first row of a multi-row entry.
            ffill_cols = df.columns[:3]
            df[ffill_cols] = df[ffill_cols].ffill()

            print("Successfully parsed Excel file into DataFrame.")
            return df
        except Exception as e:
            print(f"Error parsing Excel file {file_path}: {e}")
            raise
