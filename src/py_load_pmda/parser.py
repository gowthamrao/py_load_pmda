import logging
from pathlib import Path
from typing import List

import pandas as pd


class BaseParser:
    """
    Base class for all parsers.
    """

    def parse(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a single file and return a DataFrame.
        This method should be implemented by subclasses.
        """
        raise NotImplementedError


class ApprovalsParser(BaseParser):
    """
    Parser for New Drug Approval Excel files.
    """

    def parse(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a single Excel file containing drug approvals.

        It inspects all sheets in the Excel file and attempts to parse the one
        that contains the approval data. It assumes the data is in the first
        sheet that can be successfully parsed.

        Args:
            file_path: The local path to the .xlsx file.

        Returns:
            A pandas DataFrame with the raw data from the Excel sheet.
            Returns an empty DataFrame if parsing fails.
        """
        logging.info(f"Parsing approvals Excel file: {file_path}")
        try:
            # Use pandas ExcelFile to be able to inspect sheets first
            with pd.ExcelFile(file_path) as xls:
                # For now, we assume the relevant data is on the first sheet.
                # In a real-world scenario, we might need more complex logic
                # to identify the correct sheet by name or content.
                if not xls.sheet_names:
                    logging.warning(f"No sheets found in {file_path}.")
                    return pd.DataFrame()

                sheet_name = xls.sheet_names[0]
                logging.info(f"Reading sheet '{sheet_name}' from {file_path}")

                # We can add more sophisticated logic here to find the header row
                # if it's not always on the first line. For now, we assume row 0.
                df = pd.read_excel(xls, sheet_name=sheet_name, header=0)

                # Add metadata about the source file to each row
                df["_meta_source_file"] = file_path.name

                logging.info(f"Successfully parsed {len(df)} rows from {file_path}")
                return df
        except Exception as e:
            logging.error(f"Failed to parse Excel file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()
