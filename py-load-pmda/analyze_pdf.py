import logging
from pathlib import Path
import pandas as pd
import sys

# Add the src directory to the Python path to allow imports from py_load_pmda
sys.path.insert(0, 'src')

from py_load_pmda.parser import ReviewReportsParser

# Set up basic logging to see the parser's output
logging.basicConfig(level=logging.INFO)
# Prevent pandas from truncating long text columns
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)


def analyze_pdf(file_path: Path):
    """
    Uses the existing parser to extract and print all content from a PDF.
    """
    if not file_path.exists():
        print(f"Error: File not found at {file_path}")
        return

    parser = ReviewReportsParser()
    print(f"--- Analyzing PDF: {file_path} ---")

    try:
        full_text, all_tables = parser.parse(file_path)

        print("\n--- EXTRACTED FULL TEXT ---")
        print(full_text)
        print("--- END OF FULL TEXT ---\n")

        if all_tables:
            print(f"\n--- EXTRACTED {len(all_tables)} TABLES ---")
            for i, table_df in enumerate(all_tables):
                print(f"\n--- Table {i+1} ---")
                print(table_df.to_string())
            print("--- END OF TABLES ---")
        else:
            print("--- NO TABLES EXTRACTED ---")

    except Exception as e:
        print(f"An error occurred during parsing: {e}")

if __name__ == "__main__":
    # Path is now relative to the script's location inside py-load-pmda
    pdf_path = Path("cache/report.pdf")
    analyze_pdf(pdf_path)
