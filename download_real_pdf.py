import sys
from pathlib import Path

# Add the src directory to the Python path to allow importing the package
# Assumes the script is run from the repository root.
sys.path.insert(0, str(Path.cwd() / "src"))

from py_load_pmda.extractor import ReviewReportsExtractor

def download_report():
    """
    Uses the ReviewReportsExtractor to download a real PDF for testing.
    """
    print("Attempting to download a real review report PDF...")

    # The cache directory where the file will be saved
    cache_dir = Path("cache")
    cache_dir.mkdir(exist_ok=True)

    extractor = ReviewReportsExtractor(cache_dir=str(cache_dir))

    # Let's try to download a report for a well-known drug, Loxonin (ロキソニン)
    drug_name = "ロキソニン"

    try:
        # The extractor returns a list of (path, url) tuples
        downloaded_data, _ = extractor.extract(drug_names=[drug_name], last_state={})

        if not downloaded_data:
            print(f"Could not find or download a review report for '{drug_name}'.")
            return

        # Get the path of the first downloaded file
        original_filepath, source_url = downloaded_data[0]
        print(f"Successfully downloaded file from: {source_url}")

        # Define the target path for our test fixture
        target_filepath = cache_dir / "report.pdf"

        # Overwrite the old dummy report.pdf with our new, real one
        if target_filepath.exists():
            target_filepath.unlink()
        original_filepath.rename(target_filepath)

        print(f"Successfully downloaded and saved report to '{target_filepath}'")

    except Exception as e:
        print(f"An error occurred during PDF download: {e}")
        print("This might be due to network issues or changes in the PMDA website.")

if __name__ == "__main__":
    download_report()
