import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from py_load_pmda.extractor import ApprovalsExtractor

if __name__ == "__main__":
    print("Running extractor to download test fixture...")
    extractor = ApprovalsExtractor(cache_dir="cache") # I am inside py-load-pmda
    extractor.extract(year=2025)
    print("Extractor finished.")
