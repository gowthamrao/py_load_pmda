import pytest
from testcontainers.postgres import PostgresContainer
import re

# Read the mock excel file content once, at module load time.
with open("tests/fixtures/empty.xlsx", "rb") as f:
    excel_content = f.read()

@pytest.fixture(scope="session")
def postgres_container():
    """
    Spins up a PostgreSQL container for the test session.
    """
    with PostgresContainer("postgres:13") as postgres:
        yield postgres

@pytest.fixture
def mock_pmda_pages(requests_mock):
    """
    Mocks requests to the PMDA website for fetching approval data.
    Allows other requests (e.g., to Docker daemon) to pass through.
    """
    # By default, allow all network requests to pass through.
    # This is crucial for testcontainers to be able to communicate with the Docker daemon.
    requests_mock.real_http = True

    # Mock the main approvals list page.
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html",
        text='<html><body><a href="/review-services/drug-reviews/review-information/p-drugs/0011.html">2025年度</a></body></html>'
    )

    # Mock the year-specific page.
    requests_mock.get(
        "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0011.html",
        text='<html><body><a href="/drugs/2025/P001/01_1.xlsx">Excel Link</a></body></html>'
    )

    # Mock the Excel file download.
    requests_mock.get(
        "https://www.pmda.go.jp/drugs/2025/P001/01_1.xlsx",
        content=excel_content
    )
