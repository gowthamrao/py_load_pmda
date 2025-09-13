import pytest
import pandas as pd
from testcontainers.postgres import PostgresContainer
import requests_mock

from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.orchestrator import Orchestrator

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

@pytest.fixture
def mocked_approvals_requests(requests_mock):
    """
    Mocks the HTTP requests made by the ApprovalsExtractor to return
    controlled, local fixture data instead of hitting the live PMDA website.
    """
    requests_mock.real_http = True
    # Fixture for the main approvals page
    main_page_url = "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010.html"
    main_page_content = """<!DOCTYPE html>
<html>
<head>
    <title>Main Approvals Page</title>
</head>
<body>
    <h1>Approvals List</h1>
    <ul>
        <li><a href="/review-services/drug-reviews/review-information/p-drugs/0010_2023.html">2023年度 承認品目一覧</a></li>
        <li><a href="/review-services/drug-reviews/review-information/p-drugs/0010_2024.html">2024年度 承認品目一覧</a></li>
        <li><a href="/review-services/drug-reviews/review-information/p-drugs/0010_2025.html">2025年度 承認品目一覧</a></li>
    </ul>
</body>
</html>"""
    requests_mock.get(main_page_url, text=main_page_content)

    # Fixture for the 2025 fiscal year page
    year_page_url = "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0010_2025.html"
    year_page_content = """<!DOCTYPE html>
<html>
<head>
    <title>2025 Approvals</title>
</head>
<body>
    <h1>2025年度 承認品目一覧</h1>
    <p>Here is the list of approved drugs for fiscal year 2025.</p>
    <a href="https://www.pmda.go.jp/files/approvals_2025.xlsx">新医薬品として承認された医薬品一覧（令和7年）</a>
</body>
</html>"""
    requests_mock.get(year_page_url, text=year_page_content)

    # Fixture for the 2025 Excel file download
    excel_url = "https://www.pmda.go.jp/files/approvals_2025.xlsx"
    excel_content = open("tests/fixtures/approvals_2025.xlsx", "rb").read()
    requests_mock.get(excel_url, content=excel_content)

def test_approvals_etl_full_pipeline(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    mocked_approvals_requests,
    postgres_container: PostgresContainer,
):
    """
    Tests the full ETL pipeline for the 'approvals' dataset, from
    extraction using mocked HTTP requests through to loading into a live
    test database.
    """
    adapter, schema_name = postgres_adapter
    test_config = {
        "database": {
            "type": "postgres",
            "host": postgres_container.get_container_host_ip(),
            "port": postgres_container.get_exposed_port(5432),
            "user": postgres_container.username,
            "password": postgres_container.password,
            "dbname": postgres_container.dbname,
        },
        "datasets": {
            "approvals": {
                "extractor": "ApprovalsExtractor",
                "parser": "ApprovalsParser",
                "transformer": "ApprovalsTransformer",
                "table_name": "pmda_approvals",
                "schema_name": schema_name,
                "load_mode": "overwrite",
                 "validation": [
                    {"column": "approval_id", "check": "not_null"},
                    {"column": "approval_id", "check": "is_unique"},
                ],
            }
        },
    }

    from py_load_pmda.schemas import INGESTION_STATE_SCHEMA

    INGESTION_STATE_SCHEMA["schema_name"] = schema_name
    adapter.ensure_schema(INGESTION_STATE_SCHEMA)
    adapter.commit()

    orchestrator = Orchestrator(
        config=test_config,
        dataset="approvals",
        year=2025,
    )
    orchestrator.run()

    query = f"SELECT * FROM {schema_name}.pmda_approvals ORDER BY approval_id;"
    loaded_df = pd.read_sql(query, adapter.conn)

    # Assertions based on the content of fixtures/approvals_2025.xlsx
    assert len(loaded_df) == 2
    assert loaded_df["approval_id"].is_unique

    # Check a specific record (e.g., the first one, No. 1)
    record = loaded_df[loaded_df["approval_id"] == 1].iloc[0]
    assert record["brand_name_jp"] == "test1"
    assert record["applicant_name_jp"] == "applicant1"
    assert str(record["approval_date"]) == "2025-01-01"
    assert record["generic_name_jp"] == "generic1"
