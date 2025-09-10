import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import MagicMock
from testcontainers.postgres import PostgresContainer

from py_load_pmda.orchestrator import Orchestrator
from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.parser import ApprovalsParser

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

@pytest.fixture
def mock_approvals_parser(monkeypatch):
    """
    Mocks the ApprovalsParser to return a controlled, in-memory DataFrame,
    bypassing the need for an extractor or a real file.
    """
    mock_parser = MagicMock(spec=ApprovalsParser)

    # Create a precise, controlled DataFrame to test aggregation.
    # This represents the data *after* parsing but *before* transformation.
    raw_data = [
        # Approval ID 1: Single row
        {"分野": "第1", "承認日": "令和7年1月1日", "No.": 1.0, "販売名(会社名、法人番号)": "Drug A (Corp A、123)", "成分名(下線:新有効成分)": "Generic A", "効能・効果等": "Indication A"},
        # Approval ID 2: Two rows that need aggregation
        {"分野": "第2", "承認日": "令和7年2月2日", "No.": 2.0, "販売名(会社名、法人番号)": "Drug B (Corp B、456)", "成分名(下線:新有効成分)": "Generic B1", "効能・効果等": "Indication B1"},
        {"分野": "第2", "承認日": "令和7年2月2日", "No.": 2.0, "販売名(会社名、法人番号)": "Drug B (Corp B、456)", "成分名(下線:新有効成分)": "Generic B2", "効能・効果等": "Indication B2"},
    ]
    raw_df = pd.DataFrame(raw_data)

    # The parser is expected to return a list of DataFrames
    mock_parser.parse.return_value = [raw_df]

    # Use monkeypatch to replace the class in the orchestrator's registry
    from py_load_pmda.orchestrator import AVAILABLE_PARSERS
    monkeypatch.setitem(
        AVAILABLE_PARSERS,
        "ApprovalsParser",
        lambda: mock_parser
    )
    return mock_parser


@pytest.fixture
def mock_extractor_that_does_nothing(monkeypatch):
    """
    Since we mock the parser, the extractor will still be called but its
    output is irrelevant. We mock it to prevent any actual file downloads
    and to return dummy values of the correct type.
    """
    mock_extractor = MagicMock()
    # Return dummy values: (Path, str, dict)
    mock_extractor.extract.return_value = (Path("/fake/path"), "http://fake.url", {"etag": "fake_etag"})
    from py_load_pmda.orchestrator import AVAILABLE_EXTRACTORS
    monkeypatch.setitem(
        AVAILABLE_EXTRACTORS,
        "ApprovalsExtractor",
        lambda: mock_extractor
    )
    return mock_extractor


def test_approvals_etl_pipeline_aggregation(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    mock_approvals_parser: MagicMock,
    mock_extractor_that_does_nothing: MagicMock,
    postgres_container: PostgresContainer
):
    """
    Tests the full ETL pipeline for the 'approvals' dataset, focusing on
    the transformer's ability to correctly aggregate data.
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
                "extractor": "ApprovalsExtractor", # Will be the do-nothing mock
                "parser": "ApprovalsParser",       # Will be the mock with controlled data
                "transformer": "ApprovalsTransformer",
                "table_name": "pmda_approvals",
                "schema_name": schema_name,
                "load_mode": "overwrite",
            }
        }
    }

    from py_load_pmda.schemas import INGESTION_STATE_SCHEMA
    # The schema name in the global object must also be updated for the state table
    INGESTION_STATE_SCHEMA['schema_name'] = schema_name
    adapter.ensure_schema(INGESTION_STATE_SCHEMA)
    adapter.commit()

    orchestrator = Orchestrator(
        config=test_config,
        dataset="approvals",
        year=2025 # Dummy value, as extractor is mocked
    )
    orchestrator.run()

    query = f"SELECT * FROM {schema_name}.pmda_approvals ORDER BY approval_id;"
    loaded_df = pd.read_sql(query, adapter.conn)

    # We provided 2 unique approval_id's, so we expect 2 rows after aggregation.
    assert len(loaded_df) == 2

    # Check the single-row approval
    row_1 = loaded_df[loaded_df['approval_id'] == 1].iloc[0]
    assert row_1['brand_name_jp'] == 'Drug A'
    assert row_1['applicant_name_jp'] == 'Corp A'
    assert str(row_1['approval_date']) == '2025-01-01'

    # Check the aggregated approval
    row_2 = loaded_df[loaded_df['approval_id'] == 2].iloc[0]
    assert str(row_2['approval_date']) == '2025-02-02'
    assert row_2['applicant_name_jp'] == 'Corp B'
    # Check that text fields were correctly aggregated with unique values
    assert row_2['generic_name_jp'] == 'Generic B1\nGeneric B2'
    assert row_2['indication'] == 'Indication B1\nIndication B2'

    # Check that raw_data_full contains a JSON array of the two original rows
    # pd.read_sql auto-deserializes JSONB, so we should have a list directly.
    raw_data = row_2['raw_data_full']
    assert isinstance(raw_data, list)
    assert len(raw_data) == 2
    assert raw_data[0]['No.'] == 2.0
    assert raw_data[1]['成分名(下線:新有効成分)'] == 'Generic B2'
