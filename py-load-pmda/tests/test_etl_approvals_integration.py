from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from testcontainers.postgres import PostgresContainer

from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.orchestrator import Orchestrator
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
        {
            "application_type": "第1",
            "approval_date": "令和7年1月1日",
            "approval_id": 1.0,
            "brand_name_jp": "Drug A",
            "applicant_name_jp": "Corp A",
            "generic_name_jp": "Generic A",
            "indication": "Indication A",
        },
        # Approval ID 2: Two rows that need aggregation
        {
            "application_type": "第2",
            "approval_date": "令和7年2月2日",
            "approval_id": 2.0,
            "brand_name_jp": "Drug B",
            "applicant_name_jp": "Corp B",
            "generic_name_jp": "Generic B1",
            "indication": "Indication B1",
        },
        {
            "application_type": "第2",
            "approval_date": "令和7年2月2日",
            "approval_id": 2.0,
            "brand_name_jp": "Drug B",
            "applicant_name_jp": "Corp B",
            "generic_name_jp": "Generic B2",
            "indication": "Indication B2",
        },
    ]
    raw_df = pd.DataFrame(raw_data)

    # The parser is expected to return a list of DataFrames
    mock_parser.parse.return_value = [raw_df]

    # Use monkeypatch to replace the class in the orchestrator's registry
    from py_load_pmda.orchestrator import AVAILABLE_PARSERS

    monkeypatch.setitem(AVAILABLE_PARSERS, "ApprovalsParser", lambda: mock_parser)
    return mock_parser


@pytest.fixture
def mock_approvals_parser_with_invalid_data(monkeypatch):
    """
    Mocks the ApprovalsParser to return a DataFrame with data that violates
    the validation rules (e.g., null 'No.' which becomes null 'approval_id').
    """
    mock_parser = MagicMock(spec=ApprovalsParser)
    invalid_raw_data = [
        {
            "application_type": "第1",
            "approval_date": "令和7年1月1日",
            "approval_id": None,  # This will cause the 'approval_id' to be null
            "brand_name_jp": "Invalid Drug",
            "applicant_name_jp": "Corp C",
            "generic_name_jp": "Generic C",
            "indication": "Indication C",
        }
    ]
    invalid_df = pd.DataFrame(invalid_raw_data)
    mock_parser.parse.return_value = [invalid_df]

    from py_load_pmda.orchestrator import AVAILABLE_PARSERS

    monkeypatch.setitem(AVAILABLE_PARSERS, "ApprovalsParser", lambda: mock_parser)
    return mock_parser


@pytest.fixture
def mock_extractor_that_does_nothing(monkeypatch):
    """
    Since we mock the parser, the extractor will still be called but its
    output is irrelevant. We mock it to prevent any actual file downloads
    and to return dummy values of the correct type.
    """
    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = (
        Path("/fake/path"),
        "http://fake.url",
        {"etag": "fake_etag"},
    )
    from py_load_pmda.orchestrator import AVAILABLE_EXTRACTORS

    monkeypatch.setitem(AVAILABLE_EXTRACTORS, "ApprovalsExtractor", lambda: mock_extractor)
    return mock_extractor


def test_approvals_etl_pipeline_aggregation(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    mock_approvals_parser: MagicMock,
    mock_extractor_that_does_nothing: MagicMock,
    postgres_container: PostgresContainer,
):
    """
    Tests the ETL pipeline's end-to-end flow, focusing on
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

    assert len(loaded_df) == 2

    row_1 = loaded_df[loaded_df["approval_id"] == 1].iloc[0]
    assert row_1["brand_name_jp"] == "Drug A"
    assert row_1["applicant_name_jp"] == "Corp A"
    assert str(row_1["approval_date"]) == "2025-01-01"

    row_2 = loaded_df[loaded_df["approval_id"] == 2].iloc[0]
    assert row_2["generic_name_jp"] == "Generic B1\nGeneric B2"


def test_approvals_etl_pipeline_validation_failure(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    mock_approvals_parser_with_invalid_data: MagicMock,
    mock_extractor_that_does_nothing: MagicMock,
    postgres_container: PostgresContainer,
):
    """
    Tests that the ETL pipeline fails when data violates validation rules.
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
                ],
            }
        },
    }

    # Set up the ingestion state table, which the orchestrator needs
    from py_load_pmda.schemas import INGESTION_STATE_SCHEMA
    INGESTION_STATE_SCHEMA["schema_name"] = schema_name
    adapter.ensure_schema(INGESTION_STATE_SCHEMA)
    adapter.commit()

    orchestrator = Orchestrator(
        config=test_config,
        dataset="approvals",
        year=2025,
    )

    # Expect the orchestrator to raise a ValueError due to the validation failure
    with pytest.raises(ValueError, match="Data validation failed for table 'pmda_approvals'"):
        orchestrator.run()

    # Also, verify that no data was loaded into the table
    # We need to check if the table exists before querying it
    table_exists_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        AND table_name = 'pmda_approvals'
    );
    """
    table_exists = pd.read_sql(table_exists_query, adapter.conn).iloc[0, 0]

    if table_exists:
        query = f"SELECT COUNT(*) FROM {schema_name}.pmda_approvals;"
        count = pd.read_sql(query, adapter.conn).iloc[0, 0]
        assert count == 0
