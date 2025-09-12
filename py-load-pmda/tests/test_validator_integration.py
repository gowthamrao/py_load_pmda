"""
This module contains integration tests for the DataValidator component.

The tests are designed to be a comprehensive suite that verifies each
validation rule (`not_null`, `is_unique`, `is_in_set`, etc.) works
correctly within the full ETL orchestrator flow.

It uses a dedicated, in-memory dataset ('validation_test_dataset')
and mocks the parser to inject controlled data that specifically
triggers each validation rule's success and failure modes.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from testcontainers.postgres import PostgresContainer

from py_load_pmda.adapters.postgres import PostgreSQLAdapter
from py_load_pmda.orchestrator import Orchestrator
from py_load_pmda.parser import XMLParser

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration


@pytest.fixture
def validator_test_config(
    postgres_container: PostgresContainer, postgres_adapter: tuple[PostgreSQLAdapter, str]
) -> dict:
    """
    Creates a test-specific configuration for the Orchestrator.

    This config defines a 'validation_test_dataset' with a comprehensive
    set of validation rules. The database connection and schema name are
    dynamically obtained from the test fixtures to ensure test isolation.
    """
    _, schema_name = postgres_adapter
    return {
        "database": {
            "type": "postgres",
            "host": postgres_container.get_container_host_ip(),
            "port": postgres_container.get_exposed_port(5432),
            "user": postgres_container.username,
            "password": postgres_container.password,
            "dbname": postgres_container.dbname,
        },
        "datasets": {
            "validation_test_dataset": {
                "extractor": "BaseExtractor",
                "parser": "XMLParser",
                "transformer": "BaseTransformer",
                "table_name": "validation_test_table",
                "schema_name": schema_name,  # Use the dynamic schema name
                "load_mode": "overwrite",
                "validation": [
                    # Rule 1: Not Null
                    {"column": "id", "check": "not_null"},
                    # Rule 2: Is Unique
                    {"column": "id", "check": "is_unique"},
                    # Rule 3: Has Type (Integer)
                    {"column": "id", "check": "has_type", "type": "int64"},
                    # Rule 4: Is in Set
                    {
                        "column": "category",
                        "check": "is_in_set",
                        "allowed_values": ["TYPE_A", "TYPE_B", "TYPE_C"],
                    },
                    # Rule 5: Is in Range
                    {
                        "column": "value",
                        "check": "is_in_range",
                        "min_value": 0,
                        "max_value": 100,
                    },
                ],
            }
        },
    }


@pytest.fixture
def mock_extractor(monkeypatch):
    """
    Mocks the extractor to prevent any real file I/O. The orchestrator
    requires it to run, but its output will be ignored since the parser
    is also mocked.
    """
    mock = MagicMock()
    mock.extract.return_value = (Path("/fake/path"), "http://fake.url", {})
    from py_load_pmda.orchestrator import AVAILABLE_EXTRACTORS

    monkeypatch.setitem(AVAILABLE_EXTRACTORS, "BaseExtractor", lambda: mock)


def mock_parser_with_data(monkeypatch, df: pd.DataFrame):
    """
    A helper function to mock the XMLParser to return a specific,
    controlled DataFrame. This is the mechanism used to inject
    both valid and invalid data into the pipeline for testing.
    """
    mock = MagicMock(spec=XMLParser)
    # The XMLParser returns a single DataFrame, not a list.
    # The transformer handles both single DFs and lists, so this is fine.
    mock.parse.return_value = df
    from py_load_pmda.orchestrator import AVAILABLE_PARSERS

    monkeypatch.setitem(AVAILABLE_PARSERS, "XMLParser", lambda: mock)


# A baseline orchestrator setup function to reduce boilerplate in tests
def setup_orchestrator(config, postgres_adapter):
    """Prepares the database schema for an orchestrator run."""
    adapter, schema_name = postgres_adapter
    from py_load_pmda.schemas import INGESTION_STATE_SCHEMA
    INGESTION_STATE_SCHEMA["schema_name"] = schema_name
    adapter.ensure_schema(INGESTION_STATE_SCHEMA)
    adapter.commit()
    return Orchestrator(config=config, dataset="validation_test_dataset")


def test_validation_fails_on_null_id(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """Verifies that the 'not_null' validation rule is correctly triggered."""
    # Arrange: Create data with a null 'id'
    invalid_df = pd.DataFrame(
        [
            {"id": None, "category": "TYPE_A", "value": 50},
        ]
    )
    mock_parser_with_data(monkeypatch, invalid_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act & Assert
    with pytest.raises(ValueError, match="Column 'id' has 1 null values"):
        orchestrator.run()


def test_validation_fails_on_duplicate_id(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """Verifies that the 'is_unique' validation rule is correctly triggered."""
    # Arrange: Create data with duplicate 'id's
    invalid_df = pd.DataFrame(
        [
            {"id": 1, "category": "TYPE_A", "value": 50},
            {"id": 1, "category": "TYPE_B", "value": 60},
        ]
    )
    mock_parser_with_data(monkeypatch, invalid_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act & Assert
    with pytest.raises(ValueError, match="Column 'id' is not unique"):
        orchestrator.run()


def test_validation_fails_on_bad_type(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """Verifies that the 'has_type' validation rule is correctly triggered."""
    # Arrange: Create data with a non-integer 'id'
    # The transformer won't fix this, so the validator should catch it.
    invalid_df = pd.DataFrame(
        [
            {"id": "not-an-int", "category": "TYPE_A", "value": 50},
        ]
    )
    # Ensure the dtype is 'object' to trigger the check
    invalid_df["id"] = invalid_df["id"].astype("object")
    mock_parser_with_data(monkeypatch, invalid_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act & Assert
    # The pandas type check for int64 will fail.
    with pytest.raises(ValueError, match="expected int64"):
        orchestrator.run()


def test_validation_fails_on_value_not_in_set(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """Verifies that the 'is_in_set' validation rule is correctly triggered."""
    # Arrange: Create data with a category not in the allowed set
    invalid_df = pd.DataFrame(
        [
            {"id": 1, "category": "TYPE_D", "value": 50},  # TYPE_D is not allowed
        ]
    )
    mock_parser_with_data(monkeypatch, invalid_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act & Assert
    with pytest.raises(ValueError, match="not in the allowed set"):
        orchestrator.run()


def test_validation_fails_on_value_out_of_range(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """Verifies that the 'is_in_range' validation rule is correctly triggered."""
    # Arrange: Create data with a value outside the allowed range
    invalid_df = pd.DataFrame(
        [
            {"id": 1, "category": "TYPE_A", "value": 101},  # 101 is > 100
        ]
    )
    mock_parser_with_data(monkeypatch, invalid_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act & Assert
    with pytest.raises(ValueError, match="values outside the range"):
        orchestrator.run()


def test_validation_succeeds_with_clean_data(
    postgres_adapter: tuple[PostgreSQLAdapter, str],
    validator_test_config: dict,
    mock_extractor: MagicMock,
    monkeypatch,
):
    """
    Verifies that the orchestrator runs successfully when provided with
    clean data that passes all validation rules.
    """
    # Arrange: Create a clean DataFrame
    clean_df = pd.DataFrame(
        [
            {"id": 1, "category": "TYPE_A", "value": 0},
            {"id": 2, "category": "TYPE_B", "value": 50},
            {"id": 3, "category": "TYPE_C", "value": 100},
        ]
    )
    # Ensure correct dtype for the 'id' column
    clean_df["id"] = clean_df["id"].astype("int64")

    mock_parser_with_data(monkeypatch, clean_df)
    orchestrator = setup_orchestrator(validator_test_config, postgres_adapter)

    # Act: Run the orchestrator. No exception should be raised.
    try:
        orchestrator.run()
    except ValueError as e:
        pytest.fail(f"Validation failed unexpectedly with clean data: {e}")

    # Assert: Verify that the data was loaded correctly into the database
    adapter, schema_name = postgres_adapter
    table_name = validator_test_config["datasets"]["validation_test_dataset"]["table_name"]
    query = f'SELECT * FROM {schema_name}."{table_name}" ORDER BY id;'
    loaded_df = pd.read_sql(query, adapter.conn)

    # Check that the loaded data matches the input data
    assert len(loaded_df) == len(clean_df)
    # Reset index for comparison, as SQL-loaded DFs have a fresh index
    pd.testing.assert_frame_equal(
        clean_df, loaded_df.drop(columns=["index"], errors="ignore"), check_like=True
    )
