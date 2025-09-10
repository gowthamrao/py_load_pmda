import pytest
from unittest.mock import MagicMock, patch

from py_load_pmda.orchestrator import Orchestrator

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY

from py_load_pmda.orchestrator import Orchestrator

@pytest.fixture
def mock_config():
    """Provides a mock configuration for tests, using a realistic dataset name."""
    return {
        "database": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "user",
            "password": "password",
            "dbname": "testdb",
        },
        "datasets": {
            "jader": {
                "extractor": "JaderExtractor",
                "parser": "JaderParser",
                "transformer": "JaderTransformer",
                "schema_name": "jader_schema",
                "load_mode": "overwrite",
                # Note: table_name is not at the top level for multi-table datasets
            }
        },
        "logging": {"level": "DEBUG"},
    }


def test_orchestrator_initialization(mock_config):
    """Test that the orchestrator initializes correctly."""
    orchestrator = Orchestrator(
        config=mock_config,
        dataset="jader",
    )
    assert orchestrator.dataset == "jader"
    assert orchestrator.config == mock_config


@patch("py_load_pmda.orchestrator.get_db_adapter")
@patch("py_load_pmda.orchestrator.AVAILABLE_EXTRACTORS")
@patch("py_load_pmda.orchestrator.AVAILABLE_PARSERS")
@patch("py_load_pmda.orchestrator.AVAILABLE_TRANSFORMERS")
@patch("py_load_pmda.orchestrator.schemas")
def test_orchestrator_run_successful_jader(
    mock_schemas,
    mock_transformers,
    mock_parsers,
    mock_extractors,
    mock_get_db_adapter,
    mock_config,
):
    """Test a successful run for a jader-like dataset, mocking all external dependencies."""
    # Arrange: Set up all the mocks
    mock_adapter = MagicMock()
    mock_get_db_adapter.return_value = mock_adapter

    mock_extractor_class = MagicMock()
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.extract.return_value = ("fake_path", "fake_url", {"new": "state"})
    mock_extractor_class.return_value = mock_extractor_instance
    mock_extractors.get.return_value = mock_extractor_class

    mock_parser_class = MagicMock()
    mock_parser_instance = MagicMock()
    mock_parser_instance.parse.return_value = pd.DataFrame({"raw": [1]}) # Parser returns a DataFrame
    mock_parser_class.return_value = mock_parser_instance
    mock_parsers.get.return_value = mock_parser_class

    # For a multi-table dataset like JADER, the transformer returns a dictionary of DataFrames
    mock_transformed_data = {"demo_table": pd.DataFrame({"clean": [1]})}
    mock_transformer_class = MagicMock()
    mock_transformer_instance = MagicMock()
    mock_transformer_instance.transform.return_value = mock_transformed_data
    # The transformer is initialized with a source_url
    mock_transformer_class.return_value = mock_transformer_instance
    mock_transformers.get.return_value = mock_transformer_class

    mock_schemas.INGESTION_STATE_SCHEMA = {"schema_name": "state_schema"}
    mock_schemas.DATASET_SCHEMAS.get.return_value = {"schema_def": "..."}
    mock_adapter.get_latest_state.return_value = {"old": "state"}

    # Act: Run the orchestrator for the 'jader' dataset
    orchestrator = Orchestrator(config=mock_config, dataset="jader")
    orchestrator.run()

    # Assert: Check that the correct methods were called in order
    mock_get_db_adapter.assert_called_once_with("postgres")
    mock_adapter.connect.assert_called_once()
    mock_adapter.ensure_schema.assert_called_once()
    mock_adapter.get_latest_state.assert_called_once_with("jader", schema="state_schema")

    mock_extractors.get.assert_called_once_with("JaderExtractor")
    mock_extractor_instance.extract.assert_called_once()

    mock_parsers.get.assert_called_once_with("JaderParser")
    mock_parser_instance.parse.assert_called_once_with("fake_path")

    mock_transformers.get.assert_called_once_with("JaderTransformer")
    mock_transformer_class.assert_called_once_with(source_url="fake_url")
    mock_transformer_instance.transform.assert_called_once_with(ANY) # Check that it was called with the parsed df

    # For multi-table, bulk_load is called for each table in the transformed dict
    mock_adapter.bulk_load.assert_called_once_with(
        data=mock_transformed_data["demo_table"],
        target_table="demo_table",
        schema="jader_schema",
        mode="overwrite",
    )

    mock_adapter.update_state.assert_called_once()
    mock_adapter.commit.assert_called_once()
    mock_adapter.close.assert_called_once()
