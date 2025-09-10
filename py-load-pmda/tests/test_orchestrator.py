import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path

from py_load_pmda.orchestrator import Orchestrator

@pytest.fixture
def mock_config():
    """Provides a mock configuration for tests."""
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
            "approvals": {
                "extractor": "ApprovalsExtractor",
                "parser": "ApprovalsParser",
                "transformer": "ApprovalsTransformer",
                "schema_name": "public",
                "table_name": "pmda_approvals",
                "load_mode": "overwrite",
            }
        },
        "logging": {"level": "DEBUG"},
        "alerting": [{"type": "log"}],
    }


def test_orchestrator_initialization(mock_config):
    """Test that the orchestrator initializes correctly."""
    with patch("py_load_pmda.orchestrator.AlertManager") as mock_alert_manager:
        orchestrator = Orchestrator(
            config=mock_config,
            dataset="approvals",
        )
        assert orchestrator.dataset == "approvals"
        assert orchestrator.config == mock_config
        mock_alert_manager.assert_called_once_with([{"type": "log"}])


@patch("py_load_pmda.orchestrator.get_db_adapter")
@patch("py_load_pmda.orchestrator.AVAILABLE_EXTRACTORS")
@patch("py_load_pmda.orchestrator.AVAILABLE_PARSERS")
@patch("py_load_pmda.orchestrator.AVAILABLE_TRANSFORMERS")
@patch("py_load_pmda.orchestrator.schemas")
@patch("py_load_pmda.orchestrator.AlertManager")
@patch("py_load_pmda.orchestrator.DataValidator")
def test_orchestrator_run_successful(
    mock_validator,
    mock_alert_manager,
    mock_schemas,
    mock_transformers,
    mock_parsers,
    mock_extractors,
    mock_get_db_adapter,
    mock_config,
):
    """Test a successful run, mocking all external dependencies."""
    # Arrange
    mock_adapter = MagicMock()
    mock_get_db_adapter.return_value = mock_adapter

    mock_extractor_class = MagicMock()
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.extract.return_value = (Path("fake_path"), "fake_url", {"new": "state"})
    mock_extractor_class.return_value = mock_extractor_instance
    mock_extractors.get.return_value = mock_extractor_class

    mock_parser_class = MagicMock()
    mock_parser_instance = MagicMock()
    mock_parser_instance.parse.return_value = [pd.DataFrame({"raw": [1]})]
    mock_parser_class.return_value = mock_parser_instance
    mock_parsers.get.return_value = mock_parser_class

    mock_transformed_data = pd.DataFrame({"clean": [1]})
    mock_transformer_class = MagicMock()
    mock_transformer_instance = MagicMock()
    mock_transformer_instance.transform.return_value = mock_transformed_data
    mock_transformer_class.return_value = mock_transformer_instance
    mock_transformers.get.return_value = mock_transformer_class

    mock_schemas.INGESTION_STATE_SCHEMA = {"schema_name": "state_schema"}
    # Make sure the table name matches the one in mock_config
    mock_schemas.DATASET_SCHEMAS.get.return_value = {
        "schema_name": "public",
        "tables": {"pmda_approvals": {"columns": {"clean": "TEXT"}}}
    }
    mock_adapter.get_latest_state.return_value = {"old": "state"}

    mock_validator_instance = MagicMock()
    mock_validator_instance.validate.return_value = True
    mock_validator.return_value = mock_validator_instance

    # Act
    orchestrator = Orchestrator(config=mock_config, dataset="approvals")
    orchestrator.run()

    # Assert
    mock_adapter.bulk_load.assert_called_once_with(
        data=mock_transformed_data,
        target_table="pmda_approvals",
        schema="public",
        mode="overwrite",
    )
    # Ensure alert manager was NOT called on success
    orchestrator.alert_manager.send.assert_not_called()
    # Ensure validator was not called if no rules in config
    mock_validator.assert_not_called()


@patch("py_load_pmda.orchestrator.get_db_adapter")
@patch("py_load_pmda.orchestrator.AVAILABLE_EXTRACTORS")
@patch("py_load_pmda.orchestrator.AVAILABLE_PARSERS")
@patch("py_load_pmda.orchestrator.AVAILABLE_TRANSFORMERS")
@patch("py_load_pmda.orchestrator.DataValidator")
@patch("py_load_pmda.orchestrator.AlertManager")
def test_orchestrator_validation_failure(
    mock_alert_manager,
    mock_validator,
    mock_transformers,
    mock_parsers,
    mock_extractors,
    mock_get_db_adapter,
    mock_config,
):
    """Test that the pipeline fails and alerts if data validation fails."""
    # Arrange
    mock_config["datasets"]["approvals"]["validation"] = [{"column": "col", "check": "not_null"}]

    mock_validator_instance = MagicMock()
    mock_validator_instance.validate.return_value = False
    mock_validator_instance.errors = ["Column 'col' has nulls"]
    mock_validator.return_value = mock_validator_instance

    mock_adapter = MagicMock()
    mock_get_db_adapter.return_value = mock_adapter

    mock_extractor_instance = MagicMock()
    mock_extractor_instance.extract.return_value = (Path("fake_path"), "fake_url", {"new": "state"})
    mock_extractors.get.return_value.return_value = mock_extractor_instance

    mock_parser_class = MagicMock()
    mock_parser_instance = MagicMock()
    mock_parser_instance.parse.return_value = [pd.DataFrame()]
    mock_parser_class.return_value = mock_parser_instance
    mock_parsers.get.return_value = mock_parser_class

    mock_transformer_class = MagicMock()
    mock_transformer_instance = MagicMock()
    failing_df = pd.DataFrame({"col": [None]})
    mock_transformer_instance.transform.return_value = failing_df
    mock_transformer_class.return_value = mock_transformer_instance
    mock_transformers.get.return_value = mock_transformer_class

    # Act & Assert
    orchestrator = Orchestrator(config=mock_config, dataset="approvals")
    with pytest.raises(ValueError, match="Data validation failed"):
        orchestrator.run()

    # Assert that the validator was called with the correct DataFrame
    mock_validator.assert_called_once_with(mock_config["datasets"]["approvals"]["validation"])
    mock_validator_instance.validate.assert_called_once()
    pd.testing.assert_frame_equal(mock_validator_instance.validate.call_args[0][0], failing_df)

    # Assert that an alert was sent
    orchestrator.alert_manager.send.assert_called_once()
    assert "Data validation failed" in orchestrator.alert_manager.send.call_args[0][0]
    mock_adapter.rollback.assert_called_once()


@patch("py_load_pmda.orchestrator.get_db_adapter")
@patch("py_load_pmda.orchestrator.AVAILABLE_EXTRACTORS")
@patch("py_load_pmda.orchestrator.AlertManager")
def test_orchestrator_extractor_failure(
    mock_alert_manager,
    mock_extractors,
    mock_get_db_adapter,
    mock_config,
):
    """Test that the pipeline fails and alerts if the extractor fails."""
    # Arrange
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.extract.side_effect = RuntimeError("Could not download file")
    mock_extractors.get.return_value.return_value = mock_extractor_instance

    mock_adapter = MagicMock()
    mock_get_db_adapter.return_value = mock_adapter

    # Act & Assert
    orchestrator = Orchestrator(config=mock_config, dataset="approvals")
    with pytest.raises(RuntimeError, match="Could not download file"):
        orchestrator.run()

    # Assert that an alert was sent
    orchestrator.alert_manager.send.assert_called_once()
    assert "ETL run failed" in orchestrator.alert_manager.send.call_args[0][0]
    mock_adapter.rollback.assert_called_once()
