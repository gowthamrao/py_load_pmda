from pathlib import Path
from typing import Any

import pandas as pd
from py_load_pmda.cli import app
from typer.testing import CliRunner

runner = CliRunner()

def test_init_db_success(mocker: Any, caplog: Any) -> None:
    """
    Tests that the 'init-db' command succeeds and calls the correct methods.
    """
    # Mock the dependencies of the CLI command
    mock_load_config = mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}, "logging": {"level": "INFO"}})
    mock_adapter_class = mocker.patch("py_load_pmda.cli.PostgreSQLAdapter")
    mock_adapter_instance = mock_adapter_class.return_value

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    assert "Database initialization complete" in caplog.text

    # Verify that the dependencies were used as expected
    mock_load_config.assert_called_once()
    mock_adapter_instance.connect.assert_called_once_with({})
    mock_adapter_instance.ensure_schema.assert_called_once()

def test_init_db_connection_error(mocker: Any, caplog: Any) -> None:
    """
    Tests that 'init-db' command fails gracefully on ConnectionError.
    """
    mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}, "logging": {"level": "INFO"}})
    mock_adapter_class = mocker.patch("py_load_pmda.cli.PostgreSQLAdapter")
    mock_adapter_instance = mock_adapter_class.return_value

    # Configure the mock to raise an error
    mock_adapter_instance.connect.side_effect = ConnectionError("Test connection error")

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 1
    assert "Database initialization failed" in caplog.text
    assert "Test connection error" in caplog.text


def test_run_package_inserts_success(mocker: Any, caplog: Any) -> None:
    """
    Tests the 'run' command for the 'package_inserts' dataset.
    This test verifies that the CLI correctly parses arguments and orchestrates
    the ETL process by calling the right components with the right parameters.
    """
    # 1. Mock all external dependencies of the 'run' command
    mocker.patch("py_load_pmda.cli.load_config", return_value={
        "logging": {"level": "INFO"},
        "database": {"type": "postgres"},
        "datasets": {
            "package_inserts": {
                "extractor": "PackageInsertsExtractor",
                "parser": "PackageInsertsParser",
                "transformer": "PackageInsertsTransformer",
                "load_mode": "merge",
                "table_name": "pmda_package_inserts",
                "schema_name": "public",
                "primary_key": ["document_id"]
            }
        }
    })
    mock_db_adapter = mocker.patch("py_load_pmda.cli.get_db_adapter").return_value
    mock_db_adapter.get_latest_state.return_value = {} # Assume no prior state
    mock_db_adapter.execute_sql = mocker.MagicMock() # Mock the missing method

    # Mock the ETL classes and schema definitions
    mocker.patch("py_load_pmda.cli.schemas.DATASET_SCHEMAS", {
        "package_inserts": {
            "tables": {
                "pmda_package_inserts": {
                    "columns": {"col1": "TEXT"}
                }
            }
        }
    })
    mock_extractor_class = mocker.patch("py_load_pmda.cli.AVAILABLE_EXTRACTORS")
    mock_parser_class = mocker.patch("py_load_pmda.cli.AVAILABLE_PARSERS")
    mock_transformer_class = mocker.patch("py_load_pmda.cli.AVAILABLE_TRANSFORMERS")

    # 2. Define the behavior of the mocked ETL components
    mock_extractor = mock_extractor_class.get.return_value.return_value
    mock_parser = mock_parser_class.get.return_value.return_value
    mock_transformer = mock_transformer_class.get.return_value

    # The extractor should return the new data structure: (file_path, source_url)
    mock_file_path = mocker.MagicMock(spec=Path)
    mock_file_path.name = "test.pdf"
    mock_source_url = "https://example.com/test.pdf"
    mock_extractor.extract.return_value = ([(mock_file_path, mock_source_url)], {"state": "new"})

    # The parser returns a tuple of (text, tables)
    mock_parser.parse.return_value = ("dummy text", [pd.DataFrame({"data": [1]})])

    # 3. Invoke the CLI runner
    result = runner.invoke(app, [
        "run",
        "--dataset", "package_inserts",
        "--drug-name", "test-drug-1",
        "--drug-name", "test-drug-2"
    ])

    # 4. Assert the outcome
    assert result.exit_code == 0
    assert "ETL run for dataset 'package_inserts' completed successfully" in caplog.text

    # Assert that the extractor was called with the drug names from the CLI
    mock_extractor.extract.assert_called_once()
    call_args, call_kwargs = mock_extractor.extract.call_args
    assert call_kwargs['drug_names'] == ["test-drug-1", "test-drug-2"]

    # Assert that the transformer was initialized with the correct source_url
    mock_transformer.assert_called_once_with(source_url=mock_source_url)


def test_run_package_inserts_missing_drug_name(mocker: Any) -> None:
    """
    Tests that the 'run' command fails if 'package_inserts' is specified
    without the '--drug-name' option.
    """
    # Mock config loading to prevent it from trying to read a real file
    mocker.patch("py_load_pmda.cli.load_config", return_value={
        "database": {"type": "postgres"},
        "datasets": {
            "package_inserts": {}
        }
    })
    result = runner.invoke(app, ["run", "--dataset", "package_inserts"], catch_exceptions=True)

    assert result.exit_code == 1
    assert result.exc_info is not None
    assert isinstance(result.exc_info[1], ValueError)
    assert "At least one '--drug-name' option is required" in str(result.exc_info[1])
