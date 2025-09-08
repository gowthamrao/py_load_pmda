import pytest
from typer.testing import CliRunner
from py_load_pmda.cli import app

runner = CliRunner()

def test_init_db_success(mocker):
    """
    Tests that the 'init-db' command succeeds and calls the correct methods.
    """
    # Mock the dependencies of the CLI command
    mock_load_config = mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}})
    mock_adapter_class = mocker.patch("py_load_pmda.cli.PostgreSQLAdapter")
    mock_adapter_instance = mock_adapter_class.return_value

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    assert "Database initialization complete" in result.stdout

    # Verify that the dependencies were used as expected
    mock_load_config.assert_called_once()
    mock_adapter_instance.connect.assert_called_once_with({})
    mock_adapter_instance.ensure_schema.assert_called_once()

def test_init_db_connection_error(mocker):
    """
    Tests that 'init-db' command fails gracefully on ConnectionError.
    """
    mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}})
    mock_adapter_class = mocker.patch("py_load_pmda.cli.PostgreSQLAdapter")
    mock_adapter_instance = mock_adapter_class.return_value

    # Configure the mock to raise an error
    mock_adapter_instance.connect.side_effect = ConnectionError("Test connection error")

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 1
    assert "Database initialization failed" in result.stdout
    assert "Test connection error" in result.stdout
