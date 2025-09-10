from typing import Any

from py_load_pmda.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_init_db_success(mocker: Any, caplog: Any) -> None:
    """
    Tests that the 'init-db' command succeeds and calls the correct methods.
    """
    mock_load_config = mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}, "logging": {"level": "INFO"}})
    mock_get_db_adapter = mocker.patch("py_load_pmda.cli.get_db_adapter")
    mock_adapter_instance = mock_get_db_adapter.return_value

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    assert "Database initialization complete" in caplog.text

    mock_load_config.assert_called_once()
    mock_get_db_adapter.assert_called_once()
    mock_adapter_instance.connect.assert_called_once_with({})
    mock_adapter_instance.ensure_schema.assert_called_once()
    mock_adapter_instance.commit.assert_called_once()
    mock_adapter_instance.close.assert_called_once()


def test_init_db_connection_error(mocker: Any, caplog: Any) -> None:
    """
    Tests that 'init-db' command fails gracefully on ConnectionError.
    """
    mocker.patch("py_load_pmda.cli.load_config", return_value={"database": {}, "logging": {"level": "INFO"}})
    mock_get_db_adapter = mocker.patch("py_load_pmda.cli.get_db_adapter")
    mock_adapter_instance = mock_get_db_adapter.return_value
    mock_adapter_instance.connect.side_effect = ConnectionError("Test connection error")

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 1
    assert "Database initialization failed" in caplog.text
    assert "Test connection error" in caplog.text
    mock_adapter_instance.rollback.assert_called_once()


def test_run_command_calls_orchestrator(mocker: Any, caplog: Any) -> None:
    """
    Tests that the 'run' command correctly initializes and calls the Orchestrator.
    This test verifies the CLI layer is correctly delegating, not the orchestrator's logic itself.
    """
    # 1. Mock all external dependencies of the 'run' command in cli.py
    mock_config_data = {"config": "data"}
    mocker.patch("py_load_pmda.cli.load_config", return_value=mock_config_data)
    mock_orchestrator_class = mocker.patch("py_load_pmda.cli.Orchestrator")
    mock_orchestrator_instance = mock_orchestrator_class.return_value

    # 2. Invoke the CLI runner with specific arguments
    result = runner.invoke(app, [
        "run",
        "--dataset", "approvals",
        "--year", "2023",
        "--mode", "full"
    ])

    # 3. Assert the outcome
    assert result.exit_code == 0

    # Assert that Orchestrator was initialized with the correct config and CLI args
    mock_orchestrator_class.assert_called_once_with(
        config=mock_config_data,
        dataset="approvals",
        mode="full",
        year=2023,
        drug_name=None,
    )

    # Assert that the orchestrator's run method was called
    mock_orchestrator_instance.run.assert_called_once()


def test_run_command_handles_orchestrator_exception(mocker: Any, caplog: Any) -> None:
    """Tests that the CLI's run command handles exceptions from the Orchestrator."""
    mocker.patch("py_load_pmda.cli.load_config")
    mock_orchestrator_class = mocker.patch("py_load_pmda.cli.Orchestrator")
    mock_orchestrator_instance = mock_orchestrator_class.return_value
    mock_orchestrator_instance.run.side_effect = ValueError("Something went wrong in the orchestrator")

    result = runner.invoke(app, ["run", "--dataset", "jader"])

    assert result.exit_code == 1
    assert "CLI-level error" in caplog.text
    assert "Something went wrong in the orchestrator" in caplog.text


def test_run_approvals_missing_year(mocker: Any) -> None:
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
    result = runner.invoke(app, ["run", "--dataset", "package_inserts"])

    assert result.exit_code == 1
    assert "Error: At least one '--drug-name' option is required" in result.output
