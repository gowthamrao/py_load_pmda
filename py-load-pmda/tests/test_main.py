import subprocess
import sys

import pytest


def test_main_script_execution():
    """
    Tests that the cli.py script can be executed directly.
    We pass '--help' to ensure it runs and exits cleanly without
    performing any real operations.
    """
    # Get the path to the current python executable
    python_executable = sys.executable
    # Get the path to the cli.py script
    cli_path = "src/py_load_pmda/cli.py"

    # Execute the script as a subprocess
    result = subprocess.run(
        [python_executable, cli_path, "--help"],
        capture_output=True,
        text=True,
        check=False,  # Don't raise exception on non-zero exit
    )

    # Assert that the script ran successfully
    assert result.returncode == 0
    # Assert that the help message was printed
    assert "Usage:" in result.stdout
    assert "init-db" in result.stdout
    assert "run" in result.stdout
    assert "status" in result.stdout


def test_main_script_handles_error():
    """
    Tests that the cli.py script handles errors gracefully when executed directly.
    """
    python_executable = sys.executable
    cli_path = "src/py_load_pmda/cli.py"

    # Execute the script with an invalid command
    result = subprocess.run(
        [python_executable, cli_path, "invalid-command"],
        capture_output=True,
        text=True,
        check=False,
    )

    # Assert that the script exited with a non-zero status code
    assert result.returncode != 0
    # Assert that an error message was printed
    assert "Error" in result.stderr
    assert "No such command 'invalid-command'" in result.stderr
