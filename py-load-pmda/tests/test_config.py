from pathlib import Path

import pytest
import yaml

from py_load_pmda.config import load_config


@pytest.fixture
def temp_config_file(tmp_path: Path) -> Path:
    """Creates a temporary config file for testing."""
    config_data = {
        "database": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "testuser",
        }
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    return config_path


def test_load_config_from_file(temp_config_file: Path) -> None:
    """
    Tests that the configuration is loaded correctly from a YAML file.
    """
    config = load_config(path=str(temp_config_file))
    assert config["database"]["host"] == "localhost"
    assert config["database"]["port"] == 5432
    assert config["database"]["user"] == "testuser"


def test_load_config_env_override(temp_config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests that environment variables correctly override config file values.
    """
    # Set environment variables to override config values
    monkeypatch.setenv("PMDA_DB_HOST", "db.example.com")
    monkeypatch.setenv("PMDA_DB_PORT", "1234")
    monkeypatch.setenv("PMDA_DB_USER", "produser")

    config = load_config(path=str(temp_config_file))

    assert config["database"]["host"] == "db.example.com"
    # Port should be cast to an integer
    assert config["database"]["port"] == 1234
    assert config["database"]["user"] == "produser"


def test_load_config_file_not_found() -> None:
    """
    Tests that a FileNotFoundError is raised if the config file does not exist.
    """
    with pytest.raises(FileNotFoundError):
        load_config(path="/non/existent/path/config.yaml")


def test_load_password_from_env_only(
    temp_config_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Tests that the password can be loaded from an environment variable even
    if it is not present in the config file at all. This validates the
    secure configuration approach.
    """
    # The temp_config_file fixture does not contain a 'password' key.
    # We set it only in the environment.
    monkeypatch.setenv("PMDA_DB_PASSWORD", "supersecret")

    config = load_config(path=str(temp_config_file))

    # The 'password' key should now exist in the config dictionary.
    assert "password" in config["database"]
    assert config["database"]["password"] == "supersecret"
