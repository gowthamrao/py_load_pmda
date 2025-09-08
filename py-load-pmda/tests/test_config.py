import os
import yaml
import pytest
from pathlib import Path
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

def test_load_config_from_file(temp_config_file: Path):
    """
    Tests that the configuration is loaded correctly from a YAML file.
    """
    config = load_config(path=str(temp_config_file))
    assert config["database"]["host"] == "localhost"
    assert config["database"]["port"] == 5432
    assert config["database"]["user"] == "testuser"

def test_load_config_env_override(temp_config_file: Path, monkeypatch):
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

def test_load_config_file_not_found():
    """
    Tests that a FileNotFoundError is raised if the config file does not exist.
    """
    with pytest.raises(FileNotFoundError):
        load_config(path="/non/existent/path/config.yaml")
