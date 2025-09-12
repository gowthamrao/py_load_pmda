import os
import pytest
from py_load_pmda.config import load_config

def test_load_config_missing_password_raises_error(monkeypatch):
    """Test that load_config raises a ValueError if PMDA_DB_PASSWORD is not set."""
    # Ensure the environment variable is not set
    monkeypatch.delenv("PMDA_DB_PASSWORD", raising=False)

    with pytest.raises(ValueError) as excinfo:
        load_config()

    assert "Database password not provided" in str(excinfo.value)
