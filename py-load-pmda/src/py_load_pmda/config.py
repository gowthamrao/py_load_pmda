import os
import yaml
from pathlib import Path

CONFIG_FILENAME = "config.yaml"
ENV_PREFIX = "PMDA_DB_"

def load_config(path: str = None) -> dict:
    """
    Loads configuration from a YAML file and overrides with environment variables.

    The function looks for `config.yaml` in the path provided or in the current
    directory.

    Database settings can be overridden by environment variables with the prefix
    PMDA_DB_. For example, `PMDA_DB_HOST` will override the `host` setting in
    the `database` section of the config file.

    Args:
        path: The path to the config file. If None, looks in the current dir.

    Returns:
        A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the configuration file cannot be found.
    """
    if path:
        config_path = Path(path)
    else:
        # Look for config.yaml in the project root relative to this file
        # This makes it robust to where the script is called from.
        # src/py_load_pmda/config.py -> src/py_load_pmda -> src -> project_root
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / CONFIG_FILENAME

    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Override with environment variables
    if "database" in config:
        for key, value in config["database"].items():
            env_var = f"{ENV_PREFIX}{key.upper()}"
            if env_var in os.environ:
                # Attempt to cast env var to the same type as the default value
                original_type = type(value)
                try:
                    config["database"][key] = original_type(os.environ[env_var])
                except (ValueError, TypeError):
                    config["database"][key] = os.environ[env_var]

    return config
