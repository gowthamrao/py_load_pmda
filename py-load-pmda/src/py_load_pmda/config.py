import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, cast

import yaml
from dotenv import load_dotenv

CONFIG_FILENAME = "config.yaml"
ENV_PREFIX = "PMDA_DB_"


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
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

    # This will load the .env file in the project root if it exists
    # It's safe to call this even if the file doesn't exist.
    load_dotenv()

    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, "r") as f:
        config = cast(Dict[str, Any], yaml.safe_load(f))

    # Override with environment variables
    if "database" in config:
        # Iterate over a copy of keys since we might add 'password' if it's not there
        for key in list(config["database"].keys()) + ["password"]:
            # Ensure 'password' is in the dict for env var lookup, even if not in yaml
            if key not in config["database"]:
                config["database"][key] = None

            env_var = f"{ENV_PREFIX}{key.upper()}"
            if env_var in os.environ:
                env_value = os.environ[env_var]
                original_type = type(config["database"][key])

                # Don't print the password value
                print_val = "****" if key == "password" else env_value
                logging.info(
                    f"Overriding config '{key}' with value from environment variable {env_var}: {print_val}"
                )

                # Attempt to cast env var to the same type as the default value
                try:
                    # Handle boolean case separately
                    if original_type is bool:
                        config["database"][key] = env_value.lower() in [
                            "true",
                            "1",
                            "t",
                            "y",
                            "yes",
                        ]
                    elif config["database"][key] is not None:
                        config["database"][key] = original_type(env_value)
                    else:
                        config["database"][key] = env_value
                except (ValueError, TypeError):
                    config["database"][key] = env_value

    # Handle logging configuration
    log_level_env = os.getenv("PMDA_LOG_LEVEL")
    if log_level_env:
        logging.info(f"Overriding log level with PMDA_LOG_LEVEL: {log_level_env}")
        if "logging" not in config:
            config["logging"] = {}
        config["logging"]["level"] = log_level_env.upper()

    # Handle extractor settings with sensible defaults
    default_extractor_settings = {
        "rate_limit_seconds": 1.0,
        "retries": 3,
        "backoff_factor": 0.5,
    }

    if "extractor_settings" in config:
        # Merge defaults into the existing settings
        # The settings from the file take precedence
        merged_settings = {**default_extractor_settings, **config["extractor_settings"]}
        config["extractor_settings"] = merged_settings
    else:
        # If the section doesn't exist, create it with defaults
        config["extractor_settings"] = default_extractor_settings

    logging.info(f"Extractor settings loaded: {config['extractor_settings']}")

    # After all overrides, check for mandatory password
    if not config.get("database", {}).get("password"):
        raise ValueError(
            "Database password not provided. "
            "Set the PMDA_DB_PASSWORD environment variable."
        )

    return config
