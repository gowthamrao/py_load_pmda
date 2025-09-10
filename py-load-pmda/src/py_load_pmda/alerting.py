import logging
from typing import Any, Dict, List

from py_load_pmda.alerters.base import Alerter
from py_load_pmda.alerters.log import LogAlerter
from py_load_pmda.alerters.slack import SlackAlerter


class AlertManager:
    """
    Manages the configuration and dispatching of alerts.
    """

    def __init__(self, config: List[Dict[str, Any]]) -> None:
        """
        Initializes the AlertManager with a configuration.

        Args:
            config: A list of alerter configurations, e.g.,
                    [{'type': 'log'}, {'type': 'slack', 'token': '...', 'channel': '...'}]
        """
        self.alerters: List[Alerter] = []
        if config:
            for alerter_config in config:
                alerter_type = alerter_config.get("type")
                if alerter_type == "log":
                    self.alerters.append(LogAlerter())
                elif alerter_type == "slack":
                    try:
                        self.alerters.append(SlackAlerter(alerter_config))
                    except (ValueError, ImportError) as e:
                        logging.error(f"Failed to initialize SlackAlerter: {e}")
                else:
                    logging.warning(f"Unknown alerter type '{alerter_type}' found in config.")

    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Dispatches an alert to all configured alerters.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert.
        """
        if not self.alerters:
            logging.debug("No alerters configured, skipping alert.")
            return

        for alerter in self.alerters:
            try:
                alerter.send(message, subject=subject)
            except Exception as e:
                logging.error(f"Failed to send alert using {alerter.__class__.__name__}: {e}")
