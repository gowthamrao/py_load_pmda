import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class Alerter(ABC):
    """Abstract base class for all alerter implementations."""

    @abstractmethod
    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Sends an alert.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert.
        """
        pass


class LogAlerter(Alerter):
    """A simple alerter that writes messages to the log."""

    def send(self, message: str, subject: str = "Pipeline Alert") -> None:
        """
        Logs the alert message at the ERROR level.

        Args:
            message: The content of the alert message.
            subject: The subject or title of the alert.
        """
        logging.error(f"ALERT - {subject}: {message}")


class AlertManager:
    """
    Manages the configuration and dispatching of alerts.
    """

    def __init__(self, config: List[Dict[str, Any]]) -> None:
        """
        Initializes the AlertManager with a configuration.

        Args:
            config: A list of alerter configurations, e.g.,
                    [{'type': 'log'}, {'type': 'email', 'to': '...'}]
        """
        self.alerters: List[Alerter] = []
        if config:
            for alerter_config in config:
                alerter_type = alerter_config.get("type")
                if alerter_type == "log":
                    self.alerters.append(LogAlerter())
                # Future alerters like 'email' or 'slack' would be added here
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
